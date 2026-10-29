#!/usr/bin/env python3
"""第一階段資料管線（第 1 步：FRED + 台幣備用鏈 + 真實寫入 Supabase）。

原則：
- 查不到就記錄查不到，不編數字、不編日期。
- 日期一律是資料自己的日期，不用抓取日冒充。
- 寫入 Supabase 後必須由回傳確認筆數，沒確認不算成功。
- 每次檢查（成功／無新資料／失敗／略過）都留帶時間的紀錄，並寫進 scan_status。
- 有任何 failure，程式以結束碼 1 結束，讓 GitHub Actions 亮紅燈。

用法：
    python3 pipeline/fetch.py --job full            # FRED 總經 + 台幣
    python3 pipeline/fetch.py --job tw_close        # 台幣（台股資料第二步才接）
    python3 pipeline/fetch.py --job us_breadth      # 美股廣度（尚未實作，只留紀錄）
    python3 pipeline/fetch.py --job full --dry-run  # 照常抓資料，但不寫 Supabase
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline import config  # noqa: E402

LOG = []
CURRENT_JOB = "full"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


# ---------------------------------------------------------------- 紀錄
def _scrub(text):
    """把金鑰從任何文字中遮掉，避免寫進紀錄。"""
    text = str(text)
    for secret in (config.fred_key(), config.supabase_key()):
        if secret:
            text = text.replace(secret, "***")
    return text


def log(source, status, as_of=None, detail=""):
    row = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "job": CURRENT_JOB,
        "source": source,
        "status": status,  # success / no_new_data / failure / skipped
        "as_of": as_of,
        "detail": _scrub(detail),
    }
    LOG.append(row)
    print(json.dumps(row, ensure_ascii=False))


# ---------------------------------------------------------------- 驗證
def validate_close_date(as_of, fetch_date=None):
    """日期必須有值、格式正確、不得晚於抓取日。"""
    if not as_of:
        return False, "缺少日期"
    try:
        d = date.fromisoformat(str(as_of)[:10])
    except ValueError:
        return False, f"日期格式不正確: {as_of}"
    fetch = fetch_date or date.today()
    if d > fetch:
        return False, f"日期 {d} 晚於抓取日 {fetch}，疑似冒充"
    return True, ""


def validate_value(raw, low, high):
    """數值必須是數字且在合理範圍。回傳 (數值或 None, 原因)。"""
    if raw in (None, "", "."):
        return None, "值為空（假日或未公布）"
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None, f"不是數字: {raw}"
    if not (low <= v <= high):
        return None, f"數值 {v} 超出合理範圍 {low}~{high}，視為異常，拒收"
    return v, ""


# ---------------------------------------------------------------- 網路
def fetch_json(url, headers=None, timeout=None):
    """帶重試的抓取。失敗回傳 (None, 原因)，不拋出。"""
    last_err = ""
    for attempt in range(1, config.RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers=headers or {"User-Agent": "risk-pwa/1.0"})
            with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
                return json.loads(resp.read().decode("utf-8")), ""
        except Exception as e:  # noqa: BLE001
            last_err = _scrub(e)
            if attempt < config.RETRIES:
                time.sleep(config.BACKOFF_SECONDS * attempt)
    return None, last_err


# ---------------------------------------------------------------- Supabase
def sb_ready():
    return bool(config.supabase_url() and config.supabase_key())


def sb_request(method, path, body=None, prefer=None):
    """呼叫 Supabase REST。回傳 (資料, 錯誤)。"""
    url = config.supabase_url().rstrip("/") + "/rest/v1/" + path
    key = config.supabase_key()
    headers = {"apikey": key, "Content-Type": "application/json"}
    if key.startswith("eyJ"):  # 舊式 JWT 金鑰才需要 Authorization
        headers["Authorization"] = "Bearer " + key
    if prefer:
        headers["Prefer"] = prefer
    data = json.dumps(body).encode("utf-8") if body is not None else None
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=config.TIMEOUT_SECONDS) as resp:
            raw = resp.read()
            return (json.loads(raw) if raw else []), ""
    except urllib.error.HTTPError as e:
        return None, _scrub(f"HTTP {e.code}: {e.read()[:300]!r}")
    except Exception as e:  # noqa: BLE001
        return None, _scrub(e)


def sb_upsert(table, rows, on_conflict):
    """寫入並用回傳筆數確認。回傳 (成功與否, 說明)。"""
    if not rows:
        return False, "沒有要寫的列"
    result, err = sb_request(
        "POST", f"{table}?on_conflict={on_conflict}", rows,
        prefer="resolution=merge-duplicates,return=representation",
    )
    if result is None:
        return False, f"寫入失敗: {err}"
    if len(result) != len(rows):
        return False, f"回傳筆數 {len(result)} 與送出筆數 {len(rows)} 不符，不算寫入成功"
    return True, f"已確認寫入 {len(result)} 筆"


def sb_last_verified_twd():
    """讀回上一筆已驗證的 FRED 台幣收盤值。"""
    rows, err = sb_request(
        "GET", "twd_rates?is_close_history=eq.true&order=as_of.desc&limit=1&select=as_of,value,source,layer"
    )
    if rows:
        return rows[0]
    return None


# ---------------------------------------------------------------- 來源：FRED
def fred_latest(series_id, low, high):
    """取 FRED 最近 10 筆，挑第一筆有效值，日期用該筆自己的日期。"""
    key = config.fred_key()
    if not key:
        log(f"fred:{series_id}", "skipped", detail="未設定 FRED_API_KEY，略過；不編數字")
        return None
    url = (f"{config.FRED_SERIES_URL}?series_id={series_id}&api_key={key}"
           f"&file_type=json&sort_order=desc&limit=10")
    data, err = fetch_json(url)
    if data is None:
        log(f"fred:{series_id}", "failure", detail=f"FRED 抓取失敗: {err}")
        return None
    try:
        observations = data["observations"]
    except (KeyError, TypeError):
        log(f"fred:{series_id}", "failure", detail="FRED 回傳格式不符")
        return None
    last_reason = "沒有任何觀測值"
    for obs in observations:
        v, why = validate_value(obs.get("value"), low, high)
        if v is None:
            last_reason = why
            if "超出合理範圍" in why or "不是數字" in why:
                log(f"fred:{series_id}", "failure", as_of=obs.get("date"), detail=why)
                return None
            continue  # 假日空值，往前找
        ok, why_date = validate_close_date(obs.get("date"))
        if not ok:
            log(f"fred:{series_id}", "failure", as_of=obs.get("date"), detail=why_date)
            return None
        age = (date.today() - date.fromisoformat(obs["date"])).days
        log(f"fred:{series_id}", "success", as_of=obs["date"], detail=f"FRED 官方資料，距今 {age} 天")
        return {"series_id": series_id, "value": v, "as_of": obs["date"]}
    log(f"fred:{series_id}", "no_new_data", detail=f"最近 10 筆皆無有效值（{last_reason}）")
    return None


def exchangerate_twd():
    """第 5 層：只接受 success 且 base 為 USD；日期取服務自己的更新時間。"""
    data, err = fetch_json(config.EXCHANGERATE_API)
    if data is None:
        log("exchangerate_api:USD", "failure", detail=f"抓取失敗: {err}")
        return None
    if data.get("result") != "success" or data.get("base_code") != "USD":
        log("exchangerate_api:USD", "failure", detail="回傳非 success 或基準貨幣非 USD，拒收")
        return None
    try:
        raw = data["rates"]["TWD"]
        ts = data.get("time_last_update_unix")
        as_of = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat() if ts else None
    except (KeyError, TypeError, ValueError, OSError):
        log("exchangerate_api:USD", "failure", detail="回傳格式不符")
        return None
    v, why = validate_value(raw, *config.TWD_RANGE)
    if v is None:
        log("exchangerate_api:USD", "failure", as_of=as_of, detail=why)
        return None
    ok, why = validate_close_date(as_of)
    if not ok:
        log("exchangerate_api:USD", "failure", as_of=as_of, detail=why)
        return None
    log("exchangerate_api:USD", "success", as_of=as_of,
        detail="即期彙總價，不是官方收盤價；不進收盤歷史")
    return {"value": v, "as_of": as_of}


# ---------------------------------------------------------------- 台幣備用鏈
def twd_fallback_chain():
    """照順序試，第一個成功即停。回傳要寫入 twd_rates 的列（或 None / 沿用舊值）。"""
    for sym in ("TWD=X", "USDTWD=X"):
        log(f"twd_layer:yahoo:{sym}", "skipped", detail="本階段不使用 Yahoo（排程 IP 易被擋），走下一層")

    fred = fred_latest(config.TWD_FRED_SERIES, *config.TWD_RANGE)
    if fred:
        return {"as_of": fred["as_of"], "value": fred["value"], "kind": "official_close",
                "source": "FRED DEXTAUS", "layer": 3, "is_close_history": True}

    log("twd_layer:taiwan_official", "skipped", detail="第二階段才接台銀牌告／央行 CPX")

    spot = exchangerate_twd()
    if spot:
        return {"as_of": spot["as_of"], "value": spot["value"], "kind": "spot_aggregator",
                "source": "ExchangeRate-API", "layer": 5, "is_close_history": False}

    # 五層皆失敗：沿用上一筆已驗證值，標示它真正的日期，不重新寫入
    last = sb_last_verified_twd() if sb_ready() else None
    if last:
        log("twd_chain", "failure", as_of=last["as_of"],
            detail=f"五層皆失敗；畫面沿用上一筆已驗證值，真正日期 {last['as_of']}")
        return {"stale_fallback": True, "as_of": last["as_of"], "value": last["value"],
                "source": last["source"]}
    log("twd_chain", "failure", detail="五層皆失敗，且沒有上一筆已驗證值可沿用")
    return None


# ---------------------------------------------------------------- 寫入
def persist(dry_run, macro_rows, twd_row):
    if dry_run:
        log("persist", "skipped", detail=f"--dry-run：不寫 Supabase（本來會寫 macro {len(macro_rows)} 筆、"
                                         f"台幣 {1 if twd_row and not twd_row.get('stale_fallback') else 0} 筆）")
        return
    if not sb_ready():
        log("persist", "skipped", detail="未設定 SUPABASE_URL / SUPABASE_SERVICE_KEY，只寫本機檔，未寫正式資料庫")
        return
    if macro_rows:
        ok, msg = sb_upsert("macro_series", macro_rows, "series_id,as_of")
        log("persist:macro_series", "success" if ok else "failure", detail=msg)
    if twd_row and not twd_row.get("stale_fallback"):
        ok, msg = sb_upsert("twd_rates", [twd_row], "layer,source,as_of")
        log("persist:twd_rates", "success" if ok else "failure", detail=msg)


def flush_log(dry_run):
    """把本次紀錄寫進 scan_status。這一步失敗也要讓程式亮紅燈。"""
    if dry_run or not sb_ready():
        return True
    rows = [dict(r) for r in LOG]
    ok, msg = sb_upsert_plain("scan_status", rows)
    if not ok:
        print(f"[警告] 紀錄寫入 scan_status 失敗: {msg}", file=sys.stderr)
    return ok


def sb_upsert_plain(table, rows):
    result, err = sb_request("POST", table, rows, prefer="return=representation")
    if result is None:
        return False, f"寫入失敗: {err}"
    if len(result) != len(rows):
        return False, f"回傳筆數 {len(result)} 與送出 {len(rows)} 不符"
    return True, f"已確認寫入 {len(result)} 筆"


# ---------------------------------------------------------------- 主流程
def run_job(job, dry_run=False):
    global CURRENT_JOB
    CURRENT_JOB = job
    macro_rows, twd_row = [], None

    if job == "full":
        for sid, (label, low, high) in config.FRED_MACRO.items():
            r = fred_latest(sid, low, high)
            if r:
                macro_rows.append({"series_id": sid, "label": label, "as_of": r["as_of"],
                                   "value": r["value"], "source": "FRED"})
    if job in ("full", "tw_close"):
        twd_row = twd_fallback_chain()
    if job == "tw_close":
        log("tw_market", "skipped", detail="台股收盤資料（證交所 OpenAPI）第二步才接")
    if job == "us_breadth":
        log("us_breadth", "skipped", detail="美股廣度尚未實作（第三步）；此排程不再偷跑全部更新")

    persist(dry_run, macro_rows, twd_row)
    return {"job": job, "macro": macro_rows, "twd": twd_row}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=config.JOBS, default="full")
    parser.add_argument("--dry-run", action="store_true", help="照常抓資料，但不寫 Supabase")
    args = parser.parse_args(argv)

    result = run_job(args.job, dry_run=args.dry_run)
    log_ok = flush_log(args.dry_run)

    os.makedirs(OUT_DIR, exist_ok=True)
    output = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "dry_run" if args.dry_run else "live",
        "supabase_configured": sb_ready(),
        "result": result,
        "log": LOG,
    }
    with open(os.path.join(OUT_DIR, "output.json"), "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    with open(os.path.join(OUT_DIR, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(LOG, f, ensure_ascii=False, indent=2)

    failures = [r for r in LOG if r["status"] == "failure"]
    print(f"完成：{len(LOG)} 筆紀錄，其中 {len(failures)} 筆失敗")
    return 1 if (failures or not log_ok) else 0


if __name__ == "__main__":
    raise SystemExit(main())
