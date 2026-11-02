#!/usr/bin/env python3
"""第一階段資料管線（第 1～2 步：FRED + 台幣備用鏈 + 證交所加權指數／成交金額 + 真實寫入 Supabase）。

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
from collections import Counter
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

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


FETCH_VERSION = "v41"      # 每次發行跟 config.PIPELINE_VERSION 一起改；兩者不同就代表檔案沒有一起更新


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
            req = urllib.request.Request(url, headers=headers or {"User-Agent": "risk-pwa/1.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
                return json.loads(resp.read().decode("utf-8-sig").lstrip("\ufeff")), ""  # 期交所回傳開頭帶 BOM（連續多個也處理）
        except Exception as e:  # noqa: BLE001
            last_err = _scrub(e)
            if attempt < config.RETRIES:
                time.sleep(config.BACKOFF_SECONDS * attempt)
    return None, last_err


def fetch_raw(url, timeout=None, headers=None):
    """診斷用：不解析 JSON，只回傳 (HTTP 狀態, Content-Type, 原始位元組, 錯誤)。"""
    headers = headers or {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), resp.read(), ""
    except urllib.error.HTTPError as e:
        ctype = e.headers.get("Content-Type", "") if e.headers else ""
        return e.code, ctype, e.read()[:2000], f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, "", b"", _scrub(e)


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
def fred_latest(series_id, low, high, optional=False):
    """取 FRED 最近 10 筆，挑第一筆有效值，日期用該筆自己的日期。"""
    key = config.fred_key()
    if not key:
        log(f"fred:{series_id}", "skipped", detail="未設定 FRED_API_KEY，略過；不編數字")
        return None
    url = (f"{config.FRED_SERIES_URL}?series_id={series_id}&api_key={key}"
           f"&file_type=json&sort_order=desc&limit=10")
    data, err = fetch_json(url)
    if data is None:
        if optional:
            log(f"fred:{series_id}", "no_new_data", detail=_clip(f"FRED 查不到這個序列（可能不存在或授權限制）: {err}；這是選用項目，不算失敗"))
        else:
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


# ---------------------------------------------------------------- 來源：證交所
def parse_number(text):
    """'1,234.56' -> 1234.56；無法解析回傳 None。"""
    try:
        return float(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def twse_date_to_iso(text):
    """'2026/08/24' -> '2026-08-24'；格式不符回傳 None。"""
    try:
        y, m, d = str(text).strip().split("/")
        return date(int(y), int(m), int(d)).isoformat()
    except (ValueError, TypeError):
        return None


def _twse_month_rows(url):
    """抓一個月份的資料。回傳 (rows, 原因)。rows 是已驗證的 market_stats 列。"""
    data, err = fetch_json(url)
    if data is None:
        return None, f"證交所抓取失敗: {err}"
    if data.get("stat") != "OK":
        return [], f"證交所回覆 stat={data.get('stat')}（該月份可能尚無資料）"
    try:
        fields = data["fields"]
        idx = {name: fields.index(name) for name in ("Date", "Trade Value", "TAIEX", "Change")}
        raw_rows = data["data"]
    except (KeyError, ValueError, TypeError):
        return None, "證交所回傳欄位與預期不符（Date/Trade Value/TAIEX/Change）"

    rows, rejected = [], 0
    for r in raw_rows:
        as_of = twse_date_to_iso(r[idx["Date"]])
        close, change = parse_number(r[idx["TAIEX"]]), parse_number(r[idx["Change"]])
        turnover = parse_number(r[idx["Trade Value"]])
        ok_date, _ = validate_close_date(as_of)
        if (not ok_date or close is None or change is None or turnover is None
                or not (config.TAIEX_RANGE[0] <= close <= config.TAIEX_RANGE[1]) or turnover <= 0):
            rejected += 1
            continue
        prev_close = close - change
        if prev_close <= 0:
            rejected += 1
            continue
        rows.append({
            "market": "TW", "as_of": as_of, "close": close,
            "change_pct": round(change / prev_close * 100, 4),  # 由官方漲跌點數換算
            "turnover": turnover,  # 成交金額，單位：新台幣元
            "source": "TWSE FMTQIK",
        })
    if rejected:
        log("twse:FMTQIK", "failure", detail=f"{rejected} 列資料驗證不過（日期／數值異常），已拒收")
    return rows, ""


def twse_market():
    """加權指數收盤與成交金額。當月沒資料（例如月初）就往前一個月找。
    注意：本來源沒有漲跌家數，廣度不在此處理，也不用其他數字冒充。"""
    rows, why = _twse_month_rows(config.TWSE_FMTQIK)
    if rows is None:
        log("twse:FMTQIK", "failure", detail=why)
        return None
    if not rows:
        today = date.today()
        py, pm = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
        prev_url = f"{config.TWSE_FMTQIK}?response=json&date={py}{pm:02d}01"
        rows, why2 = _twse_month_rows(prev_url)
        if rows is None:
            log("twse:FMTQIK", "failure", detail=why2)
            return None
    if not rows:
        log("twse:FMTQIK", "no_new_data", detail="當月與上月皆無可用資料")
        return None
    latest = max(r["as_of"] for r in rows)
    age = (date.today() - date.fromisoformat(latest)).days
    log("twse:FMTQIK", "success", as_of=latest,
        detail=f"共 {len(rows)} 個交易日；最新收盤日 {latest}（距今 {age} 天）")
    return rows


def parse_count(text):
    """'3,381(16)' -> (3381, 16)；'753' -> (753, None)；無法解析 -> (None, None)。"""
    m = re.fullmatch(r"\s*([\d,]+)\s*(?:\(\s*([\d,]+)\s*\))?\s*", str(text))
    if not m:
        return None, None
    n = int(m.group(1).replace(",", ""))
    extra = int(m.group(2).replace(",", "")) if m.group(2) else None
    return n, extra


def twse_breadth():
    """台股市場廣度（漲跌家數），取證交所『漲跌證券數合計』的「股票」欄（不含權證、ETF 等）。
    日期用證交所回傳的資料日期；格式或數字不合理一律拒收，不冒充。"""
    data, err = fetch_json(config.TWSE_MI_INDEX_MS)
    if data is None:
        log("twse:breadth", "failure", detail=f"抓取失敗: {err}")
        return None
    if data.get("stat") != "OK":
        log("twse:breadth", "no_new_data", detail=f"證交所回覆 stat={data.get('stat')}")
        return None

    raw = str(data.get("date", ""))
    try:
        as_of = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])).isoformat()
    except (ValueError, TypeError):
        log("twse:breadth", "failure", detail=f"資料日期格式不符: {raw!r}")
        return None
    ok, why = validate_close_date(as_of)
    if not ok:
        log("twse:breadth", "failure", as_of=as_of, detail=why)
        return None

    table = next((t for t in (data.get("tables") or []) if "漲跌證券數合計" in str(t.get("title"))), None)
    if table is None:
        log("twse:breadth", "failure", as_of=as_of, detail="找不到『漲跌證券數合計』表格")
        return None
    fields = table.get("fields") or []
    if "類型" not in fields or "股票" not in fields:
        log("twse:breadth", "failure", as_of=as_of, detail=f"欄位與預期不符: {fields}")
        return None
    i_label, i_stock = fields.index("類型"), fields.index("股票")

    got = {}
    for r in table.get("data") or []:
        label = str(r[i_label])
        for prefix, name in (("上漲", "advancers"), ("下跌", "decliners"), ("持平", "unchanged")):
            if label.startswith(prefix):
                got[name] = parse_count(r[i_stock])
    if any(got.get(k, (None, None))[0] is None for k in ("advancers", "decliners", "unchanged")):
        log("twse:breadth", "failure", as_of=as_of, detail="上漲／下跌／持平其中一項缺漏或無法解析")
        return None

    adv, dec, unch = got["advancers"][0], got["decliners"][0], got["unchanged"][0]
    lo, hi = config.BREADTH_TOTAL_RANGE
    if not (lo <= adv + dec + unch <= hi):
        log("twse:breadth", "failure", as_of=as_of, detail=f"家數合計 {adv + dec + unch} 超出合理範圍，拒收")
        return None
    log("twse:breadth", "success", as_of=as_of,
        detail=f"股票欄：上漲{adv}(漲停{got['advancers'][1]}) 下跌{dec}(跌停{got['decliners'][1]}) 持平{unch}")
    return [{"market": "TW", "as_of": as_of, "advancers": adv, "decliners": dec,
             "unchanged": unch, "source": config.BREADTH_SOURCE}]


def twse_ymd_to_iso(raw):
    """'20261029' -> '2026-10-29'；格式不符回傳 None。"""
    raw = str(raw or "")
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])).isoformat()
    except (ValueError, TypeError):
        return None


def _twse_fetch_day(url, tag):
    """抓證交所單日資料並驗證 stat 與資料日期。回傳 (資料, 日期) 或 None（已留紀錄）。"""
    data, err = fetch_json(url)
    if data is None:
        log(tag, "failure", detail=f"抓取失敗: {err}")
        return None
    if not isinstance(data, dict) or data.get("stat") != "OK":
        log(tag, "no_new_data", detail=f"證交所回覆 stat={data.get('stat') if isinstance(data, dict) else '非預期格式'}")
        return None
    as_of = twse_ymd_to_iso(data.get("date"))
    ok, why = validate_close_date(as_of)
    if not ok:
        log(tag, "failure", as_of=as_of, detail=f"資料日期異常: {why}")
        return None
    return data, as_of


def _parse_institutional(data, as_of):
    """三大法人買賣金額（現貨）的解析與驗證，不寫紀錄。回傳 (列, 備註, 錯誤原因)。
    外資 = 外資及陸資(不含外資自營商) + 外資自營商。單位：新台幣元。買進－賣出 必須等於買賣差額，對不上就拒收。"""
    fields = data.get("fields") or []
    need = ("單位名稱", "買進金額", "賣出金額", "買賣差額")
    if any(n not in fields for n in need):
        return None, "", f"欄位與預期不符: {fields}"
    ix = {n: fields.index(n) for n in need}
    table = {}
    for r in data.get("data") or []:
        try:
            label = str(r[ix["單位名稱"]]).strip()
            table[label] = tuple(parse_number(r[ix[n]]) for n in need[1:])
        except (IndexError, TypeError):
            continue

    def find(pred):
        for label, triple in table.items():
            if pred(label.replace(" ", "")):
                return triple
        return None

    main = find(lambda l: l.startswith("外資及陸資"))
    if main is None or None in main:
        return None, "", "找不到『外資及陸資』列或數字無法解析"
    fd = find(lambda l: l == "外資自營商")
    note = ""
    if fd is None or None in fd:
        fd, note = (0.0, 0.0, 0.0), "（外資自營商列缺漏，未計入）"
    for buy, sell, diff in (main, fd):
        if abs(diff - (buy - sell)) > 1:
            return None, "", "買進－賣出 與買賣差額對不上，拒收"
    f_buy, f_sell, f_net = main[0] + fd[0], main[1] + fd[1], main[2] + fd[2]
    if not (0 <= f_buy <= config.INST_SIDE_LIMIT and 0 <= f_sell <= config.INST_SIDE_LIMIT
            and abs(f_net) <= config.INST_NET_LIMIT):
        return None, "", f"外資金額 {f_buy}/{f_sell} 超出合理範圍，拒收"

    trust = find(lambda l: l.startswith("投信"))
    dealers = [t for lab, t in table.items() if lab.replace(" ", "").startswith("自營商(") and None not in t]
    total = find(lambda l: l == "合計")
    row = {
        "as_of": as_of, "foreign_buy": f_buy, "foreign_sell": f_sell, "foreign_net": f_net,
        "trust_net": trust[2] if trust and None not in trust else None,
        "dealer_net": sum(t[2] for t in dealers) if dealers else None,
        "total_net": total[2] if total and None not in total else None,
        "source": "TWSE BFI82U",
    }
    return row, note, None


def twse_institutional():
    """三大法人買賣金額（現貨），抓最新一天。"""
    got = _twse_fetch_day(config.TWSE_BFI82U, "twse:institutional")
    if got is None:
        return None
    data, as_of = got
    row, note, err = _parse_institutional(data, as_of)
    if err:
        log("twse:institutional", "failure", as_of=as_of, detail=err)
        return None
    log("twse:institutional", "success", as_of=as_of, detail=f"外資買賣超 {row['foreign_net'] / 1e8:+.2f} 億元（現貨）{note}")
    return [row]


def tw_institutional_backfill(max_days=None):
    """模組 M12：回補外資買賣超的歷史（給門檻的近一年分布用）。
    要補哪幾天：以自建行情（tw_hist_days）裡確認過的上市交易日為準，扣掉資料庫已經有的；每次最多 INST_BATCH_DAYS 天，間隔 HIST_SLEEP 秒。
    每一天都要驗證：證交所回傳的日期必須就是我們要的那一天（否則可能是把『最新一天』當成歷史），數字用和每日一樣的驗證。
    連線失敗就整批停止（不繼續打請求）；某天沒有資料或驗證不過，只略過那一天。回傳要寫入的列，由 persist_extra 寫入。"""
    max_days = max_days or config.INST_BATCH_DAYS
    if not sb_ready():
        log("inst_backfill", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=config.HIST_WINDOW_DAYS)
    days, err = sb_request("GET", f"tw_hist_days?select=as_of&market=eq.TWSE&status=eq.ok&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000")
    if days is None:
        log("inst_backfill", "failure", detail=_clip(f"讀取交易日清單失敗：{err}"))
        return []
    have_rows, err = sb_request("GET", f"tw_institutional?select=as_of&source=eq.TWSE%20BFI82U&as_of=gte.{start.isoformat()}&limit=2000")
    have = {r["as_of"] for r in (have_rows or [])}
    todo_all = [r["as_of"] for r in days if r["as_of"] not in have]
    todo = todo_all[:max_days]
    if not todo:
        log("inst_backfill", "no_new_data", detail=f"近 {config.HIST_WINDOW_DAYS} 天的 {len(days)} 個交易日，外資買賣超都已經補齊")
        return []
    out, skipped, bad, first_err = [], 0, 0, ""
    for d in todo:
        data, err = fetch_json(config.TWSE_BFI82U + "&dayDate=" + d.replace("-", ""))
        time.sleep(config.HIST_SLEEP)
        if data is None:
            log("inst_backfill", "failure", as_of=d, detail=_clip(f"抓取失敗：{err}；已停止本次回補，不繼續打請求"))
            break
        if not isinstance(data, dict) or data.get("stat") != "OK":
            skipped += 1
            continue
        as_of = twse_ymd_to_iso(data.get("date"))
        if as_of != d:
            bad += 1
            first_err = first_err or f"要 {d} 但證交所回傳 {as_of}"
            continue
        row, _note, perr = _parse_institutional(data, as_of)
        if perr:
            bad += 1
            first_err = first_err or f"{d}：{perr}"
            continue
        out.append(row)
    log("inst_backfill", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次補 {len(out)} 天" + (f"（{out[-1]['as_of']} 到 {out[0]['as_of']}）" if out else "") +
                     f"；略過 {skipped} 天（證交所沒有資料）、{bad} 天（日期或數字驗證不過" + (f"：{first_err}" if first_err else "") + f"）；還有 {max(len(todo_all) - len(todo), 0)} 天下次繼續"))
    return out


def tw_breadth_hist_compute():
    """模組 M13：呼叫資料庫函式 compute_tw_breadth_hist，用自建行情算每天的上漲、下跌、平盤家數（4 位數代號的股票，沒有調整除權息）。
    第一次（資料表是空的）從回補範圍的起點開始，每 30 天一段分段重算（一次算全部可能被資料庫的逾時上限砍掉）；之後每天只重算最近 20 天。"""
    if not sb_ready():
        log("tw_breadth_hist", "skipped", detail="未設定 Supabase，不計算")
        return []
    first, err = sb_request("GET", "tw_breadth_hist?select=as_of&order=as_of.asc&limit=1")
    if first is None:
        log("tw_breadth_hist", "failure", detail=_clip(f"讀取失敗：{err}（請先執行 new_tables_v38.sql）"))
        return []
    today = date.today()
    if first:
        windows = [((today - timedelta(days=20)).isoformat(), None)]
    else:
        windows, a = [], today - timedelta(days=config.HIST_WINDOW_DAYS)
        while a <= today:
            b = min(a + timedelta(days=29), today)
            windows.append((a.isoformat(), b.isoformat()))
            a = b + timedelta(days=1)
    total = 0
    for p_from, p_to in windows:
        res, err = sb_request("POST", "rpc/compute_tw_breadth_hist", {"p_from": p_from, "p_to": p_to})
        if res is None:
            log("tw_breadth_hist", "failure", as_of=p_from, detail=_clip(f"呼叫 compute_tw_breadth_hist 失敗（{p_from} 起）：{err}（請先執行 new_tables_v38.sql；已算 {total} 個，下次繼續）"))
            return [{"computed": total}] if total else []
        try:
            total += int(res if not isinstance(res, list) else res[0])
        except (TypeError, ValueError, IndexError):
            pass
    log("tw_breadth_hist", "success" if total else "no_new_data", detail=f"{'全部重算（分 ' + str(len(windows)) + ' 段）' if not first else '重算最近 20 天'}：{total} 個（日期×市場）")
    return [{"computed": total}] if total else []


def twse_margin():
    """信用交易統計：融資金額餘額（仟元換算成元）。前日餘額＋買進－賣出－償還 必須等於今日餘額。"""
    got = _twse_fetch_day(config.TWSE_MI_MARGN, "twse:margin")
    if got is None:
        return None
    data, as_of = got
    need = ("項目", "買進", "賣出", "現金(券)償還", "前日餘額", "今日餘額")
    table = next((t for t in (data.get("tables") or []) if all(k in (t.get("fields") or []) for k in need)), None)
    if table is None:
        log("twse:margin", "failure", as_of=as_of, detail="找不到『信用交易統計』表格或欄位不符")
        return None
    fields = table["fields"]
    ix = {k: fields.index(k) for k in need}
    rows = {}
    for r in table.get("data") or []:
        try:
            rows[str(r[ix["項目"]]).strip()] = {k: parse_number(r[ix[k]]) for k in need[1:]}
        except (IndexError, TypeError):
            continue
    amt = rows.get("融資金額(仟元)")
    if not amt or None in amt.values():
        log("twse:margin", "failure", as_of=as_of, detail="找不到『融資金額(仟元)』列或數字無法解析")
        return None
    if abs(amt["前日餘額"] + amt["買進"] - amt["賣出"] - amt["現金(券)償還"] - amt["今日餘額"]) > 1:
        log("twse:margin", "failure", as_of=as_of, detail="前日餘額＋買進－賣出－償還 與今日餘額對不上，拒收")
        return None
    balance, prev = amt["今日餘額"] * 1000, amt["前日餘額"] * 1000
    lo, hi = config.MARGIN_BALANCE_RANGE
    if not (lo <= balance <= hi):
        log("twse:margin", "failure", as_of=as_of, detail=f"融資餘額 {balance} 超出合理範圍，拒收")
        return None
    units = (rows.get("融資(交易單位)") or {}).get("今日餘額")
    short = (rows.get("融券(交易單位)") or {}).get("今日餘額")
    log("twse:margin", "success", as_of=as_of,
        detail=f"融資餘額 {balance / 1e8:,.0f} 億元，較前日 {(balance - prev) / 1e8:+.1f} 億元")
    return [{"as_of": as_of, "margin_balance": balance, "margin_prev_balance": prev,
             "margin_units": units, "short_units": short, "source": "TWSE MI_MARGN"}]


def _norm_ymd(text):
    """'2026/10/29'、'2026-10-29'、'20261029' -> '20261029'；其他 -> None。"""
    t = re.sub(r"[/\-\s]", "", str(text or ""))
    return t if re.fullmatch(r"\d{8}", t) else None


_CSV_MAP = {  # 期交所 CSV 的中文欄位 -> 與 JSON 相同的英文鍵
    "日期": "Date", "商品名稱": "ContractCode", "身份別": "Item",
    "多方未平倉口數": "OpenInterest(Long)", "空方未平倉口數": "OpenInterest(Short)",
    "多空未平倉口數淨額": "OpenInterest(Net)", "多空未平倉契約金額淨額(千元)": "ContractValueofOpenInterest(Net)(Thousands)",
}


def _taifex_investor_csv(url):
    """讀「三大法人-區分各期貨契約」的 CSV 版本。回傳 (資料列清單, 錯誤)；欄位對不上就失敗，不猜。"""
    status, ctype, body, err = fetch_raw(url)
    if status != 200 or not body:
        return None, f"CSV 抓取失敗 HTTP {status} {err}"
    try:
        text = body.decode("utf-8-sig").lstrip("\ufeff")
    except UnicodeDecodeError:
        return None, "CSV 不是 UTF-8"
    import csv
    import io
    reader = csv.DictReader(io.StringIO(text))
    heads = [h.strip() for h in (reader.fieldnames or [])]
    missing = [k for k in _CSV_MAP if k not in heads]
    if missing:
        return None, f"CSV 欄位不符，缺 {missing[:3]}"
    rows = []
    for r in reader:
        r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        row = {en: r.get(zh, "") for zh, en in _CSV_MAP.items()}
        row["Date"] = _norm_ymd(row["Date"]) or row["Date"]
        rows.append(row)
    return (rows, "") if rows else (None, "CSV 沒有資料列")


def taifex_basis(tw_rows):
    """台指期基差 = 近月臺股期貨（一般時段）最後成交價 － 同一天加權指數收盤。單位：點。
    日期必須是同一天，否則不算；不使用盤後時段；排除價差合約。"""
    rows, err = fetch_json(config.TAIFEX_DAILY_URL)
    if rows is None:
        log("taifex:basis", "failure", detail=_clip(f"抓取失敗: {err}"))
        return None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("taifex:basis", "failure", detail="回傳不是預期的資料列清單")
        return None
    need = ("Date", "Contract", "ContractMonth(Week)", "Last", "SettlementPrice", "TradingSession")
    if any(k not in rows[0] for k in need):
        log("taifex:basis", "failure", detail=_clip(f"欄位與預期不符: {list(rows[0].keys())[:12]}"))
        return None
    dates = [twse_ymd_to_iso(r.get("Date")) for r in rows if isinstance(r, dict)]
    dates = [d for d in dates if d]
    if not dates:
        log("taifex:basis", "failure", detail="找不到有效日期")
        return None
    fut_date = max(dates)
    ok, why = validate_close_date(fut_date)
    if not ok:
        log("taifex:basis", "failure", as_of=fut_date, detail=f"期貨資料日期異常: {why}")
        return None
    cands = [r for r in rows if isinstance(r, dict) and twse_ymd_to_iso(r.get("Date")) == fut_date
             and r.get("Contract") == config.TAIFEX_DAILY_CONTRACT and r.get("TradingSession") == "一般"
             and re.fullmatch(r"\d{6}", str(r.get("ContractMonth(Week)", "")).strip())]
    if not cands:
        log("taifex:basis", "failure", as_of=fut_date, detail="找不到臺股期貨（TX）一般時段的單一月份合約")
        return None
    near = min(cands, key=lambda r: str(r["ContractMonth(Week)"]).strip())
    month = str(near["ContractMonth(Week)"]).strip()
    if month < fut_date[:4] + fut_date[5:7]:
        log("taifex:basis", "failure", as_of=fut_date, detail=f"近月合約 {month} 早於資料月份，疑似過期，拒收")
        return None
    last = parse_number(near.get("Last"))
    if last is None or not (config.TAIEX_RANGE[0] <= last <= config.TAIEX_RANGE[1]):
        log("taifex:basis", "no_new_data", as_of=fut_date, detail=f"近月 {month} 一般時段沒有可用的最後成交價（{near.get('Last')!r}）")
        return None
    idx = next((r for r in (tw_rows or []) if r.get("as_of") == fut_date), None)
    if idx is None or not idx.get("close"):
        have = max((r.get("as_of") for r in (tw_rows or [])), default=None)
        log("taifex:basis", "no_new_data", as_of=fut_date,
            detail=f"找不到同一天（{fut_date}）的加權指數收盤（指數最新資料日 {have}），日期對不上不計算")
        return None
    index_close = float(idx["close"])
    basis = last - index_close
    pct = basis / index_close * 100
    if abs(pct) > config.BASIS_LIMIT_PCT:
        log("taifex:basis", "failure", as_of=fut_date, detail=f"基差 {pct:.2f}% 超出合理範圍 {config.BASIS_LIMIT_PCT}%，拒收")
        return None
    settle = parse_number(near.get("SettlementPrice"))
    log("taifex:basis", "success", as_of=fut_date,
        detail=f"近月 {month} 一般時段最後成交價 {last:,.0f}，加權指數 {index_close:,.2f}，基差 {basis:+,.2f} 點（{pct:+.2f}%）")
    return [{"as_of": fut_date, "contract_month": month, "futures_last": last, "settlement_price": settle,
             "index_close": index_close, "basis": round(basis, 4), "basis_pct": round(pct, 4),
             "source": "TAIFEX DailyMarketReportFut + TWSE FMTQIK"}]


def taifex_futures():
    """期交所三大法人「臺股期貨」未平倉（口數）。每個身份別一列（自營商、投信、外資…）。
    外資列以身份別名稱開頭為「外資」辨識；找不到就失敗，不猜。
    多方－空方 必須等於淨額，對不上就拒收。"""
    rows, err = fetch_json(config.TAIFEX_FUTURES_URL)
    if rows is None:
        # 期交所有時不回 JSON 而回 CSV（開頭帶 BOM）。改讀 CSV，欄位與驗證規則完全相同。
        rows, err2 = _taifex_investor_csv(config.TAIFEX_FUTURES_URL)
        if rows is None:
            log("taifex:futures", "failure", detail=_clip(f"抓取失敗: JSON：{err}｜CSV：{err2}"))
            return None
        log("taifex:futures_format", "success", detail="這次期交所回傳 CSV，已改用 CSV 解析（驗證規則不變）")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("taifex:futures", "failure", detail="回傳不是預期的資料列清單")
        return None
    need = ("Date", "ContractCode", "Item", "OpenInterest(Long)", "OpenInterest(Short)", "OpenInterest(Net)")
    if any(k not in rows[0] for k in need):
        log("taifex:futures", "failure", detail=f"欄位與預期不符: {list(rows[0].keys())[:12]}")
        return None

    dated = [(twse_ymd_to_iso(r.get("Date")), r) for r in rows if isinstance(r, dict)]
    dates = [d for d, _ in dated if d]
    if not dates:
        log("taifex:futures", "failure", detail="找不到有效日期")
        return None
    as_of = max(dates)
    ok, why = validate_close_date(as_of)
    if not ok:
        log("taifex:futures", "failure", as_of=as_of, detail=f"資料日期異常: {why}")
        return None

    picked, rejected, investors = [], 0, []
    for d, r in dated:
        if d != as_of or r.get("ContractCode") != config.TAIFEX_CONTRACT:
            continue
        investor = str(r.get("Item", "")).strip()
        investors.append(investor)
        lng = parse_number(r.get("OpenInterest(Long)"))
        sht = parse_number(r.get("OpenInterest(Short)"))
        net = parse_number(r.get("OpenInterest(Net)"))
        if (None in (lng, sht, net) or lng < 0 or sht < 0 or abs(net - (lng - sht)) > 0
                or abs(net) > config.TAIFEX_NET_LIMIT):
            rejected += 1
            continue
        picked.append({
            "as_of": as_of, "contract": config.TAIFEX_CONTRACT, "investor": investor,
            "oi_long": lng, "oi_short": sht, "oi_net": net,
            "oi_net_value_thousand": parse_number(r.get("ContractValueofOpenInterest(Net)(Thousands)")),
            "source": "TAIFEX 三大法人-區分各期貨契約",
        })
    if rejected:
        log("taifex:futures", "failure", as_of=as_of, detail=f"{rejected} 列驗證不過（多空與淨額對不上或數字異常），已拒收")
    foreign = [p for p in picked if p["investor"].startswith("外資")]
    if not foreign:
        log("taifex:futures", "failure", as_of=as_of,
            detail=f"找不到外資列；{config.TAIFEX_CONTRACT} 現有身份別：{investors}")
        return None
    f = foreign[0]
    log("taifex:futures", "success", as_of=as_of,
        detail=f"{config.TAIFEX_CONTRACT} {f['investor']} 淨未平倉 {f['oi_net']:+,.0f} 口（多{f['oi_long']:,.0f}／空{f['oi_short']:,.0f}）")
    return picked


# ---------------------------------------------------------------- 探測（只讀）
def _clip(text, n=900):
    text = str(text)
    return text if len(text) <= n else text[: n - 1] + "…"


def probe_twse():
    """只讀探測：找出證交所『漲跌家數』的端點與欄位格式，結果寫進紀錄供人工確認。
    不寫任何市場資料表，不把探測結果當成正式數字。"""
    data, err = fetch_json(config.TWSE_SWAGGER)
    if data is None:
        log("probe:swagger", "failure", detail=f"抓取失敗: {err}")
    else:
        paths = data.get("paths") or {}
        hits = []
        for path, methods in paths.items():
            for m in (methods or {}).values():
                if not isinstance(m, dict):
                    continue
                text = f"{m.get('summary', '')} {m.get('description', '')}"
                if any(k in text for k in ("漲跌", "上漲", "漲停")):
                    hits.append(f"{path} | {m.get('summary', '')}")
        log("probe:swagger", "success",
            detail=_clip(f"共 {len(paths)} 個端點；含『漲跌』字樣的：" + (" ; ".join(hits) if hits else "無")))

    data, err = fetch_json(config.TWSE_MI_INDEX_MS)
    if data is None:
        log("probe:MI_INDEX_MS", "failure", detail=f"抓取失敗: {err}")
        return
    if data.get("stat") != "OK":
        log("probe:MI_INDEX_MS", "no_new_data", detail=f"stat={data.get('stat')}")
        return
    tables = data.get("tables") or []
    if not tables:
        log("probe:MI_INDEX_MS", "failure", detail=_clip(f"沒有 tables 欄位；頂層鍵：{list(data.keys())}"))
        return
    outline = [f"{t.get('title')}｜欄位{t.get('fields')}｜{len(t.get('data') or [])}列" for t in tables]
    log("probe:MI_INDEX_MS", "success", detail=_clip(f"日期{data.get('date')}；表格：" + " ;; ".join(outline)))
    for t in tables:
        if "漲跌" in str(t.get("title")):
            log("probe:breadth_table", "success",
                detail=_clip(f"{t.get('title')}｜{t.get('fields')}｜{(t.get('data') or [])[:8]}"))


def probe_extra():
    """只讀探測：外資買賣超(BFI82U)、融資餘額(MI_MARGN)、期交所外資期貨部位的真實格式。
    只把「結構與前幾列範例」寫進紀錄，不寫任何市場資料表。"""
    for name, url in config.PROBE_URLS.items():
        data, err = fetch_json(url)
        if data is None:
            log(f"probe:{name}", "failure", detail=f"抓取失敗: {err}")
            continue
        if not isinstance(data, dict):
            log(f"probe:{name}", "failure", detail=_clip(f"非預期格式: {type(data).__name__}"))
            continue
        if data.get("stat") != "OK":
            log(f"probe:{name}", "no_new_data", detail=f"stat={data.get('stat')}")
            continue
        tables = data.get("tables")
        if tables:
            for i, t in enumerate(tables):
                log(f"probe:{name}:t{i}", "success",
                    detail=_clip(f"日期{data.get('date')}｜{t.get('title')}｜欄位{t.get('fields')}｜{(t.get('data') or [])[:6]}"))
        else:
            log(f"probe:{name}", "success",
                detail=_clip(f"日期{data.get('date')}｜{data.get('title')}｜欄位{data.get('fields')}｜{(data.get('data') or [])[:7]}｜頂層鍵{list(data.keys())}"))

    data, err = fetch_json(config.TAIFEX_SWAGGER)
    if data is None:
        log("probe:taifex_swagger", "failure", detail=f"抓取失敗: {err}")
        return
    paths = data.get("paths") or {}
    hits = []
    for path, methods in paths.items():
        for m in (methods or {}).values():
            if isinstance(m, dict):
                text = f"{m.get('summary', '')} {m.get('description', '')}"
                if "三大法人" in text or "未平倉" in text:
                    hits.append(f"{path} | {m.get('summary', '')}")
    log("probe:taifex_swagger", "success",
        detail=_clip(f"共 {len(paths)} 個端點；含『三大法人／未平倉』：" + (" ; ".join(hits) if hits else "無")))

    # 找「區分各期貨契約-依日期」端點，抓一次，只記錄結構與各欄位出現的值
    target = None
    for path, methods in paths.items():
        for m in (methods or {}).values():
            if isinstance(m, dict):
                text = f"{m.get('summary', '')} {m.get('description', '')}"
                if "區分各期貨契約" in text and "依日期" in text:
                    target = path
    if target is None:
        log("probe:taifex_futures", "no_new_data", detail="swagger 中找不到『區分各期貨契約-依日期』端點")
        return
    servers = data.get("servers") or []
    if servers and isinstance(servers[0], dict) and servers[0].get("url"):
        base = servers[0]["url"]
    elif data.get("basePath"):
        base = "https://openapi.taifex.com.tw" + data["basePath"]
    else:
        base = "https://openapi.taifex.com.tw/v1"
    url = base.rstrip("/") + target
    rows, err = fetch_json(url)
    if rows is None:
        log("probe:taifex_futures", "failure", detail=_clip(f"{url} 抓取失敗: {err}"))
        status, ctype, body, err2 = fetch_raw(url)
        text = body[:300].decode("utf-8", errors="replace")
        log("probe:taifex_futures_raw", "success",
            detail=_clip(f"HTTP {status}｜Content-Type {ctype}｜總長度 {len(body)} 位元組｜開頭 {text!r}｜{err2}"))
        return
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("probe:taifex_futures", "failure", detail=_clip(f"{url} 回傳非預期格式: {str(rows)[:300]}"))
        return
    log("probe:taifex_futures_rows", "success", detail=_clip(f"{url}｜共{len(rows)}列｜前2列{rows[:2]}"))
    seen = []
    for key in rows[0]:
        vals = sorted({str(r.get(key)) for r in rows if isinstance(r, dict)})
        if 1 < len(vals) <= 15:
            seen.append(f"{key}={vals}")
    log("probe:taifex_futures_keys", "success", detail=_clip("；".join(seen) if seen else "沒有取值較少的欄位"))


def _find_keys(obj, needles, path="", out=None, depth=0):
    """在 JSON 裡找鍵名含關鍵字的欄位（例如 advanc / declin），回傳 [(路徑, 值)]。"""
    out = [] if out is None else out
    if depth > 6 or len(out) >= 12:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            here = f"{path}.{k}" if path else str(k)
            if any(n in str(k).lower() for n in needles) and not isinstance(v, (dict, list)):
                out.append((here, v))
            _find_keys(v, needles, here, out, depth + 1)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:5]):
            _find_keys(v, needles, f"{path}[{i}]", out, depth + 1)
    return out


def probe_nasdaq():
    """只讀探測：Nasdaq 官方網站的公開介面有沒有漲跌家數。只記錄結構，不寫任何資料表。
    用瀏覽器標頭、短逾時、不重試（雲端機器常被這類網站擋，快速失敗比卡住好）。"""
    headers = {"User-Agent": config.NASDAQ_UA, "Accept": "application/json, text/plain, */*",
               "Accept-Language": "en-US,en;q=0.9", "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}
    for tag, url in config.NASDAQ_PROBE_URLS.items():
        source = f"probe:nasdaq:{tag}"
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=headers)
        if status is None:
            log(source, "failure", detail=_clip(f"連線失敗（可能被擋或逾時 {config.PROBE_TIMEOUT} 秒）: {err}"))
            continue
        head = f"HTTP {status}｜{ctype}｜{len(body)} 位元組"
        try:
            data = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            log(source, "failure", detail=_clip(f"{head}｜不是 JSON，開頭：{body[:200].decode('utf-8', errors='replace')!r}"))
            continue
        if status != 200:
            log(source, "failure", detail=_clip(f"{head}｜{str(data)[:200]}"))
            continue
        hits = _find_keys(data, ("advanc", "declin", "unchang"))
        keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
        inner = data.get("data") if isinstance(data, dict) else None
        inner_keys = list(inner.keys())[:15] if isinstance(inner, dict) else (type(inner).__name__ if inner is not None else "無 data")
        log(source, "success", detail=_clip(f"{head}｜頂層鍵 {keys}｜data 鍵 {inner_keys}｜含漲跌家數字樣的欄位：" +
                                           (" ; ".join(f"{p}={v}" for p, v in hits) if hits else "沒有")))


def _pct_to_float(text):
    """'1.23%' -> 1.23；'UNCH' -> 0.0；無法解析 -> None（只用於診斷統計）。"""
    t = str(text).strip().replace("%", "").replace(",", "")
    if t.upper() in ("UNCH", "UNCHANGED"):
        return 0.0
    try:
        return float(t)
    except ValueError:
        return None


def probe_nasdaq_screener():
    """只讀探測：Nasdaq 股票清單下載介面的結構、日期欄位，以及用漲跌幅自算的家數（僅診斷，不是正式數字）。"""
    headers = {"User-Agent": config.NASDAQ_UA, "Accept": "application/json, text/plain, */*",
               "Accept-Language": "en-US,en;q=0.9", "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}
    for tag, url in config.NASDAQ_SCREENER_URLS.items():
        source = f"probe:nasdaq_screener:{tag}"
        status, ctype, body, err = fetch_raw(url, timeout=config.SCREENER_TIMEOUT, headers=headers)
        if status is None:
            log(source, "failure", detail=_clip(f"連線失敗（可能被擋或逾時 {config.SCREENER_TIMEOUT} 秒）: {err}"))
            continue
        head = f"HTTP {status}｜{ctype}｜{len(body)} 位元組"
        try:
            data = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            log(source, "failure", detail=_clip(f"{head}｜不是 JSON，開頭：{body[:200].decode('utf-8', errors='replace')!r}"))
            continue
        inner = data.get("data") if isinstance(data, dict) else None
        rows = None
        if isinstance(inner, dict):
            rows = inner.get("rows")
            if rows is None and isinstance(inner.get("table"), dict):
                rows = inner["table"].get("rows")
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            log(source, "failure", detail=_clip(f"{head}｜找不到資料列｜頂層鍵 {list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
                                               f"｜data 鍵 {list(inner.keys())[:12] if isinstance(inner, dict) else inner!r}"))
            continue
        pcts = [_pct_to_float(r.get("pctchange")) for r in rows]
        adv, dec = sum(1 for x in pcts if x is not None and x > 0), sum(1 for x in pcts if x is not None and x < 0)
        unch, bad = sum(1 for x in pcts if x == 0), sum(1 for x in pcts if x is None)
        dates = _find_keys(data, ("asof", "date", "time"))
        log(source, "success", detail=_clip(
            f"{head}｜列數 {len(rows)}｜欄位 {list(rows[0].keys())}｜日期相關欄位："
            + (" ; ".join(f"{p}={v}" for p, v in dates) if dates else "沒有")
            + f"｜自算（僅診斷）上漲 {adv}／下跌 {dec}／持平 {unch}／無法解析 {bad}｜第1列 {dict(list(rows[0].items())[:6])}"))


def _taifex_base(swagger):
    """期交所說明文件裡的伺服器網址；沒有就用已驗證過的預設。"""
    servers = swagger.get("servers") or []
    if servers and isinstance(servers[0], dict) and servers[0].get("url"):
        return servers[0]["url"].rstrip("/")
    if swagger.get("basePath"):
        return "https://openapi.taifex.com.tw" + swagger["basePath"]
    return "https://openapi.taifex.com.tw/v1"


def probe_taifex_daily():
    """只讀探測：期交所「期貨每日交易行情」的端點與欄位，以及臺股期貨各月份的列長什麼樣。
    只記錄結構與前幾列，不寫任何資料表。"""
    sw, err = fetch_json(config.TAIFEX_SWAGGER)
    if sw is None:
        log("probe:taifex_daily_swagger", "failure", detail=f"抓取失敗: {err}")
        return
    paths = sw.get("paths") or {}
    cands = []
    for path, methods in paths.items():
        for m in (methods or {}).values():
            if isinstance(m, dict):
                text = f"{m.get('summary', '')} {m.get('description', '')}"
                if "期貨" in text and "行情" in text and "選擇權" not in text:
                    cands.append((path, m.get("summary", "")))
    log("probe:taifex_daily_swagger", "success",
        detail=_clip("含『期貨』與『行情』的端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:8]) if cands else "無")))
    if not cands:
        return
    pick = next((c for c in cands if "每日" in c[1]), cands[0])
    url = _taifex_base(sw) + pick[0]
    rows, err = fetch_json(url)
    if rows is None:
        log("probe:taifex_daily_rows", "failure", detail=_clip(f"{url} 抓取失敗: {err}"))
        return
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("probe:taifex_daily_rows", "failure", detail=_clip(f"{url} 回傳非預期格式: {str(rows)[:200]}"))
        return
    log("probe:taifex_daily_rows", "success", detail=_clip(f"{url}｜共 {len(rows)} 列｜欄位 {list(rows[0].keys())}"))
    seen = []
    for key in rows[0]:
        vals = sorted({str(r.get(key)) for r in rows if isinstance(r, dict)})
        if 1 < len(vals) <= 12:
            seen.append(f"{key}={vals}")
    log("probe:taifex_daily_values", "success", detail=_clip("；".join(seen) if seen else "沒有取值較少的欄位"))
    tx = [r for r in rows if isinstance(r, dict) and any(str(v) == config.TAIFEX_CONTRACT or str(v) == "TX" for v in r.values())]
    log("probe:taifex_daily_tx", "success", detail=_clip(f"臺股期貨相關列數 {len(tx)}｜前 4 列 {tx[:4]}"))


def load_holdings():
    """用服務金鑰讀你自己的持股清單（只取名稱、代號、種類、帳戶幣別，不讀股數與金額），決定要抓哪些股票的價格。
    回傳 (清單, 錯誤)。清單每項：{market: TW|US, symbol, name, kind, pos_key}。"""
    if not sb_ready():
        return None, "未設定 Supabase，無法讀取持股清單"
    accts, e1 = sb_request("GET", "private_accounts?select=user_id,account_key,currency&limit=50")
    pos, e2 = sb_request("GET", "private_positions?select=user_id,account_key,pos_key,symbol,name,kind&limit=1000")
    if accts is None or pos is None:
        return None, f"讀取失敗：{e1 or e2}"
    cur = {(a.get("user_id"), a.get("account_key")): (a.get("currency") or "").upper() for a in accts}
    seen, out = set(), []
    for p in pos:
        c = cur.get((p.get("user_id"), p.get("account_key")), "")
        market = "TW" if c == "TWD" else "US" if c == "USD" else None
        if market is None:
            continue
        key = (market, p.get("symbol") or p.get("name"))
        if key in seen:
            continue
        seen.add(key)
        out.append({"market": market, "symbol": p.get("symbol"), "name": p.get("name"), "kind": p.get("kind"), "pos_key": p.get("pos_key")})
    return out, ""


def _openapi_base(swagger, default):
    servers = swagger.get("servers") or []
    if servers and isinstance(servers[0], dict) and servers[0].get("url"):
        return servers[0]["url"].rstrip("/")
    if swagger.get("basePath"):
        return default.rsplit("/", 1)[0] + swagger["basePath"]
    return default


def probe_prices():
    """只讀探測：持股現價的三個來源。只記錄結構、資料日期與名稱比對結果，不寫任何資料表。"""
    hold, herr = load_holdings()
    if hold is None:
        log("probe:px_holdings", "failure", detail=_clip(herr))
        hold = []
    else:
        log("probe:px_holdings", "success", detail=f"台股 {sum(1 for h in hold if h['market'] == 'TW')} 檔、美股 {sum(1 for h in hold if h['market'] == 'US')} 檔")
    tw_names = [h["name"] for h in hold if h["market"] == "TW"]

    # 1) 證交所上市：先試 rwd 版本；失敗就記原始回應，再看 OpenAPI 有哪些「收盤」端點
    data, err = fetch_json(config.TWSE_STOCK_DAY_ALL)
    twse_rows = {}
    if data is None or not isinstance(data, dict):
        status, ctype, body, err2 = fetch_raw(config.TWSE_STOCK_DAY_ALL)
        log("probe:px_twse", "failure", detail=_clip(f"rwd 版抓取失敗: {err}｜原始回應 HTTP {status}｜{ctype}｜{len(body)} 位元組｜開頭 {body[:160].decode('utf-8', errors='replace')!r}"))
    else:
        fields = data.get("fields") or []
        rows = data.get("data") or []
        log("probe:px_twse", "success" if data.get("stat") == "OK" else "no_new_data",
            detail=_clip(f"stat={data.get('stat')}｜日期{data.get('date')}｜欄位{fields}｜共 {len(rows)} 列｜前2列 {rows[:2]}"))
        if "證券名稱" in fields:
            ni = fields.index("證券名稱")
            twse_rows = {str(r[ni]).strip(): r for r in rows if len(r) > ni}
    sw1, err = fetch_json(config.TWSE_OPENAPI_SWAGGER)
    if sw1 is None or not isinstance(sw1, dict):
        log("probe:px_twse_openapi", "failure", detail=_clip(f"說明文件抓取失敗: {err}"))
    else:
        cands1 = []
        for path, methods in (sw1.get("paths") or {}).items():
            for m in (methods or {}).values():
                text = f"{m.get('summary', '')}" if isinstance(m, dict) else ""
                if "收盤" in text and "指數" not in text and "大盤" not in text and ("個股" in text or "每日" in text or "股票" in text):
                    cands1.append((path, text))
        log("probe:px_twse_openapi", "success", detail=_clip("含『收盤』的個股端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands1[:10]) if cands1 else "無")))
        for path, text in cands1[:3]:
            rows1, err = fetch_json(config.TWSE_OPENAPI_BASE + path)
            tag = path.strip("/").split("/")[-1][:30]
            if rows1 is None or not isinstance(rows1, list) or not rows1 or not isinstance(rows1[0], dict):
                log(f"probe:px_twse_oa:{tag}", "failure", detail=_clip(f"抓取失敗或格式非預期: {err or str(rows1)[:150]}"))
                continue
            nk = next((k for k in rows1[0] if "Name" in k or "名稱" in k), None)
            if nk and not twse_rows:
                twse_rows = {str(r.get(nk)).strip(): r for r in rows1}
            log(f"probe:px_twse_oa:{tag}", "success", detail=_clip(f"共 {len(rows1)} 列｜欄位 {list(rows1[0].keys())}｜第1列 {rows1[0]}"))

    # 2) 櫃買中心上櫃行情
    sw, err = fetch_json(config.TPEX_SWAGGER)
    tpex_rows = {}
    if sw is None or not isinstance(sw, dict):
        log("probe:px_tpex_swagger", "failure", detail=_clip(f"抓取失敗: {err}"))
    else:
        cands = []
        for path, methods in (sw.get("paths") or {}).items():
            for m in (methods or {}).values():
                if isinstance(m, dict) and "上櫃股票" in f"{m.get('summary', '')} {m.get('description', '')}" and "行情" in f"{m.get('summary', '')}":
                    cands.append((path, m.get("summary", "")))
        log("probe:px_tpex_swagger", "success", detail=_clip("含『上櫃股票』與『行情』的端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:8]) if cands else "無")))
        pick = next((c for c in cands if "mainboard_daily_close" in c[0] or "收盤行情" in c[1]), cands[0] if cands else None)
        if pick:
            rows, err = fetch_json(_openapi_base(sw, config.TPEX_DEFAULT_BASE) + pick[0])
            if rows is None or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
                log("probe:px_tpex", "failure", detail=_clip(f"{pick[0]} 抓取失敗或格式非預期: {err or str(rows)[:150]}"))
            else:
                log("probe:px_tpex", "success", detail=_clip(f"{pick[0]}｜共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜第1列 {rows[0]}"))
                name_key = next((k for k in rows[0] if "Name" in k or "名稱" in k), None)
                if name_key:
                    tpex_rows = {str(r.get(name_key)).strip(): r for r in rows}

    # 3) 名稱比對（不做模糊比對）
    if tw_names:
        in_twse = [n for n in tw_names if n in twse_rows]
        in_tpex = [n for n in tw_names if n in tpex_rows and n not in twse_rows]
        none = [n for n in tw_names if n not in twse_rows and n not in tpex_rows]
        log("probe:px_tw_match", "success" if not none else "failure",
            detail=_clip(f"上市對上 {len(in_twse)} 檔、上櫃對上 {len(in_tpex)} 檔（{in_tpex}）、兩邊都對不上 {len(none)} 檔（{none}）"))

    # 4) Nasdaq 個股報價：看有沒有帶時間戳，以及是不是即時價
    headers = {"User-Agent": config.NASDAQ_UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
               "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}
    for h in [x for x in hold if x["market"] == "US"]:
        sym = (h["symbol"] or h["pos_key"] or "").upper()
        first = "etf" if h["kind"] in ("etf", "leveraged_etf") else "stocks"
        done = False
        for ac in (first, "stocks" if first == "etf" else "etf"):
            status, ctype, body, err = fetch_raw(config.NASDAQ_QUOTE_URL.format(symbol=sym, assetclass=ac), timeout=config.PROBE_TIMEOUT, headers=headers)
            src = f"probe:px_nasdaq:{sym}"
            if status is None:
                log(src, "failure", detail=_clip(f"連線失敗（可能被擋或逾時）: {err}"))
                done = True
                break
            try:
                j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
            except (ValueError, UnicodeDecodeError):
                log(src, "failure", detail=_clip(f"HTTP {status}｜不是 JSON：{body[:120].decode('utf-8', errors='replace')!r}"))
                done = True
                break
            d = j.get("data") if isinstance(j, dict) else None
            if isinstance(d, dict) and isinstance(d.get("primaryData"), dict):
                pdta = d["primaryData"]
                keep = {k: pdta.get(k) for k in ("lastSalePrice", "netChange", "percentageChange", "lastTradeTimestamp", "isRealTime", "volume") if k in pdta}
                sec = d.get("secondaryData")
                log(src, "success", detail=_clip(f"assetclass={ac}｜marketStatus={d.get('marketStatus')}｜primary={keep}｜secondary={'有' if sec else '無'}｜資料鍵 {list(d.keys())[:12]}"))
                done = True
                break
        if not done:
            log(f"probe:px_nasdaq:{sym}", "failure", detail="stocks 與 etf 兩種 assetclass 都沒有取得報價資料")


_US_MONTHS = {m: i + 1 for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}
NASDAQ_HEADERS = {"User-Agent": config.NASDAQ_UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
                  "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}


def parse_us_date(text):
    """'Oct 29, 2026'（後面可帶時間）-> '2026-10-29'；格式不符回傳 None。"""
    m = re.match(r"\s*([A-Za-z]{3})[A-Za-z]*\.?\s+(\d{1,2}),\s*(\d{4})", str(text or ""))
    if not m or m.group(1).title() not in _US_MONTHS:
        return None
    try:
        return date(int(m.group(3)), _US_MONTHS[m.group(1).title()], int(m.group(2))).isoformat()
    except ValueError:
        return None


def roc_to_iso(text):
    """民國年七碼 '1151029' -> '2026-10-29'；格式不符回傳 None。"""
    t = str(text or "").strip()
    if not re.fullmatch(r"\d{7}", t):
        return None
    try:
        return date(int(t[:3]) + 1911, int(t[3:5]), int(t[5:7])).isoformat()
    except ValueError:
        return None


def _price_row(market, code, name, as_of, close, change, source, high52=None, low52=None, range_source=None):
    prev = close - change if change is not None else None
    return {"market": market, "code": code, "name": name, "as_of": as_of, "close": close, "change": change,
            "change_pct": round(change / prev * 100, 4) if prev else None, "source": source,
            "high_52w": high52, "low_52w": low52, "range_source": range_source}


def tpex_prices(hold):
    """持股裡「上櫃」股票的官方收盤價（櫃買中心）。名稱必須完全相同；同名多筆優先取四位數代號，仍不唯一就不用。"""
    names = [h["name"] for h in hold if h["market"] == "TW"]
    if not names:
        return []
    rows, err = fetch_json(config.TPEX_QUOTES_URL)
    if rows is None or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("tpex:prices", "failure", detail=_clip(f"抓取失敗或格式非預期: {err or str(rows)[:120]}"))
        return None
    if any(k not in rows[0] for k in ("Date", "SecuritiesCompanyCode", "CompanyName", "Close", "Change")):
        log("tpex:prices", "failure", detail=_clip(f"欄位與預期不符: {list(rows[0].keys())[:10]}"))
        return None
    by_name = {}
    for r in rows:
        by_name.setdefault(str(r.get("CompanyName", "")).strip(), []).append(r)
    out, not_here, skipped = [], [], []
    for n in names:
        cand = by_name.get(n, [])
        if not cand:
            not_here.append(n)
            continue
        cand = [r for r in cand if re.fullmatch(r"\d{4}", str(r.get("SecuritiesCompanyCode", "")).strip())] or cand
        if len(cand) != 1:
            skipped.append(f"{n}（同名 {len(cand)} 筆）")
            continue
        r = cand[0]
        iso = roc_to_iso(r.get("Date"))
        ok, why = validate_close_date(iso)
        close = parse_number(r.get("Close"))
        if not ok or close is None or not (0 < close < config.PRICE_LIMIT):
            skipped.append(f"{n}（{why or '收盤價無效 ' + repr(r.get('Close'))}）")
            continue
        out.append(_price_row("TW", str(r["SecuritiesCompanyCode"]).strip(), n, iso, close, parse_number(r.get("Change")), "TPEX 上櫃股票行情"))
    dates = sorted({o["as_of"] for o in out})
    log("tpex:prices", "success" if (out or not skipped) else "failure", as_of=dates[-1] if dates else None,
        detail=_clip(f"上櫃對上 {len(out)} 檔；不在上櫃清單 {len(not_here)} 檔（上市股票，由證交所來源處理）；略過 {skipped}"))
    return out


def nasdaq_hist_range(sym, assetclass, close, price_date):
    """槓桿 ETF 沒有 keyStats，改用 Nasdaq 歷史行情（約一年）算 52 週最高最低。
    回傳 (低, 高, 說明)；任何一關沒過就回傳 (None, None, 原因)，不硬算：
    資料筆數與涵蓋天數要夠、最新一筆要跟收盤價同期、且收盤價不能出現分割式的跳動（沒調整的分割會讓區間完全錯誤）。"""
    today = date.today()
    url = config.NASDAQ_HIST_URL.format(symbol=sym, assetclass=assetclass, fromdate=(today - timedelta(days=380)).isoformat(), todate=today.isoformat())
    status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        return None, None, f"歷史行情連線失敗: {err}"
    try:
        rows = (((json.loads(body.decode("utf-8-sig").lstrip("\ufeff")) or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None, None, "歷史行情不是 JSON"
    pts = []
    for r in rows:
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", str(r.get("date", "")).strip())
        c = parse_number(str(r.get("close", "")).replace("$", ""))
        h = parse_number(str(r.get("high", "")).replace("$", ""))
        lo = parse_number(str(r.get("low", "")).replace("$", ""))
        if not m or None in (c, h, lo) or not (0 < lo <= c <= h < config.PRICE_LIMIT):
            continue
        try:
            pts.append((date(int(m.group(3)), int(m.group(1)), int(m.group(2))), c, h, lo))
        except ValueError:
            continue
    if not pts:
        return None, None, "歷史行情沒有可用的資料列"
    pts.sort()
    last_day = pts[-1][0]
    window = [p for p in pts if p[0] > last_day - timedelta(days=365)]
    if len(window) < config.RANGE52_MIN_ROWS or (window[-1][0] - window[0][0]).days < config.RANGE52_MIN_COVERAGE_DAYS:
        return None, None, f"歷史只有 {len(window)} 個交易日、涵蓋 {(window[-1][0] - window[0][0]).days} 天，不足以算 52 週"
    if (date.fromisoformat(price_date) - last_day).days > 4:
        return None, None, f"歷史行情最新到 {last_day}，和收盤價日期 {price_date} 差太多"
    for a, b in zip(window, window[1:]):
        ratio = b[1] / a[1]
        if ratio >= config.SPLIT_JUMP_UP or ratio <= config.SPLIT_JUMP_DOWN:
            return None, None, f"{b[0]} 收盤價單日變動 {ratio:.2f} 倍（{a[1]}→{b[1]}），疑似分割或反向分割沒有調整，區間不採用"
    hi, lo = max(p[2] for p in window), min(p[3] for p in window)
    if not (lo * 0.98 <= close <= hi * 1.02):
        return None, None, f"收盤價 {close} 不在歷史區間 {lo}–{hi} 內，不採用"
    return lo, hi, f"Nasdaq 歷史行情（{len(window)} 個交易日，已檢查沒有分割跳動）"


def nasdaq_prices(hold):
    """持股裡美股的收盤價（Nasdaq 個股報價）。只在 marketStatus=Closed 且 isRealTime=False 時採用，避免拿盤中或盤後價冒充收盤價。"""
    out = []
    for h in [x for x in hold if x["market"] == "US"]:
        sym = (h.get("symbol") or h.get("pos_key") or "").upper().strip()
        if not sym:
            continue
        first = "etf" if h.get("kind") in ("etf", "leveraged_etf") else "stocks"
        got = None
        for ac in (first, "stocks" if first == "etf" else "etf"):
            status, ctype, body, err = fetch_raw(config.NASDAQ_QUOTE_URL.format(symbol=sym, assetclass=ac), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
            if status is None:
                log(f"nasdaq:price:{sym}", "failure", detail=_clip(f"連線失敗: {err}"))
                break
            try:
                j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
            except (ValueError, UnicodeDecodeError):
                log(f"nasdaq:price:{sym}", "failure", detail=f"HTTP {status}，回傳不是 JSON")
                break
            d = j.get("data") if isinstance(j, dict) else None
            if isinstance(d, dict) and isinstance(d.get("primaryData"), dict):
                got = d
                break
        else:
            log(f"nasdaq:price:{sym}", "failure", detail="stocks 與 etf 兩種分類都查不到報價")
            continue
        if got is None:
            continue
        pd_ = got["primaryData"]
        ms = str(got.get("marketStatus"))
        if ms != "Closed" or pd_.get("isRealTime") is True:
            log(f"nasdaq:price:{sym}", "skipped", detail=f"marketStatus={ms}、isRealTime={pd_.get('isRealTime')}，不是已收盤的價格，不使用")
            continue
        iso = parse_us_date(pd_.get("lastTradeTimestamp"))
        ok, why = validate_close_date(iso)
        close = parse_number(str(pd_.get("lastSalePrice", "")).replace("$", ""))
        if not ok or close is None or not (0 < close < config.PRICE_LIMIT):
            log(f"nasdaq:price:{sym}", "failure", as_of=iso, detail=_clip(why or f"收盤價無效 {pd_.get('lastSalePrice')!r}"))
            continue
        change = parse_number(str(pd_.get("netChange", "")).replace("$", ""))
        low52, high52 = parse_range52((got.get("keyStats") or {}).get("fiftyTwoWeekHighLow", {}).get("value") if isinstance(got.get("keyStats"), dict) else None)
        note = ""
        if high52 is not None and not (low52 * 0.98 <= close <= high52 * 1.02):
            note = f"；收盤價 {close} 不在 52 週區間 {low52}–{high52} 內，區間可能過時，不採用"
            low52 = high52 = None
        range_src = "Nasdaq keyStats（與收盤價同次取得）" if high52 is not None else None
        if high52 is None and h.get("kind") in ("etf", "leveraged_etf"):
            lo_h, hi_h, why_h = nasdaq_hist_range(sym, ac, close, iso)          # 沿用剛剛報價成功的分類（etf 或 stocks）
            if lo_h is not None:
                low52, high52, range_src = lo_h, hi_h, why_h
            else:
                note += f"；52 週區間查不到：{why_h}"
            time.sleep(1)
        out.append(_price_row("US", sym, h.get("name"), iso, close, change, "Nasdaq 個股報價（已收盤）", high52, low52, range_src))
        log(f"nasdaq:price:{sym}", "success", as_of=iso,
            detail=f"收盤價 {close}（資料日 {iso}）；52 週區間 " + (f"{low52}–{high52}" if high52 is not None else "查不到") + note)
        time.sleep(1)
    return out


def twse_prices(hold, exclude_names=()):
    """持股裡「上市」股票的官方收盤價（證交所 OpenAPI）。名稱必須完全相同；同名多筆優先取四位數代號，仍不唯一就不用。
    exclude_names：已由其他來源（上櫃）取得價格的名稱，不重複查。這個來源沒有漲跌欄位，所以 change 留空。"""
    names = [h["name"] for h in hold if h["market"] == "TW" and h["name"] not in set(exclude_names)]
    if not names:
        return []
    rows, err = fetch_json(config.TWSE_AVG_URL)
    if rows is None or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("twse:prices", "failure", detail=_clip(f"抓取失敗或格式非預期: {err or str(rows)[:120]}"))
        return None
    if any(k not in rows[0] for k in ("Date", "Code", "Name", "ClosingPrice")):
        log("twse:prices", "failure", detail=_clip(f"欄位與預期不符: {list(rows[0].keys())[:10]}"))
        return None
    wanted = set(names)
    by_name = {}
    for r in rows:
        nm = str(r.get("Name", "")).strip()
        if nm in wanted:
            by_name.setdefault(nm, []).append(r)
    out, not_here, skipped = [], [], []
    for n in names:
        cand = by_name.get(n, [])
        if not cand:
            not_here.append(n)
            continue
        cand = [r for r in cand if re.fullmatch(r"\d{4}", str(r.get("Code", "")).strip())] or cand
        if len(cand) != 1:
            skipped.append(f"{n}（同名 {len(cand)} 筆）")
            continue
        r = cand[0]
        iso = roc_to_iso(r.get("Date"))
        ok, why = validate_close_date(iso)
        close = parse_number(r.get("ClosingPrice"))
        if not ok or close is None or not (0 < close < config.PRICE_LIMIT):
            skipped.append(f"{n}（{why or '收盤價無效 ' + repr(r.get('ClosingPrice'))}）")
            continue
        out.append(_price_row("TW", str(r["Code"]).strip(), n, iso, close, None, "TWSE 上市個股日收盤價"))
    dates = sorted({o["as_of"] for o in out})
    log("twse:prices", "success" if (out or not skipped) else "failure", as_of=dates[-1] if dates else None,
        detail=_clip(f"上市對上 {len(out)} 檔；對不上 {len(not_here)} 檔 {not_here}；略過 {skipped}"))
    return out


def _describe_json(obj):
    """把任意 JSON 的結構描述成一小段文字（只用於探測）。"""
    if isinstance(obj, dict):
        parts = []
        for k, v in list(obj.items())[:14]:
            if isinstance(v, list):
                parts.append(f"{k}=[{len(v)}列, 首列 {str(v[0])[:120] if v else '空'}]")
            elif isinstance(v, dict):
                parts.append(f"{k}={{{', '.join(list(v.keys())[:8])}}}")
            else:
                parts.append(f"{k}={str(v)[:40]}")
        return "；".join(parts)
    if isinstance(obj, list):
        return f"清單 {len(obj)} 列，首列 {str(obj[0])[:200] if obj else '空'}"
    return type(obj).__name__


def probe_history():
    """只讀探測（下一階段用）：
    1) Nasdaq 個股報價的 keyStats 有沒有 52 週高低；
    2) 證交所、櫃買中心『指定日期』的全部個股收盤行情（約一年前），能不能用來回補歷史。"""
    hold, herr = load_holdings()
    if hold is None:
        log("probe:hist_holdings", "failure", detail=_clip(herr))
        hold = []
    for h in [x for x in hold if x["market"] == "US"]:
        sym = (h.get("symbol") or h.get("pos_key") or "").upper().strip()
        first = "etf" if h.get("kind") in ("etf", "leveraged_etf") else "stocks"
        for ac in (first, "stocks" if first == "etf" else "etf"):
            status, ctype, body, err = fetch_raw(config.NASDAQ_QUOTE_URL.format(symbol=sym, assetclass=ac), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
            if status is None:
                log(f"probe:hist_nasdaq:{sym}", "failure", detail=_clip(f"連線失敗: {err}"))
                break
            try:
                d = (json.loads(body.decode("utf-8-sig").lstrip("\ufeff")) or {}).get("data")
            except (ValueError, UnicodeDecodeError, AttributeError):
                log(f"probe:hist_nasdaq:{sym}", "failure", detail="不是 JSON")
                break
            if isinstance(d, dict) and d.get("keyStats") is not None:
                log(f"probe:hist_nasdaq:{sym}", "success", detail=_clip(f"keyStats={d.get('keyStats')}"))
                break
        else:
            log(f"probe:hist_nasdaq:{sym}", "failure", detail="查不到 keyStats")
        time.sleep(1)

    day = date.today() - timedelta(days=365)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    ymd, slash, roc = day.strftime("%Y%m%d"), day.strftime("%Y/%m/%d").replace("/", "%2F"), f"{day.year - 1911}/{day.month:02d}/{day.day:02d}"

    data, err = fetch_json(config.TWSE_HIST_URL.format(ymd=ymd))
    if data is None or not isinstance(data, dict):
        status, ctype, body, err2 = fetch_raw(config.TWSE_HIST_URL.format(ymd=ymd))
        log("probe:hist_twse", "failure", detail=_clip(f"{ymd} 抓取失敗: {err}｜原始回應 HTTP {status}｜{ctype}｜{len(body)} 位元組｜開頭 {body[:160].decode('utf-8', errors='replace')!r}"))
    else:
        tables = data.get("tables") or []
        outline = [f"{t.get('title')}｜欄位{t.get('fields')}｜{len(t.get('data') or [])}列" for t in tables if t.get("data")]
        log("probe:hist_twse", "success" if data.get("stat") == "OK" else "no_new_data",
            detail=_clip(f"查詢日 {ymd}｜stat={data.get('stat')}｜回傳日期 {data.get('date')}｜" + (" ;; ".join(outline) if outline else f"沒有 tables；頂層鍵 {list(data.keys())}")))
        big = max((t for t in tables if t.get("data")), key=lambda t: len(t["data"]), default=None)
        if big:
            log("probe:hist_twse_rows", "success", detail=_clip(f"最大表格 {big.get('title')}｜前 2 列 {big['data'][:2]}"))
    time.sleep(2)

    for tag, tmpl in config.TPEX_HIST_URLS.items():
        url = tmpl.format(ymd=ymd, slash=slash, roc=roc)
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT)
        src = f"probe:hist_tpex:{tag}"
        if status is None:
            log(src, "failure", detail=_clip(f"連線失敗: {err}"))
            continue
        try:
            j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            log(src, "failure", detail=_clip(f"HTTP {status}｜{ctype}｜不是 JSON，開頭 {body[:140].decode('utf-8', errors='replace')!r}"))
            continue
        log(src, "success" if status == 200 else "failure", detail=_clip(f"查詢日 {ymd}｜HTTP {status}｜{_describe_json(j)}"))
        time.sleep(2)


def parse_range52(text):
    """'114.50 - 281.62' -> (低, 高)；格式不符或低於高不成立 -> (None, None)。"""
    m = re.fullmatch(r"\s*\$?([\d,]+(?:\.\d+)?)\s*-\s*\$?([\d,]+(?:\.\d+)?)\s*", str(text or ""))
    if not m:
        return None, None
    lo, hi = float(m.group(1).replace(",", "")), float(m.group(2).replace(",", ""))
    return (lo, hi) if 0 < lo <= hi < config.PRICE_LIMIT else (None, None)


def _hist_rows(as_of, items):
    """items: [(code, close, high, low)]。只收一般股票（四位數且不以 0 開頭）；數字不合理的列略過。"""
    rows, bad = [], 0
    for code, close, high, low in items:
        if not re.fullmatch(r"[1-9]\d{3}", str(code).strip()):      # 一般股票：1xxx～9xxx；0 開頭（0050 等）是 ETF，不納入
            continue
        if None in (close, high, low) or not (0 < low <= close <= high < config.PRICE_LIMIT):
            bad += 1
            continue
        rows.append({"code": str(code).strip(), "as_of": as_of, "close": close, "high": high, "low": low})
    return rows, bad


def hist_twse_day(d):
    """證交所（上市）指定日期全部個股行情。回傳 (列, 狀態)；狀態 ok / holiday（休市或無資料）/ error:原因。"""
    ymd = d.strftime("%Y%m%d")
    data, err = fetch_json(config.TWSE_HIST_URL.format(ymd=ymd))
    if data is None or not isinstance(data, dict):
        return None, f"error:抓取失敗 {err}"
    if data.get("stat") != "OK":
        return [], "holiday"
    if str(data.get("date")) != ymd:
        return None, f"error:日期對不上（要 {ymd}，回傳 {data.get('date')}）"
    table = next((t for t in (data.get("tables") or []) if "每日收盤行情" in str(t.get("title"))), None)
    if table is None:
        return None, "error:找不到每日收盤行情表"
    f = table.get("fields") or []
    need = ("證券代號", "最高價", "最低價", "收盤價")
    if any(n not in f for n in need):
        return None, f"error:欄位不符 {f[:8]}"
    ix = {n: f.index(n) for n in need}
    items = []
    for r in table.get("data") or []:
        try:
            items.append((r[ix["證券代號"]], parse_number(r[ix["收盤價"]]), parse_number(r[ix["最高價"]]), parse_number(r[ix["最低價"]])))
        except IndexError:
            continue
    rows, _bad = _hist_rows(d.isoformat(), items)
    for r in rows:
        r["market"] = "TWSE"
    return rows, "ok"


def hist_tpex_day(d):
    """櫃買中心（上櫃）指定日期全部個股行情。欄位以名稱辨識，對不上就失敗，不猜。"""
    slash = d.strftime("%Y/%m/%d").replace("/", "%2F")
    data, err = fetch_json(config.TPEX_DAY_URL.format(slash=slash))
    if data is None or not isinstance(data, dict):
        return None, f"error:抓取失敗 {err}"
    if str(data.get("stat", "")).lower() != "ok":
        return [], "holiday"
    def _fields(t):
        return [str(x).strip() for x in (t.get("fields") or [])]       # 櫃買中心的欄位名稱常帶前後空白，比對前先去掉
    table = next((t for t in (data.get("tables") or []) if "代號" in _fields(t) and "名稱" in _fields(t)), None)
    if table is None or not table.get("data"):
        return [], "holiday"
    td = str(table.get("date", "")).strip()
    try:
        y, m, dd = td.split("/")
        tdate = date(int(y) + 1911, int(m), int(dd))
    except ValueError:
        return None, f"error:表格日期格式不符 {td!r}"
    if tdate != d:
        return None, f"error:日期對不上（要 {d}，回傳 {tdate}）"
    f = _fields(table)
    def pick(*names):
        return next((f.index(n) for n in names if n in f), None)
    ic, ih, il, ik = pick("代號"), pick("最高", "最高價"), pick("最低", "最低價"), pick("收盤", "收盤價")
    if None in (ic, ih, il, ik):
        return None, f"error:欄位不符 {f[:10]}"
    items = []
    for r in table["data"]:
        try:
            items.append((r[ic], parse_number(r[ik]), parse_number(r[ih]), parse_number(r[il])))
        except IndexError:
            continue
    rows, _bad = _hist_rows(d.isoformat(), items)
    for r in rows:
        r["market"] = "TPEX"
    return rows, "ok"


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def tw_backfill(dry_run=False, max_days=None, window_days=None):
    """回補台股歷史行情（每個交易日兩次請求，間隔 HIST_SLEEP 秒）。已完成的日期記在 tw_hist_days，不重複抓。
    休市判斷：只有 2 天以前的日期才會被標成休市，避免當天資料還沒公布就被誤標。"""
    max_days = max_days or config.HIST_BATCH_DAYS
    window_days = window_days or config.HIST_WINDOW_DAYS
    if dry_run:
        max_days = min(max_days, 1)
    if not sb_ready():
        log("tw_backfill", "skipped", detail="未設定 Supabase，無法記錄進度，不回補")
        return {"days": 0, "rows": 0}
    today = date.today()
    start = today - timedelta(days=window_days)
    done_rows, err = sb_request("GET", f"tw_hist_days?select=market,as_of&as_of=gte.{start.isoformat()}&limit=5000")
    if done_rows is None:
        log("tw_backfill", "failure", detail=_clip(f"讀取回補進度失敗：{err}（請先建立 tw_hist、tw_hist_days 資料表）"))
        return None
    done = {(r["market"], r["as_of"]) for r in done_rows}
    total_rows = days = 0
    new_days = []
    d = today
    while d >= start and days < max_days:
        if d.weekday() < 5:
            todo = [(m, fn) for m, fn in (("TWSE", hist_twse_day), ("TPEX", hist_tpex_day)) if (m, d.isoformat()) not in done]
            if todo:
                for m, fn in todo:
                    rows, status = fn(d)
                    time.sleep(config.HIST_SLEEP)
                    if status.startswith("error:"):
                        log(f"hist:{m}", "failure", as_of=d.isoformat(), detail=_clip(status[6:] + "；已停止本次回補，不繼續打請求"))
                        return {"days": days, "rows": total_rows, "stopped": True, "new_days": new_days}
                    recent = (today - d).days <= 2
                    if status == "holiday" and recent:
                        log(f"hist:{m}", "no_new_data", as_of=d.isoformat(), detail="近 2 天內沒有資料（可能尚未公布），不標成休市")
                        continue
                    if dry_run:
                        log(f"hist:{m}", "skipped", as_of=d.isoformat(), detail=f"--dry-run：{status}，{len(rows)} 檔，不寫入")
                        continue
                    ok = True
                    for chunk in _chunks(rows, 1000):
                        ok, msg = sb_upsert("tw_hist", chunk, "market,code,as_of")
                        if not ok:
                            log(f"hist:{m}", "failure", as_of=d.isoformat(), detail=_clip(f"寫入失敗：{msg}"))
                            return None
                    ok, msg = sb_upsert("tw_hist_days", [{"market": m, "as_of": d.isoformat(), "status": status, "rows_count": len(rows)}], "market,as_of")
                    if not ok:
                        log(f"hist:{m}", "failure", as_of=d.isoformat(), detail=_clip(f"進度寫入失敗：{msg}"))
                        return None
                    total_rows += len(rows)
                    if status == "ok" and d.isoformat() not in new_days:
                        new_days.append(d.isoformat())
                days += 1
        d -= timedelta(days=1)
    log("tw_backfill", "success", detail=f"本次處理 {days} 個日期、寫入 {total_rows} 筆；回補範圍從 {start} 到今天，尚未完成的日期下次繼續")
    return {"days": days, "rows": total_rows, "new_days": new_days}


def tw_ranges(price_rows, as_of_today=None):
    """用自己回補的歷史，替台股持股算 52 週高低。歷史涵蓋不足就不算（高低維持空白，畫面顯示查不到）。"""
    today = as_of_today or date.today()
    for r in price_rows:
        if r.get("market") != "TW" or r.get("high_52w") is not None:
            continue
        since = (today - timedelta(days=365)).isoformat()
        hist, err = sb_request("GET", f"tw_hist?select=as_of,high,low&code=eq.{r['code']}&as_of=gte.{since}&order=as_of.asc&limit=1000")
        if not hist:
            continue
        span = (date.fromisoformat(hist[-1]["as_of"]) - date.fromisoformat(hist[0]["as_of"])).days
        if len(hist) < config.RANGE52_MIN_ROWS or span < config.RANGE52_MIN_COVERAGE_DAYS:
            log(f"tw_range52:{r['code']}", "no_new_data", detail=f"歷史只有 {len(hist)} 個交易日、涵蓋 {span} 天，不足以算 52 週高低")
            continue
        hi, lo = max(float(x["high"]) for x in hist), min(float(x["low"]) for x in hist)
        if not (lo * 0.98 <= float(r["close"]) <= hi * 1.02):
            log(f"tw_range52:{r['code']}", "failure", detail=f"收盤價 {r['close']} 不在歷史區間 {lo}–{hi} 內，不採用")
            continue
        r["high_52w"], r["low_52w"], r["range_source"] = hi, lo, f"自建歷史（{len(hist)} 個交易日）"
        log(f"tw_range52:{r['code']}", "success", as_of=hist[-1]["as_of"], detail=f"52 週區間 {lo}–{hi}（{len(hist)} 個交易日）")
    return price_rows


def tw_breadth_update(dry_run=False, max_days=10):
    """呼叫資料庫函式 compute_tw_breadth 算台股廣度指標。最新一個兩市場都完成的交易日一定重算；
    歷史涵蓋滿 330 天後，才補最近 60 天缺的日期（避免用不完整歷史算出一堆之後會變的舊列）。"""
    if not sb_ready():
        log("tw_breadth", "skipped", detail="未設定 Supabase，不計算廣度指標")
        return None
    days, err = sb_request("GET", "tw_hist_days?select=market,as_of&status=eq.ok&order=as_of.desc&limit=1000")
    if days is None:
        log("tw_breadth", "failure", detail=_clip(f"讀取回補進度失敗：{err}"))
        return None
    by_date = {}
    for r in days:
        by_date.setdefault(r["as_of"], set()).add(r["market"])
    complete = sorted((d for d, ms in by_date.items() if {"TWSE", "TPEX"} <= ms), reverse=True)
    if not complete:
        log("tw_breadth", "no_new_data", detail="還沒有上市與上櫃都完成回補的交易日")
        return None
    latest = complete[0]
    todo = [latest]
    cover = (date.fromisoformat(latest) - date.fromisoformat(complete[-1])).days
    if cover >= 330:
        have, err = sb_request("GET", f"tw_breadth_ma?select=as_of&as_of=gte.{(date.today() - timedelta(days=60)).isoformat()}&limit=1000")
        have_set = {r["as_of"] for r in (have or [])}
        todo += [d for d in complete if d != latest and (date.today() - date.fromisoformat(d)).days <= 60 and d not in have_set][:max_days]
    done = 0
    for d in todo:
        if dry_run:
            log("tw_breadth", "skipped", as_of=d, detail="--dry-run：不計算")
            continue
        res, err = sb_request("POST", "rpc/compute_tw_breadth", {"d": d})
        if res is None:
            log("tw_breadth", "failure", as_of=d, detail=_clip(f"呼叫 compute_tw_breadth 失敗：{err}（請先執行 new_tables_breadth.sql）"))
            return None
        done += 1
    if done:
        log("tw_breadth", "success", as_of=todo[0], detail=f"已計算 {done} 個日期（最新 {latest}，歷史涵蓋 {cover} 天）")
    return done


def _hist_day_rows(day_iso):
    """讀 tw_hist 某一天全部個股的收盤（每次最多 1000 列，所以分頁）。"""
    rows, off = [], 0
    while off <= 6000:
        part, err = sb_request("GET", f"tw_hist?select=market,code,close&as_of=eq.{day_iso}&order=market.asc,code.asc&limit=1000&offset={off}")
        if part is None:
            return None, err
        rows += part
        if len(part) < 1000:
            return rows, ""
        off += 1000
    return rows, ""


def _tw_names(candidates):
    """只替入選的個股查名稱（證交所、櫃買中心各最多一次請求）。查不到就回傳空字串，不猜。"""
    names = {}
    if any(c["market"] == "TWSE" for c in candidates):
        rows, _ = fetch_json(config.TWSE_AVG_URL)
        for r in (rows if isinstance(rows, list) else []):
            names[("TWSE", str(r.get("Code", "")).strip())] = str(r.get("Name", "")).strip()
    if any(c["market"] == "TPEX" for c in candidates):
        rows, _ = fetch_json(config.TPEX_QUOTES_URL)
        for r in (rows if isinstance(rows, list) else []):
            names[("TPEX", str(r.get("SecuritiesCompanyCode", "")).strip())] = str(r.get("CompanyName", "")).strip()
    return names


def _recent_complete_days(n):
    """資料庫裡上市與上櫃都完成的最近 n 個交易日（新到舊）。"""
    if not sb_ready():
        return []
    rows, _ = sb_request("GET", "tw_hist_days?select=market,as_of&status=eq.ok&order=as_of.desc&limit=400")
    by = {}
    for r in (rows or []):
        by.setdefault(r["as_of"], set()).add(r["market"])
    return sorted((d for d, m in by.items() if {"TWSE", "TPEX"} <= m), reverse=True)[:n]


def tw_crash_check(days, dry_run=False, threshold=None):
    """台股大跌檢視（第一步，自動加入）：單日收盤較前一個交易日跌 9% 以上，加入 crash_candidates，一律「待確認」。
    跌幅由收盤價換算；當天若除權息，跌幅可能是參考價調整造成，所以每筆都帶這個提醒。不做任何分類，也不碰你的人工分類。"""
    threshold = threshold or config.CRASH_DROP_PCT
    if not sb_ready():
        log("tw_crash", "skipped", detail="未設定 Supabase，不檢查")
        return None
    done, err = sb_request("GET", "tw_hist_days?select=market,as_of&status=eq.ok&order=as_of.desc&limit=1000")
    if done is None:
        log("tw_crash", "failure", detail=_clip(f"讀取回補進度失敗：{err}"))
        return None
    by = {}
    for r in done:
        by.setdefault(r["as_of"], set()).add(r["market"])
    complete = sorted((d for d, m in by.items() if {"TWSE", "TPEX"} <= m), reverse=True)
    found = []
    cutoff = (date.today() - timedelta(days=config.CRASH_MAX_AGE_DAYS)).isoformat()
    skipped_old = [d for d in set(days) if d < cutoff]
    if skipped_old:
        log("tw_crash", "no_new_data", detail=f"略過 {len(skipped_old)} 個超過 {config.CRASH_MAX_AGE_DAYS} 天的交易日（大跌檢視只看近期）")
    days = [d for d in days if d >= cutoff]
    for d in sorted(set(days)):
        if d not in complete:
            continue
        prev = next((x for x in complete if x < d), None)
        if prev is None or (date.fromisoformat(d) - date.fromisoformat(prev)).days > config.CRASH_PREV_MAX_GAP_DAYS:
            log("tw_crash", "no_new_data", as_of=d, detail="找不到足夠近的前一個交易日，不比較")
            continue
        cur, e1 = _hist_day_rows(d)
        pre, e2 = _hist_day_rows(prev)
        if cur is None or pre is None:
            log("tw_crash", "failure", as_of=d, detail=_clip(f"讀取歷史失敗：{e1 or e2}"))
            return None
        pm = {(r["market"], r["code"]): float(r["close"]) for r in pre if r.get("close")}
        for r in cur:
            p, c = pm.get((r["market"], r["code"])), float(r["close"]) if r.get("close") else None
            if not p or not c or p <= 0 or c <= 0:
                continue
            drop = round((c / p - 1) * 100, 4)           # 先四捨五入，避免剛好 9.0% 因浮點誤差被漏掉
            if drop <= -threshold:
                found.append({"as_of": d, "market": r["market"], "code": r["code"], "close": c, "prev_close": p, "drop_pct": round(drop, 2)})
    if not found:
        log("tw_crash", "success", detail=f"檢查 {len(set(days))} 個交易日，沒有單日跌幅達 {threshold}% 的個股")
        return []
    names = _tw_names(found)
    for f in found:
        f["name"] = names.get((f["market"], f["code"]), "")
        f["note"] = "依收盤價換算；若當天除權息，跌幅可能是參考價調整造成，原因待確認"
        f["source"] = "自建歷史（證交所、櫃買中心）"
    if dry_run:
        log("tw_crash", "skipped", detail=f"--dry-run：找到 {len(found)} 檔，不寫入")
        return found
    ok, msg = sb_upsert("crash_candidates", found, "as_of,market,code")
    log("tw_crash", "success" if ok else "failure",
        detail=_clip(f"找到 {len(found)} 檔單日跌幅達 {threshold}%（{', '.join(f['code'] for f in found[:8])}）；{msg}"))
    return found if ok else None


def cftc_cot():
    """CFTC COT（Traders in Financial Futures，期貨 only）標普 500 合併合約（標準、E-mini、微型）最近兩週。
    三類交易人（交易商、資產管理機構、槓桿基金）的多空口數；報告日是週二，週五才公布，所以資料日一定落後。"""
    q = urllib.parse.urlencode({"$limit": 2, "$order": "report_date_as_yyyy_mm_dd DESC", "market_and_exchange_names": config.CFTC_MARKET},
                               quote_via=urllib.parse.quote)
    rows, err = fetch_json(config.CFTC_TFF_BASE + "?" + q)
    if rows is None or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        log("cftc:cot", "failure", detail=_clip(f"抓取失敗或格式非預期: {err or str(rows)[:120]}"))
        return None
    keys = {"open_interest": "open_interest_all", "dealer_long": "dealer_positions_long_all", "dealer_short": "dealer_positions_short_all",
            "asset_mgr_long": "asset_mgr_positions_long", "asset_mgr_short": "asset_mgr_positions_short",
            "lev_long": "lev_money_positions_long", "lev_short": "lev_money_positions_short"}
    out, bad = [], 0
    for r in rows:
        if r.get("market_and_exchange_names") != config.CFTC_MARKET:
            bad += 1
            continue
        d = str(r.get("report_date_as_yyyy_mm_dd", ""))[:10]
        ok, why = validate_close_date(d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else None)
        vals = {k: parse_number(r.get(src)) for k, src in keys.items()}
        if (not ok or None in vals.values() or vals["open_interest"] <= 0 or any(v < 0 for v in vals.values())
                or any(vals[k] > vals["open_interest"] for k in ("dealer_long", "dealer_short", "asset_mgr_long", "asset_mgr_short", "lev_long", "lev_short"))):
            bad += 1
            continue
        out.append(dict(vals, report_date=d, market="S&P 500 Consolidated", source="CFTC TFF 期貨 only（publicreporting.cftc.gov）"))
    if not out:
        log("cftc:cot", "failure", detail=f"{len(rows)} 列都沒通過驗證（市場名稱、日期、口數）")
        return None
    out.sort(key=lambda r: r["report_date"], reverse=True)
    n = lambda r, k: r[k + "_long"] - r[k + "_short"]
    log("cftc:cot", "success", as_of=out[0]["report_date"],
        detail=f"報告日 {out[0]['report_date']}：槓桿基金淨 {n(out[0], 'lev'):+,.0f}、資產管理機構淨 {n(out[0], 'asset_mgr'):+,.0f}、交易商淨 {n(out[0], 'dealer'):+,.0f}（口）" + (f"；{bad} 列未通過驗證" if bad else ""))
    return out


def _now_et():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _parse_cboe(body, d):
    """CBOE 每日統計 JSON → Put/Call 一列（日期用呼叫端給的交易日）。總比率不在 0–20 內或格式不對就回傳 None。"""
    try:
        j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None
    ratios = {str(x.get("name", "")).strip().upper(): parse_number(x.get("value")) for x in (j.get("ratios") or []) if isinstance(x, dict)} if isinstance(j, dict) else {}
    total = ratios.get("TOTAL PUT/CALL RATIO")
    if total is None or not (0 < total < 20):
        return None
    pick = lambda name: ratios.get(name) if ratios.get(name) and 0 < ratios[name] < 20 else None
    return {"as_of": d.isoformat() if hasattr(d, "isoformat") else str(d), "total_pc": total, "index_pc": pick("INDEX PUT/CALL RATIO"), "equity_pc": pick("EQUITY PUT/CALL RATIO"),
            "etp_pc": pick("EXCHANGE TRADED PRODUCTS PUT/CALL RATIO"), "source": "CBOE 每日市場統計"}


def cboe_putcall():
    """CBOE 每日市場統計的 Put/Call 比率。只收『已收盤並結算』的交易日：美東日期早於今天，或今天但已過 17:00。
    由新到舊試最多 5 個平日，第一個有資料的就用，日期用它自己的交易日。"""
    now = _now_et()
    cands, d = [], now.date()
    while len(cands) < 5 and (now.date() - d).days < 10:
        if d.weekday() < 5 and (d < now.date() or now.hour >= 17):
            cands.append(d)
        d -= timedelta(days=1)
    tried = []
    for d in cands:
        status, ctype, body, err = fetch_raw(config.CBOE_DAILY_URL.format(ymd=d.isoformat()), timeout=config.PROBE_TIMEOUT)
        tried.append(f"{d}:{status}")
        if status != 200:
            time.sleep(1)
            continue
        row = _parse_cboe(body, d)
        if row is None:
            continue
        log("cboe:putcall", "success", as_of=row["as_of"], detail=f"總比率 {row['total_pc']}；指數 {row['index_pc']}；ETP {row['etp_pc']}；股票 {row['equity_pc'] if row['equity_pc'] is not None else '查不到'}")
        return [row]
    log("cboe:putcall", "no_new_data", detail=_clip(f"最近 {len(cands)} 個平日都沒有可用資料（{', '.join(tried)}）"))
    return None


def cboe_putcall_backfill(max_days=None):
    """模組 M14：回補 Put/Call 的歷史（給門檻的近一年分布用）。
    要補哪幾天：以 FRED 標普 500 有資料的日期當美股交易日，扣掉資料庫已經有的；每次最多 PC_BATCH_DAYS 天，間隔 PC_SLEEP 秒。
    CBOE 不一定保留很舊的資料：還沒成功過就連續 5 天都沒有，或已經成功過後連續 10 天都沒有，就停止並說明（不繼續打請求）。
    連線失敗整批停止；某天格式不對只略過那一天。回傳要寫入的列，由 persist_extra 寫入。"""
    max_days = max_days or config.PC_BATCH_DAYS
    if not sb_ready():
        log("pc_backfill", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=config.HIST_WINDOW_DAYS)
    days, err = sb_request("GET", f"macro_series?select=as_of&series_id=eq.SP500&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000")
    if not days:
        log("pc_backfill", "failure" if days is None else "no_new_data", detail=_clip(f"讀不到美股交易日清單（FRED 標普 500 歷史）：{err or '沒有資料'}；請先跑 tw_backfill 補 FRED 歷史"))
        return []
    have_rows, err = sb_request("GET", f"us_putcall?select=as_of&as_of=gte.{start.isoformat()}&limit=2000")
    have = {r["as_of"] for r in (have_rows or [])}
    todo_all = [r["as_of"] for r in days if r["as_of"] not in have]
    todo = todo_all[:max_days]
    if not todo:
        log("pc_backfill", "no_new_data", detail=f"近 {config.HIST_WINDOW_DAYS} 天的 {len(days)} 個美股交易日，Put/Call 都已經補齊")
        return []
    out, miss_run, missing, bad, stopped = [], 0, 0, 0, ""
    for d in todo:
        status, ctype, body, err = fetch_raw(config.CBOE_DAILY_URL.format(ymd=d), timeout=config.PROBE_TIMEOUT)
        time.sleep(config.PC_SLEEP)
        if status is None:
            stopped = f"連線失敗：{err}；已停止本次回補，不繼續打請求"
            log("pc_backfill", "failure", as_of=d, detail=_clip(stopped))
            break
        if status != 200:
            missing += 1
            miss_run += 1
            if miss_run >= (5 if not out else 10):
                stopped = f"連續 {miss_run} 天 CBOE 都沒有資料（最近一次 {d}：HTTP {status}），可能不保留更早的歷史；已停止本次回補"
                break
            continue
        miss_run = 0
        row = _parse_cboe(body, d)
        if row is None:
            bad += 1
            continue
        out.append(row)
    log("pc_backfill", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次補 {len(out)} 天" + (f"（{out[-1]['as_of']} 到 {out[0]['as_of']}）" if out else "") + f"；沒有資料 {missing} 天、格式不符 {bad} 天" +
                     (f"；{stopped}" if stopped else f"；還有 {max(len(todo_all) - len(todo), 0)} 天下次繼續")))
    return out


def _earn_date(token):
    token = token.strip()
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", token)
    try:
        if m:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        m = re.fullmatch(r"([A-Za-z]{3})[a-z]*\.? (\d{1,2}), (\d{4})", token)
        if m and _US_MONTHS.get(m.group(1).title()):
            return date(int(m.group(3)), _US_MONTHS[m.group(1).title()], int(m.group(2)))
    except ValueError:
        return None
    return None


def parse_earnings_text(text, today=None):
    """從 Nasdaq 的 reportText 找下次財報日，回傳 (日期, 是否預估, 盤前或盤後) 或 None。
    1) 先找明確句型「…to report earnings on <日期>」，後面還有別的日期也不受影響；
    2) 找不到這個句型時，文字裡必須剛好只有一個日期才採用；
    日期必須在今天到今天加 200 天之間，否則不採用，不猜。盤前＝before market open，盤後＝after market close。"""
    today = today or date.today()
    t = str(text or "")
    d = None
    m = re.search(r"report(?:s|ing)?\s+(?:its\s+)?earnings\s+on\s+(\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]{3,9}\.? \d{1,2}, \d{4})", t, re.I)
    if m:
        d = _earn_date(m.group(1))
    else:
        found = set()
        for tok in re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", t) + [f"{x[0]} {x[1]}, {x[2]}" for x in re.findall(r"\b([A-Za-z]{3})[a-z]*\.? (\d{1,2}), (\d{4})\b", t)]:
            dd = _earn_date(tok)
            if dd is None:
                return None
            found.add(dd)
        if len(found) == 1:
            d = next(iter(found))
    if d is None or not (today <= d <= today + timedelta(days=config.EARNINGS_MAX_AHEAD_DAYS)):
        return None
    est = bool(re.search(r"estimat|tentative|projected|expected|unconfirmed|derived from|not (yet )?confirmed", t, re.I))
    timing = "pre" if re.search(r"before market open", t, re.I) else "post" if re.search(r"after market close", t, re.I) else None
    return d.isoformat(), est, timing


def nasdaq_earnings(hold):
    """美股個股的下次財報日（Nasdaq）。ETF 沒有財報，不查。解析不出唯一日期就不寫入，並把原文記在紀錄裡。"""
    out = []
    for h in [x for x in hold if x["market"] == "US" and x.get("kind") == "stock"]:
        sym = (h.get("symbol") or h.get("pos_key") or "").upper().strip()
        if not sym:
            continue
        status, ctype, body, err = fetch_raw(config.NASDAQ_EARNINGS_URL.format(symbol=sym), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
        src = f"nasdaq:earnings:{sym}"
        if status is None:
            log(src, "failure", detail=_clip(f"連線失敗: {err}"))
            continue
        try:
            j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            log(src, "failure", detail=_clip(f"HTTP {status}｜不是 JSON"))
            continue
        data = j.get("data") if isinstance(j, dict) else None
        text = (data or {}).get("reportText") if isinstance(data, dict) else None
        res = parse_earnings_text(text)
        if res is None:
            log(src, "no_new_data", detail=_clip(f"沒有解析出唯一的未來財報日；Nasdaq 原文：{str(text)[:260]!r}"))
            continue
        out.append({"symbol": sym, "report_date": res[0], "is_estimate": res[1], "timing": res[2], "raw_text": str(text)[:300], "source": "Nasdaq earnings-date"})
        log(src, "success", as_of=res[0], detail=f"下次財報日 {res[0]}（{'預估' if res[1] else '公司公布'}{'，盤前' if res[2] == 'pre' else '，盤後' if res[2] == 'post' else ''}）")
        time.sleep(1)
    return out


def probe_earnings():
    """只讀探測：持股財報日的來源。美股用 Nasdaq 個股「下次財報日」；台股看證交所與櫃買中心開放資料有哪些法說會、財報相關端點。
    只記錄結構與前幾列，不寫任何資料表。"""
    hold, herr = load_holdings()
    if hold is None:
        log("probe:earn_holdings", "failure", detail=_clip(herr))
        hold = []
    for h in [x for x in hold if x["market"] == "US" and x.get("kind") == "stock"]:       # ETF 沒有財報
        sym = (h.get("symbol") or h.get("pos_key") or "").upper().strip()
        status, ctype, body, err = fetch_raw(config.NASDAQ_EARNINGS_URL.format(symbol=sym), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
        src = f"probe:earn_us:{sym}"
        if status is None:
            log(src, "failure", detail=_clip(f"連線失敗: {err}"))
            continue
        try:
            j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            log(src, "failure", detail=_clip(f"HTTP {status}｜不是 JSON：{body[:120].decode('utf-8', errors='replace')!r}"))
            continue
        data = j.get("data") if isinstance(j, dict) else None
        log(src, "success" if data else "failure", detail=_clip(f"HTTP {status}｜data={data}｜status={(j or {}).get('status') if isinstance(j, dict) else None}"))
        time.sleep(1)
    for tag, sw_url, base in (("twse", config.TWSE_OPENAPI_SWAGGER, config.TWSE_OPENAPI_BASE), ("tpex", config.TPEX_SWAGGER, config.TPEX_DEFAULT_BASE)):
        sw, err = fetch_json(sw_url)
        if sw is None or not isinstance(sw, dict):
            log(f"probe:earn_tw:{tag}", "failure", detail=_clip(f"說明文件抓取失敗: {err}"))
            continue
        cands = []
        for path, methods in (sw.get("paths") or {}).items():
            for m in (methods or {}).values():
                text = str(m.get("summary", "")) if isinstance(m, dict) else ""
                if any(k in text for k in config.EARNINGS_KEYWORDS):
                    cands.append((path, text))
        log(f"probe:earn_tw:{tag}", "success" if cands else "no_new_data",
            detail=_clip("含法說會／財報／營收字樣的端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:14]) if cands else "無")))
        picks = []
        first_conf = next((c for c in cands if "說明會" in c[1] or "法說" in c[1]), None)
        if first_conf:
            picks.append((first_conf, False))
        first_news = next((c for c in cands if "重大訊息" in c[1]), None)
        if first_news:
            picks.append((first_news, True))                       # 重大訊息：法說會是公司用重大訊息公告的，只留含「法人說明會」的列
        for (pick, news) in picks:
            rows, err = fetch_json(_openapi_base(sw, base) + pick[0])
            key = f"{tag}:news" if news else tag
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                if news:
                    hit = [r for r in rows if any("法人說明會" in str(v) for v in r.values())]
                    log(f"probe:earn_tw_rows:{key}", "success" if hit else "no_new_data",
                        detail=_clip(f"{pick[0]}｜共 {len(rows)} 列｜含『法人說明會』{len(hit)} 列｜欄位 {list(rows[0].keys())}｜前 2 筆 {hit[:2]}"))
                else:
                    log(f"probe:earn_tw_rows:{key}", "success", detail=_clip(f"{pick[0]}｜共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
            else:
                log(f"probe:earn_tw_rows:{key}", "failure", detail=_clip(f"{pick[0]} 抓取失敗或格式非預期: {err or str(rows)[:120]}"))


def _run_module(mid, label, fn, *args, **kw):
    """執行一個模組：任何例外都只記成這個模組的失敗，不讓整個工作中斷，也不影響其他模組。
    記錄的來源是 module:<代號>，網站的『功能檢查表』和健康檢查查詢都看這個。"""
    key = next((k for k in config.FEATURES if k.startswith(mid + "_")), None)
    if key is not None and not config.FEATURES.get(key, True):
        log(f"module:{mid}", "skipped", detail=f"{label}：在設定檔 FEATURES 裡被關閉")
        return None
    try:
        res = fn(*args, **kw)
    except Exception as e:  # noqa: BLE001 — 模組隔離：不管什麼錯都只影響自己
        log(f"module:{mid}", "failure", detail=_clip(f"{label}：執行時發生例外 {type(e).__name__}: {e}"))
        return None
    n = len(res) if isinstance(res, (list, tuple, dict)) else (0 if res is None else 1)
    log(f"module:{mid}", "success" if n else "no_new_data", detail=f"{label}：產出 {n} 筆" if n else f"{label}：沒有產出資料（原因見同一輪其他紀錄）")
    return res


def parse_pct(text):
    t = str(text or "").replace("%", "").replace("+", "").strip()
    return parse_number(t)


def _nasdaq_quote(sym, classes):
    """Nasdaq 報價。回傳 (資料, 原因)。只採用 marketStatus=Closed 且非即時的價格；日期用報價自己的時間戳。"""
    last = "查不到報價"
    for ac in classes:
        status, ctype, body, err = fetch_raw(config.NASDAQ_QUOTE_URL.format(symbol=sym, assetclass=ac), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
        if status is None:
            return None, f"連線失敗: {err}"
        try:
            j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        except (ValueError, UnicodeDecodeError):
            return None, f"HTTP {status}，回傳不是 JSON"
        d = j.get("data") if isinstance(j, dict) else None
        if not isinstance(d, dict) or not isinstance(d.get("primaryData"), dict):
            last = f"{ac} 分類沒有報價資料"
            continue
        pdta = d["primaryData"]
        ms = str(d.get("marketStatus"))
        if ms != "Closed" or pdta.get("isRealTime") is True:
            return None, f"marketStatus={ms}、isRealTime={pdta.get('isRealTime')}，不是已收盤的價格，不採用"
        iso = parse_us_date(pdta.get("lastTradeTimestamp"))
        ok, why = validate_close_date(iso)
        if not ok:
            return None, why
        close = parse_number(str(pdta.get("lastSalePrice", "")).replace("$", ""))
        pct = parse_pct(pdta.get("percentageChange"))
        if close is None:
            return None, f"收盤價無效 {pdta.get('lastSalePrice')!r}"
        return {"close": close, "pct": pct, "as_of": iso, "class": ac}, ""
    return None, last


def us_extra_quotes():
    """模組 M02：SPY、TLT、GLD（備用 IAU）、QQQ（只存漲跌幅）、SOX 指數。價格超出合理範圍一律拒收。"""
    out = []
    for sym, label, kind, classes, low, high, store in config.US_QUOTES:
        q, why = _nasdaq_quote(sym, classes)
        used, used_label, lo, hi = sym, label, low, high
        if q is None and sym in config.US_QUOTE_FALLBACKS:
            fsym, flabel, lo, hi = config.US_QUOTE_FALLBACKS[sym]
            q2, why2 = _nasdaq_quote(fsym, ("etf",))
            if q2 is not None:
                q, used, used_label = q2, fsym, flabel
            else:
                why = f"{why}；備用 {fsym} 也失敗：{why2}"
        if q is None:
            log(f"us_quote:{sym}", "skipped" if "不是已收盤" in why else "failure", detail=_clip(why))
            continue
        if not (lo <= q["close"] <= hi):
            log(f"us_quote:{used}", "failure", as_of=q["as_of"], detail=f"價格 {q['close']} 不在合理範圍 {lo}–{hi}，拒收")
            continue
        if not store and q["pct"] is None:
            log(f"us_quote:{used}", "failure", as_of=q["as_of"], detail="沒有漲跌幅，這個項目只顯示漲跌幅，不寫入")
            continue
        out.append({"symbol": used, "label": used_label, "kind": kind, "as_of": q["as_of"], "value": q["close"] if store else None,
                    "change_pct": q["pct"], "source": "Nasdaq 報價（已收盤）"})
        log(f"us_quote:{used}", "success", as_of=q["as_of"], detail=f"{'收盤 ' + str(q['close']) + '，' if store else ''}漲跌 {q['pct']}%")
        time.sleep(1)
    return out


def _nasdaq_history(sym, assetclass="etf", days=75):
    """Nasdaq 歷史行情（預設約 60 天，ETF）→ [(日期, 收盤)] 由舊到新；失敗回傳 (None, 原因)。"""
    today = date.today()
    url = config.NASDAQ_HIST_URL.format(symbol=sym, assetclass=assetclass, fromdate=(today - timedelta(days=days)).isoformat(), todate=today.isoformat())
    status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        return None, f"連線失敗: {err}"
    try:
        rows = (((json.loads(body.decode("utf-8-sig").lstrip("\ufeff")) or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None, "不是 JSON"
    pts = []
    for r in rows:
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", str(r.get("date", "")).strip())
        c = parse_number(str(r.get("close", "")).replace("$", ""))
        if not m or c is None or not (0 < c < config.PRICE_LIMIT):
            continue
        try:
            pts.append((date(int(m.group(3)), int(m.group(1)), int(m.group(2))), c))
        except ValueError:
            continue
    pts.sort()
    return pts, ""


def sector_rs():
    """模組 M03：11 檔產業 ETF 對 SPY 的相對強弱。ret_N＝近 N 個交易日報酬；rs_N＝產業報酬減 SPY 報酬（百分點）；
    streak_days＝最近連續幾天每日報酬高於 SPY。兩邊最新日期必須相同，歷史太短或有疑似分割跳動的 ETF 不算。"""
    bench, why = _nasdaq_history(config.SECTOR_BENCH)
    if not bench or len(bench) < config.SECTOR_MIN_ROWS:
        log("sector_rs", "failure", detail=_clip(f"基準 {config.SECTOR_BENCH} 歷史不足或抓取失敗: {why or len(bench or [])}"))
        return []
    bmap = dict(bench)
    out = []
    for sym, label in config.SECTOR_ETFS.items():
        pts, why = _nasdaq_history(sym)
        time.sleep(1)
        if not pts or len(pts) < config.SECTOR_MIN_ROWS:
            log(f"sector_rs:{sym}", "failure", detail=_clip(f"歷史不足或抓取失敗: {why or len(pts or [])}"))
            continue
        if pts[-1][0] != bench[-1][0]:
            log(f"sector_rs:{sym}", "failure", detail=f"最新日期 {pts[-1][0]} 與 {config.SECTOR_BENCH} 的 {bench[-1][0]} 不同，不比較")
            continue
        if any(b[1] / a[1] >= 1.5 or b[1] / a[1] <= 0.67 for a, b in zip(pts, pts[1:])):
            log(f"sector_rs:{sym}", "failure", detail="收盤價出現疑似分割的跳動，不計算")
            continue
        common = [(d, c, bmap[d]) for d, c in pts if d in bmap]
        if len(common) < config.SECTOR_MIN_ROWS:
            log(f"sector_rs:{sym}", "failure", detail="和 SPY 共同的交易日不足")
            continue
        ret = lambda n, i: (common[-1][i] / common[-1 - n][i] - 1) * 100
        streak = 0
        for k in range(len(common) - 1, 0, -1):
            if common[k][1] / common[k - 1][1] > common[k][2] / common[k - 1][2]:
                streak += 1
            else:
                break
        out.append({"symbol": sym, "label": label, "as_of": common[-1][0].isoformat(), "ret_5": round(ret(5, 1), 3), "ret_20": round(ret(20, 1), 3),
                    "rs_5": round(ret(5, 1) - ret(5, 2), 3), "rs_20": round(ret(20, 1) - ret(20, 2), 3), "streak_days": streak})
    if out:
        log("sector_rs", "success", as_of=out[0]["as_of"], detail=f"{len(out)} 檔產業 ETF；基準 {config.SECTOR_BENCH}")
    return out


def tw_leaders_compute():
    """模組 M04：台積電＋確認籃的均線位置。名稱必須和證交所官方名稱一致才算；均線由自建歷史計算，歷史不足的均線留空。"""
    rows, err = fetch_json(config.TWSE_AVG_URL)
    names = {}
    if isinstance(rows, list):
        for r in rows:
            if isinstance(r, dict):
                names[str(r.get("Code", "")).strip()] = str(r.get("Name", "")).strip()
    if not names:
        log("tw_leaders", "failure", detail=_clip(f"取不到證交所代號名稱表，無法核對: {err}"))
        return []
    out = []
    since = (date.today() - timedelta(days=400)).isoformat()
    for code, (name, tier) in config.TW_LEADERS.items():
        if names.get(code) != name:
            log(f"tw_leaders:{code}", "failure", detail=f"官方名稱是 {names.get(code)!r}，和預期的 {name!r} 不符，不計算")
            continue
        hist, err = sb_request("GET", f"tw_hist?select=as_of,close&code=eq.{code}&market=eq.TWSE&as_of=gte.{since}&order=as_of.asc&limit=1000")
        if not hist or len(hist) < 21:
            log(f"tw_leaders:{code}", "failure", detail=_clip(f"自建歷史只有 {len(hist or [])} 個交易日，不足 21 天: {err or ''}"))
            continue
        closes = [float(h["close"]) for h in hist]
        ma = lambda n: round(sum(closes[-n:]) / n, 3) if len(closes) >= n else None
        ret5 = round((closes[-1] / closes[-6] - 1) * 100, 3) if len(closes) >= 6 else None
        out.append({"as_of": hist[-1]["as_of"], "code": code, "name": name, "tier": tier, "close": closes[-1],
                    "ma20": ma(20), "ma60": ma(60), "ma200": ma(200), "ret5": ret5})
    if out:
        log("tw_leaders", "success", as_of=out[0]["as_of"], detail=f"{len(out)} / {len(config.TW_LEADERS)} 檔")
    return out


def fred_history_rows(series_id, low, high):
    """FRED 最近一段歷史（用各筆自己的日期）。回傳 [{series_id,label,as_of,value,source}]，失敗回傳 None。"""
    key = config.fred_key()
    if not key:
        return None
    start = (date.today() - timedelta(days=config.FRED_HISTORY_DAYS)).isoformat()
    url = f"{config.FRED_SERIES_URL}?series_id={series_id}&api_key={key}&file_type=json&sort_order=asc&observation_start={start}"
    data, err = fetch_json(url)
    if data is None or not isinstance(data, dict) or not isinstance(data.get("observations"), list):
        log(f"fred_history:{series_id}", "failure", detail=_clip(f"FRED 歷史抓取失敗: {err}"))
        return None
    label = config.FRED_MACRO[series_id][0]
    out, bad = [], 0
    for obs in data["observations"]:
        v, why = validate_value(obs.get("value"), low, high)
        if v is None:
            if "超出合理範圍" in why:
                bad += 1
            continue                                                         # 假日空值略過
        ok, _ = validate_close_date(obs.get("date"))
        if ok:
            out.append({"series_id": series_id, "label": label, "as_of": obs["date"], "value": v, "source": "FRED"})
    if bad:
        log(f"fred_history:{series_id}", "failure", detail=f"{bad} 筆超出合理範圍，已略過")
    return out


def fred_history_backfill():
    """模組 M06：把 SP500、NASDAQCOM、DJIA 的近一年歷史寫進 macro_series（用來算從高點回落與月績效）。"""
    out = []
    for sid in config.FRED_HISTORY:
        low, high = config.FRED_MACRO[sid][1], config.FRED_MACRO[sid][2]
        rows = fred_history_rows(sid, low, high)
        if rows:
            out.extend(rows)
            log(f"fred_history:{sid}", "success", as_of=rows[-1]["as_of"], detail=f"{len(rows)} 筆（{rows[0]['as_of']} 到 {rows[-1]['as_of']}）")
    return out


def tw_index_history():
    """模組 M05：加權指數近 13 個月的收盤與成交金額（證交所 FMTQIK 每月一次，間隔 3 秒），用來算從高點回落、下跌放量。"""
    today = date.today()
    out = []
    y, m = today.year, today.month
    for i in range(config.TW_INDEX_HISTORY_MONTHS):
        url = f"{config.TWSE_FMTQIK}?response=json&date={y}{m:02d}01"
        rows, why = _twse_month_rows(url)
        time.sleep(config.HIST_SLEEP)
        if rows is None:
            log("tw_index_history", "failure", detail=_clip(f"{y}-{m:02d}：{why}；已停止，不繼續打請求"))
            return out
        out.extend(rows)
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    if out:
        log("tw_index_history", "success", as_of=max(r["as_of"] for r in out), detail=f"{len(out)} 個交易日（{min(r['as_of'] for r in out)} 到 {max(r['as_of'] for r in out)}）")
    return out


def persist_extra(dry_run, tables):
    """寫入模組產出的資料表：tables = [(表名, 列, on_conflict)]。每張表獨立，一張寫失敗不影響其他。"""
    for table, rows, conflict in tables:
        if not rows:
            continue
        if dry_run or not sb_ready():
            log(f"persist:{table}", "skipped", detail=f"{'--dry-run' if dry_run else '未設定 Supabase'}：{len(rows)} 筆不寫入")
            continue
        for chunk in _chunks(rows, 500):
            ok, msg = sb_upsert(table, chunk, conflict)
            if not ok:
                log(f"persist:{table}", "failure", detail=_clip(msg))
                break
        else:
            log(f"persist:{table}", "success", detail=f"已確認寫入 {len(rows)} 筆")


def probe_round3():
    """只讀探測第三批：1) 證交所、櫃買中心的財報資訊（三率）；2) AAII 散戶情緒；3) Nasdaq-100 名單；4) SOX 指數報價的真實結構。"""
    for tag, sw_url, base, known in (("twse", config.TWSE_OPENAPI_SWAGGER, config.TWSE_OPENAPI_BASE, None), ("tpex", config.TPEX_SWAGGER, config.TPEX_DEFAULT_BASE, "/mopsfin_t187ap06_O_ciA")):
        sw, err = fetch_json(sw_url)
        if sw is None or not isinstance(sw, dict):
            log(f"probe:fin:{tag}", "failure", detail=_clip(f"說明文件抓取失敗: {err}"))
            continue
        cands = []
        for path, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                text = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if any(k in text for k in config.FIN_KEYWORDS):
                    cands.append((path, text))
        log(f"probe:fin:{tag}", "success" if cands else "no_new_data", detail=_clip("財報資訊相關端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:12]) if cands else "無")))
        pick = known if known and any(c[0] == known for c in cands) else next((c[0] for c in cands if "一般業" in c[1] or "_ci" in c[0]), None)
        if pick:
            rows, err = fetch_json(_openapi_base(sw, base) + pick)
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                log(f"probe:fin_rows:{tag}", "success", detail=_clip(f"{pick}｜共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜第 1 列 {rows[0]}"))
            else:
                log(f"probe:fin_rows:{tag}", "failure", detail=_clip(f"{pick} 抓取失敗或格式非預期: {err or str(rows)[:120]}"))
    status, ctype, body, err = fetch_raw(config.AAII_URL, timeout=config.PROBE_TIMEOUT, headers={"User-Agent": config.NASDAQ_UA, "Accept": "text/html"})
    if status is None:
        log("probe:aaii", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body.decode("utf-8", errors="replace")))
        i = text.find("Bullish")
        log("probe:aaii", "success" if (status == 200 and i >= 0) else "failure",
            detail=_clip(f"HTTP {status}｜{ctype}｜{len(body)} 位元組｜" + (f"『Bullish』附近：{text[i:i + 300]}" if i >= 0 else "頁面文字裡找不到 Bullish（被擋或要登入）")))
    status, ctype, body, err = fetch_raw(config.NASDAQ100_URL, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        log("probe:nasdaq100", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        try:
            j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
            data = (j or {}).get("data") or {}
            rows = ((data.get("data") or {}).get("rows") if isinstance(data.get("data"), dict) else None) or data.get("rows") or []
            log("probe:nasdaq100", "success" if rows else "failure", detail=_clip(f"HTTP {status}｜{len(rows)} 列｜資料鍵 {list(data.keys())}｜第 1 列 {rows[0] if rows else None}｜日期相關 {data.get('date') or (data.get('data') or {}).get('date') if isinstance(data.get('data'), dict) else data.get('date')}"))
        except (ValueError, UnicodeDecodeError, AttributeError):
            log("probe:nasdaq100", "failure", detail=_clip(f"HTTP {status}｜不是 JSON：{body[:120].decode('utf-8', errors='replace')!r}"))
    status, ctype, body, err = fetch_raw(config.NASDAQ_QUOTE_URL.format(symbol="SOX", assetclass="index"), timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        log("probe:sox", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        try:
            d = (json.loads(body.decode("utf-8-sig").lstrip("\ufeff")) or {}).get("data")
            log("probe:sox", "success" if isinstance(d, dict) and d.get("primaryData") else "failure",
                detail=_clip(f"HTTP {status}｜marketStatus={d.get('marketStatus') if isinstance(d, dict) else None}｜primary={d.get('primaryData') if isinstance(d, dict) else None}｜資料鍵 {list(d.keys()) if isinstance(d, dict) else d}"))
        except (ValueError, UnicodeDecodeError, AttributeError):
            log("probe:sox", "failure", detail=_clip(f"HTTP {status}｜不是 JSON"))


def _fin_key(keys, needles, exclude=()):
    for k in keys:
        if any(n in k for n in needles) and not any(x in k for x in exclude):
            return k
    return None


def _fin_rows(rows, wanted, source):
    """把財報資訊（一般業）轉成三率。欄位用名稱關鍵字找，找不到就回傳 (None, 欄位清單)，不猜。"""
    if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
        return None, []
    keys = list(rows[0].keys())
    k = {"code": _fin_key(keys, ("公司代號",)), "name": _fin_key(keys, ("公司名稱",)), "year": _fin_key(keys, ("年度",)), "q": _fin_key(keys, ("季別",)),
         "rev": _fin_key(keys, ("營業收入",), ("成本",)), "gp": _fin_key(keys, ("營業毛利",), ("未實現", "已實現", "淨額")),
         "op": _fin_key(keys, ("營業利益",)), "ni": _fin_key(keys, ("本期淨利", "稅後淨利"), ("歸屬", "非控制"))}
    if None in (k["code"], k["year"], k["q"], k["rev"], k["gp"], k["op"], k["ni"]):
        return None, keys
    out = []
    for r in rows:
        code = str(r.get(k["code"], "")).strip()
        if code not in wanted:
            continue
        try:
            y = int(str(r.get(k["year"], "")).strip())
            qs = str(r.get(k["q"], ""))
            qm = re.search(r"[1-4]", qs)
            q = int(qm.group(0)) if qm else {"一": 1, "二": 2, "三": 3, "四": 4}.get(next((ch for ch in qs if ch in "一二三四"), ""))
        except ValueError:
            continue
        y = y + 1911 if y < 1911 else y
        rev, gp, op, ni = (parse_number(r.get(k[x])) for x in ("rev", "gp", "op", "ni"))
        if q is None or None in (rev, gp, op, ni) or rev <= 0 or y > date.today().year:
            continue
        gm, om, nm = (round(v / rev * 100, 3) for v in (gp, op, ni))
        if not (-1000 <= gm <= 100 and -1000 <= om <= 100 and -1000 <= nm <= 200):
            continue
        out.append({"code": code, "name": wanted[code], "year": y, "quarter": q, "revenue": rev, "gross_profit": gp, "op_income": op, "net_income": ni,
                    "gross_margin": gm, "op_margin": om, "net_margin": nm, "source": source})
    return out, keys


def tw_financials(price_rows):
    """模組 M07：持股的三率（毛利率、營益率、淨利率）。來源：證交所、櫃買中心財報資訊（一般業）。開放資料只有最新一季，
    所以『比上一季』要靠每天把最新一季存起來，等下一季出來才比得了。金融業等其他業別的持股，這個來源沒有，會寫查不到。"""
    wanted = {r["code"]: r.get("name") or r["code"] for r in (price_rows or []) if r.get("market") == "TW" and r.get("code")}
    if not wanted:
        log("tw_fin", "skipped", detail="沒有台股持股代號，不抓財報資訊")
        return []
    out = []
    cover = {}
    sw, err = fetch_json(config.TWSE_OPENAPI_SWAGGER)
    path = None
    if isinstance(sw, dict):
        for pth, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                text = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if text.startswith("上市公司") and "一般業" in text and ("綜合損益" in text or "財報資訊" in text) and "_X_" not in pth:
                    path = path or pth                                    # 必須是「上市公司」；「公發公司」（_X_）不是上市股票，選到它你的持股會全部落空
        path = path or ("/opendata/t187ap06_L_ci" if "/opendata/t187ap06_L_ci" in (sw.get("paths") or {}) else None)
    sources = [("twse", sw, config.TWSE_OPENAPI_BASE, path, "證交所 上市公司財報資訊（一般業）")]
    sw2, err2 = fetch_json(config.TPEX_SWAGGER)
    sources.append(("tpex", sw2, config.TPEX_DEFAULT_BASE, config.TPEX_FIN_PATH, "櫃買中心 上櫃公司財報資訊（一般業）"))
    for tag, swag, base, pth, label in sources:
        if not pth or not isinstance(swag, dict):
            log(f"tw_fin:{tag}", "failure", detail=_clip(f"找不到財報資訊的端點或說明文件（{err if tag == 'twse' else err2}）"))
            continue
        rows, e = fetch_json(_openapi_base(swag, base) + pth)
        got, keys = _fin_rows(rows, wanted, label)
        if got is None:
            log(f"tw_fin:{tag}", "failure", detail=_clip(f"{pth} 抓取失敗或欄位對不上：{e or ''}欄位 {keys}"))
            continue
        kc_, ky_, kq_ = _fin_key(keys, ("公司代號",)), _fin_key(keys, ("年度",)), _fin_key(keys, ("季別",))
        dicts = [r for r in rows if isinstance(r, dict)]
        present_codes = {str(r.get(kc_, "")).strip() for r in dicts}
        cover[tag] = {"n": len(dicts), "present": present_codes, "got": {g["code"] for g in got},
                      "dist": [f"{y}年第{q}季×{n}" for (y, q), n in Counter((str(r.get(ky_, "")).strip(), str(r.get(kq_, "")).strip()) for r in dicts).most_common(3)]}
        log(f"tw_fin:{tag}", "success" if got else "no_new_data", detail=f"{pth}｜{len(got)} 檔持股有三率" + (f"（{got[0]['year']} 年第 {got[0]['quarter']} 季）" if got else "；你的持股目前不在這個資料集（資料集只放已公布最新一季的公司：可能還沒公布，或不是一般業）"))
        out.extend(got)
    if cover:
        present_all = set().union(*[c["present"] for c in cover.values()]); got_all = set().union(*[c["got"] for c in cover.values()])
        absent = sorted(f"{c} {n}" for c, n in wanted.items() if c not in present_all)
        skipped = sorted(f"{c} {wanted[c]}" for c in wanted if c in present_all and c not in got_all)
        log("tw_fin:coverage", "success" if not absent and not skipped else "no_new_data",
            detail=_clip("；".join(f"{t}：資料集 {c['n']} 列（{'、'.join(c['dist'])}）" for t, c in cover.items()) +
                         f"｜持股 {len(wanted)} 檔：有三率 {len(got_all)} 檔｜不在任何資料集：{'、'.join(absent) or '無'}｜在資料集但欄位缺值被略過：{'、'.join(skipped) or '無'}"))
    return out


def us_crash_screen():
    """模組 M08：Nasdaq-100 成分股單日跌超過門檻。名單只用來初篩（它沒有資料日期），入選一定要再用個股報價確認：
    報價已收盤、日期是報價自己的、跌幅仍達門檻，才寫入。涵蓋範圍只有 Nasdaq-100，不含只在 NYSE 的大型股。"""
    status, ctype, body, err = fetch_raw(config.NASDAQ100_URL, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        log("us_crash:list", "failure", detail=_clip(f"連線失敗: {err}"))
        raise RuntimeError(f"Nasdaq-100 名單連線失敗：{err}")
    try:
        j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        data = (j or {}).get("data") or {}
        rows = ((data.get("data") or {}).get("rows") if isinstance(data.get("data"), dict) else None) or data.get("rows") or []
    except (ValueError, UnicodeDecodeError, AttributeError):
        log("us_crash:list", "failure", detail=_clip(f"HTTP {status}｜不是 JSON"))
        raise RuntimeError("Nasdaq-100 名單不是 JSON")
    if not rows:
        log("us_crash:list", "failure", detail=f"HTTP {status}｜名單是空的")
        raise RuntimeError("Nasdaq-100 名單是空的")
    cands = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = str(r.get("symbol", "")).strip().upper()
        pct = parse_pct(next((r[k] for k in ("percentageChange", "pctChange", "change_pct") if k in r), None))
        if sym and pct is not None and pct <= -config.US_CRASH_PCT:
            cands.append((sym, str(r.get("companyName") or r.get("name") or "").strip(), pct))
    cands.sort(key=lambda x: x[2])                                     # 跌最多的先確認，萬一超過上限，漏掉的是跌得比較少的
    cut = len(cands) > config.US_CRASH_MAX_CONFIRM
    log("us_crash:list", "success", detail=f"名單 {len(rows)} 檔；初篩跌幅達 {config.US_CRASH_PCT}% 的有 {len(cands)} 檔" +
        (f"；超過上限，只確認跌最多的前 {config.US_CRASH_MAX_CONFIRM} 檔，清單不完整" if cut else ""))
    out = []
    for sym, name, pct in cands[:config.US_CRASH_MAX_CONFIRM]:
        q, why = _nasdaq_quote(sym, ("stocks",))
        time.sleep(1)
        if q is None:
            log(f"us_crash:{sym}", "skipped" if "不是已收盤" in why else "failure", detail=_clip(why))
            continue
        if q["pct"] is None or q["pct"] > -config.US_CRASH_PCT:
            log(f"us_crash:{sym}", "no_new_data", as_of=q["as_of"], detail=f"名單說 {pct}%，個股報價是 {q['pct']}%，沒有達到門檻，不加入")
            continue
        out.append({"as_of": q["as_of"], "symbol": sym, "name": name, "close": q["close"], "prev_close": round(q["close"] / (1 + q["pct"] / 100), 4),
                    "drop_pct": round(q["pct"], 2), "note": "Nasdaq-100 名單初篩、個股已收盤報價確認；若當天有分割或除息，跌幅可能是調整造成，原因待確認" + (f"。當天初篩 {len(cands)} 檔，只確認跌最多的前 {config.US_CRASH_MAX_CONFIRM} 檔，清單不完整" if cut else ""),
                    "source": "Nasdaq 報價（已收盤，只涵蓋 Nasdaq-100）"})
        log(f"us_crash:{sym}", "success", as_of=q["as_of"], detail=f"單日 {q['pct']}%，收盤 {q['close']}")
    return out


def aaii_sentiment():
    """模組 M09：AAII 散戶情緒（看多、中立、看空）。只在三項都找得到、加起來約 100、而且頁面上有自己的資料日期時才採用。"""
    status, ctype, body, err = fetch_raw(config.AAII_URL, timeout=config.PROBE_TIMEOUT, headers={"User-Agent": config.NASDAQ_UA, "Accept": "text/html"})
    if status != 200:
        log("aaii", "failure", detail=_clip(f"HTTP {status}｜{err or ctype}"))
        return []
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body.decode("utf-8", errors="replace")))
    val = {}
    for key, label in (("bullish", "Bullish"), ("neutral", "Neutral"), ("bearish", "Bearish")):
        m = re.search(label + r"[^0-9%]{0,60}?(\d{1,3}(?:\.\d+)?)\s*%", text)
        val[key] = float(m.group(1)) if m else None
    dm = re.search(r"(?:week ending|weekending|survey date|results (?:for|from))[^A-Za-z0-9]{0,15}(?:the week ending )?([A-Za-z]{3,9}\.? \d{1,2}(?:, ?\d{4})?)", text, re.I)
    if None in val.values() or not dm:
        log("aaii", "failure", detail=_clip(f"頁面抓到了，但{'三項比例沒有都找到' if None in val.values() else '找不到自己的資料日期'}，不採用｜{val}｜開頭 {text[:120]!r}"))
        return []
    tok = dm.group(1).replace(",", ", ").replace(",  ", ", ")
    mm = re.match(r"([A-Za-z]{3})[a-z]*\.? (\d{1,2})(?:, (\d{4}))?", tok)
    mon = _US_MONTHS.get(mm.group(1).title()) if mm else None
    if not mon:
        log("aaii", "failure", detail=_clip(f"日期格式看不懂：{dm.group(1)!r}"))
        return []
    today = date.today()
    try:
        d = date(int(mm.group(3)) if mm.group(3) else today.year, mon, int(mm.group(2)))
        if not mm.group(3) and d > today:
            d = date(today.year - 1, mon, int(mm.group(2)))
    except ValueError:
        log("aaii", "failure", detail=f"日期無效：{dm.group(1)!r}")
        return []
    ok, why = validate_close_date(d.isoformat())
    total = sum(val.values())
    if not ok or (today - d).days > 21 or not (98 <= total <= 102):
        log("aaii", "failure", as_of=d.isoformat(), detail=_clip(f"驗證不過：日期 {d}（{why or '距今 ' + str((today - d).days) + ' 天'}）、三項合計 {total}"))
        return []
    log("aaii", "success", as_of=d.isoformat(), detail=f"看多 {val['bullish']}%、中立 {val['neutral']}%、看空 {val['bearish']}%")
    return [{"as_of": d.isoformat(), "bullish": val["bullish"], "neutral": val["neutral"], "bearish": val["bearish"], "source": "AAII 散戶情緒調查（aaii.com）"}]


def us_leaders_compute():
    """模組 M10：美股領頭股（七巨頭、AVGO、TSM ADR）對均線，並和 SPY 比近 5 日。日期必須和 SPY 的最新日期相同，疑似分割的不算。"""
    days = config.US_LEADER_HISTORY_DAYS
    bench, why = _nasdaq_history(config.SECTOR_BENCH, "etf", days)
    if not bench or len(bench) < 25:
        log("us_leaders", "failure", detail=_clip(f"基準 {config.SECTOR_BENCH} 歷史不足或抓取失敗: {why or len(bench or [])}"))
        return []
    spy5 = (bench[-1][1] / bench[-6][1] - 1) * 100
    out = []
    for sym, name in config.US_LEADERS.items():
        pts, why = _nasdaq_history(sym, "stocks", days)
        time.sleep(1)
        if not pts or len(pts) < 25:
            log(f"us_leaders:{sym}", "failure", detail=_clip(f"歷史不足或抓取失敗: {why or len(pts or [])}"))
            continue
        if pts[-1][0] != bench[-1][0]:
            log(f"us_leaders:{sym}", "failure", detail=f"最新日期 {pts[-1][0]} 與 {config.SECTOR_BENCH} 的 {bench[-1][0]} 不同，不比較")
            continue
        if any(b[1] / a[1] >= 1.5 or b[1] / a[1] <= 0.67 for a, b in zip(pts, pts[1:])):
            log(f"us_leaders:{sym}", "failure", detail="收盤價出現疑似分割的跳動，不計算")
            continue
        c = [p[1] for p in pts]
        ma = lambda n: round(sum(c[-n:]) / n, 3) if len(c) >= n else None
        r5 = (c[-1] / c[-6] - 1) * 100
        out.append({"as_of": pts[-1][0].isoformat(), "symbol": sym, "name": name, "close": c[-1], "ma20": ma(20), "ma60": ma(60), "ret5": round(r5, 3), "rel5": round(r5 - spy5, 3)})
    if out:
        log("us_leaders", "success", as_of=out[0]["as_of"], detail=f"{len(out)} / {len(config.US_LEADERS)} 檔")
    return out


def _news_path(sw):
    for pth, methods in ((sw or {}).get("paths") or {}).items():
        for mth in (methods or {}).values():
            if isinstance(mth, dict) and "重大訊息" in str(mth.get("summary", "")):
                return pth
    return None


_CONF_LABELED = re.compile(r"法人說明會(?:之|的)?(?:舉行|召開|舉辦)?(?:日期|時間)\s*[:：]?\s*(\d{2,3})[/年.](\d{1,2})[/月.](\d{1,2})")
_CONF_CANCEL = re.compile(r"取消|延期|改期|暫停|不召開|不舉行")


def _conference_date(text, fact_raw, today):
    """從一則重大訊息找法說會日期。回傳 (日期或 None, 來源說明, 不採用的原因)。
    1) 說明裡有標明「法人說明會…日期：115/11/26」→ 只能有一個不同的日期；
    2) 沒有標明，但文字裡剛好只有一個「115年11月26日」→ 採用；兩個以上（例如原訂與更正）→ 不猜；
    3) 文字裡完全沒有日期，才用公告的「事實發生日」欄（受邀參加券商舉辦的法說會，日期通常只在這一欄）。
    日期必須在昨天到 180 天後之間；公告提到取消、延期、改期的不採用。"""
    if _CONF_CANCEL.search(text):
        return None, "", "公告提到取消、延期或改期，不採用"
    labeled = {m for m in _CONF_LABELED.findall(text)}
    if len(labeled) > 1:
        return None, "", "公告裡標明的法說會日期不只一個，不採用"
    triples, how = (labeled, "") if labeled else (set(), "")
    if not triples:
        triples = set(re.findall(r"(\d{3})年(\d{1,2})月(\d{1,2})日", text))
        if len(triples) > 1:
            return None, "", "公告提到法人說明會，但沒有唯一的日期，不採用"
    if not triples:
        iso = roc_to_iso(fact_raw) if fact_raw else None
        if not iso:
            return None, "", "公告提到法人說明會，但找不到日期（說明裡沒有、事實發生日欄也是空的），不採用"
        ev, how = date.fromisoformat(iso), "【日期取自公告的事實發生日】"
    else:
        y, mo, d = next(iter(triples))
        try:
            ev = date(int(y) + 1911, int(mo), int(d))
        except ValueError:
            return None, "", "日期不合法，不採用"
    if not (today - timedelta(days=1) <= ev <= today + timedelta(days=180)):
        return None, "", f"日期 {ev} 不在合理範圍（昨天到 180 天後），不採用"
    return ev, how, ""


def tw_investor_events(price_rows):
    """模組 M11：從證交所、櫃買中心『每日重大訊息』找你持股的法人說明會日期。
    每天的訊息只有當天的公告，所以靠每天排程累積。兩個市場的欄位名稱不一樣（上櫃是英文的 SecuritiesCompanyCode、CompanyName），
    日期怎麼取見 _conference_date；對不上就不採用，不猜。"""
    wanted = {r["code"]: r.get("name") or r["code"] for r in (price_rows or []) if r.get("market") == "TW" and r.get("code")}
    if not wanted:
        log("tw_events", "skipped", detail="沒有台股持股代號，不查法說會")
        return []
    today = date.today()
    out = {}
    for tag, swurl, base in (("twse", config.TWSE_OPENAPI_SWAGGER, config.TWSE_OPENAPI_BASE), ("tpex", config.TPEX_SWAGGER, config.TPEX_DEFAULT_BASE)):
        sw, err = fetch_json(swurl)
        pth = _news_path(sw) if isinstance(sw, dict) else None
        if not pth:
            log(f"tw_events:{tag}", "failure", detail=_clip(f"找不到重大訊息的端點（{err or '說明文件裡沒有'}）"))
            continue
        rows, e = fetch_json(_openapi_base(sw, base) + pth)
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            log(f"tw_events:{tag}", "failure", detail=_clip(f"{pth} 抓取失敗或格式非預期: {e or str(rows)[:100]}"))
            continue
        keys = list(rows[0].keys())
        kc = _fin_key(keys, ("公司代號", "CompanyCode", "Code"))
        kn = _fin_key(keys, ("公司名稱", "CompanyName"))
        ks, kd, kf = _fin_key(keys, ("主旨",)), _fin_key(keys, ("發言日期",)), _fin_key(keys, ("事實發生日",))
        if not kc:
            log(f"tw_events:{tag}", "failure", detail=_clip(f"{pth}：找不到公司代號欄（欄位 {keys}），不採用"))
            continue
        hit = conf = 0
        for r in rows:
            code = str(r.get(kc, "")).strip()
            text = " ".join(str(v) for v in r.values())
            if code not in wanted or "法人說明會" not in text:
                continue
            conf += 1
            ev, how, why = _conference_date(text, r.get(kf) if kf else None, today)
            if ev is None:
                log(f"tw_events:{code}", "no_new_data", detail=_clip(f"{wanted[code]}：{why}｜{str(r.get(ks, ''))[:80]}"))
                continue
            ann = roc_to_iso(r.get(kd)) if kd else None
            out[(code, ev.isoformat())] = {"code": code, "name": (str(r.get(kn, "")).strip() if kn else "") or wanted[code], "event_date": ev.isoformat(),
                                           "announced_at": ann, "subject": (how + str(r.get(ks, "")))[:200], "source": "證交所／櫃買中心 每日重大訊息"}
            hit += 1
        log(f"tw_events:{tag}", "success", detail=f"{pth}｜{len(rows)} 則公告；你的持股有 {conf} 則提到法人說明會，其中 {hit} 則取得日期")
    return list(out.values())


def probe_round4():
    """只讀探測第四批：1) AAII 頁面裡所有『Bullish』附近的文字與日期線索；2) 證交所除權息預告；3) 櫃買中心除權息端點。"""
    status, ctype, body, err = fetch_raw(config.AAII_URL, timeout=config.PROBE_TIMEOUT, headers={"User-Agent": config.NASDAQ_UA, "Accept": "text/html"})
    if status is None:
        log("probe:aaii_deep", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        html = body.decode("utf-8", errors="replace")
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"(?is)<(script|style).*?</\1>", " ", html)))
        hits = [text[max(m.start() - 60, 0): m.start() + 220] for m in re.finditer(r"Bullish", text)][:4]
        dates = re.findall(r"(?:Survey|Week|week|Results|Updated|as of)[^.]{0,80}\d{4}", text)[:4]
        pcts = re.findall(r"(?:Bullish|Neutral|Bearish)[^%]{0,30}\d{1,2}(?:\.\d)?%", text)[:6]
        log("probe:aaii_deep", "success" if (hits or pcts) else "failure", detail=_clip(f"HTTP {status}｜頁面可見文字 {len(text)} 字｜『Bullish』附近 {hits}｜日期線索 {dates}｜百分比 {pcts}"))
        scripts = re.findall(r"\"(?:contentUrl|downloadUrl|url)\"\s*:\s*\"([^\"]+)\"", html)[:6]
        log("probe:aaii_links", "success" if scripts else "no_new_data", detail=_clip(f"頁面裡的資料連結：{scripts}"))
    j, err = fetch_json(config.EXRIGHT_URL)
    if isinstance(j, dict):
        data = j.get("data") or []
        log("probe:exright_twse", "success" if data else "no_new_data", detail=_clip(f"stat={j.get('stat')}｜欄位 {j.get('fields')}｜{len(data)} 列｜前 2 列 {data[:2]}"))
    else:
        log("probe:exright_twse", "failure", detail=_clip(f"抓取失敗或不是 JSON: {err}"))
    sw, err = fetch_json(config.TPEX_SWAGGER)
    cands = []
    if isinstance(sw, dict):
        for path, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                t = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if "除權" in t or "除息" in t:
                    cands.append((path, t))
    log("probe:exright_tpex", "success" if cands else "no_new_data", detail=_clip("櫃買中心除權息相關端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:8]) if cands else "無")))
    if cands:
        rows, err = fetch_json(_openapi_base(sw, config.TPEX_DEFAULT_BASE) + cands[0][0])
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            log("probe:exright_tpex_rows", "success", detail=_clip(f"{cands[0][0]}｜{len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))


def probe_round5():
    """只讀探測第五批：台股公司基本資料裡的「產業別」（上市與上櫃）。
    記錄：端點、列數、欄位名稱、前 2 列、產業別有哪些不同的值與各幾家（最多 45 種），下一輪才知道怎麼做台股產業相對強弱。不寫入任何資料表。"""
    for tag, swurl, base in (("twse", config.TWSE_OPENAPI_SWAGGER, config.TWSE_OPENAPI_BASE), ("tpex", config.TPEX_SWAGGER, config.TPEX_DEFAULT_BASE)):
        sw, err = fetch_json(swurl)
        if not isinstance(sw, dict):
            log(f"probe:industry:{tag}", "failure", detail=_clip(f"說明文件抓取失敗: {err}"))
            continue
        cands = []
        for pth, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                t = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if "基本資料" in t and "公司" in t:
                    cands.append((pth, t))
        log(f"probe:industry_paths:{tag}", "success" if cands else "no_new_data", detail=_clip("公司基本資料端點：" + (" ; ".join(f"{p} | {t}" for p, t in cands[:6]) if cands else "無")))
        if not cands:
            continue
        pth = cands[0][0]
        rows, e = fetch_json(_openapi_base(sw, base) + pth)
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            log(f"probe:industry:{tag}", "failure", detail=_clip(f"{pth} 抓取失敗或格式非預期: {e or str(rows)[:100]}"))
            continue
        keys = list(rows[0].keys())
        kind = _fin_key(keys, ("產業別", "Industry", "IndustryCode"))
        cnt = Counter(str(r.get(kind, "")).strip() for r in rows if isinstance(r, dict)) if kind else Counter()
        log(f"probe:industry:{tag}", "success" if kind else "no_new_data",
            detail=_clip(f"{pth}｜共 {len(rows)} 列｜欄位 {keys}｜產業欄 {kind!r}｜不同的值 {len(cnt)} 種：{dict(cnt.most_common(45))}｜前 2 列 {rows[:2]}"))


def probe_us_pressure():
    """只讀探測：美股資金壓力三項的來源，只記錄結構與資料日期，不寫任何資料表。
    1) CFTC COT（Traders in Financial Futures，標普 500）；2) FINRA 融資統計頁面；3) CBOE Put/Call 候選網址。"""
    status, ctype, body, err = fetch_raw(config.CFTC_TFF_URL, timeout=config.PROBE_TIMEOUT)
    if status is None:
        log("probe:cftc_tff", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        try:
            rows = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                names = sorted({str(r.get("market_and_exchange_names")) for r in rows})
                dates = sorted({str(r.get("report_date_as_yyyy_mm_dd"))[:10] for r in rows}, reverse=True)
                log("probe:cftc_tff", "success", detail=_clip(f"HTTP {status}｜{len(rows)} 列｜市場 {names}｜報告日 {dates}｜欄位 {list(rows[0].keys())}"))
                log("probe:cftc_tff_row", "success", detail=_clip(f"第1列 {rows[0]}"))
            else:
                log("probe:cftc_tff", "failure", detail=_clip(f"HTTP {status}｜非預期格式：{str(rows)[:200]}"))
        except (ValueError, UnicodeDecodeError):
            log("probe:cftc_tff", "failure", detail=_clip(f"HTTP {status}｜不是 JSON：{body[:120].decode('utf-8', errors='replace')!r}"))

    status, ctype, body, err = fetch_raw(config.FINRA_MARGIN_URL, timeout=config.PROBE_TIMEOUT, headers={"User-Agent": config.NASDAQ_UA, "Accept": "text/html"})
    if status is None:
        log("probe:finra_margin", "failure", detail=_clip(f"連線失敗: {err}"))
    else:
        text = re.sub(r"<[^>]+>", " ", body.decode("utf-8", errors="replace"))
        text = re.sub(r"\s+", " ", text)
        i = text.find("Debit Balances")
        log("probe:finra_margin", "success" if i >= 0 else "failure",
            detail=_clip(f"HTTP {status}｜{ctype}｜{len(body)} 位元組｜" + (f"『Debit Balances』附近：{text[i:i + 420]}" if i >= 0 else "頁面文字裡找不到 Debit Balances（可能要執行網頁程式才會出現表格）")))

    ymd = (date.today() - timedelta(days=3)).isoformat()
    while date.fromisoformat(ymd).weekday() >= 5:
        ymd = (date.fromisoformat(ymd) - timedelta(days=1)).isoformat()
    for tag, tmpl in config.CBOE_PC_URLS.items():
        status, ctype, body, err = fetch_raw(tmpl.format(ymd=ymd), timeout=config.PROBE_TIMEOUT)
        src = f"probe:cboe_pc:{tag}"
        if status is None:
            log(src, "failure", detail=_clip(f"連線失敗: {err}"))
            continue
        head = body[:260].decode("utf-8", errors="replace")
        tail = body[-200:].decode("utf-8", errors="replace") if len(body) > 600 else ""
        log(src, "success" if status == 200 else "failure", detail=_clip(f"HTTP {status}｜{ctype}｜{len(body)} 位元組｜開頭 {head!r}｜結尾 {tail!r}"))


def probe_hist2():
    """只讀探測：Nasdaq 對槓桿 ETF 的歷史行情端點、櫃買中心『上櫃股票每日收盤行情』的完整欄位。"""
    hold, herr = load_holdings()
    etfs = [h for h in (hold or []) if h["market"] == "US" and h.get("kind") in ("etf", "leveraged_etf")]
    today = date.today()
    for h in etfs[:1]:
        sym = (h.get("symbol") or h.get("pos_key") or "").upper()
        url = config.NASDAQ_HIST_URL.format(symbol=sym, assetclass="etf", fromdate=(today - timedelta(days=400)).isoformat(), todate=today.isoformat())
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
        if status is None:
            log(f"probe:hist2_nasdaq:{sym}", "failure", detail=_clip(f"連線失敗: {err}"))
        else:
            try:
                j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
                rows = (((j or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
                log(f"probe:hist2_nasdaq:{sym}", "success" if rows else "failure",
                    detail=_clip(f"HTTP {status}｜{len(rows)} 列｜首列 {rows[0] if rows else None}｜末列 {rows[-1] if rows else None}｜{_describe_json(j)[:200]}"))
            except (ValueError, UnicodeDecodeError, AttributeError):
                log(f"probe:hist2_nasdaq:{sym}", "failure", detail=_clip(f"HTTP {status}｜不是 JSON：{body[:120].decode('utf-8', errors='replace')!r}"))
    day = today - timedelta(days=7)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    status, ctype, body, err = fetch_raw(config.TPEX_DAY_URL.format(slash=day.strftime("%Y/%m/%d").replace("/", "%2F")), timeout=config.PROBE_TIMEOUT)
    try:
        j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
        t = next((t for t in (j.get("tables") or []) if t.get("data")), {})
        log("probe:hist2_tpex_fields", "success" if t else "failure", detail=_clip(f"{day}｜date={t.get('date')}｜欄位 {t.get('fields')}｜首列 {(t.get('data') or [None])[0]}"))
    except (ValueError, UnicodeDecodeError, AttributeError):
        log("probe:hist2_tpex_fields", "failure", detail=_clip(f"HTTP {status}｜{err}｜不是 JSON"))


def twd_supplement(primary):
    """FRED 官方收盤落後時，另外抓 ExchangeRate-API 的即期參考價（第 5 層）。
    只當較新的參考：標示不是官方收盤價，不進收盤歷史，不改動 FRED 那一列。"""
    if not isinstance(primary, dict) or primary.get("stale_fallback") or primary.get("layer") != 3:
        return None
    age = (date.today() - date.fromisoformat(primary["as_of"])).days
    if age < config.TWD_LAG_DAYS:
        return None
    spot = exchangerate_twd()
    if not spot:
        return None
    log("twd_supplement", "success", as_of=spot["as_of"],
        detail=f"FRED 官方收盤落後 {age} 天（{primary['as_of']}），另存較新的即期參考價 {spot['value']}（{spot['as_of']}，不是官方收盤價）")
    return {"as_of": spot["as_of"], "value": spot["value"], "kind": "spot_aggregator", "source": "ExchangeRate-API",
            "layer": 5, "is_close_history": False}


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
def persist(dry_run, macro_rows, twd_row, tw_rows=None, breadth_rows=None, inst_rows=None, margin_rows=None,
            futures_rows=None, basis_rows=None, price_rows=None, cot_rows=None, putcall_rows=None, earnings_rows=None):
    if dry_run:
        log("persist", "skipped", detail=f"--dry-run：不寫 Supabase（本來會寫 macro {len(macro_rows)} 筆、"
                                         f"台幣 {len([r for r in (twd_row if isinstance(twd_row, list) else [twd_row]) if r and not r.get('stale_fallback')])} 筆、"
                                         f"台股 {len(tw_rows or [])} 筆、廣度 {len(breadth_rows or [])} 筆、"
                                         f"外資 {len(inst_rows or [])} 筆、融資 {len(margin_rows or [])} 筆、"
                                         f"期貨部位 {len(futures_rows or [])} 筆、基差 {len(basis_rows or [])} 筆、持股現價 {len(price_rows or [])} 筆、"
                                         f"COT {len(cot_rows or [])} 筆、Put/Call {len(putcall_rows or [])} 筆、財報日 {len(earnings_rows or [])} 筆）")
        return
    if not sb_ready():
        log("persist", "skipped", detail="未設定 SUPABASE_URL / SUPABASE_SERVICE_KEY，只寫本機檔，未寫正式資料庫")
        return
    if macro_rows:
        ok, msg = sb_upsert("macro_series", macro_rows, "series_id,as_of")
        log("persist:macro_series", "success" if ok else "failure", detail=msg)
    twd_list = twd_row if isinstance(twd_row, list) else ([twd_row] if twd_row else [])
    twd_list = [r for r in twd_list if r and not r.get("stale_fallback")]
    if twd_list:
        ok, msg = sb_upsert("twd_rates", twd_list, "layer,source,as_of")
        log("persist:twd_rates", "success" if ok else "failure", detail=msg)
    if tw_rows:
        ok, msg = sb_upsert("market_stats", tw_rows, "market,as_of,source")
        log("persist:market_stats", "success" if ok else "failure", detail=msg)
    if breadth_rows:
        ok, msg = sb_upsert("market_stats", breadth_rows, "market,as_of,source")
        log("persist:market_breadth", "success" if ok else "failure", detail=msg)
    if inst_rows:
        ok, msg = sb_upsert("tw_institutional", inst_rows, "as_of,source")
        log("persist:tw_institutional", "success" if ok else "failure", detail=msg)
    if margin_rows:
        ok, msg = sb_upsert("tw_margin", margin_rows, "as_of,source")
        log("persist:tw_margin", "success" if ok else "failure", detail=msg)
    if futures_rows:
        ok, msg = sb_upsert("tw_futures_position", futures_rows, "as_of,contract,investor,source")
        log("persist:tw_futures_position", "success" if ok else "failure", detail=msg)
    if basis_rows:
        ok, msg = sb_upsert("tw_futures_basis", basis_rows, "as_of,contract_month,source")
        log("persist:tw_futures_basis", "success" if ok else "failure", detail=msg)
    if price_rows:
        ok, msg = sb_upsert("stock_prices", price_rows, "market,code,as_of,source")
        log("persist:stock_prices", "success" if ok else "failure", detail=msg)
    if cot_rows:
        ok, msg = sb_upsert("us_cot", cot_rows, "report_date,market")
        log("persist:us_cot", "success" if ok else "failure", detail=msg)
    if putcall_rows:
        ok, msg = sb_upsert("us_putcall", putcall_rows, "as_of")
        log("persist:us_putcall", "success" if ok else "failure", detail=msg)
    if earnings_rows:
        ok, msg = sb_upsert("us_earnings", earnings_rows, "symbol,report_date")
        log("persist:us_earnings", "success" if ok else "failure", detail=msg)


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
def tw_events_job(dry_run=False):
    """傍晚專用工作：只跑台股法說會（M11）。
    原因：證交所、櫃買中心的「每日重大訊息」只放當天的公告，而公司常在收盤後才發法說會公告（例如 15:22）。
    14:35 的 tw_close 太早，會漏掉大部分；所以在台北時間 20:30 和 23:15（午夜換成隔天的公告之前）再各抓一次。
    要查哪些代號：用資料庫裡最新一天的台股持股收盤價紀錄（tw_close 每天寫入），不需要重新比對持股名稱。"""
    if not sb_ready():
        log("tw_events", "skipped", detail="未設定 Supabase，無法取得持股代號，不查法說會")
        return {"job": "tw_events", "events": []}
    rows, err = sb_request("GET", "stock_prices?select=code,name,as_of&market=eq.TW&order=as_of.desc&limit=200")
    if rows is None:
        log("tw_events", "failure", detail=_clip(f"讀取持股代號失敗：{err}"))
        return {"job": "tw_events", "events": []}
    latest = rows[0]["as_of"] if rows else None
    price_rows, seen = [], set()
    for r in rows:
        if r.get("as_of") == latest and r.get("code") and r["code"] not in seen:
            seen.add(r["code"])
            price_rows.append({"market": "TW", "code": r["code"], "name": r.get("name") or r["code"]})
    if not price_rows:
        log("tw_events", "skipped", detail="資料庫裡還沒有台股持股的收盤價紀錄（先跑一次 tw_close）")
        return {"job": "tw_events", "events": []}
    events = _run_module("M11", "台股法說會日曆", tw_investor_events, price_rows) or []
    persist_extra(dry_run, [("tw_investor_events", events, "code,event_date")])
    return {"job": "tw_events", "events": events}


def run_job(job, dry_run=False):
    global CURRENT_JOB
    CURRENT_JOB = job
    if job == "tw_events":                                        # 傍晚專用：只查法說會，不碰其他資料
        return tw_events_job(dry_run)
    macro_rows, twd_row, tw_rows, breadth_rows = [], None, None, None
    inst_rows = margin_rows = futures_rows = basis_rows = price_rows = cot_rows = putcall_rows = earnings_rows = None

    if job == "full":
        for sid, (label, low, high) in config.FRED_MACRO.items():
            if sid in ("DJIA", "BAMLH0A0HYM2", "RU2000PR") and not config.FEATURES.get("M01_fred_extra", True):
                continue
            r = fred_latest(sid, low, high, optional=sid in config.FRED_OPTIONAL)
            if r:
                macro_rows.append({"series_id": sid, "label": label, "as_of": r["as_of"],
                                   "value": r["value"], "source": "FRED"})
    if job in ("full", "tw_close"):
        twd_row = twd_fallback_chain()
        sup = twd_supplement(twd_row)
        twd_row = [twd_row, sup] if sup else twd_row
        tw_rows = twse_market()
        breadth_rows = twse_breadth()
        inst_rows = twse_institutional()
        margin_rows = twse_margin()
        futures_rows = taifex_futures()
        basis_rows = taifex_basis(tw_rows)
        cot_rows = cftc_cot()
        putcall_rows = cboe_putcall()
        hold, herr = load_holdings()
        if hold is None:
            log("prices:holdings", "skipped", detail=_clip(f"{herr}；不抓持股現價"))
        else:
            tp = tpex_prices(hold) or []
            tw = twse_prices(hold, {r["name"] for r in tp}) or []
            price_rows = tw_ranges(tp + tw) + (nasdaq_prices(hold) or [])
            earnings_rows = nasdaq_earnings(hold)
    if job == "tw_probe":
        probe_twse()
        probe_extra()
        probe_taifex_daily()
    if job == "us_probe":
        probe_earnings()
        probe_round3()
        probe_round4()
        probe_round5()
    inst_hist, pc_rows = [], []                                  # 回補專用的變數；不能叫 inst_rows，否則會把每天抓到的外資買賣超清掉
    if job == "tw_backfill":
        res = tw_backfill(dry_run=dry_run) or {}
        tw_breadth_update(dry_run=dry_run, max_days=40)
        tw_crash_check(list(res.get("new_days", [])) + _recent_complete_days(config.CRASH_RECENT_DAYS), dry_run=dry_run)
        if not dry_run:
            _run_module("M13", "漲跌家數歷史（自建）", tw_breadth_hist_compute)
            inst_hist = _run_module("M12", "外資買賣歷史回補", tw_institutional_backfill) or []
            pc_rows = _run_module("M14", "Put/Call 歷史回補", cboe_putcall_backfill) or []
    if job in ("full", "tw_close") and not dry_run:
        res = tw_backfill(dry_run=False, max_days=config.HIST_DAILY_DAYS, window_days=7) or {}
        tw_breadth_update(dry_run=False, max_days=10)
        tw_crash_check(list(res.get("new_days", [])) + _recent_complete_days(1))
    if job == "us_breadth":
        log("us_breadth", "skipped", detail="美股廣度尚未實作（第三步）；此排程不再偷跑全部更新")

    extra = []
    if job in ("full", "tw_close"):
        extra.append(("us_quotes", _run_module("M02", "美股其他行情", us_extra_quotes) or [], "symbol,as_of"))
        extra.append(("sector_rs", _run_module("M03", "美股產業相對強弱", sector_rs) or [], "symbol,as_of"))
        if not dry_run:
            extra.append(("tw_leaders", _run_module("M04", "台股領頭股", tw_leaders_compute) or [], "as_of,code"))
        extra.append(("tw_fin_q", _run_module("M07", "台股三率", tw_financials, price_rows or []) or [], "code,year,quarter"))
        extra.append(("us_crash_candidates", _run_module("M08", "美股大型股暴跌", us_crash_screen) or [], "as_of,symbol"))
        extra.append(("us_aaii", _run_module("M09", "AAII 散戶情緒", aaii_sentiment) or [], "as_of"))
        extra.append(("us_leaders", _run_module("M10", "美股領頭股", us_leaders_compute) or [], "as_of,symbol"))
        extra.append(("tw_investor_events", _run_module("M11", "台股法說會日曆", tw_investor_events, price_rows or []) or [], "code,event_date"))
    if job == "tw_backfill":
        extra.append(("tw_institutional", inst_hist, "as_of,source"))
        extra.append(("us_putcall", pc_rows, "as_of"))
        extra.append(("market_stats", _run_module("M05", "加權指數歷史", tw_index_history) if not dry_run else [], "market,as_of,source"))
        extra.append(("macro_series", _run_module("M06", "FRED 歷史回補", fred_history_backfill) if not dry_run else [], "series_id,as_of"))
        if not dry_run:
            extra.append(("tw_leaders", _run_module("M04", "台股領頭股", tw_leaders_compute) or [], "as_of,code"))
    extra = [(t, r or [], c) for t, r, c in extra]
    persist(dry_run, macro_rows, twd_row, tw_rows, breadth_rows, inst_rows, margin_rows, futures_rows, basis_rows, price_rows, cot_rows, putcall_rows, earnings_rows)
    persist_extra(dry_run, extra)
    return {"job": job, "macro": macro_rows, "twd": twd_row, "tw_market": tw_rows,
            "tw_breadth": breadth_rows, "tw_institutional": inst_rows, "tw_margin": margin_rows,
            "tw_futures": futures_rows, "tw_basis": basis_rows, "prices": price_rows, "us_cot": cot_rows, "us_putcall": putcall_rows, "us_earnings": earnings_rows}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=config.JOBS, default="full")
    parser.add_argument("--dry-run", action="store_true", help="照常抓資料，但不寫 Supabase")
    args = parser.parse_args(argv)

    global CURRENT_JOB
    CURRENT_JOB = args.job
    log("pipeline:version", "success", detail=f"程式版本 {config.PIPELINE_VERSION}｜fetch {FETCH_VERSION}｜工作 {args.job}｜{'dry-run' if args.dry_run else '正式寫入'}")
    if FETCH_VERSION != config.PIPELINE_VERSION:                      # 兩個檔案沒有一起更新：新功能可能沒有生效，要讓它在 Actions 亮紅燈
        log("pipeline:version_mismatch", "failure", detail=f"config.py 是 {config.PIPELINE_VERSION}，fetch.py 是 {FETCH_VERSION}：檔案沒有一起更新，請把 pipeline 資料夾裡的 fetch.py 和 config.py 都換成同一版")
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
