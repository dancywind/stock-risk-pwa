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
import json
import os
import re
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
                return json.loads(resp.read().decode("utf-8-sig").lstrip("\ufeff")), ""  # 期交所回傳開頭帶 BOM（連續多個也處理）
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


def twse_institutional():
    """三大法人買賣金額（現貨）。外資 = 外資及陸資(不含外資自營商) + 外資自營商。單位：新台幣元。
    買進－賣出 必須等於買賣差額，對不上就拒收。"""
    got = _twse_fetch_day(config.TWSE_BFI82U, "twse:institutional")
    if got is None:
        return None
    data, as_of = got
    fields = data.get("fields") or []
    need = ("單位名稱", "買進金額", "賣出金額", "買賣差額")
    if any(n not in fields for n in need):
        log("twse:institutional", "failure", as_of=as_of, detail=f"欄位與預期不符: {fields}")
        return None
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
        log("twse:institutional", "failure", as_of=as_of, detail="找不到『外資及陸資』列或數字無法解析")
        return None
    fd = find(lambda l: l == "外資自營商")
    note = ""
    if fd is None or None in fd:
        fd, note = (0.0, 0.0, 0.0), "（外資自營商列缺漏，未計入）"
    for buy, sell, diff in (main, fd):
        if abs(diff - (buy - sell)) > 1:
            log("twse:institutional", "failure", as_of=as_of, detail="買進－賣出 與買賣差額對不上，拒收")
            return None
    f_buy, f_sell, f_net = main[0] + fd[0], main[1] + fd[1], main[2] + fd[2]
    if not (0 <= f_buy <= config.INST_SIDE_LIMIT and 0 <= f_sell <= config.INST_SIDE_LIMIT
            and abs(f_net) <= config.INST_NET_LIMIT):
        log("twse:institutional", "failure", as_of=as_of, detail=f"外資金額 {f_buy}/{f_sell} 超出合理範圍，拒收")
        return None

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
    log("twse:institutional", "success", as_of=as_of, detail=f"外資買賣超 {f_net / 1e8:+.2f} 億元（現貨）{note}")
    return [row]


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
def persist(dry_run, macro_rows, twd_row, tw_rows=None, breadth_rows=None, inst_rows=None, margin_rows=None):
    if dry_run:
        log("persist", "skipped", detail=f"--dry-run：不寫 Supabase（本來會寫 macro {len(macro_rows)} 筆、"
                                         f"台幣 {1 if twd_row and not twd_row.get('stale_fallback') else 0} 筆、"
                                         f"台股 {len(tw_rows or [])} 筆、廣度 {len(breadth_rows or [])} 筆、"
                                         f"外資 {len(inst_rows or [])} 筆、融資 {len(margin_rows or [])} 筆）")
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
    macro_rows, twd_row, tw_rows, breadth_rows = [], None, None, None
    inst_rows = margin_rows = None

    if job == "full":
        for sid, (label, low, high) in config.FRED_MACRO.items():
            r = fred_latest(sid, low, high)
            if r:
                macro_rows.append({"series_id": sid, "label": label, "as_of": r["as_of"],
                                   "value": r["value"], "source": "FRED"})
    if job in ("full", "tw_close"):
        twd_row = twd_fallback_chain()
        tw_rows = twse_market()
        breadth_rows = twse_breadth()
        inst_rows = twse_institutional()
        margin_rows = twse_margin()
    if job == "tw_probe":
        probe_twse()
        probe_extra()
    if job == "us_breadth":
        log("us_breadth", "skipped", detail="美股廣度尚未實作（第三步）；此排程不再偷跑全部更新")

    persist(dry_run, macro_rows, twd_row, tw_rows, breadth_rows, inst_rows, margin_rows)
    return {"job": job, "macro": macro_rows, "twd": twd_row, "tw_market": tw_rows,
            "tw_breadth": breadth_rows, "tw_institutional": inst_rows, "tw_margin": margin_rows}


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
