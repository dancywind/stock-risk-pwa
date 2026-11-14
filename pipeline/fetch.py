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
    python3 pipeline/fetch.py --job us_breadth      # 美股廣度（Nasdaq-100 站上 50、200 日線的比例）
    python3 pipeline/fetch.py --job full --dry-run  # 照常抓資料，但不寫 Supabase
"""
import argparse
from collections import Counter
import gzip
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


FETCH_VERSION = "v107"      # 每次發行跟 config.PIPELINE_VERSION 一起改；兩者不同就代表檔案沒有一起更新


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
_JSON_CACHE = {}


def _with_gzip(headers):
    """請求標頭一律加上 Accept-Encoding: gzip（呼叫端沒寫才加）。櫃買中心的收盤價回應有 5 MB 以上，不壓縮時曾經傳到一半被切斷（IncompleteRead）。"""
    h = dict(headers or {})
    if not any(k.lower() == "accept-encoding" for k in h):
        h["Accept-Encoding"] = "gzip"
    return h


def _read_body(resp):
    """讀回應內容；伺服器說是 gzip 就解壓縮（標頭說是 gzip 但其實不是，就當作沒壓縮）。"""
    raw = resp.read()
    hdrs = getattr(resp, "headers", None)
    enc = (hdrs.get("Content-Encoding", "") if hdrs is not None else "") or ""
    if enc.lower() == "gzip":
        try:
            return gzip.decompress(raw)
        except (OSError, EOFError):
            return raw
    return raw


EMPTY_RESPONSE_MSG = "空的回應（連線成功，但沒有任何內容）"


def is_empty_response(err):
    """抓取失敗的原因是不是「對方回傳空內容」。這通常是暫時的（正在更新資料、或沒有資料），和被擋、HTML 錯誤頁不同。"""
    return EMPTY_RESPONSE_MSG in str(err or "")


def fetch_json(url, headers=None, timeout=None):
    """帶重試的抓取。失敗回傳 (None, 原因)，不拋出。
    被對方擋下（HTTP 403、429、503）時等得比較久（BLOCK_BACKOFF_SECONDS 起跳）：這類多半是暫時的限流，馬上再試只會繼續被擋。"""
    last_err = ""
    for attempt in range(1, config.RETRIES + 1):
        blocked = False
        try:
            req = urllib.request.Request(url, headers=_with_gzip(headers or {"User-Agent": "risk-pwa/1.0", "Accept": "application/json"}))
            with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
                raw = _read_body(resp)
                text = raw.decode("utf-8-sig", errors="replace").lstrip("\ufeff")                # 期交所回傳開頭帶 BOM（連續多個也處理）
                if not text.replace("\ufeff", "").strip():
                    raise ValueError(EMPTY_RESPONSE_MSG)                                           # 連線成功但沒有任何內容：對方在更新資料或暫時沒有資料
                try:
                    return json.loads(text), ""
                except ValueError:
                    head = re.sub(r"\s+", " ", text[:60]).strip()
                    raise ValueError(f"不是 JSON（{len(raw)} 位元組；開頭是 {head!r}）")           # 讓你一眼看出收到的是 HTML 錯誤頁還是別的東西
        except urllib.error.HTTPError as e:
            last_err = _scrub(e)
            blocked = e.code in (403, 429, 503, 307, 308)                                    # 307／308 而且沒有可以跟隨的目標網址（urllib 才會丟錯）：對方的防護機制暫時擋住，和 403 一樣等久一點再試
        except Exception as e:  # noqa: BLE001
            last_err = _scrub(e)
        if attempt < config.RETRIES:
            time.sleep((config.BLOCK_BACKOFF_SECONDS if blocked else config.BACKOFF_SECONDS) * attempt)
    return None, last_err


DASH_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36", "Accept": "text/html,application/json,*/*", "Accept-Language": "zh-TW,zh;q=0.9"}


def fetch_swagger(url):
    """交易所的說明文件（swagger.json）：同一輪工作只抓一次，各模組共用（以前每個模組各抓一次，請求太密集可能被櫃買中心暫時擋下）。只快取成功的。"""
    if url in _JSON_CACHE:
        return _JSON_CACHE[url], ""
    data, err = fetch_json(url)
    if data is not None:
        _JSON_CACHE[url] = data
    return data, err


def _fetch_raw_once(url, timeout=None, headers=None):
    """單次請求：不解析 JSON，只回傳 (HTTP 狀態, Content-Type, 原始位元組, 錯誤)。"""
    headers = _with_gzip(headers or {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"})
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), _read_body(resp), ""
    except urllib.error.HTTPError as e:
        ctype = e.headers.get("Content-Type", "") if e.headers else ""
        return e.code, ctype, e.read()[:2000], f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, "", b"", _scrub(e)


def fetch_post_raw(url, form, timeout=None, headers=None):
    """表單 POST（期交所的歷史下載要用 POST）→ (HTTP 狀態, Content-Type, 位元組, 錯誤)。"""
    h = _with_gzip(dict(headers or {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)"}))
    h["Content-Type"] = "application/x-www-form-urlencoded"
    try:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(form).encode(), headers=h, method="POST")
        with urllib.request.urlopen(req, timeout=timeout or config.TIMEOUT_SECONDS) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), _read_body(resp), ""
    except urllib.error.HTTPError as e:
        return e.code, (e.headers.get("Content-Type", "") if e.headers else ""), e.read()[:2000], f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, "", b"", _scrub(e)


def _html_hint(body):
    """回傳的是網頁不是 CSV 時，抓出 <title> 和一小段文字，讓紀錄看得出是什麼頁面。"""
    t = _decode_tw_csv(body or b"")
    m = re.search(r"<title>(.*?)</title>", t, re.S | re.I)
    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"(?is)<(script|style).*?</\1>", " ", t)))
    return f"標題 {m.group(1).strip()[:60] if m else '無'}；內文 {txt.strip()[:160]}"


_NASDAQ_GUARD = {"blocked_until": 0.0, "last": None}               # Nasdaq 被擋時的「冷卻」狀態（整個執行共用）
_NASDAQ_BLOCK = (403, 429)


def fetch_raw(url, timeout=None, headers=None):
    """診斷用：不解析 JSON，只回傳 (HTTP 狀態, Content-Type, 原始位元組, 錯誤)。
    只有 api.nasdaq.com 有額外保護（GitHub 排程的機器偶爾會被 Nasdaq 暫時擋掉，回 HTTP 403）：
    403／429 或連線逾時就照 NASDAQ_RETRY_WAITS 等一下再試；最後還是 403／429，就記下「被擋」，之後 NASDAQ_COOLDOWN 秒內不再打 Nasdaq
    （直接回傳同樣的結果），避免一次產生幾十筆失敗、也避免白等。其他網站、其他狀態碼（成功、404…）完全不受影響。"""
    if "api.nasdaq.com" not in url:
        return _fetch_raw_once(url, timeout, headers)
    if time.monotonic() < _NASDAQ_GUARD["blocked_until"] and _NASDAQ_GUARD["last"]:
        st, ct, body, err = _NASDAQ_GUARD["last"]
        return st, ct, body, f"{err}｜本次執行已確認 Nasdaq 暫時擋住，不再重試"
    res = _fetch_raw_once(url, timeout, headers)
    tries = 1
    for wait in config.NASDAQ_RETRY_WAITS:
        if res[0] is not None and res[0] not in _NASDAQ_BLOCK:
            break
        time.sleep(wait + (time.time() * 1000 % 3))                  # 加一點 0 到 3 秒的零頭，避免和別的請求同時重試
        res = _fetch_raw_once(url, timeout, headers)
        tries += 1
    if res[0] in _NASDAQ_BLOCK:
        _NASDAQ_GUARD["blocked_until"] = time.monotonic() + config.NASDAQ_COOLDOWN
        _NASDAQ_GUARD["last"] = res
        log("nasdaq_guard", "no_new_data", detail=_clip(f"Nasdaq 回 HTTP {res[0]}：試了 {tries} 次仍被擋（GitHub 這台機器的 IP 可能被暫時擋掉）。{config.NASDAQ_COOLDOWN // 60} 分鐘內不再打 Nasdaq；這次美股的價格、產業等會沿用舊資料。晚一點再按「立刻更新資料」或重跑一次。"))
    elif res[0] is not None:
        _NASDAQ_GUARD["blocked_until"] = 0.0
    return res


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


def _parse_margin(data):
    """信用交易統計（上市）的解析與驗證，不寫紀錄。回傳 (列, 錯誤原因)。
    融資金額（仟元換算成元）。前日餘額＋買進－賣出－償還 必須等於今日餘額；餘額要在合理範圍。as_of 由呼叫端放進去。"""
    need = ("項目", "買進", "賣出", "現金(券)償還", "前日餘額", "今日餘額")
    table = next((t for t in (data.get("tables") or []) if all(k in (t.get("fields") or []) for k in need)), None)
    if table is None:
        return None, "找不到『信用交易統計』表格或欄位不符"
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
        return None, "找不到『融資金額(仟元)』列或數字無法解析"
    if abs(amt["前日餘額"] + amt["買進"] - amt["賣出"] - amt["現金(券)償還"] - amt["今日餘額"]) > 1:
        return None, "前日餘額＋買進－賣出－償還 與今日餘額對不上，拒收"
    balance, prev = amt["今日餘額"] * 1000, amt["前日餘額"] * 1000
    lo, hi = config.MARGIN_BALANCE_RANGE
    if not (lo <= balance <= hi):
        return None, f"融資餘額 {balance} 超出合理範圍，拒收"
    units = (rows.get("融資(交易單位)") or {}).get("今日餘額")
    short = (rows.get("融券(交易單位)") or {}).get("今日餘額")
    return {"margin_balance": balance, "margin_prev_balance": prev, "margin_units": units, "short_units": short, "source": "TWSE MI_MARGN"}, None


def twse_margin():
    """信用交易統計：融資金額餘額（仟元換算成元）。前日餘額＋買進－賣出－償還 必須等於今日餘額。"""
    got = _twse_fetch_day(config.TWSE_MI_MARGN, "twse:margin")
    if got is None:
        return None
    data, as_of = got
    row, err = _parse_margin(data)
    if err:
        log("twse:margin", "failure", as_of=as_of, detail=err)
        return None
    log("twse:margin", "success", as_of=as_of,
        detail=f"融資餘額 {row['margin_balance'] / 1e8:,.0f} 億元，較前日 {(row['margin_balance'] - row['margin_prev_balance']) / 1e8:+.1f} 億元")
    return [{"as_of": as_of, **row}]


def tw_margin_backfill(max_days=None):
    """模組 M19：回補上市融資餘額的歷史（給融資去槓桿的「單日減少」「從高點累積減少」「連續減幾天」用）。
    做法和外資歷史（M12）一樣：以自建行情確認過的上市交易日為準，扣掉資料庫已經有的；每次最多 MARGIN_BATCH_DAYS 天，間隔 HIST_SLEEP 秒；
    每一天都要驗證：證交所回傳的日期必須就是我們要的那一天，數字用和每日一樣的驗證（前日＋買進－賣出－償還＝今日）。
    連線失敗就整批停止；某天沒有資料或驗證不過，只略過那一天。"""
    max_days = max_days or config.MARGIN_BATCH_DAYS
    if not sb_ready():
        log("margin_backfill", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=config.HIST_WINDOW_DAYS)
    days, err = sb_request("GET", f"tw_hist_days?select=as_of&market=eq.TWSE&status=eq.ok&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000")
    if days is None:
        log("margin_backfill", "failure", detail=_clip(f"讀取交易日清單失敗：{err}"))
        return []
    have_rows, err = sb_request("GET", f"tw_margin?select=as_of&source=eq.TWSE%20MI_MARGN&as_of=gte.{start.isoformat()}&limit=2000")
    have = {r["as_of"] for r in (have_rows or [])}
    todo_all = [r["as_of"] for r in days if r["as_of"] not in have]
    todo = todo_all[:max_days]
    if not todo:
        log("margin_backfill", "no_new_data", detail=f"近 {config.HIST_WINDOW_DAYS} 天的 {len(days)} 個交易日，融資餘額都已經補齊")
        return []
    out, skipped, bad, first_err = [], 0, 0, ""
    for d in todo:
        data, err = fetch_json(config.TWSE_MI_MARGN + "&date=" + d.replace("-", ""))
        time.sleep(config.HIST_SLEEP)
        if data is None:
            log("margin_backfill", "failure", as_of=d, detail=_clip(f"抓取失敗：{err}；已停止本次回補，不繼續打請求"))
            break
        if not isinstance(data, dict) or data.get("stat") != "OK":
            skipped += 1
            continue
        as_of = twse_ymd_to_iso(data.get("date"))
        if as_of != d:
            bad += 1
            first_err = first_err or f"要 {d} 但證交所回傳 {as_of}"
            continue
        row, perr = _parse_margin(data)
        if perr:
            bad += 1
            first_err = first_err or f"{d}：{perr}"
            continue
        out.append({"as_of": as_of, **row})
    log("margin_backfill", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次補 {len(out)} 天" + (f"（{out[-1]['as_of']} 到 {out[0]['as_of']}）" if out else "") +
                     f"；略過 {skipped} 天（證交所沒有資料）、{bad} 天（日期或數字驗證不過" + (f"：{first_err}" if first_err else "") + f"）；還有 {max(len(todo_all) - len(todo), 0)} 天下次繼續"))
    return out


def tw_margin_long_backfill(max_days=None):
    """模組 M43：把上市融資餘額往前回補到 INDEX_LONG_YEARS 年前（給計分卡歷史驗證用，M19 只補近 400 天）。
    交易日以資料庫裡加權指數的歷史（market_stats，M05／M24 寫入）為準，扣掉 tw_margin 已經有的；每次最多 MARGIN_LONG_BATCH 天、最多跑 MARGIN_LONG_MINUTES 分鐘，
    由新到舊補。每一天的驗證和每日更新相同（日期要對、前日＋買進－賣出－償還＝今日、餘額在合理範圍）。
    連線失敗就整批停止；某天沒有資料或驗證不過只略過那一天（太舊的日期證交所格式不同時會被略過，不會寫進錯的數字）。重跑不會重複（主鍵是日期）。"""
    max_days = max_days or config.MARGIN_LONG_BATCH
    if not sb_ready():
        log("margin_long", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=int(config.INDEX_LONG_YEARS * 365))
    days, have = [], set()
    for off in range(0, 3000, 1000):
        got, err = sb_request("GET", f"market_stats?select=as_of&source=eq.TWSE%20FMTQIK&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000&offset={off}")
        if got is None:
            log("margin_long", "failure", detail=_clip(f"讀取交易日清單失敗：{err}"))
            return []
        days.extend(r["as_of"] for r in got)
        if len(got) < 1000:
            break
    for off in range(0, 3000, 1000):
        got, err = sb_request("GET", f"tw_margin?select=as_of&source=eq.TWSE%20MI_MARGN&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000&offset={off}")
        if got is None:
            log("margin_long", "failure", detail=_clip(f"讀取已有的融資日期失敗：{err}"))
            return []
        have.update(r["as_of"] for r in got)
        if len(got) < 1000:
            break
    todo_all = [d for d in days if d not in have]
    if not todo_all:
        log("margin_long", "no_new_data", detail=f"{len(days)} 個交易日的融資餘額都已經補齊")
        return []
    out, skipped, bad, first_err, tried = [], 0, 0, "", 0
    t0 = time.time()
    for d in todo_all[:max_days]:
        if time.time() - t0 > config.MARGIN_LONG_MINUTES * 60:
            break
        tried += 1
        data, err = fetch_json(config.TWSE_MI_MARGN + "&date=" + d.replace("-", ""))
        time.sleep(config.HIST_SLEEP)
        if data is None:
            log("margin_long", "failure", as_of=d, detail=_clip(f"抓取失敗：{err}；已停止本次回補，不繼續打請求"))
            break
        if not isinstance(data, dict) or data.get("stat") != "OK":
            skipped += 1
            continue
        as_of = twse_ymd_to_iso(data.get("date"))
        if as_of != d:
            bad += 1
            first_err = first_err or f"要 {d} 但證交所回傳 {as_of}"
            continue
        row, perr = _parse_margin(data)
        if perr:
            bad += 1
            first_err = first_err or f"{d}：{perr}"
            continue
        out.append({"as_of": as_of, **row})
    log("margin_long", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次試 {tried} 天，補 {len(out)} 天" + (f"（{out[-1]['as_of']} 到 {out[0]['as_of']}）" if out else "") +
                     f"；略過 {skipped} 天（證交所沒有資料）、{bad} 天（驗證不過" + (f"：{first_err}" if first_err else "") + f"）；還有 {max(len(todo_all) - tried, 0)} 天下次繼續"))
    return out


def parse_breadth_ms(data, want_iso):
    """證交所 MI_INDEX（type=MS）指定日期的回傳 → ((上漲, 下跌, 持平), 錯誤原因)。取『漲跌證券數合計』的「股票」欄（和每日更新同一個口徑，不含權證、ETF）。
    回傳的日期必須就是 want_iso；三項都要讀得出來、合計在合理範圍，否則回傳 (None, 原因)。"""
    if not isinstance(data, dict) or data.get("stat") != "OK":
        return None, "證交所沒有這天的資料"
    got_iso = twse_ymd_to_iso(data.get("date"))
    if got_iso != want_iso:
        return None, f"要 {want_iso} 但證交所回傳 {got_iso}"
    table = next((t for t in (data.get("tables") or []) if "漲跌證券數合計" in str(t.get("title"))), None)
    fields = (table or {}).get("fields") or []
    if table is None or "類型" not in fields or "股票" not in fields:
        return None, "找不到『漲跌證券數合計』表格或欄位不符"
    i_label, i_stock = fields.index("類型"), fields.index("股票")
    got = {}
    for r in table.get("data") or []:
        label = str(r[i_label])
        for prefix, name in (("上漲", "up"), ("下跌", "down"), ("持平", "flat")):
            if label.startswith(prefix):
                got[name] = parse_count(r[i_stock])[0]
    if any(got.get(k) is None for k in ("up", "down", "flat")):
        return None, "上漲／下跌／持平其中一項缺漏或無法解析"
    lo, hi = config.BREADTH_TOTAL_RANGE
    if not (lo <= got["up"] + got["down"] + got["flat"] <= hi):
        return None, f"家數合計 {got['up'] + got['down'] + got['flat']} 超出合理範圍"
    return (got["up"], got["down"], got["flat"]), ""


def tw_breadth_long_backfill(max_days=None):
    """模組 M46：上市漲跌家數往前回補到 INDEX_LONG_YEARS 年前（給「高檔但下跌家數 ≥ 60%」的歷史驗證用；自建行情 M13 只有近 400 天）。
    交易日以加權指數歷史（market_stats）為準，扣掉 tw_breadth_hist 已有的；每次最多 BREADTH_LONG_BATCH 天、BREADTH_LONG_MINUTES 分鐘，由新到舊補。
    每一天：回傳日期要等於要求的日期、三項都讀得到、合計合理；連線失敗整批停止，某天沒資料／驗證不過只略過那天。
    口徑是證交所官方「股票」欄；M13 自建的是 4 位數代號股票，兩者同一天可能差幾檔，所以已有 M13 的日期不覆蓋。"""
    max_days = max_days or config.BREADTH_LONG_BATCH
    if not sb_ready():
        log("breadth_long", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=int(config.INDEX_LONG_YEARS * 365))
    days, have = [], set()
    for off in range(0, 3000, 1000):
        got, err = sb_request("GET", f"market_stats?select=as_of&source=eq.TWSE%20FMTQIK&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000&offset={off}")
        if got is None:
            log("breadth_long", "failure", detail=_clip(f"讀取交易日清單失敗：{err}"))
            return []
        days.extend(r["as_of"] for r in got)
        if len(got) < 1000:
            break
    for off in range(0, 3000, 1000):
        got, err = sb_request("GET", f"tw_breadth_hist?select=as_of&market=eq.TWSE&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000&offset={off}")
        if got is None:
            log("breadth_long", "failure", detail=_clip(f"讀取已有的漲跌家數失敗：{err}（請先執行 new_tables_v38.sql）"))
            return []
        have.update(r["as_of"] for r in got)
        if len(got) < 1000:
            break
    todo_all = [d for d in days if d not in have]
    if not todo_all:
        log("breadth_long", "no_new_data", detail=f"{len(days)} 個交易日的漲跌家數都已經補齊")
        return []
    out, skipped, bad, first_err, tried, t0 = [], 0, 0, "", 0, time.time()
    for d in todo_all[:max_days]:
        if time.time() - t0 > config.BREADTH_LONG_MINUTES * 60:
            break
        tried += 1
        data, err = fetch_json(config.TWSE_MI_INDEX_MS + "&date=" + d.replace("-", ""))
        time.sleep(config.HIST_SLEEP)
        if data is None:
            log("breadth_long", "failure", as_of=d, detail=_clip(f"抓取失敗：{err}；已停止本次回補，不繼續打請求"))
            break
        res, why = parse_breadth_ms(data, d)
        if res is None:
            if "沒有這天" in why:
                skipped += 1
            else:
                bad += 1
                first_err = first_err or f"{d}：{why}"
            continue
        out.append({"as_of": d, "market": "TWSE", "n_up": res[0], "n_down": res[1], "n_flat": res[2]})
    log("breadth_long", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次試 {tried} 天，補 {len(out)} 天" + (f"（{out[-1]['as_of']} 到 {out[0]['as_of']}）" if out else "") + f"；略過 {skipped} 天（沒有資料）、{bad} 天（驗證不過" + (f"：{first_err}" if first_err else "") + f"）；還有 {max(len(todo_all) - tried, 0)} 天下次繼續"))
    return out


def tw_margin_ratio_backfill(max_days=None):
    """模組 M47：回補上市融資維持率估算的歷史。做法和每天的 M21 完全一樣（逐檔融資餘額 × 當天收盤價 ÷ 融資金額），
    只是改成指定日期：逐檔融資用 MI_MARGN（selectType=ALL＋date），收盤價用自建行情 tw_hist 同一天的全部上市股票。
    先驗證證交所真的給歷史：回傳日期不是要求的那天、或沒有逐檔表，就整批停止並記下原因（不寫任何數字）。
    只補自建行情有完整紀錄的上市交易日（約近 400 天）；每次最多 RATIO_BACKFILL_BATCH 天。每天的檢查同 M21：收盤價查不到的融資股票超過 5%、估算值不在 80–400% 就略過。"""
    max_days = max_days or config.RATIO_BACKFILL_BATCH
    if not sb_ready():
        log("ratio_backfill", "skipped", detail="未設定 Supabase，無法判斷哪些日期已經補過，不回補")
        return []
    start = date.today() - timedelta(days=config.HIST_WINDOW_DAYS)
    days, err = sb_request("GET", f"tw_hist_days?select=as_of&market=eq.TWSE&status=eq.ok&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000")
    have_rows, err2 = sb_request("GET", f"tw_margin_ratio?select=as_of&as_of=gte.{start.isoformat()}&limit=2000")
    if days is None or have_rows is None:
        log("ratio_backfill", "failure", detail=_clip(f"讀取交易日或已有的估算失敗：{err or err2}"))
        return []
    have = {r["as_of"] for r in have_rows}
    todo_all = [r["as_of"] for r in days if r["as_of"] not in have]
    if not todo_all:
        log("ratio_backfill", "no_new_data", detail=f"近 {config.HIST_WINDOW_DAYS} 天 {len(days)} 個交易日的維持率估算都已經補齊")
        return []
    out, skipped, bad, first_err, tried = [], 0, 0, "", 0
    t0 = time.time()
    for d in todo_all[:max_days]:
        if time.time() - t0 > config.RATIO_BACKFILL_MINUTES * 60:
            break
        tried += 1
        data, err = fetch_json(config.TWSE_MI_MARGN_ALL + "&date=" + d.replace("-", ""))
        time.sleep(config.HIST_SLEEP)
        if data is None:
            log("ratio_backfill", "failure", as_of=d, detail=_clip(f"抓取失敗：{err}；已停止本次回補"))
            break
        if not isinstance(data, dict) or data.get("stat") != "OK":
            skipped += 1
            continue
        if twse_ymd_to_iso(data.get("date")) != d:
            log("ratio_backfill", "no_new_data", as_of=d, detail=_clip(f"證交所逐檔融資不提供歷史：要 {d} 但回傳 {twse_ymd_to_iso(data.get('date'))}。維持率估算只能從啟用日起每天累積，沒有寫入任何數字"))
            return out
        units, perr = parse_margin_stock_table(data)
        total, lerr = _parse_margin(data)
        if perr or lerr:
            bad += 1
            first_err = first_err or f"{d}：{perr or lerr}"
            continue
        close, off = {}, 0
        while off < 3000:
            got, e3 = sb_request("GET", f"tw_hist?select=code,close&market=eq.TWSE&as_of=eq.{d}&order=code.asc&limit=1000&offset={off}")
            if got is None:
                log("ratio_backfill", "failure", as_of=d, detail=_clip(f"讀取 {d} 的收盤價失敗：{e3}"))
                return out
            for r in got:
                try:
                    if float(r["close"]) > 0:
                        close[str(r["code"]).strip()] = float(r["close"])
                except (KeyError, TypeError, ValueError):
                    continue
            if len(got) < 1000:
                break
            off += 1000
        value, missing = 0.0, 0
        for code, u in units.items():
            if code in close:
                value += u * 1000 * close[code]
            else:
                missing += 1
        if not units or missing / len(units) > 0.05 or not total.get("margin_balance"):
            bad += 1
            miss_codes = sorted((c_ for c_ in units if c_ not in close), key=lambda c_: -units[c_])[:12]
            first_err = first_err or f"{d}：{len(units)} 檔裡 {missing} 檔查不到收盤價（或融資金額缺）；自建行情當天有 {len(close)} 檔；缺價的前幾檔（融資張數多的先列）：" + "、".join(f"{c_}({int(units[c_])}張)" for c_ in miss_codes) + f"；融資金額 {total.get('margin_balance')}"
            continue
        ratio = value / total["margin_balance"] * 100
        if not (80 <= ratio <= 400):
            bad += 1
            first_err = first_err or f"{d}：估算值 {ratio:.1f}% 不在 80–400%"
            continue
        out.append({"as_of": d, "est_ratio": round(ratio, 2), "margin_value": round(value), "margin_loan": round(total["margin_balance"]), "n_stocks": len(units) - missing, "n_missing": missing,
                    "source": "自行估算（回補：證交所逐檔融資餘額 × 自建行情收盤價 ÷ 融資金額；不是官方整戶擔保維持率）"})
    log("ratio_backfill", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"本次試 {tried} 天，補 {len(out)} 天" + f"；略過 {skipped} 天（沒有資料）、{bad} 天（驗證不過" + (f"：{first_err}" if first_err else "") + f"）；還有 {max(len(todo_all) - tried, 0)} 天下次繼續"))
    return out


def parse_aaii_csv(text):
    """AAII 完整歷史（sentiment.xls 另存成 CSV）→ ([{as_of, bullish, neutral, bearish}] 新到舊, 略過幾列)。
    要有標題列（含 Date、Bullish、Neutral、Bearish；其他欄位忽略）。日期接受 2026-11-11、2026/11/11、11/11/2026、Nov 11, 2026；
    比例可以是 0.38（Excel 存成小數）或 38 / 38%；三項合計要在 98–102（%）才收，否則略過那一列。日期重複取第一列。"""
    import csv as _csv, io as _io
    rows = list(_csv.reader(_io.StringIO(text.lstrip("\ufeff"))))
    hi = next((i for i, r in enumerate(rows) if {"date", "bullish", "neutral", "bearish"} <= {c.strip().lower() for c in r}), None)
    if hi is None:
        return [], 0
    head = [c.strip().lower() for c in rows[hi]]
    ix = {k: head.index(k) for k in ("date", "bullish", "neutral", "bearish")}
    out, seen, skipped = [], set(), 0
    for r in rows[hi + 1:]:
        if len(r) <= max(ix.values()) or not any(c.strip() for c in r):
            continue
        t, d_ = r[ix["date"]].strip(), None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%m/%d/%y"):
            try:
                d_ = datetime.strptime(t.split(" ")[0] if " " in t and fmt in ("%Y-%m-%d", "%Y/%m/%d") else t, fmt).date()
                break
            except ValueError:
                continue
        vals = [parse_number(r[ix[k]].replace("%", "")) for k in ("bullish", "neutral", "bearish")]
        if d_ is None or None in vals:
            skipped += 1
            continue
        if max(vals) <= 1.0001:
            vals = [v * 100 for v in vals]
        if not (98 <= sum(vals) <= 102) or d_ > date.today() or d_.isoformat() in seen:
            skipped += 1
            continue
        seen.add(d_.isoformat())
        out.append({"as_of": d_.isoformat(), "bullish": round(vals[0], 2), "neutral": round(vals[1], 2), "bearish": round(vals[2], 2), "source": "AAII 完整歷史（你自己下載的 sentiment 檔，匯入）"})
    out.sort(key=lambda x: x["as_of"], reverse=True)
    return out, skipped


def aaii_xls_to_csv(body):
    """AAII 官方的完整歷史 sentiment.xls（舊版 Excel）→ CSV 文字（交給 parse_aaii_csv 驗證）。要有 xlrd 套件（工作流程會安裝）。
    在每個工作表前 30 列找含 Date、Bullish、Neutral、Bearish 的標題列；日期格是 Excel 日期就換成 YYYY-MM-DD。回傳 (csv 文字, 原因)。"""
    try:
        import xlrd
    except ImportError:
        return None, "沒有安裝 xlrd 套件，讀不了 .xls"
    import csv as _csv, io as _io
    try:
        book = xlrd.open_workbook(file_contents=body)
    except Exception as e:                                                        # 檔案壞掉、或其實不是 Excel（例如被擋的網頁）
        return None, f"打不開這個 Excel 檔：{str(e)[:80]}"
    for sh in book.sheets():
        for hr in range(min(30, sh.nrows)):
            head = [str(c.value).strip().lower() for c in sh.row(hr)]
            if {"date", "bullish", "neutral", "bearish"} <= set(head):
                buf = _io.StringIO()
                w = _csv.writer(buf)
                w.writerow(head)
                for r in range(hr + 1, sh.nrows):
                    out = []
                    for c in sh.row(r):
                        if c.ctype == xlrd.XL_CELL_DATE:
                            try:
                                y, m, d, *_ = xlrd.xldate_as_tuple(c.value, book.datemode)
                                out.append(f"{y:04d}-{m:02d}-{d:02d}")
                            except Exception:
                                out.append("")
                        else:
                            out.append(str(c.value))
                    w.writerow(out)
                return buf.getvalue(), ""
    return None, "找不到含 Date、Bullish、Neutral、Bearish 的標題列"


def aaii_csv_import():
    """模組 M48：AAII 完整歷史。先從 AAII 官方網址下載 sentiment.xls（本來就公開，不用會員）；下載或解析失敗，才改讀倉庫裡你放的 data/aaii_sentiment.csv。
    兩邊都沒有就什麼都不做（不算錯）。每一列都用 parse_aaii_csv 驗證（三項合計 98–102、日期合理、不重複），寫入的主鍵是日期，重跑不會重複。"""
    text, notes = None, []
    st, ct, body, err = fetch_raw(config.AAII_XLS_URL, timeout=60, headers={"User-Agent": config.NASDAQ_UA, "Accept": "*/*", "Referer": "https://www.aaii.com/sentimentsurvey/sent_results"})
    if st == 200 and body and body[:4] == b"\xd0\xcf\x11\xe0":
        text, why = aaii_xls_to_csv(body)
        if text is None:
            notes.append(f"下載到了但解析失敗（{why}）")
    else:
        notes.append(f"下載 sentiment.xls 失敗：HTTP {st} {err or ct or ''}".strip() + ("（不是 Excel 檔）" if st == 200 else ""))
    src = "AAII 官方 sentiment.xls（下載）"
    if text is None:
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "aaii_sentiment.csv")
        if not os.path.exists(path):
            log("aaii_csv", "no_new_data", detail=_clip("；".join(notes) + "；倉庫裡也沒有 data/aaii_sentiment.csv：只靠每週的頁面累積"))
            return []
        try:
            with open(path, encoding="utf-8-sig", errors="replace") as fh:
                text = fh.read()
            src = "你放在倉庫的 data/aaii_sentiment.csv"
        except OSError as e:
            log("aaii_csv", "failure", detail=_clip(f"讀取失敗：{e}"))
            return []
    rows, skipped = parse_aaii_csv(text)
    log("aaii_csv", "success" if rows else "failure", as_of=rows[0]["as_of"] if rows else None,
        detail=_clip(f"來源：{src}；讀到 {len(rows)} 週" + (f"（{rows[-1]['as_of']} 到 {rows[0]['as_of']}）" if rows else "：找不到需要的欄位，或全部驗證不過") + f"；略過 {skipped} 列" + ("；" + "；".join(notes) if notes else "")))
    return rows


def _decode_tw_csv(body):
    for enc in ("utf-8-sig", "cp950"):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def parse_taifex_hist_rows(inv_text, fut_text):
    """期交所歷史下載（CSV）→ 轉成和 OpenAPI 同樣格式的列，讓 compute_retail_ratio 直接用。欄位用標題找，不用位置：
    法人檔要有「日期、商品名稱、身份別」與「多方…未平倉…口數」「空方…未平倉…口數」；行情檔要有「交易日期、契約、到期月份、未沖銷契約數、交易時段」。
    欄位對不上回傳 (None, None, 原因)。"""
    import csv as _csv, io as _io
    def table(text):
        rows = [[c.strip() for c in r] for r in _csv.reader(_io.StringIO(text.lstrip("\ufeff"))) if any(c.strip() for c in r)]
        return (rows[0], rows[1:]) if rows else ([], [])
    h1, r1 = table(inv_text)
    def col(h, *need, bad=("金額", "淨額")):
        for i, c in enumerate(h):
            if all(n in c for n in need) and not any(b in c for b in bad):
                return i
        return None
    i_d, i_n, i_i = col(h1, "日期"), col(h1, "商品名稱"), col(h1, "身份別")
    i_l, i_s = col(h1, "多方", "未平倉", "口數"), col(h1, "空方", "未平倉", "口數")
    if None in (i_d, i_n, i_i, i_l, i_s):
        return None, None, f"法人檔欄位不符：{h1[:12]}"
    inv = [{"Date": re.sub(r"\D", "", r[i_d]), "ContractCode": r[i_n].replace("台", "臺"), "Item": r[i_i], "OpenInterest(Long)": r[i_l].replace(",", ""), "OpenInterest(Short)": r[i_s].replace(",", "")} for r in r1 if len(r) > max(i_d, i_n, i_i, i_l, i_s)]
    h2, r2 = table(fut_text)
    j_d, j_c, j_m, j_o, j_s = col(h2, "交易日期"), col(h2, "契約"), col(h2, "到期月份"), col(h2, "未沖銷"), col(h2, "交易時段")
    if None in (j_d, j_c, j_m, j_o):
        return None, None, f"行情檔欄位不符：{h2[:12]}"
    fut = [{"Date": re.sub(r"\D", "", r[j_d]), "Contract": r[j_c], "ContractMonth(Week)": r[j_m], "TradingSession": (r[j_s] if j_s is not None and len(r) > j_s else ""), "OpenInterest": r[j_o].replace(",", "")} for r in r2 if len(r) > max(j_d, j_c, j_m, j_o)]
    return inv, fut, ""


def tw_retail_ratio_backfill():
    """模組 M49：小台指散戶多空比往前回補（期交所網站的歷史下載，CSV，一次一個月）。
    每一天都交給和每日更新同一個 compute_retail_ratio 驗證（法人三個身份都要有、日期對得上、數字合理）。
    歷史下載抓不到、欄位對不上就整批停止並把原因寫進紀錄，不寫任何數字。每次最多補 RR_BACKFILL_MONTHS 個月，由新到舊；已經有的日期不重補。"""
    if not sb_ready():
        log("rr_backfill", "skipped", detail="未設定 Supabase，不回補")
        return []
    have_rows, err = sb_request("GET", "tw_retail_ratio?select=as_of&order=as_of.desc&limit=2000")
    if have_rows is None:
        log("rr_backfill", "failure", detail=_clip(f"讀取已有的散戶多空比失敗：{err}（請先執行 new_tables_v55.sql）"))
        return []
    have = {r["as_of"] for r in have_rows}
    oldest = date.today() - timedelta(days=config.HIST_WINDOW_DAYS)
    out, first_err, months_done, end = [], "", 0, date.today()
    while months_done < config.RR_BACKFILL_MONTHS and end >= oldest:
        start = max(end - timedelta(days=30), oldest)
        qs = f"queryStartDate={start.strftime('%Y/%m/%d')}&queryEndDate={end.strftime('%Y/%m/%d')}"
        hdr = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Referer": "https://www.taifex.com.tw/cht/3/futContractsDate"}
        form = {"queryStartDate": start.strftime("%Y/%m/%d"), "queryEndDate": end.strftime("%Y/%m/%d")}
        s1, c1, b1, e1 = fetch_post_raw("https://www.taifex.com.tw/cht/3/futContractsDateDown", dict(form, commodityId="MXF"), timeout=config.OPTIONS_TIMEOUT, headers=hdr)
        time.sleep(config.HIST_SLEEP)
        s2, c2, b2, e2 = fetch_post_raw("https://www.taifex.com.tw/cht/3/dlFutDailyMarketData", dict(form, commodity_id="MTX", down_type="1"), timeout=config.OPTIONS_TIMEOUT, headers=dict(hdr, Referer="https://www.taifex.com.tw/cht/3/dailyFuturesMarketReport"))
        time.sleep(config.HIST_SLEEP)
        if s1 != 200 or s2 != 200 or not b1 or not b2:
            first_err = f"歷史下載失敗：法人 HTTP {s1} {e1 or ''}；行情 HTTP {s2} {e2 or ''}".strip()
            break
        inv, fut, why = parse_taifex_hist_rows(_decode_tw_csv(b1), _decode_tw_csv(b2))
        if inv is None:
            first_err = why
            if (b1 or b"")[:20].lstrip().lower().startswith((b"<!doctype", b"<html")):
                first_err += "｜法人下載回傳網頁：" + _html_hint(b1)
            if (b2 or b"")[:20].lstrip().lower().startswith((b"<!doctype", b"<html")):
                first_err += "｜行情下載回傳網頁：" + _html_hint(b2)
            break
        for d in sorted({r["Date"] for r in inv if len(r["Date"]) == 8}, reverse=True):
            iso = twse_ymd_to_iso(d)
            if iso in have:
                continue
            row, w = compute_retail_ratio([r for r in inv if r["Date"] == d], [r for r in fut if r["Date"] == d])
            if row is not None:
                out.append(row)
        months_done += 1
        end = start - timedelta(days=1)
    log("rr_backfill", "success" if out else "no_new_data", as_of=out[0]["as_of"] if out else None,
        detail=_clip(f"補了 {len(out)} 天（{months_done} 個月份）" + (f"；停止原因：{first_err}" if first_err else "") + ("" if out or first_err else "；沒有新的日期")))
    return out


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


def _dash_keywords_hit(text):
    return [k for k in config.DASHBOARD_KEYWORDS if k in text]


def probe_dashboard():
    """只讀探測：官方「臺股儀表板」（2026/9/23 上線，有全市場擔保維持率）有沒有程式可以讀的資料來源。
    1) 證交所與櫃買的開放資料說明文件（swagger）裡，有沒有含「維持率／儀表板／信用交易…」的端點；
    2) 證交所首頁有沒有通往儀表板的連結，並看那一頁和它的程式檔裡出現哪些像是資料網址的字串。
    只記錄結構與找到的網址，不寫任何市場資料表，也不把探測到的數字當成正式資料。任何一步失敗都只記一筆紀錄。"""
    for name, url in (("twse", config.TWSE_SWAGGER), ("tpex", config.TPEX_SWAGGER)):
        data, err = fetch_swagger(url)
        if data is None:
            log(f"probe:dashboard_swagger_{name}", "failure", detail=f"抓取失敗: {err}")
            continue
        paths = data.get("paths") or {}
        hits = []
        for path, methods in paths.items():
            for m in (methods or {}).values():
                if isinstance(m, dict):
                    text = f"{m.get('summary', '')} {m.get('description', '')}"
                    found = _dash_keywords_hit(text) or _dash_keywords_hit(path)
                    if found:
                        hits.append(f"{path} | {m.get('summary', '')}")
        log(f"probe:dashboard_swagger_{name}", "success",
            detail=_clip(f"共 {len(paths)} 個端點；含『{'／'.join(config.DASHBOARD_KEYWORDS)}』字樣的：" + (" ; ".join(hits[:12]) if hits else "無")))

    links, seen = [], set()
    for home in config.DASHBOARD_HOME_PAGES:
        status, ctype, body, err = fetch_raw(home, timeout=config.PROBE_TIMEOUT, headers=DASH_HEADERS)
        if status is None or status != 200:
            log("probe:dashboard_home", "no_new_data", detail=_clip(f"{home}：HTTP {status}；{err}"))
            continue
        html = body.decode("utf-8", errors="replace")
        for m in re.finditer(r"<a\b[^>]*?href=[\"']([^\"']+)[\"'][^>]*>(.{0,80}?)</a>", html, re.S | re.I):
            href, txt = m.group(1).strip(), re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()
            if ("儀表板" in txt or re.search(r"dashboard|board", href, re.I)) and not href.startswith(("javascript", "#", "mailto")):
                absu = urllib.parse.urljoin(home, href)
                if absu not in seen:
                    seen.add(absu)
                    links.append((absu, txt))
        log("probe:dashboard_home", "success", detail=_clip(f"{home}：HTTP {status}，{len(body)} 位元組；找到 {len(links)} 個疑似儀表板連結：" + " ; ".join(f"{t or '（無文字）'} → {u}" for u, t in links[:6])))
        if links:
            break
    if not links:
        log("probe:dashboard_pages", "no_new_data", detail="首頁沒有找到通往儀表板的連結（可能是動態產生的，需要人工把儀表板網址貼過來再探測）")
        return
    api_re = re.compile(r"[\"'`]((?:https?:)?//[^\"'`\s]{6,140}|/[A-Za-z0-9_./?=&%-]{4,120})[\"'`]")

    def inspect_page(url, label):
        """看一頁：標題、程式檔、像資料網址的字串；很短的頁面（轉址頁）連原文一起記。回傳頁面裡指向同站 /dashboard 的子頁網址。"""
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=DASH_HEADERS)
        if status is None or status != 200:
            log(f"probe:dashboard_{label}", "no_new_data", detail=_clip(f"{url}：HTTP {status}；{err}"))
            return []
        html = body.decode("utf-8", errors="replace")
        collect(html)
        inline = [re.sub(r"\s+", " ", s.strip())[:300] for s in re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.S | re.I) if re.search(r"api|base|url|host", s, re.I)]
        title = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
        scripts = [urllib.parse.urljoin(url, s) for s in re.findall(r"<script[^>]+src=[\"']([^\"']+)[\"']", html, re.I)]
        frames = [urllib.parse.urljoin(url, s) for s in re.findall(r"<(?:iframe|frame)[^>]+src=[\"']([^\"']+)[\"']", html, re.I)]
        refresh = re.findall(r"<meta[^>]+http-equiv=[\"']?refresh[\"']?[^>]+content=[\"'][^\"']*url=([^\"';>]+)", html, re.I)
        jump = re.findall(r"(?:location\.(?:href|replace)|window\.open)\s*[=(]\s*[\"']([^\"']+)[\"']", html)
        found = sorted({u for u in api_re.findall(html) if re.search(r"\.(json|csv)\b|/rwd/|/api/|/v1/|dashboard|margin|credit", u, re.I)})
        extra = ""
        if len(body) < 1200:
            extra = "｜頁面原文：" + re.sub(r"\s+", " ", html)[:400]
        log(f"probe:dashboard_{label}", "success", detail=_clip(f"{url}｜標題 {(title.group(1).strip() if title else '（無）')[:60]}｜{status} {ctype}｜{len(body)} 位元組｜程式檔 {len(scripts)} 個：" + " ; ".join(scripts[:6]) + "｜框架 " + (" ; ".join(frames[:3]) or "無") + "｜轉址 " + (" ; ".join(refresh[:2] + jump[:2]) or "無") + "｜像資料網址的字串：" + (" ; ".join(found[:10]) if found else "無") + ("｜內嵌程式（含 api/url 字樣）：" + " ## ".join(inline[:3]) if inline else "") + extra))
        slug = urllib.parse.urlparse(url).path.rsplit("/", 2)
        slug = (slug[-2] + "-" + slug[-1].rsplit(".", 1)[0]) if len(slug) >= 2 else ""                                  # credit/margin.html → credit-margin
        mine = [s for s in scripts if urllib.parse.urlparse(s).netloc.endswith(("twse.com.tw", "tpex.org.tw")) and "/lib/" not in s]     # 函式庫（highcharts 之類）不用看
        mine.sort(key=lambda s: 0 if slug and slug in s else 1)                                                         # 頁面專屬的程式檔（credit-margin.js）排最前面
        for js in mine[: config.DASHBOARD_MAX_SCRIPTS]:
            st, ct, jb, er = fetch_raw(js, timeout=config.PROBE_TIMEOUT, headers=DASH_HEADERS)
            if st is None or st != 200:
                log("probe:dashboard_script", "no_new_data", detail=_clip(f"{js}：HTTP {st}；{er}"))
                continue
            text = jb[:2_000_000].decode("utf-8", errors="replace")
            collect(text)
            hits = sorted({u for u in api_re.findall(text) if re.search(r"\.(json|csv)\b|/rwd/|/api/|/v1/|dashboard|margin|credit|maint", u, re.I)})
            kw = [k for k in config.DASHBOARD_KEYWORDS if k in text]
            calls = [re.sub(r"\s+", " ", text[max(0, m.start() - 40): m.end() + 110]) for m in re.finditer(r"fetch\(|getJSON|\$\.get\(|\$\.ajax|axios|XMLHttpRequest|\.open\(\s*[\"']GET|\.json[\"']", text)][:5]
            log("probe:dashboard_script", "success", detail=_clip(f"{js}｜{len(jb)} 位元組｜含關鍵字 {'、'.join(kw) if kw else '無'}｜像資料網址的字串：" + (" ; ".join(hits[:14]) if hits else "無") + "｜讀資料的呼叫附近：" + (" ## ".join(calls) if calls else "無")))
        subs = []
        for u in found + [urllib.parse.urlparse(x).path for x in frames + refresh + jump]:
            absu = urllib.parse.urljoin(url, u)
            p = urllib.parse.urlparse(absu)
            if p.netloc.endswith("twse.com.tw") and p.path.startswith("/dashboard") and re.search(r"\.(html?|aspx?)$|/$", p.path) and absu != url and absu not in subs:
                subs.append(absu)
        return subs

    consts, endpoints = {}, []
    const_re = re.compile(r"(?<![\w$])([A-Za-z_$][\w$]*)\s*[:=]\s*[\"'`]((?:https?:)?//[^\"'`\s]{4,120}|/[A-Za-z0-9_./-]{2,80})[\"'`]")
    ep_re = re.compile(r"[\"'`](/[A-Za-z][A-Za-z0-9_]*/[A-Za-z0-9_./-]+(?:\?[^\"'`\s]*)?)[\"'`]")

    defs = []                                                                                                       # 名稱含 API／BASE／HOST 的常數「定義那一行」原文（右邊可能是運算式，字串比對抓不到）
    def_re = re.compile(r"(?<![\w$.])((?:const|let|var)\s+)?([A-Za-z_$][\w$]*(?:API|Api|BASE|Base|HOST|Host|ORIGIN|Origin)[\w$]*)\s*=\s*([^;\n]{1,140})")

    snips = []                                                                                                      # 指定名稱（TPEX_API、TWSE_API、ApiPath）後面 260 個字原文：壓縮過的程式碼會把好幾個定義黏成一長串，上面那種只取 140 字會被截斷
    snip_re = re.compile(r"(?:TPEX_API|TWSE_API|ApiPath\s*=|new\s+ApiPath)[^\n]{0,150}")
    cls_re = re.compile(r"(?:class|function)\s+ApiPath[^\n]{0,520}")                                               # ApiPath 這個類別的完整定義（它決定網址怎麼組；上一版和其他片段擠在一起被截斷）
    apicls = []
    assign_re = re.compile(r"(?:TPEX_API|TWSE_API)\s*=(?!=)[^\n]{0,240}")                                       # 「賦值」那一段（用到的地方很多，會把名額用完，所以另外抓）

    def collect(text):
        for m in cls_re.finditer(text):
            if len(apicls) < 2 and m.group(0) not in apicls:
                apicls.append(m.group(0))
        for m in assign_re.finditer(text):
            if len(snips) < 8 and m.group(0) not in snips:
                snips.insert(0, m.group(0))
        for m in snip_re.finditer(text):
            if len(snips) < 8 and m.group(0) not in snips:
                snips.append(m.group(0))
        for m in def_re.finditer(text):
            d = f"{m.group(2)} = {m.group(3).strip()}"
            if d not in defs and len(defs) < 14 and not re.search(r"\.(css|png|svg)", d):
                defs.append(d)
        for m in const_re.finditer(text):
            if re.search(r"api|base|host|url|domain|origin", m.group(1), re.I):
                consts.setdefault(m.group(1), m.group(2))
        for m in ep_re.finditer(text):
            p = m.group(1)
            if "/lib/" in p or p.startswith("/dashboard/") or re.search(r"\.(css|js|html?|png|jpe?g|svg|ico|woff2?|map)(\?|$)", p, re.I):
                continue
            if p not in endpoints:
                endpoints.append(p)

    visited = set()
    for url, txt in links[: config.DASHBOARD_MAX_PAGES]:
        visited.add(url)
        for sub in inspect_page(url, "page")[: config.DASHBOARD_MAX_SUBPAGES]:                                      # 再往下一層：頁面裡指向 /dashboard 的子頁（例如信用交易頁）
            if sub not in visited:
                visited.add(sub)
                inspect_page(sub, "subpage")
    if consts or endpoints:
        probe_dashboard_data(consts, endpoints, defs, snips, apicls)


def _shape(j, depth=0):
    """把 JSON 的結構與各欄位的樣本值壓成一行文字（只記結構與第一筆的值，方便判斷哪個欄位是什麼）。"""
    if isinstance(j, dict):
        if depth >= 4:
            return "{…}"
        return "{" + ", ".join(f"{k}: {_shape(v, depth + 1)}" for k, v in list(j.items())[:14]) + "}"
    if isinstance(j, list):
        if depth >= 4:
            return f"[{len(j)} 筆]"
        return f"[{len(j)} 筆；第一筆 " + (_shape(j[0], depth + 1) if j else "空") + "]"
    return repr(j)[:40]


def probe_dashboard_data(consts, endpoints, defs=None, snips=None, apicls=None):
    """只讀探測：儀表板頁面的程式讀的資料端點，真的抓一次，記回傳的結構（有沒有擔保維持率、有幾天的歷史）。"""
    log("probe:dashboard_consts", "success", detail=_clip("API 基底網址常數：" + (" ; ".join(f"{k} = {v}" for k, v in list(consts.items())[:12]) or "沒找到") + f"｜端點 {len(endpoints)} 個：" + " ; ".join(endpoints[:14])))
    log("probe:dashboard_defs", "success", detail=_clip("名稱含 API／BASE／HOST 的定義（原文）：" + (" ## ".join(defs or []) or "沒找到")))
    log("probe:dashboard_apipath", "success", detail=_clip("ApiPath 類別的完整定義：" + (" ## ".join(apicls or []) or "沒找到"), 1200))
    log("probe:dashboard_snips", "success", detail=_clip("TPEX_API／TWSE_API／ApiPath 後面的原文：" + (" ## ".join(snips or []) or "沒找到"), 1800))
    bases = [v for v in consts.values() if v.startswith(("http://", "https://", "//"))]
    for ep in endpoints[: config.DASHBOARD_MAX_ENDPOINTS]:
        want = "tpex|otc" if ep.startswith("/dashboardOtc") else "twse|tse"
        pick = [v for k, v in consts.items() if re.search(want, k, re.I) and v.startswith(("http", "//"))] or bases
        if not pick and not ep.startswith("/dashboardOtc"):
            pick = [config.TWSE_RWD_BASE.rsplit("/", 1)[0] + "/" + seg if seg else config.TWSE_RWD_BASE.rsplit("/", 1)[0] for seg in ("zh", "any", "en", "")]       # 頁面程式 new ApiPath("/rwd", lan)：語言段可能是 zh、any、en 或沒有，依序試；網址後面再加 ?response=json
        if not pick:
            log("probe:dashboard_api", "no_new_data", detail=_clip(f"{ep}：找不到 API 基底網址，沒抓"))
            continue
        for base in pick[:4]:
            base = ("https:" + base) if base.startswith("//") else base
            url = base.rstrip("/") + ep + ("?response=json" if base.startswith(config.TWSE_RWD_BASE.rsplit("/", 1)[0]) and "?" not in ep else "")             # 只有用 /rwd/zh 這個基底時才補（頁面程式就是這樣組的）
            st, ct, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=dict(DASH_HEADERS, **{"Referer": "https://www.twse.com.tw/dashboard/zh/credit/margin.html", "Accept": "application/json, text/plain, */*"}))
            if st != 200:
                log("probe:dashboard_api", "no_new_data", detail=_clip(f"{url}：HTTP {st}；{err}"))
                continue
            try:
                j = json.loads(body.decode("utf-8", errors="replace"))
            except ValueError:
                log("probe:dashboard_api", "no_new_data", detail=_clip(f"{url}：HTTP 200 但不是 JSON：{body[:80]!r}"))
                continue
            log("probe:dashboard_api", "success", detail=_clip(f"{url}｜{len(body)} 位元組｜結構 {_shape(j)}"))
            break


def probe_fin_coverage():
    """只讀探測：你的持股與候選股裡，哪些代號沒有三率？官方的各個財報資料集（上市、上櫃、各業別）和 FinMind 有沒有它們？
    先從資料庫算出「沒有三率的代號」（持股的價格表與候選股，減去 tw_fin_q 與 tw_fin_q_ext 已有的），再逐一查：
    1) 證交所與櫃買開放資料裡每一個財報／損益表資料集：筆數、欄位、你的缺三率代號出現在哪幾個；
    2) 這些代號的產業（tw_industry）；3) 最多 6 檔去問 FinMind，記它回傳哪些損益項目名稱。只記錄，不寫任何資料表。"""
    if not sb_ready():
        log("probe:fin_missing", "skipped", detail="未設定 Supabase，無法找出沒有三率的代號")
        return
    px, e1 = sb_request("GET", "stock_prices?select=code,name&market=eq.TW&limit=500")
    wl, e2 = sb_request("GET", "private_watchlist?select=code&limit=200")
    q1, e3 = sb_request("GET", "tw_fin_q?select=code&limit=2000")
    q2, e4 = sb_request("GET", "tw_fin_q_ext?select=code&limit=3000")
    if px is None or q1 is None or q2 is None:
        log("probe:fin_missing", "failure", detail=_clip(f"讀取資料庫失敗：{e1 or e3 or e4}"))
        return
    names = {str(r.get("code", "")).strip(): r.get("name") or "" for r in px if isinstance(r, dict)}
    for r in (wl or []):
        c = str(r.get("code", "")).strip() if isinstance(r, dict) else ""
        if c:
            names.setdefault(c, "（候選股）")
    have = {str(r.get("code", "")).strip() for r in (q1 or []) + (q2 or []) if isinstance(r, dict)}
    missing = sorted(c for c in names if c and c not in have)
    ind, _e5 = sb_request("GET", "tw_industry?select=code,industry&limit=3000")
    indmap = {str(r.get("code", "")).strip(): r.get("industry") for r in (ind or []) if isinstance(r, dict)}
    log("probe:fin_missing", "success", detail=_clip(f"持股與候選股共 {len(names)} 檔，已有三率 {len(names) - len(missing)} 檔，沒有 {len(missing)} 檔：" + "；".join(f"{c} {names[c]}（{indmap.get(c) or '產業查不到'}）" for c in missing[:25])))
    if not missing:
        return
    seen_paths = []
    for tag, swag_url, base in (("twse", config.TWSE_OPENAPI_SWAGGER, config.TWSE_OPENAPI_BASE), ("tpex", config.TPEX_SWAGGER, config.TPEX_DEFAULT_BASE)):
        swag, err = fetch_swagger(swag_url)
        if not isinstance(swag, dict):
            log(f"probe:fin_dataset_{tag}", "failure", detail=_clip(f"說明文件抓取失敗：{err}"))
            continue
        cands = []
        for pth, methods in (swag.get("paths") or {}).items():
            for m in (methods or {}).values():
                s = str(m.get("summary", "")) if isinstance(m, dict) else ""
                if re.search(r"財報|綜合損益|損益表", s):
                    cands.append((pth, s))
        log(f"probe:fin_dataset_{tag}", "success", detail=_clip(f"財報／損益表相關資料集 {len(cands)} 個：" + " ; ".join(f"{p} {s}" for p, s in cands[:12])))
        for pth, s in cands[: config.FIN_PROBE_MAX_DATASETS]:
            rows, e = fetch_json(_openapi_base(swag, base) + pth)
            dicts = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
            if not dicts:
                log(f"probe:fin_dataset_{tag}", "no_new_data", detail=_clip(f"{pth} {s}：沒有資料或抓取失敗（{e or ''}）"))
                continue
            kc = next((k for k in dicts[0] if "代號" in k or k.lower() in ("code", "companycode", "securitiescompanycode")), None)
            codes = {str(r.get(kc, "")).strip() for r in dicts} if kc else set()
            hit = [c for c in missing if c in codes]
            log(f"probe:fin_dataset_{tag}", "success", detail=_clip(f"{pth} {s}｜{len(dicts)} 筆｜欄位 {' / '.join(list(dicts[0])[:14])}｜缺三率的代號出現在這裡：{'、'.join(hit) if hit else '無'}"))
            seen_paths.append((pth, set(hit)))
    found_any = set().union(*[h for _p, h in seen_paths]) if seen_paths else set()
    log("probe:fin_missing_summary", "success", detail=_clip("官方財報資料集裡完全找不到的代號：" + ("、".join(c for c in missing if c not in found_any) or "無（每一檔至少出現在一個資料集）")))
    for code in missing[: config.FIN_PROBE_FINMIND]:
        j, err = fetch_json(f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id={code}&start_date={date.today().year - 1}-01-01")
        if not isinstance(j, dict) or j.get("status") != 200 or not isinstance(j.get("data"), list):
            log("probe:fin_finmind", "no_new_data", detail=_clip(f"{code} {names.get(code, '')}：FinMind 沒給資料（{err or str(j)[:60]}）"))
            continue
        types = Counter(str(r.get("type", "")) for r in j["data"] if isinstance(r, dict))
        log("probe:fin_finmind", "success", detail=_clip(f"{code} {names.get(code, '')}（{indmap.get(code) or '產業查不到'}）：FinMind 共 {len(j['data'])} 列；損益項目名稱：" + "、".join(t for t, _n in types.most_common(14))))
        time.sleep(config.FINMIND_SLEEP)


def probe_nasdaq_access():
    """只讀診斷：排程打 Nasdaq 回 HTTP 403（美股價格、SPY、QQQ、暴跌名單全部失敗）。
    1) 記下這台機器的對外 IP；2) 同一個網址用幾組不同標頭各試一次，記狀態與回應開頭（看是不是被防護機制擋掉）；
    3) 試兩個備援來源有沒有回應：Yahoo Finance 日線、Stooq 日線（CSV）。只記錄，不寫任何資料表，也不把這些資料拿去用。"""
    ua = config.NASDAQ_UA
    st, ct, body, err = fetch_raw("https://api.ipify.org?format=json", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "risk-pwa/1.0"})
    ip = body.decode("utf-8", errors="replace")[:60] if st == 200 else f"查不到（HTTP {st} {err}）"
    log("probe:nasdaq_access_ip", "success" if st == 200 else "no_new_data", detail=_clip(f"這台排程機器的對外 IP：{ip}"))
    url = config.NASDAQ_QUOTE_URL.format(symbol="SPY", assetclass="etf")
    chrome = {"User-Agent": ua, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate", "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/market-activity/etf/spy",
              "Connection": "keep-alive", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-site", "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"', "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"'}
    variants = (("目前使用的標頭", NASDAQ_HEADERS), ("完整瀏覽器標頭（含 Sec-Fetch、sec-ch-ua）", chrome), ("只有 User-Agent", {"User-Agent": ua}), ("curl 風格", {"User-Agent": "curl/8.5.0", "Accept": "*/*"}))
    for name, hdr in variants:
        st, ct, body, err = _fetch_raw_once(url, timeout=config.PROBE_TIMEOUT, headers=hdr)               # 診斷要看每一組標頭的真實結果：不走重試與冷卻
        head = body[:110].decode("utf-8", errors="replace").replace("\n", " ") if body else ""
        log("probe:nasdaq_access_hdr", "success" if st == 200 else "no_new_data", detail=_clip(f"{name}：HTTP {st}｜{ct}｜{len(body or b'')} 位元組｜開頭 {head!r}｜{err or ''}"))
        time.sleep(1)
    st, ct, body, err = fetch_raw("https://query1.finance.yahoo.com/v8/finance/chart/SPY?range=5d&interval=1d", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"})
    pts = None
    if st == 200:
        try:
            pts = parse_yahoo_chart(json.loads(body.decode("utf-8-sig")))
        except (ValueError, UnicodeDecodeError):
            pts = None
    log("probe:nasdaq_access_yahoo", "success" if pts else "no_new_data", detail=_clip(f"Yahoo SPY 日線：HTTP {st}｜{(str(len(pts)) + ' 筆，最新 ' + str(pts[-1])) if pts else '沒有解析出資料'}｜{err or ''}"))
    st, ct, body, err = fetch_raw("https://stooq.com/q/d/l/?s=spy.us&i=d", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)"})
    lines = body.decode("utf-8", errors="replace").strip().splitlines() if st == 200 and body else []
    log("probe:nasdaq_access_stooq", "success" if len(lines) > 2 else "no_new_data", detail=_clip(f"Stooq SPY 日線（CSV）：HTTP {st}｜{len(lines)} 行｜表頭 {lines[0] if lines else '無'}｜最新 {lines[-1] if lines else '無'}｜{err or ''}"))


def _obj_shape(obj, n=2):
    """把一包 JSON 描述成短短一段文字（筆數、欄位、前幾筆），給探測紀錄用。"""
    if isinstance(obj, list):
        recs = [r for r in obj if isinstance(r, dict)]
        return f"{len(obj)} 筆；欄位 {list(recs[0].keys())[:12] if recs else '（不是物件）'}；前 {n} 筆 {json.dumps(recs[:n], ensure_ascii=False)[:420]}"
    if isinstance(obj, dict):
        return f"頂層鍵 {list(obj.keys())[:12]}；{json.dumps(obj, ensure_ascii=False)[:420]}"
    return f"不是預期的格式（{type(obj).__name__}）"


def _pe_keys(obj, path="", out=None, depth=0):
    """遞迴找出 JSON 裡名稱像本益比／EPS 的欄位（欄位名稱含 pe、eps、earnings、forward），回傳 [(路徑, 值)]。"""
    if out is None:
        out = []
    if depth > 7 or len(out) >= 40:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            if isinstance(v, (dict, list)):
                _pe_keys(v, p, out, depth + 1)
            elif re.search(r"(^|[^a-z])(pe|p/e|eps)([^a-z]|$)|trailingpe|forwardpe|peratio|earningspershare|price.?earn", str(k), re.I) and v not in (None, "", "N/A"):
                out.append((p, v))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:6]):
            _pe_keys(v, f"{path}[{i}]", out, depth + 1)
    return out


def probe_us_pe():
    """只讀探測（不寫任何資料表）：美股本益比（過去四季、預估）有沒有別的來源。Nasdaq 的個股摘要確定沒有（已驗證），
    這裡試 Yahoo 報價／個股統計、Nasdaq 的 info／earnings-forecast／financials 端點，每個只記「HTTP 狀態＋名稱像本益比或 EPS 的欄位與值」，
    取不到就記 no_new_data，不丟例外、不記失敗。"""
    syms = [s for s in (load_us_watch() or [])][:2] or []
    for s in ("ORCL", "ARM"):
        if s not in syms and len(syms) < 3:
            syms.append(s)
    urls = []
    for s in syms:
        urls += [
            (f"yahoo_quote:{s}", f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={s}", None),
            (f"yahoo_summary:{s}", f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{s}?modules=summaryDetail,defaultKeyStatistics,financialData", None),
            (f"nasdaq_info:{s}", f"https://api.nasdaq.com/api/quote/{s}/info?assetclass=stocks", NASDAQ_HEADERS),
            (f"nasdaq_forecast:{s}", f"https://api.nasdaq.com/api/analyst/{s}/earnings-forecast", NASDAQ_HEADERS),
            (f"nasdaq_fin:{s}", f"https://api.nasdaq.com/api/company/{s}/financials?frequency=2", NASDAQ_HEADERS),
        ]
    for name, url, hdr in urls:
        try:
            st, ct, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=hdr or {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36", "Accept": "application/json,*/*"})
        except Exception as e:                                                                                  # noqa: BLE001
            st, ct, body, err = None, "", b"", str(e)
        time.sleep(1)
        if st != 200 or not body:
            log(f"probe:us_pe:{name}", "no_new_data", detail=_clip(f"HTTP {st} {err or ''}".strip()))
            continue
        try:
            obj = json.loads(body.decode("utf-8", errors="replace"))
        except ValueError:
            log(f"probe:us_pe:{name}", "no_new_data", detail=_clip(f"HTTP 200 但不是 JSON：{body[:120]!r}"))
            continue
        hits = _pe_keys(obj)
        log(f"probe:us_pe:{name}", "success", detail=_clip(f"HTTP 200｜名稱像本益比／EPS 的欄位 {len(hits)} 個：" + "；".join(f"{p}={str(v)[:40]}" for p, v in hits[:14]) if hits else "HTTP 200｜沒有名稱像本益比／EPS 的欄位", 700))



def probe_vix_otc():
    """只讀探測（不寫任何資料表）：1) 期交所網站的「臺指選擇權波動率指數（TAIWAN VIX）」每日收盤頁與下載檔；
    2) 櫃買指數：櫃買中心開放資料裡有沒有櫃買指數的歷史、Yahoo 的 ^TWOII 能不能用。
    用途：台指波動率（OpenAPI 沒有，但期交所網站有公布）和「個股對大盤（上市＝加權、上櫃＝櫃買指數）」的比較都需要先知道實際格式。每一項失敗都只記錄。"""
    hdr = dict(DASH_HEADERS)
    for name, url in (("vix_daily3m", "https://www.taifex.com.tw/cht/7/vixDaily3MNew"), ("vix_month", "https://www.taifex.com.tw/cht/7/vixMinNew")):
        st, ct, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=hdr)
        time.sleep(1)
        if st != 200 or not body:
            log(f"probe:{name}", "no_new_data", detail=_clip(f"{url}：HTTP {st} {err or ''}".strip()))
            continue
        html = body.decode("utf-8", errors="replace")
        links = []
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html):
            h = m.group(1)
            if re.search(r"vix|csv|zip|download|Dailydownload|\.xls", h, re.I) and h not in links and not h.startswith(("javascript", "#")):
                links.append(h)
        rows = len(re.findall(r"<tr", html))
        log(f"probe:{name}", "success", detail=_clip(f"{url}｜{len(body)} 位元組｜有「臺指選擇權波動率指數」字樣 {('臺指選擇權波動率指數' in html)}｜<tr> {rows} 列｜可能的資料連結 {len(links)} 個：" + " ## ".join(links[:14]), 1400))
        trs = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", x)).strip() for x in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I)]
        trs = [x for x in trs if x][:14]
        js_urls = []
        for m in re.finditer(r"[\"'](/[^\"'\s<>]*(?:vix|Vix|VIX)[^\"'\s<>]*)[\"']", html):                      # 程式碼裡寫死的資料網址（表格的值可能是 JavaScript 另外載入）
            if m.group(1) not in js_urls and m.group(1) not in links:
                js_urls.append(m.group(1))
        log(f"probe:{name}_table", "success", detail=_clip(f"表格各列的文字（前 {len(trs)} 列）：" + " ## ".join(trs) + f"｜程式碼裡的其他網址 {len(js_urls)} 個：" + " ## ".join(js_urls[:10]), 1500))
        data_links = [l_ for l_ in links if re.search(r"\.(csv|zip|txt|xls)|download", l_, re.I)][:2]
        for l_ in data_links:
            full = urllib.parse.urljoin(url, l_)
            st2, ct2, b2, e2 = fetch_raw(full, timeout=config.PROBE_TIMEOUT, headers=hdr)
            time.sleep(1)
            if st2 == 200 and b2:
                try:
                    sample = b2[:600].decode("utf-8")
                except UnicodeDecodeError:
                    sample = b2[:600].decode("big5", errors="replace")                                           # 期交所的檔案常是 Big5
                log(f"probe:{name}_file", "success", detail=_clip(f"{full}｜{ct2}｜{len(b2)} 位元組｜開頭 {sample!r}", 900))
            else:
                log(f"probe:{name}_file", "no_new_data", detail=_clip(f"{full}：HTTP {st2} {e2 or ''}".strip()))
    sw, serr = fetch_swagger(config.TPEX_SWAGGER)
    cands = []
    for path, methods in ((sw or {}).get("paths") or {}).items():
        for m in (methods or {}).values():
            if isinstance(m, dict):
                text = f"{m.get('summary', '')} {m.get('description', '')}"
                if "指數" in text and ("櫃買" in text or "OTC" in text.upper()) and ("收盤" in text or "歷史" in text or "加權" in text):
                    cands.append((path, str(m.get("summary", ""))[:50]))
    if sw is None:
        log("probe:otc_swagger", "no_new_data", detail=_clip(f"櫃買中心說明文件抓不到：{serr}"))
    else:
        log("probe:otc_swagger", "success", detail=_clip(f"{len((sw or {}).get('paths') or {})} 個端點；含「櫃買指數收盤／歷史」字樣的 {len(cands)} 個：" + " ## ".join(f"{p}｜{s}" for p, s in cands[:15]), 1400))
        for path, _s in [c for c in cands if "櫃買指數" in c[1] or "櫃買指數" in c[0]][:2] or cands[:1]:
            obj, e3 = fetch_json(config.TPEX_DEFAULT_BASE + path, headers=hdr)
            time.sleep(1)
            log("probe:otc_endpoint", "success" if obj else "no_new_data", detail=_clip(f"{path}：" + (_obj_shape(obj) if obj else f"抓不到（{e3}）"), 900))
    st4, ct4, b4, e4 = fetch_raw("https://query1.finance.yahoo.com/v8/finance/chart/%5ETWOII?range=3mo&interval=1d", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"})
    pts = None
    if st4 == 200 and b4:
        try:
            pts = parse_yahoo_chart(json.loads(b4.decode("utf-8-sig")))
        except (ValueError, UnicodeDecodeError):
            pts = None
    log("probe:otc_yahoo", "success" if pts else "no_new_data", detail=_clip(f"Yahoo ^TWOII（櫃買指數）日線：HTTP {st4}｜" + (f"{len(pts)} 筆，最近 3 筆 {pts[-3:]}" if pts else f"沒有解析出資料 {e4 or ''}")))


def probe_chips():
    """只讀診斷：候選股比較想加的「籌碼」資料有沒有、長什麼樣子（不寫任何資料表）。
    1) 證交所三大法人買賣超日報 T86（逐檔外資、投信）；2) 逐檔融資表的欄位（有沒有融資限額）；
    3) 集保戶股權分散表（TDCC 開放資料，400 張以上大戶持股比）；4) 證交所處置股票公告。
    每一項記：HTTP 狀態、欄位名稱、筆數、以及 3037／3189／8046 這幾檔的那一列（沒有就記前兩列）。"""
    hdr = dict(DASH_HEADERS)
    pick = ("3037", "3189", "8046")

    def show(rows):
        hit = [r for r in rows if isinstance(r, (list, tuple)) and r and str(r[0]).strip() in pick] or [r for r in rows[:2]]
        return " ## ".join(str(r)[:260] for r in hit[:3])
    t86_done = False
    for back in range(0, 7):                                                                                          # 往回找最近一個有資料的日子
        d_ = date.today() - timedelta(days=back)
        if d_.weekday() >= 5:
            continue
        st, ct, body, err = fetch_raw(f"https://www.twse.com.tw/rwd/zh/fund/T86?date={d_.strftime('%Y%m%d')}&selectType=ALLBUT0999&response=json", timeout=config.PROBE_TIMEOUT, headers=hdr)
        time.sleep(1)
        try:
            j = json.loads(body.decode("utf-8-sig")) if st == 200 and body else None
        except (ValueError, UnicodeDecodeError):
            j = None
        if isinstance(j, dict) and j.get("stat") == "OK" and j.get("data"):
            log("probe:chips_t86", "success", as_of=d_.isoformat(), detail=_clip(f"日期 {d_}｜{len(j['data'])} 檔｜欄位 {j.get('fields')}｜{show(j['data'])}", 1500))
            t86_done = True
            break
    if not t86_done:
        log("probe:chips_t86", "no_new_data", detail="往回 7 天都查不到 T86（三大法人買賣超日報）")
    data, err = fetch_json(config.TWSE_MI_MARGN_ALL, headers=hdr)
    tabs = [t for t in ((data or {}).get("tables") or [])] if isinstance(data, dict) else []
    big = max([t for t in tabs if t.get("data")], key=lambda t: len(t["data"]), default=None)
    if big:
        log("probe:chips_margin", "success", detail=_clip(f"日期 {data.get('date')}｜{len(big['data'])} 檔｜欄位 {big.get('fields')}｜{show(big['data'])}", 1500))
    else:
        log("probe:chips_margin", "no_new_data", detail=_clip(f"逐檔融資表沒有資料：{err or '格式非預期'}"))
    st, ct, body, err = fetch_raw("https://opendata.tdcc.com.tw/getOD.ashx?id=1-5", timeout=config.PROBE_TIMEOUT, headers=hdr)
    time.sleep(1)
    if st == 200 and body:
        lines = body.decode("utf-8-sig", errors="replace").splitlines()
        mine = [l for l in lines[1:] if l.split(",")[1:2] and l.split(",")[1].strip() in pick]
        dates = sorted({l.split(",")[0] for l in lines[1:300]})[:3]
        log("probe:chips_tdcc", "success" if len(lines) > 100 else "no_new_data", detail=_clip(f"{len(lines)} 行｜表頭 {lines[0] if lines else ''}｜資料日期 {dates}｜3037／3189／8046 共 {len(mine)} 行：{' ## '.join(mine[:8])}", 1500))
    else:
        log("probe:chips_tdcc", "no_new_data", detail=_clip(f"集保開放資料：HTTP {st} {err or ''}"))
    st, ct, body, err = fetch_raw("https://www.twse.com.tw/rwd/zh/announcement/punish?response=json", timeout=config.PROBE_TIMEOUT, headers=hdr)
    try:
        j = json.loads(body.decode("utf-8-sig")) if st == 200 and body else None
    except (ValueError, UnicodeDecodeError):
        j = None
    if isinstance(j, dict) and j.get("data") is not None:
        log("probe:chips_punish", "success", detail=_clip(f"{len(j['data'])} 筆｜欄位 {j.get('fields')}｜前兩筆 {[str(r)[:300] for r in j['data'][:2]]}", 1500))
    else:
        log("probe:chips_punish", "no_new_data", detail=_clip(f"處置股票公告：HTTP {st} {err or ''}｜開頭 {(body or b'')[:100]!r}"))


def probe_extra():
    """只讀探測：外資買賣超(BFI82U)、融資餘額(MI_MARGN)、期交所外資期貨部位的真實格式。
    只把「結構與前幾列範例」寫進紀錄，不寫任何市場資料表。
    迴圈裡的各個端點抓不到、或格式不是預期的，本身就是診斷資訊，記成「沒有新資料」，不讓整個 tw_probe 變紅（以前記成失敗，證交所偶爾回 307 就讓整個工作變紅）。"""
    for name, url in config.PROBE_URLS.items():
        data, err = fetch_json(url)
        if data is None:
            log(f"probe:{name}", "no_new_data", detail=f"抓取失敗: {err}")
            continue
        if not isinstance(data, dict):
            log(f"probe:{name}", "no_new_data", detail=_clip(f"非預期格式: {type(data).__name__}"))
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

    data, err = fetch_swagger(config.TAIFEX_SWAGGER)
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
    sw, err = fetch_swagger(config.TAIFEX_SWAGGER)
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


def load_us_watch():
    """讀你的美股候選股清單（private_watchlist_us），只回傳代號（大寫、格式正確、不重複）。
    這些代號不是持股：只拿來抓收盤價、52 週高低、下次財報日和產業。資料表還不存在＝還沒啟用這個功能，記一筆，不算壞掉。"""
    if not sb_ready():
        return []
    got, err = sb_request("GET", "private_watchlist_us?select=symbol&limit=500")
    if got is None:
        if "PGRST205" in str(err) or "HTTP 404" in str(err):
            log("watch_us", "no_new_data", detail=_clip("美股候選股清單的資料表還不存在（還沒執行 new_tables_v67.sql）：沒有要用美股候選股功能的話可以忽略；要用，請執行 new_tables_v67.sql，再重新跑一次。"))
        else:
            log("watch_us", "failure", detail=_clip(f"讀取美股候選股清單失敗：{err}（如果是資料表不存在，請先執行 new_tables_v67.sql）"))
        return []
    syms = sorted({s for s in (str(r.get("symbol") or "").strip().upper() for r in got if isinstance(r, dict)) if re.fullmatch(r"[A-Z][A-Z.\-]{0,5}", s)})        # 空值要先換成空字串（否則 None 會變成 "NONE"，剛好符合代號格式）
    log("watch_us", "success" if syms else "no_new_data", detail=_clip(f"美股候選股 {len(syms)} 檔：" + ("、".join(syms[:20]) or "（清單是空的）")))
    return syms


def with_us_watch(hold, syms):
    """把美股候選股接在持股清單後面（已經是持股的不重複），格式和 load_holdings 一樣；只給「抓美股價格、財報日、產業」的函式用，持股清單本身不動。"""
    held = {str(h.get("symbol") or h.get("pos_key") or "").upper().strip() for h in (hold or []) if h.get("market") == "US"}
    extra, seen = [], set(held)
    for s in (syms or []):
        u = str(s or "").upper().strip()
        if u and u not in seen:                                  # 已經是持股、或清單裡重複的，不再加一次（大小寫不分）
            seen.add(u)
            extra.append({"market": "US", "symbol": u, "name": u, "kind": "stock", "pos_key": u})
    return list(hold or []) + extra


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
    sw1, err = fetch_swagger(config.TWSE_OPENAPI_SWAGGER)
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
    sw, err = fetch_swagger(config.TPEX_SWAGGER)
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
        official_name = str(got.get("companyName") or "").strip() or None      # 官方的完整名稱（貼上的名稱常被券商畫面截斷）；沒有就退回貼上的名稱
        out.append(_price_row("US", sym, official_name or h.get("name"), iso, close, change, "Nasdaq 個股報價（已收盤）", high52, low52, range_src))
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
    done = split = 0
    for d in todo:
        if dry_run:
            log("tw_breadth", "skipped", as_of=d, detail="--dry-run：不計算")
            continue
        res, err = sb_request("POST", "rpc/compute_tw_breadth", {"d": d})
        if res is None and "57014" in str(err):                                           # 資料庫語句逾時：改成上市、上櫃各算一次（每次工作量約一半）
            res, err2 = 0, None
            for mk in ("TWSE", "TPEX"):
                r2, e2 = sb_request("POST", "rpc/compute_tw_breadth", {"d": d, "p_market": mk})
                if r2 is None:
                    res, err2 = None, f"{mk} 也失敗：{e2}"
                    break
            if res is None:
                err = f"{err}；改成上市、上櫃分開算：{err2}"
            else:
                split += 1
        if res is None:
            hint = "請執行 new_tables_v57.sql（改寫函式並放寬逾時）" if "57014" in str(err) else "請先執行 new_tables_breadth.sql"
            log("tw_breadth", "failure", as_of=d, detail=_clip(f"呼叫 compute_tw_breadth 失敗：{err}（{hint}）"))
            return None
        done += 1
    if done:
        log("tw_breadth", "success", as_of=todo[0], detail=f"已計算 {done} 個日期（最新 {latest}，歷史涵蓋 {cover} 天）" + (f"；其中 {split} 個因為一次算逾時，改成上市、上櫃分開算（建議執行 new_tables_v57.sql）" if split else ""))
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


def _cot_rows(rows, market_name, label):
    """CFTC TFF 的列 → us_cot 的列（和標普那個模組同樣的驗證：市場名稱、日期、口數都要合理）。回傳 (列, 未通過的列數)。"""
    keys = {"open_interest": "open_interest_all", "dealer_long": "dealer_positions_long_all", "dealer_short": "dealer_positions_short_all",
            "asset_mgr_long": "asset_mgr_positions_long", "asset_mgr_short": "asset_mgr_positions_short",
            "lev_long": "lev_money_positions_long", "lev_short": "lev_money_positions_short"}
    out, bad = [], 0
    for r in rows:
        if not isinstance(r, dict) or r.get("market_and_exchange_names") != market_name:
            bad += 1
            continue
        d = str(r.get("report_date_as_yyyy_mm_dd", ""))[:10]
        ok, _why = validate_close_date(d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else None)
        vals = {k: parse_number(r.get(src)) for k, src in keys.items()}
        if (not ok or None in vals.values() or vals["open_interest"] <= 0 or any(v < 0 for v in vals.values())
                or any(vals[k] > vals["open_interest"] for k in ("dealer_long", "dealer_short", "asset_mgr_long", "asset_mgr_short", "lev_long", "lev_short"))):
            bad += 1
            continue
        out.append(dict(vals, report_date=d, market=label, source="CFTC TFF 期貨 only（publicreporting.cftc.gov）"))
    out.sort(key=lambda r: r["report_date"], reverse=True)
    return out, bad


def cftc_cot_ndx():
    """模組 M33：CFTC COT（TFF 期貨 only）Nasdaq-100 期貨最近兩週的三類交易人多空口數，存進 us_cot（market 欄位區分，不和標普混在一起）。
    先找「Nasdaq-100 合併合約」，沒有就找 E-mini；兩個都沒有時，列出 CFTC 資料裡含 NASDAQ 的市場名稱，方便對照。每週才更新一次，報告日是週二、週五才公布。"""
    notes = []
    for name, label in config.CFTC_NDX_MARKETS:
        q = urllib.parse.urlencode({"$limit": 2, "$order": "report_date_as_yyyy_mm_dd DESC", "market_and_exchange_names": name}, quote_via=urllib.parse.quote)
        rows, err = fetch_json(config.CFTC_TFF_BASE + "?" + q)
        if rows is None:
            notes.append(f"{label}：抓取失敗 {err}")
            continue
        if not isinstance(rows, list) or not rows:
            notes.append(f"{label}：CFTC 沒有這個市場名稱的資料")
            continue
        out, bad = _cot_rows(rows, name, label)
        if out:
            n = lambda r, k: r[k + "_long"] - r[k + "_short"]
            log("cftc:cot_ndx", "success", as_of=out[0]["report_date"], detail=f"{label}：報告日 {out[0]['report_date']}，槓桿基金淨 {n(out[0], 'lev'):+,.0f}、資產管理機構淨 {n(out[0], 'asset_mgr'):+,.0f}、交易商淨 {n(out[0], 'dealer'):+,.0f}（口）" + (f"；{bad} 列未通過驗證" if bad else ""))
            return out
        notes.append(f"{label}：{len(rows)} 列都沒通過驗證")
    q2 = urllib.parse.urlencode({"$select": "market_and_exchange_names", "$where": "market_and_exchange_names like '%NASDAQ%'", "$group": "market_and_exchange_names", "$limit": 12}, quote_via=urllib.parse.quote)
    names, _e = fetch_json(config.CFTC_TFF_BASE + "?" + q2)
    found = [str(x.get("market_and_exchange_names")) for x in names if isinstance(x, dict)] if isinstance(names, list) else []
    log("cftc:cot_ndx", "no_new_data", detail=_clip("；".join(notes) + f"｜CFTC 資料裡含 NASDAQ 的市場名稱：{found or '查不到'}"))
    return []


def probe_round16():
    """只讀探測第十六批：Yahoo 的 NQ=F、ES=F 期貨報價有沒有 openInterest（未平倉量）欄位。有的話才值得自己每天存一筆累積；沒有或被擋就算了。不寫入任何資料表。"""
    hdr = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "application/json"}
    for tag, url in (("yahoo_quote_oi", "https://query1.finance.yahoo.com/v7/finance/quote?symbols=NQ%3DF,ES%3DF"), ("yahoo_chart_meta", "https://query1.finance.yahoo.com/v8/finance/chart/NQ%3DF?range=5d&interval=1d")):
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=hdr)
        time.sleep(1)
        if status is None:
            log(f"probe:{tag}", "no_new_data", detail=_clip(f"連線失敗: {err}"))
            continue
        txt = (body or b"").decode("utf-8", errors="replace")
        extra = ""
        try:
            j = json.loads(txt)
            if tag == "yahoo_quote_oi":
                res = (((j.get("quoteResponse") or {}).get("result")) or [])
                extra = "｜" + "；".join(f"{r.get('symbol')}：openInterest={r.get('openInterest', '沒有這個欄位')}、欄位數 {len(r)}" for r in res if isinstance(r, dict)) if res else "｜result 是空的"
            else:
                meta = ((((j.get("chart") or {}).get("result") or [{}])[0]).get("meta")) or {}
                extra = f"｜meta 欄位：{sorted(meta.keys())[:24]}｜openInterest 在 meta 裡：{'openInterest' in meta}"
        except (ValueError, AttributeError, IndexError, TypeError):
            extra = "｜不是 JSON"
        log(f"probe:{tag}", "success" if status == 200 else "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜{len(body or b'')} 位元組｜開頭 {re.sub(chr(10), ' ', txt[:140])!r}{extra}"))


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
    start = date.today() - timedelta(days=config.PC_WINDOW_DAYS)
    days, err = [], None
    for off in range(0, 3000, 1000):                                                              # 一頁最多 1000 筆，10 年要分頁
        got, err = sb_request("GET", f"macro_series?select=as_of&series_id=eq.SP500&as_of=gte.{start.isoformat()}&order=as_of.desc&limit=1000&offset={off}")
        if got is None:
            days = None
            break
        days.extend(got)
        if len(got) < 1000:
            break
    if not days:
        log("pc_backfill", "failure" if days is None else "no_new_data", detail=_clip(f"讀不到美股交易日清單（FRED 標普 500 歷史）：{err or '沒有資料'}；請先跑 tw_backfill 補 FRED 歷史"))
        return []
    have = set()
    for off in range(0, 3000, 1000):
        have_rows, err = sb_request("GET", f"us_putcall?select=as_of&as_of=gte.{start.isoformat()}&limit=1000&offset={off}")
        have.update(r["as_of"] for r in (have_rows or []))
        if not have_rows or len(have_rows) < 1000:
            break
    todo_all = [r["as_of"] for r in days if r["as_of"] not in have]
    todo = todo_all[:max_days]
    if not todo:
        log("pc_backfill", "no_new_data", detail=f"近 {config.PC_WINDOW_DAYS} 天的 {len(days)} 個美股交易日，Put/Call 都已經補齊")
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


def parse_cboe_totalpc_csv(text):
    """CBOE 舊版 Put/Call 歷史檔（totalpc.csv）→ [{as_of, total_pc}]。檔案前面有說明文字，從含「Date」與「P/C Ratio」的標題列開始讀；
    日期可以是 m/d/yyyy 或 yyyy-mm-dd；比率要在 0–20 內；讀不出來的列略過，不編。"""
    rows, hdr, di, ri = [], None, None, None
    for line in str(text).splitlines():
        cells = [c.strip().strip('"') for c in line.split(",")]
        low = [c.lower() for c in cells]
        if hdr is None:
            if "date" in low and any("p/c" in c for c in low):
                hdr, di = True, low.index("date")
                ri = next(i for i, c in enumerate(low) if "p/c" in c)
            continue
        if len(cells) <= max(di, ri):
            continue
        raw = cells[di]
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
        try:
            d_ = date(int(m.group(3)), int(m.group(1)), int(m.group(2))) if m else date.fromisoformat(raw[:10])
        except ValueError:
            continue
        v = parse_number(cells[ri])
        if v is None or not (0 < v < 20):
            continue
        rows.append({"as_of": d_.isoformat(), "total_pc": v})
    return rows


def cboe_pc_csv_history():
    """模組 M44：CBOE 舊版 Put/Call 歷史檔（totalpc.csv，只有總比率）補進 us_putcall 還沒有的日期，讓美股谷底卡的歷史驗證有更早的資料。
    每日 JSON 的資料（有指數、股票、ETP 比率）永遠優先：資料庫已有的日期不動。抓不到或格式不符只記錄（附前幾行，方便看真實格式），不讓工作變紅。"""
    status, ctype, body, err = fetch_raw(config.CBOE_PC_URLS["totalpc_csv"], timeout=config.PROBE_TIMEOUT)
    if status != 200 or not body:
        log("cboe_pc_csv", "no_new_data", detail=_clip(f"totalpc.csv 取得失敗：{'HTTP ' + str(status) if status else err}"))
        return []
    try:
        text = body.decode("utf-8-sig", errors="replace")
    except AttributeError:
        text = str(body)
    rows = parse_cboe_totalpc_csv(text)
    if not rows:
        log("cboe_pc_csv", "no_new_data", detail=_clip("totalpc.csv 讀不出資料；前幾行：" + " | ".join(text.splitlines()[:4])))
        return []
    have = set()
    if sb_ready():
        for off in range(0, 6000, 1000):
            got, _e = sb_request("GET", f"us_putcall?select=as_of&limit=1000&offset={off}")
            have.update(r["as_of"] for r in (got or []))
            if not got or len(got) < 1000:
                break
    out = [{"as_of": r["as_of"], "total_pc": r["total_pc"], "index_pc": None, "equity_pc": None, "etp_pc": None, "source": "CBOE 舊版歷史檔（totalpc.csv，只有總比率）"}
           for r in rows if r["as_of"] not in have and r["as_of"] <= date.today().isoformat()]
    log("cboe_pc_csv", "success" if out else "no_new_data", as_of=max((r["as_of"] for r in out), default=None),
        detail=_clip(f"檔案 {len(rows)} 天（{min(r['as_of'] for r in rows)} 到 {max(r['as_of'] for r in rows)}），新補 {len(out)} 天"))
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
        sw, err = fetch_swagger(sw_url)
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


def _nasdaq_rows(sym, assetclass="etf", days=75, end=None):
    """Nasdaq 歷史行情 → ([{date, close, volume}] 由舊到新, "")；失敗回傳 (None, 原因)。收盤價超出合理範圍的列丟掉；成交量讀不出來就是 None（不編）。"""
    today = end or date.today()
    url = config.NASDAQ_HIST_URL.format(symbol=sym, assetclass=assetclass, fromdate=(today - timedelta(days=days)).isoformat(), todate=today.isoformat())
    status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        return None, f"連線失敗: {err}"
    try:
        rows = (((json.loads(body.decode("utf-8-sig").lstrip("\ufeff")) or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
    except (ValueError, UnicodeDecodeError, AttributeError):
        return None, "不是 JSON"
    out = []
    for r in rows:
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", str(r.get("date", "")).strip())
        c = parse_number(str(r.get("close", "")).replace("$", ""))
        if not m or c is None or not (0 < c < config.PRICE_LIMIT):
            continue
        try:
            d_ = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            continue
        out.append({"date": d_, "close": c, "volume": parse_number(str(r.get("volume", "")))})
    out.sort(key=lambda x: x["date"])
    return out, ""


def _nasdaq_history(sym, assetclass="etf", days=75):
    """Nasdaq 歷史行情（預設約 60 天，ETF）→ [(日期, 收盤)] 由舊到新；失敗回傳 (None, 原因)。"""
    rows, why = _nasdaq_rows(sym, assetclass, days)
    if rows is None:
        return None, why
    return [(r["date"], r["close"]) for r in rows], ""


_LONG_TRACE = []


def _nasdaq_history_long(sym, assetclass="stocks", days=3700, chunk=540):
    """Nasdaq 一次最多回 400 筆，所以每 chunk 個日曆天（約 370 個交易日）分段往前抓，湊成長歷史。
    第一段失敗就回傳 (None, 原因)；後面的段落失敗就用已經抓到的，不讓整檔作廢。"""
    out, end, got, why, trace = {}, date.today(), 0, "", []
    while got < days:
        span = min(chunk, days - got)
        rows, why = _nasdaq_rows(sym, assetclass, span, end=end)
        time.sleep(config.BREADTH_NDX_SLEEP)
        trace.append(f"{(end - timedelta(days=span)).isoformat()}~{end.isoformat()}：" + (f"{len(rows)} 筆" if rows is not None else f"失敗 {why}"))
        if not _LONG_TRACE:
            _LONG_TRACE.append(trace)                                        # 只留第一檔的分段結果，寫進紀錄方便看哪一段沒回資料
        if rows is None:
            if not out:
                return None, why
            break
        for r in rows:
            out[r["date"]] = r["close"]
        got += span + 1
        end = end - timedelta(days=span + 1)
    return sorted(out.items()), ""


def us_daily_bars():
    """模組 M34：美股 ETF（目前只有 SPY）近約 130 天的日線收盤價與成交量，給「量價成本（估算）」用。
    只收「已經收盤」的日子（美東 16:30 以前，今天那一筆是盤中的、成交量還不完整，不收）；成交量要在合理範圍；至少 60 天有成交量；最新一天不能超過 7 天前。
    抓不到或不夠只記「沒有資料」，不讓工作變紅。"""
    out, notes = [], []
    lo, hi = config.US_VOL_RANGE
    for sym in config.US_BAR_SYMBOLS:
        rows, why = _nasdaq_rows(sym, "etf", config.US_BAR_DAYS)
        if rows is None:
            notes.append(f"{sym}：{why}")
            continue
        now_et = _now_et()
        last_ok = now_et.date() if (now_et.hour, now_et.minute) >= (16, 30) else now_et.date() - timedelta(days=1)
        good = [r for r in rows if r["volume"] is not None and lo <= r["volume"] <= hi and r["date"] <= last_ok]
        if len(good) < 60:
            notes.append(f"{sym}：只有 {len(good)} 天有可用的成交量（至少 60 天）")
            continue
        if (last_ok - good[-1]["date"]).days > 7:
            notes.append(f"{sym}：最新一天是 {good[-1]['date']}，太舊，不用")
            continue
        out += [{"symbol": sym, "as_of": r["date"].isoformat(), "close": r["close"], "volume": r["volume"], "source": "Nasdaq 歷史行情（日線，含成交量）"} for r in good]
    if out:
        log("us_daily_bars", "success", as_of=max(r["as_of"] for r in out), detail=f"{len(out)} 筆（{', '.join(sorted({r['symbol'] for r in out}))}），最新 {max(r['as_of'] for r in out)}" + (f"；{'；'.join(notes)}" if notes else ""))
    else:
        log("us_daily_bars", "no_new_data", detail=_clip("；".join(notes) or "沒有資料"))
    return out


def sox_history():
    """模組 M42：費城半導體指數（SOX）的每日收盤歷史（Nasdaq 官方行情，index 分類）→ macro_series（series_id=SOX），給「SOX 從高點回落」用。
    每次：近 SOX_WINDOW_DAYS 天（每天更新）＋從資料庫最早的一天再往前補最多 SOX_BACK_WINDOWS 段（每段 SOX_WINDOW_DAYS 天），最多補到 SOX_YEARS 年前。
    只收美東已經收盤的日子；價格要在合理範圍；Nasdaq 一次最多回 400 列，所以分段。抓不到只記「沒有資料」，不讓工作變紅；不用 ETF 代替（SOXX 有分割，會讓回落算錯）。
    主鍵是 series_id,as_of，重跑不會重複。"""
    sid, label, low, high = config.SOX_SERIES
    now = _now_et()
    earliest = None
    if sb_ready():
        got, _err = sb_request("GET", f"macro_series?select=as_of&series_id=eq.{sid}&order=as_of.asc&limit=1")
        if got:
            earliest = date.fromisoformat(got[0]["as_of"])
    wins = [date.today()]
    if earliest is not None:
        floor = date.today() - timedelta(days=int(config.SOX_YEARS * 365))
        end = earliest - timedelta(days=1)
        for _ in range(config.SOX_BACK_WINDOWS):
            if end <= floor:
                break
            wins.append(end)
            end = end - timedelta(days=config.SOX_WINDOW_DAYS + 1)
    out, bad, why_last = [], 0, ""
    for end in wins:
        rows, why = _nasdaq_rows("SOX", "index", config.SOX_WINDOW_DAYS, end=end)
        time.sleep(1)
        if rows is None:
            why_last = why
            break
        for r in rows:
            dd = r["date"]
            closed = dd < now.date() or (dd == now.date() and now.hour >= 17)
            if not closed or not (low <= r["close"] <= high):
                bad += 1
                continue
            out.append({"series_id": sid, "label": label, "as_of": dd.isoformat(), "value": r["close"], "source": "Nasdaq 官方行情（已收盤）"})
    if out:
        log("sox_history", "success", as_of=max(r["as_of"] for r in out), detail=_clip(f"{len(out)} 筆（{min(r['as_of'] for r in out)} 到 {max(r['as_of'] for r in out)}）" + (f"；略過 {bad} 筆（未收盤或超出範圍）" if bad else "")))
    else:
        log("sox_history", "no_new_data", detail=_clip(f"Nasdaq 沒有提供 SOX 歷史：{why_last or '沒有可用的列'}（不用 ETF 代替）"))
    return out


def us_index_quotes():
    """模組 M18：用 Nasdaq 官方已收盤的行情補上 FRED 落後的最近幾天（FRED 的美股指數常落後 3 到 5 天，這樣顯示的資料日太舊）。
    做法：先抓最近 14 天的歷史（index 分類），只收美東已經收盤的交易日（美東日期早於今天，或今天但已過 17:00）；歷史抓不到才退回只抓最新一筆已收盤報價。
    寫進 macro_series 同一個序列（series_id 與 FRED 一樣），日期用各筆自己的日期；價格超出合理範圍一律拒收。之後 FRED 補上同一天會蓋回官方值，兩者是同一個指數的收盤。"""
    now = _now_et()
    out = []
    for entry in config.US_INDEX_FEED:
        sym, sid, classes = entry[:3]
        optional = len(entry) > 3 and bool(entry[3])                                   # 選用來源：抓不到只記「沒有資料」，不讓整個工作變紅
        label, low, high = config.FRED_MACRO[sid]
        rows, why = _nasdaq_history(sym, classes[0], days=14)
        got, bad = [], 0
        if rows:
            for d_, close in rows:
                try:
                    dd = date.fromisoformat(str(d_))
                except ValueError:
                    bad += 1
                    continue
                ok, _w = validate_close_date(dd.isoformat())
                closed = dd < now.date() or (dd == now.date() and now.hour >= 17)
                if not ok or not closed or not (low <= close <= high):
                    bad += 1
                    continue
                got.append({"series_id": sid, "label": label, "as_of": dd.isoformat(), "value": close, "source": "Nasdaq 官方行情（已收盤，補 FRED 落後的天數）"})
        if got:
            out.extend(got)
            log(f"us_index:{sid}", "success", as_of=max(r["as_of"] for r in got), detail=f"歷史 {len(got)} 天（{min(r['as_of'] for r in got)} 到 {max(r['as_of'] for r in got)}）" + (f"；略過 {bad} 筆（未收盤或超出範圍）" if bad else ""))
        else:
            q, qwhy = _nasdaq_quote(sym, classes)                                            # 歷史沒有可用的：退回只抓最新一筆已收盤報價
            if q is None:
                closed_skip = "不是已收盤" in str(qwhy)
                log(f"us_index:{sid}", "skipped" if closed_skip else ("no_new_data" if optional else "failure"),
                    detail=_clip(f"歷史：{why or '沒有可用的列'}；報價：{qwhy}" + ("（選用來源，Nasdaq 沒有提供；維持 FRED 的資料）" if optional and not closed_skip else "")))
            elif not (low <= q["close"] <= high):
                log(f"us_index:{sid}", "failure", as_of=q["as_of"], detail=f"價格 {q['close']} 不在合理範圍 {low}–{high}，拒收")
            else:
                out.append({"series_id": sid, "label": label, "as_of": q["as_of"], "value": q["close"], "source": "Nasdaq 官方報價（已收盤，補 FRED 落後的天數）"})
                log(f"us_index:{sid}", "success", as_of=q["as_of"], detail=f"只取得最新報價（歷史不可用：{why or '沒有可用的列'}）：收盤 {q['close']}")
        time.sleep(1)
    return out


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


def fred_history_rows(series_id, low, high, days=None):
    """FRED 最近一段歷史（用各筆自己的日期）。回傳 [{series_id,label,as_of,value,source}]，失敗回傳 None。days 不給就用 FRED_HISTORY_DAYS（約 1 年）。"""
    key = config.fred_key()
    if not key:
        return None
    start = (date.today() - timedelta(days=days or config.FRED_HISTORY_DAYS)).isoformat()
    url = f"{config.FRED_SERIES_URL}?series_id={series_id}&api_key={key}&file_type=json&sort_order=asc&observation_start={start}"
    data, err = fetch_json(url)
    if data is None or not isinstance(data, dict) or not isinstance(data.get("observations"), list):
        log(f"fred_history:{series_id}", "failure", detail=_clip(f"FRED 歷史抓取失敗: {err}"))
        return None
    label = config.FRED_MACRO.get(series_id, (series_id,))[0]                                   # 台幣（DEXTAUS）不在 FRED_MACRO 裡
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


def us_index_long_history():
    """模組 M35：標普 500 與 Nasdaq 綜合的長期日資料（FRED，近 US_LONG_YEARS 年）→ macro_series。
    用來做美股的「從 60 日高點回落到某個程度以後，過去發生過什麼」歷史對照（和台股那張同一個算法）。
    FRED 官方收盤；用和每天更新一樣的合理範圍與日期檢查；抓不到只記錄，不讓工作變紅。寫入用 series_id,as_of 當主鍵，重跑不會重複。"""
    out = []
    for sid in config.US_LONG_SERIES:
        low, high = config.FRED_MACRO[sid][1], config.FRED_MACRO[sid][2]
        rows = fred_history_rows(sid, low, high, days=config.US_LONG_YEARS * 366)
        if rows:
            out.extend(rows)
            log(f"us_index_long:{sid}", "success", as_of=rows[-1]["as_of"], detail=f"{len(rows)} 筆（{rows[0]['as_of']} 到 {rows[-1]['as_of']}）")
        else:
            log(f"us_index_long:{sid}", "no_new_data", detail="FRED 沒有回傳資料（沒設 API 金鑰、連線失敗或空的）")
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


def tw_index_long_history(max_months=None):
    """模組 M24：回補更早的加權指數歷史（收盤與成交金額，證交所 FMTQIK 每月一次，間隔 HIST_SLEEP 秒），最多回到 INDEX_LONG_YEARS 年前。
    為什麼要：「下跌放量要放多少量才算止跌的可能」需要足夠多的大跌日當樣本，一年資料只有幾個，十年才有 2020、2022、2025 這幾次大跌。
    做法：從資料庫裡最早的一天往前，每次最多 INDEX_LONG_BATCH 個月；某個月證交所沒有資料就略過；連線失敗就整批停止；
    資料庫裡還沒有任何歷史（M05 還沒跑過）就先不做。每個月的驗證和 M05 完全一樣（日期、價格範圍、成交金額）。"""
    max_months = max_months or config.INDEX_LONG_BATCH
    if not sb_ready():
        log("tw_index_long", "skipped", detail="未設定 Supabase，無法知道最早補到哪一天，不回補")
        return []
    got, err = sb_request("GET", "market_stats?select=as_of&source=eq.TWSE%20FMTQIK&order=as_of.asc&limit=1")
    if got is None:
        log("tw_index_long", "failure", detail=_clip(f"讀取最早的一天失敗：{err}"))
        return []
    if not got:
        log("tw_index_long", "no_new_data", detail="資料庫裡還沒有加權指數歷史（要先跑一次 tw_close 讓 M05 寫入），先不回補")
        return []
    earliest = date.fromisoformat(got[0]["as_of"])
    target = date.today().replace(day=1) - timedelta(days=int(config.INDEX_LONG_YEARS * 365))
    y, m = earliest.year, earliest.month
    m -= 1
    if m == 0:
        y, m = y - 1, 12
    out, empty, done = [], 0, 0
    while done < max_months and date(y, m, 1) >= target.replace(day=1):
        rows, why = _twse_month_rows(f"{config.TWSE_FMTQIK}?response=json&date={y}{m:02d}01")
        time.sleep(config.HIST_SLEEP)
        if rows is None:
            log("tw_index_long", "failure", as_of=f"{y}-{m:02d}-01", detail=_clip(f"{y}-{m:02d}：{why}；已停止本次回補，不繼續打請求"))
            break
        if not rows:
            empty += 1
        out.extend(rows)
        done += 1
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    if out or empty or done:
        reached = min((r["as_of"] for r in out), default=None)
        left = max(0, (date(y, m, 1).year - target.year) * 12 + date(y, m, 1).month - target.month + 1) if date(y, m, 1) >= target.replace(day=1) else 0
        log("tw_index_long", "success" if out else "no_new_data", as_of=reached,
            detail=_clip(f"本次回補 {done} 個月，取得 {len(out)} 個交易日" + (f"（最早到 {reached}）" if reached else "") + (f"；{empty} 個月證交所沒有資料" if empty else "") + (f"；還有約 {left} 個月下次繼續" if left else "；已經補到目標年數")))
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
        sw, err = fetch_swagger(sw_url)
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
    sw, err = fetch_swagger(config.TWSE_OPENAPI_SWAGGER)
    path = None
    if isinstance(sw, dict):
        for pth, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                text = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if text.startswith("上市公司") and "一般業" in text and ("綜合損益" in text or "財報資訊" in text) and "_X_" not in pth:
                    path = path or pth                                    # 必須是「上市公司」；「公發公司」（_X_）不是上市股票，選到它你的持股會全部落空
        path = path or ("/opendata/t187ap06_L_ci" if "/opendata/t187ap06_L_ci" in (sw.get("paths") or {}) else None)
    sources = [("twse", sw, config.TWSE_OPENAPI_BASE, path, "證交所 上市公司財報資訊（一般業）")]
    sw2, err2 = fetch_swagger(config.TPEX_SWAGGER)
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


def parse_aaii_history(html, today=None):
    """AAII 結果頁的「每週歷史表」：每一列是「月 日 看多% 中立% 看空%」（沒有年份），由新到舊。年份用往回走的方式推：最新一列如果比今天還晚就是去年，之後每次月日變大（跨年）就再減一年。
    驗證每一列：三項合計在 99 到 101；相鄰兩列的日期差剛好 7 天（不是就從那裡停，後面的都不要，因為年份推算已經不可靠）；至少 3 列。回傳 (列表由新到舊, 原因)。"""
    today = today or date.today()
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
    i = text.find("Bullish Neutral Bearish")
    seg = text[i + len("Bullish Neutral Bearish"):] if i >= 0 else text
    found = re.findall(r"\b([A-Z][a-z]{2})\.? (\d{1,2}) (\d{1,3}(?:\.\d+)?)% (\d{1,3}(?:\.\d+)?)% (\d{1,3}(?:\.\d+)?)%", seg)
    out, year, prev = [], None, None
    for mon_s, day_s, b, n, r in found:
        mon = _US_MONTHS.get(mon_s)
        if not mon:
            break
        md = (mon, int(day_s))
        try:
            if year is None:
                year = today.year
                if date(year, *md) > today:
                    year -= 1
            elif md > prev:
                year -= 1
            d = date(year, *md)
        except ValueError:
            break
        vals = (float(b), float(n), float(r))
        if not (99 <= sum(vals) <= 101) or any(v > 100 for v in vals):
            break
        if out and (date.fromisoformat(out[-1]["as_of"]) - d).days != 7:
            break
        out.append({"as_of": d.isoformat(), "bullish": vals[0], "neutral": vals[1], "bearish": vals[2], "source": "AAII 散戶情緒調查（aaii.com 結果頁的每週歷史表）"})
        prev = md
    if len(out) < 3:
        return [], f"歷史表只讀到 {len(out)} 週（至少 3 週），不用"
    return out, ""


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
    latest = {"as_of": d.isoformat(), "bullish": val["bullish"], "neutral": val["neutral"], "bearish": val["bearish"], "source": "AAII 散戶情緒調查（aaii.com）"}
    base = f"看多 {val['bullish']}%、中立 {val['neutral']}%、看空 {val['bearish']}%"
    # 結果頁有每週歷史表：讀出來補歷史；但只有「最新一週和上面這個獨立頁面的數字一致」才採用（兩個頁面互相核對），否則只存最新一週
    st2, _ct2, body2, err2 = fetch_raw(config.AAII_RESULTS_URL, timeout=config.PROBE_TIMEOUT, headers={"User-Agent": config.NASDAQ_UA, "Accept": "text/html", "Referer": "https://www.aaii.com/"})
    hist, why = parse_aaii_history(body2.decode("utf-8", errors="replace"), today) if st2 == 200 and body2 else ([], f"結果頁 HTTP {st2} {err2}".strip())
    h0 = hist[0] if hist else None
    if h0 and h0["as_of"] == latest["as_of"] and all(abs(h0[k] - latest[k]) <= 0.05 for k in ("bullish", "neutral", "bearish")):
        log("aaii", "success", as_of=latest["as_of"], detail=f"{base}；結果頁歷史表 {len(hist)} 週（{hist[-1]['as_of']} 到 {hist[0]['as_of']}），最新一週和上面的頁面核對一致，一併存入")
        return hist
    log("aaii", "success", as_of=latest["as_of"], detail=base + "；歷史表沒採用：" + (why or f"最新一週和另一個頁面對不上（歷史表 {h0['as_of']}，{h0['bullish']}/{h0['neutral']}/{h0['bearish']}）" if h0 else why))
    return [latest]


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


def parse_ndx_constituents(j):
    """Nasdaq 官方的 Nasdaq-100 成分股清單 → 代號清單。結構是 data → data → rows，但不靠固定路徑：往下找第一個「每一項都有 symbol」的列表。
    驗證：代號是 1 到 5 個大寫字母、不重複，總數在 95 到 110 之間（Nasdaq-100 是 100 家公司、約 101 個代號）。回傳 (代號列表, 原因)。"""
    def find(o, depth=0):
        if isinstance(o, list) and o and all(isinstance(x, dict) and "symbol" in x for x in o):
            return o
        if depth > 4:
            return None
        for v in (o.values() if isinstance(o, dict) else o if isinstance(o, list) else []):
            r = find(v, depth + 1)
            if r:
                return r
        return None
    syms = []
    for r in find(j) or []:
        s_ = str(r.get("symbol", "")).strip().upper()
        if re.fullmatch(r"[A-Z]{1,5}", s_) and s_ not in syms:
            syms.append(s_)
    if not (95 <= len(syms) <= 110):
        return [], f"成分股數量 {len(syms)}（預期 95 到 110），不用"
    return syms, ""


def us_breadth_ndx():
    """模組 M32：Nasdaq-100 廣度：成分股裡，收盤高於 50 日、200 日均線的比例。美股沒有免費的「S&P 500 站上 50 日線比例」，所以用 Nasdaq 官方的成分股清單與歷史行情自己算；
    涵蓋的是 Nasdaq-100（科技股占比高），不是 S&P 500。每檔的規則和 M10 一樣：最新日期要和 SPY 同一天、收盤價有疑似分割的跳動就不算；
    均線需要足夠的天數（50 日至少 50 筆、200 日至少 200 筆，不夠的那檔只是不進那個比例）。有效的至少 80 檔才採用；抓不到或不夠只記「沒有資料」，不讓工作變紅。"""
    status, ctype, body, err = fetch_raw(config.NASDAQ_NDX_URL, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status != 200 or not body:
        log("us_breadth_ndx", "no_new_data", detail=_clip(f"成分股清單抓取失敗 HTTP {status} {err}".strip()))
        return []
    try:
        j = json.loads(body.decode("utf-8-sig").lstrip("\ufeff"))
    except (ValueError, UnicodeDecodeError):
        log("us_breadth_ndx", "no_new_data", detail="成分股清單不是 JSON")
        return []
    syms, why = parse_ndx_constituents(j)
    if not syms:
        log("us_breadth_ndx", "no_new_data", detail=_clip(why))
        return []
    bench, why = _nasdaq_history(config.SECTOR_BENCH, "etf", 40)
    if not bench or len(bench) < 5:
        log("us_breadth_ndx", "no_new_data", detail=_clip(f"基準 {config.SECTOR_BENCH} 歷史抓取失敗：{why or len(bench or [])}"))
        return []
    last = bench[-1][0]
    ok50 = ok200 = a50 = a200 = 0
    skipped, series = {}, []
    for sym in syms:
        pts, w_ = _nasdaq_history_long(sym, "stocks", config.BREADTH_NDX_HIST_DAYS)
        time.sleep(config.BREADTH_NDX_SLEEP)
        why_skip = "抓不到" if not pts else "日期不同" if pts[-1][0] != last else "疑似分割" if any(b[1] / a[1] >= 1.5 or b[1] / a[1] <= 0.67 for a, b in zip(pts[-200:], pts[-199:])) else None   # 只看最近 200 筆；更早的分割由歷史計算自己避開
        if why_skip:
            skipped.setdefault(why_skip, []).append(sym)
            continue
        c = [p[1] for p in pts]
        series.append(pts)
        if len(c) >= 50:
            ok50 += 1
            a50 += 1 if c[-1] > sum(c[-50:]) / 50 else 0
        if len(c) >= 200:
            ok200 += 1
            a200 += 1 if c[-1] > sum(c[-200:]) / 200 else 0
    # 列出被略過的代號：「疑似分割」的規則是相鄰兩天收盤價差 1.5 倍以上或 0.67 倍以下，單日暴跌超過三分之一的股票（例如財報後大跌）也會被排除，所以要讓你看得到是誰
    note = "；略過 " + "、".join(f"{k} {len(v)} 檔（{'、'.join(v[:8])}{'…' if len(v) > 8 else ''}）" for k, v in sorted(skipped.items())) if skipped else ""
    if ok50 < config.BREADTH_NDX_MIN_OK:
        log("us_breadth_ndx", "no_new_data", as_of=last.isoformat(), detail=_clip(f"成分股 {len(syms)} 檔，只有 {ok50} 檔有效（至少 {config.BREADTH_NDX_MIN_OK}），不採用{note}"))
        return []
    row = {"as_of": last.isoformat(), "n_total": len(syms), "n_ok50": ok50, "above50": a50, "n_ok200": ok200, "above200": a200, "pct50": round(a50 / ok50 * 100, 2),
           "pct200": round(a200 / ok200 * 100, 2) if ok200 >= config.BREADTH_NDX_MIN_OK else None,
           "source": "Nasdaq 官方成分股清單與歷史行情（自己計算；涵蓋 Nasdaq-100，不是 S&P 500）"}
    hist_rows = _ndx_breadth_history(series, last, len(syms))
    if _LONG_TRACE:
        note += "；第一檔分段抓取：" + " | ".join(_LONG_TRACE[0]) + f"；各檔最早日期 {min(p[0][0] for p in series).isoformat()}"
        del _LONG_TRACE[:]
    log("us_breadth_ndx", "success", as_of=row["as_of"], detail=f"站上 50 日線 {a50} / {ok50} 檔（{row['pct50']}%）；站上 200 日線 {a200} / {ok200} 檔；同時用同一批歷史補了前 {len(hist_rows)} 天" + (f"（{row['pct200']}%）" if row["pct200"] is not None else "（有效檔數不到 80，不算比例）") + note)
    return hist_rows + [row]


def _ndx_breadth_history(series, last, n_total):
    """用同一批成分股歷史（不多打請求）往前算每一天「站上 50 日線」的比例：那一天要有至少 BREADTH_NDX_MIN_OK 檔有 50 筆以上的收盤價才採用。
    200 日線只在有效檔數夠的日子才給。回傳不含最後一天（最後一天由主程式算）的列，由舊到新。"""
    dates = sorted({d for pts in series for d, _ in pts if d < last})
    def _splits(pts):
        return [i + 1 for i, (a_, b_) in enumerate(zip(pts, pts[1:])) if b_[1] / a_[1] >= 1.5 or b_[1] / a_[1] <= 0.67]
    maps = [({d: i for i, (d, _c) in enumerate(pts)}, [c for _d, c in pts], _splits(pts)) for pts in series]
    out = []
    for d in dates:
        o50 = o200 = b50 = b200 = 0
        for idx, c, sp in maps:
            i = idx.get(d)
            if i is None:
                continue
            if i + 1 >= 50 and not any(i - 49 < s <= i for s in sp):          # 50 日窗口內有疑似分割的那天，這檔不算
                o50 += 1
                b50 += 1 if c[i] > sum(c[i - 49:i + 1]) / 50 else 0
            if i + 1 >= 200 and not any(i - 199 < s <= i for s in sp):
                o200 += 1
                b200 += 1 if c[i] > sum(c[i - 199:i + 1]) / 200 else 0
        if o50 < config.BREADTH_NDX_MIN_OK:
            continue
        out.append({"as_of": d.isoformat(), "n_total": n_total, "n_ok50": o50, "above50": b50, "n_ok200": o200, "above200": b200, "pct50": round(b50 / o50 * 100, 2),
                    "pct200": round(b200 / o200 * 100, 2) if o200 >= config.BREADTH_NDX_MIN_OK else None,
                    "source": "Nasdaq 官方成分股清單與歷史行情（自己計算，往前補；涵蓋 Nasdaq-100，不是 S&P 500）"})
    return out


def probe_round15():
    """只讀探測第十五批：Nasdaq-100 成分股清單（確認格式、數量）。不寫入任何資料表。"""
    status, ctype, body, err = fetch_raw(config.NASDAQ_NDX_URL, timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
    if status is None:
        log("probe:ndx_list", "no_new_data", detail=_clip(f"連線失敗: {err}"))
        return
    txt = (body or b"").decode("utf-8", errors="replace")
    extra = ""
    try:
        syms, why = parse_ndx_constituents(json.loads(txt))
        extra = f"｜解析：{len(syms)} 檔，前 5 檔 {syms[:5]}" + (f"｜{why}" if why else "")
    except ValueError:
        extra = "｜不是 JSON"
    log("probe:ndx_list", "success" if status == 200 else "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜{len(body or b'')} 位元組｜開頭 {re.sub(chr(10), ' ', txt[:200])!r}{extra}"))


def _news_path(sw):
    for pth, methods in ((sw or {}).get("paths") or {}).items():
        for mth in (methods or {}).values():
            if isinstance(mth, dict) and "重大訊息" in str(mth.get("summary", "")):
                return pth
    return None


_EXR_DATE = re.compile(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")


def parse_exright_rows(data, today=None):
    """證交所『除權除息預告表』→ tw_exright_events 的列。回傳 (列, 略過數, 錯誤原因)。
    欄位用名稱找（不靠位置）；日期是民國年文字『115年11月06日』；現金股利可能是『待公告…』的文字（還沒公告），那就存空白，不猜。
    代號、種類（息／權／權息）、日期範圍（前 3 天到 180 天後）、配股率範圍有任何一項不合就略過那一列。"""
    today = today or date.today()
    fields = [str(f).strip() for f in (data.get("fields") or [])]
    need = {"date": "除權除息日期", "code": "股票代號", "name": "名稱", "kind": "除權息", "stock": "無償配股率", "cash": "現金股利"}
    miss = [v for v in need.values() if v not in fields]
    if miss:
        return [], 0, f"欄位與預期不符：缺 {miss}；實際 {fields[:13]}"
    ix = {k: fields.index(v) for k, v in need.items()}
    out, bad = [], 0
    for r in data.get("data") or []:
        try:
            m = _EXR_DATE.search(str(r[ix["date"]]))
            code, kind = str(r[ix["code"]]).strip(), str(r[ix["kind"]]).strip()
            if not m or not re.fullmatch(r"[0-9A-Z]{4,6}", code) or kind not in ("息", "權", "權息"):
                bad += 1
                continue
            ev = date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
            stock = parse_number(r[ix["stock"]])
            cash = parse_number(r[ix["cash"]])
            if not (today - timedelta(days=3) <= ev <= today + timedelta(days=180)) or (stock is not None and not (0 <= stock <= 10)) or (cash is not None and not (0 <= cash < 10000)):
                bad += 1
                continue
            out.append({"market": "TWSE", "code": code, "event_date": ev.isoformat(), "name": str(r[ix["name"]]).strip()[:40], "kind": kind,
                        "stock_rate": stock, "cash_dividend": cash, "source": "證交所 除權除息預告表"})
        except (IndexError, TypeError, ValueError):
            bad += 1
    return out, bad, None


_TPEX_KIND = {"除息": "息", "除權": "權", "除權息": "權息"}


def parse_tpex_exright_rows(rows, today=None):
    """櫃買中心『除權除息預告表』（/tpex_exright_prepost）→ tw_exright_events 的列。回傳 (列, 略過數, 錯誤原因)。
    欄位是英文：ExRrightsExDividendDate（民國年 7 碼，例如 1151021）、SecuritiesCompanyCode、CompanyName、ExRrightsExDividend（除息／除權／除權息）、
    StockDividendRatio、CashDividend。缺任何一個欄位就整份不採用；日期不在前 3 天到 180 天後、代號或種類不合、數字超出範圍的列略過。"""
    today = today or date.today()
    if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
        return [], 0, "格式非預期"
    need = ("ExRrightsExDividendDate", "SecuritiesCompanyCode", "CompanyName", "ExRrightsExDividend", "StockDividendRatio", "CashDividend")
    miss = [k for k in need if k not in rows[0]]
    if miss:
        return [], 0, f"欄位與預期不符：缺 {miss}；實際 {list(rows[0].keys())[:12]}"
    out, bad = [], 0
    for r in rows:
        iso = roc_to_iso(r.get("ExRrightsExDividendDate"))
        code, kind = str(r.get("SecuritiesCompanyCode", "")).strip(), _TPEX_KIND.get(str(r.get("ExRrightsExDividend", "")).strip())
        stock, cash = parse_number(r.get("StockDividendRatio")), parse_number(r.get("CashDividend"))
        try:
            ev = date.fromisoformat(iso) if iso else None
        except ValueError:
            ev = None
        if (ev is None or not re.fullmatch(r"[0-9A-Z]{4,6}", code) or kind is None or not (today - timedelta(days=3) <= ev <= today + timedelta(days=180))
                or (stock is not None and not (0 <= stock <= 10)) or (cash is not None and not (0 <= cash < 10000))):
            bad += 1
            continue
        out.append({"market": "TPEX", "code": code, "event_date": ev.isoformat(), "name": str(r.get("CompanyName", "")).strip()[:40], "kind": kind,
                    "stock_rate": stock, "cash_dividend": cash, "source": "櫃買中心 除權除息預告表"})
    return out, bad, None


def tw_exright_preview():
    """模組 M15：除權除息預告表（上市：證交所；上櫃：櫃買中心）。每天收集，預告表只列即將發生的，過去的靠每天累積。
    用途：持股的除權息日進事件日曆；大跌清單標註當天是除權息日（配股會讓價格自然下降，不是壞消息）。
    兩個市場各自獨立：一邊失敗不影響另一邊，各留一筆紀錄。"""
    out = []
    data, err = fetch_json(config.EXRIGHT_URL)
    if data is None:
        log("tw_exright:twse", "failure", detail=_clip(f"抓取失敗：{err}"))
    elif not isinstance(data, dict) or data.get("stat") != "OK":
        log("tw_exright:twse", "no_new_data", detail=f"證交所回覆 stat={data.get('stat') if isinstance(data, dict) else '非預期格式'}（可能目前沒有預告）")
    else:
        rows, bad, why = parse_exright_rows(data)
        if why:
            log("tw_exright:twse", "failure", detail=_clip(why))
        elif not rows:
            log("tw_exright:twse", "no_new_data", detail=f"預告表 {len(data.get('data') or [])} 列，沒有通過驗證的列（略過 {bad} 列）")
        else:
            nxt = sorted(rows, key=lambda r: r["event_date"])[:3]
            log("tw_exright:twse", "success", as_of=nxt[0]["event_date"], detail=_clip(f"{len(rows)} 筆除權息預告（略過 {bad} 列）；最近：" + "、".join(f"{r['event_date'][5:]} {r['code']}{r['name']}（{r['kind']}）" for r in nxt)))
            out.extend(rows)
    rows_t, err = fetch_json(config.TPEX_DEFAULT_BASE + "/tpex_exright_prepost")
    if rows_t is None:
        log("tw_exright:tpex", "failure", detail=_clip(f"抓取失敗：{err}"))
    else:
        got, bad, why = parse_tpex_exright_rows(rows_t)
        if why:
            log("tw_exright:tpex", "failure" if "欄位" in why else "no_new_data", detail=_clip(why))
        elif not got:
            log("tw_exright:tpex", "no_new_data", detail=f"預告表 {len(rows_t)} 列，沒有通過驗證的列（略過 {bad} 列）")
        else:
            nxt = sorted(got, key=lambda r: r["event_date"])[:3]
            log("tw_exright:tpex", "success", as_of=nxt[0]["event_date"], detail=_clip(f"{len(got)} 筆除權息預告（略過 {bad} 列）；最近：" + "、".join(f"{r['event_date'][5:]} {r['code']}{r['name']}（{r['kind']}）" for r in nxt)))
            out.extend(got)
    return out


_TXO_CM = re.compile(r"^(\d{6})(?:[A-Z]\d)?$")


def _taifex_date(v):
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", str(v or "").strip())
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def parse_taifex_snapshot(opt_rows, fut_rows, today=None, band=0.20):
    """期交所 OpenAPI 的選擇權每日行情（DailyMarketReportOpt）與期貨每日行情（DailyMarketReportFut）→ ({摘要}, [履約價列], 錯誤原因)。
    只用「一般」交易時段的列（盤後的列不重複計算）。選擇權只看台指選擇權（TXO），期貨只看大台（TX）的單一月份（價差組合不算）。
    資料日：兩份資料最多的日期必須相同，而且是最近 5 天內的平日。
    摘要：近月台指期結算價、台指期各月份未平倉量合計、台指選擇權買權與賣權未平倉量（所有到期日）。
    履約價列：只收『本月與下個月』到期（含週選）的合約、履約價在近月台指期結算價 ±band 內，同一履約價把買權、賣權未平倉量各自加總。
    （週選的實際到期日不在這份資料裡，所以用月份分組，不另外推算到期日。）"""
    today = today or date.today()
    for name, rows, need in (("選擇權", opt_rows, ("Date", "Contract", "ContractMonth(Week)", "StrikePrice", "CallPut", "OpenInterest")),
                             ("期貨", fut_rows, ("Date", "Contract", "ContractMonth(Week)", "SettlementPrice", "OpenInterest"))):
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            return None, [], f"{name}每日行情格式非預期"
        miss = [k for k in need if k not in rows[0]]
        if miss:
            return None, [], f"{name}每日行情欄位與預期不符：缺 {miss}"
    regular = lambda r: str(r.get("TradingSession", "")).strip() in ("一般", "")
    opt = [r for r in opt_rows if str(r.get("Contract", "")).strip() == "TXO" and regular(r)]
    tx = [r for r in fut_rows if str(r.get("Contract", "")).strip() == "TX" and regular(r) and re.fullmatch(r"\d{6}", str(r.get("ContractMonth(Week)", "")).strip())]
    if not opt or not tx:
        return None, [], f"找不到台指選擇權（{len(opt)} 列）或台指期（{len(tx)} 列）的一般時段資料"
    d1 = Counter(str(r.get("Date", "")).strip() for r in opt).most_common(1)[0][0]
    d2 = Counter(str(r.get("Date", "")).strip() for r in tx).most_common(1)[0][0]
    if d1 != d2:
        return None, [], f"選擇權的資料日是 {d1}，期貨是 {d2}，兩份對不上，不採用"
    as_of = _taifex_date(d1)
    if as_of is None or as_of.weekday() >= 5 or not (today - timedelta(days=5) <= as_of <= today):
        return None, [], f"資料日 {d1} 不是最近 5 天內的平日，不採用"
    ym = as_of.strftime("%Y%m")
    nxt = f"{as_of.year + (as_of.month == 12)}{(as_of.month % 12) + 1:02d}"
    fut_pts, fut_oi = [], 0
    for r in tx:
        m_, settle, oi = str(r["ContractMonth(Week)"]).strip(), parse_number(r.get("SettlementPrice")), parse_number(r.get("OpenInterest"))
        if oi is not None and oi >= 0:
            fut_oi += int(oi)
        if settle and settle > 0:
            fut_pts.append((m_, settle))
    near = sorted(p for p in fut_pts if p[0] >= ym)
    if not near:
        return None, [], "找不到近月台指期的結算價"
    fut_settle = near[0][1]
    if not (5000 <= fut_settle <= 100000) or fut_oi <= 0:
        return None, [], f"台指期結算價 {fut_settle} 或未平倉量 {fut_oi} 不合理"
    call_tot = put_tot = n = 0
    strikes = {}
    for r in opt:
        k, cp, oi = parse_number(r.get("StrikePrice")), str(r.get("CallPut", "")).strip(), parse_number(r.get("OpenInterest"))
        cm = _TXO_CM.match(str(r.get("ContractMonth(Week)", "")).strip())
        if not k or k <= 0 or cp not in ("買權", "賣權") or oi is None or oi < 0 or not cm:
            continue
        n += 1
        oi = int(oi)
        if cp == "買權":
            call_tot += oi
        else:
            put_tot += oi
        if cm.group(1) in (ym, nxt) and abs(k / fut_settle - 1) <= band and oi > 0:
            row = strikes.setdefault(k, {"as_of": as_of.isoformat(), "strike": k, "call_oi": 0, "put_oi": 0})
            row["call_oi" if cp == "買權" else "put_oi"] += oi
    if n < 200 or call_tot <= 0 or put_tot <= 0:
        return None, [], f"能解析的台指選擇權只有 {n} 列、買權 OI {call_tot}、賣權 OI {put_tot}，不採用"
    summary = {"as_of": as_of.isoformat(), "fut_settle": fut_settle, "fut_oi": fut_oi, "call_oi": call_tot, "put_oi": put_tot, "n_rows": n, "source": "期交所 OpenAPI（選擇權與期貨每日行情，一般時段）"}
    return summary, sorted(strikes.values(), key=lambda r: r["strike"]), None


TAIFEX_CSV_COLS = {"日期": "Date", "契約": "Contract", "到期月份(週別)": "ContractMonth(Week)", "履約價": "StrikePrice", "買賣權": "CallPut", "開盤價": "Open", "最高價": "High", "最低價": "Low",
                   "最後成交價": "Close", "成交量": "Volume", "結算價": "SettlementPrice", "未沖銷契約量": "OpenInterest", "最後最佳買價": "BestBid", "最後最佳賣價": "BestAsk",
                   "歷史最高價": "HistoricalHigh", "歷史最低價": "HistoricalLow", "是否因訊息面暫停交易": "TradingHalt", "交易時段": "TradingSession", "漲跌價": "Change",
                   "賣權成交量": "PutVolume", "買權成交量": "CallVolume", "買賣權成交量比率%": "PutCallVolumeRatio%", "賣權未平倉量": "PutOI", "買權未平倉量": "CallOI", "買賣權未平倉量比率%": "PutCallOIRatio%"}


def parse_taifex_csv(text):
    """期交所開放資料有時回傳 CSV（中文欄位名）而不是 JSON（英文欄位名）。轉成和 JSON 一樣的 [dict]：中文欄位名換成英文、日期 2026/11/06 換成 20261106，其他欄位原樣保留。
    沒有「日期」欄位、或不到 2 欄，就不是我們要的表格，回傳 None。"""
    import csv, io
    rd = csv.reader(io.StringIO(text.replace("\ufeff", "")))
    try:
        head = [h.strip() for h in next(rd)]
    except StopIteration:
        return None
    if len(head) < 2 or "日期" not in head:
        return None
    keys = [TAIFEX_CSV_COLS.get(h, h) for h in head]
    out = []
    for row in rd:
        if not row or all(not c.strip() for c in row):
            continue
        d = {k: (row[i].strip() if i < len(row) else "") for i, k in enumerate(keys)}
        d["Date"] = re.sub(r"[/\-]", "", d.get("Date", ""))
        out.append(d)
    return out or None


def fetch_taifex(url):
    """抓期交所開放資料：先當 JSON；回傳的不是 JSON（例如 CSV 表格）就改讀 CSV。回傳 (列表, 錯誤原因)。"""
    data, err = fetch_json(url)
    if data is not None or not str(err).startswith("不是 JSON"):
        return data, err
    status, ctype, body, e2 = fetch_raw(url, timeout=config.OPTIONS_TIMEOUT, headers={"User-Agent": "risk-pwa/1.0", "Accept": "*/*"})
    rows = parse_taifex_csv(body.decode("utf-8-sig", errors="replace")) if status == 200 and body else None
    return (rows, "") if rows else (None, err)


def compute_retail_ratio(inv_rows, fut_rows, today=None):
    """小台指散戶多空比 ＝（散戶多單 － 散戶空單）÷ 全市場未平倉量 × 100；散戶部位 ＝ 全市場 － 三大法人（自營商、投信、外資）。
    法人口數來自期交所「三大法人-區分各期貨契約」，全市場未平倉量來自期貨每日行情的 MTX（只算單一月份、一般時段）。
    契約名稱不寫死：找名稱含「小型」與「臺指」的那個，找不到就回傳「這份資料有哪些契約」（讓你看得到實際名稱）。三個法人缺一個、日期對不上、數字不合理都不算。回傳 (列, 原因)。"""
    today = today or date.today()
    if not (isinstance(inv_rows, list) and inv_rows and isinstance(inv_rows[0], dict) and isinstance(fut_rows, list) and fut_rows):
        return None, "資料格式非預期"
    dates = [twse_ymd_to_iso(r.get("Date")) for r in inv_rows if isinstance(r, dict)]
    as_of = max((d for d in dates if d), default=None)
    ok, why = validate_close_date(as_of) if as_of else (False, "找不到有效日期")
    if not ok:
        return None, f"法人資料日期異常：{why}"
    today_rows = [r for r in inv_rows if isinstance(r, dict) and twse_ymd_to_iso(r.get("Date")) == as_of]
    names = sorted({str(r.get("ContractCode", "")).strip() for r in today_rows})
    mini = [n for n in names if "小型" in n and "臺" in n]
    if not mini:
        return None, f"找不到小型臺指期貨（法人資料裡有這些契約：{names[:12]}）"
    groups = {}
    for r in today_rows:
        if str(r.get("ContractCode", "")).strip() != mini[0]:
            continue
        item = str(r.get("Item", "")).strip()
        g = "外資" if item.startswith("外資") else "投信" if item.startswith("投信") else "自營商" if item.startswith("自營商") else None
        lng, sht = parse_number(r.get("OpenInterest(Long)")), parse_number(r.get("OpenInterest(Short)"))
        if g is None or lng is None or sht is None or lng < 0 or sht < 0:
            continue
        a, b = groups.get(g, (0, 0))
        groups[g] = (a + lng, b + sht)
    if set(groups) != {"外資", "投信", "自營商"}:
        return None, f"{mini[0]} 的法人只找到 {sorted(groups)}，要外資、投信、自營商三個都有才算散戶"
    inst_long, inst_short = int(sum(v[0] for v in groups.values())), int(sum(v[1] for v in groups.values()))
    key = as_of.replace("-", "")
    total = 0
    for r in fut_rows:
        if not isinstance(r, dict) or str(r.get("Contract", "")).strip() != "MTX" or not re.fullmatch(r"\d{6}", str(r.get("ContractMonth(Week)", "")).strip()) or str(r.get("TradingSession", "")).strip() not in ("一般", ""):
            continue
        if re.sub(r"\D", "", str(r.get("Date", ""))) != key:
            continue
        oi = parse_number(r.get("OpenInterest"))
        if oi is not None and oi >= 0:
            total += int(oi)
    if total <= 0:
        return None, f"期貨每日行情裡找不到 {as_of} 的 MTX 未平倉量（日期對不上或還沒更新）"
    if inst_long > total * 1.0001 or inst_short > total * 1.0001:
        return None, f"法人多單 {inst_long} 或空單 {inst_short} 比全市場 {total} 還大，數字不合理，不採用"
    ratio = round((inst_short - inst_long) / total * 100, 2)
    return {"as_of": as_of, "contract": mini[0], "inst_long": inst_long, "inst_short": inst_short, "total_oi": total, "ratio": ratio, "source": "期交所三大法人與期貨每日行情推算（不是交易所直接公布的數字）"}, None


def tw_retail_ratio():
    """模組 M28：小台指散戶多空比（每天累積）。"""
    inv, err = fetch_json(config.TAIFEX_FUTURES_URL)
    if inv is None:
        inv, err2 = _taifex_investor_csv(config.TAIFEX_FUTURES_URL)
        err = f"JSON：{err}｜CSV：{err2}" if inv is None else ""
    fut, e2 = fetch_taifex(config.TAIFEX_DAILY_URL)
    if inv is None or fut is None:
        empty = all(is_empty_response(e) for e in (err, e2) if e)
        log("tw_retail", "no_new_data" if empty else "failure", detail=_clip(f"抓取失敗：法人 {err or '好'}；期貨行情 {e2 or '好'}"))
        return []
    row, why = compute_retail_ratio(inv, fut)
    if row is None:
        log("tw_retail", "no_new_data" if ("找不到" in why or "對不上" in why) else "failure", detail=_clip(why))
        return []
    log("tw_retail", "success", as_of=row["as_of"], detail=f"{row['contract']} 散戶多空比 {row['ratio']:+.2f}%（法人多單 {row['inst_long']:,}、空單 {row['inst_short']:,}，全市場未平倉 {row['total_oi']:,}）")
    return [row]


def _tpex_margin_loan(as_of, units):
    """櫃買網站「融資融券餘額」頁的合計列：取「融資金(仟元)」的資餘額，換成元。回傳 (融資金額元, 原因)；任何一項驗證不過就回傳 (None, 原因)，不猜。
    驗證：頁面日期要等於 as_of；合計(張)的資餘額要和我們逐檔加總的張數一致（誤差 0.2% 內）；融資金額自己要對得上「前日＋買進－賣出－現償＝今日」（誤差十萬分之一或 5 仟元）。"""
    roc = f"{int(as_of[:4]) - 1911}/{as_of[5:7]}/{as_of[8:10]}"
    status, ctype, body, err = fetch_raw(f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc}", headers={"User-Agent": "risk-pwa/1.0", "Accept": "application/json"})
    if status != 200 or not body:
        return None, f"彙總頁抓取失敗 HTTP {status} {err}".strip()
    try:
        j = json.loads(body.decode("utf-8-sig"))
        t0 = j["tables"][0]
        fields = [str(f).strip() for f in t0["fields"]]
        summary = t0["summary"]
        ix = {k: fields.index(k) for k in ("前資餘額(張)", "資買", "資賣", "現償", "資餘額")}
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return None, "彙總頁格式非預期（找不到 tables、fields 或 summary）"
    if str(t0.get("date", "")).strip() != roc:
        return None, f"彙總頁的日期是 {t0.get('date')}，要的是 {roc}，不採用"
    def row_of(tag):
        for r in summary if isinstance(summary, list) else []:
            if isinstance(r, list) and len(r) > 1 and tag in str(r[1]):
                return r
        return None
    unit_row, loan_row = row_of("合計(張)"), row_of("融資金")
    if unit_row is None or loan_row is None:
        return None, "彙總頁的合計列裡找不到「合計(張)」或「融資金」"
    g = lambda r, k: parse_number(r[ix[k]]) if len(r) > ix[k] else None
    bal_u, loan_k = g(unit_row, "資餘額"), g(loan_row, "資餘額")
    prev_k, buy_k, sell_k, red_k = (g(loan_row, k) for k in ("前資餘額(張)", "資買", "資賣", "現償"))
    if None in (bal_u, loan_k, prev_k, buy_k, sell_k, red_k) or loan_k <= 0:
        return None, "融資金額欄位不是數字"
    if abs(bal_u - units) > max(1, units * 0.002):
        return None, f"彙總頁合計 {bal_u:,.0f} 張，和我們逐檔加總的 {units:,} 張對不上，不採用"
    if abs(prev_k + buy_k - sell_k - red_k - loan_k) > max(5, loan_k * 1e-5):
        return None, f"融資金額自己對不上：前日 {prev_k:,.0f} ＋ 買 {buy_k:,.0f} － 賣 {sell_k:,.0f} － 償 {red_k:,.0f} ≠ {loan_k:,.0f}（仟元）"
    return loan_k * 1000, ""


def tpex_margin_daily():
    """模組 M29：上櫃融資餘額（張數）每天累積。櫃買中心的開放資料「上櫃股票融資融券餘額」是逐檔表（前日餘額、買進、賣出、償還、今日餘額，單位張），沒有融資金額，
    所以算不出上櫃的融資維持率；這裡只算總張數、前日總張數、融資股票市值（Σ 張數 × 1000 × 收盤價，受股價影響，只當參考）。
    驗證：每一列「前日＋買進－賣出－償還＝今日」要在 98% 以上對得上；融資表與收盤價的日期必須是同一天；有融資的股票查不到收盤價超過 5% 就不算。"""
    rows, e1 = fetch_json(config.TPEX_MARGIN_URL)
    px, e2 = fetch_json(config.TPEX_QUOTES_URL)
    if rows is None or px is None:
        log("tpex_margin", "no_new_data" if all(is_empty_response(e) for e in (e1, e2) if e) else "failure", detail=_clip(f"抓取失敗：融資 {e1 or '好'}；收盤價 {e2 or '好'}"))
        return []
    need = ("Date", "SecuritiesCompanyCode", "MarginPurchaseBalancePreviousDay", "MarginPurchase", "MarginSales", "CashRedemption", "MarginPurchaseBalance")
    if not (isinstance(rows, list) and rows and isinstance(rows[0], dict) and all(k in rows[0] for k in need)) or not (isinstance(px, list) and px and isinstance(px[0], dict) and all(k in px[0] for k in ("Date", "SecuritiesCompanyCode", "Close"))):
        log("tpex_margin", "failure", detail=_clip(f"欄位與預期不符：融資 {list(rows[0].keys())[:8] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else type(rows).__name__}"))
        return []
    as_of = roc_to_iso(Counter(str(r.get("Date", "")) for r in rows if isinstance(r, dict)).most_common(1)[0][0])
    ok, why = validate_close_date(as_of)
    if not ok:
        log("tpex_margin", "failure", as_of=as_of, detail=f"資料日期異常: {why}")
        return []
    px_date = roc_to_iso(Counter(str(r.get("Date", "")) for r in px if isinstance(r, dict)).most_common(1)[0][0])
    if px_date != as_of:
        log("tpex_margin", "no_new_data", as_of=as_of, detail=f"收盤價的日期是 {px_date}，融資表是 {as_of}，兩天不一致，不算")
        return []
    close = {}
    for r in px:
        c = parse_number(r.get("Close"))
        if isinstance(r, dict) and c and c > 0:
            close[str(r.get("SecuritiesCompanyCode", "")).strip()] = c
    units = prev = n_ok = n = missing = 0
    value = 0.0
    for r in rows:
        if not isinstance(r, dict):
            continue
        a, b, c_, d_, e_ = (parse_number(r.get(k)) for k in need[2:])
        if None in (a, b, c_, d_, e_) or min(a, e_) < 0:
            continue
        n += 1
        if abs(a + b - c_ - d_ - e_) <= 1:
            n_ok += 1
        units, prev = units + int(e_), prev + int(a)
        if e_ > 0:
            code = str(r.get("SecuritiesCompanyCode", "")).strip()
            if code in close:
                value += e_ * 1000 * close[code]
            else:
                missing += 1
    held = sum(1 for r in rows if isinstance(r, dict) and (parse_number(r.get("MarginPurchaseBalance")) or 0) > 0)
    if n < 100 or n_ok / n < 0.98:
        log("tpex_margin", "failure", as_of=as_of, detail=_clip(f"{n} 列可解析，只有 {n_ok} 列通過「前日＋買進－賣出－償還＝今日」，不採用（欄位意思可能和預期不同）"))
        return []
    if not (1e5 <= units <= 5e7) or prev <= 0 or held == 0 or missing / held > 0.05:
        log("tpex_margin", "failure", as_of=as_of, detail=_clip(f"總張數 {units:,}（範圍 10 萬到 5,000 萬）、前日 {prev:,}，或有融資的 {held} 檔裡 {missing} 檔查不到收盤價（超過 5%），不採用"))
        return []
    loan, why_loan = _tpex_margin_loan(as_of, units)
    est = round(value / loan * 100, 2) if loan else None
    if est is not None and not (50 <= est <= 500):
        loan, est, why_loan = None, None, f"維持率估算 {value / loan * 100:.1f}% 超出合理範圍（50 到 500），不採用"
    log("tpex_margin", "success", as_of=as_of, detail=f"上櫃融資 {units:,} 張（前日 {prev:,}，{(units / prev - 1) * 100:+.2f}%）；融資股票市值約 {value / 1e8:,.0f} 億元（{held - missing} 檔，{missing} 檔查不到收盤價）；" +
        (f"融資金額 {loan / 1e8:,.0f} 億元，維持率估算 {est:.2f}%" if loan else f"沒有融資金額、不算維持率：{why_loan}"))
    return [{"as_of": as_of, "units": units, "prev_units": prev, "value": round(value), "n_stocks": held - missing, "n_missing": missing, "loan": round(loan) if loan else None, "est_ratio": est,
             "source": "櫃買中心開放資料（逐檔融資張數）與櫃買網站融資融券餘額頁合計列（融資金額）；維持率是自己估算的"}]


def tw_options_snapshot():
    """模組 M22：台指選擇權（TXO）與台指期（TX）的每日快照（期交所 OpenAPI）。每天存摘要與履約價未平倉量，靠每天快照比較 OI 變動。
    期交所的每日行情在收盤後更新，跟台指期基差同一個時間點（tw_close）抓。"""
    opt, e1 = fetch_taifex(config.TAIFEX_OPT_URL)
    fut, e2 = fetch_taifex(config.TAIFEX_DAILY_URL)
    if opt is None or fut is None:
        real = [e for e, d in ((e1, opt), (e2, fut)) if d is None and not is_empty_response(e)]
        if not real:                                                                                  # 只有「空的回應」：對方暫時沒內容，不當成失敗
            log("tw_options", "no_new_data", detail=_clip(f"期交所回傳空內容（選擇權：{e1 or '好'}；期貨：{e2 or '好'}）：多半是對方正在更新資料或今天沒有資料，這次先不存快照，下次再抓"))
            return []
        log("tw_options", "failure", detail=_clip(f"抓取失敗：選擇權 {e1 or '好'}；期貨 {e2 or '好'}"))
        return []
    summary, strikes, err = parse_taifex_snapshot(opt, fut)
    if err:
        log("tw_options", "failure", detail=_clip(err))
        return []
    pcr = summary["put_oi"] / summary["call_oi"] * 100
    log("tw_options", "success", as_of=summary["as_of"], detail=f"近月台指期結算價 {summary['fut_settle']:,.0f}｜台指期未平倉量合計 {summary['fut_oi']:,}｜選擇權買權 OI {summary['call_oi']:,}、賣權 OI {summary['put_oi']:,}（PCR {pcr:.1f}%）｜履約價 {len(strikes)} 檔（本月與下月、±20%）")
    return [{"summary": summary, "strikes": strikes}]


def parse_taifex_pcr(rows, today=None):
    """期交所臺指選擇權 Put/Call 比 → [列]。官方一次給約 18 天（成交量比與未平倉量比，單位 %）。
    每列驗證：比率必須和自己的賣權、買權數字對得上（誤差 1 個百分點內），否則略過那一列（代表欄位意思和我們以為的不同）。"""
    today = today or date.today()
    out, bad = [], 0
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        d_ = _taifex_date(r.get("Date"))
        pv, cv, po, co = (parse_number(r.get(k)) for k in ("PutVolume", "CallVolume", "PutOI", "CallOI"))
        rv, ro = parse_number(r.get("PutCallVolumeRatio%")), parse_number(r.get("PutCallOIRatio%"))
        if d_ is None or None in (pv, cv, po, co, rv, ro) or not (today - timedelta(days=200) <= d_ <= today) or not cv or not co:
            bad += 1
            continue
        if abs(pv / cv * 100 - rv) > 1.0 or abs(po / co * 100 - ro) > 1.0 or not (0 <= rv <= 2000 and 0 <= ro <= 2000):
            bad += 1
            continue
        out.append({"as_of": d_.isoformat(), "put_vol": int(pv), "call_vol": int(cv), "pcr_vol": rv, "put_oi": int(po), "call_oi": int(co), "pcr_oi": ro, "source": "期交所 OpenAPI（臺指選擇權 Put/Call 比）"})
    return out, bad


def tw_pcr_official():
    """模組 M23：期交所公布的臺指選擇權 Put/Call 比（官方一次給約 18 天，所以一開始就有歷史可以比）。"""
    rows, err = fetch_taifex(config.TAIFEX_PCR_URL)
    if rows is None:
        log("tw_pcr", "no_new_data" if is_empty_response(err) else "failure", detail=_clip(f"抓取失敗：{err}" + ("（對方回傳空內容，多半是暫時的）" if is_empty_response(err) else "")))
        return []
    out, bad = parse_taifex_pcr(rows)
    if not out:
        log("tw_pcr", "failure", detail=_clip(f"{len(rows) if isinstance(rows, list) else 0} 列裡沒有通過驗證的（略過 {bad} 列）；回傳開頭 {str(rows)[:100]}"))
        return []
    last = max(out, key=lambda r: r["as_of"])
    log("tw_pcr", "success", as_of=last["as_of"], detail=f"{len(out)} 天（略過 {bad} 列）；最新 PCR 未平倉量比 {last['pcr_oi']}%、成交量比 {last['pcr_vol']}%")
    return out


NASDAQ_SECTOR_ZH = {"Technology": "資訊科技", "Health Care": "醫療保健", "Finance": "金融", "Consumer Discretionary": "非必需消費", "Consumer Staples": "必需消費", "Industrials": "工業",
                    "Energy": "能源", "Utilities": "公用事業", "Basic Materials": "原物料", "Real Estate": "不動產", "Telecommunications": "通訊服務", "Miscellaneous": "其他／ETF"}


def parse_nasdaq_profile(j, symbol):
    """Nasdaq 公司簡介（api.nasdaq.com/api/company/{symbol}/company-profile）→ ({產業列}, 原因)。
    只信 data 裡的 Sector 與 Industry；代號對不上、查不到、產業名稱不在對照表（不猜）就回 None 與原因。分類是 Nasdaq 自己的，和 GICS 不完全相同。"""
    d = (j or {}).get("data") if isinstance(j, dict) else None
    if not isinstance(d, dict) or not d:
        return None, "沒有公司簡介（ETF 或查不到這個代號）"
    val = lambda k: ((d.get(k) or {}).get("value") if isinstance(d.get(k), dict) else None)
    sym = str(val("Symbol") or "").strip().upper()
    if sym and sym != symbol.upper():
        return None, f"回傳的代號是 {sym}，不是 {symbol}，不採用"
    sec = str(val("Sector") or "").strip()
    if not sec:
        return None, "公司簡介裡沒有 Sector（產業）"
    zh = NASDAQ_SECTOR_ZH.get(sec)
    if zh is None:
        return None, f"Nasdaq 的產業名稱「{sec}」不在對照表（共 {len(NASDAQ_SECTOR_ZH)} 種），不猜，請你自己指定"
    ind = str(val("Industry") or "").strip() or None
    return {"symbol": symbol.upper(), "sector_en": sec, "industry_en": ind, "sector": zh, "source": "Nasdaq 公司簡介（分類和 GICS 不完全相同）"}, None


def parse_nasdaq_summary(j):
    """Nasdaq 個股摘要（/api/quote/{代號}/summary）→ (本益比、1 年預估本益比、每股盈餘、前一日收盤)。
    摘要裡每個項目是 {label, value}。先用已知的鍵名找，找不到再用標籤文字找（P/E Ratio、Forward P/E、Earnings Per Share、Previous Close），
    所以鍵名改了也不會整個壞掉；數字是 "N/A"、空的、不合理（超出範圍）的都當作沒有，不冒充。回傳 (dict, None) 或 (None, 原因)。"""
    sd = (((j or {}).get("data") or {}).get("summaryData")) if isinstance(j, dict) else None
    if not isinstance(sd, dict) or not sd:
        return None, "沒有 summaryData（ETF、代號不存在或格式變了）"

    def pick(keys, label_re, avoid=None):
        for k in keys:
            it = sd.get(k)
            if isinstance(it, dict) and "value" in it:
                return it.get("value")
        for it in sd.values():
            lab = str(it.get("label", "")) if isinstance(it, dict) else ""
            if isinstance(it, dict) and re.search(label_re, lab, re.I) and not (avoid and re.search(avoid, lab, re.I)):
                return it.get("value")
        return None

    def num(v, lo, hi):
        t = re.sub(r"[$,\s]", "", str(v if v is not None else ""))
        if t.upper() in ("", "N/A", "NA", "--", "-"):
            return None
        try:
            x = float(t)
        except ValueError:
            return None
        return x if lo < x <= hi else None
    return {"pe": num(pick(("PERatio", "PeRatio", "peRatio"), r"P/?E|Price\s*/\s*Earnings", r"Forward|Fwd|PEG|Est"), 0, 3000), "forward_pe": num(pick(("ForwardPE1Yr", "ForwardPE"), r"(Forward|Fwd).*P/?E|P/?E.*(Forward|Fwd)"), 0, 3000),
            "eps": num(pick(("EarningsPerShare",), r"Earnings Per Share|\bEPS\b"), -1000, 100000), "prev_close": num(pick(("PreviousClose",), r"Previous Close"), 0, 1e7)}, None


def tw_otc_index():
    """模組 M40：櫃買指數的每日收盤（櫃買中心開放資料 /tpex_index：只有最近約 10 個交易日，沒有日期參數，所以靠每天累積）。
    每天驗證：日期是 YYYYMMDD 且不在未來、收盤在合理範圍內；不合格的略過。回傳要寫進 tw_otc_index 的列。"""
    obj, err = fetch_json(config.TPEX_DEFAULT_BASE + "/tpex_index", headers=DASH_HEADERS)
    if not isinstance(obj, list):
        log("tw_otc_index", "no_new_data", detail=_clip(f"櫃買指數抓不到或格式不對：{err or type(obj).__name__}"))
        return []
    low, high = config.OTC_INDEX_RANGE
    out, skipped, today = [], 0, date.today()
    for r in obj:
        if not isinstance(r, dict):
            skipped += 1
            continue
        raw = str(r.get("Date", "")).strip()
        try:
            d_ = datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            skipped += 1
            continue
        close = parse_number(r.get("Close"))
        if d_ > today or close is None or not (low <= close <= high):
            skipped += 1
            continue
        out.append({"as_of": d_.isoformat(), "open": parse_number(r.get("Open")), "high": parse_number(r.get("High")), "low": parse_number(r.get("Low")), "close": close, "change": parse_number(r.get("Change")), "source": "櫃買中心開放資料 tpex_index"})
    out.sort(key=lambda x: x["as_of"])
    if not out:
        log("tw_otc_index", "no_new_data", detail=_clip(f"沒有可用的櫃買指數資料（略過 {skipped} 筆不符的）"))
        return []
    log("tw_otc_index", "success", as_of=out[-1]["as_of"], detail=_clip(f"櫃買指數 {out[-1]['close']}（{out[-1]['as_of']}）；{len(out)} 天（{out[0]['as_of']} 到 {out[-1]['as_of']}）" + (f"；略過 {skipped} 筆不符的" if skipped else "")))
    return out


def tw_rel_strength(price_rows):
    """模組 M41：台股持股與候選股「近 5 日、近 20 日」的漲跌，給網站和大盤比較用（上市對加權指數、上櫃對櫃買指數）。
    用自建歷史 tw_hist（不指定市場，從歷史列本身看這檔是上市還是上櫃）。每一項都要通過檢查才給：
    5 日前那一筆要在 5 到 12 個日曆天之前、20 日前那一筆要在 25 到 45 個日曆天之前（歷史有缺漏就不算，不拿更早的充數）；最新一筆距今不能超過 10 天。
    寫進獨立的 tw_rel_strength（不動 stock_prices）：這個資料表沒建好時，只有這一項失敗。"""
    if not sb_ready():
        log("tw_rel_strength", "skipped", detail="未設定 Supabase，不算個股對大盤")
        return []
    codes = {str(r.get("code")).strip() for r in (price_rows or []) if r.get("market") == "TW" and r.get("code")}
    wl, _e = sb_request("GET", "private_watchlist?select=code&limit=500")
    for r in (wl or []):
        c_ = str((r or {}).get("code", "")).strip()
        if re.fullmatch(r"\d{4,6}[A-Z]?", c_):
            codes.add(c_)
    if not codes:
        log("tw_rel_strength", "no_new_data", detail="沒有台股持股或候選股")
        return []
    since = (date.today() - timedelta(days=70)).isoformat()
    out, miss = [], []
    for code in sorted(codes):
        hist, err = sb_request("GET", f"tw_hist?select=market,as_of,close&code=eq.{code}&as_of=gte.{since}&order=as_of.asc&limit=300")
        rows = []
        for r in (hist if isinstance(hist, list) else []):
            try:
                rows.append({"market": str(r["market"]), "as_of": str(r["as_of"]), "close": float(r["close"])})
            except (KeyError, TypeError, ValueError):
                continue
        if not rows:
            miss.append(f"{code}（自建歷史沒有這檔）")
            continue
        mk = rows[-1]["market"]
        rows = [r for r in rows if r["market"] == mk and r["close"] > 0]
        last = rows[-1]
        d0 = date.fromisoformat(last["as_of"])
        if (date.today() - d0).days > 10:
            miss.append(f"{code}（最新一筆 {last['as_of']} 太舊）")
            continue
        prior = rows[:-1]
        row = {"code": code, "mkt": mk, "as_of": last["as_of"], "close": last["close"], "ret_5d": None, "ret_5d_from": None, "ret_20d": None, "ret_20d_from": None}
        if len(prior) >= 5 and 5 <= (d0 - date.fromisoformat(prior[-5]["as_of"])).days <= 12:
            row["ret_5d"], row["ret_5d_from"] = round((last["close"] / prior[-5]["close"] - 1) * 100, 2), prior[-5]["as_of"]
        if len(prior) >= 20 and 25 <= (d0 - date.fromisoformat(prior[-20]["as_of"])).days <= 45:
            row["ret_20d"], row["ret_20d_from"] = round((last["close"] / prior[-20]["close"] - 1) * 100, 2), prior[-20]["as_of"]
        out.append(row)
    n5 = sum(1 for r in out if r["ret_5d"] is not None)
    log("tw_rel_strength", "success" if out else "no_new_data", detail=_clip(f"{len(codes)} 檔：算出 {len(out)} 檔（其中近 5 日 {n5} 檔；上市 {sum(1 for r in out if r['mkt'] == 'TWSE')}、上櫃 {sum(1 for r in out if r['mkt'] == 'TPEX')}）" + (f"；沒有：{'、'.join(miss[:8])}" if miss else "")))
    return out


def calc_beta(stock_close, index_close, min_pairs=120):
    """Beta ＝ 個股日報酬對指數日報酬的迴歸斜率（共變異 ÷ 指數變異）。兩邊都有收盤價的日子才用；
    相鄰兩個共同日期相隔超過 6 天（資料有缺漏）的那一段報酬不算；有效報酬不到 min_pairs 個就不給（回傳 (None, 筆數)）；結果超出 -3 到 8 視為壞資料。
    stock_close、index_close：{日期字串: 收盤價}。回傳 (beta, 報酬筆數)。"""
    days = sorted(set(stock_close) & set(index_close))
    xs, ys = [], []
    for a, b in zip(days, days[1:]):
        if (date.fromisoformat(b) - date.fromisoformat(a)).days > 6:
            continue
        s0, s1, i0, i1 = stock_close[a], stock_close[b], index_close[a], index_close[b]
        if min(s0, s1, i0, i1) <= 0:
            continue
        ys.append(s1 / s0 - 1)
        xs.append(i1 / i0 - 1)
    n = len(xs)
    if n < min_pairs:
        return None, n
    mx, my = sum(xs) / n, sum(ys) / n
    var = sum((x - mx) ** 2 for x in xs)
    if var <= 0:
        return None, n
    beta = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var
    return (round(beta, 2), n) if -3 <= beta <= 8 else (None, n)


def parse_nasdaq_short_interest(j):
    """Nasdaq 放空餘額（/api/quote/{代號}/short-interest）→ 最新一期 {settle, shares, days_to_cover}；沒有或格式不對回傳 None。
    表格列要有 settlementDate（MM/DD/YYYY）與 interest（股數）；daysToCover 缺就留空。每期是每月兩次公布的，日期取表裡最新的那一列。"""
    rows = ((((j or {}).get("data") or {}).get("shortInterestTable") or {}).get("rows")) if isinstance(j, dict) else None
    best = None
    for r in (rows or []):
        iso = None
        if isinstance(r, dict):
            m_ = re.fullmatch(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*", str(r.get("settlementDate", "")))
            try:
                iso = date(int(m_.group(3)), int(m_.group(1)), int(m_.group(2))).isoformat() if m_ else parse_us_date(r.get("settlementDate"))
            except ValueError:
                iso = None
        sh = parse_number(str(r.get("interest", "")).replace(",", "")) if isinstance(r, dict) else None
        if iso and sh is not None and sh >= 0 and (best is None or iso > best["settle"]):
            dtc = parse_number(str(r.get("daysToCover", "")))
            best = {"settle": iso, "shares": sh, "days_to_cover": dtc if dtc is not None and 0 <= dtc < 1000 else None}
    return best


def parse_nasdaq_institutional(j):
    """Nasdaq 機構持股（/api/company/{代號}/institutional-holdings）→ {pct, shares_out}；pct 是「機構持股占流通在外股數 %」。
    先找 ownershipSummary 裡鍵名是 SharesOutstandingPCT 的項目，找不到再用標籤文字（含 Shares Outstanding 與 %）找；
    shares_out 是總股數：標籤有 millions 就乘 1,000,000。不合理（機構持股 ≤ 0 或 > 300%）的當作沒有。"""
    d = ((j or {}).get("data") or {}) if isinstance(j, dict) else {}
    s = d.get("ownershipSummary") if isinstance(d.get("ownershipSummary"), dict) else {}
    pct, out = None, None
    it = s.get("SharesOutstandingPCT")
    if not isinstance(it, dict):
        it = next((v for v in s.values() if isinstance(v, dict) and re.search(r"Outstanding", str(v.get("label", "")), re.I) and "%" in str(v.get("value", v.get("label", "")))), None)
    if isinstance(it, dict):
        pct = parse_number(re.sub(r"[%\s]", "", str(it.get("value", ""))))
        if pct is not None and not (0 < pct <= 300):
            pct = None
    tot = s.get("ShareoutstandingTotal") if isinstance(s.get("ShareoutstandingTotal"), dict) else next((v for k, v in s.items() if isinstance(v, dict) and re.search(r"Total.*Shares.*Outstanding|Shares.*Outstanding.*Total", str(v.get("label", "")), re.I) and "PCT" not in k.upper()), None)
    if isinstance(tot, dict):
        v = parse_number(str(tot.get("value", "")).replace(",", ""))
        if v is not None and v > 0:
            out = v * 1_000_000 if re.search(r"million", str(tot.get("label", "")), re.I) else v
    return {"pct": pct, "shares_out": out} if (pct is not None or out is not None) else None


def stock_risk_stats(holdings, tw_codes=None):
    """模組 M45：Beta（近一年日報酬對指數：美股對 SPY、上市對加權、上櫃對櫃買）、美股放空餘額占股數比例（軋空燃料）、美股機構持股比例。
    美股：Nasdaq 歷史行情、放空餘額、機構持股（皆為資料商／交易所公布，放空餘額每月兩次，不是每日）。放空占比 ＝ 放空股數 ÷ 總股數
    （總股數優先用機構持股頁的總股數，沒有就用「市值 ÷ 收盤價」；這是占「總股數」不是占「流通股數」，流通股數通常略少，所以實際占流通比例會略高一點，網站會寫明）。
    ETF 沒有放空占比與機構持股，只算 Beta。台股：用自建 tw_hist 與加權／櫃買指數算 Beta，沒有放空與機構資料。查不到的欄位留空，不編。"""
    out, notes = [], []
    us = sorted({str(h.get("symbol")).strip().upper() for h in (holdings or []) if h.get("market") == "US" and h.get("symbol")})
    kinds = {str(h.get("symbol")).strip().upper(): h.get("kind") for h in (holdings or []) if h.get("market") == "US" and h.get("symbol")}
    if us:
        spy, why = _nasdaq_rows("SPY", "etf", 400)
        spy_c = {r["date"].isoformat(): r["close"] for r in (spy or [])}
        if not spy_c:
            notes.append(f"SPY 歷史取不到（{why}），美股不算 Beta")
        for sym in us:
            is_etf = kinds.get(sym) in ("etf", "leveraged_etf")
            row = {"market": "US", "code": sym, "beta": None, "beta_n": None, "short_shares": None, "short_pct": None, "days_to_cover": None, "short_as_of": None, "inst_pct": None, "source": "Nasdaq（歷史行情自算 Beta；放空餘額、機構持股為資料商公布）"}
            rows_, w = _nasdaq_rows(sym, "etf" if is_etf else "stocks", 400)
            time.sleep(1)
            last_px = rows_[-1]["close"] if rows_ else None
            if rows_ and spy_c:
                row["beta"], row["beta_n"] = calc_beta({r["date"].isoformat(): r["close"] for r in rows_}, spy_c)
            elif not rows_:
                notes.append(f"{sym} 歷史（{w}）")
            if not is_etf:
                shares_out = None
                st, ct, body, err = fetch_raw(f"https://api.nasdaq.com/api/company/{sym}/institutional-holdings?limit=1&type=TOTAL&sortColumn=marketValue&sortOrder=DESC", timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
                time.sleep(1)
                try:
                    inst = parse_nasdaq_institutional(json.loads(body.decode("utf-8-sig"))) if st == 200 else None
                except (ValueError, UnicodeDecodeError):
                    inst = None
                if inst:
                    row["inst_pct"], shares_out = inst["pct"], inst["shares_out"]
                else:
                    notes.append(f"{sym} 機構持股（HTTP {st}）")
                st, ct, body, err = fetch_raw(f"https://api.nasdaq.com/api/quote/{sym}/short-interest?assetClass=stocks", timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
                time.sleep(1)
                try:
                    si = parse_nasdaq_short_interest(json.loads(body.decode("utf-8-sig"))) if st == 200 else None
                except (ValueError, UnicodeDecodeError):
                    si = None
                if si:
                    row["short_shares"], row["days_to_cover"], row["short_as_of"] = si["shares"], si["days_to_cover"], si["settle"]
                    if shares_out is None and last_px:
                        st, ct, body, err = fetch_raw(f"https://api.nasdaq.com/api/quote/{sym}/summary?assetclass=stocks", timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
                        time.sleep(1)
                        try:
                            mc = (((json.loads(body.decode("utf-8-sig")).get("data") or {}).get("summaryData") or {}).get("MarketCap") or {}).get("value") if st == 200 else None
                            mcv = parse_number(re.sub(r"[$,\s]", "", str(mc or "")))
                            shares_out = mcv / last_px if mcv and mcv > 0 else None
                        except (ValueError, UnicodeDecodeError, AttributeError):
                            shares_out = None
                    if shares_out and shares_out > 0:
                        pct = si["shares"] / shares_out * 100
                        row["short_pct"] = round(pct, 2) if 0 <= pct <= 150 else None
                else:
                    notes.append(f"{sym} 放空餘額（HTTP {st}）")
            out.append(row)
    codes = sorted({str(c).strip() for c in (tw_codes or []) if c} | (_tw_watch_codes() if sb_ready() else set()))
    if codes and sb_ready():
        twse, e1 = sb_request("GET", "market_stats?select=as_of,close&market=eq.TW&source=eq.TWSE%20FMTQIK&order=as_of.desc&limit=400")
        otc, e2 = sb_request("GET", "tw_otc_index?select=as_of,close&order=as_of.desc&limit=400")
        mk_idx = {"TWSE": {str(r["as_of"]): float(r["close"]) for r in (twse if isinstance(twse, list) else []) if r.get("close")},
                  "TPEX": {str(r["as_of"]): float(r["close"]) for r in (otc if isinstance(otc, list) else []) if r.get("close")}}
        since = (date.today() - timedelta(days=380)).isoformat()
        for code in codes:
            hist, err = sb_request("GET", f"tw_hist?select=market,as_of,close&code=eq.{code}&as_of=gte.{since}&order=as_of.asc&limit=1000")
            rows_ = [r for r in (hist if isinstance(hist, list) else []) if r.get("close") is not None]
            if not rows_:
                notes.append(f"{code} 自建歷史沒有")
                continue
            mk = "TWSE" if str(rows_[-1].get("market")) == "TWSE" else "TPEX"
            beta, n = calc_beta({str(r["as_of"]): float(r["close"]) for r in rows_ if str(r.get("market")) == str(rows_[-1].get("market"))}, mk_idx[mk])
            if beta is None:
                notes.append(f"{code} Beta 不夠資料（{n} 筆報酬）")
            out.append({"market": "TW", "code": code, "beta": beta, "beta_n": n if beta is not None else None, "short_shares": None, "short_pct": None, "days_to_cover": None, "short_as_of": None, "inst_pct": None,
                        "source": "自建歷史行情對" + ("加權指數" if mk == "TWSE" else "櫃買指數") + "的近一年日報酬自算"})
    n_b, n_s, n_i = sum(1 for r in out if r["beta"] is not None), sum(1 for r in out if r["short_pct"] is not None), sum(1 for r in out if r["inst_pct"] is not None)
    log("stock_risk_stats", "success" if out else "no_new_data", detail=_clip(f"{len(out)} 檔：Beta {n_b}、放空占比 {n_s}、機構持股 {n_i}" + (f"；沒有：{'、'.join(notes[:10])}" if notes else "")))
    return out


def _tw_watch_codes():
    """台股候選股代號（private_watchlist），讀不到就回空集合。"""
    wl, _e = sb_request("GET", "private_watchlist?select=code&limit=500")
    return {str(r.get("code")).strip() for r in (wl if isinstance(wl, list) else []) if isinstance(r, dict) and re.fullmatch(r"\d{4,6}[A-Z]?", str(r.get("code") or "").strip())}


def nasdaq_summary_items(j, limit=45):
    """診斷用：把 Nasdaq 個股摘要裡每個項目寫成「鍵｜標籤｜值」，看實際有哪些欄位（本益比抓不到時用）。"""
    sd = (((j or {}).get("data") or {}).get("summaryData")) if isinstance(j, dict) else None
    if not isinstance(sd, dict):
        return "沒有 summaryData；頂層鍵 " + (str(list(j.keys())[:8]) if isinstance(j, dict) else type(j).__name__) + (f"；data 的鍵 {list((j.get('data') or {}).keys())[:10]}" if isinstance(j, dict) and isinstance(j.get("data"), dict) else "")
    return " ## ".join(f"{k}｜{(it or {}).get('label') if isinstance(it, dict) else ''}｜{(it or {}).get('value') if isinstance(it, dict) else it}" for k, it in list(sd.items())[:limit])


def us_valuation(holdings):
    """模組 M39：美股持股與候選股的本益比（過去四季）與 1 年預估本益比、每股盈餘（Nasdaq 個股摘要，資料商提供）。
    只查個股（ETF、槓桿 ETF 沒有）；預估本益比是資料商的預估，不是共識中位數，也不是你自己輸入的。查不到就不寫；
    摘要有拿到但本益比是 N/A（例如虧損）也寫一列，欄位留空，讓網站能分辨「Nasdaq 沒提供」和「還沒抓過」。"""
    syms = sorted({str(h.get("symbol")).strip().upper() for h in (holdings or []) if h.get("market") == "US" and h.get("symbol") and h.get("kind") not in ("etf", "leveraged_etf")})
    if not syms:
        log("us_valuation", "skipped", detail="沒有美股個股代號，不查本益比")
        return []
    out, miss, shape = [], [], None
    for sym in syms:
        status, ctype, body, err = fetch_raw(f"https://api.nasdaq.com/api/quote/{sym}/summary?assetclass=stocks", timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
        time.sleep(1)
        if status != 200:
            miss.append(f"{sym}（HTTP {status} {err}）".strip())
            continue
        try:
            j_ = json.loads(body.decode("utf-8-sig"))
            vals, why = parse_nasdaq_summary(j_)
        except (ValueError, UnicodeDecodeError):
            miss.append(f"{sym}（不是 JSON）")
            continue
        if shape is None or (vals is not None and vals.get("pe") is None and "（" not in shape.split("｜")[0]):
            shape = f"{sym}｜" + nasdaq_summary_items(j_)                                 # 第一檔、或第一檔本益比抓不到的：把實際欄位記下來
        if vals is None:
            miss.append(f"{sym}（{why}）")
            continue
        src = "Nasdaq 個股摘要（本益比用過去四季盈餘；預估本益比是資料商的 1 年預估，不是共識中位數）"
        if vals.get("pe") is None and vals.get("eps") and vals["eps"] > 0 and vals.get("prev_close"):          # Nasdaq 沒直接給本益比，但有每股盈餘與前一日收盤：自己算並標明
            vals = dict(vals, pe=round(vals["prev_close"] / vals["eps"], 2))
            src = "Nasdaq 個股摘要（本益比是「前一日收盤 ÷ 每股盈餘」自己算的，因為 Nasdaq 沒直接提供；預估本益比是資料商的 1 年預估，不是共識中位數）"
        out.append({"symbol": sym, **vals, "source": src})
    n_pe, n_fw = sum(1 for r in out if r["pe"] is not None), sum(1 for r in out if r["forward_pe"] is not None)
    if shape:
        log("us_valuation_shape", "success", detail=_clip("Nasdaq 摘要實際的欄位（鍵｜標籤｜值）：" + shape, 1800))
    log("us_valuation", "success" if out else "no_new_data", detail=_clip(f"{len(syms)} 檔個股：摘要有 {len(out)} 檔（有本益比 {n_pe}、有預估本益比 {n_fw}）" + (f"；沒有：{'、'.join(miss[:8])}" if miss else "")))
    return out


def leveraged_underlying(name):
    """槓桿 ETF 的名稱（例如「2倍做多MU ETF」「2倍做空TSLA ETF」「2X Long NVDA ETF」）→ 標的股票代號（MU、TSLA、NVDA）。
    取名稱裡第一個全大寫的英文字（1 到 5 個字母，可有 .X），排除 ETF、ETN、X 這類不是代號的字；看不出來就回傳 None（不猜）。"""
    skip = {"ETF", "ETN", "ETP", "X", "USD", "USA", "BULL", "BEAR", "LONG", "SHORT", "ULTRA"}
    for tok in re.findall(r"(?<![A-Za-z])[A-Z]{1,5}(?:\.[A-Z])?(?![A-Za-z])", str(name or "")):
        if tok not in skip:
            return tok
    return None


def us_sector_lookup(holdings):
    """模組 M26：美股持股的產業，從 Nasdaq 公司簡介自動帶入（你在網頁自己指定的永遠優先）。已經查過、不到 45 天的不重查；
    ETF 沒有公司簡介，查不到只記沒有資料（不會讓工作變紅）。每檔之間間隔 1 秒。"""
    syms = sorted({str(h.get("symbol")).strip().upper() for h in (holdings or []) if h.get("market") == "US" and h.get("symbol")})
    if not syms:
        log("us_sector", "skipped", detail="沒有美股持股代號，不查產業")
        return []
    target = {}                                                       # 槓桿 ETF 本身沒有產業：看它的標的股票（名稱裡的代號）屬於哪一個產業
    for h in (holdings or []):
        s = str(h.get("symbol") or "").strip().upper()
        if h.get("market") == "US" and s and h.get("kind") == "leveraged_etf":
            u = leveraged_underlying(h.get("name"))
            if u and u != s:
                target[s] = u
    fresh = set()
    if sb_ready():
        got, err = sb_request("GET", "us_sector_auto?select=symbol,fetched_at&limit=1000")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
        fresh = {r["symbol"] for r in (got or []) if str(r.get("fetched_at", "")) >= cutoff}
    out, miss, cache = [], [], {}
    for sym in syms:
        if sym in fresh:
            continue
        look = target.get(sym, sym)                                   # 實際去查哪一檔的公司簡介（槓桿 ETF 查它的標的）
        if look in cache:
            row, why = cache[look]
        else:
            status, ctype, body, err = fetch_raw(f"https://api.nasdaq.com/api/company/{look}/company-profile", timeout=config.PROBE_TIMEOUT, headers=NASDAQ_HEADERS)
            time.sleep(1)
            if status != 200:
                row, why = None, f"HTTP {status} {err}".strip()
            else:
                try:
                    row, why = parse_nasdaq_profile(json.loads(body.decode("utf-8-sig")), look)
                except (ValueError, UnicodeDecodeError):
                    row, why = None, "不是 JSON"
            cache[look] = (row, why)
        if row is None:
            miss.append(f"{sym}" + (f"（標的 {look}：{why}）" if look != sym else f"（{why}）"))
        elif look != sym:                                             # 槓桿 ETF：用標的的產業，來源寫明
            out.append(dict(row, symbol=sym, source=f"依標的 {look} 的 Nasdaq 公司簡介（槓桿 ETF 本身沒有產業；分類和 GICS 不完全相同）"))
        else:
            out.append(row)
    log("us_sector", "success" if out else "no_new_data",
        detail=_clip(f"{len(syms)} 檔美股持股：新查到 {len(out)} 檔（" + "、".join(f"{r['symbol']}→{r['sector_en']}" + (f"（依標的 {target[r['symbol']]}）" if r['symbol'] in target else "") for r in out[:8]) + f"）；{len(fresh & set(syms))} 檔 45 天內查過" + (f"；查不到：{'、'.join(miss[:6])}" if miss else "")))
    return out


_FM_Q = {"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4}
_FM_TYPES = {"Revenue": "revenue", "GrossProfit": "gross_profit", "OperatingIncome": "op_income", "IncomeAfterTaxes": "net_income"}


def parse_finmind_statements(rows):
    """FinMind 綜合損益表（TaiwanStockFinancialStatements，一列一個項目）→ {(年, 季): {revenue, gross_profit, op_income, net_income}}。
    日期必須是季底（3/31、6/30、9/30、12/31）；四個項目都有才收；同一季重複就取最後一列。是單季值還是累計值，之後再判斷。"""
    tmp = {}
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date", ""))
        q, k, v = _FM_Q.get(d[5:]), _FM_TYPES.get(r.get("type")), parse_number(r.get("value"))
        if q is None or k is None or v is None or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
            continue
        tmp.setdefault((int(d[:4]), q), {})[k] = v
    return {yq: vals for yq, vals in tmp.items() if len(vals) == 4}


def detect_finmind_basis(by_code):
    """判斷 FinMind 給的是「單季值」還是「年初到本季的累計值」：看每一檔每一年（四季都有的）第 4 季營收 ÷ 第 1 季營收。
    累計值的第 4 季是全年（約第 1 季的 4 倍，幾乎一定 ≥ 2.5）；單季值的第 4 季通常在 0.5 到 2 倍之間。
    至少要有 4 個「公司年度」才判斷；90% 以上 ≥ 2.5 判為累計、80% 以上 < 2.5 判為單季，其他情況不判斷（回傳 None），寧可不寫也不猜。"""
    ratios = []
    for code, ser in by_code.items():
        for y in sorted({y for (y, q) in ser}):
            if all((y, q) in ser for q in (1, 2, 3, 4)) and ser[(y, 1)]["revenue"] > 0:
                ratios.append(ser[(y, 4)]["revenue"] / ser[(y, 1)]["revenue"])
    n = len(ratios)
    stats = {"n": n, "median": sorted(ratios)[n // 2] if n else None, "high": sum(1 for x in ratios if x >= 2.5)}
    if n < 4:
        return None, stats
    if stats["high"] / n >= 0.9:
        return "cumulative", stats
    if (n - stats["high"]) / n >= 0.8:
        return "single", stats
    return None, stats


def finmind_to_single(ser, basis):
    """累計值換成單季（第 1 季本來就是單季；第 q 季＝第 q 季累計 − 第 q−1 季累計，缺任何一季就略過）。回傳 {(年, 季): 單季值}。"""
    if basis == "single":
        return dict(ser)
    out = {}
    for (y, q), v in ser.items():
        if q == 1:
            out[(y, q)] = dict(v)
        elif (y, q - 1) in ser:
            out[(y, q)] = {k: v[k] - ser[(y, q - 1)][k] for k in v}
    return out


def _flex_date(raw):
    """民國 7 位數（1151106）或西元 8 位數（20261106）→ ISO 日期；不合法回傳 None。"""
    d = re.sub(r"\D", "", str(raw or ""))
    try:
        if len(d) == 7:
            return date(int(d[:3]) + 1911, int(d[3:5]), int(d[5:])).isoformat()
        if len(d) == 8:
            return date(int(d[:4]), int(d[4:6]), int(d[6:])).isoformat()
    except ValueError:
        return None
    return None


def _in_range(raw, lo, hi, lo_open=False):
    v = parse_number(raw)
    return v if v is not None and (lo < v if lo_open else lo <= v) and v <= hi else None


def _watch_hist_stats(code, market, as_of, close):
    """候選股的行情統計（用自建歷史 tw_hist：整個市場每天一筆，約一年）：52 週最高／最低價與最高價那一天、前一個交易日收盤與漲跌、近 20 個交易日漲跌。
    每一項都要通過檢查才給：52 週要有足夠的天數與涵蓋範圍；前一筆不能比資料日早超過 10 天；20 日前那一筆要在 25 到 45 個日曆天之前（歷史有缺漏就不算）。
    今天的收盤價如果還沒進歷史（歷史晚一天），就用證交所／櫃買的收盤價補進去比較。"""
    if close is None or not as_of:
        return {}
    since = (date.fromisoformat(as_of) - timedelta(days=365)).isoformat()
    hist, err = sb_request("GET", f"tw_hist?select=as_of,close,high,low&market=eq.{market}&code=eq.{code}&as_of=gte.{since}&order=as_of.asc&limit=1000")
    rows = []
    for r in (hist if isinstance(hist, list) else []):
        try:
            rows.append({"as_of": str(r["as_of"]), "close": float(r["close"]), "high": float(r["high"]), "low": float(r["low"])})
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        log(f"watch_hist:{code}", "no_new_data", detail=_clip(f"自建歷史裡沒有 {code}（{err or '空的'}）：不算 52 週與漲跌"))
        return {}
    out, d0 = {}, date.fromisoformat(as_of)
    prior = [r for r in rows if r["as_of"] < as_of]
    if prior and prior[-1]["close"] > 0 and (d0 - date.fromisoformat(prior[-1]["as_of"])).days <= 10:
        out["prev_close"], out["chg_pct"] = prior[-1]["close"], round((close / prior[-1]["close"] - 1) * 100, 2)
    if len(prior) >= 20 and prior[-20]["close"] > 0 and 25 <= (d0 - date.fromisoformat(prior[-20]["as_of"])).days <= 45:
        out["ret_20d"] = round((close / prior[-20]["close"] - 1) * 100, 2)
    span = (date.fromisoformat(rows[-1]["as_of"]) - date.fromisoformat(rows[0]["as_of"])).days
    if len(rows) >= config.RANGE52_MIN_ROWS and span >= config.RANGE52_MIN_COVERAGE_DAYS:
        hi_row = max(rows, key=lambda r: r["high"]); lo = min(r["low"] for r in rows)
        hi, hi_date = (hi_row["high"], hi_row["as_of"]) if hi_row["high"] >= close else (close, as_of)
        out.update({"high_52w": hi, "high_52w_date": hi_date, "low_52w": min(lo, close), "range_source": f"自建歷史（{len(rows)} 個交易日）"})
    else:
        log(f"watch_hist:{code}", "no_new_data", detail=f"歷史只有 {len(rows)} 個交易日、涵蓋 {span} 天，不足以算 52 週高低")
    return out


def watch_valuation():
    """模組 M30：候選股（你在網站輸入的 private_watchlist）的收盤價、本益比、殖利率、股價淨值比，用證交所與櫃買中心的全市場表查。
    本益比是用過去四季盈餘算的（不是預估）；虧損的公司沒有本益比，留空。代號兩個市場都查不到就列出來（可能是興櫃、已下市或代號打錯），不寫入。
    四個來源裡有失敗的只影響該來源的欄位；全部都失敗才算失敗。"""
    if not sb_ready():
        log("watch", "skipped", detail="未設定 Supabase，不查候選股")
        return []
    got, err = sb_request("GET", "private_watchlist?select=code&limit=500")
    if got is None:
        if "PGRST205" in str(err) or "HTTP 404" in str(err):                                 # 資料表還不存在＝還沒啟用候選股功能，不是壞掉
            log("watch", "no_new_data", detail=_clip("候選股清單的資料表還不存在（還沒執行 new_tables_v58.sql）：沒有要用候選股功能的話可以忽略；要用，請執行 new_tables_v58.sql，再重新跑一次。"))
        else:
            log("watch", "failure", detail=_clip(f"讀取候選股清單失敗：{err}（如果是資料表不存在，請先執行 new_tables_v58.sql）"))
        return []
    codes = sorted({str(r.get("code", "")).strip() for r in got if isinstance(r, dict) and re.fullmatch(r"\d{4,6}[A-Z]?", str(r.get("code", "")).strip())})
    if not codes:
        log("watch", "no_new_data", detail="候選股清單是空的")
        return []
    srcs = (("twse_px", config.TWSE_AVG_URL), ("twse_pe", config.TWSE_OPENAPI_BASE + "/exchangeReport/BWIBBU_ALL"), ("tpex_px", config.TPEX_QUOTES_URL), ("tpex_pe", config.TPEX_DEFAULT_BASE + "/tpex_mainboard_peratio_analysis"))
    keyf = {"twse_px": "Code", "twse_pe": "Code", "tpex_px": "SecuritiesCompanyCode", "tpex_pe": "SecuritiesCompanyCode"}
    idx, errs = {}, []
    for k, u in srcs:
        j, e = fetch_json(u)
        if isinstance(j, list) and j and isinstance(j[0], dict) and keyf[k] in j[0]:
            idx[k] = {str(r.get(keyf[k], "")).strip(): r for r in j if isinstance(r, dict)}
        else:
            errs.append(f"{k}（{e or '格式非預期'}）")
    if not idx:
        log("watch", "no_new_data" if all("空的回應" in x for x in errs) else "failure", detail=_clip("四個來源都抓不到：" + "、".join(errs)))
        return []
    out, missing, bad_date = [], [], []
    today = date.today().isoformat()
    for code in codes:
        if code in idx.get("twse_px", {}) or code in idx.get("twse_pe", {}):
            mk, px, pe = "TWSE", idx.get("twse_px", {}).get(code), idx.get("twse_pe", {}).get(code)
            name = str((px or pe).get("Name", "")).strip() or code
            close, pev, pbv, yv = (px or {}).get("ClosingPrice"), (pe or {}).get("PEratio"), (pe or {}).get("PBratio"), (pe or {}).get("DividendYield")
        elif code in idx.get("tpex_px", {}) or code in idx.get("tpex_pe", {}):
            mk, px, pe = "TPEX", idx.get("tpex_px", {}).get(code), idx.get("tpex_pe", {}).get(code)
            name = str((px or pe).get("CompanyName", "")).strip() or code
            close, pev, pbv, yv = (px or {}).get("Close"), (pe or {}).get("PriceEarningRatio"), (pe or {}).get("PriceBookRatio"), (pe or {}).get("YieldRatio")
        else:
            missing.append(code)
            continue
        as_of = _flex_date((px or pe).get("Date")) if (px or pe) else None
        v_as_of = _flex_date(pe.get("Date")) if pe else None
        if not as_of or as_of > today or (v_as_of and v_as_of > today):
            bad_date.append(code)
            continue
        row = {"code": code, "name": name, "market": mk, "as_of": as_of, "close": _in_range(close, 0, 1e6, True), "val_as_of": v_as_of,
               "pe": _in_range(pev, 0, 1000, True), "pb": _in_range(pbv, 0, 100, True), "dividend_yield": _in_range(yv, 0, 50),
               "source": "證交所／櫃買中心開放資料（本益比用過去四季盈餘算，不是預估）",
               "prev_close": None, "chg_pct": None, "ret_20d": None, "high_52w": None, "high_52w_date": None, "low_52w": None, "range_source": None}
        try:
            row.update(_watch_hist_stats(code, mk, as_of, row["close"]))                                  # 52 週、漲跌、近 20 日（一檔出錯不影響其他檔）
        except Exception as e:
            log(f"watch_hist:{code}", "failure", detail=_clip(f"行情統計出錯：{type(e).__name__}: {e}"))
        out.append(row)
    notes = ([f"查不到：{'、'.join(missing)}（可能是興櫃、已下市或代號打錯）"] if missing else []) + ([f"日期異常略過：{'、'.join(bad_date)}"] if bad_date else []) + ([f"來源失敗：{'、'.join(errs)}"] if errs else [])
    log("watch", "success" if out else "no_new_data", detail=_clip(f"{len(out)} / {len(codes)} 檔有資料" + ("｜" + "；".join(notes) if notes else "")))
    return out


def finmind_fin_history(price_rows):
    """模組 M25：台股持股的歷史季度三率（FinMind，第三方、非官方；證交所開放資料只放最新一季，所以前一季查不到）。
    存進獨立的 tw_fin_q_ext（一律單季值），網頁只在官方查不到或比不了上一季時才用，並標示來源。
    判斷單季或累計用 detect_finmind_basis；判斷不了就一列都不寫。FinMind 有請求次數限制，被限流或擋掉就停止，記「沒有資料」（不讓工作變紅）。"""
    wanted = {r["code"]: r.get("name") or r["code"] for r in (price_rows or []) if r.get("market") == "TW" and r.get("code")}
    if sb_ready():                                                                           # 候選股也一併抓三率（選股前看三率）
        wl, _e = sb_request("GET", "private_watchlist?select=code&limit=200")
        for r in (wl or []):
            c = str(r.get("code", "")).strip() if isinstance(r, dict) else ""
            if re.fullmatch(r"\d{4}", c):
                wanted.setdefault(c, c)
    if not wanted:
        log("finmind_fin", "skipped", detail="沒有台股持股或候選股代號，不抓 FinMind")
        return []
    start = f"{date.today().year - 2}-01-01"
    by_code, notes = {}, []
    for code in sorted(wanted)[:config.FINMIND_MAX_CODES]:
        j, err = fetch_json(f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id={code}&start_date={start}")
        if j is None:
            notes.append(f"{code}（{err}）")
            if any(x in str(err) for x in ("402", "429", "403")):
                notes.append("疑似被限流，停止本次")
                break
            continue
        if not isinstance(j, dict) or j.get("status") != 200 or not isinstance(j.get("data"), list):
            notes.append(f"{code}（回傳 {str(j)[:50]}）")
            continue
        ser = parse_finmind_statements(j["data"])
        if ser:
            by_code[code] = ser
        else:
            notes.append(f"{code} {wanted[code]}（沒有一般業損益項目，可能是金融業）")
        time.sleep(config.FINMIND_SLEEP)
    basis, st = detect_finmind_basis(by_code)
    if basis is None and sb_ready() and by_code:                                             # 樣本太少判斷不了：沿用資料庫裡已判斷過的結果（同一個資料集，口徑一致）
        hint, _e = sb_request("GET", "tw_fin_q_ext?select=basis&limit=1")
        if hint and isinstance(hint[0], dict) and hint[0].get("basis") in ("single", "converted"):
            basis, st = ("single" if hint[0]["basis"] == "single" else "cumulative"), dict(st or {}, n=(st or {}).get("n", 0))
    if basis is None:
        log("finmind_fin", "no_new_data", detail=_clip(f"{len(by_code)} 檔有資料，但只有 {st['n']} 個「公司年度」可判斷單季或累計（至少 4 個、且結果要一致），不寫入" + (f"；{'、'.join(notes[:5])}" if notes else "")))
        return []
    official = set()
    if sb_ready():
        got, e2 = sb_request("GET", "tw_fin_q?select=code,year,quarter&limit=2000")
        official = {(r["code"], r["year"], r["quarter"]) for r in (got or [])}
    out, skipped = [], 0
    for code, ser in by_code.items():
        singles = finmind_to_single(ser, basis)
        for (y, q) in sorted(singles, reverse=True)[:config.FINMIND_QUARTERS]:
            v = singles[(y, q)]
            if v["revenue"] <= 0:
                continue
            gm, om, nm = (round(v[k] / v["revenue"] * 100, 3) for k in ("gross_profit", "op_income", "net_income"))
            if not (-1000 <= gm <= 100 and -1000 <= om <= 100 and -1000 <= nm <= 200):
                continue
            out.append({"code": code, "name": wanted[code], "year": y, "quarter": q, "revenue": v["revenue"], "gross_profit": v["gross_profit"], "op_income": v["op_income"], "net_income": v["net_income"],
                        "gross_margin": gm, "op_margin": om, "net_margin": nm, "basis": "single" if basis == "single" else "converted", "source": "FinMind（第三方，非官方）"})
    log("finmind_fin", "success" if out else "no_new_data", detail=_clip(f"{len(by_code)} 檔、{len(out)} 個季度；判斷為{'單季值' if basis == 'single' else '累計值（已換算成單季）'}（{st['n']} 個公司年度，第 4 季 ÷ 第 1 季營收的中位數 {st['median']:.2f}）" +
                                                                       (f"｜{'、'.join(notes[:5])}" if notes else "")))
    return out


_EXR_HIST_FIELDS = {"date": "資料日期", "code": "股票代號", "name": "股票名稱", "prev": "除權息前收盤價", "ref": "除權息參考價", "kind": "權/息"}


def parse_exright_hist_rows(data, today=None):
    """證交所『除權除息計算結果表』（歷史）→ tw_exright_events 的列（只有前收盤價與參考價，沒有現金股利，所以不帶 cash_dividend／stock_rate 欄位，
    寫入時才不會把預告表已經有的股利清成空白）。回傳 (列, 略過數, 錯誤原因)。參考價必須在前收盤價的 30% 到 100% 之間。"""
    today = today or date.today()
    fields = [str(f).strip() for f in (data.get("fields") or [])]
    miss = [v for v in _EXR_HIST_FIELDS.values() if v not in fields]
    if miss:
        return [], 0, f"欄位與預期不符：缺 {miss}；實際 {fields[:12]}"
    ix = {k: fields.index(v) for k, v in _EXR_HIST_FIELDS.items()}
    out, bad = [], 0
    for r in data.get("data") or []:
        try:
            m = _EXR_DATE.search(str(r[ix["date"]]))
            code, kind = str(r[ix["code"]]).strip(), str(r[ix["kind"]]).strip()
            prev, ref = parse_number(r[ix["prev"]]), parse_number(r[ix["ref"]])
            if not m or not re.fullmatch(r"[0-9A-Z]{4,6}", code) or kind not in ("息", "權", "權息") or prev is None or ref is None:
                bad += 1
                continue
            ev = date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
            if not (today - timedelta(days=430) <= ev <= today + timedelta(days=1)) or not (prev > 0 and ref > 0 and ref <= prev * 1.0001 and ref >= prev * 0.3):
                bad += 1
                continue
            out.append({"market": "TWSE", "code": code, "event_date": ev.isoformat(), "name": str(r[ix["name"]]).strip()[:40], "kind": kind,
                        "prev_close": prev, "ref_price": ref, "source": "證交所 除權除息計算結果表"})
        except (IndexError, TypeError, ValueError):
            bad += 1
    return out, bad, None


def tw_exright_history():
    """模組 M16：回補過去的除權息（證交所『除權除息計算結果表』），含除權息前收盤價與參考價。
    為什麼要：大跌清單可以算出「扣掉除權息之後，當天實際漲跌多少」。第一次（資料表裡還沒有任何帶參考價的列）回補約 400 天，每 30 天一段；
    之後每天只重抓最近 60 天（兩段）。段與段之間間隔 HIST_SLEEP 秒；連線失敗就整批停止；某一段沒資料或格式不對只略過那一段。"""
    if not sb_ready():
        log("tw_exright_hist", "skipped", detail="未設定 Supabase，無法判斷是不是第一次，不回補")
        return []
    have, err = sb_request("GET", "tw_exright_events?select=event_date&ref_price=not.is.null&limit=1")
    if have is None:
        log("tw_exright_hist", "failure", detail=_clip(f"讀取失敗：{err}（請先執行 new_tables_v42.sql 與 new_tables_v43.sql）"))
        return []
    today = date.today()
    span = 60 if have else 400
    windows, a = [], today - timedelta(days=span)
    while a <= today:
        b = min(a + timedelta(days=29), today)
        windows.append((a.strftime("%Y%m%d"), b.strftime("%Y%m%d")))
        a = b + timedelta(days=1)
    out, empty, bad_total, why_first, stopped = [], 0, 0, "", False
    for i, (wa, wb) in enumerate(windows):
        data, err = fetch_json(config.EXRIGHT_HIST_URL.format(pa="startDate", pb="endDate", a=wa, b=wb))
        if i < len(windows) - 1:
            time.sleep(config.HIST_SLEEP)
        if data is None:
            log("tw_exright_hist", "failure", as_of=wa, detail=_clip(f"{wa}–{wb} 抓取失敗：{err}；已停止本次回補，不繼續打請求"))
            stopped = True
            break
        if not isinstance(data, dict) or data.get("stat") != "OK" or not data.get("data"):
            empty += 1
            continue
        rows, bad, why = parse_exright_hist_rows(data)
        if why:
            why_first = why_first or f"{wa}–{wb}：{why}"
            continue
        out.extend(rows)
        bad_total += bad
    if why_first and not out:
        log("tw_exright_hist", "failure", detail=_clip(why_first))
        return []
    log("tw_exright_hist", "success" if out else "no_new_data", as_of=max((r["event_date"] for r in out), default=None),
        detail=_clip(f"{'第一次回補' if not have else '每日更新最近 60 天'}：{len(windows)} 段，取得 {len(out)} 筆（略過 {bad_total} 列、{empty} 段沒有資料）" + ("；中途連線失敗，下次繼續" if stopped else "") + (f"；{why_first}" if why_first else "")))
    return out


def tw_industry_collect():
    """模組 M17：上市公司基本資料（/opendata/t187ap03_L）與公開發行公司基本資料（/opendata/t187ap03_P，可能涵蓋上櫃）裡的「產業別」代碼。
    只收 4 位數代號的股票；產業別必須是 2 位數字。上市的優先，公開發行的只補上市沒有的代號。名稱對照在網頁（用證交所公布的產業別代碼表）。"""
    out, seen = [], set()
    for tag, path in (("L", "/opendata/t187ap03_L"), ("P", "/opendata/t187ap03_P")):
        rows, err = fetch_json(config.TWSE_OPENAPI_BASE + path)
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            log(f"tw_industry:{tag}", "failure", detail=_clip(f"{path} 抓取失敗或格式非預期: {err or str(rows)[:100]}"))
            continue
        keys = list(rows[0].keys())
        kc, ki = _fin_key(keys, ("公司代號",)), _fin_key(keys, ("產業別",))
        kn = _fin_key(keys, ("公司簡稱",)) or _fin_key(keys, ("公司名稱",))                  # 簡稱優先（欄位順序裡全名排在簡稱前面）
        if not kc or not ki:
            log(f"tw_industry:{tag}", "failure", detail=_clip(f"{path}：找不到公司代號或產業別欄（欄位 {keys[:14]}），不採用"))
            continue
        n_new = bad = 0
        for r in rows:
            code, ind = str(r.get(kc, "")).strip(), str(r.get(ki, "")).strip()
            if not re.fullmatch(r"[1-9]\d{3}", code):
                continue
            if not re.fullmatch(r"\d{2}", ind):
                bad += 1
                continue
            if code in seen:
                continue
            seen.add(code)
            out.append({"code": code, "industry": ind, "name": (str(r.get(kn, "")).strip() if kn else "")[:40], "source": f"證交所 公司基本資料（{tag}）"})
            n_new += 1
        log(f"tw_industry:{tag}", "success" if n_new else "no_new_data", detail=f"{path}｜{len(rows)} 列；新收 {n_new} 家（略過產業別不是 2 位數字的 {bad} 列）")
    return out


def probe_round7():
    """只讀探測第七批：1) 公開發行公司基本資料（t187ap03_P）有沒有涵蓋上櫃；2) 證交所個股本益比、殖利率、股價淨值比（BWIBBU_ALL）；
    3) 櫃買中心個股本益比、殖利率、股價淨值比（tpex_mainboard_peratio_analysis）。只記欄位與前 2 列，不寫入任何資料表。"""
    rows, e = fetch_json(config.TWSE_OPENAPI_BASE + "/opendata/t187ap03_P")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        keys = list(rows[0].keys())
        kc = _fin_key(keys, ("公司代號",))
        codes = [str(r.get(kc, "")).strip() for r in rows if isinstance(r, dict)] if kc else []
        otc_like = [c for c in codes if re.fullmatch(r"[3568]\d{3}", c)]
        log("probe:industry_p", "success", detail=_clip(f"共 {len(rows)} 列｜4 位數代號 {sum(1 for c in codes if re.fullmatch(r'[1-9]\d{3}', c))} 家｜代號開頭 3、5、6、8（常見的上櫃）{len(otc_like)} 家，例如 {otc_like[:6]}｜欄位 {keys[:14]}"))
    else:
        log("probe:industry_p", "failure", detail=_clip(f"/opendata/t187ap03_P 抓取失敗或格式非預期: {e or str(rows)[:100]}"))
    for tag, url in (("twse_pe", config.TWSE_OPENAPI_BASE + "/exchangeReport/BWIBBU_ALL"), ("tpex_pe", config.TPEX_DEFAULT_BASE + "/tpex_mainboard_peratio_analysis")):
        rows, e = fetch_json(url)
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            log(f"probe:{tag}", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
        else:
            log(f"probe:{tag}", "failure" if rows is None else "no_new_data", detail=_clip(f"{url.rsplit('/', 1)[-1]}：{e or '沒有資料列'}｜{str(rows)[:100]}"))


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
        sw, err = fetch_swagger(swurl)
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
    sw, err = fetch_swagger(config.TPEX_SWAGGER)
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
        sw, err = fetch_swagger(swurl)
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
        pth = next((p for p, t in cands if t.startswith("上市公司") or t.startswith("上櫃公司")), cands[0][0])               # 優先選「上市公司／上櫃公司」，不選券商名單
        rows, e = fetch_json(_openapi_base(sw, base) + pth)
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            log(f"probe:industry:{tag}", "failure", detail=_clip(f"{pth} 抓取失敗或格式非預期: {e or str(rows)[:100]}"))
            continue
        keys = list(rows[0].keys())
        kind = _fin_key(keys, ("產業別", "Industry", "IndustryCode"))
        cnt = Counter(str(r.get(kind, "")).strip() for r in rows if isinstance(r, dict)) if kind else Counter()
        log(f"probe:industry:{tag}", "success" if kind else "no_new_data",
            detail=_clip(f"{pth}｜共 {len(rows)} 列｜欄位 {keys}｜產業欄 {kind!r}｜不同的值 {len(cnt)} 種：{dict(cnt.most_common(45))}｜前 2 列 {rows[:2]}"))


def probe_round6():
    """只讀探測第六批：1) 上市公司基本資料（/opendata/t187ap03_L）的產業別；2) 櫃買中心所有「上櫃」端點的名稱（找一般上櫃公司的基本資料）；
    3) 櫃買中心除權息預告表（/tpex_exright_prepost）；4) 證交所除權除息計算結果表（過去 30 天，歷史）。不寫入任何資料表。"""
    rows, e = fetch_json(config.TWSE_OPENAPI_BASE + "/opendata/t187ap03_L")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        keys = list(rows[0].keys())
        kind = _fin_key(keys, ("產業別",))
        cnt = Counter(str(r.get(kind, "")).strip() for r in rows if isinstance(r, dict)) if kind else Counter()
        log("probe:industry_l", "success" if kind else "no_new_data", detail=_clip(f"共 {len(rows)} 列｜欄位 {keys[:14]}｜產業欄 {kind!r}｜不同的值 {len(cnt)} 種：{dict(cnt.most_common(45))}｜前 2 列 {[{k: r[k] for k in keys[:5]} for r in rows[:2]]}"))
    else:
        log("probe:industry_l", "failure", detail=_clip(f"/opendata/t187ap03_L 抓取失敗或格式非預期: {e or str(rows)[:100]}"))
    sw, e = fetch_swagger(config.TPEX_SWAGGER)
    items = []
    if isinstance(sw, dict):
        for pth, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                t = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if "上櫃" in t or "基本資料" in t:
                    items.append(f"{pth} | {t}")
    chunk, n = [], 0
    for it in items[:90]:
        if sum(len(x) + 3 for x in chunk) + len(it) > 780:
            n += 1
            log(f"probe:tpex_paths:{n}", "success", detail=_clip(" ; ".join(chunk)))
            chunk = []
        chunk.append(it)
    if chunk or not items:
        log(f"probe:tpex_paths:{n + 1}", "success" if items else "no_new_data", detail=_clip(" ; ".join(chunk) if chunk else f"說明文件裡找不到上櫃相關端點（{e or ''}）"))
    rows, e = fetch_json(config.TPEX_DEFAULT_BASE + "/tpex_exright_prepost")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        log("probe:exright_tpex_pre", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
    else:
        log("probe:exright_tpex_pre", "no_new_data" if isinstance(rows, list) else "failure", detail=_clip(f"/tpex_exright_prepost：{e or '沒有資料列'}｜{str(rows)[:120]}"))
    end = date.today()
    a, b = (end - timedelta(days=30)).strftime("%Y%m%d"), end.strftime("%Y%m%d")
    tried = []
    for pa, pb in (("startDate", "endDate"), ("strDate", "endDate")):
        data, e = fetch_json(config.EXRIGHT_HIST_URL.format(pa=pa, pb=pb, a=a, b=b))
        tried.append(f"{pa}:{(data or {}).get('stat') if isinstance(data, dict) else e}")
        if isinstance(data, dict) and data.get("stat") == "OK" and data.get("data"):
            log("probe:exright_twse_hist", "success", detail=_clip(f"參數 {pa}/{pb}｜欄位 {data.get('fields')}｜{len(data['data'])} 列｜前 2 列 {data['data'][:2]}"))
            break
    else:
        log("probe:exright_twse_hist", "no_new_data", detail=_clip(f"兩種參數名稱都沒拿到資料：{'；'.join(tried)}"))


def probe_round8():
    """只讀探測第八批：標普 500 指數的備用來源（FRED 比別的指數晚一個交易日，Nasdaq 的 API 沒有提供標普）。
    候選：1) Stooq 的日線 CSV（^SPX）；2) Yahoo 的 chart API（^GSPC）。只記：連線狀態、內容類型、最近 3 個交易日的日期與收盤、有沒有看起來像盤中的數字。
    不寫入任何資料表；要不要採用，等看到哪一個真的抓得到再決定。候選來源抓不到是正常的探測結果，所以只記「沒有資料」，不會讓工作變紅。"""
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "*/*"}
    status, ctype, body, err = fetch_raw("https://stooq.com/q/d/l/?s=%5Espx&i=d", timeout=config.PROBE_TIMEOUT, headers=ua)
    if status is None:
        log("probe:sp500_alt:stooq", "no_new_data", detail=_clip(f"連線失敗: {err}"))
    else:
        text = body.decode("utf-8", errors="replace")
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        head = lines[0].split(",") if lines else []
        rows = []
        if "Date" in head and "Close" in head:
            di, ci = head.index("Date"), head.index("Close")
            for l in lines[1:]:
                c = l.split(",")
                if len(c) > max(di, ci):
                    v = parse_number(c[ci])
                    if v is not None:
                        rows.append((c[di], v))
        ok = status == 200 and len(rows) >= 5
        log("probe:sp500_alt:stooq", "success" if ok else "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜{len(body)} 位元組｜表頭 {head[:6]}｜共 {len(rows)} 個交易日｜最近 3 筆 {rows[-3:]}｜開頭 {text[:120]!r}"))
    status, ctype, body, err = fetch_raw("https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=1mo&interval=1d", timeout=config.PROBE_TIMEOUT, headers=ua)
    if status is None:
        log("probe:sp500_alt:yahoo", "no_new_data", detail=_clip(f"連線失敗: {err}"))
    else:
        try:
            res = ((json.loads(body.decode("utf-8-sig")).get("chart") or {}).get("result") or [None])[0] or {}
            ts = res.get("timestamp") or []
            closes = (((res.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
            pts = [(datetime.fromtimestamp(t_, timezone.utc).date().isoformat(), c) for t_, c in zip(ts, closes) if c is not None]
            meta = res.get("meta") or {}
            log("probe:sp500_alt:yahoo", "success" if status == 200 and len(pts) >= 5 else "no_new_data",
                detail=_clip(f"HTTP {status}｜{ctype}｜共 {len(pts)} 個交易日｜最近 3 筆 {[(d_, round(c, 2)) for d_, c in pts[-3:]]}｜marketState={meta.get('marketState')}｜regularMarketTime={meta.get('regularMarketTime')}"))
        except (ValueError, UnicodeDecodeError, AttributeError, TypeError, KeyError):
            log("probe:sp500_alt:yahoo", "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜不是預期的 JSON｜開頭 {body[:120]!r}"))


def probe_round9():
    """只讀探測第九批：「量價結構」四層（未平倉量 OI、量價成本 POC/VWAP、PCR 與履約價牆、隱含波動率 IV）與「融資去槓桿」要用的資料來源。
    1) 期交所 OpenAPI：有沒有未沖銷契約量、選擇權每日行情（履約價與 OI）、Put/Call、波動率指數；2) Cboe 延遲選擇權鏈（_SPX、SPY）：有沒有 iv、open_interest；
    3) 證交所 MI_MARGN 的逐檔融資表；4) 櫃買中心融資融券餘額；5) 兩邊 OpenAPI 有沒有名稱含「維持」或「信用」的端點。
    只記欄位、筆數與前幾列，不寫入任何資料表；候選來源不能用是正常的探測結果，只記「沒有資料」，不讓工作變紅。"""
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "application/json,*/*"}
    kw = ("未沖銷", "選擇權", "Put", "波動", "PCR", "買賣權", "OpenInterest")
    sw, err = fetch_swagger(config.TAIFEX_SWAGGER)
    items = []
    if isinstance(sw, dict):
        for pth, methods in (sw.get("paths") or {}).items():
            for mth in (methods or {}).values():
                t = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                if any(k in t for k in kw) or any(k in pth for k in ("Opt", "PutCall", "VIX", "Vix")):
                    items.append(f"{pth} | {t}")
    chunk, n = [], 0
    for it in items[:60]:
        if sum(len(x) + 3 for x in chunk) + len(it) > 780:
            n += 1
            log(f"probe:taifex_paths:{n}", "success", detail=_clip(" ; ".join(chunk)))
            chunk = []
        chunk.append(it)
    if chunk or not items:
        log(f"probe:taifex_paths:{n + 1}", "success" if items else "no_new_data", detail=_clip(" ; ".join(chunk) if chunk else f"期交所說明文件裡找不到相關端點（{err or ''}）"))
    rows, err = fetch_json(config.TAIFEX_DAILY_URL)
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        keys = list(rows[0].keys())
        oi = [k for k in keys if "Open" in k and "Interest" in k or "未沖銷" in k]
        log("probe:taifex_fut_oi", "success" if oi else "no_new_data", detail=_clip(f"期貨每日行情 {len(rows)} 列｜欄位 {keys}｜疑似未平倉量欄 {oi}｜前 1 列 {rows[0]}"))
    else:
        log("probe:taifex_fut_oi", "no_new_data", detail=_clip(f"期貨每日行情抓取失敗或格式非預期: {err or str(rows)[:100]}"))
    for sym in ("_SPX", "SPY"):
        status, ctype, body, err = fetch_raw(f"https://cdn.cboe.com/api/global/delayed_quotes/options/{sym}.json", timeout=config.PROBE_TIMEOUT, headers=ua)
        if status is None:
            log(f"probe:cboe_chain:{sym.strip('_')}", "no_new_data", detail=_clip(f"連線失敗: {err}"))
            continue
        try:
            j = json.loads(body.decode("utf-8-sig"))
            d_ = j.get("data") or {}
            opts = d_.get("options") or []
            sample = []
            for o in opts[:400]:
                m = re.fullmatch(r"([A-Z]+W?\d?)(\d{6})([CP])(\d{8})", str(o.get("option", "")))
                if m and o.get("open_interest") not in (None, 0):
                    sample.append({"到期": m.group(2), "種類": m.group(3), "履約價": int(m.group(4)) / 1000, "oi": o.get("open_interest"), "iv": o.get("iv"), "vol": o.get("volume")})
                if len(sample) >= 3:
                    break
            keys = sorted((opts[0] or {}).keys()) if opts else []
            log(f"probe:cboe_chain:{sym.strip('_')}", "success" if status == 200 and len(opts) > 10 and "iv" in keys and "open_interest" in keys else "no_new_data",
                detail=_clip(f"HTTP {status}｜{ctype}｜頂層 {sorted(j.keys())}｜data 欄位 {sorted(k for k in d_.keys() if k != 'options')[:12]}｜共 {len(opts)} 檔合約｜合約欄位 {keys[:16]}｜解析範例 {sample}｜timestamp={j.get('timestamp')}"))
        except (ValueError, UnicodeDecodeError, AttributeError, TypeError):
            log(f"probe:cboe_chain:{sym.strip('_')}", "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜不是預期的 JSON｜開頭 {body[:120]!r}"))
    data, err = fetch_json("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?selectType=ALL&response=json")
    if isinstance(data, dict) and data.get("tables"):
        tabs = data["tables"]
        big = max(tabs, key=lambda t: len(t.get("data") or []))
        log("probe:twse_margin_stock", "success" if len(big.get("data") or []) > 100 else "no_new_data",
            detail=_clip(f"stat={data.get('stat')}｜{len(tabs)} 張表｜最大一張 {len(big.get('data') or [])} 列｜欄位 {big.get('fields')}｜前 2 列 {(big.get('data') or [])[:2]}"))
    else:
        log("probe:twse_margin_stock", "no_new_data", detail=_clip(f"逐檔融資表抓取失敗或格式非預期: {err or str(data)[:100]}"))
    rows, err = fetch_json(config.TPEX_DEFAULT_BASE + "/tpex_mainboard_margin_balance")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        log("probe:tpex_margin", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
    else:
        log("probe:tpex_margin", "no_new_data", detail=_clip(f"上櫃融資融券餘額抓取失敗或格式非預期: {err or str(rows)[:100]}"))
    for tag, swurl in (("twse", config.TWSE_OPENAPI_SWAGGER), ("tpex", config.TPEX_SWAGGER)):
        sw2, e2 = fetch_swagger(swurl)
        hits = []
        if isinstance(sw2, dict):
            for pth, methods in (sw2.get("paths") or {}).items():
                for mth in (methods or {}).values():
                    t = str(mth.get("summary", "")) if isinstance(mth, dict) else ""
                    if "維持" in t or "信用交易" in t or "融資" in t:
                        hits.append(f"{pth} | {t}")
        log(f"probe:margin_paths:{tag}", "success" if hits else "no_new_data", detail=_clip(" ; ".join(hits[:14]) if hits else f"說明文件裡找不到名稱含維持、信用交易、融資的端點（{e2 or ''}）"))


def probe_round10():
    """只讀探測第十批：1) 期交所選擇權每日行情、Put/Call、指數選擇權未平倉量增減的欄位；2) 櫃買中心融資融券餘額與信用交易餘額概況的欄位；
    3) 證交所 MI_MARGN 能不能指定日期取逐檔融資（歷史）；4) 證交所全部上市股票的收盤行情欄位（算「民間算法」維持率要用）。不寫入任何資料表。"""
    base = config.TAIFEX_DAILY_URL.rsplit("/", 1)[0]
    for tag, name in (("taifex_opt", "DailyMarketReportOpt"), ("taifex_pcr", "PutCallRatio"), ("taifex_va01", "va01")):
        rows, err = fetch_json(f"{base}/{name}")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            keys = list(rows[0].keys())
            extra = ""
            if tag == "taifex_opt":
                tx = [r for r in rows if str(r.get("Contract", "")).strip() == "TXO"]
                extra = f"｜TXO 共 {len(tx)} 列｜TXO 前 2 列 {tx[:2]}"
            log(f"probe:{tag}", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {keys}｜前 1 列 {rows[0]}{extra}"))
        else:
            log(f"probe:{tag}", "no_new_data", detail=_clip(f"{name} 抓取失敗或格式非預期: {err or str(rows)[:100]}"))
    for tag, name in (("tpex_margin", "tpex_mainboard_margin_balance"), ("tpex_marginspot", "tpex_margin_trading_marginspot")):
        rows, err = fetch_json(f"{config.TPEX_DEFAULT_BASE}/{name}")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            log(f"probe:{tag}", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
        else:
            log(f"probe:{tag}", "no_new_data", detail=_clip(f"{name} 抓取失敗或格式非預期: {err or str(rows)[:100]}"))
    probe_day = (date.today() - timedelta(days=10)).strftime("%Y%m%d")
    for _ in range(4):                                                              # 往前找一個平日（可能剛好是假日，證交所會回沒有資料）
        d_ = date(int(probe_day[:4]), int(probe_day[4:6]), int(probe_day[6:]))
        if d_.weekday() < 5:
            break
        probe_day = (d_ - timedelta(days=1)).strftime("%Y%m%d")
    data, err = fetch_json(f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?selectType=ALL&response=json&date={probe_day}")
    if isinstance(data, dict) and data.get("tables"):
        big = max(data["tables"], key=lambda t: len(t.get("data") or []))
        log("probe:twse_margin_date", "success" if data.get("date") == probe_day and len(big.get("data") or []) > 100 else "no_new_data",
            detail=_clip(f"要求 {probe_day}、回傳 date={data.get('date')}｜stat={data.get('stat')}｜最大一張 {len(big.get('data') or [])} 列｜前 1 列 {(big.get('data') or [])[:1]}"))
    else:
        log("probe:twse_margin_date", "no_new_data", detail=_clip(f"要求 {probe_day}：{err or str(data)[:120]}"))
    rows, err = fetch_json(config.TWSE_OPENAPI_BASE + "/exchangeReport/STOCK_DAY_ALL")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        log("probe:twse_day_all", "success", detail=_clip(f"共 {len(rows)} 列｜欄位 {list(rows[0].keys())}｜前 2 列 {rows[:2]}"))
    else:
        log("probe:twse_day_all", "no_new_data", detail=_clip(f"STOCK_DAY_ALL 抓取失敗或格式非預期: {err or str(rows)[:100]}"))


def parse_yahoo_chart(j):
    """Yahoo 圖表 API（v8/finance/chart）→ [(日期, 收盤價)]，由舊到新。日期用交易所自己的時區（meta.gmtoffset）換算；收盤是空值的日子略過。"""
    try:
        r = j["chart"]["result"][0]
        ts, cl = r["timestamp"], r["indicators"]["quote"][0]["close"]
        off = int((r.get("meta") or {}).get("gmtoffset") or 0)
    except (KeyError, IndexError, TypeError):
        return None
    out = []
    for t_, c_ in zip(ts, cl):
        v = parse_number(c_)
        if v is None or not isinstance(t_, (int, float)):
            continue
        out.append((datetime.fromtimestamp(t_ + off, tz=timezone.utc).date().isoformat(), v))
    return out or None


def yahoo_fill():
    """模組 M27：用 Yahoo Finance（非官方）補 FRED 落後的天數：標普 500（^GSPC）、WTI 與 Brent 原油期貨（FRED 常落後 3 到 5 天），以及 VIX、VXN、Nasdaq 綜合（FRED 常落後約一個營業日以上）。
    安全做法：1) 只補「比 FRED 最新一天更新」的日子，不動已有的；2) 先拿重疊的日子和 FRED 的數字比，差太多就整批不用（價格口徑不同）；
    3) 價格要在合理範圍；4) 今天還沒收盤的不收；5) 抓不到只記「沒有資料」，不讓工作變紅。來源欄位明寫「非官方」，之後 FRED 補上同一天會蓋回官方值。"""
    if not sb_ready():
        log("yahoo_fill", "skipped", detail="未設定 Supabase，無法比對 FRED 的最新資料，不補")
        return []
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"}
    out, notes = [], []
    for sym, sid, tol in config.YAHOO_FILL:
        label, low, high = config.FRED_MACRO[sid]
        have, err = sb_request("GET", f"macro_series?select=as_of,value&series_id=eq.{sid}&order=as_of.desc&limit=12")
        if not have:
            notes.append(f"{sid}（資料庫沒有 FRED 資料可比對：{err or '空的'}）")
            continue
        status, ctype, body, e2 = fetch_raw(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range=1mo&interval=1d", timeout=config.PROBE_TIMEOUT, headers=ua)
        time.sleep(1)
        if status != 200:
            notes.append(f"{sym}（HTTP {status} {e2}）".strip())
            continue
        try:
            pts = parse_yahoo_chart(json.loads(body.decode("utf-8-sig")))
        except (ValueError, UnicodeDecodeError):
            pts = None
        if not pts:
            notes.append(f"{sym}（不是預期的格式）")
            continue
        fred = {str(r["as_of"]): float(r["value"]) for r in have if parse_number(r.get("value")) is not None}
        newest = max(fred)
        overlap = [(d_, v, fred[d_]) for d_, v in pts if d_ in fred]
        if not overlap:
            notes.append(f"{sym}（和 FRED 沒有重疊的日子，沒辦法核對，不用）")
            continue
        worst = max(abs(v / f - 1) for _d, v, f in overlap)
        if worst > tol:
            notes.append(f"{sym}（和 FRED 重疊的 {len(overlap)} 天最大差 {worst * 100:.2f}%，超過 {tol * 100:.1f}%，口徑不同，不用）")
            continue
        today_et = _now_et().date()
        got = [{"series_id": sid, "label": label, "as_of": d_, "value": round(v, 4), "source": "Yahoo Finance（非官方，補 FRED 落後的天數）"}
               for d_, v in pts if d_ > newest and d_ < today_et.isoformat() and low <= v <= high]
        if got:
            out.extend(got)
            notes.append(f"{sid}：補 {len(got)} 天（{got[0]['as_of']} 到 {got[-1]['as_of']}），和 FRED 重疊 {len(overlap)} 天最大差 {worst * 100:.2f}%")
        else:
            notes.append(f"{sid}：FRED 已經是最新（{newest}），不需要補")
    log("yahoo_fill", "success" if out else "no_new_data", detail=_clip("；".join(notes) or "沒有項目"))
    return out


def tw_keep_rate():
    """模組 M38：證交所臺股儀表板的「全市場擔保維持率」（官方，每日）。
    來源（探測 tw_probe 確認過的 API）：…/rwd/zh/marginTrading/BFIJ3U（最新一天）與 BFIJ3U_TREND（最近幾天）。
    欄位 keepRate 就是維持率（%）；其他欄位（crdAmt、belowAccNum、callAccNum、callAmt、exeAccNum、exeAmt、marginAccNum、marginAccRate）
    這裡只原樣存起來，還沒確認每個欄位的白話意思，所以畫面上不顯示、也不拿來判斷。
    每一天都要驗證：日期是 YYYYMMDD、不在未來、維持率是 KEEP_RATE_RANGE 內的數字，否則略過那一天。stat 不是 OK（例如假日）就記「沒有新資料」。"""
    base = config.TWSE_RWD_BASE + "/marginTrading"
    cur, err1 = fetch_json(base + "/BFIJ3U?response=json", headers=DASH_HEADERS)
    trend, err2 = fetch_json(base + f"/BFIJ3U_TREND?response=json&days={config.KEEP_RATE_DAYS}", headers=DASH_HEADERS)
    if cur is None and trend is None:
        log("tw_keep_rate", "failure", detail=_clip(f"證交所儀表板（全市場擔保維持率）抓取失敗：{err1}；{err2}"))
        return []
    low, high = config.KEEP_RATE_RANGE
    today, rows, skipped = date.today(), {}, 0

    def take(rec):
        nonlocal skipped
        try:
            d_ = datetime.strptime(str(rec.get("date")), "%Y%m%d").date()
        except (ValueError, TypeError):
            skipped += 1
            return
        kr = parse_number(rec.get("keepRate"))
        if d_ > today or kr is None or not (low <= kr <= high):
            skipped += 1
            return
        num = lambda k: parse_number(rec.get(k))
        rows[d_.isoformat()] = {"as_of": d_.isoformat(), "keep_rate": kr, "credit_amt": num("crdAmt"), "below_acc": num("belowAccNum"), "call_acc": num("callAccNum"), "call_amt": num("callAmt"),
                                "exe_acc": num("exeAccNum"), "exe_amt": num("exeAmt"), "margin_acc": num("marginAccNum"), "margin_acc_rate": num("marginAccRate"), "source": "TWSE 臺股儀表板 BFIJ3U"}
    stat_bad = []
    for name, obj, is_list in (("BFIJ3U_TREND", trend, True), ("BFIJ3U", cur, False)):                      # 趨勢先放、最新一天後放（同一天以最新一天為準）
        if not isinstance(obj, dict):
            continue
        if obj.get("stat") != "OK":
            stat_bad.append(f"{name} stat={obj.get('stat')}")
            continue
        if is_list:
            for rec in (obj.get("data") or []):
                if isinstance(rec, dict):
                    take(rec)
        else:
            take(dict(obj))
    out = [rows[k] for k in sorted(rows)]
    if not out:
        log("tw_keep_rate", "no_new_data", detail=_clip("沒有可用的維持率資料" + (f"（{'；'.join(stat_bad)}）" if stat_bad else "") + (f"；略過 {skipped} 筆格式或範圍不符的" if skipped else "")))
        return []
    last = out[-1]
    log("tw_keep_rate", "success", as_of=last["as_of"], detail=_clip(f"全市場擔保維持率 {last['keep_rate']:.2f}%（{last['as_of']}）；共 {len(out)} 天（{out[0]['as_of']} 到 {last['as_of']}）" + (f"；略過 {skipped} 筆不符的" if skipped else "")))
    return out


def twd_fred_history():
    """模組 M37：把 FRED 的美元兌台幣（DEXTAUS）每日收盤歷史（約 TWD_HISTORY_DAYS 天）寫進 twd_rates（layer 3、kind official_close、is_close_history = true，和每天更新的那一筆同一種列）。
    為什麼要：每天的更新只存「FRED 當下最新的一筆」，而 FRED 的台幣大約每週才更新一次，所以資料庫裡的台幣收盤很稀疏，算不出「連續回落幾天」，也沒辦法和 Yahoo 核對。
    主鍵是 layer,source,as_of，重跑只會更新、不會重複。抓不到只記錄，不讓工作變紅。"""
    low, high = config.TWD_RANGE
    rows = fred_history_rows(config.TWD_FRED_SERIES, low, high, days=config.TWD_HISTORY_DAYS)
    if not rows:
        log("twd_history", "no_new_data", detail="FRED 沒有回傳台幣歷史（沒設 API 金鑰、連線失敗或空的）")
        return []
    out = [{"as_of": r["as_of"], "value": r["value"], "kind": "official_close", "source": "FRED DEXTAUS", "layer": 3, "is_close_history": True} for r in rows]
    log("twd_history", "success", as_of=out[-1]["as_of"], detail=f"{len(out)} 筆每日收盤（{out[0]['as_of']} 到 {out[-1]['as_of']}）")
    return out


def yahoo_twd_fill(extra_fred=None):
    """模組 M36：美元兌台幣較新的天數（Yahoo Finance 的 TWD=X，非官方）。FRED 的台幣收盤（DEXTAUS）常落後約一週，這個補上之後的日子，
    存進 twd_rates 的另一層（layer 4、kind spot_aggregator、is_close_history = false，來源明寫「非官方」），不動 FRED 那些列；FRED 之後補上同一天，網頁以 FRED 為準。
    安全做法：1) 只補比 FRED 最新一天更新、而且早於今天的日子；2) 先拿重疊的日子和 FRED 比：至少 3 天、平均差不超過 0.5%、最大差不超過 1%，
    而且「日期不位移」比「位移一天」更吻合（匯率每天變動不大，差一天不容易被發現，所以要明確檢查）；3) 數值要在合理範圍；4) 抓不到只記「沒有資料」，不讓工作變紅。"""
    if not sb_ready():
        log("yahoo_twd", "skipped", detail="未設定 Supabase，無法比對 FRED 的台幣收盤，不補")
        return []
    have, err = sb_request("GET", "twd_rates?is_close_history=eq.true&order=as_of.desc&limit=15&select=as_of,value")
    fred = {str(r["as_of"]): float(r["value"]) for r in (have or []) if parse_number(r.get("value")) is not None}
    for r in (extra_fred or []):                                          # 同一次執行剛抓到、還沒寫進資料庫的 FRED 每日歷史（M37）也一起核對
        if parse_number(r.get("value")) is not None:
            fred[str(r["as_of"])] = float(r["value"])
    if len(fred) < 3:
        log("yahoo_twd", "no_new_data", detail=_clip(f"資料庫裡的 FRED 台幣收盤不到 3 筆，沒辦法核對，不補（{err or '資料不足'}）"))
        return []
    status, ctype, body, e2 = fetch_raw("https://query1.finance.yahoo.com/v8/finance/chart/TWD%3DX?range=1mo&interval=1d", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"})
    if status != 200:
        log("yahoo_twd", "no_new_data", detail=_clip(f"Yahoo TWD=X：HTTP {status} {e2}".strip()))
        return []
    try:
        pts = parse_yahoo_chart(json.loads(body.decode("utf-8-sig")))
    except (ValueError, UnicodeDecodeError):
        pts = None
    if not pts:
        log("yahoo_twd", "no_new_data", detail="Yahoo TWD=X：不是預期的格式")
        return []
    low, high = config.TWD_RANGE
    pts = [(d_, v) for d_, v in pts if low <= v <= high]

    def errs(shift):
        out = []
        for d_, v in pts:
            k = (date.fromisoformat(d_) + timedelta(days=shift)).isoformat()
            if k in fred:
                out.append(abs(v / fred[k] - 1))
        return out
    e0 = errs(0)
    if len(e0) < 3:
        log("yahoo_twd", "no_new_data", detail=_clip(f"和 FRED 重疊的日子只有 {len(e0)} 天（要至少 3 天），沒辦法核對，不用"))
        return []
    m0, x0 = sum(e0) / len(e0), max(e0)
    if m0 > 0.005 or x0 > 0.01:
        log("yahoo_twd", "no_new_data", detail=_clip(f"和 FRED 重疊的 {len(e0)} 天平均差 {m0 * 100:.2f}%、最大差 {x0 * 100:.2f}%，超過 0.5%／1%，口徑不同，不用"))
        return []
    for shift in (-1, 1):
        es = errs(shift)
        if len(es) >= 3 and sum(es) / len(es) < m0:
            log("yahoo_twd", "no_new_data", detail=_clip(f"日期位移 {shift:+d} 天比不位移更吻合 FRED（平均差 {sum(es) / len(es) * 100:.2f}% 對 {m0 * 100:.2f}%），日期可能差一天，不用"))
            return []
    newest, today = max(fred), date.today().isoformat()
    got = [{"as_of": d_, "value": round(v, 4), "kind": "spot_aggregator", "source": "Yahoo Finance（非官方，補 FRED 落後的天數）", "layer": 4, "is_close_history": False} for d_, v in pts if newest < d_ < today]
    log("yahoo_twd", "success" if got else "no_new_data", as_of=got[-1]["as_of"] if got else None,
        detail=_clip((f"補 {len(got)} 天（{got[0]['as_of']} 到 {got[-1]['as_of']}）" if got else f"FRED 已經是最新（{newest}），不需要補") + f"；和 FRED 重疊 {len(e0)} 天，平均差 {m0 * 100:.2f}%、最大差 {x0 * 100:.2f}%"))
    return got


def yahoo_dxy():
    """模組 M31：美元指數 DXY（ICE 的那個，不是聯準會的廣義美元指數），用 Yahoo Finance（非官方）的 DX-Y.NYB，近 2 年日資料存進 macro_series（series_id = DXY）。
    驗證：筆數夠（至少 100）、數值在 70 到 130、日期由舊到新且不含今天（還沒收盤）；再拿「和 FRED 廣義美元指數的日變動」算相關係數核對（兩個指數不同但高度相關，
    相關係數低於 0.5 代表可能抓到別的東西，整批不用；FRED 資料不夠核對時照收，並在紀錄裡寫明沒核對）。抓不到只記「沒有資料」，不讓工作變紅。"""
    if not sb_ready():
        log("yahoo_dxy", "skipped", detail="未設定 Supabase，不補 DXY")
        return []
    sym, sid, (lo, hi) = config.YAHOO_DXY
    status, ctype, body, err = fetch_raw(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range=2y&interval=1d", timeout=config.PROBE_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"})
    if status != 200:
        log("yahoo_dxy", "no_new_data", detail=_clip(f"{sym} HTTP {status} {err}".strip()))
        return []
    try:
        pts = parse_yahoo_chart(json.loads(body.decode("utf-8-sig")))
    except (ValueError, UnicodeDecodeError):
        pts = None
    today = _now_et().date().isoformat()
    pts = [(d_, v) for d_, v in (pts or []) if d_ < today]
    if len(pts) < 100:
        log("yahoo_dxy", "no_new_data", detail=_clip(f"{sym} 只有 {len(pts)} 筆可用（至少 100），不用"))
        return []
    if [d_ for d_, _v in pts] != sorted({d_ for d_, _v in pts}) or not all(lo <= v <= hi for _d, v in pts):
        log("yahoo_dxy", "failure", detail=_clip(f"{sym} 的日期不是由舊到新、有重複，或數值超出 {lo:g} 到 {hi:g}，不用"))
        return []
    fred, _e = sb_request("GET", "macro_series?select=as_of,value&series_id=eq.DTWEXBGS&order=as_of.desc&limit=150")
    fmap = {str(r["as_of"]): float(r["value"]) for r in (fred or []) if isinstance(r, dict) and parse_number(r.get("value")) is not None}
    common = [(d_, v, fmap[d_]) for d_, v in pts if d_ in fmap]
    note = ""
    if len(common) >= 16:
        import math
        a = [math.log(common[i][1] / common[i - 1][1]) for i in range(1, len(common))]
        b = [math.log(common[i][2] / common[i - 1][2]) for i in range(1, len(common))]
        def pearson(x, y):
            mx, my = sum(x) / len(x), sum(y) / len(y)
            den = math.sqrt(sum((p - mx) ** 2 for p in x) * sum((q - my) ** 2 for q in y))
            return sum((p - mx) * (q - my) for p, q in zip(x, y)) / den if den else 0.0
        corr0 = pearson(a, b)
        # 日變動幾乎是隨機的，兩個序列的日期只要差一天，同一天的相關係數就會掉到接近 0；所以同時看「錯開一個交易日」的相關係數。
        # next_c：DXY 的變動，出現在廣義指數的「下一個重疊日」（廣義指數的日期比 DXY 晚一天）；prev_c 相反。
        prev_c, next_c = pearson(a[1:], b[:-1]), pearson(a[:-1], b[1:])
        cands = [(corr0, "同一天", 0), (next_c, "廣義指數的日期比 DXY 晚一個交易日", 1), (prev_c, "廣義指數的日期比 DXY 早一個交易日", -1)]
        best, how, shift = max(cands, key=lambda c: c[0])
        if best < 0.5 or (shift != 0 and best - corr0 < 0.3):
            # 這個核對本來就不嚴格：聯準會廣義指數與 DXY 的成分、取價時間都不同（你資料庫的 229 個重疊日，同一天相關係數只有 0.31），所以不拿它當門檻擋掉資料，
            # 只把「核對的結果很弱、無法確認」老實寫在每一筆的來源欄。真正的把關是：代號固定、至少 100 天、數值在 70 到 130、日期由舊到新。
            note = f"；和 FRED 廣義美元指數的日變動相關係數：同一天 {corr0:.2f}、晚一個交易日 {next_c:.2f}、早一個交易日 {prev_c:.2f}，都不高，無法確認（兩個指數成分與取價時間不同，這個核對只能當粗略參考）"
            src = f"Yahoo Finance（非官方；和聯準會廣義美元指數核對的結果很弱：日變動相關係數最高 {max(corr0, next_c, prev_c):.2f}，無法確認；兩個指數的成分與取價時間不同，只能當粗略參考）"
        elif shift == 0:
            note = f"；和 FRED 廣義美元指數的日變動相關係數 {corr0:.2f}（{len(a)} 組）"
            src = f"Yahoo Finance（非官方；已和聯準會廣義美元指數核對，日變動相關係數 {corr0:.2f}）"
        else:
            note = f"；同一天相關係數只有 {corr0:.2f}，但{how}時是 {best:.2f}：兩個來源的日期差一個交易日（哪邊的日期是對的，從資料看不出來），DXY 的日期照 Yahoo 原樣存"
            src = f"Yahoo Finance（非官方；已和聯準會廣義美元指數核對，日變動相關係數 {best:.2f}，但兩者的日期差一個交易日，DXY 日期照 Yahoo 原樣）"
    else:
        note = f"；FRED 廣義美元指數重疊只有 {len(common)} 天，沒有核對"
        src = f"Yahoo Finance（非官方；沒有核對：聯準會廣義美元指數的歷史重疊只有 {len(common)} 天）"          # 核對結果寫進資料本身，卡片的「來源」就看得到
    rows = [{"series_id": sid, "label": "美元指數 DXY（ICE，Yahoo 非官方）", "as_of": d_, "value": round(v, 3), "source": src} for d_, v in pts]
    log("yahoo_dxy", "success", as_of=rows[-1]["as_of"], detail=f"{len(rows)} 天（{rows[0]['as_of']} 到 {rows[-1]['as_of']}），最新 {rows[-1]['value']}" + note)
    return rows


def probe_round14():
    """只讀探測第十四批：FINRA 融資統計（換成瀏覽器風格的標頭、找頁面上的檔案連結）、AAII 的其他讀法（結果頁、歷史檔、Nasdaq Data Link）、DXY 的備用來源（Stooq）。
    每一項記：HTTP 狀態、內容類型、位元組數、開頭；FINRA 頁面另外找表格列與 xls／csv 連結。不寫入任何資料表。"""
    browser = {"User-Agent": config.NASDAQ_UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.finra.org/", "Upgrade-Insecure-Requests": "1"}
    plain = {"User-Agent": config.NASDAQ_UA, "Accept": "*/*"}
    for tag, url, hdr in (("finra_page", config.FINRA_MARGIN_URL, browser), ("aaii_results", "https://www.aaii.com/sentimentsurvey/sent_results", dict(browser, Referer="https://www.aaii.com/")),
                          ("aaii_xls", "https://www.aaii.com/files/surveys/sentiment.xls", dict(plain, Referer="https://www.aaii.com/")), ("nasdaq_aaii", "https://data.nasdaq.com/api/v3/datasets/AAII/AAII_SENTIMENT.json?rows=3", plain),
                          ("stooq_dxy", "https://stooq.com/q/d/l/?s=dx.f&i=d", plain)):
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=hdr)
        time.sleep(1)
        if status is None:
            log(f"probe:{tag}", "no_new_data", detail=_clip(f"連線失敗: {err}"))
            continue
        raw = body or b""
        txt = raw.decode("utf-8", errors="replace")
        head = "magic " + raw[:4].hex() if tag == "aaii_xls" else re.sub(r"\s+", " ", txt[:160])
        extra = ""
        if tag == "finra_page" and status == 200:
            links = re.findall(r'href="([^"]+\.(?:xlsx?|csv))"', txt, re.I)[:4]
            rows = re.findall(r"([A-Z][a-z]{2}[- ]\d{2,4})\s*(?:</t[dh]>\s*<t[dh][^>]*>)?\s*\$?\s*([\d,]{6,})", txt)[:3]
            extra = f"｜檔案連結 {links}｜表格列 {rows}"
        if tag == "aaii_results" and status == 200:
            m = re.search(r"Bullish.{0,160}", re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt)))
            extra = f"｜Bullish 附近 {m.group(0)[:160] if m else '找不到'}"
        if tag == "stooq_dxy" and status == 200:
            extra = f"｜行數 {len(txt.splitlines())}｜前 3 行 {txt.splitlines()[:3]}"
        log(f"probe:{tag}", "success" if status == 200 else "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜{len(raw)} 位元組｜開頭 {head!r}{extra}"))


def probe_round13():
    """只讀探測第十三批：櫃買網站「融資融券餘額」頁（新格式：tables → fields、data、summary）。上一批用「今天」（週日）查，資料是空的，
    這批從今天往前找最近 8 天內第一個有資料的平日，把欄位、前 2 列、summary（合計列）和 notes 記下來，看有沒有「融資金額」（算上櫃維持率需要）。不寫入任何資料表。"""
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "application/json"}
    tried, last = [], ""
    for back in range(0, 9):
        d = date.today() - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        status, ctype, body, err = fetch_raw(f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc}", timeout=config.PROBE_TIMEOUT, headers=ua)
        time.sleep(1)
        txt = body.decode("utf-8", errors="replace") if body else ""
        tried.append(f"{roc}:{status}")
        last = txt[:200]
        try:
            j = json.loads(txt)
            t0 = (j.get("tables") or [None])[0]
            rows = t0.get("data") or []
        except (ValueError, AttributeError, IndexError, TypeError):
            continue
        if rows:
            log("probe:tpex_margin_page", "success", as_of=d.isoformat(), detail=_clip(f"{roc}｜{len(rows)} 列｜欄位 {t0.get('fields')}｜前 2 列 {rows[:2]}｜summary {json.dumps(t0.get('summary'), ensure_ascii=False)[:700]}｜notes {json.dumps(t0.get('notes'), ensure_ascii=False)[:200]}"))
            return
    log("probe:tpex_margin_page", "no_new_data", detail=_clip(f"最近 8 天（平日）都沒有資料；試過 {tried}｜最後一次開頭 {last!r}"))


def probe_round12():
    """只讀探測第十二批：櫃買中心的「融資融券餘額彙總」（要有上櫃融資『金額』才能算上櫃維持率；開放資料的逐檔表只有張數）。不寫入任何資料表。"""
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "application/json"}
    roc = f"{date.today().year - 1911}/{date.today().month:02d}/{date.today().day:02d}"
    for tag, url in (("tpex_margin_summary", f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc}"),
                     ("tpex_margin_summary2", "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json"),
                     ("tpex_openapi_list", "https://www.tpex.org.tw/openapi/v1/swagger.json")):
        status, ctype, body, err = fetch_raw(url, timeout=config.PROBE_TIMEOUT, headers=ua)
        time.sleep(1)
        if status is None:
            log(f"probe:{tag}", "no_new_data", detail=_clip(f"連線失敗: {err}"))
            continue
        txt = body.decode("utf-8", errors="replace")
        if tag == "tpex_openapi_list":
            try:
                paths = [p for p in (json.loads(txt).get("paths") or {}) if "margin" in p.lower() or "credit" in p.lower()]
                log(f"probe:{tag}", "success" if paths else "no_new_data", detail=_clip(f"HTTP {status}｜名稱含 margin 或 credit 的端點 {len(paths)} 個：{paths[:14]}"))
            except (ValueError, AttributeError):
                log(f"probe:{tag}", "no_new_data", detail=_clip(f"HTTP {status}｜不是 JSON｜開頭 {txt[:80]!r}"))
            continue
        log(f"probe:{tag}", "success" if status == 200 and txt.strip().startswith("{") else "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜{len(body)} 位元組｜開頭 {re.sub(chr(10), ' ', txt[:700])}"))


def probe_round11():
    """只讀探測第十一批：1) 三率歷史的備用來源（FinMind 的財報資料：證交所開放資料只放「已公布的最新一季」，上一季會被換掉）；
    2) Nasdaq 的公司簡介（有沒有產業 Sector／Industry，給美股持股分類用）；3) 證交所 FMTQIK 能不能取得十年前的月份。不寫入任何資料表。"""
    rows, e = fetch_json("https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id=2330&start_date=2025-01-01")
    if isinstance(rows, dict) and isinstance(rows.get("data"), list) and rows["data"]:
        d = rows["data"]
        types = sorted({str(r.get("type")) for r in d if isinstance(r, dict)})[:14]
        log("probe:finmind_fin", "success", detail=_clip(f"status={rows.get('status')}｜{len(d)} 列｜日期 {min(str(r.get('date')) for r in d)} 到 {max(str(r.get('date')) for r in d)}｜欄位 {list(d[0].keys())}｜type 種類 {types}｜前 2 列 {d[:2]}"))
    else:
        log("probe:finmind_fin", "no_new_data", detail=_clip(f"FinMind 抓取失敗或格式非預期: {e or str(rows)[:140]}"))
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa-probe/1.0)", "Accept": "application/json"}
    for sym in ("ORCL", "ARM"):
        status, ctype, body, err = fetch_raw(f"https://api.nasdaq.com/api/company/{sym}/company-profile", timeout=config.PROBE_TIMEOUT, headers=ua)
        if status is None:
            log(f"probe:nasdaq_profile:{sym}", "no_new_data", detail=_clip(f"連線失敗: {err}"))
            continue
        try:
            j = json.loads(body.decode("utf-8-sig"))
            d_ = j.get("data") or {}
            log(f"probe:nasdaq_profile:{sym}", "success" if status == 200 and d_ else "no_new_data", detail=_clip(f"HTTP {status}｜data 欄位 {list(d_.keys())[:14] if isinstance(d_, dict) else type(d_).__name__}｜前 600 字 {json.dumps(d_, ensure_ascii=False)[:600]}"))
        except (ValueError, UnicodeDecodeError, AttributeError, TypeError):
            log(f"probe:nasdaq_profile:{sym}", "no_new_data", detail=_clip(f"HTTP {status}｜{ctype}｜不是預期的 JSON｜開頭 {body[:100]!r}"))
    old_month = f"{date.today().year - 10}{date.today().month:02d}01"
    rows, why = _twse_month_rows(f"{config.TWSE_FMTQIK}?response=json&date={old_month}")
    log("probe:fmtqik_old", "success" if rows else "no_new_data", detail=_clip(f"{old_month[:6]}：{len(rows) if rows is not None else '抓取失敗'} 個交易日｜{why or ''}｜" + (f"首尾 {rows[0]['as_of']} 收 {rows[0]['close']}、{rows[-1]['as_of']} 收 {rows[-1]['close']}" if rows else "")))


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
def parse_margin_stock_table(data):
    """證交所 MI_MARGN（selectType=ALL）的逐檔融資表 → ({代號: 融資今日餘額（張）}, 錯誤原因)。
    欄位名稱有重複（融資與融券各有買進、賣出、前日餘額、今日餘額），所以用位置：代號、名稱、買進、賣出、現金償還、前日餘額、今日餘額…，
    並且逐列驗證「前日餘額＋買進－賣出－償還＝今日餘額」，通過的列要在 98% 以上，否則整份不採用（代表欄位位置和我們以為的不一樣）。"""
    tabs = [t for t in (data.get("tables") or []) if t.get("fields") and t.get("data")]
    if not tabs:
        return None, "找不到逐檔融資表"
    big = max(tabs, key=lambda t: len(t["data"]))
    f = [str(x).strip() for x in big["fields"]]
    if f[:7] != ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額"] or f.count("今日餘額") < 2:
        return None, f"逐檔融資表欄位與預期不符：{f[:9]}"
    out, ok_n, n = {}, 0, 0
    for r in big["data"]:
        try:
            code = str(r[0]).strip()
            buy, sell, rep, prev, today = (parse_number(r[i]) for i in (2, 3, 4, 5, 6))
        except (IndexError, TypeError):
            continue
        if None in (buy, sell, rep, prev, today) or not code:
            continue
        n += 1
        if abs(prev + buy - sell - rep - today) <= 1:
            ok_n += 1
        if today > 0:
            out[code] = today
    if n < 100 or ok_n / n < 0.98:
        return None, f"逐檔融資表有 {n} 列可解析，其中只有 {ok_n} 列通過「前日＋買進－賣出－償還＝今日」，不採用"
    return out, None


def tw_margin_ratio_est():
    """模組 M21：自行估算上市的融資維持率（總量推估，和民間軟體同一類算法）＝ Σ（逐檔融資餘額 × 1000 × 收盤價）÷ 融資金額。
    這不是證交所公布的「整戶擔保維持率」（官方是融資加融券合併、按每個帳戶算，數字通常高出約 30 個百分點）；畫面上一律標「估算」。
    日期必須一致：融資表的日期、全部上市股票收盤價的日期、都要等於同一天，否則不算（不拿昨天的價格配今天的餘額）。
    收盤價查不到的融資股票超過 5% 就不採用。只能從啟用那天起每天累積：證交所逐檔融資不能指定日期取得歷史。"""
    data, err = fetch_json(config.TWSE_MI_MARGN_ALL)
    if data is None:
        log("tw_margin_ratio", "failure", detail=_clip(f"逐檔融資抓取失敗: {err}"))
        return []
    if not isinstance(data, dict) or data.get("stat") != "OK":
        log("tw_margin_ratio", "no_new_data", detail=f"證交所回覆 stat={data.get('stat') if isinstance(data, dict) else '非預期格式'}")
        return []
    as_of = twse_ymd_to_iso(data.get("date"))
    ok, why = validate_close_date(as_of)
    if not ok:
        log("tw_margin_ratio", "failure", as_of=as_of, detail=f"資料日期異常: {why}")
        return []
    units, perr = parse_margin_stock_table(data)
    total, lerr = _parse_margin(data)
    if perr or lerr:
        log("tw_margin_ratio", "failure", as_of=as_of, detail=_clip(perr or lerr))
        return []
    px, err = fetch_json(config.TWSE_OPENAPI_BASE + "/exchangeReport/STOCK_DAY_ALL")
    if not (isinstance(px, list) and px and isinstance(px[0], dict)):
        log("tw_margin_ratio", "failure", as_of=as_of, detail=_clip(f"全部上市股票收盤價抓取失敗或格式非預期: {err or str(px)[:80]}"))
        return []
    dates = Counter(roc_to_iso(r.get("Date")) for r in px if isinstance(r, dict))
    px_date = dates.most_common(1)[0][0] if dates else None
    if px_date != as_of:
        log("tw_margin_ratio", "no_new_data", as_of=as_of, detail=f"收盤價的日期是 {px_date}，融資表是 {as_of}，兩天不一致，不估算（不拿別天的價格配這天的餘額）")
        return []
    close = {}
    for r in px:
        c = parse_number(r.get("ClosingPrice"))
        if isinstance(r, dict) and c and c > 0:
            close[str(r.get("Code", "")).strip()] = c
    value, missing = 0.0, 0
    for code, u in units.items():
        if code in close:
            value += u * 1000 * close[code]
        else:
            missing += 1
    if not units or missing / len(units) > 0.05:
        log("tw_margin_ratio", "failure", as_of=as_of, detail=f"有融資餘額的 {len(units)} 檔裡，{missing} 檔查不到收盤價（超過 5%），不估算")
        return []
    ratio = value / total["margin_balance"] * 100
    if not (80 <= ratio <= 400):
        log("tw_margin_ratio", "failure", as_of=as_of, detail=f"估算值 {ratio:.1f}% 不在合理範圍 80–400%，拒收")
        return []
    log("tw_margin_ratio", "success", as_of=as_of, detail=f"估算維持率 {ratio:.1f}%（{len(units) - missing} 檔、融資金額 {total['margin_balance'] / 1e8:,.0f} 億元、融資股票市值 {value / 1e8:,.0f} 億元；{missing} 檔查不到收盤價）")
    return [{"as_of": as_of, "est_ratio": round(ratio, 2), "margin_value": round(value), "margin_loan": round(total["margin_balance"]), "n_stocks": len(units) - missing, "n_missing": missing,
             "source": "自行估算（證交所逐檔融資餘額 × 收盤價 ÷ 融資金額；不是官方整戶擔保維持率）"}]


_OPT_RE = re.compile(r"^([A-Z]{1,6}\d?)(\d{6})([CP])(\d{8})$")
_OPT_SPOT_RANGE = {"SPX": (1000, 20000), "SPY": (100, 2000), "QQQ": (100, 1500)}


def parse_options_chain(j, underlying, today=None, near_days=45, band=0.15):
    """Cboe 延遲選擇權鏈 → ({摘要}, [履約價列], 錯誤原因)。
    資料日用標的最後成交時間（last_trade_time）的日期；必須是平日，且在今天往前 5 天內。現價必須在合理範圍。
    履約價列：只收到期日在 near_days 天內、履約價在現價 ±band 內的合約，同一履約價把各到期日的買權、賣權未平倉量加總；
    摘要的買權、賣權總量則包含所有到期日。IV30 是標的的 30 天隱含波動率（0.145 這種小數會換成 14.5；超出 3–200 就當作查不到）。
    OI 是前一個交易日收盤後公布的數字，所以快照要等到美東早上（OCC 更新之後）再抓。"""
    today = today or date.today()
    d_ = (j or {}).get("data") or {}
    opts = d_.get("options") or []
    spot = parse_number(d_.get("current_price"))
    lo, hi = _OPT_SPOT_RANGE[underlying]
    if spot is None or not (lo <= spot <= hi):
        return None, [], f"現價 {spot} 不在合理範圍 {lo}–{hi}"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(d_.get("last_trade_time", "")))
    if not m:
        return None, [], f"看不懂標的的最後成交時間 {d_.get('last_trade_time')!r}，不採用"
    try:
        as_of = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None, [], f"最後成交時間的日期不合法 {d_.get('last_trade_time')!r}"
    if as_of.weekday() >= 5 or not (today - timedelta(days=5) <= as_of <= today):
        return None, [], f"資料日 {as_of} 不是最近 5 天內的平日，不採用"
    if len(opts) < 100:
        return None, [], f"選擇權合約只有 {len(opts)} 檔，不採用"
    call_tot = put_tot = n = 0
    strikes = {}
    for o in opts:
        mm = _OPT_RE.match(str(o.get("option", "")))
        oi = parse_number(o.get("open_interest"))
        if not mm or oi is None or oi < 0:
            continue
        try:
            exp = date(2000 + int(mm.group(2)[:2]), int(mm.group(2)[2:4]), int(mm.group(2)[4:]))
        except ValueError:
            continue
        n += 1
        k, oi = int(mm.group(4)) / 1000, int(oi)
        if mm.group(3) == "C":
            call_tot += oi
        else:
            put_tot += oi
        if as_of <= exp <= as_of + timedelta(days=near_days) and abs(k / spot - 1) <= band and oi > 0:
            row = strikes.setdefault(k, {"underlying": underlying, "as_of": as_of.isoformat(), "strike": k, "call_oi": 0, "put_oi": 0})
            row["call_oi" if mm.group(3) == "C" else "put_oi"] += oi
    if n < 100 or call_tot + put_tot <= 0:
        return None, [], f"能解析的合約只有 {n} 檔，不採用"
    iv = parse_number(d_.get("iv30"))
    if iv is not None and iv < 3:
        iv *= 100
    if iv is not None and not (3 <= iv <= 200):
        iv = None
    chg = parse_number(d_.get("iv30_change"))
    summary = {"underlying": underlying, "as_of": as_of.isoformat(), "spot": spot, "iv30": round(iv, 2) if iv is not None else None, "iv30_change": chg,
               "call_oi": call_tot, "put_oi": put_tot, "n_contracts": n, "source": "Cboe 延遲選擇權鏈"}
    return summary, sorted(strikes.values(), key=lambda r: r["strike"]), None


def us_options_snapshot():
    """模組 M20：美股選擇權快照（Cboe 延遲選擇權鏈）：標普 500 指數（SPX，核心）、SPY 與 QQQ（選用，抓不到只記沒有資料）。
    每天存一份摘要（現價、IV30、買賣權總未平倉量）與履約價未平倉量，靠每天快照比較 OI 變動。
    在台北時間 20:45 抓（美東早上，OCC 已經更新前一天的 OI、標的價格是前一個交易日的收盤）。"""
    ua = {"User-Agent": "Mozilla/5.0 (compatible; risk-pwa/1.0)", "Accept": "application/json"}
    out = []
    for sym, und, optional in (("_SPX", "SPX", False), ("SPY", "SPY", True), ("QQQ", "QQQ", True)):
        status, ctype, body, err = fetch_raw(f"https://cdn.cboe.com/api/global/delayed_quotes/options/{sym}.json", timeout=config.OPTIONS_TIMEOUT, headers=ua)
        tag, bad = f"us_options:{und}", ("no_new_data" if optional else "failure")
        if status != 200:
            log(tag, bad, detail=_clip(f"抓取失敗：HTTP {status} {err}"))
            time.sleep(1)
            continue
        try:
            j = json.loads(body.decode("utf-8-sig"))
        except (ValueError, UnicodeDecodeError):
            log(tag, bad, detail=_clip(f"不是 JSON（{ctype}）：{body[:80]!r}"))
            continue
        summary, strikes, perr = parse_options_chain(j, und)
        if perr:
            log(tag, bad, detail=_clip(perr))
        else:
            pcr = summary["put_oi"] / summary["call_oi"] if summary["call_oi"] else None
            log(tag, "success", as_of=summary["as_of"], detail=f"現價 {summary['spot']}｜IV30 {summary['iv30']}｜買權 OI {summary['call_oi']:,}、賣權 OI {summary['put_oi']:,}（PCR {pcr:.2f}）｜履約價 {len(strikes)} 檔｜合約 {summary['n_contracts']:,} 檔")
            out.append({"summary": summary, "strikes": strikes})
        time.sleep(1)
    return out


def us_options_job(dry_run=False):
    """美股選擇權專用工作（台北時間週二到週六 20:45）：只跑 M20，把摘要與履約價未平倉量寫進資料庫。"""
    res = _run_module("M20", "美股選擇權快照（Cboe）", us_options_snapshot) or []
    persist_extra(dry_run, [("us_options_summary", [r["summary"] for r in res], "underlying,as_of"),
                            ("us_options_strikes", [row for r in res for row in r["strikes"]], "underlying,as_of,strike")])
    return {"job": "us_options", "underlyings": [r["summary"]["underlying"] for r in res]}


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
    if job == "us_options":                                       # 美股選擇權快照專用
        return us_options_job(dry_run)
    macro_rows, twd_row, tw_rows, breadth_rows = [], None, None, None
    inst_rows = margin_rows = futures_rows = basis_rows = price_rows = cot_rows = putcall_rows = earnings_rows = None
    us_watch = []                                                  # 美股候選股代號（只在 tw_close／full 讀）

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
        us_watch = load_us_watch()
        if hold is None:
            log("prices:holdings", "skipped", detail=_clip(f"{herr}；不抓持股現價"))
        else:
            tp = tpex_prices(hold) or []
            tw = twse_prices(hold, {r["name"] for r in tp}) or []
            hold_us = with_us_watch(hold, us_watch)                  # 持股＋美股候選股：美股價格與財報日一起抓
            price_rows = tw_ranges(tp + tw) + (nasdaq_prices(hold_us) or [])
            earnings_rows = nasdaq_earnings(hold_us)
    if job == "tw_probe":
        probe_twse()
        probe_extra()
        probe_taifex_daily()
        probe_dashboard()
        probe_fin_coverage()
        probe_nasdaq_access()
        probe_chips()
        probe_vix_otc()
    if job == "us_probe":
        probe_us_pe()
        probe_earnings()
        probe_round3()
        probe_round4()
        probe_round5()
        probe_round6()
        probe_round7()
        probe_round8()
        probe_round9()
        probe_round10()
        probe_round11()
        probe_round12()
        probe_round13()
        probe_round14()
        probe_round15()
        probe_round16()
    inst_hist, pc_rows, margin_hist, index_long, us_long = [], [], [], [], []                                  # 回補專用的變數；不能叫 inst_rows，否則會把每天抓到的外資買賣超清掉
    if job == "tw_backfill":
        res = tw_backfill(dry_run=dry_run) or {}
        tw_breadth_update(dry_run=dry_run, max_days=40)
        tw_crash_check(list(res.get("new_days", [])) + _recent_complete_days(config.CRASH_RECENT_DAYS), dry_run=dry_run)
        if not dry_run:
            _run_module("M13", "漲跌家數歷史（自建）", tw_breadth_hist_compute)
            inst_hist = _run_module("M12", "外資買賣歷史回補", tw_institutional_backfill) or []
            margin_hist = _run_module("M19", "上市融資餘額歷史回補", tw_margin_backfill) or []
    if job == "tw_backfill2" and not dry_run:                                                     # 長回補另開一個工作：一個工作塞太多會超過時間上限而被取消、什麼都沒寫入
        pc_rows = _run_module("M14", "Put/Call 歷史回補", cboe_putcall_backfill) or []
        pc_rows = (pc_rows or []) + (_run_module("M44", "Put/Call 舊版歷史檔（CBOE totalpc.csv）", cboe_pc_csv_history) or [])
        margin_hist = _run_module("M43", "上市融資餘額長期回補（10 年）", tw_margin_long_backfill) or []
        index_long = _run_module("M24", "加權指數長期歷史回補", tw_index_long_history) or []
        us_long = _run_module("M35", "美股指數長期歷史（標普 500、Nasdaq 綜合，FRED）", us_index_long_history) or []
    if job in ("full", "tw_close") and not dry_run:
        res = tw_backfill(dry_run=False, max_days=config.HIST_DAILY_DAYS, window_days=7) or {}
        tw_breadth_update(dry_run=False, max_days=10)
        tw_crash_check(list(res.get("new_days", [])) + _recent_complete_days(1))
    extra = []
    if job == "us_breadth":
        if dry_run:
            log("us_breadth", "skipped", detail="試跑模式不抓 100 檔成分股（要發 100 多個請求）")
        elif not config.FEATURES.get("M32_us_breadth_ndx", True):
            log("us_breadth", "skipped", detail="M32 已在設定裡關閉")
        else:
            extra.append(("us_breadth_ndx", _run_module("M32", "美股廣度（Nasdaq-100）", us_breadth_ndx) or [], "as_of"))
            extra.append(("us_daily_bars", _run_module("M34", "SPY 日線（含成交量）", us_daily_bars) or [], "symbol,as_of"))

    if job in ("full", "tw_close"):
        extra.append(("us_quotes", _run_module("M02", "美股其他行情", us_extra_quotes) or [], "symbol,as_of"))
        extra.append(("sector_rs", _run_module("M03", "美股產業相對強弱", sector_rs) or [], "symbol,as_of"))
        if not dry_run:
            extra.append(("tw_leaders", _run_module("M04", "台股領頭股", tw_leaders_compute) or [], "as_of,code"))
        extra.append(("tw_fin_q", _run_module("M07", "台股三率", tw_financials, price_rows or []) or [], "code,year,quarter"))
        extra.append(("us_crash_candidates", _run_module("M08", "美股大型股暴跌", us_crash_screen) or [], "as_of,symbol"))
        extra.append(("us_aaii", _run_module("M09", "AAII 散戶情緒", aaii_sentiment) or [], "as_of"))
        if not dry_run:
            extra.append(("us_cot", _run_module("M33", "CFTC COT：Nasdaq-100 期貨", cftc_cot_ndx) or [], "report_date,market"))
        extra.append(("us_leaders", _run_module("M10", "美股領頭股", us_leaders_compute) or [], "as_of,symbol"))
        extra.append(("tw_investor_events", _run_module("M11", "台股法說會日曆", tw_investor_events, price_rows or []) or [], "code,event_date"))
        extra.append(("tw_exright_events", _run_module("M15", "台股除權息預告（上市、上櫃）", tw_exright_preview) or [], "market,code,event_date"))
        if not dry_run:
            extra.append(("tw_exright_events", _run_module("M16", "歷史除權息（含參考價）", tw_exright_history) or [], "market,code,event_date"))
            extra.append(("tw_margin_ratio", _run_module("M21", "上市融資維持率估算", tw_margin_ratio_est) or [], "as_of"))
            twopt = _run_module("M22", "台指選擇權與台指期快照", tw_options_snapshot) or []
            extra.append(("tw_options_summary", [r["summary"] for r in twopt], "as_of"))
            extra.append(("tw_options_strikes", [row for r in twopt for row in r["strikes"]], "as_of,strike"))
            extra.append(("macro_series", _run_module("M31", "美元指數 DXY（Yahoo，非官方）", yahoo_dxy) or [], "series_id,as_of"))
            extra.append(("watch_snapshot", _run_module("M30", "候選股估值（本益比、殖利率、股價淨值比）", watch_valuation) or [], "code"))
            extra.append(("tpex_margin_daily", _run_module("M29", "上櫃融資餘額（張數）", tpex_margin_daily) or [], "as_of"))
            extra.append(("tw_retail_ratio", _run_module("M28", "小台指散戶多空比", tw_retail_ratio) or [], "as_of"))
            extra.append(("tw_pcr_official", _run_module("M23", "臺指選擇權 Put/Call 比（官方）", tw_pcr_official) or [], "as_of"))
            extra.append(("macro_series", _run_module("M27", "Yahoo 補 FRED 落後（標普、原油）", yahoo_fill) or [], "series_id,as_of"))
            extra.append(("tw_keep_rate", _run_module("M38", "臺股儀表板：全市場擔保維持率（官方）", tw_keep_rate) or [], "as_of,source"))
            twd_hist = _run_module("M37", "美元兌台幣每日歷史（FRED）", twd_fred_history) or []
            extra.append(("twd_rates", twd_hist, "layer,source,as_of"))
            extra.append(("twd_rates", _run_module("M36", "Yahoo 補美元兌台幣落後的天數（非官方）", yahoo_twd_fill, twd_hist) or [], "layer,source,as_of"))
            extra.append(("us_sector_auto", _run_module("M26", "美股產業自動帶入（Nasdaq）", us_sector_lookup, with_us_watch(hold or [], us_watch)) or [], "symbol"))
            extra.append(("us_valuation", _run_module("M39", "美股本益比（Nasdaq 個股摘要）", us_valuation, with_us_watch(hold or [], us_watch)) or [], "symbol"))
            extra.append(("stock_risk_stats", _run_module("M45", "個股 Beta、放空占比、機構持股", stock_risk_stats, with_us_watch(hold or [], us_watch), sorted({str(r.get("code")) for r in (price_rows or []) if r.get("market") == "TW" and r.get("code")})) or [], "market,code"))
            extra.append(("tw_otc_index", _run_module("M40", "櫃買指數（櫃買中心開放資料）", tw_otc_index) or [], "as_of"))
            extra.append(("tw_rel_strength", _run_module("M41", "個股對大盤（近 5、20 日）", tw_rel_strength, price_rows or []) or [], "code"))
            extra.append(("tw_fin_q_ext", _run_module("M25", "三率歷史季度（FinMind，第三方）", finmind_fin_history, price_rows or []) or [], "code,year,quarter"))
    if job in ("full", "tw_close") and not dry_run:
        extra.append(("macro_series", _run_module("M18", "美股指數官方行情（補 FRED 落後）", us_index_quotes) or [], "series_id,as_of"))
        extra.append(("macro_series", _run_module("M42", "費城半導體指數 SOX 歷史（Nasdaq）", sox_history) or [], "series_id,as_of"))
    if job == "tw_backfill2":
        extra.append(("us_putcall", pc_rows, "as_of"))
        extra.append(("tw_margin", margin_hist, "as_of,source"))
        extra.append(("market_stats", index_long, "market,as_of,source"))
        extra.append(("macro_series", us_long, "series_id,as_of"))
        if not dry_run:
            extra.append(("tw_breadth_hist", _run_module("M46", "上市漲跌家數長期回補（10 年）", tw_breadth_long_backfill) or [], "as_of,market"))
            extra.append(("tw_retail_ratio", _run_module("M49", "小台指散戶多空比回補（期交所歷史下載）", tw_retail_ratio_backfill) or [], "as_of"))
            extra.append(("us_aaii", _run_module("M48", "AAII 完整歷史匯入（你下載的檔案）", aaii_csv_import) or [], "as_of"))
            extra.append(("tw_margin_ratio", _run_module("M47", "上市融資維持率估算回補", tw_margin_ratio_backfill) or [], "as_of"))
    if job == "tw_backfill":
        extra.append(("tw_institutional", inst_hist, "as_of,source"))
        extra.append(("us_putcall", pc_rows, "as_of"))
        extra.append(("tw_margin", margin_hist, "as_of,source"))
        extra.append(("market_stats", index_long, "market,as_of,source"))
        extra.append(("macro_series", us_long, "series_id,as_of"))
        if not dry_run:
            extra.append(("tw_industry", _run_module("M17", "台股產業別", tw_industry_collect) or [], "code"))
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


def _gh_escape(text, prop=False):
    """GitHub Actions 的 ::error:: 訊息裡，% 與換行要跳脫；當作屬性（title）時，冒號和逗號也要。"""
    t = str(text).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return t.replace(":", "%3A").replace(",", "%2C") if prop else t


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
    for r in failures:                                                 # 把失敗的原因直接印在 Actions 紀錄裡（以前只有數字，看不出是哪一項）
        print(f"  失敗｜{r['source']}｜{str(r.get('detail') or '')[:300]}")
        print(f"::error title={_gh_escape(r['source'], True)}::{_gh_escape(str(r.get('detail') or '')[:300])}")      # GitHub 會把它顯示在這次執行的摘要頁最上面
    if not log_ok:
        print("  失敗｜寫入資料庫｜這一輪的紀錄（scan_status）沒有確認寫入成功，請檢查 Supabase 連線與金鑰")
        print("::error title=寫入資料庫::這一輪的紀錄沒有確認寫入成功")
    return 1 if (failures or not log_ok) else 0


if __name__ == "__main__":
    raise SystemExit(main())
