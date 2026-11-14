"""用模擬回傳測試：壞資料、舊日期、被擋、寫入不確認、備用鏈。
不連網、不需要金鑰。pytest 或 python3 直接跑都可以。"""
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline import fetch, config  # noqa: E402

TODAY = date.today()
D = lambda n: (TODAY - timedelta(days=n)).isoformat()  # noqa: E731


MODULE_FUNCS = ("us_extra_quotes", "sector_rs", "tw_leaders_compute", "tw_index_history", "fred_history_backfill",
                "tw_financials", "us_crash_screen", "aaii_sentiment", "us_leaders_compute", "tw_investor_events",
                "tw_institutional_backfill", "tw_breadth_hist_compute", "cboe_putcall_backfill", "tw_exright_preview", "tw_exright_history", "tw_industry_collect", "us_index_quotes", "tw_margin_backfill", "tw_margin_long_backfill", "sox_history", "cboe_pc_csv_history", "tw_margin_ratio_est", "us_options_snapshot", "tw_options_snapshot", "tw_pcr_official", "tw_index_long_history", "us_sector_lookup", "finmind_fin_history", "yahoo_fill", "tw_retail_ratio", "tpex_margin_daily", "watch_valuation", "yahoo_dxy", "us_breadth_ndx", "cftc_cot_ndx", "us_daily_bars", "probe_dashboard", "probe_fin_coverage", "load_us_watch", "probe_nasdaq_access", "us_index_long_history", "yahoo_twd_fill", "twd_fred_history", "tw_keep_rate", "probe_chips", "us_valuation", "probe_vix_otc", "tw_otc_index", "tw_rel_strength", "stock_risk_stats", "tw_breadth_long_backfill", "tw_margin_ratio_backfill", "aaii_csv_import")
REAL = {n: getattr(fetch, n) for n in MODULE_FUNCS}          # 模組真正的函式；測試模組本身時用 REAL[...]
OFFENDERS = []
NETWORK_CALLS = []                                             # 任何沒有被測試替換掉的真實連線都會記在這裡，最後整個測試會失敗


def reset():
    fetch.LOG.clear()
    fetch._JSON_CACHE.clear()                                   # 說明文件的快取不能跨測試
    for n in MODULE_FUNCS:                                      # 其他測試不該執行這些會連網的模組（它們有自己的測試）
        setattr(fetch, n, lambda *a, **k: [])


def fred_payload(*pairs):
    return {"observations": [{"date": d, "value": v} for d, v in pairs]}


def with_key():
    return mock.patch.object(config, "fred_key", return_value="TESTKEY")


def test_future_date_rejected():
    ok, why = fetch.validate_close_date((TODAY + timedelta(days=5)).isoformat())
    assert not ok and "晚於" in why


def test_fred_skips_holiday_dot_and_keeps_own_date():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(fred_payload((D(0), "."), (D(1), "4.05")), "")):
        r = fetch.fred_latest("DGS10", 0, 15)
    assert r["value"] == 4.05 and r["as_of"] == D(1)  # 用它自己的日期，不是今天


def test_fred_out_of_range_rejected():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(fred_payload((D(1), "999")), "")):
        r = fetch.fred_latest("DGS10", 0, 15)
    assert r is None and fetch.LOG[-1]["status"] == "failure"


def test_fred_future_date_rejected():
    reset()
    future = (TODAY + timedelta(days=3)).isoformat()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(fred_payload((future, "4.0")), "")):
        assert fetch.fred_latest("DGS10", 0, 15) is None


def test_fred_blocked_logs_failure_not_fabricate():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.fred_latest("DGS10", 0, 15) is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_no_key_is_skipped_not_failure():
    reset()
    with mock.patch.object(config, "fred_key", return_value=""):
        assert fetch.fred_latest("DGS10", 0, 15) is None
    assert fetch.LOG[-1]["status"] == "skipped"


def test_key_never_appears_in_log():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "boom TESTKEY boom")):
        fetch.fred_latest("DGS10", 0, 15)
    assert "TESTKEY" not in str(fetch.LOG)


def test_twd_fred_first_and_counts_as_close_history():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(fred_payload((D(1), "31.5")), "")):
        row = fetch.twd_fallback_chain()
    assert row["layer"] == 3 and row["is_close_history"] is True


def test_twd_layer5_never_close_history():
    reset()
    spot = {"result": "success", "base_code": "USD", "rates": {"TWD": 31.9},
            "time_last_update_unix": int(__import__("time").time()) - 3600}
    def fake(url, **k):
        return (spot, "") if "er-api" in url else (None, "blocked")
    with with_key(), mock.patch.object(fetch, "fetch_json", side_effect=fake):
        row = fetch.twd_fallback_chain()
    assert row["layer"] == 5 and row["is_close_history"] is False and row["kind"] == "spot_aggregator"


def test_exchangerate_rejects_wrong_base():
    reset()
    bad = {"result": "success", "base_code": "EUR", "rates": {"TWD": 31.9}, "time_last_update_unix": 1}
    with mock.patch.object(fetch, "fetch_json", return_value=(bad, "")):
        assert fetch.exchangerate_twd() is None


def test_all_layers_fail_reuses_last_verified_with_real_date():
    reset()
    last = {"as_of": "2026-10-20", "value": 31.4, "source": "FRED DEXTAUS", "layer": 3}
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "down")), \
            mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "sb_last_verified_twd", return_value=last):
        row = fetch.twd_fallback_chain()
    assert row["stale_fallback"] is True and row["as_of"] == "2026-10-20"  # 舊值配舊日期


def test_upsert_count_mismatch_is_not_success():
    with mock.patch.object(fetch, "sb_request", return_value=([{"id": 1}], "")):
        ok, _ = fetch.sb_upsert("macro_series", [{"a": 1}, {"a": 2}], "series_id,as_of")
    assert not ok


def test_upsert_http_error_is_failure():
    with mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 401")):
        ok, _ = fetch.sb_upsert("macro_series", [{"a": 1}], "series_id,as_of")
    assert not ok


def test_us_breadth_job_does_not_run_full_update():
    reset()
    with mock.patch.object(fetch, "fred_latest") as f, mock.patch.object(fetch, "twd_fallback_chain") as t:
        fetch.run_job("us_breadth", dry_run=True)
    f.assert_not_called()
    t.assert_not_called()


def test_dry_run_does_not_write_supabase():
    reset()
    with mock.patch.object(fetch, "sb_request") as req, mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "fred_latest", return_value=None), \
            mock.patch.object(fetch, "twse_market", return_value=None), \
            mock.patch.object(fetch, "twse_breadth", return_value=None), \
            mock.patch.object(fetch, "twse_institutional", return_value=None), \
            mock.patch.object(fetch, "twse_margin", return_value=None), \
            mock.patch.object(fetch, "taifex_futures", return_value=None), \
            mock.patch.object(fetch, "taifex_basis", return_value=None), \
            mock.patch.object(fetch, "cftc_cot", return_value=None), mock.patch.object(fetch, "cboe_putcall", return_value=None), \
            mock.patch.object(fetch, "load_holdings", return_value=(None, "測試")), \
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None):
        fetch.run_job("full", dry_run=True)
    req.assert_not_called()


# ---------------------------------------------------------------- 證交所 FMTQIK
FIELDS = ["Date", "Trade Volume", "Trade Value", "Transaction", "TAIEX", "Change"]


def twse_payload(*rows, stat="OK"):
    return {"stat": stat, "fields": FIELDS, "data": [list(r) for r in rows]}


def good_row(d, close="44,762.32", change="-461.97", value="656,280,679,689"):
    return [d, "8,082,548,083", value, "3,876,321", close, change]


def test_twse_parses_commas_and_converts_pct():
    reset()
    d = TODAY - timedelta(days=1)
    day = f"{d.year}/{d.month:02d}/{d.day:02d}"
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_payload(good_row(day)), "")):
        rows = fetch.twse_market()
    r = rows[0]
    assert r["as_of"] == d.isoformat()            # 用資料自己的日期
    assert r["close"] == 44762.32 and r["turnover"] == 656280679689.0
    assert abs(r["change_pct"] - (-461.97 / (44762.32 + 461.97) * 100)) < 0.001
    assert "breadth" not in str(r) and "advancers" not in r  # 不冒充廣度


def test_twse_future_date_rejected():
    reset()
    f = TODAY + timedelta(days=3)
    day = f"{f.year}/{f.month:02d}/{f.day:02d}"
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_payload(good_row(day)), "")):
        result = fetch.twse_market()
    assert not result
    assert any(x["status"] in ("failure", "no_new_data") for x in fetch.LOG)


def test_twse_out_of_range_index_rejected():
    reset()
    d = TODAY - timedelta(days=1)
    day = f"{d.year}/{d.month:02d}/{d.day:02d}"
    bad = good_row(day, close="5.00", change="0.10")
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_payload(bad), "")):
        rows = fetch.twse_market()
    assert not rows
    assert any(x["status"] == "failure" for x in fetch.LOG)


def test_twse_zero_turnover_rejected():
    reset()
    d = TODAY - timedelta(days=1)
    day = f"{d.year}/{d.month:02d}/{d.day:02d}"
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_payload(good_row(day, value="0")), "")):
        assert not fetch.twse_market()


def test_twse_blocked_logs_failure():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.twse_market() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_twse_unexpected_fields_is_failure_not_guess():
    reset()
    weird = {"stat": "OK", "fields": ["A", "B"], "data": [["1", "2"]]}
    with mock.patch.object(fetch, "fetch_json", return_value=(weird, "")):
        assert fetch.twse_market() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_twse_empty_month_falls_back_to_previous_month():
    reset()
    d = TODAY - timedelta(days=1)
    day = f"{d.year}/{d.month:02d}/{d.day:02d}"
    calls = []
    def fake(url, **k):
        calls.append(url)
        if "date=" in url:
            return twse_payload(good_row(day)), ""
        return {"stat": "No data"}, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fake):
        rows = fetch.twse_market()
    assert rows and len(calls) == 2 and "date=" in calls[1]


def test_tw_close_job_writes_market_stats_with_confirmation():
    reset()
    rows = [{"market": "TW", "as_of": D(1), "close": 44000.0, "change_pct": 0.5,
             "turnover": 1e12, "source": "TWSE FMTQIK"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "sb_upsert", return_value=(True, "已確認寫入 1 筆")) as up:
        fetch.persist(False, [], None, rows)
    assert up.call_args[0][0] == "market_stats" and up.call_args[0][2] == "market,as_of,source"


# ---------------------------------------------------------------- 台股廣度
BREADTH_TABLE = {"title": "漲跌證券數合計", "fields": ["類型", "整體市場", "股票"],
                 "data": [["上漲(漲停)", "3,381(16)", "232(7)"], ["下跌(跌停)", "11,943(85)", "737(3)"],
                          ["持平", "753", "108"], ["未成交", "21,104", "6"], ["無比價", "2,823", "4"]]}


def breadth_payload(table=BREADTH_TABLE, date_str=None, stat="OK"):
    d = date_str or (TODAY - timedelta(days=1)).strftime("%Y%m%d")
    return {"stat": stat, "date": d, "tables": [{"title": "價格指數", "fields": ["a"], "data": []}, table]}


def test_parse_count():
    assert fetch.parse_count("3,381(16)") == (3381, 16)
    assert fetch.parse_count("753") == (753, None)
    assert fetch.parse_count("abc") == (None, None)
    assert fetch.parse_count("") == (None, None)


def test_breadth_uses_stock_column_not_whole_market():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(), "")):
        rows = fetch.twse_breadth()
    r = rows[0]
    assert (r["advancers"], r["decliners"], r["unchanged"]) == (232, 737, 108)  # 股票欄，不是 3381/11943
    assert r["as_of"] == (TODAY - timedelta(days=1)).isoformat()
    assert "close" not in r and "turnover" not in r  # 不冒充指數資料
    assert r["source"] == config.BREADTH_SOURCE


def test_breadth_missing_table_is_failure():
    reset()
    payload = {"stat": "OK", "date": (TODAY - timedelta(days=1)).strftime("%Y%m%d"), "tables": []}
    with mock.patch.object(fetch, "fetch_json", return_value=(payload, "")):
        assert fetch.twse_breadth() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_breadth_missing_stock_column_is_failure():
    reset()
    bad = {"title": "漲跌證券數合計", "fields": ["類型", "整體市場"], "data": [["上漲(漲停)", "1(1)"]]}
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(bad), "")):
        assert fetch.twse_breadth() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_breadth_missing_row_is_failure():
    reset()
    bad = dict(BREADTH_TABLE, data=[["上漲(漲停)", "3,381(16)", "232(7)"], ["持平", "753", "108"]])
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(bad), "")):
        assert fetch.twse_breadth() is None


def test_breadth_absurd_total_rejected():
    reset()
    bad = dict(BREADTH_TABLE, data=[["上漲(漲停)", "1,0(0)", "1"], ["下跌(跌停)", "2", "1"], ["持平", "3", "1"]])
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(bad), "")):
        assert fetch.twse_breadth() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_breadth_future_date_rejected():
    reset()
    future = (TODAY + timedelta(days=4)).strftime("%Y%m%d")
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(date_str=future), "")):
        assert fetch.twse_breadth() is None


def test_breadth_bad_date_format_rejected():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(date_str="oops"), "")):
        assert fetch.twse_breadth() is None


def test_breadth_not_ok_and_blocked():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(breadth_payload(stat="很抱歉"), "")):
        assert fetch.twse_breadth() is None
    assert fetch.LOG[-1]["status"] == "no_new_data"
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.twse_breadth() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_persist_writes_breadth_as_separate_upsert():
    reset()
    b = [{"market": "TW", "as_of": D(1), "advancers": 1, "decliners": 1, "unchanged": 1, "source": config.BREADTH_SOURCE}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, b)
    assert up.call_args[0][0] == "market_stats" and up.call_args[0][1] == b


def test_tw_close_runs_breadth_and_market():
    reset()
    with mock.patch.object(fetch, "twse_market", return_value=None) as m, \
            mock.patch.object(fetch, "twse_breadth", return_value=None) as b, \
            mock.patch.object(fetch, "twse_institutional", return_value=None) as i, \
            mock.patch.object(fetch, "twse_margin", return_value=None) as g, \
            mock.patch.object(fetch, "taifex_futures", return_value=None) as fu, \
            mock.patch.object(fetch, "taifex_basis", return_value=None) as ba, \
            mock.patch.object(fetch, "cftc_cot", return_value=None), mock.patch.object(fetch, "cboe_putcall", return_value=None), \
            mock.patch.object(fetch, "load_holdings", return_value=(None, "測試")), \
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None):
        fetch.run_job("tw_close", dry_run=True)
    fu.assert_called_once()
    ba.assert_called_once()
    m.assert_called_once()
    b.assert_called_once()
    i.assert_called_once()
    g.assert_called_once()


# ---------------------------------------------------------------- 探測
def test_probe_finds_breadth_endpoint_and_table():
    reset()
    swagger = {"paths": {"/x/A": {"get": {"summary": "每日收盤行情-漲跌證券數合計"}},
                         "/x/B": {"get": {"summary": "其他"}}}}
    mi = {"stat": "OK", "date": "20261028", "tables": [
        {"title": "價格指數", "fields": ["a"], "data": [["1"]]},
        {"title": "漲跌證券數合計", "fields": ["類型", "整體市場", "股票"],
         "data": [["上漲(漲停)", "500(3)", "400(3)"], ["下跌(跌停)", "300(1)", "250(1)"]]}]}
    def fake(url, **k):
        return (swagger, "") if "swagger" in url else (mi, "")
    with mock.patch.object(fetch, "fetch_json", side_effect=fake):
        fetch.probe_twse()
    text = " ".join(x["detail"] for x in fetch.LOG)
    assert "/x/A" in text and "/x/B" not in text
    assert any(x["source"] == "probe:breadth_table" and "整體市場" in x["detail"] for x in fetch.LOG)


def test_probe_failures_are_logged_not_hidden():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        fetch.probe_twse()
    assert [x["status"] for x in fetch.LOG].count("failure") == 2


def test_probe_never_writes_market_tables():
    reset()
    with mock.patch.object(fetch, "probe_twse"), mock.patch.object(fetch, "probe_extra"), mock.patch.object(fetch, "probe_taifex_daily"), \
            mock.patch.object(fetch, "sb_upsert") as up:
        fetch.run_job("tw_probe", dry_run=False)
    up.assert_not_called()


def test_probe_detail_is_clipped():
    reset()
    big = {"stat": "OK", "date": "1", "tables": [{"title": "t" * 5000, "fields": [], "data": []}]}
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (({"paths": {}}, "") if "swagger" in u else (big, ""))):
        fetch.probe_twse()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_extra_logs_structure_and_never_writes():
    reset()
    bfi = {"stat": "OK", "date": "20261029", "title": "三大法人", "fields": ["單位名稱", "買進金額", "賣出金額", "買賣差額"],
           "data": [["外資及陸資(不含外資自營商)", "1,000", "1,200", "-200"]]}
    margn = {"stat": "OK", "date": "20261029", "tables": [{"title": "信用交易統計", "fields": ["項目", "今日餘額"], "data": [["融資(交易單位)", "1"]]}]}
    swagger = {"paths": {"/p1": {"get": {"summary": "三大法人-區分各期貨契約"}}, "/p2": {"get": {"summary": "其他"}}}}
    def fake(url, **k):
        if "BFI82U" in url:
            return bfi, ""
        if "MI_MARGN" in url:
            return margn, ""
        return swagger, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fake), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_extra()
    text = " ".join(x["detail"] for x in fetch.LOG)
    assert "外資及陸資" in text and "融資" in text and "/p1" in text and "/p2" not in text
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_extra_failures_logged():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        fetch.probe_extra()
    st_ = [x["status"] for x in fetch.LOG]; assert st_.count("no_new_data") == len(config.PROBE_URLS) and st_.count("failure") == 1 and [x for x in fetch.LOG if x["status"] == "failure"][0]["source"] == "probe:taifex_swagger", "迴圈裡的端點抓不到：記成「沒有新資料」，不讓整個工作變紅；期交所 swagger 那段維持原樣: " + str(st_)
    # 一個探測回 307（沒有目標網址）：同樣只記錄，不丟例外、不記失敗
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 307: Temporary Redirect")):
        fetch.probe_extra()
    assert [x["source"] for x in fetch.LOG if x["status"] == "failure"] == ["probe:taifex_swagger"] and any("307" in x["detail"] and x["status"] == "no_new_data" for x in fetch.LOG), "307 的那些端點只記錄"


# ---------------------------------------------------------------- 外資買賣超、融資（真實格式）
def ymd(days_ago=1):
    return (TODAY - timedelta(days=days_ago)).strftime("%Y%m%d")


BFI_FIELDS = ["單位名稱", "買進金額", "賣出金額", "買賣差額"]
BFI_ROWS = [
    ["自營商(自行買賣)", "8,707,783,064", "8,891,915,950", "-184,132,886"],
    ["自營商(避險)", "23,003,271,986", "44,319,158,013", "-21,315,886,027"],
    ["投信", "20,194,219,519", "13,149,917,223", "7,044,302,296"],
    ["外資及陸資(不含外資自營商)", "285,346,964,081", "354,162,240,591", "-68,815,276,510"],
    ["外資自營商", "0", "0", "0"],
    ["合計", "337,252,238,650", "420,523,231,777", "-83,270,993,127"],
]


def bfi_payload(rows=None, fields=None, date_str=None, stat="OK"):
    return {"stat": stat, "date": date_str or ymd(), "fields": fields or BFI_FIELDS, "data": rows or BFI_ROWS}


def test_institutional_real_format():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(), "")):
        rows = fetch.twse_institutional()
    r = rows[0]
    assert r["foreign_net"] == -68815276510.0       # 外資及陸資 + 外資自營商(0)
    assert r["foreign_buy"] == 285346964081.0 and r["foreign_sell"] == 354162240591.0
    assert r["trust_net"] == 7044302296.0
    assert r["dealer_net"] == -184132886.0 + -21315886027.0   # 兩種自營商相加，不含外資自營商
    assert r["total_net"] == -83270993127.0
    assert r["as_of"] == (TODAY - timedelta(days=1)).isoformat()


def test_institutional_adds_foreign_dealer():
    reset()
    rows = [list(x) for x in BFI_ROWS]
    rows[4] = ["外資自營商", "300", "100", "200"]
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(rows), "")):
        r = fetch.twse_institutional()[0]
    assert r["foreign_net"] == -68815276510.0 + 200


def test_institutional_identity_mismatch_rejected():
    reset()
    rows = [list(x) for x in BFI_ROWS]
    rows[3][3] = "-1"          # 買賣差額和 買進-賣出 對不上
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(rows), "")):
        assert fetch.twse_institutional() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_institutional_missing_foreign_row_failure():
    reset()
    rows = [x for x in BFI_ROWS if not x[0].startswith("外資及陸資")]
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(rows), "")):
        assert fetch.twse_institutional() is None


def test_institutional_wrong_fields_failure():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(fields=["a", "b"]), "")):
        assert fetch.twse_institutional() is None


def test_institutional_absurd_amount_rejected():
    reset()
    rows = [list(x) for x in BFI_ROWS]
    rows[3] = ["外資及陸資(不含外資自營商)", "9,000,000,000,000", "1,000", "8,999,999,999,000"]
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(rows), "")):
        assert fetch.twse_institutional() is None


def test_institutional_future_date_blocked_and_not_ok():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(date_str=ymd(-5)), "")):
        assert fetch.twse_institutional() is None
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.twse_institutional() is None
    assert fetch.LOG[-1]["status"] == "failure"
    with mock.patch.object(fetch, "fetch_json", return_value=(bfi_payload(stat="no"), "")):
        assert fetch.twse_institutional() is None
    assert fetch.LOG[-1]["status"] == "no_new_data"


MARGIN_FIELDS = ["項目", "買進", "賣出", "現金(券)償還", "前日餘額", "今日餘額"]
MARGIN_ROWS = [
    ["融資(交易單位)", "321,858", "280,384", "3,623", "9,550,908", "9,588,759"],
    ["融券(交易單位)", "21,600", "21,127", "3,565", "193,040", "189,002"],
    ["融資金額(仟元)", "25,149,066", "22,968,994", "568,776", "632,299,552", "633,910,848"],
]


def margin_payload(rows=None, fields=None, date_str=None):
    return {"stat": "OK", "date": date_str or ymd(),
            "tables": [{"title": "信用交易統計", "fields": fields or MARGIN_FIELDS, "data": rows or MARGIN_ROWS},
                       {"title": None, "fields": None, "data": []}]}


def test_margin_real_format_converts_thousand_to_yuan():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(margin_payload(), "")):
        r = fetch.twse_margin()[0]
    assert r["margin_balance"] == 633910848 * 1000      # 仟元 -> 元
    assert r["margin_prev_balance"] == 632299552 * 1000
    assert r["margin_units"] == 9588759 and r["short_units"] == 189002
    assert r["source"] == "TWSE MI_MARGN"


def test_margin_identity_mismatch_rejected():
    reset()
    rows = [list(x) for x in MARGIN_ROWS]
    rows[2][5] = "700,000,000"
    with mock.patch.object(fetch, "fetch_json", return_value=(margin_payload(rows), "")):
        assert fetch.twse_margin() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_margin_missing_row_or_table_failure():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(margin_payload(MARGIN_ROWS[:2]), "")):
        assert fetch.twse_margin() is None
    with mock.patch.object(fetch, "fetch_json", return_value=(margin_payload(fields=["x", "y"]), "")):
        assert fetch.twse_margin() is None


def test_margin_out_of_range_rejected():
    reset()
    rows = [["融資金額(仟元)", "1", "1", "0", "5", "5"]]
    with mock.patch.object(fetch, "fetch_json", return_value=(margin_payload(rows), "")):
        assert fetch.twse_margin() is None


def test_persist_writes_institutional_and_margin_tables():
    reset()
    inst = [{"as_of": D(1), "foreign_net": 1.0, "source": "TWSE BFI82U"}]
    marg = [{"as_of": D(1), "margin_balance": 1.0, "source": "TWSE MI_MARGN"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, inst, marg)
    tables = [c[0][0] for c in up.call_args_list]
    assert tables == ["tw_institutional", "tw_margin"]
    assert all(c[0][2] == "as_of,source" for c in up.call_args_list)


def test_taifex_futures_probe_logs_structure_only():
    reset()
    swagger = {"servers": [{"url": "https://openapi.taifex.com.tw/v1"}],
               "paths": {"/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate":
                         {"get": {"summary": "期貨市場三大法人-區分各期貨契約-依日期"}}}}
    rows = [{"Date": "20261029", "Item": "臺股期貨", "ItemOfTheInstitutionalTraders": "外資及陸資"},
            {"Date": "20261029", "Item": "小型臺指期貨", "ItemOfTheInstitutionalTraders": "投信"}]
    calls = []
    def fake(url, **k):
        calls.append(url)
        if "swagger" in url:
            return swagger, ""
        if "BFI82U" in url or "MI_MARGN" in url:
            return None, "skip"
        return rows, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fake), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_extra()
    assert any("MarketDataOfMajor" in c for c in calls)
    assert any(x["source"] == "probe:taifex_futures_keys" and "臺股期貨" in x["detail"] for x in fetch.LOG)
    up.assert_not_called()


def test_taifex_swagger_url_has_no_v1_prefix():
    assert config.TAIFEX_SWAGGER == "https://openapi.taifex.com.tw/swagger.json"


def test_fetch_json_accepts_utf8_bom():
    class FakeResp:
        def __init__(self, raw): self.raw = raw
        def read(self): return self.raw
        def __enter__(self): return self
        def __exit__(self, *a): return False
    payload = b"\xef\xbb\xbf" + '[{"a": "臺股期貨"}]'.encode("utf-8")   # 帶 BOM 的 JSON
    with mock.patch.object(fetch.urllib.request, "urlopen", return_value=FakeResp(payload)):
        data, err = fetch.fetch_json("https://example.test/x")
    assert err == "" and data == [{"a": "臺股期貨"}]
    double = b"\xef\xbb\xbf\xef\xbb\xbf" + '[1]'.encode("utf-8")                  # 連續兩個 BOM
    with mock.patch.object(fetch.urllib.request, "urlopen", return_value=FakeResp(double)):
        data, err = fetch.fetch_json("https://example.test/z")
    assert data == [1]
    plain = '{"b": 1}'.encode("utf-8")                                      # 不帶 BOM 也要能讀
    with mock.patch.object(fetch.urllib.request, "urlopen", return_value=FakeResp(plain)):
        data, err = fetch.fetch_json("https://example.test/y")
    assert data == {"b": 1}


def test_taifex_futures_failure_logs_raw_diagnostics():
    reset()
    swagger = {"servers": [{"url": "https://openapi.taifex.com.tw/v1"}],
               "paths": {"/X": {"get": {"summary": "期貨市場三大法人-區分各期貨契約-依日期"}}}}
    def fake(url, **k):
        if "swagger" in url:
            return swagger, ""
        return None, "Expecting value: line 1 column 1 (char 0)"
    with mock.patch.object(fetch, "fetch_json", side_effect=fake), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", "<html>blocked</html>".encode(), "")):
        fetch.probe_extra()
    raw = [x for x in fetch.LOG if x["source"] == "probe:taifex_futures_raw"]
    assert raw and "text/html" in raw[0]["detail"] and "blocked" in raw[0]["detail"]
    assert any(x["source"] == "probe:taifex_futures" and x["status"] == "failure" for x in fetch.LOG)


def test_fetch_raw_never_raises():
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=OSError("boom")):
        status, ctype, body, err = fetch.fetch_raw("https://example.test")
    assert status is None and body == b"" and "boom" in err


# ---------------------------------------------------------------- 期交所外資期貨部位
def fut_row(item, lng, sht, net, code="臺股期貨", d=None):
    return {"Date": d or ymd(), "ContractCode": code, "Item": item,
            "OpenInterest(Long)": str(lng), "OpenInterest(Short)": str(sht), "OpenInterest(Net)": str(net),
            "ContractValueofOpenInterest(Net)(Thousands)": "1000"}


# 自營商那列取自真實回傳（5606-4095=1511）；外資列數字為測試用，身份別以「外資」開頭辨識
FUT_ROWS = [fut_row("自營商", 5606, 4095, 1511), fut_row("投信", 100, 50, 50),
            fut_row("外資及陸資", 30000, 52000, -22000),
            fut_row("外資及陸資", 1, 2, -1, code="小型臺指期貨")]


def test_futures_picks_contract_and_all_investors():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(FUT_ROWS, "")):
        rows = fetch.taifex_futures()
    assert {r["investor"] for r in rows} == {"自營商", "投信", "外資及陸資"}   # 不含小型臺指期貨
    f = [r for r in rows if r["investor"].startswith("外資")][0]
    assert f["oi_net"] == -22000 and f["oi_long"] == 30000 and f["contract"] == "臺股期貨"
    assert f["as_of"] == (TODAY - timedelta(days=1)).isoformat()


def test_futures_identity_mismatch_row_rejected():
    reset()
    bad = [fut_row("外資及陸資", 30000, 52000, -1)]          # 多空相減不等於淨額
    with mock.patch.object(fetch, "fetch_json", return_value=(bad, "")):
        assert fetch.taifex_futures() is None
    assert any(x["status"] == "failure" for x in fetch.LOG)


def test_futures_no_foreign_row_is_failure_and_lists_investors():
    reset()
    rows = [fut_row("自營商", 5, 4, 1), fut_row("投信", 2, 1, 1)]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        assert fetch.taifex_futures() is None
    assert "自營商" in fetch.LOG[-1]["detail"]


def test_futures_uses_latest_date_only():
    reset()
    old = fut_row("外資及陸資", 1, 2, -1, d=ymd(5))
    new = fut_row("外資及陸資", 30000, 52000, -22000, d=ymd(1))
    with mock.patch.object(fetch, "fetch_json", return_value=([old, new], "")):
        rows = fetch.taifex_futures()
    assert len(rows) == 1 and rows[0]["oi_net"] == -22000


def test_futures_future_date_and_bad_shapes():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=([fut_row("外資及陸資", 2, 1, 1, d=ymd(-4))], "")):
        assert fetch.taifex_futures() is None
    with mock.patch.object(fetch, "fetch_json", return_value=({"not": "list"}, "")):
        assert fetch.taifex_futures() is None
    with mock.patch.object(fetch, "fetch_json", return_value=([{"x": 1}], "")):
        assert fetch.taifex_futures() is None
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"denied", "HTTP 403")):       # 備用的 CSV 路徑也要替換掉，不能真的連線
        assert fetch.taifex_futures() is None


def test_futures_absurd_net_rejected():
    reset()
    bad = [fut_row("外資及陸資", 900000, 100, 899900)]
    with mock.patch.object(fetch, "fetch_json", return_value=(bad, "")):
        assert fetch.taifex_futures() is None


def test_persist_writes_futures_with_conflict_key():
    reset()
    rows = [{"as_of": D(1), "contract": "臺股期貨", "investor": "外資及陸資", "oi_long": 1, "oi_short": 1,
             "oi_net": 0, "source": "TAIFEX 三大法人-區分各期貨契約"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), \
            mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, None, None, rows)
    assert up.call_args[0][0] == "tw_futures_position" and up.call_args[0][2] == "as_of,contract,investor,source"


# ---------------------------------------------------------------- 美股廣度探測（Nasdaq）
def test_nasdaq_probe_finds_breadth_fields():
    reset()
    body = json.dumps({"data": {"marketInfo": {"advancers": 2105, "decliners": "2,833", "unchanged": 154}, "other": 1}, "status": {"rCode": 200}}).encode()
    seen = []
    def fake(url, timeout=None, headers=None):
        seen.append((url, timeout, headers))
        return 200, "application/json", body, ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=fake), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_nasdaq()
    text = " ".join(x["detail"] for x in fetch.LOG)
    assert "advancers=2105" in text and "decliners=2,833" in text and "unchanged=154" in text
    assert len(seen) == len(config.NASDAQ_PROBE_URLS)
    assert all(t == config.PROBE_TIMEOUT for _, t, _ in seen)               # 短逾時
    assert all("Chrome" in h["User-Agent"] for _, _, h in seen)              # 瀏覽器標頭
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_nasdaq_probe_reports_block_html_and_http_errors():
    reset()
    results = iter([(None, "", b"", "timed out"), (403, "text/html", b"<html>Access Denied</html>", "HTTP 403"),
                    (200, "text/html", b"<html>no json</html>", ""), (500, "application/json", b'{"e": 1}', "HTTP 500")])
    with mock.patch.object(fetch, "fetch_raw", side_effect=lambda *a, **k: next(results)):
        fetch.probe_nasdaq()
    assert [x["status"] for x in fetch.LOG] == ["failure"] * 4
    assert "逾時" in fetch.LOG[0]["detail"] and "不是 JSON" in fetch.LOG[1]["detail"]


def test_nasdaq_probe_no_breadth_fields_says_so():
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", b'{"data": {"marketStatus": "Closed"}}', "")):
        fetch.probe_nasdaq()
    assert all("含漲跌家數字樣的欄位：沒有" in x["detail"] for x in fetch.LOG)


def test_us_probe_job_only_probes():
    reset()
    with mock.patch.object(fetch, "probe_us_pe"), mock.patch.object(fetch, "probe_round16"), mock.patch.object(fetch, "probe_round15"), mock.patch.object(fetch, "probe_round14"), mock.patch.object(fetch, "probe_round13"), mock.patch.object(fetch, "probe_round12"), mock.patch.object(fetch, "probe_round11"), mock.patch.object(fetch, "probe_round10"), mock.patch.object(fetch, "probe_round9"), mock.patch.object(fetch, "probe_round8"), mock.patch.object(fetch, "probe_round7"), mock.patch.object(fetch, "probe_round6"), mock.patch.object(fetch, "probe_round5"), mock.patch.object(fetch, "probe_round4"), mock.patch.object(fetch, "probe_round3"), mock.patch.object(fetch, "probe_earnings") as pn, mock.patch.object(fetch, "fred_latest") as f, \
            mock.patch.object(fetch, "twse_market") as tw, mock.patch.object(fetch, "sb_upsert") as up:
        fetch.run_job("us_probe", dry_run=False)
    pn.assert_called_once(); f.assert_not_called(); tw.assert_not_called(); up.assert_not_called()
    assert "us_probe" in config.JOBS


def test_fetch_raw_accepts_custom_headers():
    captured = {}
    class FakeResp:
        status = 200; headers = {"Content-Type": "application/json"}
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False
    def fake_open(req, timeout=None):
        captured["ua"] = req.get_header("User-agent"); captured["timeout"] = timeout; return FakeResp()
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=fake_open):
        fetch.fetch_raw("https://example.test", timeout=7, headers={"User-Agent": "X-Test"})
    assert captured == {"ua": "X-Test", "timeout": 7}


def test_screener_probe_counts_and_reports_dates():
    reset()
    rows = [{"symbol": "A", "pctchange": "1.50%"}, {"symbol": "B", "pctchange": "-0.20%"}, {"symbol": "C", "pctchange": "UNCH"},
            {"symbol": "D", "pctchange": "0.00%"}, {"symbol": "E", "pctchange": ""}, {"symbol": "F", "pctchange": "2.0%"}]
    body = json.dumps({"data": {"asOf": "Last price as of Oct 29, 2026", "rows": rows}, "status": {"rCode": 200}}).encode()
    seen = []
    with mock.patch.object(fetch, "fetch_raw", side_effect=lambda url, timeout=None, headers=None: (seen.append(timeout) or (200, "application/json", body, ""))), \
            mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_nasdaq_screener()
    assert len(fetch.LOG) == len(config.NASDAQ_SCREENER_URLS) and all(x["status"] == "success" for x in fetch.LOG)
    d = fetch.LOG[0]["detail"]
    assert "上漲 2／下跌 1／持平 2／無法解析 1" in d and "asOf=Last price as of Oct 29, 2026" in d and "僅診斷" in d
    assert all(t == config.SCREENER_TIMEOUT for t in seen)
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_screener_probe_failures_are_reported():
    reset()
    results = iter([(None, "", b"", "timed out"), (403, "text/html", b"<html>denied</html>", "HTTP 403"),
                    (200, "application/json", b'{"data": {"rows": []}}', "")])
    with mock.patch.object(fetch, "fetch_raw", side_effect=lambda *a, **k: next(results)):
        fetch.probe_nasdaq_screener()
    assert [x["status"] for x in fetch.LOG] == ["failure"] * 3
    assert "逾時" in fetch.LOG[0]["detail"] and "不是 JSON" in fetch.LOG[1]["detail"] and "找不到資料列" in fetch.LOG[2]["detail"]


def test_pct_parser():
    assert fetch._pct_to_float("1.23%") == 1.23 and fetch._pct_to_float("-0.5%") == -0.5
    assert fetch._pct_to_float("UNCH") == 0.0 and fetch._pct_to_float("") is None and fetch._pct_to_float("abc") is None


def test_taifex_daily_probe_logs_structure_and_tx_rows():
    reset()
    swagger = {"servers": [{"url": "https://openapi.taifex.com.tw/v1"}],
               "paths": {"/DailyMarketReportFut": {"get": {"summary": "期貨每日交易行情"}},
                         "/DailyMarketReportOpt": {"get": {"summary": "選擇權每日交易行情"}}}}
    rows = [{"Date": "20261029", "Contract": "臺股期貨", "ContractMonth(Week)": "202611", "Last": "45000", "SettlementPrice": "45010", "TradingSession": "一般"},
            {"Date": "20261029", "Contract": "臺股期貨", "ContractMonth(Week)": "202612", "Last": "45100", "SettlementPrice": "45090", "TradingSession": "一般"},
            {"Date": "20261029", "Contract": "小型臺指期貨", "ContractMonth(Week)": "202611", "Last": "45001", "SettlementPrice": "45011", "TradingSession": "盤後"}]
    calls = []
    def fake(url, **k):
        calls.append(url)
        return (swagger, "") if "swagger" in url else (rows, "")
    with mock.patch.object(fetch, "fetch_json", side_effect=fake), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_taifex_daily()
    assert any(c.endswith("/DailyMarketReportFut") for c in calls) and not any(c.endswith("/DailyMarketReportOpt") for c in calls)
    by = {x["source"]: x["detail"] for x in fetch.LOG}
    assert "DailyMarketReportFut" in by["probe:taifex_daily_swagger"] and "DailyMarketReportOpt" not in by["probe:taifex_daily_swagger"]
    assert "ContractMonth(Week)" in by["probe:taifex_daily_rows"] and "共 3 列" in by["probe:taifex_daily_rows"]
    assert "TradingSession=" in by["probe:taifex_daily_values"] and "盤後" in by["probe:taifex_daily_values"]
    assert "臺股期貨相關列數 2" in by["probe:taifex_daily_tx"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_taifex_daily_probe_failures():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "boom")):
        fetch.probe_taifex_daily()
    assert fetch.LOG[-1]["status"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=({"paths": {"/x": {"get": {"summary": "其他"}}}}, "")):
        fetch.probe_taifex_daily()
    assert "無" in fetch.LOG[-1]["detail"]


# ---------------------------------------------------------------- 台指期基差
def fut_daily(month, last, session="一般", d=None, contract="TX", settle="46569"):
    return {"Date": d or ymd(), "Contract": contract, "ContractMonth(Week)": month, "Open": "1", "High": "1", "Low": "1",
            "Last": last, "Change": "0", "%": "0%", "Volume": "1", "SettlementPrice": settle, "OpenInterest": "1",
            "BestBid": "1", "BestAsk": "1", "HistoricalHigh": "1", "HistoricalLow": "1", "TradingHalt": "",
            "TradingSession": session}


def tw_idx(close, days_ago=1):
    return {"as_of": (TODAY - timedelta(days=days_ago)).isoformat(), "close": close}


# 真實回傳的格式：TX 近月 202611 一般時段最後成交價 46577、結算價 46569、盤後另一列
DAILY_ROWS = [fut_daily("202611", "46577"), fut_daily("202611", "47121", session="盤後", settle="NULL"),
              fut_daily("202612", "46700"), fut_daily("202611/202612", "-123"), fut_daily("202611", "999", contract="MTX"),
              fut_daily("202703", "46800")]


def test_basis_uses_near_month_regular_session_and_same_day_index():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(DAILY_ROWS, "")):
        r = fetch.taifex_basis([tw_idx(46368.33)])[0]
    assert r["contract_month"] == "202611" and r["futures_last"] == 46577       # 不用盤後 47121、不用小型、不用價差
    assert abs(r["basis"] - (46577 - 46368.33)) < 0.01 and abs(r["basis_pct"] - 0.45) < 0.01
    assert r["settlement_price"] == 46569 and r["index_close"] == 46368.33
    assert r["as_of"] == (TODAY - timedelta(days=1)).isoformat()


def test_basis_date_mismatch_is_not_computed():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(DAILY_ROWS, "")):
        assert fetch.taifex_basis([tw_idx(46000, days_ago=2)]) is None          # 指數是前一天
        assert fetch.taifex_basis([]) is None and fetch.taifex_basis(None) is None
    assert fetch.LOG[-1]["status"] == "no_new_data" and "日期對不上" in fetch.LOG[-1]["detail"]


def test_basis_no_regular_last_price_is_no_data():
    reset()
    rows = [fut_daily("202611", "-"), fut_daily("202612", "46700")]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        assert fetch.taifex_basis([tw_idx(46368.33)]) is None
    assert fetch.LOG[-1]["status"] == "no_new_data"


def test_basis_absurd_or_bad_inputs_rejected():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=([fut_daily("202611", "90000")], "")):
        assert fetch.taifex_basis([tw_idx(46368.33)]) is None                    # 基差超過 5%
    assert fetch.LOG[-1]["status"] == "failure"
    with mock.patch.object(fetch, "fetch_json", return_value=([fut_daily("202511", "46000")], "")):
        assert fetch.taifex_basis([tw_idx(46000)]) is None                       # 近月早於資料月份
    with mock.patch.object(fetch, "fetch_json", return_value=([{"x": 1}], "")):
        assert fetch.taifex_basis([tw_idx(46000)]) is None
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.taifex_basis([tw_idx(46000)]) is None
    with mock.patch.object(fetch, "fetch_json", return_value=([fut_daily("202611", "46500", d=ymd(-3))], "")):
        assert fetch.taifex_basis([tw_idx(46000)]) is None                       # 未來日期


def test_basis_persist_conflict_key():
    reset()
    rows = [{"as_of": D(1), "contract_month": "202611", "futures_last": 1, "index_close": 1, "basis": 0, "source": "x"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, None, None, None, rows)
    assert up.call_args[0][0] == "tw_futures_basis" and up.call_args[0][2] == "as_of,contract_month,source"


def test_run_job_passes_index_rows_to_basis():
    reset()
    idx = [tw_idx(100.0)]
    with mock.patch.object(fetch, "twse_market", return_value=idx), mock.patch.object(fetch, "twse_breadth", return_value=None), \
            mock.patch.object(fetch, "twse_institutional", return_value=None), mock.patch.object(fetch, "twse_margin", return_value=None), \
            mock.patch.object(fetch, "taifex_futures", return_value=None), mock.patch.object(fetch, "twd_fallback_chain", return_value=None), \
            mock.patch.object(fetch, "load_holdings", return_value=(None, "測試")), \
            mock.patch.object(fetch, "cftc_cot", return_value=None), mock.patch.object(fetch, "cboe_putcall", return_value=None), \
            mock.patch.object(fetch, "taifex_basis", return_value=None) as ba:
        fetch.run_job("tw_close", dry_run=True)
    ba.assert_called_once_with(idx)


# ---------------------------------------------------------------- 外資期貨：JSON 失敗時改讀 CSV
CSV_TEXT = ("\ufeff日期,商品名稱,身份別,多方交易口數,多方交易契約金額(千元),空方交易口數,空方交易契約金額(千元),多空交易口數淨額,"
            "多空交易契約金額淨額(千元),多方未平倉口數,多方未平倉契約金額(千元),空方未平倉口數,空方未平倉契約金額(千元),"
            "多空未平倉口數淨額,多空未平倉契約金額淨額(千元)\n"
            "{d},臺股期貨,自營商,1,1,1,1,0,0,5606,1,4095,1,1511,100\n"
            "{d},臺股期貨,外資及陸資,1,1,1,1,0,0,30000,1,52000,1,-22000,-200\n"
            "{d},小型臺指期貨,外資及陸資,1,1,1,1,0,0,9,1,1,1,8,1\n")


def test_foreign_futures_falls_back_to_csv_when_json_fails():
    reset()
    d = (TODAY - timedelta(days=1)).strftime("%Y/%m/%d")             # CSV 的日期可能帶斜線
    body = CSV_TEXT.replace("{d}", d).encode("utf-8")
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "Expecting value: line 1 column 1 (char 0)")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/octet-stream", body, "")):
        rows = fetch.taifex_futures()
    f = [r for r in rows if r["investor"].startswith("外資")][0]
    assert f["oi_net"] == -22000 and f["oi_long"] == 30000 and f["oi_short"] == 52000
    assert {r["investor"] for r in rows} == {"自營商", "外資及陸資"}                  # 不含小型臺指期貨
    assert f["as_of"] == (TODAY - timedelta(days=1)).isoformat()
    assert any(x["source"] == "taifex:futures_format" for x in fetch.LOG)


def test_csv_fallback_wrong_headers_or_empty_is_failure():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "x")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", "a,b\n1,2\n".encode(), "")):
        assert fetch.taifex_futures() is None
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "x")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"denied", "HTTP 403")):
        assert fetch.taifex_futures() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_csv_fallback_identity_mismatch_rejected():
    reset()
    d = (TODAY - timedelta(days=1)).strftime("%Y%m%d")
    body = CSV_TEXT.replace("{d}", d).replace("-22000", "-1").encode("utf-8")      # 多空相減不等於淨額
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "x")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", body, "")):
        assert fetch.taifex_futures() is None


def test_norm_ymd():
    assert fetch._norm_ymd("2026/10/29") == "20261029" and fetch._norm_ymd("2026-10-29") == "20261029"
    assert fetch._norm_ymd("20261029") == "20261029" and fetch._norm_ymd("abc") is None and fetch._norm_ymd(None) is None


# ---------------------------------------------------------------- 持股現價探測
def sb_fake(accts, pos):
    def f(method, path, body=None, prefer=None):
        return ((accts if "private_accounts" in path else pos), "")
    return f


def test_load_holdings_maps_currency_to_market_and_dedupes():
    accts = [{"user_id": "u", "account_key": "台", "currency": "TWD"}, {"user_id": "u", "account_key": "美", "currency": "usd"},
             {"user_id": "u", "account_key": "其他", "currency": "JPY"}]
    pos = [{"user_id": "u", "account_key": "台", "pos_key": "台積電", "symbol": None, "name": "台積電", "kind": "stock"},
           {"user_id": "u", "account_key": "美", "pos_key": "MUU", "symbol": "MUU", "name": "2倍做多MU ETF", "kind": "leveraged_etf"},
           {"user_id": "u", "account_key": "其他", "pos_key": "X", "symbol": "X", "name": "X", "kind": "stock"},
           {"user_id": "u", "account_key": "台", "pos_key": "台積電", "symbol": None, "name": "台積電", "kind": "stock"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=sb_fake(accts, pos)):
        hold, err = fetch.load_holdings()
    assert err == "" and [(h["market"], h["name"]) for h in hold] == [("TW", "台積電"), ("US", "2倍做多MU ETF")]
    assert all(set(h) == {"market", "symbol", "name", "kind", "pos_key"} for h in hold)      # 不帶股數與金額


def test_load_holdings_without_supabase_or_on_error():
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert fetch.load_holdings()[0] is None
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 401")):
        hold, err = fetch.load_holdings()
    assert hold is None and "401" in err


def test_probe_prices_reports_all_sources_and_matches_names_exactly():
    reset()
    hold = [{"market": "TW", "symbol": None, "name": "台積電", "kind": "stock", "pos_key": "台積電"},
            {"market": "TW", "symbol": None, "name": "金居", "kind": "stock", "pos_key": "金居"},
            {"market": "TW", "symbol": None, "name": "不存在股", "kind": "stock", "pos_key": "不存在股"},
            {"market": "US", "symbol": "MUU", "name": "2倍做多MU ETF", "kind": "leveraged_etf", "pos_key": "MUU"}]
    twse = {"stat": "OK", "date": "20261029", "fields": ["證券代號", "證券名稱", "收盤價"], "data": [["2330", "台積電", "2,370.00"], ["2317", "鴻海", "200"]]}
    sw = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/tpex_mainboard_daily_close_quotes": {"get": {"summary": "上櫃股票收盤行情"}}}}
    tpex = [{"Date": "1151029", "SecuritiesCompanyCode": "8358", "CompanyName": "金居", "Close": "400.00"}]
    def fj(url, **k):
        if "STOCK_DAY_ALL" in url: return twse, ""
        if "swagger" in url: return sw, ""
        return tpex, ""
    nas = json.dumps({"data": {"symbol": "MUU", "marketStatus": "Closed", "primaryData": {"lastSalePrice": "$24.60", "netChange": "-1.00", "percentageChange": "-3.9%",
                      "lastTradeTimestamp": "Oct 29, 2026 4:00 PM ET", "isRealTime": False}, "secondaryData": None}}).encode()
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_json", side_effect=fj), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas, "")), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_prices()
    by = {x["source"]: x for x in fetch.LOG}
    assert "日期20261029" in by["probe:px_twse"]["detail"]
    assert "CompanyName" in by["probe:px_tpex"]["detail"]
    m = by["probe:px_tw_match"]
    assert m["status"] == "failure" and "上市對上 1 檔" in m["detail"] and "上櫃對上 1 檔" in m["detail"] and "不存在股" in m["detail"]
    n = by["probe:px_nasdaq:MUU"]["detail"]
    assert "lastTradeTimestamp" in n and "Oct 29, 2026 4:00 PM ET" in n and "isRealTime" in n
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_prices_failures_are_logged():
    reset()
    with mock.patch.object(fetch, "load_holdings", return_value=(None, "未設定 Supabase")), \
            mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")), \
            mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")):
        fetch.probe_prices()
    sts = {x["source"]: x["status"] for x in fetch.LOG}
    assert sts["probe:px_holdings"] == "failure" and sts["probe:px_twse"] == "failure" and sts["probe:px_tpex_swagger"] == "failure"


def test_probe_prices_nasdaq_tries_other_assetclass_and_reports_non_json():
    reset()
    hold = [{"market": "US", "symbol": "ARM", "name": "Arm", "kind": "stock", "pos_key": "ARM"}]
    ok = json.dumps({"data": {"marketStatus": "Closed", "primaryData": {"lastSalePrice": "$208.50", "lastTradeTimestamp": "Oct 29, 2026 4:00 PM ET", "isRealTime": False}}}).encode()
    calls = []
    def raw(url, timeout=None, headers=None):
        calls.append(url)
        return (200, "application/json", b'{"data": null, "status": {"rCode": 400}}' if "assetclass=stocks" in url else ok, "")
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_json", return_value=(None, "x")), \
            mock.patch.object(fetch, "fetch_raw", side_effect=raw):
        fetch.probe_prices()
    nq = [c for c in calls if "api.nasdaq.com" in c]
    assert len(nq) == 2 and "assetclass=etf" in nq[1]
    assert [x for x in fetch.LOG if x["source"] == "probe:px_nasdaq:ARM"][0]["status"] == "success"


# ---------------------------------------------------------------- 持股現價（正式）
def test_date_parsers():
    assert fetch.roc_to_iso("1151029") == "2026-10-29" and fetch.roc_to_iso("115102") is None and fetch.roc_to_iso("abc") is None
    assert fetch.roc_to_iso("1151345") is None and fetch.roc_to_iso(None) is None
    assert fetch.parse_us_date("Oct 29, 2026") == "2026-10-29" and fetch.parse_us_date("Oct 29, 2026 4:00 PM ET") == "2026-10-29"
    assert fetch.parse_us_date("Sept 3, 2026") is None or fetch.parse_us_date("Sept 3, 2026") == "2026-09-03"
    assert fetch.parse_us_date("Foo 29, 2026") is None and fetch.parse_us_date("") is None and fetch.parse_us_date(None) is None


def roc(days_ago=1):
    d = TODAY - timedelta(days=days_ago)
    return f"{d.year - 1911}{d.month:02d}{d.day:02d}"


def tpex_row(code, name, close="400.00", change="-1.50 ", d=None):
    return {"Date": d or roc(), "SecuritiesCompanyCode": code, "CompanyName": name, "Close": close, "Change": change,
            "Open": "1", "High": "1", "Low": "1"}


HOLD = [{"market": "TW", "symbol": None, "name": "金居", "kind": "stock", "pos_key": "金居"},
        {"market": "TW", "symbol": None, "name": "台積電", "kind": "stock", "pos_key": "台積電"},
        {"market": "TW", "symbol": None, "name": "環球晶", "kind": "stock", "pos_key": "環球晶"}]


def test_tpex_prices_exact_name_dates_and_change():
    reset()
    rows = [tpex_row("8358", "金居", "400.00", "-1.50 "), tpex_row("6488", "環球晶", "743.00", "+3.0"), tpex_row("00411A", "主動統一前沿科技", "10")]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = fetch.tpex_prices(HOLD)
    by = {o["name"]: o for o in out}
    assert set(by) == {"金居", "環球晶"}                                  # 台積電不在上櫃，不得硬配
    g = by["金居"]
    assert g["close"] == 400.0 and g["change"] == -1.5 and g["code"] == "8358" and g["as_of"] == (TODAY - timedelta(days=1)).isoformat()
    assert abs(g["change_pct"] - (-1.5 / 401.5 * 100)) < 0.001 and g["market"] == "TW"
    assert "不在上櫃清單 1 檔" in fetch.LOG[-1]["detail"]


def test_tpex_prices_ambiguous_missing_and_future_rejected():
    reset()
    rows = [tpex_row("8358", "金居"), tpex_row("9999", "金居"), tpex_row("6488", "環球晶", "--"), tpex_row("2330", "台積電", d=roc(-3))]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = fetch.tpex_prices(HOLD)
    assert out == []                                                       # 同名兩筆且皆四碼不唯一、無收盤價、未來日期：全部不用
    d = fetch.LOG[-1]["detail"]
    assert "同名 2 筆" in d and "收盤價無效" in d and fetch.LOG[-1]["status"] == "failure"


def test_tpex_prices_prefers_four_digit_code_among_same_name():
    reset()
    rows = [tpex_row("03001P", "金居", "5"), tpex_row("8358", "金居", "400")]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = fetch.tpex_prices(HOLD[:1])
    assert len(out) == 1 and out[0]["code"] == "8358"


def test_tpex_prices_failures_and_no_tw_holdings():
    reset()
    assert fetch.tpex_prices([{"market": "US", "name": "x", "symbol": "X", "kind": "stock", "pos_key": "X"}]) == []
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.tpex_prices(HOLD) is None
    with mock.patch.object(fetch, "fetch_json", return_value=([{"x": 1}], "")):
        assert fetch.tpex_prices(HOLD) is None
    assert fetch.LOG[-1]["status"] == "failure"


def nas_body(status="Closed", real=False, ts="Oct 29, 2026", price="$139.56", net="-4.98"):
    d = {"data": {"symbol": "ORCL", "marketStatus": status, "primaryData": {"lastSalePrice": price, "netChange": net, "percentageChange": "-3.45%",
         "lastTradeTimestamp": ts, "isRealTime": real}, "secondaryData": None}}
    return json.dumps(d).encode()


US_HOLD = [{"market": "US", "symbol": "ORCL", "name": "甲骨文", "kind": "stock", "pos_key": "ORCL"}]


def test_nasdaq_prices_only_uses_closed_non_realtime():
    reset()
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_body(ts=ts), "")), mock.patch.object(fetch.time, "sleep"):
        out = fetch.nasdaq_prices(US_HOLD)
    assert len(out) == 1 and out[0]["close"] == 139.56 and out[0]["change"] == -4.98 and out[0]["code"] == "ORCL"
    assert out[0]["as_of"] == (TODAY - timedelta(days=1)).isoformat() and out[0]["market"] == "US"
    for status, real in (("After-Hours", False), ("Open", False), ("Pre-Market", False), ("Closed", True)):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_body(status=status, real=real, ts=ts), "")):
            assert fetch.nasdaq_prices(US_HOLD) == []
        assert fetch.LOG[-1]["status"] == "skipped" and "不使用" in fetch.LOG[-1]["detail"], status


def test_nasdaq_prices_records_the_official_name_and_falls_back_to_the_pasted_one():
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    base = json.loads(nas_body(ts=ts))
    def body(**extra):
        b = json.loads(json.dumps(base)); b["data"].update(extra); return json.dumps(b).encode()
    def run(b):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", b, "")), mock.patch.object(fetch.time, "sleep"):
            return fetch.nasdaq_prices(US_HOLD)
    assert run(body(companyName="Oracle Corporation Common Stock"))[0]["name"] == "Oracle Corporation Common Stock", "官方有完整名稱：用官方的（貼上的名稱常被券商畫面截斷）"
    assert run(body(companyName="  GraniteShares 2x Long INTC Daily ETF  "))[0]["name"] == "GraniteShares 2x Long INTC Daily ETF", "前後空白去掉"
    for extra in ({}, {"companyName": ""}, {"companyName": "   "}, {"companyName": None}):
        assert run(body(**extra))[0]["name"] == "甲骨文", extra


def test_nasdaq_prices_bad_inputs_rejected():
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_body(ts="Dec 25, 2999"), "")):
        assert fetch.nasdaq_prices(US_HOLD) == []
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_body(price="N/A"), "")):
        assert fetch.nasdaq_prices(US_HOLD) == []
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")):
        assert fetch.nasdaq_prices(US_HOLD) == []
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>", "")):
        assert fetch.nasdaq_prices(US_HOLD) == []
    assert all(x["status"] == "failure" for x in fetch.LOG)


def test_nasdaq_prices_falls_back_to_other_assetclass():
    reset()
    hold = [{"market": "US", "symbol": "MUU", "name": "x", "kind": "leveraged_etf", "pos_key": "MUU"}]
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    calls = []
    def raw(url, timeout=None, headers=None):
        calls.append(url)
        return 200, "application/json", (b'{"data": null}' if "assetclass=etf" in url else nas_body(ts=ts)), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = fetch.nasdaq_prices(hold)
    assert len(out) == 1 and "assetclass=etf" in calls[0] and "assetclass=stocks" in calls[1]
    assert all("Chrome" in h for h in [fetch.NASDAQ_HEADERS["User-Agent"]])


def test_persist_writes_stock_prices_with_conflict_key():
    reset()
    rows = [{"market": "US", "code": "ORCL", "name": "x", "as_of": D(1), "close": 1.0, "source": "s"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, None, None, None, None, rows)
    assert up.call_args[0][0] == "stock_prices" and up.call_args[0][2] == "market,code,as_of,source"


def test_run_job_collects_tpex_and_nasdaq_prices_when_holdings_available():
    reset()
    hold = HOLD + US_HOLD
    with mock.patch.object(fetch, "twse_market", return_value=None), mock.patch.object(fetch, "twse_breadth", return_value=None), \
            mock.patch.object(fetch, "twse_institutional", return_value=None), mock.patch.object(fetch, "twse_margin", return_value=None), \
            mock.patch.object(fetch, "taifex_futures", return_value=None), mock.patch.object(fetch, "taifex_basis", return_value=None), \
            mock.patch.object(fetch, "cftc_cot", return_value=None), mock.patch.object(fetch, "cboe_putcall", return_value=None), \
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None), mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), \
            mock.patch.object(fetch, "nasdaq_earnings", return_value=[]), mock.patch.object(fetch, "tpex_prices", return_value=[{"k": 1, "name": "金居"}]) as tp, \
            mock.patch.object(fetch, "twse_prices", return_value=[{"k": 3}]) as tw, mock.patch.object(fetch, "nasdaq_prices", return_value=[{"k": 2}]) as nq:
        res = fetch.run_job("tw_close", dry_run=True)
    tp.assert_called_once_with(hold); nq.assert_called_once_with(hold)
    tw.assert_called_once_with(hold, {"金居"})                           # 上櫃已有的名稱不重複查上市
    assert res["prices"] == [{"k": 1, "name": "金居"}, {"k": 3}, {"k": 2}]


def test_load_us_watch_reads_valid_unique_uppercase_symbols_and_handles_missing_table_and_errors():
    got = [{"symbol": "orcl"}, {"symbol": " NVDA "}, {"symbol": "ORCL"}, {"symbol": "BRK.B"}, {"symbol": "2330"}, {"symbol": "ABCDEFG"}, {"symbol": ""}, {"symbol": None}, {"x": 1}, "壞的"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(got, None)) as sq:
        syms = REAL["load_us_watch"]()
    assert syms == ["BRK.B", "NVDA", "ORCL"], "大寫、去空白、去重、只留格式正確的: " + str(syms)
    assert sq.call_args[0][0] == "GET" and "private_watchlist_us" in sq.call_args[0][1], "只讀"
    lg = [x for x in fetch.LOG if x["source"] == "watch_us"]; assert len(lg) == 1 and lg[0]["status"] == "success" and "3 檔：BRK.B、NVDA、ORCL" in lg[0]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], None)):
        assert REAL["load_us_watch"]() == []
    assert [(x["status"], "清單是空的" in x["detail"]) for x in fetch.LOG if x["source"] == "watch_us"] == [("no_new_data", True)]
    for err in ("PGRST205 relation does not exist", "HTTP 404"):
        reset()
        with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, err)):
            assert REAL["load_us_watch"]() == []
        lg = [x for x in fetch.LOG if x["source"] == "watch_us"]; assert lg[0]["status"] == "no_new_data" and "new_tables_v67.sql" in lg[0]["detail"], "資料表還不存在：不算壞掉"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 500 boom")):
        assert REAL["load_us_watch"]() == []
    lg = [x for x in fetch.LOG if x["source"] == "watch_us"]; assert lg[0]["status"] == "failure" and "HTTP 500" in lg[0]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "sb_request") as sq:
        assert REAL["load_us_watch"]() == [] and sq.call_count == 0 and not [x for x in fetch.LOG if x["source"] == "watch_us"], "沒設 Supabase：不查、不記"


def test_with_us_watch_appends_only_symbols_not_already_held_and_leaves_holdings_untouched():
    hold = [{"market": "TW", "symbol": None, "name": "台積電", "kind": "stock", "pos_key": "台積電"}, {"market": "US", "symbol": "ORCL", "name": "甲骨文", "kind": "stock", "pos_key": "ORCL"},
            {"market": "US", "symbol": None, "name": "ARM", "kind": "stock", "pos_key": "arm"}]
    out = fetch.with_us_watch(hold, ["AMD", "orcl", "ARM", "NVDA", "nvda", "", None])
    assert out[:3] == hold and out is not hold and len(hold) == 3, "持股清單本身不動"
    assert [x["symbol"] for x in out[3:]] == ["AMD", "NVDA"], "已經是持股的（不分大小寫，也看 pos_key）、清單裡重複的、空的都不加: " + str(out[3:])
    assert out[3] == {"market": "US", "symbol": "AMD", "name": "AMD", "kind": "stock", "pos_key": "AMD"}
    assert fetch.with_us_watch(None, ["AMD"]) == [{"market": "US", "symbol": "AMD", "name": "AMD", "kind": "stock", "pos_key": "AMD"}] and fetch.with_us_watch(hold, None) == hold and fetch.with_us_watch(hold, []) == hold and fetch.with_us_watch(None, None) == []
    assert not [x for x in fetch.with_us_watch([{"market": "TW", "symbol": "AMD", "name": "x", "kind": "stock", "pos_key": "AMD"}], ["AMD"]) if x["market"] == "TW" and x is None], "台股持股不影響美股候選"
    assert len([x for x in fetch.with_us_watch([{"market": "TW", "symbol": "AMD", "name": "x", "kind": "stock", "pos_key": "AMD"}], ["AMD"]) if x["market"] == "US"]) == 1


def test_run_job_tw_close_feeds_us_watch_symbols_to_nasdaq_prices_earnings_and_sector_without_changing_tw_steps():
    reset()
    hold = HOLD + US_HOLD
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    seen = {}
    def cap(name, ret):
        def f(h, *a, **k):
            seen[name] = h
            return ret
        return f
    got = []
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "load_us_watch", return_value=["AMD", "ORCL"]),
              mock.patch.object(fetch, "tpex_prices", side_effect=cap("tpex", [])), mock.patch.object(fetch, "twse_prices", side_effect=cap("twse", [])), mock.patch.object(fetch, "nasdaq_prices", side_effect=cap("px", [])),
              mock.patch.object(fetch, "nasdaq_earnings", side_effect=cap("earn", [])), mock.patch.object(fetch, "us_sector_lookup", side_effect=cap("sec", [])), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(t_)),
              mock.patch.object(fetch, "fred_latest", return_value=None)]
    for p_ in patches: p_.start()
    try:
        fetch.run_job("tw_close", dry_run=False)
    finally:
        for p_ in patches: p_.stop()
    want = hold + [{"market": "US", "symbol": "AMD", "name": "AMD", "kind": "stock", "pos_key": "AMD"}]
    assert seen["px"] == want and seen["earn"] == want and seen["sec"] == want, "美股價格、財報日、產業都包含候選股（ORCL 已持有，不重複）: " + str({k: len(v) for k, v in seen.items()})
    assert seen["tpex"] == hold and seen["twse"] == hold, "台股價格只看持股，不受影響"
    reset()
    with mock.patch.object(fetch, "twse_market", return_value=None), mock.patch.object(fetch, "twse_breadth", return_value=None), mock.patch.object(fetch, "twse_institutional", return_value=None), mock.patch.object(fetch, "twse_margin", return_value=None), \
            mock.patch.object(fetch, "taifex_futures", return_value=None), mock.patch.object(fetch, "taifex_basis", return_value=None), mock.patch.object(fetch, "cftc_cot", return_value=None), mock.patch.object(fetch, "cboe_putcall", return_value=None), \
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None), mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "load_us_watch", return_value=[]), mock.patch.object(fetch, "nasdaq_earnings", return_value=[]) as ne, \
            mock.patch.object(fetch, "tpex_prices", return_value=[]), mock.patch.object(fetch, "twse_prices", return_value=[]), mock.patch.object(fetch, "nasdaq_prices", return_value=[]) as npx:
        fetch.run_job("tw_close", dry_run=True)
    npx.assert_called_once_with(hold); ne.assert_called_once_with(hold)


def test_probe_twse_failure_logs_raw_response_and_openapi_candidates():
    reset()
    sw = {"paths": {"/exchangeReport/STOCK_DAY_AVG_ALL": {"get": {"summary": "上市個股日收盤價及月平均價"}},
                    "/exchangeReport/MI_INDEX": {"get": {"summary": "每日收盤行情-大盤統計資訊"}}}}
    avg = [{"Date": "1151029", "Code": "2330", "Name": "台積電", "ClosePrice": "2370.00", "MonthlyAveragePrice": "2400"}]
    def fj(url, **k):
        if "STOCK_DAY_ALL?response" in url: return None, "Expecting value"
        if "twse.com.tw/v1/swagger" in url or url.endswith("swagger.json") and "openapi.twse" in url: return sw, ""
        if "tpex" in url: return None, "x"
        if url.endswith("STOCK_DAY_AVG_ALL"): return avg, ""
        return None, "x"
    with mock.patch.object(fetch, "load_holdings", return_value=([], "")), mock.patch.object(fetch, "fetch_json", side_effect=fj), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", "<html>no</html>".encode(), "")):
        fetch.probe_prices()
    by = {x["source"]: x["detail"] for x in fetch.LOG}
    assert "原始回應 HTTP 200" in by["probe:px_twse"] and "text/html" in by["probe:px_twse"]
    assert "STOCK_DAY_AVG_ALL" in by["probe:px_twse_openapi"] and "MI_INDEX" not in by["probe:px_twse_openapi"]
    assert "Date" in by["probe:px_twse_oa:STOCK_DAY_AVG_ALL"] and "ClosePrice" in by["probe:px_twse_oa:STOCK_DAY_AVG_ALL"]


# ---------------------------------------------------------------- 上市個股收盤價（證交所 OpenAPI，格式已驗證）
def avg_row(code, name, close="2370.00", d=None):
    return {"Date": d or roc(), "Code": code, "Name": name, "ClosingPrice": close, "MonthlyAveragePrice": "1"}


def test_twse_prices_exact_name_date_and_no_change_field():
    reset()
    rows = [avg_row("2330", "台積電", "2370.00"), avg_row("2454", "聯發科", "3855.00"), avg_row("00400A", "主動國泰動能高息", "14.36")]
    hold = [{"market": "TW", "symbol": None, "name": "台積電", "kind": "stock", "pos_key": "台積電"},
            {"market": "TW", "symbol": None, "name": "聯發科", "kind": "stock", "pos_key": "聯發科"},
            {"market": "TW", "symbol": None, "name": "金居", "kind": "stock", "pos_key": "金居"}]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = fetch.twse_prices(hold, exclude_names={"金居"})
    by = {o["name"]: o for o in out}
    assert set(by) == {"台積電", "聯發科"} and by["台積電"]["close"] == 2370.0 and by["台積電"]["code"] == "2330"
    assert by["台積電"]["change"] is None and by["台積電"]["change_pct"] is None            # 這個來源沒有漲跌欄位，不編
    assert by["台積電"]["as_of"] == (TODAY - timedelta(days=1)).isoformat()
    assert "對不上 0 檔" in fetch.LOG[-1]["detail"]                                          # 金居已排除，不算對不上


def test_twse_prices_ambiguous_invalid_and_failures():
    reset()
    hold = [{"market": "TW", "symbol": None, "name": n, "kind": "stock", "pos_key": n} for n in ("甲", "乙", "丙", "丁")]
    rows = [avg_row("1111", "甲"), avg_row("2222", "甲"), avg_row("3333", "乙", "--"), avg_row("4444", "丙", d=roc(-3)), avg_row("5555", "戊")]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = fetch.twse_prices(hold)
    assert out == []
    d = fetch.LOG[-1]["detail"]
    assert "同名 2 筆" in d and "收盤價無效" in d and "對不上 1 檔" in d and fetch.LOG[-1]["status"] == "failure"
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403")):
        assert fetch.twse_prices(hold) is None
    with mock.patch.object(fetch, "fetch_json", return_value=([{"x": 1}], "")):
        assert fetch.twse_prices(hold) is None
    assert fetch.twse_prices([{"market": "US", "name": "x", "symbol": "X", "kind": "stock", "pos_key": "X"}]) == []


# ---------------------------------------------------------------- 歷史資料探測
def test_probe_history_logs_keystats_and_both_exchanges():
    reset()
    hold = [{"market": "US", "symbol": "ORCL", "name": "甲骨文", "kind": "stock", "pos_key": "ORCL"}]
    nas = json.dumps({"data": {"keyStats": {"FiftTwoWeekHighLow": {"label": "52 Week High/Low", "value": "$345.72/$118.65"}}}}).encode()
    twse = {"stat": "OK", "date": "20251030", "tables": [{"title": "每日收盤行情", "fields": ["證券代號", "收盤價"], "data": [["2330", "1,500.00"], ["2317", "200.00"]]}, {"title": "小表", "fields": [], "data": []}]}
    raws = []
    def raw(url, timeout=None, headers=None):
        raws.append(url)
        if "api.nasdaq.com" in url: return 200, "application/json", nas, ""
        if "dailyQuotes" in url: return 200, "application/json", json.dumps({"tables": [{"title": "x", "data": [["8358", "400"]]}]}).encode(), ""
        if "stk_quote_result" in url: return 200, "text/html", b"<html>nope</html>", ""
        return None, "", b"", "timed out"
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_json", return_value=(twse, "")), \
            mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_history()
    by = {x["source"]: x for x in fetch.LOG}
    assert "52 Week High/Low" in by["probe:hist_nasdaq:ORCL"]["detail"] and "$345.72/$118.65" in by["probe:hist_nasdaq:ORCL"]["detail"]
    assert "每日收盤行情" in by["probe:hist_twse"]["detail"] and "小表" not in by["probe:hist_twse"]["detail"]      # 空表格不列出
    assert "前 2 列" in by["probe:hist_twse_rows"]["detail"] and "2330" in by["probe:hist_twse_rows"]["detail"]
    assert by["probe:hist_tpex:www_dailyQuotes"]["status"] == "success"
    assert by["probe:hist_tpex:web_roc"]["status"] == "failure" and "不是 JSON" in by["probe:hist_tpex:web_roc"]["detail"]
    assert by["probe:hist_tpex:www_otc"]["status"] == "failure" and "連線失敗" in by["probe:hist_tpex:www_otc"]["detail"]
    assert any("stk_quote_result" in u and "d=" in u for u in raws) and any("dailyQuotes" in u and "date=" in u for u in raws)   # 櫃買的查詢都帶日期
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_history_picks_weekday_a_year_back_and_handles_failures():
    reset()
    urls = []
    def fj(url, **k):
        urls.append(url); return None, "x"
    with mock.patch.object(fetch, "load_holdings", return_value=(None, "未設定")), mock.patch.object(fetch, "fetch_json", side_effect=fj), \
            mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", b"a,b", "")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_history()
    ymd = urls[0].split("date=")[1][:8]
    d = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
    assert d.weekday() < 5 and 360 <= (TODAY - d).days <= 368
    assert {x["source"]: x["status"] for x in fetch.LOG}["probe:hist_twse"] == "failure"


# ---------------------------------------------------------------- 台股歷史回補與 52 週
TWSE_FIELDS = ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差"]


def twse_hist_payload(d, rows, stat="OK", date_str=None):
    return {"stat": stat, "date": date_str or d.strftime("%Y%m%d"),
            "tables": [{"title": "價格指數", "fields": ["指數"], "data": [["x"]]},
                       {"title": f"{d.year - 1911}年{d.month}月{d.day}日 每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))", "fields": TWSE_FIELDS, "data": rows}]}


def trow(code, high, low, close):
    return [code, "名", "1", "1", "1", "1", high, low, close, "", "0"]


def test_hist_twse_day_parses_filters_and_validates():
    d = TODAY - timedelta(days=30)
    rows = [trow("2330", "1,520.00", "1,490.00", "1,500.00"), trow("0050", "64.8", "64.0", "64.4"), trow("2317", "--", "--", "--"),
            trow("1101", "30", "31", "30.5")]                                     # 0050 非四碼、2317 無成交、1101 高低顛倒
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_hist_payload(d, rows), "")):
        out, st = fetch.hist_twse_day(d)
    assert st == "ok" and [r["code"] for r in out] == ["2330"]
    r = out[0]
    assert (r["close"], r["high"], r["low"], r["market"], r["as_of"]) == (1500.0, 1520.0, 1490.0, "TWSE", d.isoformat())


def test_hist_twse_day_holiday_mismatch_and_errors():
    d = TODAY - timedelta(days=30)
    with mock.patch.object(fetch, "fetch_json", return_value=({"stat": "很抱歉，沒有符合條件的資料!"}, "")):
        assert fetch.hist_twse_day(d) == ([], "holiday")
    with mock.patch.object(fetch, "fetch_json", return_value=(twse_hist_payload(d, [], date_str="20200101"), "")):
        assert fetch.hist_twse_day(d)[1].startswith("error:日期對不上")
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        assert fetch.hist_twse_day(d)[1].startswith("error:")
    bad = twse_hist_payload(d, []); bad["tables"][1]["fields"] = ["a"]
    with mock.patch.object(fetch, "fetch_json", return_value=(bad, "")):
        assert fetch.hist_twse_day(d)[1].startswith("error:欄位不符")
    nt = twse_hist_payload(d, []); nt["tables"] = nt["tables"][:1]
    with mock.patch.object(fetch, "fetch_json", return_value=(nt, "")):
        assert "找不到" in fetch.hist_twse_day(d)[1]


def tpex_hist_payload(d, rows, stat="ok", fields=None, date_str=None):
    return {"stat": stat, "date": d.strftime("%Y%m%d"),
            "tables": [{"title": "上櫃股票每日收盤行情(不含定價)", "date": date_str or f"{d.year - 1911}/{d.month:02d}/{d.day:02d}",
                        "fields": fields or ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低"], "data": rows}]}


def test_hist_tpex_day_parses_by_field_names():
    d = TODAY - timedelta(days=30)
    rows = [["8358", "金居", "400.00", "-1.5", "401", "405", "398"], ["6488", "環球晶", "743.00", "3", "740", "750", "735"], ["00679B", "ETF", "30", "0", "30", "30", "30"]]
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, rows), "")):
        out, st = fetch.hist_tpex_day(d)
    assert st == "ok" and {r["code"] for r in out} == {"8358", "6488"} and all(r["market"] == "TPEX" for r in out)
    g = [r for r in out if r["code"] == "8358"][0]
    assert (g["close"], g["high"], g["low"]) == (400.0, 405.0, 398.0)


def test_hist_tpex_day_holiday_mismatch_and_field_errors():
    d = TODAY - timedelta(days=30)
    with mock.patch.object(fetch, "fetch_json", return_value=({"stat": "error"}, "")):
        assert fetch.hist_tpex_day(d) == ([], "holiday")
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, []), "")):
        assert fetch.hist_tpex_day(d) == ([], "holiday")                     # 沒有資料列
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, [["1", "a", "1", "1", "1", "1", "1"]], date_str="100/01/03"), "")):
        assert "日期對不上" in fetch.hist_tpex_day(d)[1]
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, [["1", "a"]], fields=["代號", "名稱"]), "")):
        assert fetch.hist_tpex_day(d)[1].startswith("error:欄位不符")
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, [["1", "a", "1", "1", "1", "1", "1"]], date_str="bad"), "")):
        assert "格式不符" in fetch.hist_tpex_day(d)[1]


def test_hist_tpex_day_tolerates_spaces_in_field_names():
    d = TODAY - timedelta(days=30)
    fields = ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  ", " 成交金額(元)", " 成交筆數 "]      # 真實回傳的樣子
    rows = [["8358", "金居", "400.00", "-1.5", "401.00", "405.00", "398.00", "1", "1", "1"]]
    with mock.patch.object(fetch, "fetch_json", return_value=(tpex_hist_payload(d, rows, fields=fields), "")):
        out, st = fetch.hist_tpex_day(d)
    assert st == "ok" and out[0]["close"] == 400.0 and out[0]["high"] == 405.0 and out[0]["low"] == 398.0


def weekdays_back(n):
    out, d = [], TODAY
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return out


def run_backfill(done_rows=(), twse=None, tpex=None, **kw):
    calls, ups = [], []
    def tw(d): calls.append(("TWSE", d)); return (twse or (lambda x: ([{"market": "TWSE", "code": "2330", "as_of": x.isoformat(), "close": 1, "high": 1, "low": 1}], "ok")))(d)
    def tp(d): calls.append(("TPEX", d)); return (tpex or (lambda x: ([{"market": "TPEX", "code": "8358", "as_of": x.isoformat(), "close": 1, "high": 1, "low": 1}], "ok")))(d)
    def up(table, rows, key):
        ups.append((table, rows, key)); return True, "ok"
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(list(done_rows), "")), \
            mock.patch.object(fetch, "hist_twse_day", side_effect=tw), mock.patch.object(fetch, "hist_tpex_day", side_effect=tp), \
            mock.patch.object(fetch, "sb_upsert", side_effect=up), mock.patch.object(fetch.time, "sleep") as sl:
        res = fetch.tw_backfill(**kw)
    return res, calls, ups, sl


def test_backfill_newest_first_limited_and_records_progress():
    reset()
    res, calls, ups, sl = run_backfill(max_days=2)
    wd = weekdays_back(2)
    assert [c[1] for c in calls] == [wd[0], wd[0], wd[1], wd[1]] and {c[0] for c in calls} == {"TWSE", "TPEX"}
    assert res["days"] == 2 and sl.call_count == 4
    hist = [u for u in ups if u[0] == "tw_hist"]; days = [u for u in ups if u[0] == "tw_hist_days"]
    assert len(hist) == 4 and len(days) == 4 and hist[0][2] == "market,code,as_of" and days[0][2] == "market,as_of"
    assert days[0][1][0]["status"] == "ok" and days[0][1][0]["rows_count"] == 1


def test_backfill_skips_done_dates_per_market():
    reset()
    wd = weekdays_back(2)
    done = [{"market": "TWSE", "as_of": wd[0].isoformat()}, {"market": "TPEX", "as_of": wd[0].isoformat()}, {"market": "TWSE", "as_of": wd[1].isoformat()}]
    res, calls, ups, _ = run_backfill(done_rows=done, max_days=2)
    assert [(c[0], c[1]) for c in calls] == [("TPEX", wd[1]), ("TWSE", weekdays_back(3)[2]), ("TPEX", weekdays_back(3)[2])]


def test_backfill_holiday_recorded_only_when_old_enough():
    reset()
    holiday = lambda x: ([], "holiday")
    res, calls, ups, _ = run_backfill(twse=holiday, tpex=holiday, max_days=1)
    days = [u for u in ups if u[0] == "tw_hist_days"]
    d0 = weekdays_back(1)[0]
    if (TODAY - d0).days <= 2:
        assert days == [] and any(x["status"] == "no_new_data" for x in fetch.LOG)       # 太新：不標休市
    old = TODAY - timedelta(days=20)
    while old.weekday() >= 5:
        old -= timedelta(days=1)
    reset()
    res, calls, ups, _ = run_backfill(twse=holiday, tpex=holiday, max_days=40, window_days=21)
    olds = [u[1][0] for u in ups if u[0] == "tw_hist_days" and date.fromisoformat(u[1][0]["as_of"]) <= TODAY - timedelta(days=3)]
    assert olds and all(o["status"] == "holiday" and o["rows_count"] == 0 for o in olds)
    assert not any(u[0] == "tw_hist" for u in ups)                                        # 休市沒有行情列可寫


def test_backfill_stops_on_error_and_never_hammers():
    reset()
    boom = lambda x: (None, "error:抓取失敗 HTTP 403")
    res, calls, ups, _ = run_backfill(twse=boom, max_days=5)
    assert len(calls) == 1 and res.get("stopped") and ups == []
    assert fetch.LOG[-1]["status"] == "failure" and "已停止本次回補" in fetch.LOG[-1]["detail"]


def test_backfill_dry_run_and_no_supabase_and_progress_read_failure():
    reset()
    res, calls, ups, _ = run_backfill(dry_run=True, max_days=40)
    assert ups == [] and res["days"] == 1 and len(calls) == 2                             # dry-run 只試一天、不寫入
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        fetch.tw_backfill()
    assert fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 404")):
        assert fetch.tw_backfill() is None
    assert "建立 tw_hist" in fetch.LOG[-1]["detail"]


def test_backfill_write_failure_stops_without_marking_done():
    reset()
    ups = []
    def up(table, rows, key):
        ups.append(table); return (False, "boom") if table == "tw_hist" else (True, "ok")
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")), \
            mock.patch.object(fetch, "hist_twse_day", return_value=([{"market": "TWSE", "code": "2330", "as_of": "x", "close": 1, "high": 1, "low": 1}], "ok")), \
            mock.patch.object(fetch, "hist_tpex_day", return_value=([], "ok")), mock.patch.object(fetch, "sb_upsert", side_effect=up), mock.patch.object(fetch.time, "sleep"):
        assert fetch.tw_backfill(max_days=3) is None
    assert ups == ["tw_hist"]                                                                # 行情沒寫成功就不記進度


def test_run_job_daily_append_only_when_not_dry_run():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist")]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "tw_backfill") as bf:
            fetch.run_job("tw_close", dry_run=True); bf.assert_not_called()
            fetch.run_job("tw_close", dry_run=False); bf.assert_called_once_with(dry_run=False, max_days=config.HIST_DAILY_DAYS, window_days=7)
        with mock.patch.object(fetch, "tw_backfill") as bf:
            fetch.run_job("tw_backfill", dry_run=False); bf.assert_called_once_with(dry_run=False)
    finally:
        for p_ in patches: p_.stop()


def test_parse_range52_and_nasdaq_keystats():
    assert fetch.parse_range52("114.50 - 281.62") == (114.5, 281.62) and fetch.parse_range52("$1,114.50 - $2,281.62") == (1114.5, 2281.62)
    assert fetch.parse_range52("NA") == (None, None) and fetch.parse_range52("300 - 200") == (None, None) and fetch.parse_range52(None) == (None, None)
    reset()
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    def body(close, stats):
        d = {"data": {"marketStatus": "Closed", "primaryData": {"lastSalePrice": f"${close}", "netChange": "-1", "lastTradeTimestamp": ts, "isRealTime": False}, "keyStats": stats}}
        return json.dumps(d).encode()
    stats = {"fiftyTwoWeekHighLow": {"label": "52 Week Range:", "value": "114.50 - 281.62"}}
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body(139.56, stats), "")), mock.patch.object(fetch.time, "sleep"):
        r = fetch.nasdaq_prices(US_HOLD)[0]
    assert r["high_52w"] == 281.62 and r["low_52w"] == 114.5 and "keyStats" in r["range_source"]
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body(500.0, stats), "")), mock.patch.object(fetch.time, "sleep"):
        r = fetch.nasdaq_prices(US_HOLD)[0]
    assert r["high_52w"] is None and "不採用" in fetch.LOG[-1]["detail"]                      # 收盤價跑出區間 → 區間不用
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body(26.98, None), "")), mock.patch.object(fetch.time, "sleep"):
        r = fetch.nasdaq_prices(US_HOLD)[0]
    assert r["high_52w"] is None and r["range_source"] is None and set(r) >= {"high_52w", "low_52w", "range_source"}      # ETF 沒有 → 空白但欄位齊全


def hist_rows_for(code, n, span_days, hi=120.0, lo=80.0):
    start = TODAY - timedelta(days=span_days)
    step = span_days / max(n - 1, 1)
    out = []
    for i in range(n):
        d = start + timedelta(days=int(i * step))
        out.append({"as_of": d.isoformat(), "high": hi if i == 5 else 100.0, "low": lo if i == 7 else 95.0})
    return out


def test_tw_ranges_needs_enough_coverage():
    reset()
    pr = [{"market": "TW", "code": "2330", "close": 100.0, "high_52w": None, "low_52w": None, "range_source": None}]
    with mock.patch.object(fetch, "sb_request", return_value=(hist_rows_for("2330", 240, 360), "")):
        out = fetch.tw_ranges([dict(pr[0])])
    assert out[0]["high_52w"] == 120.0 and out[0]["low_52w"] == 80.0 and "240 個交易日" in out[0]["range_source"]
    reset()
    with mock.patch.object(fetch, "sb_request", return_value=(hist_rows_for("2330", 60, 90), "")):
        out = fetch.tw_ranges([dict(pr[0])])
    assert out[0]["high_52w"] is None and "不足以算 52 週" in fetch.LOG[-1]["detail"]          # 歷史不夠就不算
    reset()
    with mock.patch.object(fetch, "sb_request", return_value=(hist_rows_for("2330", 240, 360), "")):
        out = fetch.tw_ranges([dict(pr[0], close=999.0)])
    assert out[0]["high_52w"] is None and fetch.LOG[-1]["status"] == "failure"                # 收盤價不在區間 → 不採用
    with mock.patch.object(fetch, "sb_request", return_value=([], "")):
        assert fetch.tw_ranges([dict(pr[0])])[0]["high_52w"] is None
    us = [{"market": "US", "code": "ORCL", "close": 1.0, "high_52w": 2.0}]
    with mock.patch.object(fetch, "sb_request") as req:
        assert fetch.tw_ranges(us) == us; req.assert_not_called()                             # 美股不查台股歷史


def test_probe_hist2_logs_etf_history_and_tpex_fields():
    reset()
    hold = [{"market": "US", "symbol": "MUU", "name": "x", "kind": "leveraged_etf", "pos_key": "MUU"}]
    nas = json.dumps({"data": {"tradesTable": {"rows": [{"date": "10/29/2026", "close": "$26.98", "high": "$28", "low": "$26"}, {"date": "10/30/2025", "close": "$9", "high": "$9", "low": "$8"}]}}}).encode()
    tp = json.dumps(tpex_hist_payload(TODAY - timedelta(days=7), [["8358", "金居", "400", "-1", "401", "405", "398"]])).encode()
    def raw(url, timeout=None, headers=None):
        return (200, "application/json", nas if "api.nasdaq.com" in url else tp, "")
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_hist2()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:hist2_nasdaq:MUU"]["status"] == "success" and "2 列" in by["probe:hist2_nasdaq:MUU"]["detail"]
    assert "收盤" in by["probe:hist2_tpex_fields"]["detail"] and "最高" in by["probe:hist2_tpex_fields"]["detail"]
    up.assert_not_called()


# ---------------------------------------------------------------- 台股廣度指標（呼叫資料庫函式）
def days_rows(spec):
    """spec: {日期: [市場...]}"""
    return [{"market": m, "as_of": d} for d, ms in spec.items() for m in ms]


def run_breadth(days, have=(), rpc_ok=True, **kw):
    rpcs = []
    def req(method, path, body=None, prefer=None):
        if path.startswith("tw_hist_days"): return days, ""
        if path.startswith("tw_breadth_ma"): return [{"as_of": h} for h in have], ""
        if path.startswith("rpc/compute_tw_breadth"):
            rpcs.append(body["d"]); return (1, "") if rpc_ok else (None, "HTTP 404")
        raise AssertionError(path)
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req):
        res = fetch.tw_breadth_update(**kw)
    return res, rpcs


def test_breadth_update_latest_only_while_history_short():
    reset()
    d = [(TODAY - timedelta(days=i)).isoformat() for i in range(0, 100)]
    spec = {x: ["TWSE", "TPEX"] for x in d}
    spec[d[0]] = ["TWSE"]                                                 # 最新一天只有上市完成 → 不算
    res, rpcs = run_breadth(days_rows(spec))
    assert rpcs == [d[1]] and res == 1                                    # 涵蓋不到 330 天：只算最新完成日


def test_breadth_update_backfills_missing_recent_days_when_history_full():
    reset()
    d = [(TODAY - timedelta(days=i)).isoformat() for i in range(1, 400, 3)]
    spec = {x: ["TWSE", "TPEX"] for x in d}
    have = [d[2]]
    res, rpcs = run_breadth(days_rows(spec), have=have, max_days=4)
    assert rpcs[0] == d[0] and d[2] not in rpcs and len(rpcs) == 5        # 最新 + 4 個缺的，已算過的跳過
    assert all((TODAY - date.fromisoformat(x)).days <= 60 for x in rpcs)  # 只補最近 60 天


def test_breadth_update_dry_run_failures_and_missing_pieces():
    reset()
    spec = {TODAY.isoformat(): ["TWSE", "TPEX"]}
    res, rpcs = run_breadth(days_rows(spec), dry_run=True)
    assert rpcs == [] and fetch.LOG[-1]["status"] == "skipped"
    reset()
    res, rpcs = run_breadth(days_rows(spec), rpc_ok=False)
    assert res is None and fetch.LOG[-1]["status"] == "failure" and "new_tables_breadth.sql" in fetch.LOG[-1]["detail"]
    reset()
    res, rpcs = run_breadth(days_rows({TODAY.isoformat(): ["TWSE"]}))
    assert res is None and rpcs == [] and fetch.LOG[-1]["status"] == "no_new_data"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert fetch.tw_breadth_update() is None
    assert fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 404")):
        assert fetch.tw_breadth_update() is None
    assert fetch.LOG[-1]["status"] == "failure"


def test_run_job_breadth_update_wiring():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist")]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "tw_backfill"), mock.patch.object(fetch, "tw_breadth_update") as bu:
            fetch.run_job("tw_close", dry_run=True); bu.assert_not_called()
            fetch.run_job("tw_close", dry_run=False); bu.assert_called_once_with(dry_run=False, max_days=10)
        with mock.patch.object(fetch, "tw_backfill"), mock.patch.object(fetch, "tw_breadth_update") as bu:
            fetch.run_job("tw_backfill", dry_run=True); bu.assert_called_once_with(dry_run=True, max_days=40)
    finally:
        for p_ in patches: p_.stop()


def test_workflow_has_nightly_backfill_and_serial_concurrency():
    y = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".github", "workflows", "daily.yml"), encoding="utf-8").read()
    assert '- cron: "0 20 * * *"' in y and '"0 20 * * *")  echo "job=tw_backfill"' in y
    assert "concurrency:" in y and "cancel-in-progress: false" in y and "timeout-minutes: 90" in y


# ---------------------------------------------------------------- 台幣：FRED 落後時另存較新的即期參考價
def fred_row(days_ago):
    return {"as_of": (TODAY - timedelta(days=days_ago)).isoformat(), "value": 31.4, "kind": "official_close", "source": "FRED DEXTAUS", "layer": 3, "is_close_history": True}


def test_twd_supplement_only_when_fred_lags():
    reset()
    spot = {"value": 31.9, "as_of": TODAY.isoformat()}
    with mock.patch.object(fetch, "exchangerate_twd", return_value=spot) as ex:
        assert fetch.twd_supplement(fred_row(1)) is None and ex.call_count == 0              # 只落後 1 天：不需要
        r = fetch.twd_supplement(fred_row(7))
    assert r == {"as_of": TODAY.isoformat(), "value": 31.9, "kind": "spot_aggregator", "source": "ExchangeRate-API", "layer": 5, "is_close_history": False}
    assert "落後 7 天" in fetch.LOG[-1]["detail"] and "不是官方收盤價" in fetch.LOG[-1]["detail"]


def test_twd_supplement_ignores_fallbacks_and_failures():
    with mock.patch.object(fetch, "exchangerate_twd", return_value=None):
        assert fetch.twd_supplement(fred_row(7)) is None                                     # 備用來源也失敗：不編
    for bad in (None, {"stale_fallback": True, "as_of": D(9), "layer": 3}, dict(fred_row(9), layer=5)):
        with mock.patch.object(fetch, "exchangerate_twd") as ex:
            assert fetch.twd_supplement(bad) is None and ex.call_count == 0


def test_persist_writes_primary_and_supplement_together_never_stale_fallback():
    reset()
    prim, sup = fred_row(7), {"as_of": D(0), "value": 31.9, "kind": "spot_aggregator", "source": "ExchangeRate-API", "layer": 5, "is_close_history": False}
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], [prim, sup])
        assert up.call_args[0][0] == "twd_rates" and up.call_args[0][1] == [prim, sup]
        up.reset_mock()
        fetch.persist(False, [], {"stale_fallback": True, "as_of": D(9), "value": 1})
        up.assert_not_called()                                                                 # 沿用舊值不重新寫入
        fetch.persist(False, [], prim)
        assert up.call_args[0][1] == [prim]                                                    # 單列寫法仍可用


def test_run_job_adds_supplement_row_to_twd():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x"))]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "twd_fallback_chain", return_value=fred_row(7)), mock.patch.object(fetch, "exchangerate_twd", return_value={"value": 31.9, "as_of": D(0)}):
            res = fetch.run_job("tw_close", dry_run=True)
    finally:
        for p_ in patches: p_.stop()
    assert isinstance(res["twd"], list) and [r["layer"] for r in res["twd"]] == [3, 5]


# ---------------------------------------------------------------- 台股大跌檢視（第一步：自動加入，待確認）
def crash_env(by_day, hist_days=None, names=None):
    """by_day: {日期: [(市場, 代號, 收盤)]}；回傳 patch 用的 sb_request。"""
    days = sorted(by_day)
    hist_days = hist_days or {d: ["TWSE", "TPEX"] for d in days}
    def req(method, path, body=None, prefer=None):
        if path.startswith("tw_hist_days"):
            return [{"market": m, "as_of": d} for d, ms in hist_days.items() for m in ms], ""
        if path.startswith("tw_hist?"):
            d = path.split("as_of=eq.")[1].split("&")[0]
            off = int(path.split("offset=")[1]) if "offset=" in path else 0
            rows = [{"market": m, "code": c, "close": cl} for m, c, cl in by_day.get(d, [])]
            return rows[off:off + 1000], ""
        raise AssertionError(path)
    return req


def test_crash_check_flags_nine_percent_drops_only():
    reset()
    d0, d1 = D(3), D(2)
    by_day = {d0: [("TWSE", "2330", 100.0), ("TWSE", "1101", 50.0), ("TPEX", "8358", 400.0), ("TWSE", "2317", 100.0)],
              d1: [("TWSE", "2330", 90.9), ("TWSE", "1101", 46.0), ("TPEX", "8358", 360.0), ("TWSE", "2317", 100.0)]}
    names = [{"Code": "2330", "Name": "台積電"}, {"Code": "1101", "Name": "台泥"}]
    tp = [{"SecuritiesCompanyCode": "8358", "CompanyName": "金居"}]
    ups = []
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day)), \
            mock.patch.object(fetch, "fetch_json", side_effect=lambda url, **k: ((names if "twse" in url else tp), "")), \
            mock.patch.object(fetch, "sb_upsert", side_effect=lambda t, r, k: ups.append((t, r, k)) or (True, "ok")):
        found = fetch.tw_crash_check([d1])
    got = {f["code"]: f for f in found}
    assert set(got) == {"2330", "8358"}                       # 2330 跌 9.1%、8358 跌 10% 入選；1101 只跌 8%、2317 沒跌，不入選
    assert got["2330"]["drop_pct"] == -9.1 and got["8358"]["drop_pct"] == -10.0 and got["8358"]["name"] == "金居"
    assert ups[0][0] == "crash_candidates" and ups[0][2] == "as_of,market,code"


def test_crash_check_boundaries_prev_gap_dry_run_and_errors():
    reset()
    a, b = D(30), D(2)
    by_day = {a: [("TWSE", "2330", 100.0)], b: [("TWSE", "2330", 50.0)]}
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day)), mock.patch.object(fetch, "sb_upsert") as up:
        assert fetch.tw_crash_check([b]) == []
        assert any(x["status"] == "no_new_data" and "不比較" in x["detail"] for x in fetch.LOG)       # 前一個交易日隔太久 → 不比較
        up.assert_not_called()
    reset()
    by_day = {D(3): [("TWSE", "2330", 100.0), ("TWSE", "1101", 100.0)], D(2): [("TWSE", "2330", 91.01), ("TWSE", "1101", 91.0)]}   # -8.99% 不入選、-9.0% 入選
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day)), \
            mock.patch.object(fetch, "fetch_json", return_value=([], "")), mock.patch.object(fetch, "sb_upsert") as up:
        found = fetch.tw_crash_check([D(2)], dry_run=True)
    assert [f["code"] for f in found] == ["1101"] and found[0]["name"] == "" and up.call_count == 0
    assert "原因待確認" in found[0]["note"] and "除權息" in found[0]["note"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert fetch.tw_crash_check([D(2)]) is None and fetch.LOG[-1]["status"] == "skipped"
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 404")):
        assert fetch.tw_crash_check([D(2)]) is None and fetch.LOG[-1]["status"] == "failure"


def test_crash_check_skips_days_not_complete_and_paginates():
    reset()
    big = [("TWSE", f"{1000 + i}", 100.0) for i in range(1500)]
    by_day = {D(3): big, D(2): [(m, c, 80.0 if i < 3 else 100.0) for i, (m, c, _) in enumerate(big)]}
    hd = {D(3): ["TWSE", "TPEX"], D(2): ["TWSE", "TPEX"], D(1): ["TWSE"]}
    ups = []
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day, hist_days=hd)), \
            mock.patch.object(fetch, "fetch_json", return_value=([], "")), mock.patch.object(fetch, "sb_upsert", side_effect=lambda t, r, k: ups.append(r) or (True, "ok")):
        found = fetch.tw_crash_check([D(2), D(1)])                    # D(1) 只有上市完成 → 略過
    assert len(found) == 3 and {f["as_of"] for f in found} == {D(2)}   # 1500 列要翻頁才讀得完


def test_crash_check_only_looks_at_recent_days_not_the_whole_backfill():
    reset()
    old, recent = D(60), D(2)
    prev_old, prev_recent = D(61), D(3)
    by_day = {prev_old: [("TWSE", "2330", 100.0)], old: [("TWSE", "2330", 80.0)],             # 兩個月前的大跌：不收
              prev_recent: [("TWSE", "1101", 100.0)], recent: [("TWSE", "1101", 85.0)]}       # 近期的大跌：收
    ups = []
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day)), \
            mock.patch.object(fetch, "fetch_json", return_value=([], "")), mock.patch.object(fetch, "sb_upsert", side_effect=lambda t_, r, k: ups.append(r) or (True, "ok")):
        found = fetch.tw_crash_check([old, recent, prev_old])
    assert [f["code"] for f in found] == ["1101"] and all(f["as_of"] == recent for f in ups[0])
    assert any("略過 2 個超過 30 天" in x["detail"] for x in fetch.LOG)
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=crash_env(by_day)), mock.patch.object(fetch, "sb_upsert") as up:
        assert fetch.tw_crash_check([old, prev_old]) == []                                   # 全部太舊：不寫入
    up.assert_not_called()
    assert config.CRASH_MAX_AGE_DAYS == 30


def test_backfill_returns_new_days_and_crash_check_wiring():
    reset()
    res, calls, ups, _ = run_backfill(max_days=2)
    assert res["new_days"] == [d.isoformat() for d in weekdays_back(2)]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
              mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "_recent_complete_days", return_value=["R"])]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "tw_backfill", return_value={"new_days": ["N1"]}), mock.patch.object(fetch, "tw_crash_check") as cc:
            fetch.run_job("tw_close", dry_run=False)
            assert cc.call_args[0][0] == ["N1", "R"] and cc.call_args.kwargs == {}
            fetch.run_job("tw_close", dry_run=True); assert cc.call_count == 1                 # dry-run 不檢查
            fetch.run_job("tw_backfill", dry_run=True)
            assert cc.call_args[0][0] == ["N1", "R"] and cc.call_args.kwargs == {"dry_run": True}
    finally:
        for p_ in patches: p_.stop()


# ---------------------------------------------------------------- 槓桿 ETF 52 週（Nasdaq 歷史行情，含分割偵測）
def etf_hist_rows(n=260, start_close=20.0, jump_at=None, jump_ratio=1.0, end_date=None, hi_spike=None):
    end = end_date or (TODAY - timedelta(days=1))
    while end.weekday() >= 5:
        end -= timedelta(days=1)
    days, d = [], end
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    days.sort()
    rows, c = [], start_close
    for i, day in enumerate(days):
        if jump_at is not None and i == jump_at:
            c *= jump_ratio
        else:
            c *= 1.002 if i % 2 else 0.999
        high = c * 1.03 if hi_spike != i else c * 1.5
        rows.append({"date": day.strftime("%m/%d/%Y"), "close": f"{c:.3f}", "volume": "1", "open": f"{c:.3f}", "high": f"${high:.3f}", "low": f"{c * 0.97:.3f}"})
    return list(reversed(rows))                                     # Nasdaq 由新到舊


def hist_body(rows):
    return json.dumps({"data": {"symbol": "MUU", "tradesTable": {"rows": rows}}}).encode()


def run_etf_range(rows, close, price_date=None):
    pd_ = price_date or (TODAY - timedelta(days=1)).isoformat()
    while date.fromisoformat(pd_).weekday() >= 5:
        pd_ = (date.fromisoformat(pd_) - timedelta(days=1)).isoformat()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", hist_body(rows), "")):
        return fetch.nasdaq_hist_range("MUU", "etf", close, pd_)


def test_etf_range_uses_highs_lows_when_clean():
    rows = etf_hist_rows()
    last = float(rows[0]["close"])
    lo, hi, why = run_etf_range(rows, last)
    assert lo == min(float(r["low"]) for r in rows) and hi == max(float(r["high"].replace("$", "")) for r in rows)
    assert "沒有分割跳動" in why and "260 個交易日" in why


def test_etf_range_rejects_unadjusted_split_and_short_or_stale_history():
    rows = etf_hist_rows(jump_at=120, jump_ratio=15.0)                      # 反向分割沒調整：1.8 → 27 的 15 倍跳動（跟 MUU 的情況一樣）
    lo, hi, why = run_etf_range(rows, float(rows[0]["close"]))
    assert lo is None and "分割" in why and "15.00 倍" in why
    rows = etf_hist_rows(jump_at=120, jump_ratio=0.1)                       # 一股分成十股
    assert run_etf_range(rows, float(rows[0]["close"]))[0] is None
    rows = etf_hist_rows(n=120)                                             # 歷史太短（這檔 ETF 才上市不到一年）
    lo, hi, why = run_etf_range(rows, float(rows[0]["close"]))
    assert lo is None and "不足以算 52 週" in why
    rows = etf_hist_rows(end_date=TODAY - timedelta(days=20))               # 最新一筆太舊
    assert "差太多" in run_etf_range(rows, float(rows[0]["close"]))[2]
    rows = etf_hist_rows()
    assert "不在歷史區間" in run_etf_range(rows, 999.0)[2]                  # 收盤價跑出區間
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")):
        assert fetch.nasdaq_hist_range("MUU", "etf", 1.0, D(1))[0] is None
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>", "")):
        assert "不是 JSON" in fetch.nasdaq_hist_range("MUU", "etf", 1.0, D(1))[2]
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", hist_body([]), "")):
        assert "沒有可用" in fetch.nasdaq_hist_range("MUU", "etf", 1.0, D(1))[2]


def test_nasdaq_prices_fills_etf_range_from_history_but_not_for_plain_stocks():
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    hist = etf_hist_rows()
    close = float(hist[0]["close"])
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url)
        if "/historical" in url:
            return 200, "application/json", hist_body(hist), ""
        d = {"data": {"marketStatus": "Closed", "assetClass": "ETF", "primaryData": {"lastSalePrice": f"${close}", "netChange": "-1", "lastTradeTimestamp": ts, "isRealTime": False}, "keyStats": None}}
        return 200, "application/json", json.dumps(d).encode(), ""
    muu = [{"market": "US", "symbol": "MUU", "name": "x", "kind": "leveraged_etf", "pos_key": "MUU"}]
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        r = fetch.nasdaq_prices(muu)[0]
    assert r["low_52w"] is not None and "Nasdaq 歷史行情" in r["range_source"]
    assert any("/historical" in u and "assetclass=etf" in u for u in seen)            # 歷史行情沿用報價成功的分類
    # 反向分割沒調整 → 區間不採用，價格本身仍照存，原因寫進紀錄
    bad = etf_hist_rows(jump_at=120, jump_ratio=15.0)
    def raw2(url, timeout=None, headers=None):
        if "/historical" in url:
            return 200, "application/json", hist_body(bad), ""
        return raw(url)
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw2), mock.patch.object(fetch.time, "sleep"):
        r = fetch.nasdaq_prices(muu)[0]
    assert r["high_52w"] is None and r["range_source"] is None and r["close"] == close
    assert "52 週區間查不到" in fetch.LOG[-1]["detail"] and "分割" in fetch.LOG[-1]["detail"]
    # 一般股票沒有 keyStats 也不去打歷史行情（避免多餘請求）
    calls = []
    def raw3(url, timeout=None, headers=None):
        calls.append(url); return raw(url)
    stock = [{"market": "US", "symbol": "ORCL", "name": "x", "kind": "stock", "pos_key": "ORCL"}]
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw3), mock.patch.object(fetch.time, "sleep"):
        fetch.nasdaq_prices(stock)
    assert not any("/historical" in u for u in calls)


# ---------------------------------------------------------------- 美股資金壓力來源探測
def test_probe_us_pressure_logs_three_sources():
    reset()
    cftc = json.dumps([{"market_and_exchange_names": "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE", "report_date_as_yyyy_mm_dd": "2026-10-27T00:00:00.000",
                        "lev_money_positions_long": "100", "lev_money_positions_short": "300"}]).encode()
    finra = b"<html><body><table><tr><td>Debit Balances in Customers' Securities Margin Accounts</td><td>1,234,567</td></tr></table></body></html>"
    def raw(url, timeout=None, headers=None):
        if "cftc.gov" in url: return 200, "application/json", cftc, ""
        if "finra.org" in url: return 200, "text/html", finra, ""
        if "totalpc.csv" in url: return 200, "text/csv", b"DATE,CALL,PUT,TOTAL,P/C Ratio\n2019-10-04,1,1,2,0.9\n", ""
        return 404, "text/html", b"not found", "HTTP 404"
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_us_pressure()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:cftc_tff"]["status"] == "success" and "S&P 500 Consolidated" in by["probe:cftc_tff"]["detail"] and "2026-10-27" in by["probe:cftc_tff"]["detail"]
    assert "lev_money_positions_long" in by["probe:cftc_tff_row"]["detail"]
    assert by["probe:finra_margin"]["status"] == "success" and "1,234,567" in by["probe:finra_margin"]["detail"]
    assert by["probe:cboe_pc:totalpc_csv"]["status"] == "success" and "2019-10-04" in by["probe:cboe_pc:totalpc_csv"]["detail"]
    assert by["probe:cboe_pc:daily_json"]["status"] == "failure"
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_us_pressure_failures_and_js_only_finra_page():
    reset()
    def raw(url, timeout=None, headers=None):
        if "finra.org" in url: return 200, "text/html", b"<html><script>app()</script></html>", ""
        return None, "", b"", "timed out"
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw):
        fetch.probe_us_pressure()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:cftc_tff"]["status"] == "failure" and "連線失敗" in by["probe:cftc_tff"]["detail"]
    assert by["probe:finra_margin"]["status"] == "failure" and "找不到 Debit Balances" in by["probe:finra_margin"]["detail"]


# ---------------------------------------------------------------- CFTC COT（標普 500 合併合約）
MKT = "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE"


def cot_row(d, oi=2000000, dl=100, ds=300, al=900000, ash=500000, ll=400000, ls=700000, market=MKT):
    return {"market_and_exchange_names": market, "report_date_as_yyyy_mm_dd": f"{d}T00:00:00.000", "open_interest_all": str(oi),
            "dealer_positions_long_all": str(dl), "dealer_positions_short_all": str(ds), "asset_mgr_positions_long": str(al),
            "asset_mgr_positions_short": str(ash), "lev_money_positions_long": str(ll), "lev_money_positions_short": str(ls)}


def test_cftc_cot_parses_two_weeks_with_own_report_date():
    reset()
    rows = [cot_row(D(10)), cot_row(D(17), ll=380000, ls=650000)]
    urls = []
    def fj(url, **k):
        urls.append(url); return rows, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = fetch.cftc_cot()
    assert [r["report_date"] for r in out] == [D(10), D(17)]                             # 新到舊，日期是報告日自己的日期
    assert out[0]["lev_long"] == 400000 and out[0]["lev_short"] == 700000 and out[0]["open_interest"] == 2000000
    assert out[0]["market"] == "S&P 500 Consolidated"
    assert "S%26P%20500%20Consolidated" in urls[0] and "gpe5-46if" in urls[0] and "%24limit=2" in urls[0] and "report_date_as_yyyy_mm_dd%20DESC" in urls[0]
    assert "槓桿基金淨 -300,000" in fetch.LOG[-1]["detail"] and fetch.LOG[-1]["as_of"] == D(10)


def test_cftc_cot_rejects_bad_rows_and_wrong_market():
    reset()
    rows = [cot_row(D(10), market="E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"),         # 不是合併合約
            cot_row(D(17), al=3000000),                                                      # 多方超過未平倉
            cot_row("2999-01-01"), cot_row(D(24), oi=0), cot_row(D(31), ll=-5)]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        assert fetch.cftc_cot() is None
    assert fetch.LOG[-1]["status"] == "failure" and "沒通過驗證" in fetch.LOG[-1]["detail"]
    ok_and_bad = [cot_row(D(10)), cot_row(D(17), oi=0)]
    with mock.patch.object(fetch, "fetch_json", return_value=(ok_and_bad, "")):
        out = fetch.cftc_cot()
    assert len(out) == 1 and "1 列未通過驗證" in fetch.LOG[-1]["detail"]
    for bad in ((None, "HTTP 403"), ([], ""), ({"x": 1}, ""), ([{"x": 1}], "")):
        reset()
        with mock.patch.object(fetch, "fetch_json", return_value=bad):
            assert fetch.cftc_cot() is None
        assert fetch.LOG[-1]["status"] == "failure"


# ---------------------------------------------------------------- CBOE Put/Call
def cboe_body(total="0.93", extra=True):
    r = [{"name": "TOTAL PUT/CALL RATIO", "value": total}, {"name": "INDEX PUT/CALL RATIO", "value": "1.06"},
         {"name": "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO", "value": "1.09"}]
    if extra:
        r.append({"name": "EQUITY PUT/CALL RATIO", "value": "0.62"})
    return json.dumps({"ratios": r, "SUM OF ALL PRODUCTS": [{"name": "VOLUME", "call": 1, "put": 1, "total": 2}]}).encode()


def fake_et(y, m, d, hour):
    from zoneinfo import ZoneInfo
    return datetime(y, m, d, hour, 0, tzinfo=ZoneInfo("America/New_York"))


def test_cboe_putcall_uses_closed_trading_day_with_its_own_date():
    reset()
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url); return 200, "application/json", cboe_body(), ""
    with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 10, 30, 10)), mock.patch.object(fetch, "fetch_raw", side_effect=raw):
        out = fetch.cboe_putcall()
    assert out[0]["as_of"] == "2026-10-29" and "2026-10-29_daily_options" in seen[0]               # 週五早上 → 取週四（不取當天還沒收盤的）
    assert (out[0]["total_pc"], out[0]["index_pc"], out[0]["etp_pc"], out[0]["equity_pc"]) == (0.93, 1.06, 1.09, 0.62)


def test_cboe_putcall_same_day_only_after_five_pm_and_skips_weekends():
    reset()
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url); return 200, "application/json", cboe_body(), ""
    with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 10, 30, 18)), mock.patch.object(fetch, "fetch_raw", side_effect=raw):
        assert fetch.cboe_putcall()[0]["as_of"] == "2026-10-30"                                      # 收盤後的當天可用
    seen.clear()
    with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 11, 2, 9)), mock.patch.object(fetch, "fetch_raw", side_effect=raw):
        assert fetch.cboe_putcall()[0]["as_of"] == "2026-10-30"                                      # 週一早上 → 取上週五，跳過週末
    assert all("2026-10-31" not in u and "2026-11-01" not in u for u in seen)


def test_cboe_putcall_falls_back_to_earlier_day_and_never_fabricates():
    reset()
    def raw(url, timeout=None, headers=None):
        return (404, "text/html", b"nope", "HTTP 404") if "2026-10-29" in url else (200, "application/json", cboe_body(extra=False), "")
    with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 10, 30, 10)), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = fetch.cboe_putcall()
    assert out[0]["as_of"] == "2026-10-28" and out[0]["equity_pc"] is None                          # 沒有的欄位留空，不編
    reset()
    with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 10, 30, 10)), mock.patch.object(fetch, "fetch_raw", return_value=(404, "x", b"", "HTTP 404")), mock.patch.object(fetch.time, "sleep"):
        assert fetch.cboe_putcall() is None
    assert fetch.LOG[-1]["status"] == "no_new_data" and "2026-10-29:404" in fetch.LOG[-1]["detail"]
    for body in (b"not json", cboe_body(total="0"), cboe_body(total="99"), json.dumps({"ratios": []}).encode()):
        reset()
        with mock.patch.object(fetch, "_now_et", return_value=fake_et(2026, 10, 30, 10)), mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body, "")), mock.patch.object(fetch.time, "sleep"):
            assert fetch.cboe_putcall() is None


def test_persist_writes_cot_and_putcall_and_job_wires_them():
    reset()
    cot = [{"report_date": D(10), "market": "S&P 500 Consolidated"}]
    pc = [{"as_of": D(1), "total_pc": 0.9}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, None, None, None, None, None, cot, pc)
    calls = {c[0][0]: c[0][2] for c in up.call_args_list}
    assert calls == {"us_cot": "report_date,market", "us_putcall": "as_of"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x"))]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "cftc_cot", return_value=cot) as c1, mock.patch.object(fetch, "cboe_putcall", return_value=pc) as c2:
            res = fetch.run_job("tw_close", dry_run=True)
        c1.assert_called_once(); c2.assert_called_once()
        assert res["us_cot"] == cot and res["us_putcall"] == pc
    finally:
        for p_ in patches: p_.stop()


def test_fred_macro_includes_quarterly_margin_series_with_sane_range():
    label, lo, hi = config.FRED_MACRO["BOGZ1FL663067003Q"]
    assert "Z.1" in label and "季資料" in label and lo < 622199 < hi              # Fed 公布的 2026 Q1 數字（百萬美元）要落在合理範圍內
    reset()
    obs = {"observations": [{"date": "2026-04-01", "value": "."}, {"date": "2026-01-01", "value": "622199"}]}
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(obs, "")):
        r = fetch.fred_latest("BOGZ1FL663067003Q", lo, hi)
    assert r["value"] == 622199.0 and r["as_of"] == "2026-01-01"                    # 季資料：用該筆自己的日期，最新一季還沒公布就取上一季
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=({"observations": [{"date": "2026-01-01", "value": "6221"}]}, "")):
        assert fetch.fred_latest("BOGZ1FL663067003Q", lo, hi) is None               # 單位弄錯（少三個零）→ 拒收


def test_main_logs_version_first_and_returns_failure_exit_code(tmp_path=None):
    reset()
    with mock.patch.object(fetch, "run_job", return_value={}) as rj, mock.patch.object(fetch, "flush_log", return_value=True), \
            mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()):
        code = fetch.main(["--job", "tw_probe", "--dry-run"])
    assert code == 0 and rj.call_args[0][0] == "tw_probe"
    first = fetch.LOG[0]
    assert first["source"] == "pipeline:version" and first["status"] == "success" and first["job"] == "tw_probe"
    assert config.PIPELINE_VERSION in first["detail"] and "dry-run" in first["detail"]
    assert config.PIPELINE_VERSION.startswith("v") and config.PIPELINE_VERSION[1:].isdigit()
    reset()
    def boom(job, dry_run=False):
        fetch.log("x", "failure", detail="boom"); return {}
    with mock.patch.object(fetch, "run_job", side_effect=boom), mock.patch.object(fetch, "flush_log", return_value=True), \
            mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()):
        assert fetch.main(["--job", "full"]) == 1                                  # 有失敗 → 結束碼 1（Actions 亮紅燈）
    assert fetch.LOG[0]["source"] == "pipeline:version" and "正式寫入" in fetch.LOG[0]["detail"]


# ---------------------------------------------------------------- 財報日來源探測
def test_probe_earnings_logs_us_dates_and_tw_endpoints_without_writing():
    reset()
    hold = [{"market": "US", "symbol": "ORCL", "name": "甲骨文", "kind": "stock", "pos_key": "ORCL"},
            {"market": "US", "symbol": "MUU", "name": "x", "kind": "leveraged_etf", "pos_key": "MUU"}]
    nas = json.dumps({"data": {"announcement": "Earnings announcement* for ORCL: Dec 9, 2026", "reportText": "x"}, "status": {"rCode": 200}}).encode()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap": {"get": {"summary": "上市公司法人說明會"}}, "/x": {"get": {"summary": "其他"}}}}
    urls = []
    def fj(url, **k):
        urls.append(url)
        if url.endswith("swagger.json"): return sw, ""
        return [{"公司代號": "2330", "召開法人說明會日期": "1151015"}], ""
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas, "")) as raw, \
            mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_earnings()
    assert raw.call_count == 1 and "ORCL" in raw.call_args[0][0] and "earnings-date" in raw.call_args[0][0]       # ETF 不查財報日
    by = {x["source"]: x for x in fetch.LOG}
    assert "Dec 9, 2026" in by["probe:earn_us:ORCL"]["detail"]
    assert "法人說明會" in by["probe:earn_tw:twse"]["detail"] and "/x" not in by["probe:earn_tw:twse"]["detail"]
    assert "召開法人說明會日期" in by["probe:earn_tw_rows:twse"]["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_earnings_looks_into_material_news_for_investor_conferences():
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap04_L": {"get": {"summary": "上市公司每日重大訊息"}}, "/x": {"get": {"summary": "其他"}}}}
    rows = [{"公司代號": "2330", "主旨": "公告本公司召開法人說明會相關資訊", "說明": "1.召開法人說明會之日期:115/11/13"},
            {"公司代號": "1101", "主旨": "公告董事會決議", "說明": "x"}]
    urls = []
    def fj(url, **k):
        urls.append(url)
        return (sw, "") if url.endswith("swagger.json") else (rows, "")
    with mock.patch.object(fetch, "load_holdings", return_value=([], "")), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_earnings()
    by = {x["source"]: x for x in fetch.LOG}
    d = by["probe:earn_tw_rows:twse:news"]["detail"]
    assert by["probe:earn_tw_rows:twse:news"]["status"] == "success" and "含『法人說明會』1 列" in d and "115/11/13" in d and "公告董事會決議" not in d
    assert any(u.endswith("/opendata/t187ap04_L") for u in urls)
    up.assert_not_called()
    rows[0]["主旨"] = rows[0]["說明"] = "沒有"                                                    # 沒有任何法說會公告：要說沒有，不是失敗
    reset()
    with mock.patch.object(fetch, "load_holdings", return_value=([], "")), mock.patch.object(fetch, "fetch_json", side_effect=fj):
        fetch.probe_earnings()
    assert {x["source"]: x for x in fetch.LOG}["probe:earn_tw_rows:twse:news"]["status"] == "no_new_data"


def test_probe_earnings_failures_are_reported():
    reset()
    hold = [{"market": "US", "symbol": "ORCL", "name": "x", "kind": "stock", "pos_key": "ORCL"}]
    with mock.patch.object(fetch, "load_holdings", return_value=(hold, "")), mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")), \
            mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_earnings()
    sts = {x["source"]: x["status"] for x in fetch.LOG}
    assert sts["probe:earn_us:ORCL"] == "failure" and sts["probe:earn_tw:twse"] == "failure" and sts["probe:earn_tw:tpex"] == "failure"


# ---------------------------------------------------------------- 美股財報日（Nasdaq）
T0 = date(2026, 10, 30)


def test_parse_earnings_text_requires_exactly_one_plausible_future_date():
    f = lambda s: fetch.parse_earnings_text(s, T0)
    # 你貼的 Nasdaq 真實原文（雙空格、星號、盤前盤後）
    assert f("Circle Internet Group, Inc. Class A Common Stock is expected* to report earnings on  11/04/2026 before market open. ") == ("2026-11-04", True, "pre")
    assert f("The Arm Holdings plc American Depositary Shares is expected* to report earnings on  11/04/2026 after market close.  The report date") == ("2026-11-04", True, "post")
    assert f("Oracle Corporation Common Stock is estimated to report earnings on  12/09/2026. The upcoming earnings date is derived from an algorithm") == ("2026-12-09", True, None)
    assert f("Arm Holdings plc is expected to report earnings on Dec 9, 2026 after market close") == ("2026-12-09", True, "post")
    assert f("Earnings announcement for ORCL: Feb 3, 2027, confirmed by the company") == ("2027-02-03", False, None)
    assert f("The company has not yet confirmed. Estimated date 11/18/2026") == ("2026-11-18", True, None)
    assert f("Report on 12/09/2026 (12/09/2026)") == ("2026-12-09", False, None)                 # 同一天重複出現算同一個日期
    # 明確句型優先：後面又出現別的日期也不受影響（真實原文後面常有說明句）
    assert f("X is expected* to report earnings on 11/04/2026. Last report 08/05/2026.") == ("2026-11-04", True, None)
    assert f("Oracle reports 12/09/2026 and previously 09/10/2026") is None                     # 沒有明確句型又有兩個日期：不猜
    assert f("Reported on 10/01/2026") is None                                                  # 過去的日期
    assert f("X is expected to report earnings on 12/09/2027") is None                          # 太遠，疑似解析錯誤
    assert f("X is expected to report earnings on 10/01/2026") is None                          # 明確句型但日期已過
    assert f("Earnings 13/45/2026") is None and f("X to report earnings on 13/45/2026") is None
    assert f("no date here") is None and f("") is None and f(None) is None
    assert f("on 10/30/2026") == ("2026-10-30", False, None)                                    # 今天算


def earn_body(text):
    return json.dumps({"data": {"reportText": text}, "status": {"rCode": 200}}).encode()


def test_nasdaq_earnings_skips_etfs_and_never_writes_unparsed_dates():
    reset()
    ahead = (date.today() + timedelta(days=40))
    txt = f"Oracle Corporation Common Stock is estimated to report earnings on {ahead.month:02d}/{ahead.day:02d}/{ahead.year}."
    hold = [{"market": "US", "symbol": "ORCL", "name": "x", "kind": "stock", "pos_key": "ORCL"},
            {"market": "US", "symbol": "ARM", "name": "x", "kind": "stock", "pos_key": "ARM"},
            {"market": "US", "symbol": "MUU", "name": "x", "kind": "leveraged_etf", "pos_key": "MUU"},
            {"market": "TW", "symbol": None, "name": "台積電", "kind": "stock", "pos_key": "台積電"}]
    urls = []
    def raw(url, timeout=None, headers=None):
        urls.append(url)
        return 200, "application/json", earn_body(txt if "/ORCL/" in url else "No earnings date is available."), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = fetch.nasdaq_earnings(hold)
    assert len(urls) == 2 and not any("MUU" in u for u in urls)                                  # ETF 與台股不查
    assert len(out) == 1 and out[0]["symbol"] == "ORCL" and out[0]["is_estimate"] is True and out[0]["report_date"] == ahead.isoformat() and out[0]["timing"] is None
    assert "estimated" in out[0]["raw_text"]
    miss = [x for x in fetch.LOG if x["source"] == "nasdaq:earnings:ARM"][0]
    assert miss["status"] == "no_new_data" and "No earnings date is available" in miss["detail"]   # 沒解析出來要把原文留下來


def test_nasdaq_earnings_keeps_timing_and_all_rows_have_same_keys():
    reset()
    ahead = date.today() + timedelta(days=5)
    fmt = f"{ahead.month:02d}/{ahead.day:02d}/{ahead.year}"
    hold = [{"market": "US", "symbol": "ARM", "name": "x", "kind": "stock", "pos_key": "ARM"}, {"market": "US", "symbol": "CRCL", "name": "x", "kind": "stock", "pos_key": "CRCL"},
            {"market": "US", "symbol": "ORCL", "name": "x", "kind": "stock", "pos_key": "ORCL"}]
    texts = {"/ARM/": f"Arm is expected* to report earnings on  {fmt} after market close.", "/CRCL/": f"Circle is expected* to report earnings on  {fmt} before market open.", "/ORCL/": f"Oracle is estimated to report earnings on  {fmt}."}
    def raw(url, timeout=None, headers=None):
        return 200, "application/json", earn_body(next(v for k, v in texts.items() if k in url)), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = fetch.nasdaq_earnings(hold)
    assert {r["symbol"]: r["timing"] for r in out} == {"ARM": "post", "CRCL": "pre", "ORCL": None}
    assert len({tuple(sorted(r)) for r in out}) == 1                                              # 每列欄位一致，批次寫入才不會出現缺欄位
    assert "，盤後" in [x for x in fetch.LOG if x["source"] == "nasdaq:earnings:ARM"][0]["detail"]


def test_nasdaq_earnings_failures_are_logged_not_fabricated():
    reset()
    hold = [{"market": "US", "symbol": "ORCL", "name": "x", "kind": "stock", "pos_key": "ORCL"}]
    for ret in ((None, "", b"", "timed out"), (200, "text/html", b"<html>", "")):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=ret), mock.patch.object(fetch.time, "sleep"):
            assert fetch.nasdaq_earnings(hold) == []
        assert fetch.LOG[-1]["status"] == "failure"


def test_persist_writes_earnings_and_job_wires_them():
    reset()
    rows = [{"symbol": "ORCL", "report_date": D(-30), "is_estimate": True, "raw_text": "x", "source": "s"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", return_value=(True, "ok")) as up:
        fetch.persist(False, [], None, None, None, None, None, None, None, None, None, None, rows)
    assert up.call_args[0][0] == "us_earnings" and up.call_args[0][2] == "symbol,report_date"
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=([{"market": "US"}], "")),
              mock.patch.object(fetch, "tpex_prices", return_value=[]), mock.patch.object(fetch, "twse_prices", return_value=[]), mock.patch.object(fetch, "nasdaq_prices", return_value=[]), mock.patch.object(fetch, "tw_ranges", side_effect=lambda x: x)]
    for p_ in patches: p_.start()
    try:
        with mock.patch.object(fetch, "nasdaq_earnings", return_value=rows) as ne:
            res = fetch.run_job("tw_close", dry_run=True)
        ne.assert_called_once()
        assert res["us_earnings"] == rows
    finally:
        for p_ in patches: p_.stop()


# ================================================================ 模組隔離與各模組（M01–M06）
def test_run_module_isolates_exceptions_flags_and_empty_results():
    reset()
    assert fetch._run_module("M02", "測試", lambda: 1 / 0) is None                           # 例外只變成這個模組的失敗
    x = fetch.LOG[-1]
    assert x["source"] == "module:M02" and x["status"] == "failure" and "ZeroDivisionError" in x["detail"]
    reset()
    assert fetch._run_module("M02", "測試", lambda: [1, 2]) == [1, 2] and fetch.LOG[-1]["status"] == "success" and "產出 2 筆" in fetch.LOG[-1]["detail"]
    assert fetch._run_module("M03", "測試", lambda: []) == [] and fetch.LOG[-1]["status"] == "no_new_data"
    assert fetch._run_module("M99", "不在開關表裡的模組", lambda: [1]) == [1]                  # 沒有開關的模組一律執行
    old = dict(config.FEATURES)
    try:
        config.FEATURES["M02_us_quotes"] = False
        called = []
        assert fetch._run_module("M02", "測試", lambda: called.append(1)) is None and called == []     # 關掉就完全不執行
        assert fetch.LOG[-1]["status"] == "skipped" and "FEATURES" in fetch.LOG[-1]["detail"]
    finally:
        config.FEATURES.clear(); config.FEATURES.update(old)


def test_fred_optional_series_never_turn_the_job_red():
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 400")):
        assert fetch.fred_latest("RU2000PR", 100, 20000, optional=True) is None
    assert fetch.LOG[-1]["status"] == "no_new_data" and "選用項目" in fetch.LOG[-1]["detail"]
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 400")):
        assert fetch.fred_latest("DGS10", 0, 15) is None
    assert fetch.LOG[-1]["status"] == "failure"
    assert "RU2000PR" in config.FRED_OPTIONAL and {"DJIA", "BAMLH0A0HYM2", "RU2000PR"} <= set(config.FRED_MACRO)
    assert config.FRED_MACRO["BAMLH0A0HYM2"][1] < 4 < config.FRED_MACRO["BAMLH0A0HYM2"][2]          # 真實利差約 3–4%，要落在合理範圍內


def nas_quote_body(price="$745.20", pct="+0.45%", ms="Closed", real=False, ts=None, asset="etf"):
    ts = ts or (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    return json.dumps({"data": {"marketStatus": ms, "assetClass": asset, "primaryData": {"lastSalePrice": price, "netChange": "+3", "percentageChange": pct, "lastTradeTimestamp": ts, "isRealTime": real}}}).encode()


def test_nasdaq_quote_only_accepts_closed_prices_with_their_own_date():
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_quote_body(), "")):
        q, why = fetch._nasdaq_quote("SPY", ("etf",))
    assert q["close"] == 745.2 and q["pct"] == 0.45 and q["as_of"] == (TODAY - timedelta(days=1)).isoformat() and why == ""
    for body in (nas_quote_body(ms="Pre-Market", real=True), nas_quote_body(real=True), nas_quote_body(ms="Open")):
        with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body, "")):
            q, why = fetch._nasdaq_quote("SPY", ("etf",))
        assert q is None and "不是已收盤" in why
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_quote_body(ts="Dec 31, 2099"), "")):
        assert fetch._nasdaq_quote("SPY", ("etf",))[0] is None                                   # 日期在未來
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas_quote_body(price="N/A"), "")):
        assert fetch._nasdaq_quote("SPY", ("etf",))[0] is None
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")):
        assert "連線失敗" in fetch._nasdaq_quote("SPY", ("etf",))[1]
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>", "")):
        assert "不是 JSON" in fetch._nasdaq_quote("SPY", ("etf",))[1]
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url)
        return 200, "application/json", (json.dumps({"data": None}).encode() if "assetclass=stocks" in url else nas_quote_body()), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw):                                 # 第一個分類沒資料 → 試下一個分類
        assert fetch._nasdaq_quote("X", ("stocks", "etf"))[0]["class"] == "etf" and len(seen) == 2


def test_us_extra_quotes_stores_qqq_as_percent_only_and_falls_back_for_gold():
    reset()
    ts = (TODAY - timedelta(days=1)).strftime("%b %d, %Y").replace(" 0", " ")
    prices = {"SPY": "$745.20", "TLT": "$88.10", "IAU": "$55.30", "QQQ": "$640.00", "SOX": "6,100.55", "RUT": "2,450.10", "IWM": "$245.00"}
    def raw(url, timeout=None, headers=None):
        sym = url.split("/quote/")[1].split("/")[0]
        if sym in ("GLD", "RUT"):
            return None, "", b"", "timed out"                                                      # GLD 失敗 → 改用 IAU；RUT 失敗 → 改用 IWM
        if sym == "SOX":
            return 200, "application/json", nas_quote_body(price=prices[sym], asset="index"), ""
        return 200, "application/json", nas_quote_body(price=prices[sym]), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = REAL["us_extra_quotes"]()
    by = {r["symbol"]: r for r in out}
    assert set(by) == {"SPY", "TLT", "IAU", "QQQ", "SOX", "IWM"} and "GLD" not in by and "不是指數" in by["IWM"]["label"]
    assert by["QQQ"]["value"] is None and by["QQQ"]["change_pct"] == 0.45 and by["QQQ"]["kind"] == "etf_proxy"      # QQQ 只存漲跌幅，不存價格
    assert by["SPY"]["value"] == 745.2 and by["SOX"]["value"] == 6100.55 and by["SOX"]["kind"] == "index"
    assert "備用" in by["IAU"]["label"] and "不是黃金期貨" in by["IAU"]["label"]
    assert len({tuple(sorted(r)) for r in out}) == 1                                              # 每列欄位一致


def test_us_extra_quotes_rejects_out_of_range_and_unclosed_prices():
    reset()
    def raw(url, timeout=None, headers=None):
        sym = url.split("/quote/")[1].split("/")[0]
        if sym == "SPY": return 200, "application/json", nas_quote_body(price="$7.45"), ""            # 少一位數 → 超出範圍
        if sym == "TLT": return 200, "application/json", nas_quote_body(ms="Pre-Market", real=True), ""
        if sym == "QQQ": return 200, "application/json", nas_quote_body(pct="N/A"), ""               # 沒有漲跌幅的 QQQ 不寫
        return None, "", b"", "timed out"
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_extra_quotes"]() == []
    st = {x["source"]: x for x in fetch.LOG}
    assert st["us_quote:SPY"]["status"] == "failure" and "不在合理範圍" in st["us_quote:SPY"]["detail"]
    assert st["us_quote:TLT"]["status"] == "skipped"
    assert st["us_quote:QQQ"]["status"] == "failure" and "沒有漲跌幅" in st["us_quote:QQQ"]["detail"]


def hist_pts(n, growth, end=None, start=100.0):
    end = end or (TODAY - timedelta(days=1))
    while end.weekday() >= 5:
        end -= timedelta(days=1)
    days, d = [], end
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    days.sort()
    out, c = [], start
    for g in growth if isinstance(growth, list) else [growth] * n:
        out.append(c); c *= 1 + g
    return list(zip(days, out))


def test_sector_rs_computes_relative_strength_and_streak():
    reset()
    spy = hist_pts(30, 0.001)
    series = {"SPY": spy, "XLK": hist_pts(30, 0.003), "XLE": hist_pts(30, 0.001), "XLF": hist_pts(30, -0.001)}
    def nh(sym):
        return (series[sym], "") if sym in series else (None, "查無資料")
    with mock.patch.object(fetch, "_nasdaq_history", side_effect=nh), mock.patch.object(fetch.time, "sleep"):
        out = REAL["sector_rs"]()
    by = {r["symbol"]: r for r in out}
    assert set(by) == {"XLK", "XLE", "XLF"}                                                       # 抓不到的產業 ETF 不出現，也不編
    ret5 = lambda pts: (pts[-1][1] / pts[-6][1] - 1) * 100
    assert abs(by["XLK"]["ret_5"] - ret5(series["XLK"])) < 0.01
    assert abs(by["XLK"]["rs_5"] - (ret5(series["XLK"]) - ret5(spy))) < 0.01 and by["XLK"]["rs_5"] > 0
    assert by["XLK"]["streak_days"] == 29 and by["XLF"]["streak_days"] == 0 and by["XLF"]["rs_5"] < 0     # 一直贏 SPY / 一直輸
    assert by["XLE"]["streak_days"] == 0 and abs(by["XLE"]["rs_5"]) < 0.01                       # 和 SPY 一樣：不算贏
    assert by["XLK"]["as_of"] == series["SPY"][-1][0].isoformat()
    assert any(x["source"] == "sector_rs:XLV" and x["status"] == "failure" for x in fetch.LOG)


def test_sector_rs_rejects_stale_short_split_and_missing_benchmark():
    reset()
    spy = hist_pts(30, 0.001)
    series = {"SPY": spy, "XLK": hist_pts(30, 0.002, end=TODAY - timedelta(days=9)),             # 最新日期和 SPY 不同
              "XLF": hist_pts(10, 0.002),                                                          # 歷史太短
              "XLE": hist_pts(30, [0.0] * 14 + [1.6] + [0.0] * 15)}                                # 單日 2.6 倍：疑似分割
    with mock.patch.object(fetch, "_nasdaq_history", side_effect=lambda s: (series.get(s), "")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["sector_rs"]() == []
    st = {x["source"]: x["detail"] for x in fetch.LOG}
    assert "不同" in st["sector_rs:XLK"] and "不足" in st["sector_rs:XLF"] and "分割" in st["sector_rs:XLE"]
    reset()
    with mock.patch.object(fetch, "_nasdaq_history", return_value=(None, "連線失敗")):
        assert REAL["sector_rs"]() == []
    assert fetch.LOG[-1]["source"] == "sector_rs" and fetch.LOG[-1]["status"] == "failure"


def test_nasdaq_history_parses_rows_and_skips_bad_ones():
    reset()
    rows = [{"date": "10/29/2026", "close": "$745.20"}, {"date": "10/28/2026", "close": "743.10"}, {"date": "bad", "close": "1"}, {"date": "10/27/2026", "close": "N/A"}, {"date": "13/45/2026", "close": "5"}]
    body = json.dumps({"data": {"tradesTable": {"rows": rows}}}).encode()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body, "")):
        pts, why = fetch._nasdaq_history("SPY")
    assert pts == [(date(2026, 10, 28), 743.1), (date(2026, 10, 29), 745.2)]                      # 由舊到新
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timed out")):
        assert fetch._nasdaq_history("SPY")[0] is None
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>", "")):
        assert fetch._nasdaq_history("SPY")[1] == "不是 JSON"


def leader_hist(code, n=260, start=100.0, step=0.5):
    d0 = TODAY - timedelta(days=n + 40)
    return [{"as_of": (d0 + timedelta(days=i)).isoformat(), "close": start + i * step} for i in range(n)]


def test_tw_leaders_validates_official_names_and_computes_moving_averages():
    reset()
    names = [{"Code": c, "Name": nm} for c, (nm, _) in config.TW_LEADERS.items()]
    names[1]["Name"] = "別家公司"                                                                   # 2317 的官方名稱對不上
    hist = {c: leader_hist(c) for c in config.TW_LEADERS}
    hist["2454"] = leader_hist("2454", n=40)                                                       # 歷史只有 40 天：只有 20 日均線
    hist["3711"] = leader_hist("3711", n=10)                                                       # 太短：不算
    def req(method, path, body=None, prefer=None):
        code = path.split("code=eq.")[1].split("&")[0]
        return hist[code], ""
    with mock.patch.object(fetch, "fetch_json", return_value=(names, "")), mock.patch.object(fetch, "sb_request", side_effect=req):
        out = REAL["tw_leaders_compute"]()
    by = {r["code"]: r for r in out}
    assert set(by) == {"2330", "2454", "2308", "2383"}                                             # 2317 名稱不符、3711 歷史太短
    closes = [h["close"] for h in hist["2330"]]
    assert by["2330"]["close"] == closes[-1] and by["2330"]["tier"] == 1 and by["2454"]["tier"] == 2
    assert by["2330"]["ma20"] == round(sum(closes[-20:]) / 20, 3) and by["2330"]["ma60"] == round(sum(closes[-60:]) / 60, 3) and by["2330"]["ma200"] == round(sum(closes[-200:]) / 200, 3)
    assert by["2330"]["ret5"] == round((closes[-1] / closes[-6] - 1) * 100, 3)
    assert by["2454"]["ma20"] is not None and by["2454"]["ma60"] is None and by["2454"]["ma200"] is None   # 不夠的均線留空，不硬算
    assert by["2330"]["as_of"] == hist["2330"][-1]["as_of"]
    st = {x["source"]: x["detail"] for x in fetch.LOG}
    assert "別家公司" in st["tw_leaders:2317"] and "不足 21 天" in st["tw_leaders:3711"]
    assert len({tuple(sorted(r)) for r in out}) == 1


def test_tw_leaders_without_name_table_computes_nothing():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch, "sb_request") as req:
        assert REAL["tw_leaders_compute"]() == []
    req.assert_not_called()                                                                       # 核對不了名稱就連資料庫都不查
    assert fetch.LOG[-1]["status"] == "failure" and "無法核對" in fetch.LOG[-1]["detail"]


def test_fred_history_rows_uses_own_dates_and_skips_holidays():
    reset()
    obs = {"observations": [{"date": D(5), "value": "."}, {"date": D(4), "value": "6000.5"}, {"date": D(3), "value": "6010"}, {"date": D(2), "value": "6020"}, {"date": "2999-01-01", "value": "6030"}]}
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(obs, "")) as fj:
        rows = fetch.fred_history_rows("SP500", 500, 30000)
    assert [r["as_of"] for r in rows] == [D(4), D(3), D(2)] and rows[0]["value"] == 6000.5 and rows[0]["series_id"] == "SP500" and rows[0]["source"] == "FRED"
    assert "observation_start=" in fj.call_args[0][0] and "sort_order=asc" in fj.call_args[0][0]
    bad = {"observations": [{"date": D(4), "value": "6000"}, {"date": D(3), "value": "9999999"}]}
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(bad, "")):
        assert len(fetch.fred_history_rows("SP500", 500, 30000)) == 1
    assert any("超出合理範圍" in x["detail"] for x in fetch.LOG)
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 500")):
        assert fetch.fred_history_rows("SP500", 500, 30000) is None
    with mock.patch.object(config, "fred_key", return_value=""):
        assert fetch.fred_history_rows("SP500", 500, 30000) is None


def test_fred_history_backfill_collects_all_configured_series():
    reset()
    def fj(url, **k):                                                            # 每個序列要給它合理範圍內的數字（VIX 不可能是 6000）
        sid = url.split("series_id=")[1].split("&")[0]
        return {"observations": [{"date": D(3), "value": "20" if sid in ("VIXCLS", "VXNCLS") else "120" if sid == "DTWEXBGS" else "6000"}]}, ""
    with with_key(), mock.patch.object(fetch, "fetch_json", side_effect=fj):
        rows = REAL["fred_history_backfill"]()
    assert {r["series_id"] for r in rows} == set(config.FRED_HISTORY) == {"SP500", "NASDAQCOM", "DJIA", "VIXCLS", "VXNCLS", "DTWEXBGS"}
    assert [r["value"] for r in rows if r["series_id"] == "VIXCLS"] == [20.0]
    reset()
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=({"observations": [{"date": D(3), "value": "6000"}]}, "")):      # 用錯單位的 VIX 要被拒收，不能被寫進去
        rows = REAL["fred_history_backfill"]()
    assert "VIXCLS" not in {r["series_id"] for r in rows}


def test_tw_index_history_walks_months_back_and_stops_on_error():
    reset()
    calls = []
    def mrows(url):
        calls.append(url)
        ym = url.split("date=")[1][:6]
        return [{"market": "TW", "as_of": f"{ym[:4]}-{ym[4:]}-05", "close": 22000.0, "change_pct": 0.1, "turnover": 5e11, "source": "TWSE FMTQIK"}], ""
    with mock.patch.object(fetch, "_twse_month_rows", side_effect=mrows), mock.patch.object(fetch.time, "sleep") as sl:
        out = REAL["tw_index_history"]()
    assert len(out) == config.TW_INDEX_HISTORY_MONTHS == len(calls) == sl.call_count
    months = [u.split("date=")[1][:6] for u in calls]
    assert months[0] == TODAY.strftime("%Y%m") and len(set(months)) == len(months)               # 從本月往前，每月一次，沒有重複
    assert all(r["source"] == "TWSE FMTQIK" for r in out)
    reset()
    state = {"n": 0}
    def flaky(url):
        state["n"] += 1
        return (None, "HTTP 403") if state["n"] == 3 else ([{"market": "TW", "as_of": "2026-01-05", "close": 1.0, "change_pct": 0, "turnover": 1, "source": "TWSE FMTQIK"}], "")
    with mock.patch.object(fetch, "_twse_month_rows", side_effect=flaky), mock.patch.object(fetch.time, "sleep"):
        out = REAL["tw_index_history"]()
    assert len(out) == 2 and state["n"] == 3 and fetch.LOG[-1]["status"] == "failure" and "已停止" in fetch.LOG[-1]["detail"]    # 出錯就停，不繼續打請求


def test_persist_extra_tables_are_independent_chunked_and_dry_run_safe():
    reset()
    ups = []
    def up(table, rows, key):
        ups.append((table, len(rows), key))
        return (False, "boom") if table == "sector_rs" else (True, "ok")
    big = [{"symbol": str(i), "as_of": "x"} for i in range(1100)]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", side_effect=up):
        fetch.persist_extra(False, [("us_quotes", [{"a": 1}], "symbol,as_of"), ("sector_rs", [{"a": 1}], "symbol,as_of"), ("tw_leaders", [{"a": 1}], "as_of,code"), ("macro_series", big, "series_id,as_of"), ("empty", [], "k")])
    assert [(u[0], u[1]) for u in ups] == [("us_quotes", 1), ("sector_rs", 1), ("tw_leaders", 1), ("macro_series", 500), ("macro_series", 500), ("macro_series", 100)]   # 一張失敗不影響後面的表；大表分批
    st = {x["source"]: x["status"] for x in fetch.LOG}
    assert st["persist:sector_rs"] == "failure" and st["persist:us_quotes"] == "success" and st["persist:tw_leaders"] == "success" and "persist:empty" not in st
    ups.clear(); reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_upsert", side_effect=up):
        fetch.persist_extra(True, [("us_quotes", [{"a": 1}], "k")])
    assert ups == [] and fetch.LOG[-1]["status"] == "skipped"


def test_run_job_wires_modules_per_job_and_isolates_a_crashing_module():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
              mock.patch.object(fetch, "tw_backfill", return_value={"new_days": []}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"), mock.patch.object(fetch, "persist_extra")]
    for p_ in patches: p_.start()
    try:
        quotes = [{"symbol": "SPY"}]
        def boom(): raise RuntimeError("壞掉了")
        with mock.patch.object(fetch, "us_extra_quotes", return_value=quotes), mock.patch.object(fetch, "sector_rs", side_effect=boom), \
                mock.patch.object(fetch, "tw_leaders_compute", return_value=[{"code": "2330"}]):
            fetch.run_job("tw_close", dry_run=False)
            args = fetch.persist_extra.call_args[0]
            tables = {t: r for t, r, c in args[1]}
            assert tables["us_quotes"] == quotes and tables["sector_rs"] == [] and tables["tw_leaders"] == [{"code": "2330"}]    # M03 壞了，M02、M04 照常
            st = {x["source"]: x["status"] for x in fetch.LOG}
            assert st["module:M02"] == "success" and st["module:M03"] == "failure" and st["module:M04"] == "success"
            fetch.persist_extra.reset_mock()
            fetch.run_job("tw_close", dry_run=True)
            assert "tw_leaders" not in {t for t, r, c in fetch.persist_extra.call_args[0][1]}                                   # dry-run 不算領頭股（會讀資料庫）
        with mock.patch.object(fetch, "tw_index_history", return_value=[{"market": "TW"}]) as ih, mock.patch.object(fetch, "fred_history_backfill", return_value=[{"series_id": "SP500"}]) as fh, \
                mock.patch.object(fetch, "tw_leaders_compute", return_value=[]):
            fetch.persist_extra.reset_mock()
            fetch.run_job("tw_backfill", dry_run=False)
            ih.assert_called_once(); fh.assert_called_once()
            keys = {t: c for t, r, c in fetch.persist_extra.call_args[0][1]}
            assert keys["market_stats"] == "market,as_of,source" and keys["macro_series"] == "series_id,as_of"
            ih.reset_mock(); fh.reset_mock()
            fetch.run_job("tw_backfill", dry_run=True)
            ih.assert_not_called(); fh.assert_not_called()                                                                         # dry-run 不打這些請求
    finally:
        for p_ in patches: p_.stop()


def test_probe_round3_logs_four_sources_without_writing():
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap06_L_ci": {"get": {"summary": "上市公司綜合損益表(一般業)"}}, "/x": {"get": {"summary": "其他"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap06_O_ciA": {"get": {"summary": "上櫃公司財報資訊( 一般業)"}}}}
    fin = [{"公司代號": "2330", "營業收入": "1", "營業毛利": "1", "營業利益": "1", "稅後淨利": "1"}]
    def fj(url, **k):
        if "twse" in url and url.endswith("swagger.json"): return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        return fin, ""
    aaii = b"<html><body><div>Bullish 38.5% Neutral 30% Bearish 31.5%</div></body></html>"
    n100 = json.dumps({"data": {"data": {"rows": [{"symbol": "AAPL", "pctChange": "-1.2%"}]}, "date": "Oct 30, 2026"}}).encode()
    sox = json.dumps({"data": {"marketStatus": "Closed", "primaryData": {"lastSalePrice": "6,100.55", "lastTradeTimestamp": "Oct 30, 2026"}}}).encode()
    def raw(url, timeout=None, headers=None):
        if "aaii" in url: return 200, "text/html", aaii, ""
        if "nasdaq100" in url: return 200, "application/json", n100, ""
        return 200, "application/json", sox, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round3()
    by = {x["source"]: x for x in fetch.LOG}
    assert "營業毛利" in by["probe:fin_rows:twse"]["detail"] and "營業毛利" in by["probe:fin_rows:tpex"]["detail"] and "/x" not in by["probe:fin:twse"]["detail"]
    assert by["probe:aaii"]["status"] == "success" and "Bullish 38.5%" in by["probe:aaii"]["detail"]
    assert by["probe:nasdaq100"]["status"] == "success" and "AAPL" in by["probe:nasdaq100"]["detail"]
    assert by["probe:sox"]["status"] == "success" and "6,100.55" in by["probe:sox"]["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_round3_failures_are_reported_not_hidden():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"denied", "HTTP 403")):
        fetch.probe_round3()
    sts = {x["source"]: x["status"] for x in fetch.LOG}
    assert sts["probe:fin:twse"] == "failure" and sts["probe:aaii"] == "failure" and sts["probe:nasdaq100"] == "failure" and sts["probe:sox"] == "failure"


# ================================================================ v32 模組：M07 三率、M08 美股暴跌、M09 AAII、M10 美股領頭股、M11 法說會
def fin_row(code, name, y="115", q="2", rev="1,000,000", gp="500,000", op="300,000", ni="250,000"):
    return {"出表日期": "1150815", "年度": y, "季別": q, "公司代號": code, "公司名稱": name, "營業收入": rev, "營業成本": "500,000", "營業毛利（毛損）": gp,
            "未實現銷貨（損）益": "0", "營業毛利（毛損）淨額": "999", "營業利益（損失）": op, "本期淨利（淨損）": ni, "歸屬於母公司業主之本期淨利（淨損）": "1"}


def test_fin_rows_compute_three_margins_and_skip_bad_rows():
    wanted = {"2330": "台積電", "2454": "聯發科", "3711": "日月光投控"}
    rows = [fin_row("2330", "台積電"), fin_row("2454", "聯發科", rev="0"), fin_row("9999", "不在持股"), fin_row("3711", "日月光投控", y="2999"),
            fin_row("2330", "台積電", y="114", q="第四季", gp="400,000", op="200,000", ni="150,000"), fin_row("2454", "聯發科", gp="x")]
    out, keys = fetch._fin_rows(rows, wanted, "測試")
    by = {(r["code"], r["year"], r["quarter"]): r for r in out}
    assert set(by) == {("2330", 2026, 2), ("2330", 2025, 4)}                                       # 民國年轉西元；季別「第四季」→ 4；營收 0、未來年度、壞數字、不在持股的都不收
    r = by[("2330", 2026, 2)]
    assert (r["gross_margin"], r["op_margin"], r["net_margin"]) == (50.0, 30.0, 25.0) and r["name"] == "台積電"      # 毛利取「營業毛利（毛損）」不是「淨額」；淨利取合併本期淨利不是歸屬母公司
    assert by[("2330", 2025, 4)]["gross_margin"] == 40.0
    bad = [{k: v for k, v in fin_row("2330", "台積電").items() if "毛利" not in k}]
    assert fetch._fin_rows(bad, wanted, "x")[0] is None and "公司代號" in fetch._fin_rows(bad, wanted, "x")[1]       # 缺欄位：不猜，回傳欄位清單
    assert fetch._fin_rows([], wanted, "x") == (None, []) and fetch._fin_rows("x", wanted, "x") == (None, [])
    out, _ = fetch._fin_rows([fin_row("2330", "台積電", gp="2,000,000")], wanted, "x"); assert out == []              # 毛利率超過 100%：拒收


def test_tw_financials_reads_both_exchanges_and_reports_missing_holdings():
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap06_L_ci": {"get": {"summary": "上市公司綜合損益表(一般業)"}}, "/opendata/t187ap06_L_basi": {"get": {"summary": "上市公司綜合損益表(金融業)"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap06_O_ciA": {"get": {"summary": "上櫃公司財報資訊( 一般業)"}}}}
    urls = []
    def fj(url, **k):
        urls.append(url)
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap06_L_ci"): return [fin_row("2330", "台積電")], ""
        if url.endswith("mopsfin_t187ap06_O_ciA"): return [fin_row("8358", "金居", gp="300,000", op="100,000", ni="80,000")], ""
        raise AssertionError(url)
    prices = [{"market": "TW", "code": "2330", "name": "台積電"}, {"market": "TW", "code": "8358", "name": "金居"}, {"market": "TW", "code": "2884", "name": "玉山金"}, {"market": "US", "code": "ORCL", "name": "x"}]
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = REAL["tw_financials"](prices)
    assert sorted(r["code"] for r in out) == ["2330", "8358"] and not any("basi" in u for u in urls)                      # 一般業端點，不用金融業那個
    assert {x["source"]: x["status"] for x in fetch.LOG}["tw_fin:twse"] == "success" and "2026 年第 2 季" in [x for x in fetch.LOG if x["source"] == "tw_fin:twse"][0]["detail"]
    reset()
    assert REAL["tw_financials"]([{"market": "US", "code": "ORCL"}]) == [] and fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        assert REAL["tw_financials"](prices) == []
    assert {x["source"]: x["status"] for x in fetch.LOG}["tw_fin:twse"] == "failure" and {x["source"]: x["status"] for x in fetch.LOG}["tw_fin:tpex"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (sw, "") if "twse" in u and u.endswith("swagger.json") else (sw2, "") if "swagger" in u else ([{"x": 1}], "")):
        assert REAL["tw_financials"](prices) == []
    assert "欄位 ['x']" in [x for x in fetch.LOG if x["source"] == "tw_fin:twse"][0]["detail"]                            # 欄位對不上要把欄位清單記下來，我才知道怎麼改


def test_tw_financials_logs_coverage_so_missing_holdings_can_be_explained():
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap06_L_ci": {"get": {"summary": "上市公司綜合損益表(一般業)"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap06_O_ciA": {"get": {"summary": "上櫃公司財報資訊( 一般業)"}}}}
    bad = fin_row("2454", "聯發科"); bad["營業毛利（毛損）"] = ""                                             # 在資料集，但毛利是空的 → 被略過
    def fj(url, **k):
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap06_L_ci"): return [fin_row("2383", "台光電"), bad, fin_row("1101", "台泥")], ""      # 台積電根本不在資料集
        if url.endswith("mopsfin_t187ap06_O_ciA"): return [fin_row("8358", "金居")], ""
        raise AssertionError(url)
    prices = [{"market": "TW", "code": c, "name": n} for c, n in (("2383", "台光電"), ("2330", "台積電"), ("2454", "聯發科"), ("8358", "金居"))]
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = REAL["tw_financials"](prices)
    assert sorted(r["code"] for r in out) == ["2383", "8358"]
    cov = [x for x in fetch.LOG if x["source"] == "tw_fin:coverage"][0]
    assert cov["status"] == "no_new_data" and "持股 4 檔：有三率 2 檔" in cov["detail"]
    assert "不在任何資料集：2330 台積電" in cov["detail"] and "在資料集但欄位缺值被略過：2454 聯發科" in cov["detail"], cov["detail"]
    assert "twse：資料集 3 列" in cov["detail"] and "tpex：資料集 1 列" in cov["detail"] and "年第" in cov["detail"]
    reset()                                                                                                     # 全部都有 → success、寫「無」
    prices2 = [{"market": "TW", "code": "2383", "name": "台光電"}, {"market": "TW", "code": "8358", "name": "金居"}]
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        REAL["tw_financials"](prices2)
    cov = [x for x in fetch.LOG if x["source"] == "tw_fin:coverage"][0]
    assert cov["status"] == "success" and "不在任何資料集：無" in cov["detail"] and "被略過：無" in cov["detail"]


def nas100_body(rows):
    return json.dumps({"data": {"data": {"rows": rows}}}).encode()


def test_us_crash_screen_confirms_each_candidate_with_a_dated_closed_quote():
    reset()
    lst = [{"symbol": "AAA", "companyName": "A Inc", "percentageChange": "-6.10%"}, {"symbol": "BBB", "companyName": "B", "percentageChange": "-2.00%"},
           {"symbol": "CCC", "companyName": "C", "percentageChange": "-8.00%"}, {"symbol": "DDD", "companyName": "D", "percentageChange": "-7.00%"}]
    def raw(url, timeout=None, headers=None):
        if "list-type/nasdaq100" in url: return 200, "application/json", nas100_body(lst), ""
        sym = url.split("/quote/")[1].split("/")[0]
        if sym == "AAA": return 200, "application/json", nas_quote_body(price="$93.90", pct="-6.10%", asset="stocks"), ""
        if sym == "CCC": return 200, "application/json", nas_quote_body(ms="Open", real=True), ""            # 還沒收盤：不採用
        if sym == "DDD": return 200, "application/json", nas_quote_body(price="$97.00", pct="-3.00%"), ""      # 名單和個股報價對不上：不加入
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = REAL["us_crash_screen"]()
    assert [r["symbol"] for r in out] == ["AAA"] and out[0]["drop_pct"] == -6.1 and out[0]["prev_close"] == 100.0 and out[0]["close"] == 93.9
    assert out[0]["as_of"] == (TODAY - timedelta(days=1)).isoformat() and "只涵蓋 Nasdaq-100" in out[0]["source"] and "原因待確認" in out[0]["note"]
    st = {x["source"]: x["status"] for x in fetch.LOG}
    assert st["us_crash:CCC"] == "skipped" and st["us_crash:DDD"] == "no_new_data" and st["us_crash:AAA"] == "success" and "us_crash:BBB" not in st and "初篩跌幅達 5.0% 的有 3 檔" in [x for x in fetch.LOG if x["source"] == "us_crash:list"][0]["detail"]
    for ret in ((None, "", b"", "timed out"), (200, "application/json", nas100_body([]), ""), (200, "text/html", b"<html>", "")):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=ret):
            try:
                REAL["us_crash_screen"](); raise AssertionError("名單抓不到應該要讓模組失敗")
            except RuntimeError:
                pass
        assert fetch.LOG[-1]["source"] == "us_crash:list" and fetch.LOG[-1]["status"] == "failure"
        reset()
        with mock.patch.object(fetch, "us_crash_screen", side_effect=lambda: REAL["us_crash_screen"]()), mock.patch.object(fetch, "fetch_raw", return_value=ret):
            assert fetch._run_module("M08", "美股大型股暴跌", fetch.us_crash_screen) is None
        assert {x["source"]: x["status"] for x in fetch.LOG}["module:M08"] == "failure"                            # 名單抓不到＝模組失敗，不是「沒有資料」
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", nas100_body([{"symbol": "AAA", "companyName": "A", "percentageChange": "-1.00%"}]), "")):
        assert REAL["us_crash_screen"]() == []                                                                       # 名單正常但沒人跌超過門檻：回傳空，這是正常狀態
    assert fetch.LOG[-1]["status"] == "success" and "初篩跌幅達 5.0% 的有 0 檔" in fetch.LOG[-1]["detail"]


def aaii_page(date_text, b="38.5", n="30.2", br="31.3"):
    return f"<html><body><h1>Sentiment Survey</h1><p>Results for the week ending {date_text}</p><table><tr><td>Bullish</td><td>{b}%</td></tr><tr><td>Neutral</td><td>{n}%</td></tr><tr><td>Bearish</td><td>{br}%</td></tr></table></body></html>".encode()


def test_aaii_requires_three_ratios_that_add_up_and_its_own_recent_date():
    reset()
    d = TODAY - timedelta(days=2)
    fmt = lambda x, year=True: x.strftime("%b ") + str(x.day) + (", " + str(x.year) if year else "")
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", aaii_page(fmt(d)), "")):
        out = REAL["aaii_sentiment"]()
    assert out == [{"as_of": d.isoformat(), "bullish": 38.5, "neutral": 30.2, "bearish": 31.3, "source": "AAII 散戶情緒調查（aaii.com）"}]
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", aaii_page(fmt(d, year=False)), "")):
        assert REAL["aaii_sentiment"]()[0]["as_of"] == d.isoformat()                                           # 沒寫年份：取不晚於今天的那一年
    for body, why in ((aaii_page(fmt(d), b="60", n="30", br="30"), "合計"), (aaii_page(fmt(TODAY - timedelta(days=40))), "距今"), (b"<html>Bullish 38.5% Neutral 30% Bearish 31.5%</html>", "日期"),
                      (aaii_page(fmt(d)).replace(b"Bearish", b"Other"), "比例"), (aaii_page("Foo 99"), "日期")):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", body, "")):
            assert REAL["aaii_sentiment"]() == []
        assert fetch.LOG[-1]["status"] == "failure" and why in fetch.LOG[-1]["detail"], (why, fetch.LOG[-1]["detail"])
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"denied", "HTTP 403")):
        assert REAL["aaii_sentiment"]() == []
    assert "HTTP 403" in fetch.LOG[-1]["detail"]


def _aaii_hist_html(rows, extra=""):
    body = "".join(f"<tr><td>{d.strftime('%b')} {d.day}</td><td>{b}%</td><td>{n}%</td><td>{r}%</td></tr>" for d, b, n, r in rows)
    return ("<html><body><table><tr><td>Historical Average</td><td>37.5%</td><td>31.5%</td><td>31.0%</td></tr></table><table><tr><th>Reported Date</th><th>Bullish</th><th>Neutral</th><th>Bearish</th></tr>" + body + "</table>" + extra + "</body></html>")


def test_parse_aaii_history_reads_the_weekly_table_infers_years_and_stops_at_doubtful_rows():
    real = ("Bullish Neutral Bearish Nov 4 34.1% 17.5% 48.4% Oct 28 37.8% 20.7% 41.4% Oct 21 36.9% 17.3% 45.8% Oct 14 38.7% 19.7% 41.6% Oct 7 40.3% 20.8% 39.0% Sep 30 34.6% 25.0% 40.4%")
    rows, why = fetch.parse_aaii_history(real, date(2026, 11, 8))                                       # 你的探測結果裡的真實文字
    assert why == "" and [(r["as_of"], r["bullish"], r["neutral"], r["bearish"]) for r in rows] == [("2026-11-04", 34.1, 17.5, 48.4), ("2026-10-28", 37.8, 20.7, 41.4), ("2026-10-21", 36.9, 17.3, 45.8), ("2026-10-14", 38.7, 19.7, 41.6), ("2026-10-07", 40.3, 20.8, 39.0), ("2026-09-30", 34.6, 25.0, 40.4)]
    assert "每週歷史表" in rows[0]["source"]
    wrap = "Bullish Neutral Bearish Jan 7 35.0% 30.0% 35.0% Dec 31 36.0% 30.0% 34.0% Dec 24 33.0% 33.0% 34.0% Dec 17 31.0% 34.0% 35.0%"
    assert [r["as_of"] for r in fetch.parse_aaii_history(wrap, date(2026, 1, 10))[0]] == ["2026-01-07", "2025-12-31", "2025-12-24", "2025-12-17"], "跨年：月日變大就減一年"
    assert [r["as_of"] for r in fetch.parse_aaii_history("Bullish Neutral Bearish Dec 31 36.0% 30.0% 34.0% Dec 24 33.0% 33.0% 34.0% Dec 17 31.0% 34.0% 35.0%", date(2026, 1, 3))[0]] == ["2025-12-31", "2025-12-24", "2025-12-17"], "最新一列比今天還晚：算去年"
    bad_sum = "Bullish Neutral Bearish Nov 4 34.1% 17.5% 48.4% Oct 28 37.8% 20.7% 41.4% Oct 21 50.0% 20.0% 20.0% Oct 14 38.7% 19.7% 41.6%"
    r2, w2 = fetch.parse_aaii_history(bad_sum, date(2026, 11, 8)); assert r2 == [] and "只讀到 2 週" in w2, "合計不對的那一列：從那裡停（後面的也不要）"
    gap = "Bullish Neutral Bearish Nov 4 34.1% 17.5% 48.4% Oct 28 37.8% 20.7% 41.4% Oct 14 38.7% 19.7% 41.6% Oct 7 40.3% 20.8% 39.0%"
    assert fetch.parse_aaii_history(gap, date(2026, 11, 8))[0] == [], "少一週（差 14 天）：年份推算不可靠，從那裡停；只剩 2 列所以整個不用"
    assert fetch.parse_aaii_history("沒有表格", date(2026, 11, 8))[0] == [] and fetch.parse_aaii_history("", date(2026, 11, 8))[1].startswith("歷史表只讀到 0 週")
    html, _ = fetch.parse_aaii_history(_aaii_hist_html([(date(2026, 11, 4) - timedelta(days=7 * k), 34.1, 17.5, 48.4) for k in range(4)]), date(2026, 11, 8)); assert len(html) == 4, "HTML 標籤和『歷史平均』那列（沒有日期）不影響"


def test_aaii_sentiment_stores_the_history_only_when_two_pages_agree_on_the_latest_week():
    d = TODAY - timedelta(days=2)
    fmt = d.strftime("%b ") + str(d.day) + ", " + str(d.year)
    land = aaii_page(fmt)                                                                                         # 38.5 / 30.2 / 31.3
    hist = [(d - timedelta(days=7 * k), 38.5 if k == 0 else 36.0, 30.2 if k == 0 else 32.0, 31.3 if k == 0 else 32.0) for k in range(6)]
    def run(results):
        def raw(url, timeout=None, headers=None):
            return (200, "text/html", land, "") if url == config.AAII_URL else results
        reset()
        with mock.patch.object(fetch, "fetch_raw", side_effect=raw):
            return REAL["aaii_sentiment"](), [x for x in fetch.LOG if x["source"] == "aaii"][-1]
    out, lg = run((200, "text/html", _aaii_hist_html(hist).encode(), ""))
    assert len(out) == 6 and out[0]["as_of"] == d.isoformat() and out[-1]["as_of"] == (d - timedelta(days=35)).isoformat() and "每週歷史表" in out[0]["source"]
    assert lg["status"] == "success" and "歷史表 6 週" in lg["detail"] and "核對一致" in lg["detail"], lg["detail"]
    off = [(hist[0][0], 40.0, 28.7, 31.3)] + hist[1:]                                                              # 最新一週的數字和另一個頁面對不上
    out, lg = run((200, "text/html", _aaii_hist_html(off).encode(), ""))
    assert len(out) == 1 and out[0]["source"] == "AAII 散戶情緒調查（aaii.com）" and "歷史表沒採用" in lg["detail"] and "對不上" in lg["detail"], lg["detail"]
    old = [(d - timedelta(days=7), 38.5, 30.2, 31.3)] + [(d - timedelta(days=14 + 7 * k), 36.0, 32.0, 32.0) for k in range(4)]            # 歷史表最新一列不是同一週
    out, lg = run((200, "text/html", _aaii_hist_html(old).encode(), ""))
    assert len(out) == 1 and "對不上" in lg["detail"]
    out, lg = run((403, "text/html", b"denied", "HTTP 403")); assert len(out) == 1 and "結果頁 HTTP 403" in lg["detail"] and lg["status"] == "success", "結果頁抓不到：只存最新一週，不報錯"
    out, lg = run((200, "text/html", b"<html>no table</html>", "")); assert len(out) == 1 and "至少 3 週" in lg["detail"]


def test_us_leaders_compute_ma_relative_to_spy_and_rejects_mismatch_or_split():
    reset()
    spy = hist_pts(70, 0.001)
    series = {"SPY": spy, "NVDA": hist_pts(70, 0.004), "AAPL": hist_pts(70, 0.001), "MSFT": hist_pts(70, -0.002),
              "TSLA": hist_pts(70, 0.002, end=TODAY - timedelta(days=12)), "META": hist_pts(10, 0.001), "AMZN": hist_pts(70, [0.0] * 30 + [1.6] + [0.0] * 39)}
    calls = []
    def nh(sym, assetclass="etf", days=75):
        calls.append((sym, assetclass, days)); return (series.get(sym), "查無資料")
    with mock.patch.object(fetch, "_nasdaq_history", side_effect=nh), mock.patch.object(fetch.time, "sleep"):
        out = REAL["us_leaders_compute"]()
    by = {r["symbol"]: r for r in out}
    assert set(by) == {"NVDA", "AAPL", "MSFT"}                                                              # 日期對不上、歷史太短、疑似分割、抓不到的都不出現
    assert ("NVDA", "stocks", config.US_LEADER_HISTORY_DAYS) in calls and ("SPY", "etf", config.US_LEADER_HISTORY_DAYS) in calls
    c = [p[1] for p in series["NVDA"]]
    assert by["NVDA"]["close"] == c[-1] and by["NVDA"]["ma20"] == round(sum(c[-20:]) / 20, 3) and by["NVDA"]["ma60"] == round(sum(c[-60:]) / 60, 3)
    spy5 = (spy[-1][1] / spy[-6][1] - 1) * 100
    assert abs(by["NVDA"]["rel5"] - ((c[-1] / c[-6] - 1) * 100 - spy5)) < 0.001 and by["NVDA"]["rel5"] > 0 and by["MSFT"]["rel5"] < 0 and abs(by["AAPL"]["rel5"]) < 0.01
    assert by["NVDA"]["name"] == "輝達" and len({tuple(sorted(r)) for r in out}) == 1
    st = {x["source"]: x["detail"] for x in fetch.LOG}
    assert "不同" in st["us_leaders:TSLA"] and "不足" in st["us_leaders:META"] and "分割" in st["us_leaders:AMZN"] and "us_leaders:AVGO" in st
    reset()
    with mock.patch.object(fetch, "_nasdaq_history", return_value=(None, "連線失敗")):
        assert REAL["us_leaders_compute"]() == []
    assert fetch.LOG[-1]["source"] == "us_leaders" and fetch.LOG[-1]["status"] == "failure"


def roc_text(d):
    return f"{d.year - 1911}/{d.month}/{d.day}"


def test_tw_investor_events_picks_only_held_companies_with_one_clear_date():
    reset()
    ev = TODAY + timedelta(days=10)
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap04_L": {"get": {"summary": "上市公司每日重大訊息"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap04_O": {"get": {"summary": "上櫃公司每日重大訊息"}}}}
    ann = f"{TODAY.year - 1911}{TODAY.month:02d}{TODAY.day:02d}"
    news = [{"發言日期": ann, "公司代號": "2330", "公司名稱": "台積電", "主旨": "公告本公司召開法人說明會", "說明": f"1.召開法人說明會之日期:{roc_text(ev)} 2.召開地點:台北"},
            {"發言日期": ann, "公司代號": "2330", "公司名稱": "台積電", "主旨": "法人說明會更正", "說明": f"原訂 {TODAY.year - 1911}年{TODAY.month}月{TODAY.day}日 改為 {ev.year - 1911}年{ev.month}月{ev.day}日 召開法人說明會"},
            {"發言日期": ann, "公司代號": "1101", "公司名稱": "台泥", "主旨": "公告召開法人說明會", "說明": f"召開法人說明會之日期:{roc_text(ev)}"},
            {"發言日期": ann, "公司代號": "2454", "公司名稱": "聯發科", "主旨": "公告董事會決議", "說明": "董事會通過股利"},
            {"發言日期": ann, "公司代號": "2454", "公司名稱": "聯發科", "主旨": "法人說明會", "說明": f"召開法人說明會之日期:{roc_text(TODAY - timedelta(days=30))}"}]
    def fj(url, **k):
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap04_L"): return news, ""
        return [], ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = REAL["tw_investor_events"]([{"market": "TW", "code": "2330", "name": "台積電"}, {"market": "TW", "code": "2454", "name": "聯發科"}])
    assert [r["code"] for r in out] == ["2330"] and out[0]["event_date"] == ev.isoformat() and out[0]["announced_at"] == TODAY.isoformat() and "召開法人說明會" in out[0]["subject"]
    st = [x for x in fetch.LOG if x["source"] == "tw_events:2330"]
    assert st and st[0]["status"] == "no_new_data" and "沒有唯一的日期" in st[0]["detail"]                      # 同一則裡有兩個日期：不猜
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        assert REAL["tw_investor_events"]([{"market": "TW", "code": "2330", "name": "台積電"}]) == []
    assert {x["source"]: x["status"] for x in fetch.LOG}["tw_events:twse"] == "failure"
    reset()
    assert REAL["tw_investor_events"]([]) == [] and fetch.LOG[-1]["status"] == "skipped"


def test_tw_events_reads_real_formats_otc_english_keys_and_fact_date_fallback():
    reset()
    ev = TODAY + timedelta(days=12)
    fact = f"{ev.year - 1911}{ev.month:02d}{ev.day:02d}"
    ann = f"{TODAY.year - 1911}{TODAY.month:02d}{TODAY.day:02d}"
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap04_L": {"get": {"summary": "上市公司每日重大訊息"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap04_O": {"get": {"summary": "上櫃公司每日重大訊息"}}}}
    twse = [  # 上市：欄位名稱「主旨 」後面有空白（真實回傳就是這樣）；受邀參加型，日期只在「事實發生日」
        {"出表日期": ann, "發言日期": ann, "發言時間": "140235", "公司代號": "2330", "公司名稱": "台積電", "主旨 ": "本公司受邀參加凱基證券公司所舉辦之上市公司法人說明會，說明本公司營運展望與經營績效", "符合條款": "第12款", "事實發生日": fact, "說明": "1.事實發生日:民國 2.公司名稱:台積電"},
        {"出表日期": ann, "發言日期": ann, "發言時間": "140300", "公司代號": "1101", "公司名稱": "台泥", "主旨 ": "公告本公司受邀參加法人說明會", "符合條款": "第12款", "事實發生日": fact, "說明": "x"},      # 不是持股
        {"出表日期": ann, "發言日期": ann, "發言時間": "140400", "公司代號": "2454", "公司名稱": "聯發科", "主旨 ": "公告本公司取消法人說明會", "符合條款": "第12款", "事實發生日": fact, "說明": "因故取消"}]    # 取消
    tpex = [  # 上櫃：欄位是英文（SecuritiesCompanyCode、CompanyName）；說明裡標明日期（句型「召開法人說明會日期」沒有「之」）
        {"Date": ann, "發言日期": ann, "發言時間": "152221", "SecuritiesCompanyCode": "8358", "CompanyName": "金居", "主旨": "本公司受邀參加富邦證券舉辦之法人說明會", "符合條款": "第12款", "事實發生日": ann, "說明": f"1.召開法人說明會日期：{ev.year - 1911}/{ev.month}/{ev.day} 2.地點：線上"},
        {"Date": ann, "發言日期": ann, "發言時間": "152300", "SecuritiesCompanyCode": "6488", "CompanyName": "環球晶", "主旨": "公告受邀參加法人說明會", "符合條款": "第12款", "事實發生日": f"{TODAY.year - 1911 - 1}0101", "說明": "x"},         # 事實發生日在過去 → 不採用
        {"Date": ann, "發言日期": ann, "發言時間": "152400", "SecuritiesCompanyCode": "3105", "CompanyName": "穩懋", "主旨": "公告法人說明會", "符合條款": "第12款", "事實發生日": "", "說明": "x"}]                                    # 完全沒有日期 → 不採用
    def fj(url, **k):
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap04_L"): return twse, ""
        if url.endswith("t187ap04_O"): return tpex, ""
        return [], ""
    held = [{"market": "TW", "code": c, "name": n} for c, n in (("2330", "台積電"), ("2454", "聯發科"), ("8358", "金居"), ("6488", "環球晶"), ("3105", "穩懋"))]
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = REAL["tw_investor_events"](held)
    by = {r["code"]: r for r in out}
    assert sorted(by) == ["2330", "8358"], "取得日期的只有：受邀型（事實發生日）的台積電，與說明裡標明日期的上櫃金居: " + str(sorted(by))
    assert by["2330"]["event_date"] == ev.isoformat() and by["2330"]["subject"].startswith("【日期取自公告的事實發生日】") and "受邀參加" in by["2330"]["subject"]
    assert by["8358"]["event_date"] == ev.isoformat() and by["8358"]["name"] == "金居" and not by["8358"]["subject"].startswith("【"), "上櫃（英文欄位）要讀得到，且日期來自說明、不是事實發生日"
    assert by["8358"]["announced_at"] == TODAY.isoformat()
    why = {x["source"].split(":")[1]: x["detail"] for x in fetch.LOG if x["source"].startswith("tw_events:") and x["status"] == "no_new_data"}
    assert "取消、延期或改期" in why["2454"] and "不在合理範圍" in why["6488"] and "找不到日期" in why["3105"], why
    ok = {x["source"]: x["detail"] for x in fetch.LOG if x["status"] == "success"}
    assert "你的持股有 2 則提到法人說明會，其中 1 則取得日期" in ok["tw_events:twse"] and "你的持股有 3 則提到法人說明會，其中 1 則取得日期" in ok["tw_events:tpex"], ok
    reset()
    bad_keys = [{"foo": "1", "bar": "2"}]                                                                        # 欄位完全認不得：失敗並說明，不亂猜
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (sw, "") if u.endswith("swagger.json") and "twse" in u else (sw2, "") if "swagger" in u else (bad_keys, "")):
        assert REAL["tw_investor_events"](held) == []
    assert all(x["status"] == "failure" and "找不到公司代號欄" in x["detail"] for x in fetch.LOG if x["source"].startswith("tw_events:t"))


def test_run_job_wires_v32_modules_and_isolates_failures():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    prices = [{"market": "TW", "code": "2330", "name": "台積電"}]
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=([{"market": "TW", "name": "台積電", "symbol": None, "kind": "stock", "pos_key": "台積電"}], "")),
              mock.patch.object(fetch, "tpex_prices", return_value=[]), mock.patch.object(fetch, "twse_prices", return_value=prices), mock.patch.object(fetch, "nasdaq_prices", return_value=[]),
              mock.patch.object(fetch, "tw_ranges", side_effect=lambda x: x), mock.patch.object(fetch, "nasdaq_earnings", return_value=[]), mock.patch.object(fetch, "persist"),
              mock.patch.object(fetch, "persist_extra"), mock.patch.object(fetch, "tw_backfill", return_value={"new_days": []}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check")]
    for p_ in patches: p_.start()
    try:
        def boom(*a): raise RuntimeError("壞掉了")
        with mock.patch.object(fetch, "tw_financials", return_value=[{"code": "2330"}]) as fin, mock.patch.object(fetch, "us_crash_screen", side_effect=boom), \
                mock.patch.object(fetch, "aaii_sentiment", return_value=[{"as_of": "x"}]), mock.patch.object(fetch, "us_leaders_compute", return_value=[{"symbol": "NVDA"}]), \
                mock.patch.object(fetch, "tw_investor_events", return_value=[{"code": "2330"}]) as evs:
            fetch.run_job("tw_close", dry_run=False)
            fin.assert_called_once_with(prices); evs.assert_called_once_with(prices)                         # 持股代號傳給需要的模組
            tables = {t: (r, c) for t, r, c in fetch.persist_extra.call_args[0][1]}
            assert tables["tw_fin_q"] == ([{"code": "2330"}], "code,year,quarter") and tables["us_crash_candidates"][0] == [] and tables["us_aaii"][1] == "as_of"
            assert tables["us_leaders"][1] == "as_of,symbol" and tables["tw_investor_events"][1] == "code,event_date"
            st = {x["source"]: x["status"] for x in fetch.LOG}
            assert st["module:M08"] == "failure" and st["module:M07"] == "success" and st["module:M09"] == "success" and st["module:M10"] == "success" and st["module:M11"] == "success"
        old = dict(config.FEATURES)
        try:
            config.FEATURES["M09_aaii"] = False
            with mock.patch.object(fetch, "aaii_sentiment") as aa:
                fetch.run_job("tw_close", dry_run=False); aa.assert_not_called()                              # 關掉的模組完全不執行
        finally:
            config.FEATURES.clear(); config.FEATURES.update(old)
    finally:
        for p_ in patches: p_.stop()


def test_tw_financials_uses_listed_company_endpoint_never_the_public_issuer_one():
    reset()
    # 真實的證交所說明文件裡，「公發公司（_X_）」排在「上市公司（_L_）」前面；選錯的話，上市持股全部落空
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {
        "/opendata/t187ap06_X_ci": {"get": {"summary": "公發公司綜合損益表-一般業"}}, "/opendata/t187ap06_L_bd": {"get": {"summary": "上市公司綜合損益表(證券期貨業)"}},
        "/opendata/t187ap06_L_ci": {"get": {"summary": "上市公司綜合損益表(一般業)"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap06_O_ciA": {"get": {"summary": "上櫃公司財報資訊( 一般業)"}}}}
    urls = []
    def fj(url, **k):
        urls.append(url)
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap06_L_ci"): return [fin_row("2330", "台積電")], ""
        if url.endswith("t187ap06_X_ci"): return [fin_row("9999", "不是上市公司")], ""
        if url.endswith("mopsfin_t187ap06_O_ciA"): return [fin_row("8358", "金居", gp="300,000", op="100,000", ni="80,000")], ""
        raise AssertionError(url)
    prices = [{"market": "TW", "code": "2330", "name": "台積電"}, {"market": "TW", "code": "8358", "name": "金居"}]
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        out = REAL["tw_financials"](prices)
    assert sorted(r["code"] for r in out) == ["2330", "8358"], "上市的台積電必須抓得到"
    assert not any(u.endswith("_X_ci") for u in urls) and any(u.endswith("t187ap06_L_ci") for u in urls)
    reset()
    sw_only_x = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap06_X_ci": {"get": {"summary": "公發公司綜合損益表-一般業"}}}}
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (sw_only_x, "") if "twse" in u and u.endswith("swagger.json") else (sw2, "") if "swagger" in u else ([fin_row("8358", "金居")], "")):
        out = REAL["tw_financials"](prices)
    assert [r["code"] for r in out] == ["8358"], "只有公發公司端點時，上市那邊寫失敗，不拿它充數"
    assert {x["source"]: x["status"] for x in fetch.LOG}["tw_fin:twse"] == "failure"


def test_us_crash_screen_confirms_deepest_drops_first_and_says_when_it_truncates():
    reset()
    lst = [{"symbol": f"S{i:02d}", "companyName": f"公司{i}", "percentageChange": f"-{5 + i * 0.1:.2f}%"} for i in range(10)] + [{"symbol": "SMALL", "companyName": "x", "percentageChange": "-1.00%"}]
    confirmed = []
    def raw(url, timeout=None, headers=None):
        if "list-type/nasdaq100" in url: return 200, "application/json", nas100_body(lst), ""
        sym = url.split("/quote/")[1].split("/")[0]
        confirmed.append(sym)
        i = int(sym[1:])
        return 200, "application/json", nas_quote_body(price="$90.00", pct=f"-{5 + i * 0.1:.2f}%", asset="stocks"), ""
    with mock.patch.object(config, "US_CRASH_MAX_CONFIRM", 4), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = REAL["us_crash_screen"]()
    assert confirmed == ["S09", "S08", "S07", "S06"], "跌最多的先確認: " + str(confirmed)
    assert [r["symbol"] for r in out] == ["S09", "S08", "S07", "S06"]
    assert "只確認跌最多的前 4 檔，清單不完整" in [x for x in fetch.LOG if x["source"] == "us_crash:list"][0]["detail"]
    assert "初篩跌幅達 5.0% 的有 10 檔" in [x for x in fetch.LOG if x["source"] == "us_crash:list"][0]["detail"]
    assert all("當天初篩 10 檔，只確認跌最多的前 4 檔，清單不完整" in r["note"] for r in out)
    reset(); confirmed.clear()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):          # 沒超過上限：不該有「不完整」字樣
        out = REAL["us_crash_screen"]()
    assert len(out) == 10 and not any("不完整" in r["note"] for r in out) and "不完整" not in [x for x in fetch.LOG if x["source"] == "us_crash:list"][0]["detail"]
    assert config.US_CRASH_MAX_CONFIRM >= 60


def _inst_env(todo_dates, have=(), payload_for=None, fail_on=None):
    """外資歷史回補的模擬環境：交易日清單、資料庫已有的日期、每個日期證交所的回傳。回傳 (patch 清單, 被請求的網址紀錄)"""
    urls = []
    def req(method, path, body=None, **k):
        if path.startswith("tw_hist_days"): return [{"as_of": d} for d in todo_dates], ""
        if path.startswith("tw_institutional"): return [{"as_of": d} for d in have], ""
        raise AssertionError(path)
    def fj(url, **k):
        urls.append(url)
        d = url.split("dayDate=")[1]
        if fail_on and d == fail_on: return None, "HTTP 503"
        return (payload_for(d) if payload_for else bfi_payload(date_str=d)), ""
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req),
            mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep")], urls


def _with(patches, fn):
    for p_ in patches: p_.start()
    try: return fn()
    finally:
        for p_ in patches: p_.stop()


def test_inst_backfill_fetches_missing_trading_days_and_checks_each_returned_date():
    reset()
    days = [D(1), D(2), D(3), D(4)]
    patches, urls = _inst_env(days, have=[D(2)])
    rows = _with(patches, lambda: REAL["tw_institutional_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1), D(3), D(4)], "已經有的那天不重抓"
    assert all(r["source"] == "TWSE BFI82U" and r["foreign_net"] == -68815276510.0 for r in rows)
    assert len(urls) == 3 and all("&dayDate=" in u and u.startswith(config.TWSE_BFI82U) for u in urls) and urls[0].endswith(D(1).replace("-", ""))
    log = [x for x in fetch.LOG if x["source"] == "inst_backfill"][0]
    assert log["status"] == "success" and "本次補 3 天" in log["detail"] and "還有 0 天下次繼續" in log["detail"]


def test_inst_backfill_rejects_wrong_date_no_data_and_bad_numbers_but_keeps_going():
    reset()
    days = [D(1), D(2), D(3), D(4)]
    def pf(d):
        if d == D(1).replace("-", ""): return bfi_payload(date_str=D(5).replace("-", ""))           # 要 D(1)，卻回傳別天（可能是把最新一天當歷史）
        if d == D(2).replace("-", ""): return {"stat": "很抱歉，沒有符合條件的資料!"}                    # 休市或沒資料
        if d == D(3).replace("-", ""):
            rows = [list(x) for x in BFI_ROWS]; rows[3][3] = "-1"                                      # 買進－賣出 對不上
            return bfi_payload(rows, date_str=d)
        return bfi_payload(date_str=d)
    patches, urls = _inst_env(days, payload_for=pf)
    rows = _with(patches, lambda: REAL["tw_institutional_backfill"]())
    assert [r["as_of"] for r in rows] == [D(4)] and len(urls) == 4, "壞的那幾天略過，其他照常"
    d = [x for x in fetch.LOG if x["source"] == "inst_backfill"][0]["detail"]
    assert "略過 1 天（證交所沒有資料）" in d and "2 天（日期或數字驗證不過：要 " in d and "但證交所回傳" in d, d


def test_inst_backfill_stops_on_connection_failure_and_respects_batch_cap():
    reset()
    days = [D(i) for i in range(1, 6)]
    patches, urls = _inst_env(days, fail_on=D(2).replace("-", ""))
    rows = _with(patches, lambda: REAL["tw_institutional_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1)] and len(urls) == 2, "連線失敗就整批停止，不繼續打請求"
    f = [x for x in fetch.LOG if x["source"] == "inst_backfill"]
    assert f[0]["status"] == "failure" and "已停止本次回補" in f[0]["detail"] and f[0]["as_of"] == D(2) and f[-1]["status"] == "success"
    reset()
    patches, urls = _inst_env(days)
    rows = _with(patches, lambda: REAL["tw_institutional_backfill"](max_days=2))
    assert len(rows) == 2 and len(urls) == 2
    assert "還有 3 天下次繼續" in [x for x in fetch.LOG if x["source"] == "inst_backfill"][0]["detail"]


def test_inst_backfill_done_and_unconfigured_cases():
    reset()
    patches, urls = _inst_env([D(1), D(2)], have=[D(1), D(2)])
    assert _with(patches, lambda: REAL["tw_institutional_backfill"]()) == [] and urls == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "inst_backfill"] == ["no_new_data"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "fetch_json") as fj:
        assert REAL["tw_institutional_backfill"]() == []
        fj.assert_not_called()
    assert [x["status"] for x in fetch.LOG if x["source"] == "inst_backfill"] == ["skipped"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 404")):
        assert REAL["tw_institutional_backfill"]() == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "inst_backfill"] == ["failure"]


def test_run_job_daily_jobs_still_persist_todays_institutional_and_putcall_rows():
    """回歸測試：每天抓到的外資買賣超、Put/Call 必須原封不動交給 persist（v38 曾經不小心把它們清成空的）。"""
    reset()
    inst = [{"as_of": D(1), "foreign_buy": 1.0, "foreign_sell": 2.0, "foreign_net": -1.0, "source": "TWSE BFI82U"}]
    pc = [{"as_of": D(1), "total_pc": 0.9, "source": "CBOE 每日市場統計"}]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=inst, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=pc, twd_fallback_chain=None)
    for job in ("tw_close", "full"):
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist_extra"),
                                                                                       mock.patch.object(fetch, "fred_latest", return_value=None)]
        for p_ in patches: p_.start()
        try:
            with mock.patch.object(fetch, "persist") as pr:
                res = fetch.run_job(job, dry_run=True)
            args = pr.call_args[0]
            assert args[5] == inst, f"{job}：每天的外資買賣超不能被清掉（persist 第 6 個參數）: {args[5]!r}"
            assert args[11] == pc and res["tw_institutional"] == inst and res["us_putcall"] == pc
        finally:
            for p_ in patches: p_.stop()
        reset()


def test_tw_events_job_checks_only_event_announcements_for_codes_from_the_database():
    reset()
    got = []
    ev = [{"code": "8358", "name": "金居", "event_date": D(-9), "announced_at": D(0), "subject": "x", "source": "s"}]
    seen_codes = []
    def reader(method, path, body=None, **k):
        assert method == "GET" and path.startswith("stock_prices?select=code,name,as_of&market=eq.TW")
        return [{"code": "2330", "name": "台積電", "as_of": D(1)}, {"code": "8358", "name": "金居", "as_of": D(1)}, {"code": "2330", "name": "台積電", "as_of": D(1)},
                {"code": "9999", "name": "舊的持股", "as_of": D(5)}], ""                                    # 重複的代號、不是最新一天的都不算
    def fake_events(price_rows):
        seen_codes.extend(sorted(r["code"] for r in price_rows))
        return ev
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=reader), \
            mock.patch.object(fetch, "tw_investor_events", side_effect=fake_events), mock.patch.object(fetch, "persist_extra", side_effect=lambda dry, t: got.append((dry, t))), \
            mock.patch.object(fetch, "persist") as pr, mock.patch.object(fetch, "twse_market") as tm, mock.patch.object(fetch, "fred_latest") as fl:
        res = fetch.run_job("tw_events", dry_run=False)
        pr.assert_not_called(); tm.assert_not_called(); fl.assert_not_called()                                 # 這個工作不碰其他資料
    assert seen_codes == ["2330", "8358"] and res == {"job": "tw_events", "events": ev}
    assert got == [(False, [("tw_investor_events", ev, "code,event_date")])], "只寫法說會的表，用 (code, event_date) 去重"
    assert {x["source"]: x["status"] for x in fetch.LOG}["module:M11"] == "success"
    assert "tw_events" in config.JOBS
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert fetch.run_job("tw_events")["events"] == []
    assert fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")):
        assert fetch.run_job("tw_events")["events"] == []
    assert fetch.LOG[-1]["status"] == "skipped" and "先跑一次 tw_close" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 401")):
        assert fetch.run_job("tw_events")["events"] == []
    assert fetch.LOG[-1]["status"] == "failure" and "讀取持股代號失敗" in fetch.LOG[-1]["detail"]


def test_workflow_schedules_evening_events_job_and_every_cron_maps_to_a_job():
    import re
    y = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".github", "workflows", "daily.yml"), encoding="utf-8").read()
    crons = re.findall(r'- cron: "([^"]+)"', y)
    mapped = dict(re.findall(r'"([0-9*/ ,\-]+)"\)\s+echo "job=(\w+)"', y))
    assert set(crons) == set(mapped), f"每個排程都要有對應的工作：{set(crons) ^ set(mapped)}"
    assert mapped["30 12 * * *"] == "tw_events" and mapped["15 15 * * *"] == "tw_events" and mapped["45 12 * * 2-6"] == "us_options", "美股選擇權快照：週二到週六 20:45（台北時間）"
    assert "tw_events" in re.search(r"options: \[([^\]]+)\]", y).group(1)
    assert all(j in config.JOBS for j in mapped.values()) and len(crons) == len(set(crons))


def test_run_job_daily_persists_exright_preview_rows_and_isolates_its_failure():
    reset()
    row = {"market": "TWSE", "code": "2330", "event_date": D(-9), "name": "台積電", "kind": "息", "stock_rate": 0.0, "cash_dividend": 5.0, "source": "證交所 除權除息預告表"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    for job in ("tw_close", "full"):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_exright_preview", return_value=[row]),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda dry, tables: got.append(dict((n, (r, c)) for n, r, c in tables)))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=True)
            assert got[0]["tw_exright_events"] == ([row], "market,code,event_date"), job
            assert {x["source"]: x["status"] for x in fetch.LOG}["module:M15"] == "success"
        finally:
            for p_ in patches: p_.stop()
        reset()
    got = []
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
              mock.patch.object(fetch, "tw_exright_preview", side_effect=RuntimeError("boom")), mock.patch.object(fetch, "persist_extra", side_effect=lambda dry, tables: got.append(tables))]
    for p_ in patches: p_.start()
    try:
        fetch.run_job("tw_close", dry_run=True)
        st = {x["source"]: x["status"] for x in fetch.LOG}
        assert st["module:M15"] == "failure" and "module:M08" in st, "除權息壞了不影響其他模組"
    finally:
        for p_ in patches: p_.stop()


def _idx_env(hist=None, quote=None, now=None):
    """美股指數官方行情的模擬環境：hist 與 quote 是 {Nasdaq 代號: 回傳值}；now 是美東現在時間。"""
    from datetime import datetime as _dt
    now = now or _dt(2026, 11, 3, 18, 0)                                           # 美東 18:00：今天已收盤
    return [mock.patch.object(fetch, "_now_et", return_value=now), mock.patch.object(fetch.time, "sleep"),
            mock.patch.object(fetch, "_nasdaq_history", side_effect=lambda sym, ac="etf", days=75: (hist or {}).get(sym, (None, "沒有歷史"))),
            mock.patch.object(fetch, "_nasdaq_quote", side_effect=lambda sym, classes: (quote or {}).get(sym, (None, "沒有報價")))]


THREE_FEED = (("COMP", "NASDAQCOM", ("index",)), ("SPX", "SP500", ("index",)), ("DJIA", "DJIA", ("index",)))


def test_real_index_feed_only_contains_sources_verified_to_work():
    assert [e[0] for e in config.US_INDEX_FEED] == ["COMP"], "SPX、DJIA 在 Nasdaq 的 API 實測沒有資料，不能放進來"
    assert all(e[1] in config.FRED_MACRO for e in config.US_INDEX_FEED)


def test_us_index_quotes_fills_lagging_days_from_official_history_only_for_closed_days():
    reset()
    _feed = mock.patch.object(config, "US_INDEX_FEED", THREE_FEED); _feed.start()
    d = lambda n: date(2026, 11, 3) - timedelta(days=n)
    hist = {"COMP": ([(d(4), 23000.5), (d(3), 23100.0), (d(2), 23250.25), (d(1), 23300.0), (d(0), 23400.0)], ""),                      # 今天（美東 11/3）18:00 已收盤 → 全收
            "SPX": ([(d(1), 7700.0), (d(0), 7710.0), (d(2), 9999999.0)], ""),                                                           # 超出合理範圍的拒收
            "DJIA": ([(d(1), 48000.0)], "")}
    patches = _idx_env(hist=hist)
    rows = _with(patches, lambda: REAL["us_index_quotes"]())
    by = {}
    for r in rows: by.setdefault(r["series_id"], []).append(r)
    assert sorted(by) == ["DJIA", "NASDAQCOM", "SP500"] and len(by["NASDAQCOM"]) == 5 and len(by["SP500"]) == 2 and len(by["DJIA"]) == 1, {k: len(v) for k, v in by.items()}
    assert all(r["source"].startswith("Nasdaq 官方行情") and r["label"] for r in rows) and {r["as_of"] for r in by["SP500"]} == {d(0).isoformat(), d(1).isoformat()}
    lg = {x["source"]: x for x in fetch.LOG}
    assert lg["us_index:NASDAQCOM"]["status"] == "success" and "歷史 5 天" in lg["us_index:NASDAQCOM"]["detail"] and "略過 1 筆" in lg["us_index:SP500"]["detail"]
    reset()
    from datetime import datetime as _dt
    patches = _idx_env(hist={"COMP": ([(d(1), 23300.0), (d(0), 23400.0)], ""), "SPX": (None, "x"), "DJIA": (None, "x")}, now=_dt(2026, 11, 3, 11, 0))      # 美東 11:00：今天還沒收盤
    rows = _with(patches, lambda: REAL["us_index_quotes"]())
    assert [r["as_of"] for r in rows] == [d(1).isoformat()], "今天還沒收盤的那一天不採用（盤中數字不能當收盤）"
    _feed.stop()


def test_us_index_quotes_falls_back_to_closed_quote_and_reports_failures():
    reset()
    _feed = mock.patch.object(config, "US_INDEX_FEED", THREE_FEED); _feed.start()
    quote = {"COMP": ({"close": 23400.0, "pct": 0.5, "as_of": "2026-11-03", "class": "index"}, ""), "SPX": (None, "marketStatus=Open、isRealTime=False，不是已收盤的價格，不採用"),
             "DJIA": ({"close": 999999999.0, "pct": 0.1, "as_of": "2026-11-03", "class": "index"}, "")}
    patches = _idx_env(hist={}, quote=quote)                                       # 歷史全部抓不到
    rows = _with(patches, lambda: REAL["us_index_quotes"]())
    assert [(r["series_id"], r["value"], r["as_of"]) for r in rows] == [("NASDAQCOM", 23400.0, "2026-11-03")], "只收到通過驗證的最新報價"
    st = {x["source"]: x for x in fetch.LOG}
    assert st["us_index:NASDAQCOM"]["status"] == "success" and "只取得最新報價" in st["us_index:NASDAQCOM"]["detail"]
    assert st["us_index:SP500"]["status"] == "skipped" and st["us_index:DJIA"]["status"] == "failure" and "拒收" in st["us_index:DJIA"]["detail"]
    reset()
    patches = _idx_env(hist={}, quote={})
    assert _with(patches, lambda: REAL["us_index_quotes"]()) == []
    assert all(x["status"] == "failure" for x in fetch.LOG if x["source"].startswith("us_index:"))
    _feed.stop()


def test_us_index_quotes_optional_source_never_turns_the_job_red_when_unavailable():
    reset()
    feed = (("COMP", "NASDAQCOM", ("index",)), ("SPX", "SP500", ("index",), True), ("DJIA", "DJIA", ("index",), True))
    quote = {"COMP": (None, "index 分類沒有報價資料"), "SPX": (None, "index 分類沒有報價資料"), "DJIA": ({"close": 999999999.0, "pct": 0.1, "as_of": "2026-11-03", "class": "index"}, "")}
    with mock.patch.object(config, "US_INDEX_FEED", feed):
        rows = _with(_idx_env(hist={}, quote=quote), lambda: REAL["us_index_quotes"]())
    st = {x["source"]: x for x in fetch.LOG}
    assert rows == []
    assert st["us_index:NASDAQCOM"]["status"] == "failure", "核心來源抓不到要記失敗"
    assert st["us_index:SP500"]["status"] == "no_new_data" and "選用來源" in st["us_index:SP500"]["detail"], "選用來源抓不到只記沒有資料"
    assert st["us_index:DJIA"]["status"] == "failure" and "拒收" in st["us_index:DJIA"]["detail"], "資料超出合理範圍是可疑的，選用來源也要記失敗"


def test_run_job_daily_persists_index_rows_and_isolates_m18():
    reset()
    rows = [{"series_id": "NASDAQCOM", "label": "Nasdaq 綜合指數", "as_of": D(1), "value": 23400.0, "source": "Nasdaq 官方行情"}]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(job, dry, **kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "us_index_quotes", **(kw or {"return_value": rows})),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    for job in ("tw_close", "full"):
        reset(); tables, st = go(job, False)
        assert ("macro_series", rows, "series_id,as_of") in tables and st["module:M18"] == "success", job
    reset(); tables, st = go("tw_close", True)
    assert "module:M18" not in st, "dry-run 不碰"
    reset(); tables, st = go("tw_close", False, side_effect=RuntimeError("boom"))
    assert st["module:M18"] == "failure" and "module:M08" in st, "M18 壞了不影響其他模組"
    reset(); tables, st = go("tw_backfill", False)
    assert "module:M18" not in st, "夜間回補不跑"


def test_main_prints_each_failure_reason_and_github_annotations():
    import io, contextlib
    reset()
    def fake_run_job(job, dry_run=False):
        fetch.log("us_index:NASDAQCOM", "failure", detail="歷史：HTTP 403；報價：連線失敗: timeout\n第二行,含逗號:冒號%")
        fetch.log("tw_close", "success", detail="這筆成功不該被印成失敗")
        fetch.log("fred:DGS10", "failure", detail="FRED 回傳格式非預期")
        return {}
    buf = io.StringIO()
    with mock.patch.object(fetch, "run_job", side_effect=fake_run_job), mock.patch.object(fetch, "flush_log", return_value=True), mock.patch.object(fetch, "sb_ready", return_value=False), \
            mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()), contextlib.redirect_stdout(buf):
        code = fetch.main(["--job", "tw_close", "--dry-run"])
    out = buf.getvalue()
    assert code == 1 and "其中 2 筆失敗" in out
    assert "  失敗｜us_index:NASDAQCOM｜歷史：HTTP 403；報價：連線失敗: timeout" in out and "  失敗｜fred:DGS10｜FRED 回傳格式非預期" in out, out
    assert not [l for l in out.splitlines() if l.startswith("  失敗｜tw_close")], "成功的那一筆不能被印成失敗（dry-run 會另外印出整份紀錄，所以只檢查「失敗｜」開頭的行）"
    ann = [l for l in out.splitlines() if l.startswith("::error")]
    assert len(ann) == 2 and ann[0].startswith("::error title=us_index%3ANASDAQCOM::") and "%0A第二行,含逗號:冒號%25" in ann[0] and "\n" not in ann[0], ann[0]
    reset()
    buf = io.StringIO()
    with mock.patch.object(fetch, "run_job", return_value={}), mock.patch.object(fetch, "flush_log", return_value=True), mock.patch.object(fetch, "sb_ready", return_value=False), \
            mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()), contextlib.redirect_stdout(buf):
        code = fetch.main(["--job", "tw_close", "--dry-run"])
    assert code == 0 and "其中 0 筆失敗" in buf.getvalue() and "::error" not in buf.getvalue(), "沒有失敗就不印錯誤"
    reset()
    buf = io.StringIO()
    with mock.patch.object(fetch, "run_job", return_value={}), mock.patch.object(fetch, "flush_log", return_value=False), mock.patch.object(fetch, "sb_ready", return_value=False), \
            mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()), contextlib.redirect_stdout(buf):
        code = fetch.main(["--job", "tw_close", "--dry-run"])
    assert code == 1 and "失敗｜寫入資料庫" in buf.getvalue() and "::error title=寫入資料庫::" in buf.getvalue(), "紀錄沒寫進資料庫也要明講"


def test_workflow_uses_node24_action_versions():
    y = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".github", "workflows", "daily.yml"), encoding="utf-8").read()
    uses = dict(__import__("re").findall(r"uses: (actions/[\w-]+)@v(\d+)", y))
    assert int(uses["actions/checkout"]) >= 6 and int(uses["actions/setup-python"]) >= 6 and int(uses["actions/upload-artifact"]) >= 6, uses


STOCK_FIELDS = ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "次一營業日限額", "買進", "賣出", "現券償還", "前日餘額", "今日餘額", "次一營業日限額", "資券互抵", "註記"]


def stock_margin_rows(n=200, units=50000, bad_identity=0):
    rows = []
    for i in range(n):
        today = units + (7 if i < bad_identity else 0)                                              # 故意讓一部分列的「前日＋買進－賣出－償還＝今日」對不上
        rows.append([f"{1100 + i}", f"股{i}", "2,000", "1,000", "0", f"{units - 1000:,}", f"{today:,}", "999,999", "0", "0", "0", "0", "0", "999,999", "0", " "])
    return rows


def margin_all_payload(stock_rows=None, summary_rows=None, date_str=None, stat="OK"):
    return {"stat": stat, "date": date_str or ymd(), "tables": [{"title": "信用交易統計", "fields": MARGIN_FIELDS, "data": summary_rows or MARGIN_ROWS},
                                                              {"title": "融資融券彙總", "fields": STOCK_FIELDS, "data": stock_rows if stock_rows is not None else stock_margin_rows()}]}


def day_all_payload(n=200, price=140.0, date_str=None, drop=0):
    d = date_str or ymd()
    roc = f"{int(d[:4]) - 1911}{d[4:]}"
    return [{"Date": roc, "Code": f"{1100 + i}", "Name": f"股{i}", "ClosingPrice": ("" if i < drop else f"{price}")} for i in range(n)]


def _ratio_env(margin, px):
    def fj(url, **k):
        if "MI_MARGN" in url and "selectType=ALL" in url: return margin
        if url.endswith("STOCK_DAY_ALL"): return px
        raise AssertionError(url)
    return mock.patch.object(fetch, "fetch_json", side_effect=fj)


def test_margin_ratio_est_computes_market_value_over_loan_with_matching_dates():
    reset()
    with _ratio_env((margin_all_payload(), ""), (day_all_payload(), "")):
        rows = REAL["tw_margin_ratio_est"]()
    loan = 633910848 * 1000
    exp = 200 * 50000 * 1000 * 140.0 / loan * 100
    assert len(rows) == 1 and rows[0]["as_of"] == twse_iso(ymd()) and abs(rows[0]["est_ratio"] - round(exp, 2)) < 0.011, (rows, exp)
    assert rows[0]["n_stocks"] == 200 and rows[0]["n_missing"] == 0 and rows[0]["margin_loan"] == loan and "不是官方整戶擔保維持率" in rows[0]["source"]
    lg = [x for x in fetch.LOG if x["source"] == "tw_margin_ratio"][0]
    assert lg["status"] == "success" and "估算維持率" in lg["detail"]


def twse_iso(ymd_s): return f"{ymd_s[:4]}-{ymd_s[4:6]}-{ymd_s[6:]}"


def test_margin_ratio_est_refuses_mismatched_dates_missing_prices_and_bad_tables():
    def last(): return [x for x in fetch.LOG if x["source"] == "tw_margin_ratio"][-1]
    reset()
    with _ratio_env((margin_all_payload(), ""), (day_all_payload(date_str=ymd(-1)), "")):                      # 收盤價是昨天的
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "no_new_data" and "兩天不一致" in last()["detail"]
    reset()
    with _ratio_env((margin_all_payload(), ""), (day_all_payload(drop=20), "")):                                # 10% 的股票查不到收盤價
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure" and "超過 5%" in last()["detail"]
    reset()
    with _ratio_env((margin_all_payload(), ""), (day_all_payload(drop=8), "")):                                 # 4% 在容許範圍內
        rows = REAL["tw_margin_ratio_est"]()
    assert rows and rows[0]["n_missing"] == 8 and rows[0]["n_stocks"] == 192
    reset()
    with _ratio_env((margin_all_payload(stock_margin_rows(bad_identity=60)), ""), (day_all_payload(), "")):    # 30% 的列對不上：欄位位置可能不對
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure" and "通過" in last()["detail"]
    reset()
    bad_summary = [list(x) for x in MARGIN_ROWS]; bad_summary[2][5] = "700,000,000"
    with _ratio_env((margin_all_payload(summary_rows=bad_summary), ""), (day_all_payload(), "")):
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure" and "對不上" in last()["detail"]
    reset()
    with _ratio_env((margin_all_payload(), ""), (day_all_payload(price=9000.0), "")):                           # 估出來 14,000% 之類：拒收
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure" and "不在合理範圍" in last()["detail"]
    reset()
    with _ratio_env(({"stat": "很抱歉，沒有符合條件的資料"}, ""), ([], "")):
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "no_new_data"
    reset()
    with _ratio_env((None, "HTTP 403"), ([], "")):
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure"
    reset()
    with _ratio_env((margin_all_payload(), ""), (None, "HTTP 500")):
        assert REAL["tw_margin_ratio_est"]() == []
    assert last()["status"] == "failure" and "收盤價抓取失敗" in last()["detail"]
    reset()
    wrong = margin_all_payload(); wrong["tables"][1]["fields"] = ["代號", "名稱", "x"] + STOCK_FIELDS[3:]
    with _ratio_env((wrong, ""), (day_all_payload(), "")):
        assert REAL["tw_margin_ratio_est"]() == []
    assert "欄位與預期不符" in last()["detail"]


def _chain(as_of, spot=7500.0, iv30=0.145, extra_fillers=120, last_trade=None):
    e = lambda n: (as_of + timedelta(days=n)).strftime("%y%m%d")
    mk = lambda root, exp, cp, k, oi: {"option": f"{root}{exp}{cp}{int(k * 1000):08d}", "iv": 0.2, "open_interest": oi, "volume": 0, "bid": 1, "ask": 2}
    opts = [mk("SPXW", e(7), "C", 7400, 100), mk("SPXW", e(7), "P", 7400, 900), mk("SPXW", e(7), "C", 7500, 500), mk("SPXW", e(7), "P", 7500, 600), mk("SPXW", e(7), "C", 7600, 700), mk("SPXW", e(7), "P", 7600, 50),
            mk("SPX", e(40), "C", 7500, 1000), mk("SPX", e(40), "P", 7500, 2000),                      # 40 天內：和上面同一履約價的要加總
            mk("SPX", e(90), "P", 7400, 5000),                                                          # 90 天後：不進履約價列，但算進總量
            mk("SPX", e(7), "C", 9000, 10)]                                                             # 超出 ±15%：不進履約價列，但算進總量
    opts += [mk("SPX", e(90), "C", 9100 + i * 5, 1) for i in range(extra_fillers)]
    lt = last_trade or f"{as_of.isoformat()}T16:00:00"
    return {"timestamp": "2026-11-03 13:03:16", "symbol": "_SPX", "data": {"current_price": spot, "iv30": iv30, "iv30_change": -0.12, "last_trade_time": lt, "options": opts}}


def _last_weekday(d):
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d


def test_parse_options_chain_aggregates_strikes_within_45_days_and_band_and_all_expiries_for_totals():
    as_of = _last_weekday(TODAY - timedelta(days=1))
    summ, strikes, err = fetch.parse_options_chain(_chain(as_of), "SPX", today=TODAY)
    assert err is None and summ["as_of"] == as_of.isoformat() and summ["spot"] == 7500.0 and summ["iv30"] == 14.5 and summ["iv30_change"] == -0.12
    assert summ["call_oi"] == 100 + 500 + 700 + 1000 + 10 + 120 and summ["put_oi"] == 900 + 600 + 50 + 2000 + 5000, "總量包含所有到期日、所有履約價"
    assert summ["n_contracts"] == 10 + 120 and summ["source"] == "Cboe 延遲選擇權鏈"
    by = {r["strike"]: r for r in strikes}
    assert sorted(by) == [7400.0, 7500.0, 7600.0], "只收 45 天內到期、現價 ±15% 的履約價"
    assert (by[7500.0]["call_oi"], by[7500.0]["put_oi"]) == (500 + 1000, 600 + 2000), "同一履約價把各到期日加總"
    assert (by[7400.0]["call_oi"], by[7400.0]["put_oi"]) == (100, 900), "90 天後的 5000 不能算進來"
    assert all(r["underlying"] == "SPX" and r["as_of"] == as_of.isoformat() for r in strikes)
    for raw, exp in ((14.5, 14.5), (0.145, 14.5), (0.5, 50.0), (0.01, None), (500, None), (None, None), ("abc", None)):
        sm, _, e = fetch.parse_options_chain(_chain(as_of, iv30=raw), "SPX", today=TODAY)
        assert e is None and sm["iv30"] == exp, (raw, sm["iv30"])


def test_parse_options_chain_rejects_bad_dates_prices_and_thin_chains():
    as_of = _last_weekday(TODAY - timedelta(days=1))
    sat = as_of
    while sat.weekday() != 5: sat += timedelta(days=1)
    for chain, today, frag in ((_chain(sat), sat, "不是最近 5 天內的平日"), (_chain(as_of - timedelta(days=10)), TODAY, "不是最近 5 天內的平日"), (_chain(as_of, spot=50.0), TODAY, "不在合理範圍"),
                               (_chain(as_of, last_trade="昨天"), TODAY, "看不懂"), (_chain(as_of, extra_fillers=0), TODAY, "只有 10 檔"), ({"data": {}}, TODAY, "不在合理範圍"),
                               (_chain(as_of, last_trade="2026-13-45T00:00:00"), TODAY, "不合法")):
        summ, strikes, err = fetch.parse_options_chain(chain, "SPX", today=today)
        assert summ is None and strikes == [] and frag in err, (frag, err)


def test_us_options_snapshot_core_failure_optional_quiet_and_job_persists_both_tables():
    reset()
    as_of = _last_weekday(date.today() - timedelta(days=1))
    good = json.dumps(_chain(as_of)).encode()
    spy = json.dumps(_chain(as_of, spot=750.0)).encode()
    def raw(url, timeout=None, headers=None):
        assert timeout == config.OPTIONS_TIMEOUT
        if "_SPX" in url: return 200, "application/json", good, ""
        if "SPY" in url: return 200, "application/json", spy, ""
        return 403, "text/html", b"blocked", "HTTP 403"                                              # QQQ 被擋
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        res = REAL["us_options_snapshot"]()
    assert [r["summary"]["underlying"] for r in res] == ["SPX", "SPY"] and len(res[0]["strikes"]) == 3
    st = {x["source"]: x for x in fetch.LOG}
    assert st["us_options:SPX"]["status"] == "success" and "PCR" in st["us_options:SPX"]["detail"] and st["us_options:SPY"]["status"] == "success"
    assert st["us_options:QQQ"]["status"] == "no_new_data" and "HTTP 403" in st["us_options:QQQ"]["detail"], "選用的標的被擋只記沒有資料"
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_options_snapshot"]() == []
    st = {x["source"]: x["status"] for x in fetch.LOG}
    assert st["us_options:SPX"] == "failure" and st["us_options:SPY"] == "no_new_data" and st["us_options:QQQ"] == "no_new_data", "核心來源失敗要記失敗；選用的不能讓工作變紅"
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>nope</html>", "")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_options_snapshot"]() == []
    assert "不是 JSON" in [x for x in fetch.LOG if x["source"] == "us_options:SPX"][0]["detail"]


def test_run_job_us_options_writes_summary_and_strikes_and_isolates_failure():
    reset()
    res = [{"summary": {"underlying": "SPX", "as_of": D(1), "spot": 7500.0}, "strikes": [{"underlying": "SPX", "as_of": D(1), "strike": 7400.0, "call_oi": 1, "put_oi": 2}, {"underlying": "SPX", "as_of": D(1), "strike": 7500.0, "call_oi": 3, "put_oi": 4}]}]
    got = []
    with mock.patch.object(fetch, "us_options_snapshot", return_value=res), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append((d_, [(n, r, c) for n, r, c in tables]))), \
            mock.patch.object(fetch, "persist") as pr, mock.patch.object(fetch, "twse_market") as tm:
        out = fetch.run_job("us_options", dry_run=False)
        pr.assert_not_called(); tm.assert_not_called()
    assert out == {"job": "us_options", "underlyings": ["SPX"]}
    assert got[0][0] is False and got[0][1][0] == ("us_options_summary", [res[0]["summary"]], "underlying,as_of") and got[0][1][1] == ("us_options_strikes", res[0]["strikes"], "underlying,as_of,strike")
    assert {x["source"]: x["status"] for x in fetch.LOG}["module:M20"] == "success"
    reset(); got.clear()
    with mock.patch.object(fetch, "us_options_snapshot", side_effect=RuntimeError("boom")), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append(tables)):
        out = fetch.run_job("us_options", dry_run=True)
    assert out["underlyings"] == [] and {x["source"]: x["status"] for x in fetch.LOG}["module:M20"] == "failure" and all(r == [] for _, r, _ in got[0]), "壞了不寫任何東西"
    assert "us_options" in config.JOBS


def test_run_job_daily_persists_margin_ratio_estimate_and_isolates_m21():
    reset()
    row = {"as_of": D(1), "est_ratio": 148.2, "margin_value": 9.4e11, "margin_loan": 6.3e11, "n_stocks": 1100, "n_missing": 3, "source": "自行估算"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(job, dry, **kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_margin_ratio_est", **(kw or {"return_value": [row]})),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    for job in ("tw_close", "full"):
        reset(); tables, st = go(job, False)
        assert ("tw_margin_ratio", [row], "as_of") in tables and st["module:M21"] == "success", job
    reset(); tables, st = go("tw_close", True)
    assert "module:M21" not in st
    reset(); tables, st = go("tw_close", False, side_effect=RuntimeError("boom"))
    assert st["module:M21"] == "failure" and "module:M08" in st


def test_http_helpers_ask_for_gzip_decompress_it_and_survive_truncated_downloads():
    import gzip as _gz, http.client
    reset()
    payload = json.dumps({"ok": 1, "x": "中文"}).encode("utf-8")
    seen = []
    class Hdr(dict):
        def get(self, k, d=None): return super().get(k, d)
    class Resp:
        status = 200
        def __init__(self, body, enc=None, ctype="application/json"):
            self._b = body; self.headers = Hdr({"Content-Type": ctype, **({"Content-Encoding": enc} if enc else {})})
        def read(self): return self._b
        def __enter__(self): return self
        def __exit__(self, *a): return False
    def opener(resp):
        def f(req, timeout=None):
            seen.append(req)
            return resp
        return f
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(Resp(_gz.compress(payload), "gzip"))):
        assert fetch.fetch_json("https://x/y") == ({"ok": 1, "x": "中文"}, "")
    assert seen[-1].get_header("Accept-encoding") == "gzip", "一定要求 gzip（大檔案才不會傳到一半被切斷）"
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(Resp(payload))):                       # 沒壓縮的照樣可以
        assert fetch.fetch_json("https://x/y")[0] == {"ok": 1, "x": "中文"}
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(Resp(payload, "gzip"))):               # 標頭說 gzip 但其實不是：當作沒壓縮
        assert fetch.fetch_json("https://x/y")[0] == {"ok": 1, "x": "中文"}
    class NoHdr:                                                                                                      # 舊測試用的簡化回應（沒有 headers 屬性）也要能讀
        def read(self): return payload
        def __enter__(self): return self
        def __exit__(self, *a): return False
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(NoHdr())):
        assert fetch.fetch_json("https://x/y")[0] == {"ok": 1, "x": "中文"}
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(Resp(_gz.compress(payload), "gzip"))):
        st, ct, body, err = fetch.fetch_raw("https://x/y", headers={"User-Agent": "u"})
    assert st == 200 and body == payload and err == "" and seen[-1].get_header("Accept-encoding") == "gzip" and seen[-1].get_header("User-agent") == "u", "fetch_raw：自訂標頭保留、再補上 gzip、回傳的是解壓縮後的內容"
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=opener(Resp(payload))):
        fetch.fetch_raw("https://x/y", headers={"Accept-Encoding": "identity"})
    assert seen[-1].get_header("Accept-encoding") == "identity", "呼叫端自己指定的不覆蓋"
    # 傳到一半被切斷（IncompleteRead）：重試，之後成功就成功
    calls = {"n": 0}
    def flaky(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            class Cut:
                headers = Hdr()
                def read(self): raise http.client.IncompleteRead(b"x" * 10, 100)
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return Cut()
        return Resp(_gz.compress(payload), "gzip")
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=flaky), mock.patch.object(fetch.time, "sleep") as sl:
        assert fetch.fetch_json("https://x/y")[0] == {"ok": 1, "x": "中文"}
    assert calls["n"] == 3 and [c.args[0] for c in sl.call_args_list] == [config.BACKOFF_SECONDS, config.BACKOFF_SECONDS * 2], "被切斷用一般等待重試"
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=lambda req, timeout=None: (_ for _ in ()).throw(http.client.IncompleteRead(b"x", 5))), mock.patch.object(fetch.time, "sleep"):
        data, err = fetch.fetch_json("https://x/y")
    assert data is None and "IncompleteRead" in err, "三次都被切斷：回傳失敗與原因，不拋例外"


def _txo(ds, cm, strike, cp, oi, session="一般", contract="TXO"):
    return {"Date": ds, "Contract": contract, "ContractMonth(Week)": cm, "StrikePrice": str(strike), "CallPut": cp, "Open": "-", "High": "-", "Low": "-", "Close": "-", "Volume": "0",
            "SettlementPrice": "100", "OpenInterest": str(oi), "BestBid": "-", "BestAsk": "-", "TradingSession": session}


def _tx(ds, cm, settle, oi, session="一般", contract="TX"):
    return {"Date": ds, "Contract": contract, "ContractMonth(Week)": cm, "Open": "1", "High": "1", "Low": "1", "Last": "1", "Volume": "10", "SettlementPrice": str(settle), "OpenInterest": str(oi), "TradingSession": session}


def _tw_chain(ad):
    ds, ym = ad.strftime("%Y%m%d"), ad.strftime("%Y%m")
    nxt = f"{ad.year + (ad.month == 12)}{(ad.month % 12) + 1:02d}"
    far = f"{ad.year + (ad.month + 3 > 12)}{((ad.month + 2) % 12) + 1:02d}"
    opt, exp_call, exp_put = [], 0, 0
    def add(cm, k, cp, oi, **kw):
        nonlocal exp_call, exp_put
        opt.append(_txo(ds, cm, k, cp, oi, **kw))
        if kw.get("session", "一般") == "一般" and kw.get("contract", "TXO") == "TXO":
            if cp == "買權": exp_call += oi
            else: exp_put += oi
    for k in (40000, 40500, 41000, 41500, 42000):                                          # 本月、±20% 內
        add(ym, k, "買權", 100 + k // 1000); add(ym, k, "賣權", 200 + k // 1000)
    add(ym + "W2", 41000, "買權", 50); add(ym + "F1", 41000, "賣權", 70)                      # 週選：同一履約價加總
    add(nxt, 41000, "買權", 1000); add(nxt, 41000, "賣權", 2000)                              # 下個月：也算
    add(far, 41000, "買權", 9000); add(far, 41000, "賣權", 8000)                              # 更遠的月份：只進總量
    add(ym, 55000, "買權", 700)                                                              # 超出 ±20%：只進總量
    for i in range(210): add(far, 10000 + i * 100, "賣權" if i % 2 else "買權", 5)             # 湊足 200 列以上（只進總量）
    opt += [_txo(ds, ym, 41000, "買權", 999999, session="盤後"), _txo(ds, ym, 41000, "買權", 888, contract="TEO"), _txo(ds, ym, "-", "買權", 5), _txo(ds, ym, 41000, "亂寫", 5), _txo(ds, ym, 41000, "買權", "-")]
    fut = [_tx(ds, ym, 41000, 80000), _tx(ds, nxt, 41100, 20000), _tx(ds, far, 41300, 5000), _tx(ds, f"{ym}/{nxt}", 100, 7777), _tx(ds, ym, 40900, 1, session="盤後"), _tx(ds, ym, 123, 55555, contract="MTX")]
    return opt, fut, exp_call, exp_put


def test_parse_taifex_snapshot_aggregates_regular_session_txo_and_tx_only():
    ad = _last_weekday(TODAY - timedelta(days=1))
    opt, fut, exp_call, exp_put = _tw_chain(ad)
    summ, strikes, err = fetch.parse_taifex_snapshot(opt, fut, today=TODAY)
    assert err is None and summ["as_of"] == ad.isoformat() and summ["fut_settle"] == 41000 and summ["fut_oi"] == 80000 + 20000 + 5000, "近月結算價；未平倉量合計各月份，不含價差、盤後、小台"
    assert summ["call_oi"] == exp_call and summ["put_oi"] == exp_put
    by = {r["strike"]: r for r in strikes}
    assert sorted(by) == [40000, 40500, 41000, 41500, 42000], "只收本月與下月、現價 ±20% 內的履約價"
    assert by[41000]["call_oi"] == (100 + 41) + 50 + 1000 and by[41000]["put_oi"] == (200 + 41) + 70 + 2000, "週選與下個月的同一履約價要加總：" + str(by[41000])
    assert 9000 not in [r["call_oi"] for r in strikes] and all(r["as_of"] == ad.isoformat() for r in strikes), "更遠月份、超出範圍的不進履約價列"
    assert summ["n_rows"] == len([r for r in opt if r["TradingSession"] == "一般" and r["Contract"] == "TXO" and r["StrikePrice"] != "-" and r["CallPut"] in ("買權", "賣權") and r["OpenInterest"] != "-"])


def test_parse_taifex_snapshot_rejects_mismatched_stale_thin_and_malformed_data():
    ad = _last_weekday(TODAY - timedelta(days=1))
    opt, fut, _, _ = _tw_chain(ad)
    def err(o, f, today=TODAY): return fetch.parse_taifex_snapshot(o, f, today=today)[2]
    other = ad - timedelta(days=1)
    assert "對不上" in err(opt, [dict(r, Date=other.strftime("%Y%m%d")) for r in fut])
    sat = ad
    while sat.weekday() != 5: sat += timedelta(days=1)
    o2, f2, _, _ = _tw_chain(sat)
    assert "不是最近 5 天內的平日" in err(o2, f2, today=sat)
    o3, f3, _, _ = _tw_chain(ad - timedelta(days=12))
    assert "不是最近 5 天內的平日" in err(o3, f3)
    assert "只有" in err(opt[:12], fut) and "格式非預期" in err([], fut) and "格式非預期" in err(opt, None)
    assert "欄位與預期不符" in err([{"Date": "x"}], fut) and "欄位與預期不符" in err(opt, [{"Date": "x", "Contract": "TX"}])
    assert "找不到台指選擇權" in err([r for r in opt if r["Contract"] != "TXO"], fut) and "找不到台指選擇權" in err(opt, [r for r in fut if r["Contract"] != "TX"])
    assert "不合理" in err(opt, [dict(r, SettlementPrice="12") if r["Contract"] == "TX" else r for r in fut])
    assert "不合理" in err(opt, [dict(r, OpenInterest="0") if r["Contract"] == "TX" else r for r in fut])
    assert "找不到近月台指期" in err(opt, [_tx(ad.strftime("%Y%m%d"), "200001", 41000, 5)]), "只剩已經過期的月份：沒有近月可用"

def test_tw_options_snapshot_module_success_and_failures():
    reset()
    ad = _last_weekday(date.today() - timedelta(days=1))
    opt, fut, _, _ = _tw_chain(ad)
    def fj(url, **k):
        if url == config.TAIFEX_OPT_URL: return opt, ""
        if url == config.TAIFEX_DAILY_URL: return fut, ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        res = REAL["tw_options_snapshot"]()
    assert len(res) == 1 and res[0]["summary"]["fut_settle"] == 41000 and len(res[0]["strikes"]) == 5
    lg = [x for x in fetch.LOG if x["source"] == "tw_options"][0]
    assert lg["status"] == "success" and "近月台指期結算價 41,000" in lg["detail"] and "PCR" in lg["detail"] and lg["as_of"] == ad.isoformat()
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda url, **k: (None, "HTTP 403") if url == config.TAIFEX_OPT_URL else (fut, "")):
        assert REAL["tw_options_snapshot"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "選擇權 HTTP 403" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda url, **k: ([{"x": 1}], "")):
        assert REAL["tw_options_snapshot"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "欄位與預期不符" in fetch.LOG[-1]["detail"]


def test_tw_pcr_official_parses_validates_ratios_and_reports_failures():
    reset()
    d = lambda n: (date.today() - timedelta(days=n)).strftime("%Y%m%d")
    good = {"Date": d(1), "PutVolume": "148092", "CallVolume": "157966", "PutCallVolumeRatio%": "93.75", "PutOI": "69770", "CallOI": "113305", "PutCallOIRatio%": "61.58"}
    rows = [good, dict(good, Date=d(2), PutCallOIRatio="x", **{"PutCallOIRatio%": "99.99"}),                  # 比率和自己的數字對不上
            dict(good, Date="亂碼"), dict(good, Date=d(3), CallVolume="0"), dict(good, Date=d(400)), dict(good, Date=d(4), PutOI="-"), "壞列", dict(good, Date=d(5), PutVolume="150000", **{"PutCallVolumeRatio%": "94.95"})]
    with mock.patch.object(fetch, "fetch_json", return_value=(rows, "")):
        out = REAL["tw_pcr_official"]()
    assert [r["as_of"] for r in out] == [(date.today() - timedelta(days=1)).isoformat(), (date.today() - timedelta(days=5)).isoformat()], [r["as_of"] for r in out]
    assert out[0]["pcr_oi"] == 61.58 and out[0]["pcr_vol"] == 93.75 and out[0]["put_oi"] == 69770 and out[0]["call_vol"] == 157966 and out[0]["source"].startswith("期交所")
    lg = [x for x in fetch.LOG if x["source"] == "tw_pcr"][0]
    assert lg["status"] == "success" and "2 天（略過 5 列）" in lg["detail"] and "61.58%" in lg["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        assert REAL["tw_pcr_official"]() == []
    assert fetch.LOG[-1]["status"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=([{"foo": 1}], "")):
        assert REAL["tw_pcr_official"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "沒有通過驗證" in fetch.LOG[-1]["detail"]


def test_run_job_daily_persists_tw_options_and_pcr_and_isolates_them():
    reset()
    res = [{"summary": {"as_of": D(1), "fut_settle": 41000, "fut_oi": 100000, "call_oi": 5, "put_oi": 4, "n_rows": 300, "source": "x"}, "strikes": [{"as_of": D(1), "strike": 41000, "call_oi": 3, "put_oi": 2}, {"as_of": D(1), "strike": 41500, "call_oi": 1, "put_oi": 1}]}]
    pcr = [{"as_of": D(1), "put_vol": 1, "call_vol": 2, "pcr_vol": 50.0, "put_oi": 3, "call_oi": 4, "pcr_oi": 75.0, "source": "x"}]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(job, dry, **kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_options_snapshot", **(kw.get("opt") or {"return_value": res})),
                  mock.patch.object(fetch, "tw_pcr_official", **(kw.get("pcr") or {"return_value": pcr})),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append({n: (r, c) for n, r, c in tables}))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    for job in ("tw_close", "full"):
        reset(); tables, st = go(job, False)
        assert tables["tw_options_summary"] == ([res[0]["summary"]], "as_of") and tables["tw_options_strikes"] == (res[0]["strikes"], "as_of,strike") and tables["tw_pcr_official"] == (pcr, "as_of"), job
        assert st["module:M22"] == "success" and st["module:M23"] == "success"
    reset(); tables, st = go("tw_close", True)
    assert "module:M22" not in st and "module:M23" not in st, "dry-run 不碰"
    reset(); tables, st = go("tw_close", False, opt={"side_effect": RuntimeError("boom")})
    assert st["module:M22"] == "failure" and st["module:M23"] == "success" and tables["tw_options_summary"][0] == [] and tables["tw_pcr_official"][0] == pcr, "選擇權壞了不影響官方 PCR"
    reset(); tables, st = go("tw_close", False, pcr={"side_effect": RuntimeError("boom")})
    assert st["module:M23"] == "failure" and st["module:M22"] == "success" and "module:M08" in st


def _long_env(earliest, month_rows=None, fail_at=None):
    urls = []
    def req(method, path, body=None, **k):
        assert method == "GET" and path.startswith("market_stats?select=as_of&source=eq.TWSE%20FMTQIK&order=as_of.asc&limit=1"), path
        return ([{"as_of": earliest.isoformat()}] if earliest else []), ""
    def mrows(url):
        urls.append(url)
        ym = url.split("date=")[1][:6]
        if fail_at and ym == fail_at: return None, "證交所抓取失敗: HTTP 503"
        r = (month_rows or {}).get(ym)
        return ([{"market": "TW", "as_of": f"{ym[:4]}-{ym[4:]}-15", "close": 9000.0, "change": 1.0, "change_pct": 0.01, "turnover": 1e11, "source": "TWSE FMTQIK"}] if r is None else r), ""
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "_twse_month_rows", side_effect=mrows), mock.patch.object(fetch.time, "sleep")], urls


def test_tw_index_long_history_goes_back_month_by_month_with_cap_and_stops():
    reset()
    earliest = date.today().replace(day=1) - timedelta(days=400)
    ym = lambda d: f"{d.year}{d.month:02d}"
    patches, urls = _long_env(earliest.replace(day=10))
    rows = _with(patches, lambda: REAL["tw_index_long_history"](max_months=3))
    prev = lambda d: (d.replace(day=1) - timedelta(days=1))
    m1 = prev(earliest); m2 = prev(m1); m3 = prev(m2)
    assert [u.split("date=")[1][:6] for u in urls] == [ym(m1), ym(m2), ym(m3)], "從資料庫最早那個月的前一個月開始，一個月一個月往前"
    assert len(rows) == 3 and all(u.endswith("01") for u in [x.split("date=")[1] for x in urls]) and "FMTQIK" in urls[0] and "response=json" in urls[0]
    lg = [x for x in fetch.LOG if x["source"] == "tw_index_long"][0]
    assert lg["status"] == "success" and "本次回補 3 個月，取得 3 個交易日" in lg["detail"] and "下次繼續" in lg["detail"], lg["detail"]
    reset()
    near_target = (date.today().replace(day=1) - timedelta(days=int(config.INDEX_LONG_YEARS * 365))) + timedelta(days=40)
    patches, urls = _long_env(near_target)                                                         # 只剩一兩個月就到目標：補完就停
    rows = _with(patches, lambda: REAL["tw_index_long_history"](max_months=24))
    assert len(urls) <= 3 and "已經補到目標年數" in [x for x in fetch.LOG if x["source"] == "tw_index_long"][0]["detail"], "不會一直往前打到天荒地老: %d" % len(urls)
    reset()
    patches, urls = _long_env(earliest.replace(day=10), month_rows={ym(m1): [], ym(m2): []})        # 兩個月證交所沒資料：略過、算「空」
    rows = _with(patches, lambda: REAL["tw_index_long_history"](max_months=3))
    assert len(rows) == 1 and "2 個月證交所沒有資料" in [x for x in fetch.LOG if x["source"] == "tw_index_long"][0]["detail"]
    reset()
    patches, urls = _long_env(earliest.replace(day=10), fail_at=ym(m2))                            # 第二個月連線失敗：整批停止，第一個月保留
    rows = _with(patches, lambda: REAL["tw_index_long_history"](max_months=5))
    assert len(rows) == 1 and len(urls) == 2
    sts = [(x["status"], x["detail"]) for x in fetch.LOG if x["source"] == "tw_index_long"]
    assert sts[0][0] == "failure" and "不繼續打請求" in sts[0][1] and sts[-1][0] == "success"


def test_tw_index_long_history_edge_cases():
    reset()
    patches, urls = _long_env(None)
    assert _with(patches, lambda: REAL["tw_index_long_history"]()) == [] and urls == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "tw_index_long"] == ["no_new_data"] and "還沒有加權指數歷史" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["tw_index_long_history"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 401")):
        assert REAL["tw_index_long_history"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "讀取最早的一天失敗" in fetch.LOG[-1]["detail"]


def test_run_job_tw_backfill_persists_long_index_history_and_isolates_m24():
    reset()
    row = {"market": "TW", "as_of": "2017-03-15", "close": 9800.0, "change": 5.0, "change_pct": 0.05, "turnover": 9e10, "source": "TWSE FMTQIK"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(**kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_backfill", return_value={}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"),
                  mock.patch.object(fetch, "tw_index_long_history", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "tw_industry_collect", return_value=[]),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job("tw_backfill", dry_run=False)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go()
    assert ("market_stats", [row], "market,as_of,source") in tables and st["module:M24"] == "success"
    reset(); tables, st = go(side_effect=RuntimeError("boom"))
    assert st["module:M24"] == "failure" and "module:M12" in st and "module:M19" in st, "長期歷史壞了不影響夜間的其他模組"


def test_probe_round11_reports_finmind_nasdaq_profile_and_old_month_without_writing():
    reset()
    fm = {"status": 200, "data": [{"date": "2025-03-31", "stock_id": "2330", "type": "GrossProfit", "value": 5e11, "origin_name": "營業毛利"}, {"date": "2026-06-30", "stock_id": "2330", "type": "Revenue", "value": 9e11, "origin_name": "營業收入"}]}
    prof = json.dumps({"data": {"CompanyName": {"label": "Company Name", "value": "Oracle"}, "Sector": {"label": "Sector", "value": "Technology"}, "Industry": {"label": "Industry", "value": "Software"}}}).encode()
    def raw(url, timeout=None, headers=None):
        assert "company-profile" in url
        return 200, "application/json", prof, ""
    with mock.patch.object(fetch, "fetch_json", return_value=(fm, "")), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up, \
            mock.patch.object(fetch, "_twse_month_rows", return_value=([{"as_of": "2016-11-01", "close": 9200.0}, {"as_of": "2016-11-30", "close": 9250.0}], "")):
        fetch.probe_round11()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:finmind_fin"]["status"] == "success" and "2 列" in by["probe:finmind_fin"]["detail"] and "GrossProfit" in by["probe:finmind_fin"]["detail"] and "2025-03-31 到 2026-06-30" in by["probe:finmind_fin"]["detail"]
    assert by["probe:nasdaq_profile:ORCL"]["status"] == "success" and "Sector" in by["probe:nasdaq_profile:ORCL"]["detail"] and "Technology" in by["probe:nasdaq_profile:ORCL"]["detail"]
    assert by["probe:fmtqik_old"]["status"] == "success" and "2 個交易日" in by["probe:fmtqik_old"]["detail"] and "2016-11-01" in by["probe:fmtqik_old"]["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG) and not [x for x in fetch.LOG if x["status"] == "failure"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"<html>", "")), mock.patch.object(fetch, "_twse_month_rows", return_value=(None, "x")):
        fetch.probe_round11()
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "候選來源被擋是正常的探測結果，不能讓工作變紅"


def _fm_rows(code, series, extra_types=True):
    """series: {(年, 季): (營收, 毛利, 營業利益, 稅後淨利)} → FinMind 一列一個項目的格式"""
    qd = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    out = []
    for (y, q), (rev, gp, op, ni) in series.items():
        for typ, v in (("Revenue", rev), ("GrossProfit", gp), ("OperatingIncome", op), ("IncomeAfterTaxes", ni)):
            out.append({"date": f"{y}-{qd[q]}", "stock_id": code, "type": typ, "value": v, "origin_name": typ})
        if extra_types:
            out.append({"date": f"{y}-{qd[q]}", "stock_id": code, "type": "EPS", "value": 5.5, "origin_name": "基本每股盈餘"})
    return out


def _single_series(scale=1.0):
    """兩年八季的單季值：營收小幅波動（第 4 季 ÷ 第 1 季約 1.1～1.3），毛利率 50%、營益率 40%、淨利率 30%"""
    revs = [100, 110, 105, 120, 118, 125, 122, 130]
    return {(2024 + i // 4, i % 4 + 1): (r * scale * 1e9, r * scale * 5e8, r * scale * 4e8, r * scale * 3e8) for i, r in enumerate(revs)}


def _cumulative_of(series):
    out, run = {}, None
    for (y, q) in sorted(series):
        v = series[(y, q)]
        run = tuple(a + b for a, b in zip(run, v)) if q > 1 and run else v
        out[(y, q)] = run
    return out


def test_parse_finmind_statements_requires_quarter_end_dates_and_all_four_items():
    rows = _fm_rows("2330", {(2025, 1): (100, 50, 40, 30), (2025, 2): (110, 55, 44, 33)})
    rows += [{"date": "2025-09-30", "type": "Revenue", "value": 100}, {"date": "2025-09-30", "type": "GrossProfit", "value": 50}]      # 缺營業利益與淨利：整季不收
    rows += [{"date": "2025-05-15", "type": "Revenue", "value": 1}, {"date": "壞", "type": "Revenue", "value": 1}, "壞列", {"date": "2025-12-31", "type": "Revenue", "value": "x"}]
    got = fetch.parse_finmind_statements(rows)
    assert sorted(got) == [(2025, 1), (2025, 2)] and got[(2025, 2)] == {"revenue": 110, "gross_profit": 55, "op_income": 44, "net_income": 33}
    rows2 = rows + [{"date": "2025-03-31", "type": "Revenue", "value": 999}]
    assert fetch.parse_finmind_statements(rows2)[(2025, 1)]["revenue"] == 999, "同一季重複取最後一列"
    assert fetch.parse_finmind_statements(None) == {} and fetch.parse_finmind_statements([]) == {}


def test_detect_finmind_basis_single_cumulative_and_undecidable():
    single = {f"{2330 + i}": fetch.parse_finmind_statements(_fm_rows(str(2330 + i), _single_series(1 + i * 0.2))) for i in range(3)}
    b, st = fetch.detect_finmind_basis(single)
    assert b == "single" and st["n"] == 6 and st["high"] == 0, (b, st)
    cum = {k: fetch.parse_finmind_statements(_fm_rows(k, _cumulative_of(_single_series(1 + i * 0.2)))) for i, k in enumerate(("2330", "2317", "2454"))}
    b, st = fetch.detect_finmind_basis(cum)
    assert b == "cumulative" and st["n"] == 6 and st["high"] == 6 and 3.5 < st["median"] < 4.5, (b, st)
    b, st = fetch.detect_finmind_basis({"2330": fetch.parse_finmind_statements(_fm_rows("2330", _single_series()))})
    assert b is None and st["n"] == 2, "公司年度不到 4 個：不判斷"
    mixed = {"a": fetch.parse_finmind_statements(_fm_rows("a", _single_series())), "b": fetch.parse_finmind_statements(_fm_rows("b", _single_series())),
             "c": fetch.parse_finmind_statements(_fm_rows("c", _cumulative_of(_single_series()))), "d": fetch.parse_finmind_statements(_fm_rows("d", _cumulative_of(_single_series())))}
    assert fetch.detect_finmind_basis(mixed)[0] is None, "一半單季一半累計：結果不一致，不判斷"
    assert fetch.detect_finmind_basis({})[0] is None
    partial = {k: fetch.parse_finmind_statements(_fm_rows(k, {(2025, q): v for (y, q), v in _single_series().items() if y == 2025 and q < 4})) for k in "abcde"}
    assert fetch.detect_finmind_basis(partial)[0] is None, "沒有四季都齊的年度就沒有依據"


def test_finmind_to_single_converts_cumulative_and_skips_gaps():
    ser = fetch.parse_finmind_statements(_fm_rows("x", _cumulative_of(_single_series())))
    single = fetch.finmind_to_single(ser, "cumulative"), _single_series()
    for yq, v in single[1].items():
        got = single[0][yq]
        assert [round(got[k]) for k in ("revenue", "gross_profit", "op_income", "net_income")] == [round(x) for x in v], yq
    assert fetch.finmind_to_single(ser, "single") == ser
    gap = {k: v for k, v in ser.items() if k != (2024, 2)}
    out = fetch.finmind_to_single(gap, "cumulative")
    assert (2024, 2) not in out and (2024, 3) not in out and (2024, 1) in out and (2024, 4) in out, "缺第 2 季：第 2 季和第 3 季換算不出來要略過，第 4 季（有第 3 季）還是可以"


def _fm_env(per_code, bad=None):
    calls = []
    def fj(url, **k):
        calls.append(url)
        code = url.split("data_id=")[1].split("&")[0]
        if bad and code in bad: return bad[code]
        return {"msg": "success", "status": 200, "data": per_code.get(code, [])}, ""
    return mock.patch.object(fetch, "fetch_json", side_effect=fj), calls


def test_finmind_fin_history_writes_single_quarter_rows_for_single_or_cumulative_sources():
    prices = [{"market": "TW", "code": c, "name": n} for c, n in (("2330", "台積電"), ("2317", "鴻海"), ("2454", "聯發科"), ("2882", "國泰金"), ("00878", "ETF"))] + [{"market": "US", "code": "ORCL", "name": "x"}]
    ser = {c: _single_series(1 + i * 0.3) for i, c in enumerate(("2330", "2317", "2454"))}
    for mode in ("single", "cumulative"):
        reset()
        per = {c: _fm_rows(c, s if mode == "single" else _cumulative_of(s)) for c, s in ser.items()}
        per["2882"] = [{"date": "2025-03-31", "stock_id": "2882", "type": "Revenue", "value": 1e9}]           # 金融業：沒有一般業項目
        p, calls = _fm_env(per)
        with p, mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
            rows = REAL["finmind_fin_history"](prices)
        assert len(calls) == 5 and all("TaiwanStockFinancialStatements" in u and "start_date=" in u for u in calls) and not any("ORCL" in u for u in calls), "台股 5 個代號各查一次（00878 也會查但沒資料），美股不查: %s" % calls
        by = {(r["code"], r["year"], r["quarter"]): r for r in rows}
        assert len(rows) == 3 * config.FINMIND_QUARTERS and all(r["source"] == "FinMind（第三方，非官方）" for r in rows)
        r = by[("2330", 2025, 4)]
        assert round(r["revenue"]) == round(_single_series()[(2025, 4)][0]) and r["gross_margin"] == 50.0 and r["op_margin"] == 40.0 and r["net_margin"] == 30.0, (mode, r)
        assert r["basis"] == ("single" if mode == "single" else "converted") and r["name"] == "台積電"
        assert sorted({(y, q) for (c, y, q) in by if c == "2330"}, reverse=True)[0] == (2025, 4) and len({(y, q) for (c, y, q) in by if c == "2330"}) == config.FINMIND_QUARTERS, "每檔只存最近幾季"
        lg = [x for x in fetch.LOG if x["source"] == "finmind_fin"][0]
        assert lg["status"] == "success" and ("單季值" if mode == "single" else "累計值（已換算成單季）") in lg["detail"] and "2882 國泰金（沒有一般業損益項目" in lg["detail"], lg["detail"]


def test_finmind_fin_history_refuses_undecidable_data_and_stops_when_rate_limited():
    prices = [{"market": "TW", "code": c, "name": c} for c in ("2330", "2317")]
    reset()
    p, calls = _fm_env({"2330": _fm_rows("2330", _single_series())})
    with p, mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        assert REAL["finmind_fin_history"]([{"market": "TW", "code": "2330", "name": "台積電"}]) == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "不寫入" in fetch.LOG[-1]["detail"] and "至少 4 個" in fetch.LOG[-1]["detail"]
    reset()
    codes = ["2330", "2317", "2454", "2412"]
    p, calls = _fm_env({}, bad={"2330": (None, "HTTP Error 402: Payment Required")})              # 代號排序後 2317 先、2330 第二個被擋、其後不再查
    with p, mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        assert REAL["finmind_fin_history"]([{"market": "TW", "code": c, "name": c} for c in codes]) == []
    assert len(calls) == 2 and "疑似被限流，停止本次" in fetch.LOG[-1]["detail"] and fetch.LOG[-1]["status"] == "no_new_data", "402 之後不再繼續打：%s" % calls
    reset()
    p, calls = _fm_env({}, bad={"2330": ({"status": 400, "msg": "bad"}, ""), "2317": ({"data": "x"}, "")})
    with p, mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        assert REAL["finmind_fin_history"]([{"market": "TW", "code": c, "name": c} for c in ("2330", "2317")]) == []
    assert "2330（回傳" in fetch.LOG[-1]["detail"] and "2317（回傳" in fetch.LOG[-1]["detail"]
    reset()
    assert REAL["finmind_fin_history"]([{"market": "US", "code": "ORCL"}]) == [] and fetch.LOG[-1]["status"] == "skipped"


def test_finmind_fin_history_lets_official_data_win_in_its_own_table_flow():
    # 官方資料在 tw_fin_q，FinMind 在獨立的 tw_fin_q_ext：兩邊互不覆蓋，網頁決定用哪個
    reset()
    ser = {c: _single_series(1 + i * 0.3) for i, c in enumerate(("2330", "2317", "2454"))}
    p, calls = _fm_env({c: _fm_rows(c, s) for c, s in ser.items()})
    with p, mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([{"code": "2330", "year": 2026, "quarter": 3}], "")), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["finmind_fin_history"]([{"market": "TW", "code": c, "name": c} for c in ser])
    assert len(rows) == 3 * config.FINMIND_QUARTERS, "官方已經有的季度，FinMind 照樣存在自己的表（網頁決定要不要用）"


def test_parse_nasdaq_profile_maps_sectors_and_refuses_guesses():
    prof = lambda sym, sec, ind="Semiconductors": {"data": {"CompanyName": {"value": "x"}, "Symbol": {"label": "Symbol", "value": sym}, "Industry": {"label": "Industry", "value": ind}, "Sector": {"label": "Sector", "value": sec}}}
    row, why = fetch.parse_nasdaq_profile(prof("ARM", "Technology"), "arm")
    assert why is None and row == {"symbol": "ARM", "sector_en": "Technology", "industry_en": "Semiconductors", "sector": "資訊科技", "source": "Nasdaq 公司簡介（分類和 GICS 不完全相同）"}
    for en, zh in (("Health Care", "醫療保健"), ("Finance", "金融"), ("Consumer Discretionary", "非必需消費"), ("Consumer Staples", "必需消費"), ("Industrials", "工業"), ("Energy", "能源"),
                   ("Utilities", "公用事業"), ("Basic Materials", "原物料"), ("Real Estate", "不動產"), ("Telecommunications", "通訊服務"), ("Miscellaneous", "其他／ETF")):
        assert fetch.parse_nasdaq_profile(prof("X", en), "X")[0]["sector"] == zh, en
    assert fetch.parse_nasdaq_profile(prof("X", "Technology", ""), "X")[0]["industry_en"] is None
    assert "不在對照表" in fetch.parse_nasdaq_profile(prof("X", "Crypto Things"), "X")[1] and fetch.parse_nasdaq_profile(prof("X", "Crypto Things"), "X")[0] is None
    assert "不是 AAPL" in fetch.parse_nasdaq_profile(prof("MSFT", "Technology"), "AAPL")[1]
    assert "沒有 Sector" in fetch.parse_nasdaq_profile({"data": {"Symbol": {"value": "X"}, "Sector": {"value": None}}}, "X")[1]
    for bad in (None, {}, {"data": None}, {"data": {}}, {"data": "x"}, []):
        assert fetch.parse_nasdaq_profile(bad, "X")[0] is None and "沒有公司簡介" in fetch.parse_nasdaq_profile(bad, "X")[1], bad
    sm = set(fetch.NASDAQ_SECTOR_ZH.values()); assert sm == {"資訊科技", "通訊服務", "非必需消費", "必需消費", "醫療保健", "金融", "工業", "能源", "公用事業", "原物料", "不動產", "其他／ETF"}


def test_us_sector_lookup_skips_fresh_handles_etfs_and_never_turns_red():
    reset()
    hold = [{"market": "US", "symbol": "orcl", "name": "甲骨文"}, {"market": "US", "symbol": "ORCL", "name": "dup"}, {"market": "US", "symbol": "ARM", "name": "ARM"}, {"market": "US", "symbol": "MUU", "name": "槓桿"},
            {"market": "US", "symbol": "XYZ", "name": "擋"}, {"market": "TW", "symbol": None, "name": "台積電"}, {"market": "US", "symbol": None, "name": "沒代號"}]
    body = lambda sym, sec: json.dumps({"data": {"Symbol": {"value": sym}, "Industry": {"value": "Ind"}, "Sector": {"value": sec}}}).encode()
    seen = []
    def raw(url, timeout=None, headers=None):
        sym = url.split("/company/")[1].split("/")[0]; seen.append(sym)
        if sym == "ARM": return 200, "application/json", body("ARM", "Technology"), ""
        if sym == "MUU": return 200, "application/json", json.dumps({"data": None}).encode(), ""
        if sym == "XYZ": return 403, "text/html", b"blocked", "HTTP 403"
        return 200, "application/json", body(sym, "Technology"), ""
    recent = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([{"symbol": "ORCL", "fetched_at": recent}], "")), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["us_sector_lookup"](hold)
    assert sorted(seen) == ["ARM", "MUU", "XYZ"], "ORCL 3 天前查過，不重查；台股與沒代號的不查；重複的只算一次: %s" % seen
    assert [r["symbol"] for r in rows] == ["ARM"] and rows[0]["sector"] == "資訊科技"
    lg = [x for x in fetch.LOG if x["source"] == "us_sector"][0]
    assert lg["status"] == "success" and "新查到 1 檔" in lg["detail"] and "ARM→Technology" in lg["detail"] and "MUU（沒有公司簡介" in lg["detail"] and "XYZ（HTTP 403" in lg["detail"] and "1 檔 45 天內查過" in lg["detail"], lg["detail"]
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "查不到的（ETF、被擋）只記沒有資料，不讓工作變紅"
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_sector_lookup"]([{"market": "US", "symbol": "ARM"}]) == []
    assert fetch.LOG[-1]["status"] == "no_new_data"
    reset()
    assert REAL["us_sector_lookup"]([]) == [] and fetch.LOG[-1]["status"] == "skipped" and REAL["us_sector_lookup"]([{"market": "TW", "name": "台積電"}]) == []


def _wk_days(n, end="2026-11-11"):
    out, d_ = [], date.fromisoformat(end)
    while len(out) < n:
        if d_.weekday() < 5: out.append(d_.isoformat())
        d_ -= timedelta(days=1)
    return out[::-1]


def _hist_rows(n=260, spike_at=200, spike_high=150.0, end="2026-11-11"):
    days = _wk_days(n, end)
    return [{"as_of": d_, "close": 100.0 + (i % 7), "high": spike_high if i == spike_at else 101.0 + (i % 7), "low": 99.0 + (i % 7)} for i, d_ in enumerate(days)]


def _stats(hist, as_of="2026-11-12", close=120.0, err=None):
    reset()
    with mock.patch.object(fetch, "sb_request", return_value=(hist, err)) as sq:
        out = fetch._watch_hist_stats("3189", "TWSE", as_of, close)
    return out, sq


def test_watch_hist_stats_computes_52w_high_with_date_change_and_20_day_return_with_sanity_checks():
    hist = _hist_rows(); days = [r["as_of"] for r in hist]
    out, sq = _stats(hist)
    assert "tw_hist?select=as_of,close,high,low&market=eq.TWSE&code=eq.3189&as_of=gte.2025-11-12&order=as_of.asc" in sq.call_args[0][1], sq.call_args[0][1]
    assert out["high_52w"] == 150.0 and out["high_52w_date"] == days[200] and out["low_52w"] == 99.0 and out["range_source"] == "自建歷史（260 個交易日）", out
    prev = hist[-1]; assert out["prev_close"] == prev["close"] and out["chg_pct"] == round((120.0 / prev["close"] - 1) * 100, 2), "前一個交易日收盤與漲跌"
    ref = hist[-20]; assert out["ret_20d"] == round((120.0 / ref["close"] - 1) * 100, 2), "近 20 個交易日：往前數第 20 筆"
    # 今天的收盤價比歷史上所有高點都高：最高價就是今天
    out, _s = _stats(hist, close=160.0); assert out["high_52w"] == 160.0 and out["high_52w_date"] == "2026-11-12"
    # 今天收盤比最低價還低：最低價是今天
    out, _s = _stats(hist, close=80.0); assert out["low_52w"] == 80.0
    # 歷史已經包含資料日當天：前一筆是更早的一天，當天那筆不當成「前一日」
    same = hist + [{"as_of": "2026-11-12", "close": 120.0, "high": 121.0, "low": 118.0}]
    out, _s = _stats(same); assert out["prev_close"] == prev["close"] and out["high_52w"] == 150.0
    # 歷史不夠長：不給 52 週，但漲跌照給；並記一筆說明
    out, _s = _stats(hist[-100:]); assert "high_52w" not in out and "chg_pct" in out and "ret_20d" in out and "只有 100 個交易日" in [x for x in fetch.LOG if x["source"] == "watch_hist:3189"][0]["detail"]
    # 前一筆太舊（超過 10 天）：不算漲跌；20 日前那筆日期對不上（歷史有缺漏）：不算 20 日
    old = [dict(r) for r in hist[:-1]]; out, _s = _stats(old, as_of="2026-11-30"); assert "chg_pct" not in out
    gap = [r for i, r in enumerate(hist) if not (len(hist) - 25 <= i <= len(hist) - 8)]; out, _s = _stats(gap); assert "ret_20d" not in out, out
    # 空的、出錯、壞列：不拋例外
    out, _s = _stats([]); assert out == {} and "沒有 3189" in fetch.LOG[-1]["detail"]
    out, _s = _stats(None, err="HTTP 500"); assert out == {} and "HTTP 500" in fetch.LOG[-1]["detail"]
    bad = [{"as_of": "2026-11-10", "close": "壞", "high": 1, "low": 1}, {"x": 1}, "字串"] + hist; out, _s = _stats(bad); assert out["high_52w"] == 150.0, "壞列略過、其他照算"
    assert fetch._watch_hist_stats("X", "TWSE", "2026-11-12", None) == {} and fetch._watch_hist_stats("X", "TWSE", None, 5.0) == {}


def test_watch_valuation_attaches_history_stats_to_each_candidate_row_and_one_failure_does_not_stop_the_others():
    reset()
    twse_px = [{"Code": "3189", "Name": "景碩", "ClosingPrice": "120.00", "Date": "1151112"}, {"Code": "8046", "Name": "南電", "ClosingPrice": "130.00", "Date": "1151112"}]
    twse_pe = [{"Code": "3189", "Name": "景碩", "PEratio": "30.1", "PBratio": "4.2", "DividendYield": "1.0", "Date": "1151112"}, {"Code": "8046", "Name": "南電", "PEratio": "40.1", "PBratio": "5.2", "DividendYield": "0.8", "Date": "1151112"}]
    def sbr(method, path, body=None, prefer=None):
        if "private_watchlist" in path: return [{"code": "3189"}, {"code": "8046"}], None
        if "code=eq.8046" in path: raise RuntimeError("boom")
        return _hist_rows(), None
    def fj(url, headers=None, timeout=None):
        if url == config.TWSE_AVG_URL: return twse_px, ""
        if "BWIBBU_ALL" in url: return twse_pe, ""
        return [], "空的回應"
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=sbr), mock.patch.object(fetch, "fetch_json", side_effect=fj):
        rows = REAL["watch_valuation"]()
    by = {r["code"]: r for r in rows}
    assert sorted(by) == ["3189", "8046"] and by["3189"]["high_52w"] == 150.0 and by["3189"]["high_52w_date"] and by["3189"]["chg_pct"] is not None and by["3189"]["ret_20d"] is not None, by.get("3189")
    assert by["8046"]["close"] == 130.0 and by["8046"]["pe"] == 40.1 and by["8046"]["high_52w"] is None and by["8046"]["chg_pct"] is None, "一檔行情統計出錯：只少那幾個欄位，其他欄位與其他檔照常"
    assert any(x["source"] == "watch_hist:8046" and x["status"] == "failure" for x in fetch.LOG)
    assert set(by["3189"]) == set(by["8046"]), "所有列欄位一致（一次寫入）"


def test_leveraged_underlying_extracts_the_stock_ticker_from_the_etf_name_and_never_guesses():
    f = fetch.leveraged_underlying
    assert f("2倍做多MU ETF") == "MU" and f("2倍做多INTC ETF") == "INTC" and f("2倍做空TSLA ETF") == "TSLA" and f("2X Long NVDA ETF") == "NVDA" and f("3x BULL AAPL ETF") == "AAPL" and f("2倍做多BRK.B ETF") == "BRK.B"
    for bad in ("元大台灣50正2", "槓桿", "2倍做多 ETF", "Direxion Daily Semiconductor Bull 3X", "", None, "2倍做多abc ETF"):
        assert f(bad) is None, bad


def test_fetch_json_treats_an_unfollowable_307_or_308_like_a_temporary_block_and_waits_longer():
    import urllib.error
    for code in (307, 308, 403):
        sleeps = []
        err = urllib.error.HTTPError("https://x.test/a", code, "Temporary Redirect", {}, None)
        with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=err), mock.patch.object(fetch.time, "sleep", side_effect=sleeps.append):
            data, why = fetch.fetch_json("https://x.test/a")
        assert data is None and str(code) in why and sleeps and all(s >= config.BLOCK_BACKOFF_SECONDS for s in sleeps), "%d：等得比較久（被擋）: %s" % (code, sleeps)
    sleeps = []
    err = urllib.error.HTTPError("https://x.test/a", 404, "Not Found", {}, None)
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=err), mock.patch.object(fetch.time, "sleep", side_effect=sleeps.append):
        fetch.fetch_json("https://x.test/a")
    assert sleeps and all(s < config.BLOCK_BACKOFF_SECONDS for s in sleeps), "404 不當成被擋: " + str(sleeps)


def test_us_index_long_history_fetches_ten_years_for_both_indexes_and_never_turns_the_job_red():
    reset()
    seen = []
    def fr(sid, low, high, days=None):
        seen.append((sid, days))
        return [{"series_id": sid, "label": "x", "as_of": "2016-11-14", "value": 2100.0, "source": "FRED"}, {"series_id": sid, "label": "x", "as_of": "2026-11-10", "value": 6800.0, "source": "FRED"}] if sid == "SP500" else None
    with mock.patch.object(fetch, "fred_history_rows", side_effect=fr):
        rows = REAL["us_index_long_history"]()
    assert [s for s, _d in seen] == ["SP500", "NASDAQCOM", "VIXCLS", "VXNCLS"] and all(d == config.US_LONG_YEARS * 366 for _s, d in seen), "兩個指數與 VIX、VXN 都要 10 年: " + str(seen)
    assert len(rows) == 2 and {r["series_id"] for r in rows} == {"SP500"}
    st = {x["source"]: x["status"] for x in fetch.LOG}; assert st["us_index_long:SP500"] == "success" and st["us_index_long:NASDAQCOM"] == "no_new_data" and st["us_index_long:VIXCLS"] == "no_new_data" and not [x for x in fetch.LOG if x["status"] == "failure"], "抓不到的只記沒有資料"
    # fred_history_rows 的 days 參數：不給就是約一年，給了就用給的
    captured = []
    def fj(url, **k):
        captured.append(url); return {"observations": []}, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(config, "fred_key", return_value="k"):
        fetch.fred_history_rows("SP500", 500.0, 30000.0); fetch.fred_history_rows("SP500", 500.0, 30000.0, days=3660)
    s1 = captured[0].split("observation_start=")[1]; s2 = captured[1].split("observation_start=")[1]
    assert (date.fromisoformat(s1) - date.fromisoformat(s2)).days == 3660 - config.FRED_HISTORY_DAYS and (TODAY - date.fromisoformat(s1)).days == config.FRED_HISTORY_DAYS, "預設約一年、指定天數照給: " + s1 + " / " + s2


def test_run_job_tw_backfill_persists_us_index_long_history_and_isolates_a_crash():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    long_rows = [{"series_id": "SP500", "label": "x", "as_of": "2026-11-10", "value": 6800.0, "source": "FRED"}]
    def go(**kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_backfill", return_value={}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"),
                  mock.patch.object(fetch, "us_index_long_history", **kw), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job("tw_backfill", dry_run=False)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(return_value=long_rows)
    assert ("macro_series", long_rows, "series_id,as_of") in tables and st["module:M35"] == "success", "寫進 macro_series，主鍵是 series_id,as_of"
    reset(); tables, st = go(side_effect=RuntimeError("boom"))
    assert st["module:M35"] == "failure" and ("macro_series", long_rows, "series_id,as_of") not in tables and "market_stats" in [n for n, _r, _c in tables], "這個模組壞掉不影響其他回補"


def test_fetch_raw_guards_only_nasdaq_with_waited_retries_and_a_cooldown_after_a_confirmed_block():
    N = "https://api.nasdaq.com/api/quote/SPY/info?assetclass=etf"
    ok_, blk = (200, "application/json", b"{}", ""), (403, "text/html", b"denied", "HTTP 403")
    def run(seq, url=N, times=None):
        calls, sleeps = [], []
        def once(u, timeout=None, headers=None):
            calls.append(u); return seq[min(len(calls) - 1, len(seq) - 1)]
        clock = iter(times) if times else None
        with mock.patch.object(fetch, "_fetch_raw_once", side_effect=once), mock.patch.object(fetch.time, "sleep", side_effect=sleeps.append), \
                (mock.patch.object(fetch.time, "monotonic", side_effect=lambda: next(clock)) if clock else mock.patch.object(fetch.time, "monotonic", return_value=1000.0)):
            res = fetch.fetch_raw(url)
        return res, len(calls), sleeps
    def fresh(): fetch._NASDAQ_GUARD.update({"blocked_until": 0.0, "last": None}); fetch.LOG.clear()
    fresh(); res, n, sl = run([blk], url="https://www.twse.com.tw/x"); assert res == blk and n == 1 and sl == [], "不是 Nasdaq：不重試、不等"
    fresh(); res, n, sl = run([ok_]); assert res == ok_ and n == 1 and sl == [], "成功：一次就好"
    fresh(); res, n, sl = run([(404, "", b"", "HTTP 404")]); assert res[0] == 404 and n == 1 and sl == [], "404 不重試"
    fresh(); res, n, sl = run([blk, ok_]); assert res == ok_ and n == 2 and len(sl) == 1 and 20 <= sl[0] < 24 and fetch._NASDAQ_GUARD["blocked_until"] == 0.0 and not fetch.LOG, "403 之後等 20 秒再試就成功：不進冷卻、不記失敗"
    fresh(); res, n, sl = run([blk, blk, ok_]); assert res == ok_ and n == 3 and len(sl) == 2 and 20 <= sl[0] < 24 and 60 <= sl[1] < 64, "第二次還是 403：再等 60 秒"
    fresh(); res, n, sl = run([(None, "", b"", "逾時"), ok_]); assert res == ok_ and n == 2, "逾時也會再試一次"
    # 一直被擋：試 3 次、進冷卻、記一筆說明；冷卻期間直接回傳不再打；冷卻過了再試
    fresh(); res, n, sl = run([blk], times=[1000.0, 1000.0, 1000.0]); assert res[0] == 403 and n == 3 and len(sl) == 2, "試 3 次"
    g = [x for x in fetch.LOG if x["source"] == "nasdaq_guard"]; assert len(g) == 1 and g[0]["status"] == "no_new_data" and "試了 3 次仍被擋" in g[0]["detail"] and "10 分鐘內不再打 Nasdaq" in g[0]["detail"], g
    assert fetch._NASDAQ_GUARD["blocked_until"] == 1000.0 + config.NASDAQ_COOLDOWN
    res, n, sl = run([ok_], times=[1100.0]); assert res[0] == 403 and n == 0 and sl == [] and "本次執行已確認 Nasdaq 暫時擋住，不再重試" in res[3], "冷卻中：不打、不等"
    res, n, sl = run([ok_], times=[1000.0 + config.NASDAQ_COOLDOWN + 1, 0, 0]); assert res == ok_ and n == 1 and fetch._NASDAQ_GUARD["blocked_until"] == 0.0, "冷卻過了：再試，成功就解除"
    # 逾時（不是明確被擋）：不進冷卻
    fresh(); res, n, sl = run([(None, "", b"", "逾時")], times=[1000.0] * 3); assert res[0] is None and n == 3 and fetch._NASDAQ_GUARD["blocked_until"] == 0.0 and not fetch.LOG, "只是逾時：試 3 次但不進冷卻"
    # 冷卻只影響 Nasdaq
    fresh(); fetch._NASDAQ_GUARD.update({"blocked_until": 5000.0, "last": blk})
    res, n, sl = run([ok_], url="https://www.twse.com.tw/x", times=[1.0]); assert res == ok_ and n == 1, "冷卻中，其他網站照常"
    fresh()


def test_us_sector_lookup_uses_the_underlying_stock_for_leveraged_etfs_and_says_so():
    reset()
    hold = [{"market": "US", "symbol": "MUU", "name": "2倍做多MU ETF", "kind": "leveraged_etf"}, {"market": "US", "symbol": "INTW", "name": "2倍做多INTC ETF", "kind": "leveraged_etf"}, {"market": "US", "symbol": "MU", "name": "美光", "kind": "stock"},
            {"market": "US", "symbol": "ZZZU", "name": "槓桿 ETF（名稱看不出標的）", "kind": "leveraged_etf"}, {"market": "US", "symbol": "SPYX", "name": "某 ETF", "kind": "etf"}]
    body = lambda sym, sec: json.dumps({"data": {"Symbol": {"value": sym}, "Industry": {"value": "Semis"}, "Sector": {"value": sec}}}).encode()
    seen = []
    def raw(url, timeout=None, headers=None):
        sym = url.split("/company/")[1].split("/")[0]; seen.append(sym)
        if sym in ("MU", "INTC"): return 200, "application/json", body(sym, "Technology"), ""
        return 200, "application/json", json.dumps({"data": None}).encode(), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["us_sector_lookup"](hold)
    by = {r["symbol"]: r for r in rows}
    assert sorted(by) == ["INTW", "MU", "MUU"] and by["MUU"]["sector"] == "資訊科技" and by["INTW"]["sector"] == "資訊科技" and by["MU"]["sector"] == "資訊科技", "槓桿 ETF 用標的的產業: " + str(sorted(by))
    assert "依標的 MU" in by["MUU"]["source"] and "槓桿 ETF 本身沒有產業" in by["MUU"]["source"] and "依標的" not in by["MU"]["source"], "來源寫明依哪一檔標的"
    assert sorted(seen) == ["INTC", "MU", "SPYX", "ZZZU"] and seen.count("MU") == 1, "標的 MU 同時是持股：只查一次；ETF 本身不去查（槓桿 ETF），一般 ETF 照舊: " + str(seen)
    lg = [x for x in fetch.LOG if x["source"] == "us_sector"][0]["detail"]
    assert "MUU→Technology（依標的 MU）" in lg and "ZZZU（沒有公司簡介" in lg and "SPYX（沒有公司簡介" in lg, lg
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"blocked", "HTTP 403")), mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_sector_lookup"]([{"market": "US", "symbol": "MUU", "name": "2倍做多MU ETF", "kind": "leveraged_etf"}]) == []
    assert "MUU（標的 MU：HTTP 403" in fetch.LOG[-1]["detail"] and fetch.LOG[-1]["status"] == "no_new_data", "標的查不到：寫出是查哪一檔、為什麼"


def test_run_job_daily_persists_us_sector_and_finmind_tables_and_isolates_them():
    reset()
    sec = [{"symbol": "ARM", "sector_en": "Technology", "industry_en": "Semiconductors", "sector": "資訊科技", "source": "x"}]
    fin = [{"code": "2330", "name": "台積電", "year": 2026, "quarter": 2, "revenue": 1.0, "gross_profit": 0.5, "op_income": 0.4, "net_income": 0.3, "gross_margin": 50, "op_margin": 40, "net_margin": 30, "basis": "single", "source": "FinMind（第三方，非官方）"}]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(job, dry, **kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "us_sector_lookup", **(kw.get("sec") or {"return_value": sec})),
                  mock.patch.object(fetch, "finmind_fin_history", **(kw.get("fin") or {"return_value": fin})),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append({n: (r, c) for n, r, c in tables}))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    for job in ("tw_close", "full"):
        reset(); tables, st = go(job, False)
        assert tables["us_sector_auto"] == (sec, "symbol") and tables["tw_fin_q_ext"] == (fin, "code,year,quarter") and st["module:M26"] == "success" and st["module:M25"] == "success", job
    reset(); tables, st = go("tw_close", True)
    assert "module:M25" not in st and "module:M26" not in st, "dry-run 不碰第三方與 Nasdaq"
    reset(); tables, st = go("tw_close", False, sec={"side_effect": RuntimeError("boom")})
    assert st["module:M26"] == "failure" and st["module:M25"] == "success" and tables["us_sector_auto"][0] == [] and tables["tw_fin_q_ext"][0] == fin
    reset(); tables, st = go("tw_close", False, fin={"side_effect": RuntimeError("boom")})
    assert st["module:M25"] == "failure" and st["module:M26"] == "success" and "module:M07" in st, "FinMind 壞了不影響官方三率與其他模組"


def test_fetch_json_reports_what_it_received_for_empty_and_non_json_bodies():
    import gzip as _gz
    reset()
    class Hdr(dict): pass
    class Resp:
        def __init__(self, body, enc=None): self._b = body; self.headers = Hdr({"Content-Encoding": enc} if enc else {})
        def read(self): return self._b
        def __enter__(self): return self
        def __exit__(self, *a): return False
    def run(body, enc=None):
        calls = []
        def op(req, timeout=None):
            calls.append(1); return Resp(body, enc)
        with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=op), mock.patch.object(fetch.time, "sleep") as sl:
            res = fetch.fetch_json("https://x/y")
        return res, len(calls), [c.args[0] for c in sl.call_args_list]
    (data, err), n, sleeps = run(b"")
    assert data is None and err == fetch.EMPTY_RESPONSE_MSG and fetch.is_empty_response(err) and n == config.RETRIES and sleeps == [config.BACKOFF_SECONDS, config.BACKOFF_SECONDS * 2], "空的回應：重試後回傳明確原因"
    (data, err), n, _ = run("  \n\ufeff ".encode("utf-8"))
    assert data is None and fetch.is_empty_response(err), "只有空白或 BOM 也算空的"
    (data, err), n, _ = run(_gz.compress(b""), "gzip")
    assert data is None and fetch.is_empty_response(err), "壓縮後是空的也算空的"
    (data, err), n, _ = run("<html><body>Service Unavailable</body></html>".encode())
    assert data is None and not fetch.is_empty_response(err) and "不是 JSON（" in err and "位元組" in err and "<html><body>Service Unavailable</body>" in err, err
    (data, err), n, _ = run(b"\xef\xbb\xbf" + b'{"ok": 1}')
    assert data == {"ok": 1} and err == "" and n == 1, "有 BOM 的正常 JSON 照常"
    long_html = ("<html>" + "x" * 500).encode()
    assert len(run(long_html)[0][1]) < 150, "錯誤訊息裡的開頭只截 60 個字，不會把整頁貼進去"
    assert not fetch.is_empty_response(None) and not fetch.is_empty_response("HTTP 403") and fetch.is_empty_response("抓取失敗：" + fetch.EMPTY_RESPONSE_MSG)


def test_taifex_modules_treat_empty_responses_as_no_data_but_real_errors_as_failures():
    ad = _last_weekday(date.today() - timedelta(days=1))
    opt, fut, _, _ = _tw_chain(ad)
    E, H = fetch.EMPTY_RESPONSE_MSG, "不是 JSON（120 位元組；開頭是 '<html>'）"
    def go(e_opt, e_fut):
        reset()
        def fj(url, **k):
            if url == config.TAIFEX_OPT_URL: return (None, e_opt) if e_opt else (opt, "")
            if url == config.TAIFEX_DAILY_URL: return (None, e_fut) if e_fut else (fut, "")
        with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>error</html>", "")):    # HTML 錯誤頁：CSV 後備也讀不了
            res = REAL["tw_options_snapshot"]()
        return res, [x for x in fetch.LOG if x["source"] == "tw_options"][-1]
    res, lg = go(E, None); assert res == [] and lg["status"] == "no_new_data" and "空內容" in lg["detail"] and "下次再抓" in lg["detail"], lg
    res, lg = go(E, E); assert res == [] and lg["status"] == "no_new_data"
    res, lg = go(H, None); assert res == [] and lg["status"] == "failure" and "不是 JSON" in lg["detail"], "HTML 錯誤頁是真的錯誤，要標失敗"
    res, lg = go(E, "HTTP 403"); assert res == [] and lg["status"] == "failure", "一個空、一個被擋：有真的錯誤，要標失敗"
    res, lg = go(None, None); assert len(res) == 1 and lg["status"] == "success"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, E)):
        assert REAL["tw_pcr_official"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "暫時" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, H)), mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>error</html>", "")):
        assert REAL["tw_pcr_official"]() == []
    assert fetch.LOG[-1]["status"] == "failure"


def _yh_json(days_vals, off=0, hour_utc=16):
    """days_vals: [(日期字串, 收盤價或 None)] → Yahoo chart 格式"""
    ts = [int(datetime(int(d[:4]), int(d[5:7]), int(d[8:]), hour_utc, tzinfo=timezone.utc).timestamp()) - off for d, v in days_vals]
    return {"chart": {"result": [{"meta": {"gmtoffset": off}, "timestamp": ts, "indicators": {"quote": [{"close": [v for d, v in days_vals]}]}}], "error": None}}


def test_parse_yahoo_chart_converts_exchange_local_dates_and_skips_nulls():
    rows = fetch.parse_yahoo_chart(_yh_json([("2026-11-03", 7500.5), ("2026-11-04", None), ("2026-11-05", 7600.0)]))
    assert rows == [("2026-11-03", 7500.5), ("2026-11-05", 7600.0)]
    j = _yh_json([("2026-11-03", 7500.0)], off=-18000, hour_utc=21)                       # 美東：UTC 21:00 ＝ 當地 16:00，同一天
    assert fetch.parse_yahoo_chart(j) == [("2026-11-03", 7500.0)]
    for bad in (None, {}, {"chart": {"result": None}}, {"chart": {"result": [{}]}}, _yh_json([("2026-11-03", None)]), "x"):
        assert fetch.parse_yahoo_chart(bad) is None, bad


def _yh_env(fred_rows, yahoo_by_sym, status=200):
    def req(method, path, *a, **k):
        sid = path.split("series_id=eq.")[1].split("&")[0]
        return fred_rows.get(sid, []), ""
    def raw(url, timeout=None, headers=None):
        sym = urllib_unquote(url.split("/chart/")[1].split("?")[0])
        if status != 200: return status, "text/html", b"limited", f"HTTP {status}"
        return 200, "application/json", json.dumps(yahoo_by_sym[sym]).encode(), ""
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep")]


def urllib_unquote(s):
    import urllib.parse as up
    return up.unquote(s)


def test_yahoo_fill_adds_only_newer_days_after_cross_checking_against_fred():
    today = fetch._now_et().date()
    dd = lambda n: (today - timedelta(days=n)).isoformat()
    fred = {"SP500": [{"as_of": dd(n), "value": 7000.0 + n} for n in (10, 9, 8, 7, 6)], "DCOILWTICO": [{"as_of": dd(n), "value": 60.0} for n in (10, 9, 8, 7, 6)], "DCOILBRENTEU": []}
    sp = [(dd(n), 7000.0 + n) for n in (10, 9, 8, 7, 6)] + [(dd(5), 7100.0), (dd(3), 7110.0), (dd(1), 7120.0), (today.isoformat(), 7130.0)]       # 今天的不收
    cl = [(dd(n), 60.5) for n in (10, 9, 8, 7, 6)] + [(dd(2), 61.0), (dd(1), 400.0)]                                                              # 400 超出合理範圍
    reset()
    ps = _yh_env(fred, {"^GSPC": _yh_json(sp), "CL=F": _yh_json(cl), "BZ=F": _yh_json(cl)})
    rows = _with(ps, lambda: REAL["yahoo_fill"]())
    by = {}
    for r in rows: by.setdefault(r["series_id"], []).append(r)
    assert [r["as_of"] for r in by["SP500"]] == [dd(5), dd(3), dd(1)] and all(r["source"].startswith("Yahoo Finance（非官方") and r["value"] > 7000 for r in by["SP500"]), "只補比 FRED 更新的日子，今天的不收"
    assert [r["as_of"] for r in by["DCOILWTICO"]] == [dd(2)], "超出合理範圍的 400 被拒"
    assert "DCOILBRENTEU" not in by, "資料庫沒有 Brent 的 FRED 資料可比對：不補"
    lg = [x for x in fetch.LOG if x["source"] == "yahoo_fill"][0]
    assert lg["status"] == "success" and "SP500：補 3 天" in lg["detail"] and "DCOILBRENTEU（資料庫沒有 FRED 資料可比對" in lg["detail"], lg["detail"]
    # 重疊日子差太多（口徑不同）→ 整批不用
    reset()
    bad = [(dd(n), 7000.0 + n + 200) for n in (10, 9, 8, 7, 6)] + [(dd(1), 7300.0)]
    rows = _with(_yh_env(fred, {"^GSPC": _yh_json(bad), "CL=F": _yh_json(cl), "BZ=F": _yh_json(cl)}), lambda: REAL["yahoo_fill"]())
    assert not [r for r in rows if r["series_id"] == "SP500"] and "口徑不同，不用" in fetch.LOG[-1]["detail"]
    # 沒有重疊 → 不用；FRED 已最新 → 不需要補
    reset()
    rows = _with(_yh_env(fred, {"^GSPC": _yh_json([(dd(1), 7100.0)]), "CL=F": _yh_json([(dd(1), 60.0)]), "BZ=F": _yh_json([(dd(1), 60.0)])}), lambda: REAL["yahoo_fill"]())
    assert rows == [] and "沒有重疊的日子" in fetch.LOG[-1]["detail"]
    reset()
    fresh = {"SP500": [{"as_of": dd(1), "value": 7100.0}], "DCOILWTICO": [], "DCOILBRENTEU": []}
    rows = _with(_yh_env(fresh, {"^GSPC": _yh_json([(dd(2), 7050.0), (dd(1), 7100.0)]), "CL=F": _yh_json([]), "BZ=F": _yh_json([])}), lambda: REAL["yahoo_fill"]())
    assert rows == [] and "FRED 已經是最新" in fetch.LOG[-1]["detail"]


def test_yahoo_fill_also_fills_vix_vxn_and_nasdaq_composite_when_fred_lags_and_rejects_mismatched_ones():
    today = fetch._now_et().date()
    dd = lambda n: (today - timedelta(days=n)).isoformat()
    assert [x[:2] for x in config.YAHOO_FILL if x[1] in ("VIXCLS", "VXNCLS", "NASDAQCOM")] == [("^VIX", "VIXCLS"), ("^VXN", "VXNCLS"), ("^IXIC", "NASDAQCOM")], "設定裡有這三個"
    fred = {"SP500": [], "DCOILWTICO": [], "DCOILBRENTEU": [],
            "VIXCLS": [{"as_of": dd(n), "value": 18.0 + n * 0.1} for n in (5, 4, 3, 2)], "VXNCLS": [{"as_of": dd(n), "value": 22.0 + n * 0.1} for n in (5, 4, 3, 2)],
            "NASDAQCOM": [{"as_of": dd(n), "value": 24000.0 + n} for n in (5, 4, 3, 2)]}
    vix = [(dd(n), 18.0 + n * 0.1) for n in (5, 4, 3, 2)] + [(dd(1), 19.5), (today.isoformat(), 20.0)]                      # FRED 只到 dd(2)；Yahoo 多一天（今天的不收）
    vxn = [(dd(n), 22.0 + n * 0.1) for n in (5, 4, 3, 2)] + [(dd(1), 23.5)]
    ixic = [(dd(n), 24300.0 + n) for n in (5, 4, 3, 2)] + [(dd(1), 24400.0)]                                              # 和 FRED 差 1.2%，超過 0.5%：口徑不同，整批不用
    reset()
    rows = _with(_yh_env(fred, {"^GSPC": _yh_json([]), "CL=F": _yh_json([]), "BZ=F": _yh_json([]), "^VIX": _yh_json(vix), "^VXN": _yh_json(vxn), "^IXIC": _yh_json(ixic)}), lambda: REAL["yahoo_fill"]())
    by = {}
    for r in rows: by.setdefault(r["series_id"], []).append(r)
    assert [r["as_of"] for r in by["VIXCLS"]] == [dd(1)] and by["VIXCLS"][0]["value"] == 19.5 and by["VIXCLS"][0]["source"].startswith("Yahoo Finance（非官方"), "VIX 補上 FRED 還沒有的那一天、今天的不收: " + str(by.get("VIXCLS"))
    assert [r["as_of"] for r in by["VXNCLS"]] == [dd(1)], "VXN 也補"
    assert "NASDAQCOM" not in by and "口徑不同，不用" in [x for x in fetch.LOG if x["source"] == "yahoo_fill"][0]["detail"], "Nasdaq 綜合和 FRED 差太多：整批不用"


def test_yahoo_fill_never_turns_the_job_red_when_blocked_or_unconfigured():
    reset()
    rows = _with(_yh_env({"SP500": [{"as_of": "2026-11-01", "value": 7000.0}]}, {}, status=429), lambda: REAL["yahoo_fill"]())
    assert rows == [] and fetch.LOG[-1]["status"] == "no_new_data" and "HTTP 429" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["yahoo_fill"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"
    assert not [x for x in fetch.LOG if x["status"] == "failure"]


def test_probe_round12_reports_tpex_margin_summary_candidates_without_writing():
    reset()
    page = json.dumps({"aaData": [["融資金額(仟元)", "123"]], "reportDate": "115/11/06"}, ensure_ascii=False).encode()
    sw = json.dumps({"paths": {"/tpex_mainboard_margin_balance": {}, "/tpex_other": {}, "/credit_x": {}}}).encode()
    def raw(url, timeout=None, headers=None):
        return (200, "application/json", sw, "") if "swagger" in url else (200, "application/json", page, "")
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up, mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round12()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:tpex_margin_summary"]["status"] == "success" and "融資金額" in by["probe:tpex_margin_summary"]["detail"] and by["probe:tpex_openapi_list"]["status"] == "success" and "/tpex_mainboard_margin_balance" in by["probe:tpex_openapi_list"]["detail"]
    up.assert_not_called()
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(404, "text/html", b"<html>", "")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round12()
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "探測被擋或找不到是正常結果，不能讓工作變紅"


def _to_csv(rows, cols_en2zh, date_slash=False):
    import csv, io
    keys = [k for k in rows[0].keys() if k in cols_en2zh]
    buf = io.StringIO(); w = csv.writer(buf); w.writerow([cols_en2zh[k] for k in keys])
    for r in rows:
        w.writerow([(f"{r[k][:4]}/{r[k][4:6]}/{r[k][6:]}" if date_slash and k == "Date" and len(r[k]) == 8 else r[k]) for k in keys])
    return buf.getvalue()


def test_taifex_csv_is_parsed_into_the_same_rows_as_json_and_gives_identical_results():
    zh = {v: k for k, v in fetch.TAIFEX_CSV_COLS.items()}
    ad = _last_weekday(date.today() - timedelta(days=1))
    opt, fut, _, _ = _tw_chain(ad)
    for slash in (False, True):
        o2 = fetch.parse_taifex_csv(_to_csv(opt, zh, slash)); f2 = fetch.parse_taifex_csv("\ufeff" + _to_csv(fut, zh, slash))
        assert o2 and f2 and o2[0]["Date"] == ad.strftime("%Y%m%d") and o2[0]["CallPut"] in ("買權", "賣權") and "ContractMonth(Week)" in o2[0] and "OpenInterest" in o2[0], "中文欄位換成英文、日期換成 8 位數"
        a = fetch.parse_taifex_snapshot(opt, fut, today=date.today()); b = fetch.parse_taifex_snapshot(o2, f2, today=date.today())
        assert a[2] is None and b[2] is None and a[0] == b[0] and a[1] == b[1], ("CSV 與 JSON 結果要完全一樣", a[2], b[2])
    pcr_zh = "日期,賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,買權未平倉量,買賣權未平倉量比率%\n" + (date.today() - timedelta(days=1)).strftime("%Y/%m/%d") + ",148092,157966,93.75,69770,113305,61.58\n"
    rows = fetch.parse_taifex_csv(pcr_zh); out, bad = fetch.parse_taifex_pcr(rows)
    assert len(out) == 1 and out[0]["pcr_oi"] == 61.58 and out[0]["pcr_vol"] == 93.75 and bad == 0
    for junk in ("", "\n", "只有一欄\n1\n", "A,B\n1,2\n", "日期,契約\n"):
        assert fetch.parse_taifex_csv(junk) is None, repr(junk)
    assert fetch.parse_taifex_csv("日期,契約,未知欄位\n20261106,TXO,x\n,,\n")[0]["未知欄位"] == "x", "不認得的欄位原樣保留，空列略過"
    assert fetch.parse_taifex_csv("日期,契約,成交量\n20261106,TXO\n")[0]["Volume"] == "", "列比欄位少：補空字串，不報錯"


def test_fetch_taifex_falls_back_to_csv_only_when_the_body_is_not_json():
    zh = {v: k for k, v in fetch.TAIFEX_CSV_COLS.items()}
    ad = _last_weekday(date.today() - timedelta(days=1))
    opt, _, _, _ = _tw_chain(ad)
    csvb = _to_csv(opt, zh, True).encode("utf-8-sig")
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "不是 JSON（890797 位元組；開頭是 '日期,契約'）")), mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", csvb, "")):
        rows, err = fetch.fetch_taifex("https://x/opt")
    assert err == "" and len(rows) == len(opt) and rows[0]["Contract"] == "TXO"
    with mock.patch.object(fetch, "fetch_json", return_value=([{"Date": "20261106"}], "")), mock.patch.object(fetch, "fetch_raw", side_effect=AssertionError("JSON 成功時不能再抓一次")):
        assert fetch.fetch_taifex("https://x/opt")[0] == [{"Date": "20261106"}]
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP Error 403: Forbidden")), mock.patch.object(fetch, "fetch_raw", side_effect=AssertionError("被擋時不改抓")):
        assert fetch.fetch_taifex("https://x/opt") == (None, "HTTP Error 403: Forbidden")
    with mock.patch.object(fetch, "fetch_json", return_value=(None, fetch.EMPTY_RESPONSE_MSG)), mock.patch.object(fetch, "fetch_raw", side_effect=AssertionError("空的回應不改抓")):
        assert fetch.is_empty_response(fetch.fetch_taifex("https://x/opt")[1])
    err0 = "不是 JSON（120 位元組；開頭是 '<html>'）"
    with mock.patch.object(fetch, "fetch_json", return_value=(None, err0)), mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html><body>error</body></html>", "")):
        assert fetch.fetch_taifex("https://x/opt") == (None, err0), "HTML 錯誤頁不是 CSV：維持原本的錯誤原因"
    with mock.patch.object(fetch, "fetch_json", return_value=(None, err0)), mock.patch.object(fetch, "fetch_raw", return_value=(500, "", b"", "HTTP 500")):
        assert fetch.fetch_taifex("https://x/opt") == (None, err0)
    # 整條流程：選擇權是 CSV、期貨是 JSON → 照樣存得到快照
    opt2, fut2, _, _ = _tw_chain(ad)
    def fj(url, **k):
        if url == config.TAIFEX_OPT_URL: return None, "不是 JSON（890797 位元組；開頭是 '日期,契約'）"
        return fut2, ""
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", csvb, "")):
        res = REAL["tw_options_snapshot"]()
    assert len(res) == 1 and res[0]["summary"]["fut_settle"] == 41000 and [x for x in fetch.LOG if x["source"] == "tw_options"][0]["status"] == "success"


def _inv(ad, name, rows3):
    ds = ad.strftime("%Y%m%d")
    return [{"Date": ds, "ContractCode": name, "Item": it, "OpenInterest(Long)": str(l), "OpenInterest(Short)": str(s_), "OpenInterest(Net)": str(l - s_)} for it, l, s_ in rows3]


def test_compute_retail_ratio_math_names_and_rejections():
    ad = _last_weekday(date.today() - timedelta(days=1)); ds = ad.strftime("%Y%m%d")
    mtx = [_tx(ds, "202611", 41000, 70000, contract="MTX"), _tx(ds, "202612", 41100, 20000, contract="MTX"), _tx(ds, "202611/202612", 1, 999999, contract="MTX"), _tx(ds, "202611", 1, 5, session="盤後", contract="MTX"), _tx(ds, "202611", 1, 555, contract="TX")]
    inv = _inv(ad, "臺股期貨", [("自營商", 1, 1), ("投信", 1, 1), ("外資及陸資", 1, 1)]) + _inv(ad, "小型臺指期貨", [("自營商", 5000, 12000), ("投信", 100, 300), ("外資及陸資", 20000, 28000)])
    row, why = fetch.compute_retail_ratio(inv, mtx, today=date.today())
    assert why is None and row["contract"] == "小型臺指期貨" and row["total_oi"] == 90000 and row["inst_long"] == 25100 and row["inst_short"] == 40300
    assert row["ratio"] == round((40300 - 25100) / 90000 * 100, 2) == 16.89 and row["as_of"] == ad.isoformat(), "（法人空 − 法人多）÷ 全市場 ＝ 散戶多 − 散戶空"
    assert "不是交易所直接公布" in row["source"]
    neg, _ = fetch.compute_retail_ratio(_inv(ad, "小型臺指期貨", [("自營商", 30000, 1000), ("投信", 100, 300), ("外資及陸資", 40000, 2000)]), mtx, today=date.today()); assert neg["ratio"] < 0, "法人大量做多 → 散戶淨空（負數）"
    def bad(inv_, fut_): return fetch.compute_retail_ratio(inv_, fut_, today=date.today())[1]
    assert "找不到小型臺指期貨" in bad(_inv(ad, "臺股期貨", [("自營商", 1, 1), ("投信", 1, 1), ("外資及陸資", 1, 1)]), mtx) and "臺股期貨" in bad(_inv(ad, "臺股期貨", [("自營商", 1, 1)]), mtx), "找不到要列出實際有哪些契約"
    assert "只找到" in bad(_inv(ad, "小型臺指期貨", [("自營商", 5, 5), ("外資及陸資", 5, 5)]), mtx)
    assert "找不到" in bad(inv, [_tx(ds, "202611", 1, 5, contract="TX")]) and "MTX" in bad(inv, [_tx(ds, "202611", 1, 5, contract="TX")])
    assert "找不到" in bad(inv, [_tx((ad - timedelta(days=1)).strftime("%Y%m%d"), "202611", 1, 70000, contract="MTX")]), "期貨行情的日期和法人資料對不上"
    assert "不合理" in bad(_inv(ad, "小型臺指期貨", [("自營商", 90000, 1), ("投信", 100, 300), ("外資及陸資", 20000, 28000)]), mtx)
    assert fetch.compute_retail_ratio(None, mtx)[1] == "資料格式非預期" and fetch.compute_retail_ratio(inv, [])[1] == "資料格式非預期"
    stale = _inv(date.today() + timedelta(days=3), "小型臺指期貨", [("自營商", 1, 1), ("投信", 1, 1), ("外資及陸資", 1, 1)]); assert "日期異常" in bad(stale, mtx)


def test_tw_retail_ratio_module_json_csv_and_failure_classification():
    ad = _last_weekday(date.today() - timedelta(days=1)); ds = ad.strftime("%Y%m%d")
    mtx = [_tx(ds, "202611", 41000, 90000, contract="MTX")]
    inv = _inv(ad, "小型臺指期貨", [("自營商", 5000, 12000), ("投信", 100, 300), ("外資及陸資", 20000, 28000)])
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(inv, "")), mock.patch.object(fetch, "fetch_taifex", return_value=(mtx, "")):
        res = REAL["tw_retail_ratio"]()
    assert len(res) == 1 and res[0]["ratio"] == 16.89 and [x for x in fetch.LOG if x["source"] == "tw_retail"][0]["status"] == "success" and "+16.89%" in fetch.LOG[-1]["detail"]
    reset()                                                                                                    # 法人資料是 CSV：改走 CSV 後備
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "不是 JSON")), mock.patch.object(fetch, "_taifex_investor_csv", return_value=(inv, "")), mock.patch.object(fetch, "fetch_taifex", return_value=(mtx, "")):
        assert len(REAL["tw_retail_ratio"]()) == 1
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch, "_taifex_investor_csv", return_value=(None, "CSV 抓取失敗")), mock.patch.object(fetch, "fetch_taifex", return_value=(mtx, "")):
        assert REAL["tw_retail_ratio"]() == []
    assert fetch.LOG[-1]["status"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, fetch.EMPTY_RESPONSE_MSG)), mock.patch.object(fetch, "_taifex_investor_csv", return_value=(None, fetch.EMPTY_RESPONSE_MSG)), mock.patch.object(fetch, "fetch_taifex", return_value=(None, fetch.EMPTY_RESPONSE_MSG)):
        assert REAL["tw_retail_ratio"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data", "全是空內容：不當成失敗"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(_inv(ad, "臺股期貨", [("自營商", 1, 1)]), "")), mock.patch.object(fetch, "fetch_taifex", return_value=(mtx, "")):
        assert REAL["tw_retail_ratio"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "找不到小型臺指期貨" in fetch.LOG[-1]["detail"] and "臺股期貨" in fetch.LOG[-1]["detail"], "名稱不對：記沒有資料並列出實際契約名稱，不讓工作變紅"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(_inv(ad, "小型臺指期貨", [("自營商", 100000, 1), ("投信", 1, 1), ("外資及陸資", 1, 1)]), "")), mock.patch.object(fetch, "fetch_taifex", return_value=(mtx, "")):
        assert REAL["tw_retail_ratio"]() == []
    assert fetch.LOG[-1]["status"] == "failure", "數字不合理才算失敗"


def test_run_job_daily_persists_retail_ratio_and_isolates_m28():
    reset()
    row = {"as_of": D(1), "contract": "小型臺指期貨", "inst_long": 1, "inst_short": 2, "total_oi": 10, "ratio": 10.0, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "tw_retail_ratio", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append({n: (r, c) for n, r, c in tables}))]
        for p_ in ps: p_.start()
        try: fetch.run_job("tw_close", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert tables["tw_retail_ratio"] == ([row], "as_of") and st["module:M28"] == "success"
    reset(); tables, st = go(True); assert "module:M28" not in st
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M28"] == "failure" and "module:M07" in st and tables["tw_retail_ratio"][0] == []


def _otc_margin(n=200, units=10000, bad=0, date_s=None):
    d = date_s or ymd(); roc = f"{int(d[:4]) - 1911}{d[4:]}"
    rows = [{"Date": roc, "SecuritiesCompanyCode": f"{3000 + i}", "CompanyName": f"股{i}", "MarginPurchaseBalancePreviousDay": str(units + 100), "MarginPurchase": "50", "MarginSales": "100", "CashRedemption": "50",
             "MarginPurchaseBalance": str(units + (7 if i < bad else 0))} for i in range(n)]                       # 前日 units+100，買 50、賣 100、償 50 → 今日 units
    return rows


def _otc_px(n=200, price=80.0, date_s=None, drop=0):
    d = date_s or ymd(); roc = f"{int(d[:4]) - 1911}{d[4:]}"
    return [{"Date": roc, "SecuritiesCompanyCode": f"{3000 + i}", "CompanyName": f"股{i}", "Close": ("" if i < drop else str(price)), "Change": "0"} for i in range(n)]


def _otc_page(units=2000000, prev_units=2020000, loan_k=100000000, date_s=None, roc=None, ident_ok=True, units_ok=True):
    d = date_s or ymd(); r = roc or f"{int(d[:4]) - 1911}/{d[4:6]}/{d[6:]}"
    fields = ["代號", "名稱", "前資餘額(張)", "資買", "資賣", "現償", "資餘額", "資屬證金", "資使用率(%)", "資限額"]
    sell_k, red_k, buy_k = 16770744, 2860184, 5568509; prev_k = loan_k + sell_k + red_k - buy_k + (0 if ident_ok else 99999)
    u = lambda v: f"{v:,}"
    summ = [["", "合計(張)", u(prev_units), "1", "1", "1", u(units if units_ok else units + 50000), "", "", ""], ["", "融資金(仟元)", u(prev_k), u(buy_k), u(sell_k), u(red_k), u(loan_k), "", "", ""]]
    return json.dumps({"tables": [{"title": "上櫃股票融資融券餘額", "date": r, "fields": fields, "data": [["x"]], "summary": summ, "notes": []}], "stat": "ok"}, ensure_ascii=False).encode()


def _otc_env(margin, px, page=None, page_status=200):
    p = page if page is not None else _otc_page()
    return _ExitStack([mock.patch.object(fetch, "fetch_json", side_effect=lambda url, **k: margin if url == config.TPEX_MARGIN_URL else px),
                       mock.patch.object(fetch, "fetch_raw", return_value=(page_status, "application/json", p if page_status == 200 else b"", "" if page_status == 200 else "HTTP"))])


class _ExitStack:
    def __init__(self, ps): self.ps = ps
    def __enter__(self):
        for p in self.ps: p.start()
        return self
    def __exit__(self, *a):
        for p in reversed(self.ps): p.stop()
        return False


def test_tpex_margin_daily_sums_units_and_value_with_validation():
    reset()
    with _otc_env((_otc_margin(), ""), (_otc_px(), "")):
        rows = REAL["tpex_margin_daily"]()
    assert len(rows) == 1 and rows[0]["units"] == 200 * 10000 and rows[0]["prev_units"] == 200 * 10100 and rows[0]["value"] == 200 * 10000 * 1000 * 80 and rows[0]["n_stocks"] == 200 and rows[0]["n_missing"] == 0 and "融資金額" in rows[0]["source"]
    assert rows[0]["loan"] == 100000000 * 1000 and rows[0]["est_ratio"] == 160.0 and "維持率是自己估算的" in rows[0]["source"], f"維持率＝市值 1,600 億 ÷ 融資金額 1,000 億 ＝ 160%（算法和上市一樣）: {rows[0]}｜{[x['detail'] for x in fetch.LOG if x['source'] == 'tpex_margin']}"
    lg = [x for x in fetch.LOG if x["source"] == "tpex_margin"][0]; assert lg["status"] == "success" and "上櫃融資 2,000,000 張（前日 2,020,000，-0.99%）" in lg["detail"] and "維持率估算 160.00%" in lg["detail"], lg["detail"]
    def last(): return [x for x in fetch.LOG if x["source"] == "tpex_margin"][-1]
    for m, p, st, frag in (((_otc_margin(), ""), (_otc_px(date_s=ymd(-1)), ""), "no_new_data", "兩天不一致"), ((_otc_margin(bad=60), ""), (_otc_px(), ""), "failure", "通過"), ((_otc_margin(), ""), (_otc_px(drop=20), ""), "failure", "超過 5%"),
                           ((_otc_margin(units=1), ""), (_otc_px(), ""), "failure", "範圍"), ((None, "HTTP 403"), ([], ""), "failure", "抓取失敗"), ((None, fetch.EMPTY_RESPONSE_MSG), (None, fetch.EMPTY_RESPONSE_MSG), "no_new_data", "抓取失敗"),
                           (([{"Date": "x"}], ""), (_otc_px(), ""), "failure", "欄位與預期不符"), ((_otc_margin(), ""), ([{"Date": "x"}], ""), "failure", "欄位與預期不符")):
        reset()
        with _otc_env(m, p):
            assert REAL["tpex_margin_daily"]() == [], frag
        assert last()["status"] == st and frag in last()["detail"], (frag, last())
    reset()
    with _otc_env((_otc_margin(), ""), (_otc_px(drop=8), "")):                                                   # 4% 查不到收盤價：在容許範圍
        r = REAL["tpex_margin_daily"]()
    assert r and r[0]["n_missing"] == 8 and r[0]["n_stocks"] == 192


def test_tpex_margin_loan_is_validated_and_a_failure_only_drops_the_ratio():
    ok = lambda **k: _otc_env((_otc_margin(), ""), (_otc_px(), ""), **k)
    cases = (("頁面失敗", dict(page=b"", page_status=503), "彙總頁抓取失敗"), ("日期不對", dict(page=_otc_page(roc="115/01/01")), "日期是 115/01/01"), ("張數對不上", dict(page=_otc_page(units_ok=False)), "對不上，不採用"),
             ("融資金額自己對不上", dict(page=_otc_page(ident_ok=False)), "融資金額自己對不上"), ("維持率離譜", dict(page=_otc_page(loan_k=100000)), "超出合理範圍"), ("格式不對", dict(page=b'{"tables": []}'), "格式非預期"), ("不是 JSON", dict(page=b"<html>"), "格式非預期"))
    for name, kw, frag in cases:
        reset()
        with ok(**kw):
            rows = REAL["tpex_margin_daily"]()
        assert len(rows) == 1 and rows[0]["loan"] is None and rows[0]["est_ratio"] is None and rows[0]["units"] == 2000000, (name, rows)
        lg = [x for x in fetch.LOG if x["source"] == "tpex_margin"][-1]; assert lg["status"] == "success" and "沒有融資金額、不算維持率" in lg["detail"] and frag in lg["detail"], (name, lg["detail"])
    reset()
    with ok(page=_otc_page(units=2000001)):                                                                  # 差 1 張（誤差 0.2% 內）：照收
        r = REAL["tpex_margin_daily"]()
    assert r[0]["loan"] == 100000000 * 1000


def test_run_job_daily_persists_tpex_margin_and_isolates_m29():
    reset()
    row = {"as_of": D(1), "units": 2000000, "prev_units": 2020000, "value": 1e11, "n_stocks": 880, "n_missing": 2, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "tpex_margin_daily", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append({n: (r, c) for n, r, c in tables}))]
        for p_ in ps: p_.start()
        try: fetch.run_job("tw_close", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert tables["tpex_margin_daily"] == ([row], "as_of") and st["module:M29"] == "success"
    reset(); tables, st = go(True); assert "module:M29" not in st
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M29"] == "failure" and "module:M07" in st and tables["tpex_margin_daily"][0] == []


def test_tw_breadth_update_splits_by_market_when_the_database_times_out():
    TO = 'HTTP 500: {"code":"57014","message":"canceling statement due to statement timeout"}'
    days = [{"market": m, "as_of": "2026-11-06"} for m in ("TWSE", "TPEX")]
    def env(full, per):
        calls = []
        def req(method, path, body=None, **k):
            if method == "GET": return (days, "") if path.startswith("tw_hist_days") else ([], "")
            calls.append(body)
            return full if "p_market" not in body else per[body["p_market"]]
        return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req)], calls
    reset()
    ps, calls = env((None, TO), {"TWSE": (1, ""), "TPEX": (1, "")})
    assert _with(ps, lambda: fetch.tw_breadth_update()) == 1
    assert [c.get("p_market") for c in calls] == [None, "TWSE", "TPEX"] and all(c["d"] == "2026-11-06" for c in calls), "先一次算，逾時就上市、上櫃各算一次"
    lg = [x for x in fetch.LOG if x["source"] == "tw_breadth"][-1]; assert lg["status"] == "success" and "改成上市、上櫃分開算" in lg["detail"] and "new_tables_v57.sql" in lg["detail"]
    reset()
    ps, calls = env((None, TO), {"TWSE": (1, ""), "TPEX": (None, TO)})
    assert _with(ps, lambda: fetch.tw_breadth_update()) is None
    lg = [x for x in fetch.LOG if x["source"] == "tw_breadth"][-1]; assert lg["status"] == "failure" and "TPEX 也失敗" in lg["detail"] and "new_tables_v57.sql" in lg["detail"], lg["detail"]
    reset()
    ps, calls = env((None, "HTTP 404: function not found"), {})
    assert _with(ps, lambda: fetch.tw_breadth_update()) is None and len(calls) == 1, "不是逾時的錯誤：不分開重試"
    assert "new_tables_breadth.sql" in [x for x in fetch.LOG if x["source"] == "tw_breadth"][-1]["detail"]
    reset()
    ps, calls = env((1, ""), {})
    assert _with(ps, lambda: fetch.tw_breadth_update()) == 1 and len(calls) == 1 and "分開算" not in [x for x in fetch.LOG if x["source"] == "tw_breadth"][-1]["detail"], "一次算成功就不用分開"


def _roc(d): return f"{d.year - 1911}{d.month:02d}{d.day:02d}"


def _watch_env(codes, twse_px=None, twse_pe=None, tpex_px=None, tpex_pe=None, fail=()):
    y = date.today() - timedelta(days=1)
    r = _roc(y)
    data = {"twse_px": twse_px if twse_px is not None else [{"Date": r, "Code": "2330", "Name": "台積電", "ClosingPrice": "1000.00", "MonthlyAveragePrice": "990"}, {"Date": r, "Code": "1101", "Name": "台泥", "ClosingPrice": "30.5", "MonthlyAveragePrice": "30"}],
            "twse_pe": twse_pe if twse_pe is not None else [{"Date": r, "Code": "2330", "Name": "台積電", "PEratio": "25.50", "DividendYield": "1.80", "PBratio": "6.20"}, {"Date": r, "Code": "1101", "Name": "台泥", "PEratio": "", "DividendYield": "3.2", "PBratio": "0.9"}],
            "tpex_px": tpex_px if tpex_px is not None else [{"Date": r, "SecuritiesCompanyCode": "6488", "CompanyName": "環球晶", "Close": "500", "Change": "1"}],
            "tpex_pe": tpex_pe if tpex_pe is not None else [{"Date": _roc(y - timedelta(days=1)), "SecuritiesCompanyCode": "6488", "CompanyName": "環球晶", "PriceEarningRatio": "18.2", "DividendPerShare": "5", "YieldRatio": "1.0", "PriceBookRatio": "3.1"}]}
    def fj(url, **k):
        key = "twse_px" if url == config.TWSE_AVG_URL else "twse_pe" if url.endswith("BWIBBU_ALL") else "tpex_px" if url == config.TPEX_QUOTES_URL else "tpex_pe"
        return (None, "HTTP 503") if key in fail else (data[key], "")
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([{"code": c} for c in codes], "")), mock.patch.object(fetch, "fetch_json", side_effect=fj)], y


def test_watch_valuation_builds_rows_for_both_markets_and_reports_what_it_cannot_find():
    reset()
    ps, y = _watch_env(["2330", "6488", "1101", "9999", "abc", "2330"])
    rows = {r["code"]: r for r in _with(ps, lambda: REAL["watch_valuation"]())}
    assert sorted(rows) == ["1101", "2330", "6488"]
    a = rows["2330"]; assert a["market"] == "TWSE" and a["name"] == "台積電" and a["close"] == 1000.0 and a["pe"] == 25.5 and a["pb"] == 6.2 and a["dividend_yield"] == 1.8 and a["as_of"] == y.isoformat() and a["val_as_of"] == y.isoformat() and "不是預估" in a["source"]
    b = rows["6488"]; assert b["market"] == "TPEX" and b["close"] == 500.0 and b["pe"] == 18.2 and b["as_of"] == y.isoformat() and b["val_as_of"] == (y - timedelta(days=1)).isoformat(), "價格和估值的資料日分開記"
    c = rows["1101"]; assert c["pe"] is None and c["pb"] == 0.9 and c["dividend_yield"] == 3.2, "虧損的公司沒有本益比：留空，不補 0"
    lg = [x for x in fetch.LOG if x["source"] == "watch"][-1]; assert lg["status"] == "success" and "3 / 4 檔有資料" in lg["detail"] and "查不到：9999" in lg["detail"] and "abc" not in lg["detail"], lg["detail"]


def test_watch_valuation_validates_ranges_dates_and_source_failures():
    reset()
    y = date.today() - timedelta(days=1); r = _roc(y); fut = _roc(date.today() + timedelta(days=3))
    ps, _ = _watch_env(["2330", "1101", "2317"], twse_px=[{"Date": r, "Code": "2330", "Name": "A", "ClosingPrice": "0"}, {"Date": fut, "Code": "1101", "Name": "B", "ClosingPrice": "30"}, {"Date": r, "Code": "2317", "Name": "C", "ClosingPrice": "150"}],
                       twse_pe=[{"Date": r, "Code": "2330", "Name": "A", "PEratio": "-5", "DividendYield": "80", "PBratio": "500"}, {"Date": r, "Code": "2317", "Name": "C", "PEratio": "2000", "DividendYield": "-1", "PBratio": "0"}])
    rows = {x["code"]: x for x in _with(ps, lambda: REAL["watch_valuation"]())}
    assert "1101" not in rows and "日期異常略過：1101" in [x for x in fetch.LOG if x["source"] == "watch"][-1]["detail"], "未來的日期不收"
    for code in ("2330", "2317"): assert rows[code]["pe"] is None and rows[code]["pb"] is None and rows[code]["dividend_yield"] is None, code
    assert rows["2330"]["close"] is None and rows["2317"]["close"] == 150.0, "收盤價 0 不收"
    reset()
    ps, _ = _watch_env(["2330", "6488"], fail=("tpex_pe", "twse_pe"))
    rows = {x["code"]: x for x in _with(ps, lambda: REAL["watch_valuation"]())}
    assert rows["2330"]["close"] == 1000.0 and rows["2330"]["pe"] is None and rows["6488"]["pe"] is None, "估值來源失敗：只少那幾欄，收盤價照樣有"
    lg = [x for x in fetch.LOG if x["source"] == "watch"][-1]; assert lg["status"] == "success" and "來源失敗：twse_pe（HTTP 503）、tpex_pe（HTTP 503）" in lg["detail"]
    reset()
    ps, _ = _watch_env(["2330"], fail=("twse_px", "twse_pe", "tpex_px", "tpex_pe")); assert _with(ps, lambda: REAL["watch_valuation"]()) == [] and fetch.LOG[-1]["status"] == "failure"
    reset()
    ps, _ = _watch_env([]); assert _with(ps, lambda: REAL["watch_valuation"]()) == [] and fetch.LOG[-1]["status"] == "no_new_data" and "清單是空的" in fetch.LOG[-1]["detail"]
    reset()
    for e in ("HTTP 404: b'{\"code\":\"PGRST205\",\"message\":\"Could not find the table public.private_watchlist in the schema cache\"}'", "HTTP 404: relation does not exist"):
        reset()
        with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, e)):
            assert REAL["watch_valuation"]() == []
        assert fetch.LOG[-1]["status"] == "no_new_data" and "new_tables_v58.sql" in fetch.LOG[-1]["detail"] and "可以忽略" in fetch.LOG[-1]["detail"], "表不存在：還沒啟用這個功能，不讓工作變紅"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 500: boom")):
        assert REAL["watch_valuation"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "讀取候選股清單失敗" in fetch.LOG[-1]["detail"], "其他錯誤照舊算失敗"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["watch_valuation"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"


def test_finmind_includes_watchlist_codes_and_reuses_the_known_basis_when_the_sample_is_too_small():
    ser = _single_series(1.2)
    per = {"2330": _fm_rows("2330", ser)}
    def req(method, path, *a, **k):
        if path.startswith("private_watchlist"): return [{"code": "2330"}, {"code": "00878"}, {"code": "abc"}], ""
        if path.startswith("tw_fin_q_ext"): return [{"basis": "single"}], ""
        return [], ""
    reset()
    p, calls = _fm_env(per)
    with p, mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["finmind_fin_history"]([])                                                 # 沒有持股，只有候選股
    assert len(calls) == 1 and "data_id=2330" in calls[0], "只查 4 位數的候選股（ETF 和亂打的不查）: %s" % calls
    assert len(rows) == config.FINMIND_QUARTERS and rows[0]["code"] == "2330" and rows[0]["basis"] == "single" and rows[0]["gross_margin"] == 50.0, "單一檔判斷不了單季或累計：沿用資料庫已判斷的口徑"
    reset()
    per2 = {"2330": _fm_rows("2330", _cumulative_of(ser))}
    def req2(method, path, *a, **k):
        return ([{"code": "2330"}], "") if path.startswith("private_watchlist") else ([{"basis": "converted"}], "") if path.startswith("tw_fin_q_ext") else ([], "")
    p, calls = _fm_env(per2)
    with p, mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req2), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["finmind_fin_history"]([])
    assert len(rows) == config.FINMIND_QUARTERS and rows[0]["basis"] == "converted" and rows[0]["gross_margin"] == 50.0, "資料庫記的是累計：照累計換算成單季"
    reset()
    def req3(method, path, *a, **k):
        return ([{"code": "2330"}], "") if path.startswith("private_watchlist") else ([], "")
    p, calls = _fm_env(per)
    with p, mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req3), mock.patch.object(fetch.time, "sleep"):
        assert REAL["finmind_fin_history"]([]) == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "至少 4 個" in fetch.LOG[-1]["detail"], "資料庫也沒有判斷過、樣本又太少：不寫，不猜"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")):
        assert REAL["finmind_fin_history"]([]) == []
    assert fetch.LOG[-1]["status"] == "skipped" and "持股或候選股" in fetch.LOG[-1]["detail"]


def test_run_job_daily_persists_watch_snapshot_and_isolates_m30():
    reset()
    row = {"code": "2330", "name": "台積電", "market": "TWSE", "as_of": D(1), "close": 1000.0, "val_as_of": D(1), "pe": 25.5, "pb": 6.2, "dividend_yield": 1.8, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "watch_valuation", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append({n: (r, c) for n, r, c in tables}))]
        for p_ in ps: p_.start()
        try: fetch.run_job("tw_close", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert tables["watch_snapshot"] == ([row], "code") and st["module:M30"] == "success"
    reset(); tables, st = go(True); assert "module:M30" not in st
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M30"] == "failure" and "module:M07" in st and tables["watch_snapshot"][0] == []


def test_probe_round13_walks_back_to_the_latest_weekday_with_data_and_never_writes():
    def page(rows, summary=None):
        return json.dumps({"date": "x", "tables": [{"title": "上櫃股票融資融券餘額", "fields": ["代號", "資餘額"], "data": rows, "summary": summary or [], "notes": ["註"]}], "stat": "ok"}, ensure_ascii=False).encode()
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url)
        n = len(seen)
        return (200, "application/json", page([]), "") if n < 3 else (200, "application/json", page([["6488", "100"], ["3293", "50"], ["x", "1"]], [["合計", "融資金額(仟元)", "123456"]]), "")
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up, mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round13()
    lg = [x for x in fetch.LOG if x["source"] == "probe:tpex_margin_page"][-1]
    assert lg["status"] == "success" and "3 列" in lg["detail"] and "融資金額(仟元)" in lg["detail"] and "summary" in lg["detail"] and "['6488', '100']" in lg["detail"] and len(seen) == 3, (lg, len(seen))
    assert all("margin_bal_result.php" in u and "d=" in u for u in seen) and len({u.split("d=")[-1] for u in seen}) == 3, "每次換一個日期"
    up.assert_not_called()
    reset(); seen.clear()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", page([]), "")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round13()
    lg = fetch.LOG[-1]; assert lg["status"] == "no_new_data" and "都沒有資料" in lg["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>nope</html>", "")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round13()
    assert fetch.LOG[-1]["status"] == "no_new_data" and "<html>nope" in fetch.LOG[-1]["detail"], "不是 JSON：記下開頭，不報錯"
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round13()
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "探測連不上也不能讓工作變紅"


def _dxy_env(vals, fred=None, status=200):
    today = fetch._now_et().date()
    n = len(vals)
    pts = [((today - timedelta(days=n - i)).isoformat(), v) for i, v in enumerate(vals)]
    def req(method, path, *a, **k):
        return (fred if fred is not None else [], "")
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req),
            mock.patch.object(fetch, "fetch_raw", return_value=(status, "application/json", json.dumps(_yh_json(pts)).encode() if status == 200 else b"", "" if status == 200 else "HTTP")), mock.patch.object(fetch.time, "sleep")], pts


def _walk(n, seed=3):
    import random
    r = random.Random(seed); v, out = 100.0, []
    for _ in range(n):
        v *= 1 + r.uniform(-0.006, 0.006); out.append(round(v, 3))
    return out


def test_yahoo_dxy_validates_and_cross_checks_against_the_fed_broad_index():
    vals = _walk(160)
    ps, pts = _dxy_env(vals)
    corr_fred = [{"as_of": d_, "value": round(v * 1.2 + 3, 3)} for d_, v in pts[-120:]]                                  # 和 DXY 同向的廣義指數
    ps, pts = _dxy_env(vals, corr_fred); reset()
    rows = _with(ps, lambda: REAL["yahoo_dxy"]())
    assert len(rows) == 160 and rows[0]["series_id"] == "DXY" and rows[-1]["value"] == vals[-1] and "非官方" in rows[0]["source"] and "已和聯準會廣義美元指數核對，日變動相關係數 1.00" in rows[0]["source"] and all(r["source"] == rows[0]["source"] for r in rows) and "DXY" in rows[0]["label"] and rows[0]["as_of"] < rows[-1]["as_of"]
    lg = [x for x in fetch.LOG if x["source"] == "yahoo_dxy"][-1]; assert lg["status"] == "success" and "相關係數 1.00" in lg["detail"] and "160 天" in lg["detail"], lg["detail"]
    anti = [{"as_of": d_, "value": round(200 - v, 3)} for d_, v in pts[-120:]]                                              # 反向：核對的結果很弱
    ps, _ = _dxy_env(vals, anti); reset()
    rows = _with(ps, lambda: REAL["yahoo_dxy"]()); lg = [x for x in fetch.LOG if x["source"] == "yahoo_dxy"][-1]
    assert len(rows) == 160 and lg["status"] == "success", "核對結果很弱：不再整批擋下（兩個指數本來就不同），照收"
    assert all("核對的結果很弱" in r["source"] and "無法確認" in r["source"] and "只能當粗略參考" in r["source"] for r in rows) and "都不高，無法確認" in lg["detail"] and "同一天" in lg["detail"] and "晚一個交易日" in lg["detail"] and "早一個交易日" in lg["detail"], "老實寫進每一筆的來源，並列出三個相關係數: " + lg["detail"]
    late = [{"as_of": pts[i][0], "value": round(vals[i - 1] * 1.2 + 3, 3)} for i in range(len(pts) - 120, len(pts))]                       # 廣義指數的變動比 DXY 晚一天出現（你資料庫裡就是這種）
    ps, _ = _dxy_env(vals, late); reset()
    rows = _with(ps, lambda: REAL["yahoo_dxy"]()); lg = [x for x in fetch.LOG if x["source"] == "yahoo_dxy"][-1]
    assert len(rows) == 160 and lg["status"] == "success" and "同一天相關係數只有" in lg["detail"] and "廣義指數的日期比 DXY 晚一個交易日時是 1.00" in lg["detail"] and "哪邊的日期是對的，從資料看不出來" in lg["detail"], "日期差一天、錯開後相關很高：採用，並明寫日期落差: " + lg["detail"]
    assert all("日期差一個交易日，DXY 日期照 Yahoo 原樣" in r["source"] and "已和聯準會廣義美元指數核對" in r["source"] for r in rows) and [r["as_of"] for r in rows] == [p[0] for p in pts], "DXY 自己的日期一個字都不改"
    early = [{"as_of": pts[i][0], "value": round(vals[i + 1] * 1.2 + 3, 3)} for i in range(len(pts) - 121, len(pts) - 1)]                       # 反方向：廣義指數比 DXY 早一天
    ps, _ = _dxy_env(vals, early); reset()
    rows = _with(ps, lambda: REAL["yahoo_dxy"]()); assert len(rows) == 160 and "廣義指數的日期比 DXY 早一個交易日時是 1.00" in [x for x in fetch.LOG if x["source"] == "yahoo_dxy"][-1]["detail"], "另一個方向也認得出來"
    ps, _ = _dxy_env(vals, corr_fred[:5]); reset()                                                                           # 重疊太少：照收，但寫明沒核對
    rows = _with(ps, lambda: REAL["yahoo_dxy"]()); assert len(rows) == 160 and "沒有核對" in [x for x in fetch.LOG if x["source"] == "yahoo_dxy"][-1]["detail"] and "沒有核對：聯準會廣義美元指數的歷史重疊只有 5 天" in rows[0]["source"], "沒核對就明寫在每一筆的來源欄"
    ps, _ = _dxy_env(_walk(160)[:99] if False else _walk(99)); reset()
    assert _with(ps, lambda: REAL["yahoo_dxy"]()) == [] and fetch.LOG[-1]["status"] == "no_new_data" and "至少 100" in fetch.LOG[-1]["detail"]
    ps, _ = _dxy_env([v * 2 for v in vals]); reset()                                                                          # 數值超出 70 到 130
    assert _with(ps, lambda: REAL["yahoo_dxy"]()) == [] and fetch.LOG[-1]["status"] == "failure" and "超出 70 到 130" in fetch.LOG[-1]["detail"]
    ps, _ = _dxy_env(vals, status=429); reset()
    assert _with(ps, lambda: REAL["yahoo_dxy"]()) == [] and fetch.LOG[-1]["status"] == "no_new_data" and "HTTP 429" in fetch.LOG[-1]["detail"], "被擋：只記沒有資料，不讓工作變紅"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["yahoo_dxy"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"
    # 今天的資料（還沒收盤）不收
    today = fetch._now_et().date().isoformat()
    pts2 = [((fetch._now_et().date() - timedelta(days=130 - i)).isoformat(), v) for i, v in enumerate(_walk(130))] + [(today, 101.0)]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")), mock.patch.object(fetch.time, "sleep"), \
         mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", json.dumps(_yh_json(pts2)).encode(), "")):
        rows = REAL["yahoo_dxy"]()
    assert rows and all(r["as_of"] < today for r in rows) and len(rows) == 130


def test_run_job_daily_persists_dxy_and_isolates_m31():
    reset()
    row = {"series_id": "DXY", "label": "x", "as_of": D(1), "value": 99.5, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "yahoo_dxy", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in ps: p_.start()
        try: fetch.run_job("tw_close", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert ("macro_series", [row], "series_id,as_of") in tables and st["module:M31"] == "success"
    reset(); tables, st = go(True); assert "module:M31" not in st
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M31"] == "failure" and "module:M07" in st


def test_probe_round14_logs_each_candidate_source_and_never_writes_or_turns_red():
    pages = {"finra.org": (200, "text/html", b'<html><a href="/sites/default/files/margin-statistics.xlsx">x</a><table><tr><td>Sep-26</td><td>1,234,567</td></tr></table></html>', ""),
             "sent_results": (200, "text/html", b"<html><body><div>Bullish</div><div>38.5%</div></body></html>", ""), "sentiment.xls": (200, "application/vnd.ms-excel", b"\xd0\xcf\x11\xe0rest", ""),
             "data.nasdaq.com": (403, "application/json", b'{"quandl_error":"QECx02"}', "HTTP 403"), "stooq.com": (200, "text/csv", b"Date,Open,High,Low,Close\n2026-11-06,99,100,98,99.5\n", "")}
    def raw(url, timeout=None, headers=None):
        for k, v in pages.items():
            if k in url: return v
        return (None, "", b"", "unexpected")
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up, mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round14()
    by = {x["source"]: x for x in fetch.LOG}
    assert set(by) == {"probe:finra_page", "probe:aaii_results", "probe:aaii_xls", "probe:nasdaq_aaii", "probe:stooq_dxy"}
    assert by["probe:finra_page"]["status"] == "success" and "margin-statistics.xlsx" in by["probe:finra_page"]["detail"] and "Sep-26" in by["probe:finra_page"]["detail"] and "1,234,567" in by["probe:finra_page"]["detail"]
    assert "Bullish 附近" in by["probe:aaii_results"]["detail"] and "38.5%" in by["probe:aaii_results"]["detail"] and "magic d0cf11e0" in by["probe:aaii_xls"]["detail"]
    assert by["probe:nasdaq_aaii"]["status"] == "no_new_data" and "HTTP 403" in by["probe:nasdaq_aaii"]["detail"] and "前 3 行" in by["probe:stooq_dxy"]["detail"]
    up.assert_not_called()
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round14()
    assert not [x for x in fetch.LOG if x["status"] == "failure"] and len(fetch.LOG) == 5, "連不上也不能讓工作變紅"


def _ndx_syms(n=101):
    return [f"S{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(n)]


def _ndx_json(n=101, nested=False):
    rows = [{"symbol": s_, "companyName": "x", "marketCap": "1"} for s_ in _ndx_syms(n)]
    return {"data": {"date": "Nov 6, 2026", "data": {"rows": rows}}, "status": {"rCode": 200}} if not nested else {"payload": [{"meta": 1}, {"deep": {"list": rows}}]}


def _series(kind, last, n=260):
    base = [last - timedelta(days=n - 1 - i) for i in range(n)]
    px = [100 + i * 0.5 for i in range(n)] if kind == "up" else [200 - i * 0.5 for i in range(n)]
    return list(zip(base, px))


def _ndx_env(spec, last=None, list_resp=None):
    """spec: {代號序號: ('up'|'down', 筆數) | None | 'date' | 'split'}"""
    last = last or date.today() - timedelta(days=1)
    syms = _ndx_syms(101)
    def hist(sym, assetclass="etf", days=75):
        if sym == config.SECTOR_BENCH:
            return _series("up", last, 40), ""
        k = spec.get(syms.index(sym))
        if k is None: return None, "連線失敗"
        if k == "date": return _series("up", last - timedelta(days=3)), ""
        if k == "split":
            pts = _series("up", last); pts[-30] = (pts[-30][0], pts[-30][1] * 3); return pts, ""
        return _series(k[0], last, k[1]), ""
    resp = list_resp if list_resp is not None else (200, "application/json", json.dumps(_ndx_json()).encode(), "")
    return [mock.patch.object(fetch, "fetch_raw", return_value=resp), mock.patch.object(fetch, "_nasdaq_history", side_effect=hist), mock.patch.object(fetch.time, "sleep")], last


def test_parse_ndx_constituents_finds_the_list_and_validates_symbols_and_count():
    for nested in (False, True):
        syms, why = fetch.parse_ndx_constituents(_ndx_json(nested=nested)); assert why == "" and len(syms) == 101 and syms[0] == "SAA", nested
    rows = _ndx_json()["data"]["data"]["rows"]
    messy = {"data": {"data": {"rows": rows + [{"symbol": "sab"}, {"symbol": "BRK/B"}, {"symbol": "TOOLONGX"}, {"symbol": ""}, {"symbol": "AAPL"}]}}}
    s2, w2 = fetch.parse_ndx_constituents(messy); assert w2 == "" and len(s2) == 102 and "AAPL" in s2 and s2.count("SAB") == 1 and "BRK/B" not in s2 and "TOOLONGX" not in s2, "小寫轉大寫、重複只算一次、格式不對的丟掉"
    for n, frag in ((94, "94"), (111, "111"), (0, "0")):
        s3, w3 = fetch.parse_ndx_constituents(_ndx_json(n) if n else {"data": {}}); assert s3 == [] and "預期 95 到 110" in w3 and frag in w3, n
    assert fetch.parse_ndx_constituents(None)[0] == [] and fetch.parse_ndx_constituents("x")[0] == [] and fetch.parse_ndx_constituents({"rows": [{"symbol": "A"}]})[0] == []


def test_us_breadth_ndx_counts_above_50_and_200_day_averages_and_skips_doubtful_stocks():
    spec = {}
    for i in range(0, 50): spec[i] = ("up", 260)
    for i in range(50, 80): spec[i] = ("down", 260)
    for i in range(80, 85): spec[i] = ("up", 80)
    for i in range(85, 90): spec[i] = ("down", 80)
    for i in range(90, 93): spec[i] = None
    for i in range(93, 95): spec[i] = "date"
    spec[95] = "split"
    for i in range(96, 101): spec[i] = ("up", 260)
    ps, last = _ndx_env(spec); reset()
    rows = _with(ps, lambda: REAL["us_breadth_ndx"]())
    assert len(rows) == 1; r = rows[0]
    assert r["as_of"] == last.isoformat() and r["n_total"] == 101 and r["n_ok50"] == 95 and r["above50"] == 60 and r["n_ok200"] == 85 and r["above200"] == 55, r
    assert r["pct50"] == round(60 / 95 * 100, 2) == 63.16 and r["pct200"] == round(55 / 85 * 100, 2) == 64.71 and "不是 S&P 500" in r["source"]
    lg = [x for x in fetch.LOG if x["source"] == "us_breadth_ndx"][-1]; assert lg["status"] == "success" and "站上 50 日線 60 / 95 檔（63.16%）" in lg["detail"] and "抓不到 3 檔（SDM、SDN、SDO）、日期不同 2 檔（SDP、SDQ）、疑似分割 1 檔（SDR）" in lg["detail"], lg["detail"]
    # 200 日線有效檔數不到 80：只給 50 日的比例，200 日留空
    spec2 = {i: ("up", 260) if i < 30 else ("down", 80) for i in range(101)}
    ps, _ = _ndx_env(spec2); reset(); r2 = _with(ps, lambda: REAL["us_breadth_ndx"]())[0]
    assert r2["n_ok50"] == 101 and r2["n_ok200"] == 30 and r2["pct200"] is None and r2["pct50"] == round(30 / 101 * 100, 2), r2
    assert "有效檔數不到 80，不算比例" in [x for x in fetch.LOG if x["source"] == "us_breadth_ndx"][-1]["detail"]


def test_us_breadth_ndx_failure_paths_never_turn_the_job_red():
    ok_spec = {i: ("up", 260) for i in range(101)}
    def go(spec, **kw):
        ps, _ = _ndx_env(spec, **kw); reset(); return _with(ps, lambda: REAL["us_breadth_ndx"]()), [x for x in fetch.LOG if x["source"] == "us_breadth_ndx"][-1]
    rows, lg = go({i: ("up", 260) if i < 70 else None for i in range(101)}); assert rows == [] and lg["status"] == "no_new_data" and "只有 70 檔有效" in lg["detail"] and "抓不到 31 檔（" in lg["detail"] and "…）" in lg["detail"], lg["detail"]
    rows, lg = go(ok_spec, list_resp=(403, "text/html", b"denied", "HTTP 403")); assert rows == [] and lg["status"] == "no_new_data" and "HTTP 403" in lg["detail"]
    rows, lg = go(ok_spec, list_resp=(200, "text/html", b"<html>nope</html>", "")); assert rows == [] and lg["status"] == "no_new_data" and "不是 JSON" in lg["detail"]
    rows, lg = go(ok_spec, list_resp=(200, "application/json", json.dumps(_ndx_json(40)).encode(), "")); assert rows == [] and lg["status"] == "no_new_data" and "預期 95 到 110" in lg["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", json.dumps(_ndx_json()).encode(), "")), mock.patch.object(fetch, "_nasdaq_history", return_value=(None, "連線失敗")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_breadth_ndx"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "基準 SPY" in fetch.LOG[-1]["detail"]
    assert not [x for x in fetch.LOG if x["status"] == "failure"]


def test_run_job_us_breadth_persists_ndx_breadth_and_isolates_m32():
    reset()
    row = {"as_of": D(1), "n_total": 101, "n_ok50": 95, "above50": 60, "n_ok200": 85, "above200": 55, "pct50": 63.16, "pct200": 64.71, "source": "x"}
    def go(job, dry, **kw):
        got = []
        base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "us_breadth_ndx", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in ps: p_.start()
        try: fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go("us_breadth", False); assert ("us_breadth_ndx", [row], "as_of") in tables and st["module:M32"] == "success"
    reset(); tables, st = go("us_breadth", True); assert not [x for x in tables if x[0] == "us_breadth_ndx"] and st["us_breadth"] == "skipped" and "module:M32" not in st, "試跑不抓 100 多個請求"
    reset(); tables, st = go("us_breadth", False, side_effect=RuntimeError("boom")); assert st["module:M32"] == "failure" and [x for x in tables if x[0] == "us_breadth_ndx"][0][1] == []
    reset(); tables, st = go("tw_close", False); assert "module:M32" not in st, "只有 us_breadth 這個工作會跑 M32，不拖慢 tw_close"


def test_probe_round15_reports_the_constituents_endpoint_without_writing():
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", json.dumps(_ndx_json()).encode(), "")), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round15()
    lg = [x for x in fetch.LOG if x["source"] == "probe:ndx_list"][-1]; assert lg["status"] == "success" and "解析：101 檔，前 5 檔 ['SAA', 'SAB'" in lg["detail"]; up.assert_not_called()
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"<html>denied</html>", "HTTP 403")):
        fetch.probe_round15()
    assert fetch.LOG[-1]["status"] == "no_new_data" and "不是 JSON" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")):
        fetch.probe_round15()
    assert fetch.LOG[-1]["status"] == "no_new_data" and not [x for x in fetch.LOG if x["status"] == "failure"]


NDX_C = "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE"
NDX_E = "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE"


def test_cftc_cot_ndx_prefers_consolidated_falls_back_to_emini_and_lists_names_when_none():
    def run(by_name, names=None):
        urls = []
        def fj(url, **k):
            urls.append(url)
            if "%24select" in url: return (names if names is not None else []), ""
            for nm, v in by_name.items():
                if urllib_unquote(url).find("market_and_exchange_names=" + nm) >= 0: return v
            return [], ""
        reset()
        with mock.patch.object(fetch, "fetch_json", side_effect=fj):
            return REAL["cftc_cot_ndx"](), urls, [x for x in fetch.LOG if x["source"] == "cftc:cot_ndx"][-1]
    out, urls, lg = run({NDX_C: ([cot_row(D(10), market=NDX_C), cot_row(D(17), market=NDX_C, ll=380000, ls=650000)], "")})
    assert [r["report_date"] for r in out] == [D(10), D(17)] and out[0]["market"] == "Nasdaq-100 Consolidated" and out[0]["lev_long"] == 400000 and "gpe5-46if" in urls[0] and "NASDAQ-100" in urllib_unquote(urls[0]) and len(urls) == 1, "找到合併合約就不用再找 E-mini"
    assert lg["status"] == "success" and "Nasdaq-100 Consolidated：報告日" in lg["detail"] and "槓桿基金淨 -300,000" in lg["detail"]
    out, urls, lg = run({NDX_E: ([cot_row(D(10), market=NDX_E)], "")})
    assert out and out[0]["market"] == "Nasdaq-100 E-mini" and len(urls) == 2, "合併合約沒資料：改找 E-mini"
    bad = cot_row(D(10), market=NDX_C, ll=9999999)                                                                     # 槓桿基金多單比未平倉量還大：不合理
    out, urls, lg = run({NDX_C: ([bad], ""), NDX_E: ([cot_row(D(10), market=NDX_E)], "")}); assert out and out[0]["market"] == "Nasdaq-100 E-mini", "合併合約的數字不合理：不採用，改找 E-mini"
    out, urls, lg = run({}, names=[{"market_and_exchange_names": "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE"}, {"market_and_exchange_names": "NASDAQ MICRO"}])
    assert out == [] and lg["status"] == "no_new_data" and "CFTC 沒有這個市場名稱的資料" in lg["detail"] and "NASDAQ-100 STOCK INDEX (MINI)" in lg["detail"] and "NASDAQ MICRO" in lg["detail"], "找不到：列出 CFTC 裡實際含 NASDAQ 的名稱，讓你對照: " + lg["detail"]
    out, urls, lg = run({NDX_C: (None, "HTTP 503"), NDX_E: (None, "HTTP 503")}); assert out == [] and lg["status"] == "no_new_data" and "抓取失敗 HTTP 503" in lg["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "timeout")):
        assert REAL["cftc_cot_ndx"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "查不到" in fetch.LOG[-1]["detail"] and not [x for x in fetch.LOG if x["status"] == "failure"], "連不上：只記沒有資料，不讓工作變紅"


def test_run_job_daily_persists_ndx_cot_and_isolates_m33():
    reset()
    row = {"report_date": D(10), "market": "Nasdaq-100 Consolidated", "open_interest": 2000000, "dealer_long": 1, "dealer_short": 2, "asset_mgr_long": 3, "asset_mgr_short": 4, "lev_long": 5, "lev_short": 6, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "cftc_cot_ndx", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in ps: p_.start()
        try: fetch.run_job("tw_close", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert ("us_cot", [row], "report_date,market") in tables and st["module:M33"] == "success"
    reset(); tables, st = go(True); assert "module:M33" not in st
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M33"] == "failure" and "module:M07" in st


def test_probe_round16_reports_whether_yahoo_futures_quotes_carry_open_interest():
    def raw(oi):
        def f(url, timeout=None, headers=None):
            if "v7/finance/quote" in url:
                res = [{"symbol": "NQ=F", "regularMarketPrice": 21000, **({"openInterest": 250000} if oi else {})}, {"symbol": "ES=F", "regularMarketPrice": 5800, **({"openInterest": 2100000} if oi else {})}]
                return 200, "application/json", json.dumps({"quoteResponse": {"result": res}}).encode(), ""
            return 200, "application/json", json.dumps({"chart": {"result": [{"meta": {"symbol": "NQ=F", "regularMarketPrice": 21000, "currency": "USD"}}]}}).encode(), ""
        return f
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw(True)), mock.patch.object(fetch, "sb_upsert") as up, mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round16()
    by = {x["source"]: x for x in fetch.LOG}
    assert "NQ=F：openInterest=250000" in by["probe:yahoo_quote_oi"]["detail"] and "ES=F：openInterest=2100000" in by["probe:yahoo_quote_oi"]["detail"] and "openInterest 在 meta 裡：False" in by["probe:yahoo_chart_meta"]["detail"]; up.assert_not_called()
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw(False)), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round16()
    assert "openInterest=沒有這個欄位" in [x for x in fetch.LOG if x["source"] == "probe:yahoo_quote_oi"][0]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(401, "application/json", b'{"finance":{"error":{"code":"Unauthorized"}}}', "HTTP 401")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round16()
    assert all(x["status"] == "no_new_data" for x in fetch.LOG if x["source"].startswith("probe:")) and "result 是空的" in [x for x in fetch.LOG if x["source"] == "probe:yahoo_quote_oi"][0]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch.time, "sleep"):
        fetch.probe_round16()
    assert not [x for x in fetch.LOG if x["status"] == "failure"] and len(fetch.LOG) == 2


def _bars_body(end, n=70, vol=60_000_000, extra=()):
    """由 end 往前數 n 個平日（含 end 若是平日）的 Nasdaq 日線 JSON；extra：額外的列"""
    rows, d_ = [], end
    while len(rows) < n:
        if d_.weekday() < 5:
            rows.append({"date": d_.strftime("%m/%d/%Y"), "close": f"${700 + len(rows) * 0.1:.2f}", "volume": f"{vol + len(rows) * 1000:,}", "open": "$1", "high": "$1", "low": "$1"})
        d_ -= timedelta(days=1)
    return json.dumps({"data": {"tradesTable": {"rows": rows + list(extra)}}}).encode()


def test_us_daily_bars_keeps_only_closed_days_with_plausible_volume():
    now = datetime(2026, 11, 9, 10, 0)                                                    # 週一美東 10:00：今天的那一筆還在盤中
    fri = date(2026, 11, 6)
    def run(body, status=200, when=now):
        reset()
        with mock.patch.object(fetch, "fetch_raw", return_value=(status, "application/json", body, "" if status == 200 else "HTTP")), mock.patch.object(fetch, "_now_et", return_value=when):
            return REAL["us_daily_bars"](), [x for x in fetch.LOG if x["source"] == "us_daily_bars"][-1]
    partial = {"date": "11/09/2026", "close": "$715.00", "volume": "3,000,000", "open": "$1", "high": "$1", "low": "$1"}         # 今天盤中：成交量只有一小部分
    out, lg = run(_bars_body(fri, 70, extra=[partial]))
    assert len(out) == 70 and max(r["as_of"] for r in out) == "2026-11-06" and all(r["symbol"] == "SPY" and r["volume"] > 1e6 and r["close"] > 0 for r in out), "盤中那一筆不收"
    assert "2026-11-09" not in {r["as_of"] for r in out} and "Nasdaq" in out[0]["source"] and lg["status"] == "success" and "70 筆（SPY）" in lg["detail"], lg["detail"]
    out2, _ = run(_bars_body(date(2026, 11, 9), 70), when=datetime(2026, 11, 9, 17, 0))                                            # 收盤後（美東 17:00）：今天那一筆可以收
    assert max(r["as_of"] for r in out2) == "2026-11-09", "收盤後今天的那一筆才收"
    bad_vol = [{"date": "11/05/2026", "close": "$701.00", "volume": "12", "open": "$1", "high": "$1", "low": "$1"}, {"date": "11/04/2026", "close": "$701.00", "volume": "", "open": "$1", "high": "$1", "low": "$1"}, {"date": "11/03/2026", "close": "$701.00", "volume": "N/A", "open": "$1", "high": "$1", "low": "$1"}]
    out3, lg3 = run(_bars_body(fri, 70, extra=bad_vol)); n_before = len(out); assert len(out3) == n_before and all(r["volume"] >= 1e6 for r in out3), "成交量不合理或讀不出來的列丟掉，不編"
    out4, lg4 = run(_bars_body(fri, 30)); assert out4 == [] and lg4["status"] == "no_new_data" and "只有 30 天有可用的成交量（至少 60 天）" in lg4["detail"]
    out5, lg5 = run(_bars_body(date(2026, 10, 20), 70)); assert out5 == [] and lg5["status"] == "no_new_data" and "太舊" in lg5["detail"], "最新一天超過 7 天前：不用"
    out6, lg6 = run(b"", status=403); assert out6 == [] and lg6["status"] == "no_new_data"
    out7, lg7 = run(b"<html>nope</html>"); assert out7 == [] and lg7["status"] == "no_new_data" and "不是 JSON" in lg7["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")), mock.patch.object(fetch, "_now_et", return_value=now):
        assert REAL["us_daily_bars"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "連線失敗" in fetch.LOG[-1]["detail"] and not [x for x in fetch.LOG if x["status"] == "failure"], "連不上：只記沒有資料，不讓工作變紅"


def test_run_job_us_breadth_also_persists_spy_bars_and_isolates_m34():
    reset()
    bar = {"symbol": "SPY", "as_of": D(1), "close": 712.5, "volume": 61234500, "source": "x"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(dry, **kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "us_breadth_ndx", return_value=[{"as_of": D(1)}]),
              mock.patch.object(fetch, "us_daily_bars", **(kw or {"return_value": [bar]})), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in ps: p_.start()
        try: fetch.run_job("us_breadth", dry_run=dry)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(False); assert ("us_daily_bars", [bar], "symbol,as_of") in tables and st["module:M34"] == "success" and st["module:M32"] == "success"
    reset(); tables, st = go(True); assert not [x for x in tables if x[0] == "us_daily_bars"] and "module:M34" not in st, "試跑不抓"
    reset(); tables, st = go(False, side_effect=RuntimeError("boom")); assert st["module:M34"] == "failure" and st["module:M32"] == "success" and [x for x in tables if x[0] == "us_daily_bars"][0][1] == [], "M34 壞了不影響 M32"


def _dash_env(swagger=None, pages=None):
    """探測用的假環境：swagger（網址→dict 或 None）與網頁（網址→(狀態, 內容)，沒列到的網址回 404）。回傳 (收到的請求網址清單, 修補清單)"""
    asked = []
    swagger = swagger or {}
    pages = pages or {}
    def fake_raw(url, timeout=None, headers=None):
        asked.append(url)
        st, body = pages.get(url, (404, ""))
        return st, "text/html", body.encode("utf-8"), "" if st == 200 else f"HTTP {st}"
    def fake_swagger(url):
        d = swagger.get(url)
        return (d, "") if d is not None else (None, "連線失敗")
    return asked, [mock.patch.object(fetch, "fetch_raw", side_effect=fake_raw), mock.patch.object(fetch, "fetch_swagger", side_effect=fake_swagger), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "persist_extra")]


def _run_dash(env):
    asked, patches = env
    reset()
    for p in patches: p.start()
    try:
        REAL["probe_dashboard"]()
    finally:
        for p in patches: p.stop()
    return asked, {x["source"] + "#" + str(i): x for i, x in enumerate(fetch.LOG) if x["source"].startswith("probe:dashboard")}, patches


def test_probe_dashboard_finds_keyword_endpoints_links_and_data_urls():
    home = "https://www.twse.com.tw/zh/"
    sw_twse = {"paths": {"/exchangeReport/MI_MARGN": {"get": {"summary": "融資融券彙總"}}, "/credit/maintenance": {"get": {"summary": "全市場擔保維持率", "description": "每日"}}, "/x/y": {"get": {"summary": "無關的東西"}}}}
    sw_tpex = {"paths": {"/openapi/v1/credit": {"get": {"summary": "信用交易統計"}}}}
    html_home = '<html><a href="/zh/about.html">關於我們</a> <a href="/zh/dashboard/index.html"><span>臺股</span>儀表板</a> <a href="javascript:void(0)">儀表板</a> <a href="#top">儀表板</a> <a href="/zh/news">新聞</a></html>'
    html_dash = '<html><title> 臺股儀表板 </title><script src="/assets/app.js"></script><script src="https://cdn.example.com/lib.js"></script><script>var u = "/rwd/zh/dashboard/credit?response=json"; var v = "/zh/logo.png";</script></html>'
    js = 'fetch("/api/v1/margin/maintenance?d=1"); const t = "全市場擔保維持率"; const o = "https://www.twse.com.tw/rwd/zh/credit/MI_CREDIT.json"; var x = "/css/main.css";'
    pages = {home: (200, html_home), "https://www.twse.com.tw/zh/dashboard/index.html": (200, html_dash), "https://www.twse.com.tw/assets/app.js": (200, js)}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: sw_twse, config.TPEX_SWAGGER: sw_tpex}, pages))
    by = {k.split("#")[0]: v for k, v in logs.items()}
    assert "共 3 個端點" in by["probe:dashboard_swagger_twse"]["detail"] and "/credit/maintenance | 全市場擔保維持率" in by["probe:dashboard_swagger_twse"]["detail"] and "/x/y" not in by["probe:dashboard_swagger_twse"]["detail"] and "MI_MARGN" not in by["probe:dashboard_swagger_twse"]["detail"], "只列含關鍵字的端點: " + by["probe:dashboard_swagger_twse"]["detail"]
    assert "/openapi/v1/credit | 信用交易統計" in by["probe:dashboard_swagger_tpex"]["detail"]
    d = by["probe:dashboard_home"]["detail"]; assert by["probe:dashboard_home"]["status"] == "success" and "找到 1 個疑似儀表板連結" in d and "臺股 儀表板 → https://www.twse.com.tw/zh/dashboard/index.html" in d and "javascript" not in d and "about" not in d, "首頁：只收真的儀表板連結（排除 javascript、#、無關連結）: " + d
    d = by["probe:dashboard_page"]["detail"]; assert "標題 臺股儀表板" in d and "程式檔 2 個" in d and "/rwd/zh/dashboard/credit?response=json" in d and "logo.png" not in d.split("頁面原文")[0] and "頁面原文：" in d, "頁面：標題、程式檔、像資料網址的字串（很短的頁面連原文一起記）: " + d
    d = by["probe:dashboard_script"]["detail"]; assert "含關鍵字 維持率" in d and "/api/v1/margin/maintenance?d=1" in d and "MI_CREDIT.json" in d and "main.css" not in d.split("讀資料的呼叫附近")[0] and "讀資料的呼叫附近：fetch(" in d, "程式檔: " + d
    assert "https://cdn.example.com/lib.js" not in asked, "外部網域的程式檔不抓"; assert asked.count("https://www.twse.com.tw/assets/app.js") == 1
    assert all(x["status"] in ("success",) for k, x in by.items() if k != "probe:dashboard_api") and not [x for x in fetch.LOG if x["status"] == "failure"], "全部成功、沒有失敗（找得到端點但沒有基底網址的那幾筆是「沒抓」，不算失敗）"


def test_probe_dashboard_follows_redirect_pages_to_subpages_one_level_only():
    home = "https://www.twse.com.tw/zh/"
    stub = '<html><head><title>臺股儀表板</title></head><body><a href="/dashboard/zh/credit/margin.html">信用交易</a><a href="/dashboard/zh/default/other.html">其他</a><a href="/elsewhere/x.html">別的</a><a href="https://evil.example.com/dashboard/zh/y.html">外站</a></body></html>'
    pages = {home: (200, '<a href="/dashboard/zh/index.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/index.html": (200, stub),
             "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, '<html><title>融資</title><script src="/dashboard/js/margin.js"></script><iframe src="/dashboard/zh/credit/frame.html"></iframe><meta http-equiv="refresh" content="0; url=/dashboard/zh/credit/next.html"><script>location.href = "/dashboard/zh/loop/index.html";</script>' + "x" * 1300 + "</html>"),
             "https://www.twse.com.tw/dashboard/zh/default/other.html": (200, "<title>other</title>"),
             "https://www.twse.com.tw/dashboard/js/margin.js": (200, 'fetch("/dashboard/api/credit/margin.json"); var m = "維持率";')}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    page = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_page"]; subs = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_subpage"]; scripts = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_script"]
    assert len(page) == 1 and "頁面原文：" in page[0] and "信用交易" in page[0], "短頁面（轉址頁）：連原文一起記: " + page[0]
    assert len(subs) == 2 and "融資" in subs[0] and "標題 other" in subs[1], "往下一層：只追同站 /dashboard 的子頁（排除別的路徑、外站）: " + str(subs)
    assert "框架 https://www.twse.com.tw/dashboard/zh/credit/frame.html" in subs[0] and "轉址 /dashboard/zh/credit/next.html" in subs[0] and "/dashboard/zh/loop/index.html" in subs[0] and "頁面原文" not in subs[0], "框架、meta 轉址、location 轉址都記下來；長頁面不記原文: " + subs[0]
    assert "https://www.twse.com.tw/dashboard/js/margin.js" in asked and "/dashboard/api/credit/margin.json" in scripts[0] and "含關鍵字 維持率" in scripts[0], "子頁的程式檔也看: " + scripts[0]
    asked_set = [u for u in asked if "/dashboard/zh/" in u]
    assert "https://www.twse.com.tw/dashboard/zh/credit/frame.html" not in asked and "https://www.twse.com.tw/dashboard/zh/loop/index.html" not in asked and "https://www.twse.com.tw/elsewhere/x.html" not in asked and not [u for u in asked if "evil.example.com" in u], "只再往下一層，不追子頁的子頁；不碰外站: " + str(asked_set)
    assert asked.count("https://www.twse.com.tw/dashboard/zh/index.html") == 1, "同一頁不重複抓"


def test_probe_dashboard_reads_page_specific_script_first_skips_libraries_and_shows_data_calls():
    home = "https://www.twse.com.tw/zh/"
    page = '<title>信用交易</title><script src="https://www.googletagmanager.com/gtag/js?id=G-1"></script><script src="/dashboard/js/main.js"></script><script src="/dashboard/lib/hc/highcharts.js"></script><script src="/dashboard/lib/hc/accessibility.js"></script><script src="/dashboard/js/credit-margin.js"></script>' + "x" * 1300
    pages = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, page),
             "https://www.twse.com.tw/dashboard/js/main.js": (200, "var a = 1;"), "https://www.twse.com.tw/dashboard/lib/hc/highcharts.js": (200, "x"), "https://www.twse.com.tw/dashboard/lib/hc/accessibility.js": (200, "x"),
             "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, "$.getJSON('/dashboard/api/credit/margin?type=day', function (d) { render(d); }); // 維持率")}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    js = [u for u in asked if u.endswith(".js")]
    assert js == ["https://www.twse.com.tw/dashboard/js/credit-margin.js", "https://www.twse.com.tw/dashboard/js/main.js"], "頁面專屬的程式檔排最前面；函式庫（lib）和外站的不看: " + str(js)
    d = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_script"][0]
    assert "credit-margin.js" in d and "含關鍵字 維持率" in d and "/dashboard/api/credit/margin?type=day" in d and "讀資料的呼叫附近：$.getJSON('/dashboard/api/credit/margin?type=day'" in d, "第一支就是頁面專屬的，並記下讀資料呼叫附近的字: " + d


def test_probe_dashboard_collects_api_constants_and_fetches_endpoints_logging_response_shape():
    home = "https://www.twse.com.tw/zh/"
    page = '<title>信用交易</title><script src="/dashboard/js/main.js"></script><script src="/dashboard/js/credit-margin.js"></script><script>window.cfg = {TWSE_API: "https://www.twse.com.tw/api"}; var x = 1;</script>' + "x" * 1300
    main = 'const TPEX_API = "https://www.tpex.org.tw/otc"; var APP_NAME = "x";'
    cm = ('$.get(TWSE_API + "/marginTrading/MI_MARGN_TREND"); $.get("/marginTrading/BFIJ3U", ok); $.get(TPEX_API + "/dashboardOtc/marginTrend?lang=zh-tw&days=5"); var css = "/dashboard/css/page.css"; var img = "/dashboard/img/a.png"; var h = "/dashboard/zh/credit/lending.html";')
    pages = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, page), "https://www.twse.com.tw/dashboard/js/main.js": (200, main), "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, cm),
             "https://www.twse.com.tw/api/marginTrading/MI_MARGN_TREND": (200, '{"stat": "OK", "data": [{"date": "20261110", "maintRatio": 193.9, "balance": 123456}, {"date": "20261109", "maintRatio": 194.2, "balance": 123000}]}'),
             "https://www.twse.com.tw/api/marginTrading/BFIJ3U": (500, ""),
             "https://www.tpex.org.tw/otc/dashboardOtc/marginTrend?lang=zh-tw&days=5": (200, "<html>not json</html>")}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    by = {}
    for v in logs.values(): by.setdefault(v["source"], []).append(v["detail"])
    c = by["probe:dashboard_consts"][0]
    assert "TWSE_API = https://www.twse.com.tw/api" in c and "TPEX_API = https://www.tpex.org.tw/otc" in c and "APP_NAME" not in c and "/marginTrading/MI_MARGN_TREND" in c and "/dashboardOtc/marginTrend?lang=zh-tw&days=5" in c and "page.css" not in c and "a.png" not in c and "lending.html" not in c, "基底網址常數與端點（排除靜態檔）: " + c
    api = by["probe:dashboard_api"]
    ok = [d for d in api if "結構" in d]; assert len(ok) == 1 and "https://www.twse.com.tw/api/marginTrading/MI_MARGN_TREND｜" in ok[0] and "stat: 'OK'" in ok[0] and "[2 筆；第一筆 {date: '20261110', maintRatio: 193.9, balance: 123456}]" in ok[0], "記下回傳結構與樣本值: " + str(api)
    assert any("BFIJ3U：HTTP 500" in d or "BFIJ3U" in d and "HTTP 500" in d for d in api) and any("dashboardOtc/marginTrend" in d and "不是 JSON" in d for d in api), "非 200、不是 JSON：各記一筆: " + str(api)
    assert "https://www.tpex.org.tw/otc/dashboardOtc/marginTrend?lang=zh-tw&days=5" in asked and "https://www.twse.com.tw/api/marginTrading/MI_MARGN_TREND" in asked and not [u for u in asked if u.startswith("https://www.twse.com.tw/api/dashboardOtc")], "上櫃端點用 TPEX 的基底、上市端點用 TWSE 的基底"
    # 沒有基底網址：不猜，記「沒抓」
    pages2 = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, '<title>x</title><script src="/dashboard/js/credit-margin.js"></script>' + "x" * 1300), "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, '$.get(API_ROOT + "/marginTrading/X1");')}
    asked2, logs2, _p = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages2))
    d2 = [v["detail"] for v in logs2.values() if v["source"] == "probe:dashboard_api"]; assert [d.split("：")[0] for d in d2] == ["https://www.twse.com.tw/rwd/%smarginTrading/X1?response=json" % s for s in ("zh/", "any/", "en/", "")] and all("HTTP 404" in d for d in d2) and not [u for u in asked2 if "tpex" in u.lower() and "dashboardOtc" in u], "沒有基底網址：證交所的端點依序試 zh、any、en、沒有語言段（有依據），櫃買的不亂猜: " + str(d2)
    # 端點數量上限
    eps = " ".join(f'$.get("/marginTrading/E{i}");' for i in range(25))
    pages3 = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, '<title>x</title><script src="/dashboard/js/credit-margin.js"></script>' + "x" * 1300), "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, 'var TWSE_API = "https://www.twse.com.tw/api"; ' + eps)}
    asked3, logs3, _p = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages3))
    assert len([u for u in asked3 if "/marginTrading/E" in u]) == config.DASHBOARD_MAX_ENDPOINTS, "端點最多抓 DASHBOARD_MAX_ENDPOINTS 個: " + str(len([u for u in asked3 if '/marginTrading/E' in u]))


def _fin_env(swagger, datasets, finmind=None, tables=None, sb=True):
    """三率覆蓋探測用的假環境：資料庫表（路徑開頭→列）、swagger、開放資料資料集（網址尾→列）、FinMind（代號→回傳）。"""
    tables = tables or {}
    finmind = finmind or {}
    asked = []
    def fake_sb(method, path, *a, **k):
        asked.append(path)
        for pre, rows in tables.items():
            if path.startswith(pre):
                return (rows, None) if rows is not None else (None, "讀取失敗")
        return [], None
    def fake_json(url, *a, **k):
        asked.append(url)
        if "finmindtrade" in url:
            code = url.split("data_id=")[1].split("&")[0]
            return finmind.get(code, (None, "HTTP 402"))
        for tail, rows in datasets.items():
            if url.endswith(tail):
                return rows, None
        return None, "HTTP 404"
    def fake_swagger(url):
        d = swagger.get(url)
        return (d, "") if d is not None else (None, "連線失敗")
    return asked, [mock.patch.object(fetch, "sb_ready", return_value=sb), mock.patch.object(fetch, "sb_request", side_effect=fake_sb), mock.patch.object(fetch, "fetch_json", side_effect=fake_json), mock.patch.object(fetch, "fetch_swagger", side_effect=fake_swagger), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "persist_extra")]


def _run_fin(env):
    asked, patches = env
    reset()
    for p in patches: p.start()
    try:
        REAL["probe_fin_coverage"]()
    finally:
        for p in patches: p.stop()
    return asked, [x for x in fetch.LOG if x["source"].startswith("probe:fin")]


def test_probe_fin_coverage_finds_missing_codes_and_checks_every_statement_dataset_and_finmind():
    swagger = {config.TWSE_OPENAPI_SWAGGER: {"paths": {"/opendata/t187ap06_L_ci": {"get": {"summary": "上市公司綜合損益表-一般業"}}, "/opendata/t187ap06_L_basi": {"get": {"summary": "上市公司綜合損益表-銀行業"}}, "/opendata/other": {"get": {"summary": "無關的東西"}}}},
               config.TPEX_SWAGGER: {"paths": {"/mopsfin_t187ap06_O_ciA": {"get": {"summary": "上櫃公司財報資訊（一般業）"}}}}}
    datasets = {"/opendata/t187ap06_L_ci": [{"公司代號": "2330", "營業收入": "1"}, {"公司代號": "1597", "營業收入": "1"}], "/opendata/t187ap06_L_basi": [{"公司代號": "2882", "淨收益": "9"}, {"公司代號": "2891", "淨收益": "9"}], "/mopsfin_t187ap06_O_ciA": [{"SecuritiesCompanyCode": "6862", "營業收入": "1"}]}
    tables = {"stock_prices": [{"code": "2330", "name": "台積電"}, {"code": "2882", "name": "國泰金"}, {"code": "1597", "name": "直得"}, {"code": "6862", "name": "新股"}], "private_watchlist": [{"code": "2317"}, {"code": "00878"}],
              "tw_fin_q?": [{"code": "2330"}], "tw_fin_q_ext": [{"code": "1597"}, {"code": "2317"}], "tw_industry": [{"code": "2882", "industry": "金融保險業"}, {"code": "6862", "industry": "電子零組件業"}]}
    finmind = {"2882": ({"status": 200, "data": [{"type": "Revenue"}, {"type": "NetIncome"}, {"type": "NetIncome"}, {"type": "EPS"}]}, None), "6862": ({"status": 200, "data": []}, None)}
    asked, logs = _run_fin(_fin_env(swagger, datasets, finmind, tables))
    by = {}
    for x in logs: by.setdefault(x["source"], []).append(x["detail"])
    m = by["probe:fin_missing"][0]
    assert "共 6 檔，已有三率 3 檔，沒有 3 檔" in m and "00878 （候選股）（產業查不到）" in m and "2882 國泰金（金融保險業）" in m and "6862 新股（電子零組件業）" in m and "2330" not in m.split("沒有 3 檔")[1], "沒有三率的代號與產業: " + m
    twse = " ## ".join(by["probe:fin_dataset_twse"]); tpex = " ## ".join(by["probe:fin_dataset_tpex"])
    assert "財報／損益表相關資料集 2 個" in twse and "/opendata/other" not in twse and "t187ap06_L_basi 上市公司綜合損益表-銀行業｜2 筆｜欄位 公司代號 / 淨收益｜缺三率的代號出現在這裡：2882" in twse and "t187ap06_L_ci 上市公司綜合損益表-一般業｜2 筆｜欄位 公司代號 / 營業收入｜缺三率的代號出現在這裡：無" in twse, "上市各資料集: " + twse
    assert "mopsfin_t187ap06_O_ciA 上櫃公司財報資訊（一般業）｜1 筆｜欄位 SecuritiesCompanyCode / 營業收入｜缺三率的代號出現在這裡：6862" in tpex, "上櫃資料集（欄位名不是『公司代號』也認得）: " + tpex
    assert "官方財報資料集裡完全找不到的代號：00878" in by["probe:fin_missing_summary"][0] and "2882" not in by["probe:fin_missing_summary"][0].split("：")[1], "完全找不到: " + by["probe:fin_missing_summary"][0]
    fm = by["probe:fin_finmind"]; assert any("2882 國泰金（金融保險業）：FinMind 共 4 列；損益項目名稱：NetIncome、Revenue、EPS" in d for d in fm) and any("6862" in d and "共 0 列" in d for d in fm) and any("00878" in d and "FinMind 沒給資料" in d for d in fm), "FinMind 回傳的項目名稱: " + str(fm)
    assert not [x for x in logs if x["status"] == "failure"], "沒有失敗"


def test_probe_fin_coverage_failures_skips_and_limits():
    sw = {config.TWSE_OPENAPI_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}
    # 沒設資料庫：略過
    asked, logs = _run_fin(_fin_env(sw, {}, sb=False)); assert [(x["source"], x["status"]) for x in logs] == [("probe:fin_missing", "skipped")] and asked == []
    # 讀資料庫失敗
    asked, logs = _run_fin(_fin_env(sw, {}, tables={"stock_prices": None}))
    assert logs[0]["status"] == "failure" and "讀取資料庫失敗" in logs[0]["detail"] and len(logs) == 1
    # 每一檔都有三率：只記一筆就結束，不去抓資料集
    asked, logs = _run_fin(_fin_env(sw, {}, tables={"stock_prices": [{"code": "2330", "name": "台積電"}], "tw_fin_q?": [{"code": "2330"}]}))
    assert len(logs) == 1 and "沒有 0 檔" in logs[0]["detail"] and not [u for u in asked if "openapi" in u or "finmind" in u], "沒有缺的就不再往下查"
    # 說明文件抓不到、資料集沒有資料：各記一筆，不丟例外
    swb = {config.TWSE_OPENAPI_SWAGGER: {"paths": {"/a": {"get": {"summary": "上市公司財報資訊"}}}}}
    asked, logs = _run_fin(_fin_env(swb, {}, tables={"stock_prices": [{"code": "2882", "name": "國泰金"}]}))
    st = [(x["source"], x["status"]) for x in logs]; assert ("probe:fin_dataset_tpex", "failure") in st and ("probe:fin_dataset_twse", "no_new_data") in st and ("probe:fin_finmind", "no_new_data") in st, "各步驟各記一筆: " + str(st)
    # 數量上限：資料集最多 FIN_PROBE_MAX_DATASETS 個、FinMind 最多 FIN_PROBE_FINMIND 檔
    many = {config.TWSE_OPENAPI_SWAGGER: {"paths": {f"/p{i}": {"get": {"summary": f"上市公司財報資訊{i}"}} for i in range(30)}}, config.TPEX_SWAGGER: {"paths": {}}}
    ds = {f"/p{i}": [{"公司代號": "9999"}] for i in range(30)}
    px = [{"code": f"{3000 + i}", "name": f"股{i}"} for i in range(10)]
    asked, logs = _run_fin(_fin_env(many, ds, {}, {"stock_prices": px}))
    assert len([u for u in asked if "/p" in u and "finmind" not in u]) == config.FIN_PROBE_MAX_DATASETS and len([u for u in asked if "finmindtrade" in u]) == config.FIN_PROBE_FINMIND, "資料集與 FinMind 都有上限: " + str((len([u for u in asked if '/p' in u]), len([u for u in asked if 'finmindtrade' in u])))
    # 只讀：探測不寫任何資料列
    import inspect
    src = inspect.getsource(fetch.probe_fin_coverage); assert "persist(" not in src and '"POST"' not in src and '"PATCH"' not in src and '"DELETE"' not in src, "只讀"


def test_probe_dashboard_logs_api_definition_lines_even_when_the_right_side_is_an_expression():
    home = "https://www.twse.com.tw/zh/"
    page = '<title>x</title><script src="/dashboard/js/main.js"></script><script src="/dashboard/js/credit-margin.js"></script>' + "x" * 1300
    main = 'const TPEX_API = isProd ? "https://www.tpex.org.tw/dashboard" : "https://test.example/x"; var TWSE_API=location.origin+"/dashboardApi"; var ICON_BASE = "/img/a.png"; let appName = "x";'
    pages = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, page), "https://www.twse.com.tw/dashboard/js/main.js": (200, main), "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, '$.get(TWSE_API + "/marginTrading/X1");')}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    d = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_defs"]
    sn = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_snips"]
    assert len(sn) == 1 and 'TPEX_API = isProd ? "https://www.tpex.org.tw/dashboard" : "https://test.example/x"' in sn[0] and 'TWSE_API=location.origin+"/dashboardApi"' in sn[0], "指定名稱後面的原文也記下: " + str(sn)
    long_main = 'var a=1,webHost=oWL.hostname,isDev=/^(?:local|^192\\.168\\.)/.test(webHost),isZ="zh"===lan,api=new ApiPath("/rwd",lan),TPEX_API=isDev?"/tpex-api":"https://www.tpex.org.tw/openapi/v1",TWSE_API="/x";'
    pages2 = dict(pages); pages2["https://www.twse.com.tw/dashboard/js/main.js"] = (200, long_main)
    asked2, logs2, _p2 = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages2))
    sn2 = [v["detail"] for v in logs2.values() if v["source"] == "probe:dashboard_snips"][0]
    assert 'TPEX_API=isDev?"/tpex-api":"https://www.tpex.org.tw/openapi/v1"' in sn2 and 'api=new ApiPath("/rwd",lan)' in sn2 or 'new ApiPath("/rwd",lan)' in sn2, "壓縮成一長串也要抓到完整的 TPEX_API 值: " + sn2
    assert len(d) == 1 and 'TPEX_API = isProd ? "https://www.tpex.org.tw/dashboard" : "https://test.example/x"' in d[0] and 'TWSE_API = location.origin+"/dashboardApi"' in d[0] and "a.png" not in d[0] and "appName" not in d[0], "右邊是運算式也記下原文: " + str(d)


def test_probe_nasdaq_access_logs_ip_header_variants_and_backup_sources_without_writing_anything():
    reset()
    calls = []
    def fake(url, timeout=None, headers=None):
        calls.append((url, dict(headers or {})))
        if "ipify" in url: return 200, "application/json", b'{"ip":"20.1.2.3"}', ""
        if "api.nasdaq.com" in url:
            full = "Sec-Fetch-Mode" in (headers or {})
            return (200, "application/json", b'{"data":{"symbol":"SPY"}}', "") if full else (403, "text/html", b"<HTML><HEAD><TITLE>Access Denied</TITLE>", "")
        if "yahoo" in url: return 200, "application/json", json.dumps({"chart": {"result": [{"timestamp": [1762300800, 1762387200], "indicators": {"quote": [{"close": [680.1, 683.2]}]}}]}}).encode(), ""
        if "stooq" in url: return 200, "text/csv", b"Date,Open,High,Low,Close,Volume\n2026-11-10,1,2,0.5,1.5,100\n2026-11-11,1,2,0.5,1.6,110\n", ""
        return None, "", b"", "unexpected"
    with mock.patch.object(fetch, "fetch_raw", side_effect=fake), mock.patch.object(fetch, "_fetch_raw_once", side_effect=fake), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "persist") as per, mock.patch.object(fetch, "persist_extra") as pe:
        REAL["probe_nasdaq_access"]()
    by = {}
    for x in fetch.LOG: by.setdefault(x["source"], []).append(x)
    assert "20.1.2.3" in by["probe:nasdaq_access_ip"][0]["detail"], "記下對外 IP"
    hdr = by["probe:nasdaq_access_hdr"]; assert len(hdr) == 4 and [("HTTP 403" in h["detail"]) for h in hdr] == [True, False, True, True] and "Access Denied" in hdr[0]["detail"] and hdr[1]["status"] == "success" and hdr[0]["status"] == "no_new_data", "四組標頭各記一筆: " + str([h["detail"][:70] for h in hdr])
    assert "目前使用的標頭" in hdr[0]["detail"] and "完整瀏覽器標頭" in hdr[1]["detail"] and "只有 User-Agent" in hdr[2]["detail"] and "curl 風格" in hdr[3]["detail"]
    assert "2 筆，最新" in by["probe:nasdaq_access_yahoo"][0]["detail"] and by["probe:nasdaq_access_yahoo"][0]["status"] == "success"
    assert "3 行" in by["probe:nasdaq_access_stooq"][0]["detail"] and "2026-11-11,1,2,0.5,1.6,110" in by["probe:nasdaq_access_stooq"][0]["detail"]
    assert per.call_count == 0 and pe.call_count == 0 and len([c for c in calls if "api.nasdaq.com" in c[0]]) == 4, "只讀；Nasdaq 只打 4 次（每組標頭一次）"
    # 全部連不上：只記錄，不丟例外
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "逾時")), mock.patch.object(fetch, "_fetch_raw_once", return_value=(None, "", b"", "逾時")), mock.patch.object(fetch.time, "sleep"):
        REAL["probe_nasdaq_access"]()
    assert all(x["status"] in ("no_new_data",) for x in fetch.LOG if x["source"].startswith("probe:nasdaq_access")) and len([x for x in fetch.LOG if x["source"].startswith("probe:nasdaq_access")]) == 7


def _twd_env(fred_rows, yahoo_pts, status=200, ready=True):
    def sbr(method, path, body=None, prefer=None):
        return (fred_rows, "") if "twd_rates" in path else ([], "")
    def raw(url, timeout=None, headers=None):
        if status != 200: return status, "", b"", f"HTTP {status}"
        return 200, "application/json", json.dumps({"chart": {"result": [{"meta": {"gmtoffset": 0}, "timestamp": [int(datetime.fromisoformat(d_ + "T12:00:00+00:00").timestamp()) for d_, _v in yahoo_pts], "indicators": {"quote": [{"close": [v for _d, v in yahoo_pts]}]}}]}}).encode(), ""
    return [mock.patch.object(fetch, "sb_ready", return_value=ready), mock.patch.object(fetch, "sb_request", side_effect=sbr), mock.patch.object(fetch, "fetch_raw", side_effect=raw)]


def test_yahoo_twd_fill_adds_only_newer_days_in_a_separate_unofficial_layer_after_value_and_date_checks():
    dd = lambda n: (date.today() - timedelta(days=n)).isoformat()
    vals = [31.0, 31.4, 30.9, 31.5, 31.1, 31.6]                           # 每天變動約 1%：日期差一天一定看得出來
    fred = [{"as_of": dd(12 - i), "value": v} for i, v in enumerate(vals)][::-1]                       # 由新到舊（資料庫的 order=as_of.desc）
    same = [(dd(12 - i), v * 1.001) for i, v in enumerate(vals)] + [(dd(5), 31.7), (dd(3), 31.2), (dd(1), 31.3), (date.today().isoformat(), 31.4), (dd(2), 3.1)]
    reset()
    rows = _with(_twd_env(fred, same), lambda: REAL["yahoo_twd_fill"]())
    assert [r["as_of"] for r in rows] == [dd(5), dd(3), dd(1)], "只補比 FRED 最新一天（%s）更新、早於今天的日子；超出合理範圍的 3.1 不收: %s" % (dd(7), [r["as_of"] for r in rows])
    assert all(r["layer"] == 4 and r["kind"] == "spot_aggregator" and r["is_close_history"] is False and r["source"].startswith("Yahoo Finance（非官方") for r in rows), "另一層、不算收盤歷史、標非官方"
    lg = [x for x in fetch.LOG if x["source"] == "yahoo_twd"][0]; assert lg["status"] == "success" and "補 3 天" in lg["detail"] and "平均差 0.10%" in lg["detail"], lg["detail"]
    # 數值口徑差太多
    reset(); rows = _with(_twd_env(fred, [(d_, v * 1.03) for d_, v in same]), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and "口徑不同" in fetch.LOG[-1]["detail"]
    # 日期差一天：數值平均差看起來可能不大，但位移一天比不位移更吻合 → 不用
    shifted = [((date.fromisoformat(d_) + timedelta(days=1)).isoformat(), v) for d_, v in [(dd(12 - i), v) for i, v in enumerate(vals)]] + [(dd(3), 31.2)]
    reset(); rows = _with(_twd_env(fred, shifted), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and ("日期位移" in fetch.LOG[-1]["detail"] or "口徑不同" in fetch.LOG[-1]["detail"]), fetch.LOG[-1]["detail"]
    # 重疊太少、FRED 資料太少、Yahoo 擋住、格式不對、沒設 Supabase：都只記錄、不補、不變紅
    reset(); rows = _with(_twd_env(fred, [(dd(1), 31.3), (dd(2), 31.2)]), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and "只有 0 天" in fetch.LOG[-1]["detail"]
    reset(); rows = _with(_twd_env(fred[:2], same), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and "不到 3 筆" in fetch.LOG[-1]["detail"]
    reset(); rows = _with(_twd_env(fred, same, status=429), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and "HTTP 429" in fetch.LOG[-1]["detail"]
    reset(); rows = _with(_twd_env(fred, same, ready=False), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and fetch.LOG[-1]["status"] == "skipped"
    assert not [x for x in fetch.LOG if x["status"] == "failure"]
    # FRED 已經是最新：不需要補
    reset(); rows = _with(_twd_env(fred, [(d_, v) for d_, v in same if d_ <= dd(7)]), lambda: REAL["yahoo_twd_fill"]()); assert rows == [] and "FRED 已經是最新" in fetch.LOG[-1]["detail"]
    # 資料庫裡的 FRED 太稀疏（只有 2 筆）：同一次執行剛抓到的每日歷史（extra_fred）也算進核對
    reset(); extra = [{"as_of": dd(12 - i), "value": v} for i, v in enumerate(vals)]
    rows = _with(_twd_env(fred[:2], same), lambda: REAL["yahoo_twd_fill"](extra)); assert [r["as_of"] for r in rows] == [dd(5), dd(3), dd(1)], "資料庫稀疏、但本次歷史夠：照樣核對並補"


def test_run_job_tw_close_persists_yahoo_twd_rows_in_their_own_layer_and_isolates_a_crash():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    rows_ = [{"as_of": "2026-11-10", "value": 31.2, "kind": "spot_aggregator", "source": "Yahoo Finance（非官方，補 FRED 落後的天數）", "layer": 4, "is_close_history": False}]
    def go(**kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "yahoo_twd_fill", **kw), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
        for p_ in ps: p_.start()
        try:
            fetch.run_job("tw_close", dry_run=False)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(return_value=rows_); assert ("twd_rates", rows_, "layer,source,as_of") in tables and st["module:M36"] == "success", "寫進 twd_rates，主鍵是 layer,source,as_of"
    reset(); tables, st = go(side_effect=RuntimeError("boom")); assert st["module:M36"] == "failure" and ("twd_rates", rows_, "layer,source,as_of") not in tables and "module:M27" in st, "壞掉不影響其他模組"


def test_twd_fred_history_writes_daily_official_rows_in_the_same_shape_as_the_daily_row_and_never_turns_red():
    reset()
    seen = []
    def fr(sid, low, high, days=None):
        seen.append((sid, low, high, days))
        return [{"series_id": sid, "label": sid, "as_of": "2026-10-29", "value": 31.4, "source": "FRED"}, {"series_id": sid, "label": sid, "as_of": "2026-10-30", "value": 31.3, "source": "FRED"}]
    with mock.patch.object(fetch, "fred_history_rows", side_effect=fr):
        rows = REAL["twd_fred_history"]()
    assert seen == [(config.TWD_FRED_SERIES, config.TWD_RANGE[0], config.TWD_RANGE[1], config.TWD_HISTORY_DAYS)] and config.TWD_HISTORY_DAYS >= 300
    assert rows == [{"as_of": d_, "value": v, "kind": "official_close", "source": "FRED DEXTAUS", "layer": 3, "is_close_history": True} for d_, v in (("2026-10-29", 31.4), ("2026-10-30", 31.3))], "和每天更新的那一筆同一種列（才會用同一個主鍵蓋過去）: " + str(rows)
    lg = [x for x in fetch.LOG if x["source"] == "twd_history"][0]; assert lg["status"] == "success" and "2 筆每日收盤" in lg["detail"]
    reset()
    with mock.patch.object(fetch, "fred_history_rows", return_value=None):
        assert REAL["twd_fred_history"]() == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and not [x for x in fetch.LOG if x["status"] == "failure"]
    # fred_history_rows 本身：台幣不在 FRED_MACRO 裡也不會出錯（標籤用代號）
    with mock.patch.object(fetch, "fetch_json", return_value=({"observations": [{"date": (date.today() - timedelta(days=3)).isoformat(), "value": "31.25"}, {"date": (date.today() - timedelta(days=4)).isoformat(), "value": "."}]}, "")), mock.patch.object(config, "fred_key", return_value="k"):
        got = fetch.fred_history_rows("DEXTAUS", 20.0, 45.0, days=400)
    assert len(got) == 1 and got[0]["label"] == "DEXTAUS" and got[0]["value"] == 31.25, got


def test_run_job_tw_close_persists_twd_history_and_lets_the_yahoo_fill_check_against_it_in_the_same_run():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    hist = [{"as_of": "2026-10-29", "value": 31.4, "kind": "official_close", "source": "FRED DEXTAUS", "layer": 3, "is_close_history": True}]
    yh = [{"as_of": "2026-11-10", "value": 31.0, "kind": "spot_aggregator", "source": "Yahoo Finance（非官方，補 FRED 落後的天數）", "layer": 4, "is_close_history": False}]
    got, passed = [], []
    ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
          mock.patch.object(fetch, "twd_fred_history", return_value=hist), mock.patch.object(fetch, "yahoo_twd_fill", side_effect=lambda extra=None: (passed.append(extra), yh)[1]), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
    for p_ in ps: p_.start()
    try:
        fetch.run_job("tw_close", dry_run=False)
    finally:
        for p_ in ps: p_.stop()
    tables = got[0]; st = {x["source"]: x["status"] for x in fetch.LOG}
    assert ("twd_rates", hist, "layer,source,as_of") in tables and ("twd_rates", yh, "layer,source,as_of") in tables and passed == [hist], "歷史與補充都寫入；補充用同一次抓到的歷史核對: " + str(passed)
    assert st["module:M37"] == "success" and st["module:M36"] == "success"
    # 歷史壞掉：不影響 Yahoo 補充（它還有資料庫裡的可以用）與其他模組
    reset(); got.clear(); passed.clear()
    ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
          mock.patch.object(fetch, "twd_fred_history", side_effect=RuntimeError("boom")), mock.patch.object(fetch, "yahoo_twd_fill", side_effect=lambda extra=None: (passed.append(extra), [])[1]), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
    for p_ in ps: p_.start()
    try:
        fetch.run_job("tw_close", dry_run=False)
    finally:
        for p_ in ps: p_.stop()
    st = {x["source"]: x["status"] for x in fetch.LOG}; assert st["module:M37"] == "failure" and st["module:M36"] == "no_new_data" and passed == [[]] and "module:M27" in st, "歷史壞掉：Yahoo 補充照常執行（沒產出資料就是 no_new_data）、其他模組照常: " + str(st)


def test_probe_dashboard_falls_back_to_the_twse_rwd_base_for_margin_endpoints():
    home = "https://www.twse.com.tw/zh/"
    page = '<title>x</title><script src="/dashboard/js/credit-margin.js"></script>' + "x" * 1300
    pages = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, page),
             "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, 'a=apiAny.name("/marginTrading/BFIJ3U")+"?response=json";$.get(TPEX_API+"/dashboardOtc/margin?lang=zh-tw");')}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    assert any(u.startswith("https://www.twse.com.tw/rwd/zh/marginTrading/BFIJ3U?response=json") for u in asked), "證交所的端點用 /rwd/zh 當基底並加 ?response=json: " + str([u for u in asked if "rwd" in u or "marginTrading" in u])
    api = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_api"]
    assert any("/dashboardOtc/margin" in d and "找不到 API 基底網址" in d for d in api), "櫃買的還是不知道基底：照實記錄、不猜: " + str(api)


def _keep_rec(n, kr, **kw):
    d_ = (date.today() - timedelta(days=n)).strftime("%Y%m%d")
    r = {"date": d_, "crdAmt": 142552753320, "keepRate": kr, "belowAccNum": 3382, "callAccNum": 3085, "callAmt": 3806099400, "exeAccNum": 171, "exeAmt": 516535190, "marginAccNum": 85123, "marginAccRate": 0.31}
    r.update(kw); return r


def _with_keep(cur, trend):
    seen = []
    def fj(url, headers=None, timeout=None):
        seen.append(url)
        if "BFIJ3U_TREND" in url: return trend if isinstance(trend, tuple) else (trend, "")
        return cur if isinstance(cur, tuple) else (cur, "")
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        rows = REAL["tw_keep_rate"]()
    return rows, seen


def test_tw_keep_rate_reads_the_official_dashboard_api_validates_each_day_and_prefers_the_latest_call():
    reset()
    trend = {"stat": "OK", "days": 5, "data": [_keep_rec(6, 158.05), _keep_rec(5, 160.5), _keep_rec(4, 165.2), _keep_rec(3, 170.0), _keep_rec(2, 171.0)]}
    cur = dict(_keep_rec(2, 173.99, crdAmt=128232305040, belowAccNum=238, callAccNum=102, callAmt=89224800, exeAccNum=56, exeAmt=29619157, marginAccNum=71213, marginAccRate=0.26), stat="OK")
    rows, seen = _with_keep((cur, ""), (trend, ""))
    assert [r["keep_rate"] for r in rows] == [158.05, 160.5, 165.2, 170.0, 173.99], "由舊到新；同一天以最新一天的呼叫為準（171.0 被 173.99 蓋掉）: " + str([r["keep_rate"] for r in rows])
    assert seen[0].startswith(config.TWSE_RWD_BASE + "/marginTrading/BFIJ3U?response=json") and "BFIJ3U_TREND?response=json&days=%d" % config.KEEP_RATE_DAYS in seen[1], seen
    last = rows[-1]; assert last["credit_amt"] == 128232305040 and last["below_acc"] == 238 and last["call_acc"] == 102 and last["call_amt"] == 89224800 and last["exe_acc"] == 56 and last["exe_amt"] == 29619157 and last["margin_acc"] == 71213 and last["margin_acc_rate"] == 0.26 and last["source"] == "TWSE 臺股儀表板 BFIJ3U", last
    assert last["as_of"] == (date.today() - timedelta(days=2)).isoformat() and all(set(r) == set(last) for r in rows), "日期轉成 ISO、欄位一致"
    lg = [x for x in fetch.LOG if x["source"] == "tw_keep_rate"][0]; assert lg["status"] == "success" and "全市場擔保維持率 173.99%" in lg["detail"] and "共 5 天" in lg["detail"], lg["detail"]
    # 每一天都驗證：未來日期、範圍外、非數字、壞日期、空值都略過（並記下略過幾筆）
    reset(); bad = {"stat": "OK", "data": [_keep_rec(3, 160.0), _keep_rec(-2, 170.0), _keep_rec(2, 5.0), _keep_rec(2, 999.0), _keep_rec(2, "壞"), _keep_rec(2, None), dict(_keep_rec(2, 165.0), date="2026-13-45"), "不是字典", _keep_rec(1, 168.0)]}
    rows, _s = _with_keep((None, "x"), (bad, "")); assert [r["keep_rate"] for r in rows] == [160.0, 168.0], [r["keep_rate"] for r in rows]
    assert "略過 6 筆不符的" in [x for x in fetch.LOG if x["source"] == "tw_keep_rate"][0]["detail"], fetch.LOG[-1]["detail"]
    # 趨勢抓不到、只有最新一天：照樣收
    reset(); rows, _s = _with_keep((cur, ""), (None, "HTTP 503")); assert len(rows) == 1 and rows[0]["keep_rate"] == 173.99 and fetch.LOG[-1]["status"] == "success"
    # stat 不是 OK（例如休市）：沒有新資料，不算壞掉
    reset(); rows, _s = _with_keep(({"stat": "很抱歉，沒有符合條件的資料!"}, ""), ({"stat": "很抱歉，沒有符合條件的資料!", "data": []}, "")); assert rows == [] and fetch.LOG[-1]["status"] == "no_new_data" and "BFIJ3U stat=" in fetch.LOG[-1]["detail"]
    # 兩個都抓不到：failure（要看得到）
    reset(); rows, _s = _with_keep((None, "HTTP 403"), (None, "逾時")); assert rows == [] and fetch.LOG[-1]["status"] == "failure" and "HTTP 403" in fetch.LOG[-1]["detail"] and "逾時" in fetch.LOG[-1]["detail"]
    # 非預期的格式（不是 dict／data 不是 list）不丟例外
    reset(); rows, _s = _with_keep((["x"], ""), ({"stat": "OK", "data": "壞"}, "")); assert rows == [] and fetch.LOG[-1]["status"] == "no_new_data"


def test_run_job_tw_close_persists_keep_rate_rows_and_isolates_a_crash():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    rows_ = [{"as_of": "2026-11-12", "keep_rate": 173.99, "credit_amt": 1.0, "below_acc": 1, "call_acc": 1, "call_amt": 1.0, "exe_acc": 1, "exe_amt": 1.0, "margin_acc": 1, "margin_acc_rate": 0.26, "source": "TWSE 臺股儀表板 BFIJ3U"}]
    def go(**kw):
        got = []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "tw_keep_rate", **kw), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
        for p_ in ps: p_.start()
        try:
            fetch.run_job("tw_close", dry_run=False)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go(return_value=rows_); assert ("tw_keep_rate", rows_, "as_of,source") in tables and st["module:M38"] == "success"
    reset(); tables, st = go(side_effect=RuntimeError("boom")); assert st["module:M38"] == "failure" and ("tw_keep_rate", rows_, "as_of,source") not in tables and "module:M27" in st


def test_probe_chips_logs_the_shape_of_each_chip_source_with_the_three_stocks_rows_and_writes_nothing():
    reset()
    t86 = {"stat": "OK", "fields": ["證券代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "投信買進股數"], "data": [["2330", "台積電", "1", "2"], ["3037", "欣興", "10", "20"], ["3189", "景碩", "30", "40"]]}
    margin = {"stat": "OK", "date": "20261112", "tables": [{"fields": ["x"], "data": [["a"]]}, {"fields": ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "限額"], "data": [["3037", "欣興", "1", "2", "3", "100", "99", "5000"], ["3189", "景碩", "1", "2", "3", "100", "99", "4000"], ["1101", "台泥", "0", "0", "0", "5", "5", "100"]]}]}
    tdcc = "資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%\n" + "\n".join(f"20261106,{c},{lv},{10 * lv},{1000 * lv},{lv}.5" for c in ("3037", "3189", "1101", "1102", "1216", "2002", "2317", "2412", "2454", "2603") for lv in range(1, 16))
    punish = {"stat": "OK", "fields": ["編號", "公布日期", "證券代號", "證券名稱"], "data": [["1", "115/11/10", "3189", "景碩"], ["2", "115/11/09", "9999", "某某"]]}
    def raw(url, timeout=None, headers=None):
        if "T86" in url: return 200, "application/json", json.dumps(t86 if "date=%s" % (date.today() - timedelta(days=0 if date.today().weekday() < 5 else date.today().weekday() - 4)).strftime("%Y%m%d") in url else {"stat": "很抱歉，沒有符合條件的資料!"}).encode(), ""
        if "tdcc" in url: return 200, "text/csv", tdcc.encode("utf-8"), ""
        if "punish" in url: return 200, "application/json", json.dumps(punish).encode(), ""
        return None, "", b"", "unexpected"
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "fetch_json", return_value=(margin, "")), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "persist") as per, mock.patch.object(fetch, "persist_extra") as pe:
        REAL["probe_chips"]()
    by = {x["source"]: x for x in fetch.LOG}
    assert "欄位 ['證券代號'" in by["probe:chips_t86"]["detail"] and "['3037', '欣興', '10', '20']" in by["probe:chips_t86"]["detail"] and "['3189', '景碩', '30', '40']" in by["probe:chips_t86"]["detail"] and "['2330'" not in by["probe:chips_t86"]["detail"] and "3 檔" in by["probe:chips_t86"]["detail"], by["probe:chips_t86"]["detail"]
    assert "'限額'" in by["probe:chips_margin"]["detail"] and "['3037', '欣興'" in by["probe:chips_margin"]["detail"] and "3 檔" in by["probe:chips_margin"]["detail"] and "['a']" not in by["probe:chips_margin"]["detail"], "取最大的那張表、列出限額欄: " + by["probe:chips_margin"]["detail"]
    assert "表頭 資料日期,證券代號,持股分級" in by["probe:chips_tdcc"]["detail"] and "資料日期 ['20261106']" in by["probe:chips_tdcc"]["detail"] and "共 30 行" in by["probe:chips_tdcc"]["detail"] and by["probe:chips_tdcc"]["status"] == "success", by["probe:chips_tdcc"]["detail"]
    assert "2 筆" in by["probe:chips_punish"]["detail"] and "景碩" in by["probe:chips_punish"]["detail"] and by["probe:chips_punish"]["status"] == "success"
    assert per.call_count == 0 and pe.call_count == 0, "只讀"
    # 全部連不上：只記錄、不丟例外
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "逾時")), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch.time, "sleep"):
        REAL["probe_chips"]()
    st = {x["source"]: x["status"] for x in fetch.LOG}; assert st == {"probe:chips_t86": "no_new_data", "probe:chips_margin": "no_new_data", "probe:chips_tdcc": "no_new_data", "probe:chips_punish": "no_new_data"}, st


def _summary(pe="33.7", fw="29.6", eps="$6.7", prev="$225.89", keys=True, label_only=False):
    d = {"Exchange": {"label": "Exchange", "value": "NASDAQ-GS"}}
    items = {"PERatio": ("P/E Ratio", pe), "ForwardPE1Yr": ("Forward P/E 1 Yr.", fw), "EarningsPerShare": ("Earnings Per Share(EPS)", eps), "PreviousClose": ("Previous Close", prev)}
    for k, (lab, v) in items.items():
        d[("X_" + k) if label_only else k] = {"label": lab, "value": v}
    return {"data": {"symbol": "AMD", "summaryData": d}, "status": {"rCode": 200}}


def test_parse_nasdaq_summary_reads_pe_forward_pe_eps_by_key_or_label_and_rejects_bad_numbers():
    p = fetch.parse_nasdaq_summary
    v, why = p(_summary()); assert why is None and v == {"pe": 33.7, "forward_pe": 29.6, "eps": 6.7, "prev_close": 225.89}, v
    v, _w = p(_summary(label_only=True)); assert v["pe"] == 33.7 and v["forward_pe"] == 29.6 and v["eps"] == 6.7, "鍵名改了：用標籤文字也找得到: " + str(v)
    v, _w = p(_summary(pe="N/A", fw="", eps="-1.25", prev="--")); assert v == {"pe": None, "forward_pe": None, "eps": -1.25, "prev_close": None}, "N/A、空的、--：當作沒有；負的每股盈餘保留: " + str(v)
    v, _w = p(_summary(pe="0", fw="99999", eps="壞", prev="$-5")); assert v["pe"] is None and v["forward_pe"] is None and v["eps"] is None and v["prev_close"] is None, "不合理的數字不收: " + str(v)
    v, _w = p(_summary(pe="1,234.5")); assert v["pe"] == 1234.5, "千分位"
    for bad in (None, {}, {"data": None}, {"data": {"summaryData": None}}, {"data": {"summaryData": {}}}, [], "x"):
        v, why = p(bad); assert v is None and "summaryData" in why, bad


def test_us_valuation_fetches_each_stock_summary_skips_etfs_and_logs_what_is_missing():
    reset()
    hold = [{"market": "US", "symbol": "AMD", "kind": "stock"}, {"market": "US", "symbol": "MUU", "kind": "leveraged_etf"}, {"market": "US", "symbol": "SPY", "kind": "etf"}, {"market": "US", "symbol": "ORCL", "kind": "stock"},
            {"market": "US", "symbol": "LOSS", "kind": "stock"}, {"market": "US", "symbol": "BAD", "kind": "stock"}, {"market": "US", "symbol": "NOJ", "kind": "stock"}, {"market": "TW", "symbol": None, "kind": "stock"}]
    seen = []
    def raw(url, timeout=None, headers=None):
        sym = url.split("/quote/")[1].split("/")[0]; seen.append(sym)
        if sym == "AMD": return 200, "application/json", json.dumps(_summary()).encode(), ""
        if sym == "ORCL": return 200, "application/json", json.dumps(_summary(pe="41.2", fw="31.0", eps="$5.1")).encode(), ""
        if sym == "LOSS": return 200, "application/json", json.dumps(_summary(pe="N/A", fw="N/A", eps="-2.1")).encode(), ""
        if sym == "BAD": return 404, "text/html", b"x", "HTTP 404"
        return 200, "application/json", b"not json", ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["us_valuation"](hold)
    assert sorted(seen) == ["AMD", "BAD", "LOSS", "NOJ", "ORCL"], "只查個股（ETF 與台股不查）: " + str(seen)
    by = {r["symbol"]: r for r in rows}; assert sorted(by) == ["AMD", "LOSS", "ORCL"], sorted(by)
    assert by["AMD"]["pe"] == 33.7 and by["AMD"]["forward_pe"] == 29.6 and by["AMD"]["eps"] == 6.7 and "不是共識中位數" in by["AMD"]["source"]
    assert by["LOSS"]["pe"] is None and by["LOSS"]["forward_pe"] is None and by["LOSS"]["eps"] == -2.1, "虧損：摘要有拿到、本益比留空，仍寫一列"
    lg = [x for x in fetch.LOG if x["source"] == "us_valuation"][0]; assert lg["status"] == "success" and "摘要有 3 檔（有本益比 2、有預估本益比 2）" in lg["detail"] and "BAD（HTTP 404" in lg["detail"] and "NOJ（不是 JSON）" in lg["detail"], lg["detail"]
    reset(); assert REAL["us_valuation"]([{"market": "US", "symbol": "SPY", "kind": "etf"}]) == [] and fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(403, "", b"", "HTTP 403")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["us_valuation"]([{"market": "US", "symbol": "AMD", "kind": "stock"}]) == []
    assert fetch.LOG[-1]["status"] == "no_new_data" and "AMD（HTTP 403" in fetch.LOG[-1]["detail"] and not [x for x in fetch.LOG if x["status"] == "failure"], "被擋：只記錄、不變紅"


def test_parse_nasdaq_summary_accepts_other_label_wordings_and_never_mixes_up_forward_and_trailing():
    p = fetch.parse_nasdaq_summary
    def mk(items): return {"data": {"summaryData": {f"K{i}": {"label": lab, "value": v} for i, (lab, v) in enumerate(items)}}}
    v, _w = p(mk([("PE Ratio (TTM)", "41.5"), ("Forward P/E (1yr)", "30.2"), ("PEG Ratio", "2.1"), ("EPS", "$3.4"), ("Previous Close", "$140.00")])); assert v["pe"] == 41.5 and v["forward_pe"] == 30.2 and v["eps"] == 3.4 and v["prev_close"] == 140.0, v
    v, _w = p(mk([("Forward P/E 1 Yr.", "28.0"), ("P/E Ratio", "N/A")])); assert v["pe"] is None and v["forward_pe"] == 28.0, "只有預估、沒有本益比：兩者不互相冒充: " + str(v)
    v, _w = p(mk([("PEG Ratio", "1.5")])); assert v["pe"] is None and v["forward_pe"] is None, "PEG 不是本益比"
    items = fetch.nasdaq_summary_items(mk([("P/E Ratio", "N/A"), ("Market Cap", "1,000")])); assert "K0｜P/E Ratio｜N/A" in items and "K1｜Market Cap｜1,000" in items, items
    assert "沒有 summaryData" in fetch.nasdaq_summary_items({"data": {"x": 1}}) and "沒有 summaryData" in fetch.nasdaq_summary_items(None)


def test_us_valuation_derives_pe_from_eps_when_nasdaq_gives_none_marks_it_and_logs_the_real_field_list():
    reset()
    body = json.dumps({"data": {"summaryData": {"PERatio": {"label": "P/E Ratio", "value": "N/A"}, "EarningsPerShare": {"label": "Earnings Per Share(EPS)", "value": "$4.00"}, "PreviousClose": {"label": "Previous Close", "value": "$200.00"}, "ForwardPE1Yr": {"label": "Forward P/E 1 Yr.", "value": "25.0"}}}}).encode()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body, "")), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["us_valuation"]([{"market": "US", "symbol": "AMD", "kind": "stock"}])
    assert len(rows) == 1 and rows[0]["pe"] == 50.0 and rows[0]["forward_pe"] == 25.0 and "自己算的" in rows[0]["source"] and "前一日收盤 ÷ 每股盈餘" in rows[0]["source"], rows
    sh = [x for x in fetch.LOG if x["source"] == "us_valuation_shape"]; assert len(sh) == 1 and sh[0]["status"] == "success" and "AMD｜PERatio｜P/E Ratio｜N/A" in sh[0]["detail"] and "EarningsPerShare｜Earnings Per Share(EPS)｜$4.00" in sh[0]["detail"], "把實際欄位記下來: " + str(sh)
    # 每股盈餘是負的（虧損）：不自己算
    reset(); body2 = json.dumps({"data": {"summaryData": {"PERatio": {"label": "P/E Ratio", "value": "N/A"}, "EarningsPerShare": {"label": "EPS", "value": "-1.5"}, "PreviousClose": {"label": "Previous Close", "value": "$20"}}}}).encode()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "application/json", body2, "")), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["us_valuation"]([{"market": "US", "symbol": "LOSS", "kind": "stock"}])
    assert rows[0]["pe"] is None and "自己算" not in rows[0]["source"], "虧損不算本益比"


def test_run_job_tw_close_persists_us_valuation_rows_and_isolates_a_crash():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    rows_ = [{"symbol": "AMD", "pe": 33.7, "forward_pe": 29.6, "eps": 6.7, "prev_close": 225.89, "source": "Nasdaq 個股摘要"}]
    def go(**kw):
        got, seen = [], []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "load_us_watch", return_value=["AMD"]), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "us_valuation", side_effect=lambda h: (seen.append(h), kw["ret"])[1] if "ret" in kw else (_ for _ in ()).throw(RuntimeError("boom"))), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
        for p_ in ps: p_.start()
        try:
            fetch.run_job("tw_close", dry_run=False)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}, seen
    tables, st, seen = go(ret=rows_); assert ("us_valuation", rows_, "symbol") in tables and st["module:M39"] == "success" and [h["symbol"] for h in seen[0]] == ["AMD"], "候選股也一起查、主鍵是 symbol"
    reset(); tables, st, _s = go(); assert st["module:M39"] == "failure" and ("us_valuation", rows_, "symbol") not in tables and "module:M26" in st, "壞掉不影響其他模組"


def test_probe_vix_otc_logs_pages_links_files_swagger_candidates_and_yahoo_writes_nothing_and_never_raises():
    reset()
    page = '<html><a href="/cht/7/vixDaily3MNew">x</a><a href="/file/taifex/Dailydownload/vix/log2data/2026/vix_202610.csv">2026/10</a><a href="javascript:void(0)">y</a><a href="/file/a.zip">z</a><table><tr><td>臺指選擇權波動率指數</td></tr><tr><td>1</td></tr></table></html>'.encode("utf-8")
    csv = "日期,臺指選擇權波動率指數\n20261112,22.5\n20261111,23.1\n".encode("big5")
    chart = json.dumps({"chart": {"result": [{"timestamp": [1762300800, 1762387200, 1762473600], "indicators": {"quote": [{"close": [440.1, 441.2, 442.3]}]}}]}}).encode()
    seen = []
    def raw(url, timeout=None, headers=None):
        seen.append(url)
        if "vixDaily3MNew" in url or "vixMinNew" in url: return 200, "text/html", page, ""
        if url.endswith("vix_202610.csv"): return 200, "text/csv", csv, ""
        if url.endswith("a.zip"): return 404, "", b"", "HTTP 404"
        if "TWOII" in url: return 200, "application/json", chart, ""
        return None, "", b"", "unexpected"
    swagger = {"paths": {"/tpex_index": {"get": {"summary": "櫃買指數每日收盤歷史", "description": ""}}, "/other": {"get": {"summary": "上櫃股票行情", "description": ""}}, "/tpex_50": {"get": {"summary": "櫃買富櫃50指數當日收盤", "description": "櫃買指數"}}}}
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "fetch_swagger", return_value=(swagger, "")), mock.patch.object(fetch, "fetch_json", return_value=([{"Date": "1151112", "Index": "300.5"}, {"Date": "1151111", "Index": "299.1"}], "")), mock.patch.object(fetch.time, "sleep"), \
            mock.patch.object(fetch, "persist") as per, mock.patch.object(fetch, "persist_extra") as pe:
        REAL["probe_vix_otc"]()
    by = {}
    for x in fetch.LOG: by.setdefault(x["source"], []).append(x)
    d = by["probe:vix_daily3m"][0]; assert d["status"] == "success" and "有「臺指選擇權波動率指數」字樣 True" in d["detail"] and "vix_202610.csv" in d["detail"] and "javascript" not in d["detail"] and "<tr> 2 列" in d["detail"], d["detail"]
    tb = by["probe:vix_daily3m_table"][0]; assert tb["status"] == "success" and "臺指選擇權波動率指數 ## 1" in tb["detail"], "表格各列的文字也記下來（值可能在表格裡）: " + tb["detail"]
    files = by["probe:vix_daily3m_file"]; assert any(x["status"] == "success" and "20261112,22.5" in x["detail"] and "big5" not in x["detail"] for x in files) and any(x["status"] == "no_new_data" and "HTTP 404" in x["detail"] for x in files), "csv 用 Big5 解碼、404 的記錄原因: " + str([x["detail"][:90] for x in files])
    sw = by["probe:otc_swagger"][0]; assert "含「櫃買指數收盤／歷史」字樣的 2 個" in sw["detail"] and "/tpex_index" in sw["detail"] and "/other" not in sw["detail"], sw["detail"]
    ep = by["probe:otc_endpoint"]; assert ep and ep[0]["status"] == "success" and "/tpex_index" in ep[0]["detail"] and "2 筆" in ep[0]["detail"] and "Index" in ep[0]["detail"], ep[0]["detail"]
    assert by["probe:otc_yahoo"][0]["status"] == "success" and "3 筆" in by["probe:otc_yahoo"][0]["detail"]
    assert per.call_count == 0 and pe.call_count == 0, "只讀"
    # 全部失敗：只記錄、不丟例外、不記失敗
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "逾時")), mock.patch.object(fetch, "fetch_swagger", return_value=(None, "HTTP 503")), mock.patch.object(fetch.time, "sleep"):
        REAL["probe_vix_otc"]()
    srcs = [x for x in fetch.LOG if x["source"].startswith("probe:vix") or x["source"].startswith("probe:otc")]
    assert srcs and all(x["status"] == "no_new_data" for x in srcs) and not [x for x in fetch.LOG if x["status"] == "failure"], str([(x["source"], x["status"]) for x in srcs])


def test_tw_otc_index_reads_the_open_data_validates_each_day_and_sorts_oldest_first():
    reset()
    dd = lambda n: (TODAY - timedelta(days=n)).strftime("%Y%m%d")
    obj = [{"Date": dd(1), "Open": "368.22", "High": "373.28", "Low": "362.18", "Close": "369.24", "Change": "-1.02"}, {"Date": dd(2), "Open": "366", "High": "367", "Low": "345", "Close": "345.52", "Change": "-23.72"},
           {"Date": dd(-3), "Close": "300"}, {"Date": dd(3), "Close": "9999"}, {"Date": dd(4), "Close": "壞"}, {"Date": "2026-13-40", "Close": "300"}, "不是物件", {"Date": dd(5), "Close": ""}]
    with mock.patch.object(fetch, "fetch_json", return_value=(obj, "")) as fj:
        rows = REAL["tw_otc_index"]()
    assert fj.call_args[0][0].endswith("/tpex_index"), fj.call_args
    assert [r["as_of"] for r in rows] == [(TODAY - timedelta(days=2)).isoformat(), (TODAY - timedelta(days=1)).isoformat()] and rows[1]["close"] == 369.24 and rows[1]["change"] == -1.02 and rows[1]["open"] == 368.22 and rows[0]["source"] == "櫃買中心開放資料 tpex_index", rows
    lg = [x for x in fetch.LOG if x["source"] == "tw_otc_index"][0]; assert lg["status"] == "success" and "2 天" in lg["detail"] and "略過 6 筆不符的" in lg["detail"], lg["detail"]
    for bad in ((None, "HTTP 503"), ({"x": 1}, ""), ([], ""), ([{"Date": "x"}], "")):
        reset()
        with mock.patch.object(fetch, "fetch_json", return_value=bad):
            assert REAL["tw_otc_index"]() == []
        assert fetch.LOG[-1]["status"] == "no_new_data" and not [x for x in fetch.LOG if x["status"] == "failure"], bad


def _hist(rows_by_code):
    def sbr(method, path, body=None, prefer=None):
        if path.startswith("private_watchlist"): return ([{"code": "6488"}, {"code": "壞"}], "")
        for code, rows in rows_by_code.items():
            if f"code=eq.{code}&" in path: return (rows, "")
        return ([], "")
    return sbr


def test_tw_rel_strength_computes_5_and_20_day_returns_and_the_market_from_history_with_gap_checks():
    reset()
    base = TODAY - timedelta(days=1)
    while base.weekday() >= 5: base -= timedelta(days=1)                      # 最近一個平日
    wd = []                                                                    # 連續 30 個「交易日」（只算平日），每天 +1
    d_ = base
    while len(wd) < 30:
        if d_.weekday() < 5: wd.append(d_)
        d_ -= timedelta(days=1)
    wd.reverse()
    daily = [{"market": "TWSE", "as_of": x.isoformat(), "close": 100.0 + i} for i, x in enumerate(wd)]       # 最新 129；5 筆前 124；20 筆前 109
    gap = [{"market": "TPEX", "as_of": (base - timedelta(days=(29 - i) * 3)).isoformat(), "close": 100.0 + i} for i in range(30)]       # 每 3 天一筆：5 筆前是 15 天前（超過 12 天）、20 筆前是 60 天前 → 都不算
    old = [{"market": "TWSE", "as_of": (TODAY - timedelta(days=30)).isoformat(), "close": 50.0}]
    price_rows = [{"market": "TW", "code": "2330", "close": 1}, {"market": "TW", "code": "6488", "close": 1}, {"market": "TW", "code": "9999", "close": 1}, {"market": "TW", "code": "1111", "close": 1}, {"market": "US", "code": "ORCL"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=_hist({"2330": daily, "6488": gap, "9999": [], "1111": old})):
        rows = REAL["tw_rel_strength"](price_rows)
    by = {r["code"]: r for r in rows}; assert sorted(by) == ["2330", "6488"], sorted(by)
    a = by["2330"]; assert a["mkt"] == "TWSE" and a["as_of"] == base.isoformat() and a["close"] == 129.0 and a["ret_5d"] == round((129 / 124 - 1) * 100, 2) and a["ret_5d_from"] == wd[-6].isoformat() and a["ret_20d"] == round((129 / 109 - 1) * 100, 2) and a["ret_20d_from"] == wd[-21].isoformat(), a
    b = by["6488"]; assert b["mkt"] == "TPEX" and b["ret_5d"] is None and b["ret_20d"] is None, "歷史有缺漏（間隔太大）：不算、不拿更早的充數: " + str(b)
    lg = [x for x in fetch.LOG if x["source"] == "tw_rel_strength"][0]; assert lg["status"] == "success" and "算出 2 檔（其中近 5 日 1 檔；上市 1、上櫃 1）" in lg["detail"] and "9999（自建歷史沒有這檔）" in lg["detail"] and "1111（最新一筆" in lg["detail"], lg["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["tw_rel_strength"](price_rows) == [] and fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")):
        assert REAL["tw_rel_strength"]([]) == [] and fetch.LOG[-1]["status"] == "no_new_data"


def test_run_job_tw_close_persists_otc_index_and_rel_strength_in_their_own_tables_and_isolates_crashes():
    reset()
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    otc = [{"as_of": "2026-11-12", "close": 369.2, "source": "x"}]; rel = [{"code": "2330", "mkt": "TWSE", "as_of": "2026-11-12", "close": 1.0, "ret_5d": 1.0, "ret_5d_from": "2026-11-05", "ret_20d": None, "ret_20d_from": None}]
    def go(otc_kw, rel_kw):
        got, seen = [], []
        ps = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "fred_latest", return_value=None),
              mock.patch.object(fetch, "tw_otc_index", **otc_kw), mock.patch.object(fetch, "tw_rel_strength", **rel_kw), mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, t_: got.append(list(t_)))]
        for p_ in ps: p_.start()
        try:
            fetch.run_job("tw_close", dry_run=False)
        finally:
            for p_ in ps: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go({"return_value": otc}, {"return_value": rel}); assert ("tw_otc_index", otc, "as_of") in tables and ("tw_rel_strength", rel, "code") in tables and st["module:M40"] == "success" and st["module:M41"] == "success"
    reset(); tables, st = go({"side_effect": RuntimeError("boom")}, {"return_value": rel}); assert st["module:M40"] == "failure" and ("tw_rel_strength", rel, "code") in tables and ("tw_otc_index", otc, "as_of") not in tables, "櫃買指數壞掉不影響個股對大盤"
    reset(); tables, st = go({"return_value": otc}, {"side_effect": RuntimeError("boom")}); assert st["module:M41"] == "failure" and ("tw_otc_index", otc, "as_of") in tables and "module:M27" in st


def test_probe_dashboard_stops_at_the_first_working_language_segment_and_logs_the_full_apipath_class():
    home = "https://www.twse.com.tw/zh/"
    cls = 'class ApiPath{constructor(e="",t="en"){this.pre=e,this.lan=t,this.suf=""}name(e){return this.pre+"/"+(this.lan||"any")+e+this.suf}}'
    page = '<title>x</title><script src="/dashboard/js/credit-margin.js"></script><script src="/dashboard/js/main.js"></script>' + "x" * 1300
    long_use = 'a=1;' + ('$.get(TPEX_API+"/dashboardOtc/marginTrend?lang=zh-tw&days=6").done((e=>{const n="ok"===e.stat&&e.trend?e.trend:[];}));' * 12)
    pages = {home: (200, '<a href="/dashboard/zh/credit/margin.html">臺股儀表板</a>'), "https://www.twse.com.tw/dashboard/zh/credit/margin.html": (200, page),
             "https://www.twse.com.tw/dashboard/js/credit-margin.js": (200, 'a=apiAny.name("/marginTrading/BFIJ3U")+"?response=json";' + long_use), "https://www.twse.com.tw/dashboard/js/main.js": (200, cls)}
    env = _dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages)
    asked, logs, patches = _run_dash(env)
    ap = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_apipath"]
    assert len(ap) == 1 and cls in ap[0], "完整的類別定義單獨記一筆（不會被別的片段擠掉）: " + str(ap)
    sn = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_snips"][0]
    assert "class ApiPath" not in sn and len(sn) < 1800, "其他片段變短，不再把總長度用完"


def test_probe_dashboard_failures_are_logged_not_raised_and_never_write_tables():
    home = "https://www.twse.com.tw/zh/"
    # 全部連不上：每一步各記一筆，不丟例外
    asked, logs, patches = _run_dash(_dash_env({}, {}))
    by = {k.split("#")[0]: v for k, v in logs.items()}
    assert by["probe:dashboard_swagger_twse"]["status"] == "failure" and by["probe:dashboard_swagger_tpex"]["status"] == "failure" and by["probe:dashboard_home"]["status"] == "no_new_data" and by["probe:dashboard_pages"]["status"] == "no_new_data" and "需要人工把儀表板網址貼過來" in by["probe:dashboard_pages"]["detail"], "全部失敗: " + str({k: v["status"] for k, v in by.items()})
    assert len([u for u in asked if u in config.DASHBOARD_HOME_PAGES]) == 2, "兩個首頁網址都試"
    # 首頁沒有儀表板連結
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, {home: (200, "<html><a href='/zh/news'>新聞</a></html>")}))
    by = {k.split("#")[0]: v for k, v in logs.items()}; homes = [v["detail"] for v in logs.values() if v["source"] == "probe:dashboard_home"]
    assert any("找到 0 個疑似儀表板連結" in d for d in homes) and len(homes) == 2 and any("HTTP 404" in d for d in homes), "第一個首頁沒有連結、第二個首頁 404：兩個都記: " + str(homes)
    assert by["probe:dashboard_pages"]["status"] == "no_new_data" and "共 0 個端點" in by["probe:dashboard_swagger_twse"]["detail"] and "：無" in by["probe:dashboard_swagger_twse"]["detail"]
    # 儀表板頁 404、程式檔 500：記 no_new_data，其他照常
    pages = {home: (200, '<a href="/d1">儀表板</a><a href="/d2">儀表板</a>'), "https://www.twse.com.tw/d1": (404, ""), "https://www.twse.com.tw/d2": (200, '<title>x</title><script src="/a.js"></script><script src="/b.js"></script>'), "https://www.twse.com.tw/a.js": (500, ""), "https://www.twse.com.tw/b.js": (200, 'var k = "/rwd/x.json"')}
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pages))
    statuses = [(v["source"], v["status"]) for v in logs.values()]; assert ("probe:dashboard_page", "no_new_data") in statuses and ("probe:dashboard_page", "success") in statuses and statuses.count(("probe:dashboard_script", "no_new_data")) == 1 and statuses.count(("probe:dashboard_script", "success")) == 1, "壞掉的那一頁、那一支程式檔只記一筆，其他照常: " + str(statuses)
    # 數量上限：5 個連結只抓 3 頁；每頁最多 3 支程式檔
    links = "".join(f'<a href="/p{i}">儀表板{i}</a>' for i in range(5)); pg = {home: (200, links)}
    for i in range(5): pg[f"https://www.twse.com.tw/p{i}"] = (200, "<title>t</title>" + "".join(f'<script src="/s{j}.js"></script>' for j in range(6)))
    for j in range(6): pg[f"https://www.twse.com.tw/s{j}.js"] = (200, "x")
    asked, logs, patches = _run_dash(_dash_env({config.TWSE_SWAGGER: {"paths": {}}, config.TPEX_SWAGGER: {"paths": {}}}, pg))
    assert len([u for u in asked if u.startswith("https://www.twse.com.tw/p")]) == 3 and len([u for u in asked if u.endswith(".js")]) == 3 * config.DASHBOARD_MAX_SCRIPTS, "最多 3 頁、每頁最多 DASHBOARD_MAX_SCRIPTS 支程式檔: " + str(len(asked))


def test_probe_dashboard_runs_only_in_tw_probe_job_and_never_persists():
    reset()
    called = []
    with mock.patch.object(fetch, "probe_dashboard", side_effect=lambda: called.append(1)), mock.patch.object(fetch, "probe_twse"), mock.patch.object(fetch, "probe_extra"), mock.patch.object(fetch, "probe_taifex_daily"), mock.patch.object(fetch, "persist") as per, mock.patch.object(fetch, "persist_extra") as pe:
        fetch.run_job("tw_probe", dry_run=False)
    assert called == [1], "tw_probe 會跑儀表板探測"
    assert per.call_args[0][1] == [] and all(a is None for a in per.call_args[0][2:]) and pe.call_args[0][1] == [], "探測不會多寫任何市場資料列（寫入的資料表參數都是空的）: " + str((per.call_args[0][1:4], pe.call_args[0][1]))
    import inspect
    src = inspect.getsource(fetch.run_job); a = src.index('if job == "tw_probe":'); b = src.index('if job == "us_probe":')
    assert src.count("probe_dashboard()") == 1 and a < src.index("probe_dashboard()") < b, "只在 tw_probe 這個工作裡呼叫，其他工作不跑它"


def test_breadth_hist_compute_full_in_windows_then_incremental_and_failures():
    reset()
    calls = []
    first, rpc = [[]], [5]
    def req(method, path, body=None, **k):
        calls.append((method, path, body))
        if method == "GET": return first[0], ""
        return rpc[0], ""
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req):
        out = REAL["tw_breadth_hist_compute"]()
    posts = [c for c in calls if c[0] == "POST"]
    assert all(c[1] == "rpc/compute_tw_breadth_hist" for c in posts) and len(posts) == 14, "400 天分成每 30 天一段：%d 段" % len(posts)
    assert posts[0][2]["p_from"] == (TODAY - timedelta(days=config.HIST_WINDOW_DAYS)).isoformat() and posts[-1][2]["p_to"] == TODAY.isoformat(), "從回補範圍的起點到今天"
    for (a1, a2), (b1, b2) in zip([(c[2]["p_from"], c[2]["p_to"]) for c in posts], [(c[2]["p_from"], c[2]["p_to"]) for c in posts[1:]]):
        assert date.fromisoformat(b1) == date.fromisoformat(a2) + timedelta(days=1), "每一段接著上一段，不重疊也不漏"
        assert (date.fromisoformat(a2) - date.fromisoformat(a1)).days <= 29
    assert out == [{"computed": 5 * 14}] and "全部重算（分 14 段）" in [x for x in fetch.LOG if x["source"] == "tw_breadth_hist"][0]["detail"]
    reset(); calls.clear(); first[0] = [{"as_of": "2025-10-01"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req):
        REAL["tw_breadth_hist_compute"]()
    posts = [c for c in calls if c[0] == "POST"]
    assert len(posts) == 1 and posts[0][2] == {"p_from": (TODAY - timedelta(days=20)).isoformat(), "p_to": None}, "之後只重算最近 20 天"
    reset(); n = [0]
    def flaky(method, path, body=None, **k):                                                                 # 第 3 段失敗：前 2 段的結果要保留、並說明下次繼續
        if method == "GET": return [], ""
        n[0] += 1
        return (None, "HTTP 500 statement timeout") if n[0] == 3 else (4, "")
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=flaky):
        out = REAL["tw_breadth_hist_compute"]()
    f = [x for x in fetch.LOG if x["source"] == "tw_breadth_hist"][0]
    assert out == [{"computed": 8}] and n[0] == 3 and f["status"] == "failure" and "已算 8 個，下次繼續" in f["detail"] and "new_tables_v38.sql" in f["detail"], f["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=lambda m, p, b=None, **k: (None, "HTTP 404")):
        assert REAL["tw_breadth_hist_compute"]() == []
    assert "new_tables_v38.sql" in [x for x in fetch.LOG if x["source"] == "tw_breadth_hist"][0]["detail"]
    reset(); rpc[0] = 0; first[0] = [{"as_of": "x"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req):
        assert REAL["tw_breadth_hist_compute"]() == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "tw_breadth_hist"] == ["no_new_data"]


def _pc_env(days, have=(), http=None):
    """Put/Call 回補的模擬環境：FRED 標普 500 的日期清單、資料庫已有的日期、CBOE 對每個日期的回應（預設都是 200 並給正常內容）。"""
    urls = []
    def req(method, path, body=None, **k):
        if path.startswith("macro_series"): return [{"as_of": d} for d in days], ""
        if path.startswith("us_putcall"): return [{"as_of": d} for d in have], ""
        raise AssertionError(path)
    def raw(url, timeout=None, headers=None):
        urls.append(url)
        d = url.split("/daily/")[1].split("_daily_options")[0]
        res = http(d) if http else (200, "application/json", cboe_body(), "")
        return res
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req),
            mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep")], urls


def test_pc_backfill_fetches_missing_days_with_their_own_dates():
    reset()
    days = [D(1), D(2), D(3), D(4)]
    patches, urls = _pc_env(days, have=[D(2)])
    rows = _with(patches, lambda: REAL["cboe_putcall_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1), D(3), D(4)] and all(r["total_pc"] == 0.93 and r["source"] == "CBOE 每日市場統計" for r in rows), "已經有的不重抓，日期用請求的交易日"
    assert len(urls) == 3 and urls[0] == config.CBOE_DAILY_URL.format(ymd=D(1))
    d = [x for x in fetch.LOG if x["source"] == "pc_backfill"][0]
    assert d["status"] == "success" and "本次補 3 天" in d["detail"] and "還有 0 天下次繼續" in d["detail"]


def test_pc_backfill_skips_bad_days_stops_when_cboe_has_no_old_data_and_on_connection_failure():
    reset()
    days = [D(i) for i in range(1, 20)]
    def http(d):                                                                                     # 最近 2 天正常，第 3 天格式壞，之後全部 404（CBOE 不保留更早的）
        if d in (D(1), D(2)): return 200, "application/json", cboe_body(), ""
        if d == D(3): return 200, "application/json", b"not json", ""
        return 404, "text/html", b"nope", "HTTP 404"
    patches, urls = _pc_env(days, http=http)
    rows = _with(patches, lambda: REAL["cboe_putcall_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1), D(2)]
    assert len(urls) == 3 + 10, "已經成功過後，連續 10 天沒有資料才停（共 3 + 10 次請求），不是把 19 天都打一遍: %d" % len(urls)
    d = [x for x in fetch.LOG if x["source"] == "pc_backfill"][0]["detail"]
    assert "格式不符 1 天" in d and "連續 10 天 CBOE 都沒有資料" in d and "已停止本次回補" in d and "下次繼續" not in d, d
    reset()
    days = [D(i) for i in range(1, 20)]
    patches, urls = _pc_env(days, http=lambda d: (404, "text/html", b"", "HTTP 404"))                # 一天都沒成功過：連續 5 天沒有就停
    assert _with(patches, lambda: REAL["cboe_putcall_backfill"]()) == [] and len(urls) == 5, "不能把 19 天都打一遍"
    assert [x["status"] for x in fetch.LOG if x["source"] == "pc_backfill"] == ["no_new_data"]
    reset()
    patches, urls = _pc_env([D(i) for i in range(1, 6)], http=lambda d: (None, "", b"", "timeout") if d == D(2) else (200, "application/json", cboe_body(), ""))
    rows = _with(patches, lambda: REAL["cboe_putcall_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1)] and len(urls) == 2, "連線失敗整批停止"
    f = [x for x in fetch.LOG if x["source"] == "pc_backfill"]
    assert f[0]["status"] == "failure" and "不繼續打請求" in f[0]["detail"]


def test_pc_backfill_batch_cap_done_and_unconfigured():
    reset()
    patches, urls = _pc_env([D(i) for i in range(1, 7)])
    rows = _with(patches, lambda: REAL["cboe_putcall_backfill"](max_days=2))
    assert len(rows) == 2 and len(urls) == 2 and "還有 4 天下次繼續" in [x for x in fetch.LOG if x["source"] == "pc_backfill"][0]["detail"]
    reset()
    patches, urls = _pc_env([D(1), D(2)], have=[D(1), D(2)])
    assert _with(patches, lambda: REAL["cboe_putcall_backfill"]()) == [] and urls == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "pc_backfill"] == ["no_new_data"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "fetch_raw") as fr:
        assert REAL["cboe_putcall_backfill"]() == []
        fr.assert_not_called()
    assert [x["status"] for x in fetch.LOG if x["source"] == "pc_backfill"] == ["skipped"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")):          # 還沒有 FRED 歷史
        assert REAL["cboe_putcall_backfill"]() == []
    assert "請先跑 tw_backfill" in [x for x in fetch.LOG if x["source"] == "pc_backfill"][0]["detail"]


def test_run_job_tw_backfill_runs_history_modules_and_persists_institutional_rows():
    reset()
    got = []
    row = {"as_of": D(3), "foreign_buy": 1.0, "foreign_sell": 2.0, "foreign_net": -1.0, "source": "TWSE BFI82U"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [
        mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"), mock.patch.object(fetch, "tw_backfill", return_value={}),
        mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"),
        mock.patch.object(fetch, "tw_institutional_backfill", return_value=[row]), mock.patch.object(fetch, "tw_breadth_hist_compute", return_value=[{"computed": 3}]),
        mock.patch.object(fetch, "cboe_putcall_backfill", return_value=[{"as_of": D(3), "total_pc": 0.9, "source": "CBOE 每日市場統計"}]),
        mock.patch.object(fetch, "persist_extra", side_effect=lambda dry, tables: got.append((dry, [(n, rows_, c) for n, rows_, c in tables])))]
    for p_ in patches: p_.start()
    try:
        fetch.run_job("tw_backfill", dry_run=False)
        st = {x["source"]: x["status"] for x in fetch.LOG}
        assert st["module:M12"] == "success" and st["module:M13"] == "success"
        tables = dict((n, (r, c)) for n, r, c in got[0][1])
        assert tables["tw_institutional"] == ([row], "as_of,source"), "外資歷史要寫進 tw_institutional，用 (as_of, source) 去重"
        assert tables["us_putcall"][1] == "as_of" and tables["us_putcall"][0][0]["total_pc"] == 0.9 and st["module:M14"] == "success", "Put/Call 歷史要寫進 us_putcall，用 as_of 去重"
        got.clear(); fetch.LOG.clear()
        fetch.run_job("tw_backfill", dry_run=True)
        assert "module:M12" not in {x["source"] for x in fetch.LOG}, "dry-run 不碰這兩個模組"
    finally:
        for p_ in patches: p_.stop()


def test_probe_round5_reports_industry_field_and_value_counts_without_writing():
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/opendata/t187ap03_L": {"get": {"summary": "上市公司基本資料"}}, "/opendata/x": {"get": {"summary": "其他"}}}}
    sw2 = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap03_O": {"get": {"summary": "上櫃公司基本資料"}}}}
    rows = [{"公司代號": "2330", "公司名稱": "台積電", "產業別": "24"}, {"公司代號": "2317", "公司名稱": "鴻海", "產業別": "25"}, {"公司代號": "2454", "公司名稱": "聯發科", "產業別": "24"}]
    otc = [{"SecuritiesCompanyCode": "8358", "CompanyName": "金居", "SecuritiesIndustryCode": "28"}]
    def fj(url, **k):
        if url.endswith("swagger.json") and "twse" in url: return sw, ""
        if "tpex" in url and "swagger" in url: return sw2, ""
        if url.endswith("t187ap03_L"): return rows, ""
        if url.endswith("t187ap03_O"): return otc, ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round5()
    by = {x["source"]: x for x in fetch.LOG}
    assert "/opendata/t187ap03_L" in by["probe:industry_paths:twse"]["detail"] and "/opendata/x" not in by["probe:industry_paths:twse"]["detail"]
    d = by["probe:industry:twse"]["detail"]
    assert by["probe:industry:twse"]["status"] == "success" and "共 3 列" in d and "'24': 2" in d and "'25': 1" in d and "產業欄 '產業別'" in d and "台積電" in d
    d2 = by["probe:industry:tpex"]["detail"]
    assert by["probe:industry:tpex"]["status"] == "success" and "SecuritiesIndustryCode" in d2 and "'28': 1" in d2
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_round5_failures_are_reported():
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        fetch.probe_round5()
    assert {x["source"]: x["status"] for x in fetch.LOG} == {"probe:industry:twse": "failure", "probe:industry:tpex": "failure"}
    reset()
    sw = {"servers": [{"url": "https://openapi.twse.com.tw/v1"}], "paths": {"/a": {"get": {"summary": "別的"}}}}
    with mock.patch.object(fetch, "fetch_json", return_value=(sw, "")):
        fetch.probe_round5()
    assert all(x["status"] == "no_new_data" and "無" in x["detail"] for x in fetch.LOG)


EXR_FIELDS = ["除權除息日期", "股票代號", "名稱", "除權息", "無償配股率", "現金增資配股率", "現金增資認購價", "現金股利", "詳細資料", "參考價<br>試算", "最近一次申報資料 季別/日期", "最近一次申報每股 (單位)淨值", "最近一次申報每股 (單位)盈餘"]


def exr_row(ev, code, name, kind, stock="0.00000000", cash="5.00000000"):
    roc = f"{ev.year - 1911}年{ev.month:02d}月{ev.day:02d}日"
    return [roc, code, name, kind, stock, "0.00000000", "0.00000000", cash, f"{code},{ev:%Y%m%d}", "", "", "0.00", ""]


def _exr_fetch(twse=None, tpex=None):
    """除權息預告的模擬連線：證交所網址回傳 twse，櫃買網址回傳 tpex（各自可以是 (資料, 錯誤)）。"""
    def fj(url, **k):
        if "TWT48U" in url: return twse if isinstance(twse, tuple) else (twse, "")
        if "tpex_exright_prepost" in url: return tpex if isinstance(tpex, tuple) else (tpex, "")
        raise AssertionError(url)
    return fj


def _tpex_exr_row(ev, code, name, kind="除息", stock="0.00000000", cash="0.67000000"):
    return {"ExRrightsExDividendDate": f"{ev.year - 1911}{ev:%m%d}", "SecuritiesCompanyCode": code, "CompanyName": name, "ExRrightsExDividend": kind, "StockDividendRatio": stock,
            "SubscriptionRatioToNewSharesIssued": "0.00000000", "SubscriptionPricePerShare": "0.00", "CashDividend": cash, "AllocatedForPublicUnderwriting": "0"}


def test_tw_exright_preview_parses_real_format_and_rejects_bad_rows():
    reset()
    ev = TODAY + timedelta(days=9)
    data = {"stat": "OK", "date": ymd(), "fields": EXR_FIELDS, "data": [
        exr_row(ev, "00400A", "主動國泰動能高息", "息", cash="<p style= text-align:center;>待公告實際收益分配金額</p>"),     # ETF，現金股利還沒公告 → 存空白
        exr_row(ev, "2330", "台積電", "息", cash="5.00000000"),
        exr_row(ev + timedelta(days=2), "2454", "聯發科", "權息", stock="0.50000000", cash="10"),
        exr_row(ev, "ABCD", "壞代號", "股"),                                                                            # 種類不對
        exr_row(ev, "12", "太短的代號", "息"),
        exr_row(TODAY + timedelta(days=400), "2317", "太遠", "息"),                                                      # 日期不在範圍
        exr_row(TODAY - timedelta(days=30), "1101", "太舊", "息"),
        exr_row(ev, "2412", "配股率亂填", "權", stock="99"),
        ["亂碼", "2002", "x", "息"] + [""] * 9,                                                                         # 日期格式不對
        ["115年11月06日"]]                                                                                              # 列太短
    }
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch(data, [])):
        rows = REAL["tw_exright_preview"]()
    by = {r["code"]: r for r in rows}
    assert sorted(by) == ["00400A", "2330", "2454"], sorted(by)
    assert by["00400A"]["cash_dividend"] is None and by["00400A"]["stock_rate"] == 0.0 and by["00400A"]["kind"] == "息", "還沒公告的現金股利存空白，不猜"
    assert by["2330"]["event_date"] == ev.isoformat() and by["2330"]["cash_dividend"] == 5.0 and by["2330"]["market"] == "TWSE" and by["2330"]["source"] == "證交所 除權除息預告表"
    assert by["2454"]["kind"] == "權息" and by["2454"]["stock_rate"] == 0.5 and by["2454"]["cash_dividend"] == 10.0
    lg = [x for x in fetch.LOG if x["source"] == "tw_exright:twse"][0]
    assert lg["status"] == "success" and "3 筆除權息預告（略過 7 列）" in lg["detail"] and lg["as_of"] == ev.isoformat()


def test_tw_exright_preview_reads_tpex_real_format_independently_of_twse():
    reset()
    ev = TODAY + timedelta(days=6)
    rows_t = [_tpex_exr_row(ev, "8358", "金居", "除息", cash="20.00000000"), _tpex_exr_row(ev, "6488", "環球晶", "除權息", stock="0.30000000", cash="3"),
              _tpex_exr_row(TODAY - timedelta(days=10), "00789B", "復華公司債A3"),                                          # 已經過了（探測時看到的第一列就是這種）
              _tpex_exr_row(ev, "3105", "穩懋", "亂寫"), _tpex_exr_row(ev, "xx", "壞代號"),
              {**_tpex_exr_row(ev, "3293", "鈊象"), "ExRrightsExDividendDate": ""}]
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch((None, "HTTP 403"), rows_t)):                      # 證交所壞掉，櫃買照常
        rows = REAL["tw_exright_preview"]()
    by = {r["code"]: r for r in rows}
    assert sorted(by) == ["6488", "8358"] and all(r["market"] == "TPEX" and r["source"] == "櫃買中心 除權除息預告表" for r in rows)
    assert by["8358"]["kind"] == "息" and by["8358"]["cash_dividend"] == 20.0 and by["6488"]["kind"] == "權息" and by["6488"]["stock_rate"] == 0.3 and by["8358"]["event_date"] == ev.isoformat()
    st = {x["source"]: x["status"] for x in fetch.LOG}
    assert st["tw_exright:twse"] == "failure" and st["tw_exright:tpex"] == "success", "兩個市場各自獨立"
    reset()
    bad = [{"foo": 1}]
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch((None, "x"), bad)):
        assert REAL["tw_exright_preview"]() == []
    assert {x["source"]: x["status"] for x in fetch.LOG}["tw_exright:tpex"] == "failure" and "欄位與預期不符" in [x for x in fetch.LOG if x["source"] == "tw_exright:tpex"][0]["detail"]


def test_tw_exright_preview_failures_never_write_guesses():
    reset()
    ev = TODAY + timedelta(days=9)
    def last(src): return [x for x in fetch.LOG if x["source"] == src][-1]
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch((None, "HTTP 403"), (None, "HTTP 403"))):
        assert REAL["tw_exright_preview"]() == []
    assert last("tw_exright:twse")["status"] == "failure" and last("tw_exright:tpex")["status"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch({"stat": "很抱歉，沒有符合條件的資料!"}, [])):
        assert REAL["tw_exright_preview"]() == []
    assert last("tw_exright:twse")["status"] == "no_new_data"
    reset()
    bad_fields = {"stat": "OK", "fields": ["日期", "代號"], "data": [["115年11月06日", "2330"]]}
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch(bad_fields, [])):
        assert REAL["tw_exright_preview"]() == []
    assert last("tw_exright:twse")["status"] == "failure" and "欄位與預期不符" in last("tw_exright:twse")["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=_exr_fetch({"stat": "OK", "fields": EXR_FIELDS, "data": []}, [])):
        assert REAL["tw_exright_preview"]() == []
    assert last("tw_exright:twse")["status"] == "no_new_data"


HIST_FIELDS = ["資料日期", "股票代號", "股票名稱", "除權息前收盤價", "除權息參考價", "權值+息值", "權/息", "漲停價格", "跌停價格", "開盤競價基準", "減除股利參考價", "詳細資料", "最近一次申報資料 季別/日期", "最近一次申報每股 (單位)淨值", "最近一次申報每股 (單位)盈餘"]


def hist_row(ev, code, name, prev, ref, kind="息"):
    return [f"{ev.year - 1911}年{ev.month:02d}月{ev.day:02d}日", code, name, prev, ref, "0.138000", kind, "10.98", "9.00", ref, ref, f"{code},{ev:%Y%m%d}", "", "", ""]


def test_exright_hist_parser_checks_reference_price_and_omits_dividend_columns():
    reset()
    ev = TODAY - timedelta(days=20)
    data = {"stat": "OK", "fields": HIST_FIELDS, "data": [
        hist_row(ev, "00406A", "主動中信台灣收益", "10.13", "9.99"),                      # 正常
        hist_row(ev, "1101", "台泥", "50.00", "45.46", "權"),                              # 配股：參考價比前收盤低約 9.1%
        hist_row(ev, "2330", "台積電", "1000", "1100"),                                    # 參考價高過前收盤：不可能
        hist_row(ev, "2317", "鴻海", "100", "20"),                                         # 低於 30%：不合理
        hist_row(ev, "2454", "聯發科", "", "9"),                                            # 沒有前收盤
        hist_row(ev, "2412", "中華電", "100", "99", "股"),                                  # 種類不對
        hist_row(TODAY - timedelta(days=500), "1216", "太舊", "100", "99"),
        hist_row(TODAY + timedelta(days=30), "1303", "未來", "100", "99")]}
    rows, bad, why = fetch.parse_exright_hist_rows(data)
    assert why is None and sorted(r["code"] for r in rows) == ["00406A", "1101"] and bad == 6
    r = {x["code"]: x for x in rows}["1101"]
    assert r["prev_close"] == 50.0 and r["ref_price"] == 45.46 and r["kind"] == "權" and r["market"] == "TWSE" and r["event_date"] == ev.isoformat()
    assert "cash_dividend" not in r and "stock_rate" not in r, "不帶股利欄位，寫入時才不會把預告表已有的股利清掉"
    assert fetch.parse_exright_hist_rows({"fields": ["資料日期", "股票代號"], "data": []})[2].startswith("欄位與預期不符")


def test_exright_history_first_run_backfills_windows_then_daily_refreshes_two():
    reset()
    ev = TODAY - timedelta(days=5)
    urls = []
    def fj(url, **k):
        urls.append(url)
        return {"stat": "OK", "fields": HIST_FIELDS, "data": [hist_row(ev, "1101", "台泥", "50", "45.46", "權")] if len(urls) == 1 else []}, ""
    def req(method, path, body=None, **k):
        assert method == "GET" and path.startswith("tw_exright_events?select=event_date&ref_price=not.is.null")
        return have[0], ""
    have = [[]]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep") as sl:
        rows = REAL["tw_exright_history"]()
    assert len(urls) == 14 and "startDate=" in urls[0] and "endDate=" in urls[0] and urls[-1].endswith(f"endDate={TODAY:%Y%m%d}&response=json"), "400 天分成每 30 天一段：%d 段" % len(urls)
    assert sl.call_count == 13, "段與段之間要等"
    assert [r["code"] for r in rows] == ["1101"]
    lg = [x for x in fetch.LOG if x["source"] == "tw_exright_hist"][0]
    assert lg["status"] == "success" and "第一次回補：14 段，取得 1 筆" in lg["detail"] and "13 段沒有資料" in lg["detail"]
    reset(); urls.clear(); have[0] = [{"event_date": "2026-10-05"}]
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep"):
        REAL["tw_exright_history"]()
    assert len(urls) == 3 and "每日更新最近 60 天" in [x for x in fetch.LOG if x["source"] == "tw_exright_hist"][0]["detail"], "之後每天只重抓最近 60 天"


def test_exright_history_stops_on_connection_failure_and_reports_problems():
    reset()
    calls = []
    def fj(url, **k):
        calls.append(url)
        return (None, "HTTP 503") if len(calls) == 3 else ({"stat": "OK", "fields": HIST_FIELDS, "data": [hist_row(TODAY - timedelta(days=5), "1101", "台泥", "50", "45")]}, "")
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep"):
        rows = REAL["tw_exright_history"]()
    assert len(calls) == 3 and len(rows) == 2, "連線失敗就整批停止，已經拿到的保留"
    lgs = [x for x in fetch.LOG if x["source"] == "tw_exright_hist"]
    assert lgs[0]["status"] == "failure" and "不繼續打請求" in lgs[0]["detail"] and lgs[-1]["status"] == "success" and "中途連線失敗" in lgs[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=([], "")), mock.patch.object(fetch.time, "sleep"), \
            mock.patch.object(fetch, "fetch_json", return_value=({"stat": "OK", "fields": ["a", "b"], "data": [["x", "y"]]}, "")):
        assert REAL["tw_exright_history"]() == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "tw_exright_hist"] == ["failure"] and "欄位與預期不符" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 400 column ref_price does not exist")):
        assert REAL["tw_exright_history"]() == []
    assert "new_tables_v43.sql" in fetch.LOG[-1]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["tw_exright_history"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"


def test_tw_industry_collect_merges_listed_first_and_validates_codes():
    reset()
    L_ = [{"出表日期": "1151101", "公司代號": "2330", "公司名稱": "台灣積體電路製造股份有限公司", "公司簡稱": "台積電", "產業別": "24"}, {"公司代號": "1101", "公司簡稱": "台泥", "產業別": "01"},
          {"公司代號": "9999", "公司簡稱": "壞產業", "產業別": "半導體"}, {"公司代號": "00878", "公司簡稱": "ETF", "產業別": "24"}, {"公司代號": "ab12", "公司簡稱": "壞代號", "產業別": "24"}]
    P_ = [{"公司代號": "2330", "公司簡稱": "台積電(重複)", "產業別": "99"}, {"公司代號": "8358", "公司簡稱": "金居", "產業別": "28"}, {"公司代號": "6488", "公司簡稱": "環球晶", "產業別": "24"}]
    def fj(url, **k):
        if url.endswith("t187ap03_L"): return L_, ""
        if url.endswith("t187ap03_P"): return P_, ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        rows = REAL["tw_industry_collect"]()
    by = {r["code"]: r for r in rows}
    assert sorted(by) == ["1101", "2330", "6488", "8358"], sorted(by)
    assert by["2330"]["industry"] == "24" and by["2330"]["name"] == "台積電", "上市的優先，公開發行的只補沒有的"
    assert by["8358"]["industry"] == "28" and by["8358"]["source"].endswith("（P）") and by["1101"]["source"].endswith("（L）")
    sts = {x["source"]: x for x in fetch.LOG}
    assert "新收 2 家（略過產業別不是 2 位數字的 1 列）" in sts["tw_industry:L"]["detail"] and "新收 2 家" in sts["tw_industry:P"]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (L_, "") if u.endswith("_L") else (None, "HTTP 404")):     # 公開發行那份抓不到：上市照收
        rows = REAL["tw_industry_collect"]()
    assert sorted(r["code"] for r in rows) == ["1101", "2330"] and {x["source"]: x["status"] for x in fetch.LOG}["tw_industry:P"] == "failure"
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=([{"foo": "1"}], "")):                                                       # 欄位認不得：不猜
        assert REAL["tw_industry_collect"]() == []
    assert all(x["status"] == "failure" and "找不到公司代號或產業別欄" in x["detail"] for x in fetch.LOG if x["source"].startswith("tw_industry:"))


def test_probe_round7_logs_p_coverage_and_pe_sources_without_writing():
    reset()
    P_ = [{"公司代號": c, "公司簡稱": "x", "產業別": "24"} for c in ("2330", "8358", "6488", "3105", "00878", "1101")]
    def fj(url, **k):
        if url.endswith("t187ap03_P"): return P_, ""
        if url.endswith("BWIBBU_ALL"): return [{"Date": "1151102", "Code": "2330", "Name": "台積電", "PEratio": "25.1", "DividendYield": "1.5", "PBratio": "7.2"}], ""
        if url.endswith("tpex_mainboard_peratio_analysis"): return [{"Date": "1151102", "SecuritiesCompanyCode": "8358", "PriceEarningRatio": "12"}], ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round7()
    by = {x["source"]: x for x in fetch.LOG}
    assert "共 6 列" in by["probe:industry_p"]["detail"] and "常見的上櫃）3 家" in by["probe:industry_p"]["detail"] and "4 位數代號 5 家" in by["probe:industry_p"]["detail"]
    assert "PEratio" in by["probe:twse_pe"]["detail"] and "PriceEarningRatio" in by["probe:tpex_pe"]["detail"]
    up.assert_not_called()
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        fetch.probe_round7()
    assert {x["source"]: x["status"] for x in fetch.LOG} == {"probe:industry_p": "failure", "probe:twse_pe": "failure", "probe:tpex_pe": "failure"}


def test_run_job_wires_exright_history_and_industry_modules_with_isolation():
    reset()
    ex_hist = [{"market": "TWSE", "code": "1101", "event_date": D(5), "name": "台泥", "kind": "權", "prev_close": 50.0, "ref_price": 45.46, "source": "證交所 除權除息計算結果表"}]
    ind = [{"code": "2330", "industry": "24", "name": "台積電", "source": "x"}]
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(job, dry, hist=None, indfn=None):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_backfill", return_value={}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"),
                  mock.patch.object(fetch, "tw_exright_history", **(hist or {"return_value": ex_hist})), mock.patch.object(fetch, "tw_industry_collect", **(indfn or {"return_value": ind})),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job(job, dry_run=dry)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go("tw_close", False)
    assert ("tw_exright_events", ex_hist, "market,code,event_date") in tables and st["module:M16"] == "success", "每天的工作要寫入歷史除權息"
    assert not any(n == "tw_industry" for n, _, _ in tables), "產業別不在每天的工作裡"
    reset(); tables, st = go("tw_close", True)
    assert "module:M16" not in st, "dry-run 不碰歷史回補"
    reset(); tables, st = go("tw_backfill", False)
    assert ("tw_industry", ind, "code") in tables and st["module:M17"] == "success", "產業別在夜間的回補工作"
    reset(); tables, st = go("tw_close", False, hist={"side_effect": RuntimeError("boom")})
    assert st["module:M16"] == "failure" and st["module:M08"] in ("success", "no_new_data"), "歷史除權息壞了不影響其他模組"
    reset(); tables, st = go("tw_backfill", False, indfn={"side_effect": RuntimeError("boom")})
    assert st["module:M17"] == "failure" and "module:M12" in st and "module:M14" in st, "產業別壞了不影響夜間的其他模組"


def test_probe_round6_logs_listed_industry_otc_paths_prepost_and_history_without_writing():
    reset()
    listed = [{"出表日期": "1151101", "公司代號": "2330", "公司名稱": "台積電", "產業別": "24"}, {"出表日期": "1151101", "公司代號": "2317", "公司名稱": "鴻海", "產業別": "25"}, {"出表日期": "1151101", "公司代號": "2454", "公司名稱": "聯發科", "產業別": "24"}]
    sw = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/mopsfin_t187ap03_O": {"get": {"summary": "上櫃公司基本資料"}}, "/x": {"get": {"summary": "其他"}}, "/tpex_exright_prepost": {"get": {"summary": "上櫃股票除權除息預告表"}}}}
    def fj(url, **k):
        if url.endswith("/opendata/t187ap03_L"): return listed, ""
        if url == config.TPEX_SWAGGER: return sw, ""
        if url.endswith("/tpex_exright_prepost"): return [{"Date": "20261105", "SecuritiesCompanyCode": "8358", "CompanyName": "金居"}], ""
        if "TWT49U" in url: return ({"stat": "OK", "fields": ["資料日期", "股票代號"], "data": [["115年10月20日", "2330"]]} if "strDate" in url else {"stat": "很抱歉，沒有符合條件的資料!"}), ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round6()
    by = {x["source"]: x for x in fetch.LOG}
    assert "'24': 2" in by["probe:industry_l"]["detail"] and "產業欄 '產業別'" in by["probe:industry_l"]["detail"] and by["probe:industry_l"]["status"] == "success"
    assert "/mopsfin_t187ap03_O | 上櫃公司基本資料" in by["probe:tpex_paths:1"]["detail"] and "/x |" not in by["probe:tpex_paths:1"]["detail"]
    assert "8358" in by["probe:exright_tpex_pre"]["detail"] and "SecuritiesCompanyCode" in by["probe:exright_tpex_pre"]["detail"]
    h = by["probe:exright_twse_hist"]
    assert h["status"] == "success" and "參數 strDate/endDate" in h["detail"], "第一種參數名稱沒資料，要換第二種再試: " + h["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        fetch.probe_round6()
    st = {x["source"]: x["status"] for x in fetch.LOG}
    assert st["probe:industry_l"] == "failure" and st["probe:exright_twse_hist"] == "no_new_data" and st["probe:exright_tpex_pre"] == "failure" and st["probe:tpex_paths:1"] == "no_new_data"


def test_probe_round8_reports_sp500_alternative_sources_without_writing():
    reset()
    csv = b"Date,Open,High,Low,Close,Volume\n2026-10-28,7000,7100,6990,7050.5,0\n2026-10-29,7050,7120,7040,7100.1,0\n2026-10-30,7100,7200,7090,7150.2,0\n2026-11-02,7150,7210,7140,7200.9,0\n2026-11-03,7200,7250,7190,7210.0,0\n"
    yj = json.dumps({"chart": {"result": [{"meta": {"marketState": "CLOSED", "regularMarketTime": 1793000000}, "timestamp": [1792000000 + i * 86400 for i in range(6)],
                                          "indicators": {"quote": [{"close": [7000.0, 7010.5, None, 7030.25, 7040.0, 7050.75]}]}}]}}).encode()
    def raw(url, timeout=None, headers=None):
        if "stooq" in url: return 200, "text/csv", csv, ""
        if "yahoo" in url: return 200, "application/json", yj, ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round8()
    by = {x["source"]: x for x in fetch.LOG}
    s_, y_ = by["probe:sp500_alt:stooq"], by["probe:sp500_alt:yahoo"]
    assert s_["status"] == "success" and "共 5 個交易日" in s_["detail"] and "('2026-11-03', 7210.0)" in s_["detail"] and "表頭 ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']" in s_["detail"], s_["detail"]
    assert y_["status"] == "success" and "共 5 個交易日" in y_["detail"] and "7050.75" in y_["detail"] and "marketState=CLOSED" in y_["detail"], y_["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)
    reset()
    def bad(url, timeout=None, headers=None):
        if "stooq" in url: return 200, "text/html", b"<html>Exceeded the daily hits limit</html>", ""
        return None, "", b"", "timeout"
    with mock.patch.object(fetch, "fetch_raw", side_effect=bad):
        fetch.probe_round8()
    st = {x["source"]: x for x in fetch.LOG}
    assert st["probe:sp500_alt:stooq"]["status"] == "no_new_data" and "共 0 個交易日" in st["probe:sp500_alt:stooq"]["detail"] and "Exceeded" in st["probe:sp500_alt:stooq"]["detail"], "被擋或限流要記失敗並留下開頭內容"
    assert st["probe:sp500_alt:yahoo"]["status"] == "no_new_data" and "連線失敗" in st["probe:sp500_alt:yahoo"]["detail"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", b"<html>login</html>", "")):
        fetch.probe_round8()
    assert {x["source"]: x["status"] for x in fetch.LOG}["probe:sp500_alt:yahoo"] == "no_new_data"


def test_probe_round9_reports_oi_options_cboe_chain_margin_and_paths_without_writing():
    reset()
    taifex_sw = {"paths": {"/DailyMarketReportFut": {"get": {"summary": "期貨每日交易行情"}}, "/DailyMarketReportOpt": {"get": {"summary": "選擇權每日交易行情"}},
                           "/PutCallRatio": {"get": {"summary": "臺指選擇權Put/Call比"}}, "/OpenInterestFut": {"get": {"summary": "各期貨商品未沖銷部位"}}, "/Other": {"get": {"summary": "其他"}}}}
    fut = [{"Date": "20261103", "Contract": "TX", "OpenInterest": "98765", "Volume": "1234", "SettlementPrice": "45000"}]
    chain = json.dumps({"timestamp": "2026-11-03 17:00:00", "data": {"current_price": 7500.1, "options": [
        {"option": "SPXW261106C07500000", "iv": 0.152, "open_interest": 1200, "volume": 30, "bid": 1, "ask": 2},
        {"option": "SPXW261106P07400000", "iv": 0.181, "open_interest": 0, "volume": 5, "bid": 1, "ask": 2},
        {"option": "SPXW261106P07450000", "iv": 0.17, "open_interest": 800, "volume": 5, "bid": 1, "ask": 2}] + [{"option": f"SPXW261107C0{7000 + i}0000", "iv": 0.2, "open_interest": 10, "volume": 1, "bid": 1, "ask": 2} for i in range(20)]}}).encode()
    margin = {"stat": "OK", "tables": [{"title": "信用交易統計", "fields": ["項目", "買進"], "data": [["融資(交易單位)", "1"]]}, {"title": "融資融券彙總", "fields": ["代號", "名稱", "融資買進", "融資賣出", "現金償還", "前日餘額", "今日餘額"], "data": [[f"{1100 + i}", "x", "1", "2", "3", "4", "5"] for i in range(150)]}]}
    twse_sw = {"paths": {"/a": {"get": {"summary": "信用交易統計"}}, "/b": {"get": {"summary": "其他"}}}}
    def fj(url, **k):
        if url == config.TAIFEX_SWAGGER: return taifex_sw, ""
        if url == config.TAIFEX_DAILY_URL: return fut, ""
        if "MI_MARGN" in url: return margin, ""
        if url.endswith("tpex_mainboard_margin_balance"): return [{"Date": "1151103", "SecuritiesCompanyCode": "8358", "MarginPurchase": "1"}], ""
        if url == config.TWSE_OPENAPI_SWAGGER: return twse_sw, ""
        if url == config.TPEX_SWAGGER: return {"paths": {}}, ""
        raise AssertionError(url)
    def raw(url, timeout=None, headers=None):
        assert "cboe.com" in url
        return 200, "application/json", chain, ""
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round9()
    by = {x["source"]: x for x in fetch.LOG}
    assert "/DailyMarketReportOpt | 選擇權每日交易行情" in by["probe:taifex_paths:1"]["detail"] and "/PutCallRatio" in by["probe:taifex_paths:1"]["detail"] and "/OpenInterestFut" in by["probe:taifex_paths:1"]["detail"] and "/Other" not in by["probe:taifex_paths:1"]["detail"]
    assert by["probe:taifex_fut_oi"]["status"] == "success" and "['OpenInterest']" in by["probe:taifex_fut_oi"]["detail"]
    c = by["probe:cboe_chain:SPX"]
    assert c["status"] == "success" and "共 23 檔合約" in c["detail"] and "'履約價': 7500.0" in c["detail"] and "'種類': 'C'" in c["detail"] and "'履約價': 7450.0" in c["detail"], c["detail"]
    assert "'履約價': 7400.0" not in c["detail"], "OI 是 0 的合約不當範例"
    assert by["probe:twse_margin_stock"]["status"] == "success" and "最大一張 150 列" in by["probe:twse_margin_stock"]["detail"] and "融資買進" in by["probe:twse_margin_stock"]["detail"]
    assert by["probe:tpex_margin"]["status"] == "success" and "MarginPurchase" in by["probe:tpex_margin"]["detail"]
    assert by["probe:margin_paths:twse"]["status"] == "success" and "/a | 信用交易統計" in by["probe:margin_paths:twse"]["detail"] and by["probe:margin_paths:tpex"]["status"] == "no_new_data"
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG) and not [x for x in fetch.LOG if x["status"] == "failure"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")), mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"<html>blocked</html>", "")):
        fetch.probe_round9()
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "候選來源被擋是正常的探測結果，不能讓工作變紅"
    assert {x["source"]: x["status"] for x in fetch.LOG}["probe:cboe_chain:SPY"] == "no_new_data"


def test_fetch_json_waits_longer_when_blocked_and_shares_swagger_within_a_run():
    import io, urllib.error
    reset()
    class Resp:
        def __init__(self, body): self._b = body
        def read(self): return self._b
        def __enter__(self): return self
        def __exit__(self, *a): return False
    def http_err(code): return urllib.error.HTTPError("https://x/y", code, "err", {}, io.BytesIO(b""))
    calls = {"n": 0}
    def urlopen_403_then_ok(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3: raise http_err(403)
        return Resp(b'{"ok": 1}')
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=urlopen_403_then_ok), mock.patch.object(fetch.time, "sleep") as sl:
        assert fetch.fetch_json("https://x/y") == ({"ok": 1}, "")
    assert [c.args[0] for c in sl.call_args_list] == [config.BLOCK_BACKOFF_SECONDS * 1, config.BLOCK_BACKOFF_SECONDS * 2], "被擋（403）要等比較久：20 秒、40 秒"
    calls["n"] = 0
    def urlopen_404(req, timeout=None):
        calls["n"] += 1
        raise http_err(404)
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=urlopen_404), mock.patch.object(fetch.time, "sleep") as sl:
        data, err = fetch.fetch_json("https://x/y")
    assert data is None and "404" in err and calls["n"] == config.RETRIES and [c.args[0] for c in sl.call_args_list] == [config.BACKOFF_SECONDS * 1, config.BACKOFF_SECONDS * 2], "不是被擋（404）就用一般的等待"
    for code in (429, 503):
        with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=lambda req, timeout=None, c=code: (_ for _ in ()).throw(http_err(c))), mock.patch.object(fetch.time, "sleep") as sl:
            fetch.fetch_json("https://x/y")
        assert sl.call_args_list[0].args[0] == config.BLOCK_BACKOFF_SECONDS, code
    with mock.patch.object(fetch.urllib.request, "urlopen", side_effect=OSError("timeout")), mock.patch.object(fetch.time, "sleep") as sl:
        assert fetch.fetch_json("https://x/y")[0] is None
    assert sl.call_args_list[0].args[0] == config.BACKOFF_SECONDS, "連線逾時用一般的等待"
    # 說明文件同一輪只抓一次；失敗的不快取
    reset()
    n = {"c": 0}
    def fj(url, **k):
        n["c"] += 1
        return ({"paths": {}}, "") if n["c"] > 1 else (None, "HTTP 403")
    with mock.patch.object(fetch, "fetch_json", side_effect=fj):
        assert fetch.fetch_swagger("https://s/swagger.json")[0] is None
        assert fetch.fetch_swagger("https://s/swagger.json")[0] == {"paths": {}}
        assert fetch.fetch_swagger("https://s/swagger.json")[0] == {"paths": {}}
    assert n["c"] == 2, "失敗的不快取、成功之後不再重抓：%d" % n["c"]


def _margin_env(days, have=(), http=None):
    urls = []
    def req(method, path, body=None, **k):
        if path.startswith("tw_hist_days"): return [{"as_of": d} for d in days], ""
        if path.startswith("tw_margin"): return [{"as_of": d} for d in have], ""
        raise AssertionError(path)
    def fj(url, **k):
        urls.append(url)
        d = url.split("date=")[1][:8]
        return http(d) if http else (margin_payload(date_str=d), "")
    return [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep")], urls


def test_margin_backfill_fetches_missing_days_validates_dates_and_numbers():
    reset()
    days = [D(1), D(2), D(3), D(4), D(5)]
    patches, urls = _margin_env(days, have=[D(2)])
    rows = _with(patches, lambda: REAL["tw_margin_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1), D(3), D(4), D(5)] and len(urls) == 4, "已經有的不重抓"
    assert all(r["margin_balance"] == 633910848 * 1000 and r["source"] == "TWSE MI_MARGN" and r["margin_prev_balance"] == 632299552 * 1000 for r in rows)
    assert all("selectType=MS" in u and "&date=" in u for u in urls) and urls[0].endswith("&date=" + D(1).replace("-", ""))
    lg = [x for x in fetch.LOG if x["source"] == "margin_backfill"][0]
    assert lg["status"] == "success" and "本次補 4 天" in lg["detail"] and "還有 0 天下次繼續" in lg["detail"]
    reset()
    def http(d):                                                                                  # D(1) 回傳別的日期；D(2) 數字對不上；D(3) 沒資料；D(4) 正常
        if d == D(1).replace("-", ""): return margin_payload(date_str=D(9).replace("-", "")), ""
        if d == D(2).replace("-", ""):
            bad = [list(x) for x in MARGIN_ROWS]; bad[2][5] = "700,000,000"
            return margin_payload(bad, date_str=d), ""
        if d == D(3).replace("-", ""): return {"stat": "很抱歉，沒有符合條件的資料!"}, ""
        return margin_payload(date_str=d), ""
    patches, urls = _margin_env([D(1), D(2), D(3), D(4)], http=http)
    rows = _with(patches, lambda: REAL["tw_margin_backfill"]())
    assert [r["as_of"] for r in rows] == [D(4)]
    d = [x for x in fetch.LOG if x["source"] == "margin_backfill"][0]["detail"]
    assert "略過 1 天（證交所沒有資料）、2 天" in d and "要 " + D(1) + " 但證交所回傳 " + D(9) in d, d


def test_margin_long_backfill_uses_index_days_skips_existing_and_stops_on_failure():
    reset()
    days = [D(i) for i in range(1, 6)]
    def req(method, path, body=None, **k):
        if path.startswith("market_stats"): return ([{"as_of": d} for d in days] if "offset=0" in path else []), ""
        if path.startswith("tw_margin"): return ([{"as_of": D(2)}] if "offset=0" in path else []), ""
        raise AssertionError(path)
    urls = []
    def fj(url, **k):
        urls.append(url); d = url.split("date=")[1][:8]
        if d == D(4).replace("-", ""): return None, "HTTP 403"
        return margin_payload(date_str=d), ""
    patches = [mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", side_effect=req), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch.time, "sleep")]
    rows = _with(patches, lambda: REAL["tw_margin_long_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1), D(3)] and len(urls) == 3, "已有的不重抓；連線失敗整批停止"
    lg = [x for x in fetch.LOG if x["source"] == "margin_long"]
    assert lg[0]["status"] == "failure" and "不繼續打請求" in lg[0]["detail"] and lg[-1]["status"] == "success"
    reset()
    patches[2] = mock.patch.object(fetch, "fetch_json", side_effect=lambda u, **k: (margin_payload(date_str=u.split("date=")[1][:8]), ""))
    days[:] = [D(1)]
    def req2(method, path, body=None, **k):
        return ([{"as_of": D(1)}] if "offset=0" in path else []), ""
    patches[1] = mock.patch.object(fetch, "sb_request", side_effect=req2)
    assert _with(patches, lambda: REAL["tw_margin_long_backfill"]()) == []
    assert [x for x in fetch.LOG if x["source"] == "margin_long"][0]["status"] == "no_new_data"


def test_margin_backfill_cap_stop_on_failure_and_nothing_to_do():
    reset()
    patches, urls = _margin_env([D(i) for i in range(1, 8)])
    rows = _with(patches, lambda: REAL["tw_margin_backfill"](max_days=3))
    assert len(rows) == 3 and "還有 4 天下次繼續" in [x for x in fetch.LOG if x["source"] == "margin_backfill"][0]["detail"]
    reset()
    patches, urls = _margin_env([D(i) for i in range(1, 6)], http=lambda d: (None, "HTTP 403") if d == D(2).replace("-", "") else (margin_payload(date_str=d), ""))
    rows = _with(patches, lambda: REAL["tw_margin_backfill"]())
    assert [r["as_of"] for r in rows] == [D(1)] and len(urls) == 2, "連線失敗整批停止"
    assert [x["status"] for x in fetch.LOG if x["source"] == "margin_backfill"][0] == "failure" and "不繼續打請求" in [x for x in fetch.LOG if x["source"] == "margin_backfill"][0]["detail"]
    reset()
    patches, urls = _margin_env([D(1), D(2)], have=[D(1), D(2)])
    assert _with(patches, lambda: REAL["tw_margin_backfill"]()) == [] and urls == []
    assert [x["status"] for x in fetch.LOG if x["source"] == "margin_backfill"] == ["no_new_data"]
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False):
        assert REAL["tw_margin_backfill"]() == []
    assert fetch.LOG[-1]["status"] == "skipped"
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=True), mock.patch.object(fetch, "sb_request", return_value=(None, "HTTP 401")):
        assert REAL["tw_margin_backfill"]() == []
    assert fetch.LOG[-1]["status"] == "failure" and "讀取交易日清單失敗" in fetch.LOG[-1]["detail"]


def test_run_job_tw_backfill_persists_margin_history_and_isolates_m19():
    reset()
    row = {"as_of": D(3), "margin_balance": 6.3e11, "margin_prev_balance": 6.2e11, "margin_units": 1, "short_units": 1, "source": "TWSE MI_MARGN"}
    base = dict(twse_market=None, twse_breadth=None, twse_institutional=None, twse_margin=None, taifex_futures=None, taifex_basis=None, cftc_cot=None, cboe_putcall=None, twd_fallback_chain=None)
    def go(**kw):
        got = []
        patches = [mock.patch.object(fetch, k, return_value=v) for k, v in base.items()] + [mock.patch.object(fetch, "load_holdings", return_value=(None, "x")), mock.patch.object(fetch, "persist"),
                  mock.patch.object(fetch, "fred_latest", return_value=None), mock.patch.object(fetch, "tw_backfill", return_value={}), mock.patch.object(fetch, "tw_breadth_update"), mock.patch.object(fetch, "tw_crash_check"),
                  mock.patch.object(fetch, "tw_margin_backfill", **(kw or {"return_value": [row]})), mock.patch.object(fetch, "tw_industry_collect", return_value=[]),
                  mock.patch.object(fetch, "persist_extra", side_effect=lambda d_, tables: got.append([(n, r, c) for n, r, c in tables]))]
        for p_ in patches: p_.start()
        try:
            fetch.run_job("tw_backfill", dry_run=False)
        finally:
            for p_ in patches: p_.stop()
        return got[0], {x["source"]: x["status"] for x in fetch.LOG}
    tables, st = go()
    assert ("tw_margin", [row], "as_of,source") in tables and st["module:M19"] == "success"
    reset(); tables, st = go(side_effect=RuntimeError("boom"))
    assert st["module:M19"] == "failure" and "module:M12" in st and "module:M14" in st, "融資回補壞了不影響夜間的其他模組"


def test_probe_round10_reports_options_pcr_tpex_margin_and_date_history_without_writing():
    reset()
    opt = [{"Date": "20261103", "Contract": "TXO", "ContractMonth(Week)": "202611", "StrikePrice": "23000", "CallPut": "Call", "OpenInterest": "1500", "Volume": "10"}] * 3 + [{"Contract": "TEO", "StrikePrice": "1"}]
    pcr = [{"Date": "20261103", "PutVolume": "1", "CallVolume": "2", "PutCallVolumeRatio%": "50", "PutOI": "3", "CallOI": "4", "PutCallOIRatio%": "75"}]
    def fj(url, **k):
        if url.endswith("/DailyMarketReportOpt"): return opt, ""
        if url.endswith("/PutCallRatio"): return pcr, ""
        if url.endswith("/va01"): return [{"Date": "20261103", "Item": "x"}], ""
        if url.endswith("tpex_mainboard_margin_balance"): return [{"Date": "1151103", "SecuritiesCompanyCode": "8358", "MarginPurchase": "1"}], ""
        if url.endswith("tpex_margin_trading_marginspot"): return None, "HTTP 403"
        if "MI_MARGN" in url:
            d = url.split("date=")[1]
            return {"stat": "OK", "date": d, "tables": [{"fields": ["代號"], "data": [[f"{1100 + i}"] for i in range(150)]}]}, ""
        if url.endswith("STOCK_DAY_ALL"): return [{"Date": "1151103", "Code": "2330", "Name": "台積電", "TradeVolume": "1", "ClosingPrice": "1000"}], ""
        raise AssertionError(url)
    with mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round10()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:taifex_opt"]["status"] == "success" and "TXO 共 3 列" in by["probe:taifex_opt"]["detail"] and "StrikePrice" in by["probe:taifex_opt"]["detail"]
    assert by["probe:taifex_pcr"]["status"] == "success" and "PutCallOIRatio%" in by["probe:taifex_pcr"]["detail"] and by["probe:taifex_va01"]["status"] == "success"
    assert by["probe:tpex_margin"]["status"] == "success" and "MarginPurchase" in by["probe:tpex_margin"]["detail"] and by["probe:tpex_marginspot"]["status"] == "no_new_data"
    assert by["probe:twse_margin_date"]["status"] == "success" and "最大一張 150 列" in by["probe:twse_margin_date"]["detail"] and by["probe:twse_day_all"]["status"] == "success" and "ClosingPrice" in by["probe:twse_day_all"]["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG) and not [x for x in fetch.LOG if x["status"] == "failure"]
    reset()
    with mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        fetch.probe_round10()
    assert not [x for x in fetch.LOG if x["status"] == "failure"], "候選來源被擋是正常的探測結果，不能讓工作變紅"


def test_probe_round4_logs_aaii_clues_and_exright_sources_without_writing():
    reset()
    html = (b"<html><script>var x={\"contentUrl\":\"https://www.aaii.com/files/sentiment.xls\"}</script><body>"
            b"<p>Survey results for the week ending Oct 28, 2026</p><div>Bullish 38.5% Neutral 30.0% Bearish 31.5%</div></body></html>")
    sw = {"servers": [{"url": "https://www.tpex.org.tw/openapi/v1"}], "paths": {"/tpex_exright_pre": {"get": {"summary": "上櫃股票除權除息預告"}}, "/x": {"get": {"summary": "其他"}}}}
    ex = {"stat": "OK", "fields": ["資料日期", "股票代號", "股票名稱", "除權息"], "data": [["1151105", "2330", "台積電", "息"]]}
    def fj(url, **k):
        if "exRight" in url: return ex, ""
        if url.endswith("swagger.json"): return sw, ""
        return [{"代號": "8358", "除息日": "1151110"}], ""
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/html", html, "")), mock.patch.object(fetch, "fetch_json", side_effect=fj), mock.patch.object(fetch, "sb_upsert") as up:
        fetch.probe_round4()
    by = {x["source"]: x for x in fetch.LOG}
    assert by["probe:aaii_deep"]["status"] == "success" and "Bullish 38.5%" in by["probe:aaii_deep"]["detail"] and "week ending Oct 28, 2026" in by["probe:aaii_deep"]["detail"]
    assert "sentiment.xls" in by["probe:aaii_links"]["detail"]
    assert by["probe:exright_twse"]["status"] == "success" and "2330" in by["probe:exright_twse"]["detail"]
    assert "/tpex_exright_pre" in by["probe:exright_tpex"]["detail"] and "/x" not in by["probe:exright_tpex"]["detail"] and "8358" in by["probe:exright_tpex_rows"]["detail"]
    up.assert_not_called()
    assert all(len(x["detail"]) <= 900 for x in fetch.LOG)


def test_probe_round4_failures_are_reported_not_hidden():
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(403, "text/html", b"denied", "HTTP 403")), mock.patch.object(fetch, "fetch_json", return_value=(None, "HTTP 403")):
        fetch.probe_round4()
    sts = {x["source"]: x["status"] for x in fetch.LOG}
    assert sts["probe:aaii_deep"] == "failure" and sts["probe:exright_twse"] == "failure" and sts["probe:exright_tpex"] == "no_new_data"


def test_version_stamp_covers_both_files_and_flags_a_partial_upload():
    reset()
    assert fetch.FETCH_VERSION == config.PIPELINE_VERSION, "發行時兩個檔案的版本要一樣"
    with mock.patch.object(fetch, "run_job", return_value={}), mock.patch.object(fetch, "flush_log", return_value=True), \
            mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()):
        assert fetch.main(["--job", "tw_probe", "--dry-run"]) == 0
    first = fetch.LOG[0]
    assert first["source"] == "pipeline:version" and f"程式版本 {config.PIPELINE_VERSION}｜fetch {fetch.FETCH_VERSION}｜" in first["detail"]
    assert not any(x["source"] == "pipeline:version_mismatch" for x in fetch.LOG)
    reset()
    with mock.patch.object(fetch, "FETCH_VERSION", "v20"), mock.patch.object(fetch, "run_job", return_value={}), mock.patch.object(fetch, "flush_log", return_value=True), \
            mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "OUT_DIR", tempfile.mkdtemp()):
        code = fetch.main(["--job", "tw_probe", "--dry-run"])
    mm = [x for x in fetch.LOG if x["source"] == "pipeline:version_mismatch"]
    assert code == 1 and mm and mm[0]["status"] == "failure" and "fetch.py 是 v20" in mm[0]["detail"] and config.PIPELINE_VERSION in mm[0]["detail"] and "沒有一起更新" in mm[0]["detail"]


def test_twd_layer_order_fixed():
    assert config.TWD_LAYERS == ["yahoo_twd_x", "yahoo_usdtwd_x", "fred_dextaus",
                                "taiwan_official", "exchangerate_api"]


if __name__ == "__main__":
    import urllib.error
    import urllib.request

    def _guard(req, *a, **k):
        NETWORK_CALLS.append(getattr(req, "full_url", str(req)))
        raise urllib.error.URLError("測試期間不得連網")
    urllib.request.urlopen = _guard
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and (not os.environ.get("ONLY_TESTS") or any(k in name for k in os.environ["ONLY_TESTS"].split(","))):          # 反向驗證時可以只跑指定的測試（環境變數 ONLY_TESTS）
            before = len(NETWORK_CALLS)
            fn(); n += 1; print("PASS", name)
            if len(NETWORK_CALLS) != before:
                OFFENDERS.append((name, NETWORK_CALLS[before:][:2]))
    assert not OFFENDERS, "這些測試偷偷連了真實網路（要用 mock 替換）：\n" + "\n".join(f"  {a}: {b}" for a, b in OFFENDERS)
    print(f"全部 {n} 項測試通過（且沒有任何真實連網）")


def test_probe_us_pe_logs_pe_like_fields_only_reads_and_never_raises():
    reset()
    quote = json.dumps({"quoteResponse": {"result": [{"symbol": "ORCL", "trailingPE": 31.2, "forwardPE": 25.1, "epsTrailingTwelveMonths": 5.3, "marketCap": 1}]}}).encode()
    def raw(url, timeout=None, headers=None):
        if "v7/finance/quote" in url: return 200, "application/json", quote, ""
        if "quoteSummary" in url: return 200, "text/html", b"<html>", ""
        return 403, "", b"", "HTTP 403"
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "load_us_watch", return_value=["AMD"]), \
            mock.patch.object(fetch, "persist") as per, mock.patch.object(fetch, "persist_extra") as pe:
        fetch.probe_us_pe()
    by = {x["source"]: x for x in fetch.LOG}
    q = by["probe:us_pe:yahoo_quote:AMD"]; assert q["status"] == "success" and "trailingPE=31.2" in q["detail"] and "forwardPE=25.1" in q["detail"] and "marketCap" not in q["detail"], q["detail"]
    assert by["probe:us_pe:yahoo_summary:AMD"]["status"] == "no_new_data" and by["probe:us_pe:nasdaq_info:AMD"]["status"] == "no_new_data"
    assert per.call_count == 0 and pe.call_count == 0 and not [x for x in fetch.LOG if x["status"] == "failure"]
    reset()
    with mock.patch.object(fetch, "fetch_raw", side_effect=RuntimeError("boom")), mock.patch.object(fetch.time, "sleep"), mock.patch.object(fetch, "load_us_watch", return_value=[]):
        fetch.probe_us_pe()
    assert fetch.LOG and all(x["status"] == "no_new_data" for x in fetch.LOG)


def test_sox_history_keeps_closed_days_in_range_and_never_raises():
    reset()
    old = date.today() - timedelta(days=5)
    rows = [{"date": old, "close": 5000.0, "volume": None}, {"date": old + timedelta(days=1), "close": 5.0, "volume": None}, {"date": date.today() + timedelta(days=0), "close": 5100.0, "volume": None}]
    with mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "_nasdaq_rows", return_value=(rows, "")), mock.patch.object(fetch.time, "sleep"), \
         mock.patch.object(fetch, "_now_et", return_value=fetch.datetime(date.today().year, date.today().month, date.today().day, 12, 0)):
        out = REAL["sox_history"]()
    assert [r["as_of"] for r in out] == [old.isoformat()] and out[0]["series_id"] == "SOX" and out[0]["value"] == 5000.0
    reset()
    with mock.patch.object(fetch, "sb_ready", return_value=False), mock.patch.object(fetch, "_nasdaq_rows", return_value=(None, "連線失敗")), mock.patch.object(fetch.time, "sleep"):
        assert REAL["sox_history"]() == []
    assert [x for x in fetch.LOG if x["source"] == "sox_history"][0]["status"] == "no_new_data"


def test_parse_cboe_totalpc_csv_skips_preamble_and_bad_rows():
    text = "CBOE Total Put/Call\n\nDATE,CALLS,PUTS,TOTAL,P/C Ratio\n11/1/2006,976510,623929,1600439,0.64\n2006-11-02,1,1,2,0.80\n11/3/2006,1,1,2,abc\nbad,1,1,2,0.5\n11/6/2006,1,1,2,99\n"
    rows = fetch.parse_cboe_totalpc_csv(text)
    assert rows == [{"as_of": "2006-11-01", "total_pc": 0.64}, {"as_of": "2006-11-02", "total_pc": 0.8}]
    assert fetch.parse_cboe_totalpc_csv("no header here\n1,2,3") == []


def test_cboe_pc_csv_history_only_adds_missing_dates_and_never_raises():
    reset()
    body = "DATE,CALLS,PUTS,TOTAL,P/C Ratio\n11/1/2006,1,1,2,0.64\n11/2/2006,1,1,2,0.7\n".encode()
    with mock.patch.object(fetch, "fetch_raw", return_value=(200, "text/csv", body, "")), mock.patch.object(fetch, "sb_ready", return_value=True), \
         mock.patch.object(fetch, "sb_request", side_effect=lambda m, p, *a, **k: ([{"as_of": "2006-11-01"}], "")):
        out = REAL["cboe_pc_csv_history"]()
    assert [r["as_of"] for r in out] == ["2006-11-02"] and out[0]["total_pc"] == 0.7 and out[0]["index_pc"] is None
    reset()
    with mock.patch.object(fetch, "fetch_raw", return_value=(None, "", b"", "timeout")):
        assert REAL["cboe_pc_csv_history"]() == []
    assert [x for x in fetch.LOG if x["source"] == "cboe_pc_csv"][0]["status"] == "no_new_data"


def test_calc_beta_regression_slope_and_guards():
    import random
    random.seed(1)
    d0 = date(2026, 1, 1)
    idx, stk, pi, ps = {}, {}, 100.0, 50.0
    for i in range(200):
        d = (d0 + timedelta(days=i)).isoformat()
        r = random.uniform(-0.02, 0.02)
        pi *= 1 + r; ps *= 1 + 1.6 * r
        idx[d], stk[d] = pi, ps
    b, n = fetch.calc_beta(stk, idx)
    assert n == 199 and 1.55 <= b <= 1.65, (b, n)
    assert fetch.calc_beta(stk, idx, min_pairs=500) == (None, 199)
    sparse = {k: v for i, (k, v) in enumerate(sorted(stk.items())) if i % 10 == 0}
    assert fetch.calc_beta(sparse, idx)[0] is None


def test_parse_nasdaq_short_interest_and_institutional():
    j = {"data": {"shortInterestTable": {"rows": [{"settlementDate": "10/15/2026", "interest": "12,000,000", "avgDailyShareVolume": "5,000,000", "daysToCover": "2.4"}, {"settlementDate": "09/30/2026", "interest": "9,000,000", "daysToCover": "1.0"}]}}}
    r = fetch.parse_nasdaq_short_interest(j)
    assert r == {"settle": "2026-10-15", "shares": 12000000.0, "days_to_cover": 2.4}, r
    assert fetch.parse_nasdaq_short_interest({"data": None}) is None
    i = fetch.parse_nasdaq_institutional({"data": {"ownershipSummary": {"SharesOutstandingPCT": {"label": "Institutional Ownership", "value": "72.67%"}, "ShareoutstandingTotal": {"label": "Total Shares Outstanding (millions)", "value": "15,334"}}}})
    assert i == {"pct": 72.67, "shares_out": 15334000000.0}, i
    assert fetch.parse_nasdaq_institutional({"data": {"ownershipSummary": {"SharesOutstandingPCT": {"value": "900%"}}}}) is None


def test_stock_risk_stats_us_computes_beta_short_pct_and_inst_and_leaves_missing_empty():
    reset()
    d0 = date.today() - timedelta(days=260)
    spy_rows, st_rows = [], []
    import random
    random.seed(2)
    p, q = 400.0, 100.0
    for i in range(260):
        r = random.uniform(-0.015, 0.015); p *= 1 + r; q *= 1 + 2.2 * r
        spy_rows.append({"date": d0 + timedelta(days=i), "close": p, "volume": 1}); st_rows.append({"date": d0 + timedelta(days=i), "close": q, "volume": 1})
    def rows(sym, ac="etf", days=75, end=None):
        return (spy_rows if sym == "SPY" else st_rows), ""
    def raw(url, **kw):
        if "institutional-holdings" in url:
            return 200, "application/json", json.dumps({"data": {"ownershipSummary": {"SharesOutstandingPCT": {"value": "60.5%"}, "ShareoutstandingTotal": {"label": "Total Shares Outstanding (millions)", "value": "100"}}}}).encode(), ""
        if "short-interest" in url:
            return 200, "application/json", json.dumps({"data": {"shortInterestTable": {"rows": [{"settlementDate": "10/31/2026", "interest": "12,000,000", "daysToCover": "3.1"}]}}}).encode(), ""
        return 404, "", b"", "nope"
    with mock.patch.object(fetch, "_nasdaq_rows", rows), mock.patch.object(fetch, "fetch_raw", raw), mock.patch.object(fetch.time, "sleep", lambda s: None), mock.patch.object(fetch, "sb_ready", lambda: False):
        out = REAL["stock_risk_stats"]([{"market": "US", "symbol": "AAA", "kind": "stock"}, {"market": "US", "symbol": "QQQ", "kind": "etf"}], [])
    a = [r for r in out if r["code"] == "AAA"][0]; e = [r for r in out if r["code"] == "QQQ"][0]
    assert 2.1 <= a["beta"] <= 2.3 and a["inst_pct"] == 60.5 and a["short_pct"] == 12.0 and a["days_to_cover"] == 3.1 and a["short_as_of"] == "2026-10-31", a
    assert e["beta"] is not None and e["short_pct"] is None and e["inst_pct"] is None, e
    assert [x for x in fetch.LOG if x["source"] == "stock_risk_stats"][0]["status"] == "success"


def _ms_json(d, up, down, flat):
    return {"stat": "OK", "date": d.replace("-", ""), "tables": [{"title": "漲跌證券數合計", "fields": ["類型", "整體市場", "股票"], "data": [["上漲(漲停)", "1,000(5)", f"{up}(3)"], ["下跌(跌停)", "900(2)", f"{down}(1)"], ["持平", "100", f"{flat}"]]}]}


def test_parse_breadth_ms_checks_date_and_counts():
    assert fetch.parse_breadth_ms(_ms_json("2020-03-12", 100, 800, 50), "2020-03-12") == ((100, 800, 50), "")
    r, why = fetch.parse_breadth_ms(_ms_json("2020-03-13", 100, 800, 50), "2020-03-12"); assert r is None and "回傳" in why
    r, why = fetch.parse_breadth_ms({"stat": "很抱歉，沒有符合條件的資料!"}, "2020-03-12"); assert r is None and "沒有這天" in why
    r, why = fetch.parse_breadth_ms(_ms_json("2020-03-12", 1, 1, 1), "2020-03-12"); assert r is None and "合理" in why


def test_tw_breadth_long_backfill_fills_missing_days_and_skips_bad_ones():
    reset()
    days = ["2020-03-13", "2020-03-12", "2020-03-11"]
    def sbr(method, path, *a, **k):
        if "market_stats" in path: return ([{"as_of": d} for d in days] if "offset=0" in path else []), ""
        if "tw_breadth_hist" in path: return ([{"as_of": "2020-03-13"}] if "offset=0" in path else []), ""
        return [], ""
    def fj(url, **k):
        if "date=20200312" in url: return _ms_json("2020-03-12", 100, 800, 50), ""
        return {"stat": "很抱歉，沒有符合條件的資料!"}, ""
    with mock.patch.object(fetch, "sb_request", sbr), mock.patch.object(fetch, "sb_ready", lambda: True), mock.patch.object(fetch, "fetch_json", fj), mock.patch.object(fetch.time, "sleep", lambda s: None):
        out = REAL["tw_breadth_long_backfill"]()
    assert out == [{"as_of": "2020-03-12", "market": "TWSE", "n_up": 100, "n_down": 800, "n_flat": 50}], out
    assert "略過 1 天" in [x for x in fetch.LOG if x["source"] == "breadth_long"][0]["detail"]


def test_tw_margin_ratio_backfill_stops_when_twse_ignores_date_and_computes_when_it_works():
    reset()
    def sbr(method, path, *a, **k):
        if "tw_hist_days" in path: return [{"as_of": "2026-11-12"}], ""
        if "tw_margin_ratio" in path: return [], ""
        if "tw_hist?" in path: return ([{"code": "2330", "close": 1000}, {"code": "2317", "close": 200}] if "offset=0" in path else []), ""
        return [], ""
    base = {"stat": "OK", "date": "20261112"}
    with mock.patch.object(fetch, "sb_request", sbr), mock.patch.object(fetch, "sb_ready", lambda: True), mock.patch.object(fetch.time, "sleep", lambda s: None), \
         mock.patch.object(fetch, "fetch_json", lambda url, **k: (dict(base, date="20261113"), "")):
        assert REAL["tw_margin_ratio_backfill"]() == []
    assert "不提供歷史" in [x for x in fetch.LOG if x["source"] == "ratio_backfill"][0]["detail"]
    reset()
    with mock.patch.object(fetch, "sb_request", sbr), mock.patch.object(fetch, "sb_ready", lambda: True), mock.patch.object(fetch.time, "sleep", lambda s: None), \
         mock.patch.object(fetch, "fetch_json", lambda url, **k: (base, "")), \
         mock.patch.object(fetch, "parse_margin_stock_table", lambda d: ({"2330": 1000, "2317": 500}, None)), \
         mock.patch.object(fetch, "_parse_margin", lambda d: ({"margin_balance": 1000 * 1000 * 1000 * 0.5 + 500 * 1000 * 200 * 0.5}, None)):
        out = REAL["tw_margin_ratio_backfill"]()
    assert len(out) == 1 and out[0]["as_of"] == "2026-11-12" and abs(out[0]["est_ratio"] - 200.0) < 0.01, out


def test_parse_aaii_csv_accepts_fractions_percent_and_skips_bad_rows():
    txt = "AAII Sentiment Survey,,,\nDate,Bullish,Neutral,Bearish,Total\n11/05/2026,0.38,0.30,0.32,1\n2026-10-29,45%,25%,30%,100%\n2026-10-22,10,10,10,30\n11/05/2026,0.4,0.3,0.3,1\nbad,1,2,3,4\n"
    rows, sk = fetch.parse_aaii_csv(txt)
    assert [r["as_of"] for r in rows] == ["2026-11-05", "2026-10-29"] and rows[0]["bullish"] == 38.0 and rows[1]["bearish"] == 30.0 and sk == 3, (rows, sk)
    assert fetch.parse_aaii_csv("a,b\n1,2")[0] == []
