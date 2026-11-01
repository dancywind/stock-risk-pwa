"""用模擬回傳測試：壞資料、舊日期、被擋、寫入不確認、備用鏈。
不連網、不需要金鑰。pytest 或 python3 直接跑都可以。"""
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline import fetch, config  # noqa: E402

TODAY = date.today()
D = lambda n: (TODAY - timedelta(days=n)).isoformat()  # noqa: E731


MODULE_FUNCS = ("us_extra_quotes", "sector_rs", "tw_leaders_compute", "tw_index_history", "fred_history_backfill",
                "tw_financials", "us_crash_screen", "aaii_sentiment", "us_leaders_compute", "tw_investor_events")
REAL = {n: getattr(fetch, n) for n in MODULE_FUNCS}          # 模組真正的函式；測試模組本身時用 REAL[...]
OFFENDERS = []
NETWORK_CALLS = []                                             # 任何沒有被測試替換掉的真實連線都會記在這裡，最後整個測試會失敗


def reset():
    fetch.LOG.clear()
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
    assert [x["status"] for x in fetch.LOG].count("failure") == 3


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
    with mock.patch.object(fetch, "probe_round4"), mock.patch.object(fetch, "probe_round3"), mock.patch.object(fetch, "probe_earnings") as pn, mock.patch.object(fetch, "fred_latest") as f, \
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
    assert "concurrency:" in y and "cancel-in-progress: false" in y and "timeout-minutes: 30" in y


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
    prices = {"SPY": "$745.20", "TLT": "$88.10", "IAU": "$55.30", "QQQ": "$640.00", "SOX": "6,100.55"}
    def raw(url, timeout=None, headers=None):
        sym = url.split("/quote/")[1].split("/")[0]
        if sym == "GLD":
            return None, "", b"", "timed out"                                                      # GLD 失敗 → 改用 IAU
        if sym == "SOX":
            return 200, "application/json", nas_quote_body(price=prices[sym], asset="index"), ""
        return 200, "application/json", nas_quote_body(price=prices[sym]), ""
    with mock.patch.object(fetch, "fetch_raw", side_effect=raw), mock.patch.object(fetch.time, "sleep"):
        out = REAL["us_extra_quotes"]()
    by = {r["symbol"]: r for r in out}
    assert set(by) == {"SPY", "TLT", "IAU", "QQQ", "SOX"} and "GLD" not in by
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
    with with_key(), mock.patch.object(fetch, "fetch_json", return_value=({"observations": [{"date": D(3), "value": "6000"}]}, "")):
        rows = REAL["fred_history_backfill"]()
    assert {r["series_id"] for r in rows} == set(config.FRED_HISTORY) == {"SP500", "NASDAQCOM", "DJIA"}


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
        if name.startswith("test_"):
            before = len(NETWORK_CALLS)
            fn(); n += 1; print("PASS", name)
            if len(NETWORK_CALLS) != before:
                OFFENDERS.append((name, NETWORK_CALLS[before:][:2]))
    assert not OFFENDERS, "這些測試偷偷連了真實網路（要用 mock 替換）：\n" + "\n".join(f"  {a}: {b}" for a, b in OFFENDERS)
    print(f"全部 {n} 項測試通過（且沒有任何真實連網）")
