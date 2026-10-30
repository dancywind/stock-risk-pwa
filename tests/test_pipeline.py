"""用模擬回傳測試：壞資料、舊日期、被擋、寫入不確認、備用鏈。
不連網、不需要金鑰。pytest 或 python3 直接跑都可以。"""
import os
import sys
from datetime import date, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline import fetch, config  # noqa: E402

TODAY = date.today()
D = lambda n: (TODAY - timedelta(days=n)).isoformat()  # noqa: E731


def reset():
    fetch.LOG.clear()


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
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None):
        fetch.run_job("tw_close", dry_run=True)
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
    with mock.patch.object(fetch, "probe_twse"), mock.patch.object(fetch, "probe_extra"), \
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
    plain = '{"b": 1}'.encode("utf-8")                                      # 不帶 BOM 也要能讀
    with mock.patch.object(fetch.urllib.request, "urlopen", return_value=FakeResp(plain)):
        data, err = fetch.fetch_json("https://example.test/y")
    assert data == {"b": 1}


def test_twd_layer_order_fixed():
    assert config.TWD_LAYERS == ["yahoo_twd_x", "yahoo_usdtwd_x", "fred_dextaus",
                                "taiwan_official", "exchangerate_api"]


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); n += 1; print("PASS", name)
    print(f"全部 {n} 項測試通過")
