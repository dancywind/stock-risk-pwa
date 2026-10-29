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


def test_tw_close_job_does_not_claim_breadth():
    reset()
    with mock.patch.object(fetch, "twse_market", return_value=None), \
            mock.patch.object(fetch, "twd_fallback_chain", return_value=None):
        fetch.run_job("tw_close", dry_run=True)
    assert any(x["source"] == "tw_breadth" and x["status"] == "skipped" for x in fetch.LOG)


def test_twd_layer_order_fixed():
    assert config.TWD_LAYERS == ["yahoo_twd_x", "yahoo_usdtwd_x", "fred_dextaus",
                                "taiwan_official", "exchangerate_api"]


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); n += 1; print("PASS", name)
    print(f"全部 {n} 項測試通過")
