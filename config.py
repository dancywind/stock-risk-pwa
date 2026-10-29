"""來源與備用鏈設定。不含任何金鑰，金鑰一律在執行時從環境變數讀取。"""
import os

TIMEOUT_SECONDS = 60
RETRIES = 3
BACKOFF_SECONDS = 5

FRED_SERIES_URL = "https://api.stlouisfed.org/fred/series/observations"
EXCHANGERATE_API = "https://open.er-api.com/v6/latest/USD"

# 證交所「每日市場成交資訊」：加權指數收盤、成交金額、成交股數、成交筆數（官方）
# 不帶 date 參數＝當月；帶 date=YYYYMM01＝指定月份。
TWSE_FMTQIK = "https://www.twse.com.tw/rwd/en/afterTrading/FMTQIK"
TAIEX_RANGE = (1000.0, 100000.0)  # 加權指數合理範圍，超出視為壞資料

# 台幣備用鏈，順序固定
TWD_LAYERS = [
    "yahoo_twd_x",       # 第 1 層：本階段不使用 Yahoo（排程 IP 易被擋），直接略過並記錄
    "yahoo_usdtwd_x",    # 第 2 層：同上
    "fred_dextaus",      # 第 3 層：FRED 官方日資料
    "taiwan_official",   # 第 4 層：台銀牌告／央行 CPX，第二階段才接
    "exchangerate_api",  # 第 5 層：最後一層，即期彙總價
]

# FRED 總經序列：series_id -> (白話名稱, 合理範圍下限, 上限)
# 超出合理範圍一律拒收（防止壞資料），不寫入。
FRED_MACRO = {
    "DGS10": ("美國10年期公債殖利率(%)", 0.0, 15.0),
    "DGS2": ("美國2年期公債殖利率(%)", 0.0, 15.0),
    "VIXCLS": ("VIX 恐慌指數", 5.0, 100.0),
    "DCOILWTICO": ("WTI 原油現貨價(美元/桶)", -50.0, 250.0),
    "DCOILBRENTEU": ("Brent 原油現貨價(美元/桶)", 5.0, 250.0),
    "SP500": ("S&P 500 指數", 500.0, 30000.0),
    "NASDAQCOM": ("Nasdaq 綜合指數", 1000.0, 80000.0),
    # 注意：這是美國聯準會的「廣義美元指數」，不是 ICE 的 DXY，不可標成 DXY
    "DTWEXBGS": ("美元廣義指數(聯準會，非 ICE DXY)", 50.0, 200.0),
}
TWD_FRED_SERIES = "DEXTAUS"
TWD_RANGE = (20.0, 45.0)  # 美元兌台幣合理範圍

# 排程工作：每個工作只做自己的事
# 探測用（只讀、不寫市場資料表）：確認證交所「漲跌家數」的真實端點與欄位格式
TWSE_SWAGGER = "https://openapi.twse.com.tw/v1/swagger.json"
TWSE_MI_INDEX_MS = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=MS&response=json"

JOBS = ("full", "tw_close", "us_breadth", "tw_probe")


def fred_key():
    return os.environ.get("FRED_API_KEY", "")


def supabase_url():
    return os.environ.get("SUPABASE_URL", "")


def supabase_key():
    return os.environ.get("SUPABASE_SERVICE_KEY", "")
