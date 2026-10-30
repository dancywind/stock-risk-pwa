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
BREADTH_TOTAL_RANGE = (300, 5000)  # 股票欄「上漲+下跌+持平」合計的合理範圍，超出視為壞資料
BREADTH_SOURCE = "TWSE MI_INDEX 股票"

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

# 第 4 步探測：外資買賣超、融資餘額（證交所）與期交所外資期貨部位。只讀，用來確認真實格式。
# 已用真實回傳驗證過格式：三大法人買賣金額(BFI82U)、信用交易統計(MI_MARGN)
TWSE_BFI82U = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&response=json"
TWSE_MI_MARGN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?selectType=MS&response=json"
PROBE_URLS = {"BFI82U": TWSE_BFI82U, "MI_MARGN": TWSE_MI_MARGN}
# 期交所開放資料說明文件（網址依 data.gov.tw 官方資料集頁面；先前的 /v1/swagger.json 回傳非 JSON）
TAIFEX_SWAGGER = "https://openapi.taifex.com.tw/swagger.json"
# 期交所「期貨市場三大法人-區分各期貨契約-依日期」。網址與欄位名稱已用真實回傳驗證。
# 必須帶 Accept: application/json（fetch_json 預設已帶）。
TAIFEX_FUTURES_URL = ("https://openapi.taifex.com.tw/v1/"
                      "MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate")
TAIFEX_CONTRACT = "臺股期貨"      # 只取大台指（TX）；小型、微型台指目前不計
TAIFEX_NET_LIMIT = 200000         # 單一身份別淨未平倉口數合理上限，超過視為壞資料
# 外資買賣超（新台幣元）合理上限；超過視為壞資料
INST_NET_LIMIT = 2e12
INST_SIDE_LIMIT = 5e12
# 融資金額餘額（新台幣元）合理範圍
MARGIN_BALANCE_RANGE = (1e10, 5e13)

# 美股廣度探測（只讀）：Nasdaq 官方網站使用的公開介面，沒有保證穩定；每次只打少數幾個請求。
NASDAQ_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/124.0.0.0 Safari/537.36")
NASDAQ_PROBE_URLS = {
    "market_info": "https://api.nasdaq.com/api/market-info",
    "comp_summary": "https://api.nasdaq.com/api/quote/COMP/summary?assetclass=index",
    "comp_info": "https://api.nasdaq.com/api/quote/COMP/info?assetclass=index",
    "ndx_summary": "https://api.nasdaq.com/api/quote/NDX/summary?assetclass=index",
}
PROBE_TIMEOUT = 20
# 第二輪：Nasdaq 網站的「股票清單下載」介面，每檔股票帶當日漲跌幅，可自行統計家數（先只探測）
NASDAQ_SCREENER_URLS = {
    "nasdaq": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&exchange=nasdaq&download=true",
    "nyse": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&exchange=nyse&download=true",
    "amex": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&exchange=amex&download=true",
}
SCREENER_TIMEOUT = 45

JOBS = ("full", "tw_close", "us_breadth", "tw_probe", "us_probe")


def fred_key():
    return os.environ.get("FRED_API_KEY", "")


def supabase_url():
    return os.environ.get("SUPABASE_URL", "")


def supabase_key():
    return os.environ.get("SUPABASE_SERVICE_KEY", "")
