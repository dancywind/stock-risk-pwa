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
    # 聯準會 Z.1「券商對客戶的融資及其他應收款」季資料（百萬美元）。範圍和頻率都和 FINRA 的月資料不同，只當大方向參考。
    "BOGZ1FL663067003Q": ("美國券商對客戶融資及其他應收款(Fed Z.1 季資料，百萬美元)", 100000.0, 5000000.0),
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
TAIFEX_NET_LIMIT = 200000
# 期交所「期貨每日交易行情」：網址與欄位名稱已用真實回傳驗證（Contract 是代碼 TX，交易時段分「一般」「盤後」）
TAIFEX_DAILY_URL = "https://openapi.taifex.com.tw/v1/DailyMarketReportFut"
TAIFEX_DAILY_CONTRACT = "TX"
BASIS_LIMIT_PCT = 5.0             # 基差佔指數超過這個百分比視為壞資料         # 單一身份別淨未平倉口數合理上限，超過視為壞資料
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

# 持股現價探測（只讀）：證交所每日收盤行情（含資料日期）、櫃買中心上櫃行情、Nasdaq 個股報價
TWSE_STOCK_DAY_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
TPEX_SWAGGER = "https://www.tpex.org.tw/openapi/swagger.json"
TPEX_DEFAULT_BASE = "https://www.tpex.org.tw/openapi/v1"
NASDAQ_QUOTE_URL = "https://api.nasdaq.com/api/quote/{symbol}/info?assetclass={assetclass}"

# 已用真實回傳驗證：櫃買中心上櫃股票行情（Date 是民國年七碼，例如 1151029）
TPEX_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TWSE_OPENAPI_SWAGGER = "https://openapi.twse.com.tw/v1/swagger.json"
TWSE_OPENAPI_BASE = "https://openapi.twse.com.tw/v1"
# 已用真實回傳驗證：證交所 OpenAPI「上市個股日收盤價及月平均價」，欄位 Date(民國年七碼)/Code/Name/ClosingPrice
TWSE_AVG_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
# 歷史資料探測（只讀）：證交所／櫃買中心指定日期的全部個股收盤行情，用來之後算 52 週高低與廣度指標
TWSE_HIST_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=ALLBUT0999&response=json"
TPEX_HIST_URLS = {
    "web_roc": "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json&d={roc}",
    "www_dailyQuotes": "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date={slash}&id=&response=json",
    "www_otc": "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={slash}&type=EW&response=json",
}
# 台股歷史回補：每個交易日向證交所（上市）、櫃買中心（上櫃）各查一次全部個股行情，存最高、最低、收盤
TPEX_DAY_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={slash}&type=EW&response=json"
HIST_SLEEP = 3.0                  # 每次請求之間等幾秒，避免被交易所限流
HIST_BATCH_DAYS = 70              # 每次回補最多處理幾個交易日（約 15 到 20 分鐘，低於工作的 30 分鐘上限）
HIST_WINDOW_DAYS = 400            # 回補到幾天前
HIST_DAILY_DAYS = 3               # 每日排程順手補最近幾個平日
RANGE52_MIN_COVERAGE_DAYS = 330   # 歷史涵蓋不到這麼多天，就不算 52 週高低
RANGE52_MIN_ROWS = 200
NASDAQ_HIST_URL = "https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass={assetclass}&fromdate={fromdate}&limit=400&todate={todate}"
# 台幣：FRED 官方收盤落後超過這麼多天，就另外抓較新的即期參考價（只當參考，不進收盤歷史）
TWD_LAG_DAYS = 2
# 大跌檢視清單（台灣：單日跌幅 9% 以上自動加入，一律標「待確認」）
CRASH_DROP_PCT = 9.0
CRASH_RECENT_DAYS = 10            # 回補工作每次重新檢查最近幾個完成的交易日
CRASH_MAX_AGE_DAYS = 30           # 只檢查最近這麼多天內的交易日；更早的大跌不加入清單（避免把歷史回補的整年資料都收進來）
CRASH_PREV_MAX_GAP_DAYS = 10      # 前一個交易日距離超過這麼多天，就不比較（避免拿很舊的價格算跌幅）
# 槓桿 ETF 的 52 週區間：用 Nasdaq 歷史行情算。價格出現這個倍數以上的單日跳動，視為分割或反向分割沒有調整，不採用。
SPLIT_JUMP_UP = 2.5
SPLIT_JUMP_DOWN = 0.33
# 美股資金壓力三項的來源探測（只讀）：CFTC COT、FINRA 融資統計、CBOE Put/Call
CFTC_TFF_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json?$limit=6&$order=report_date_as_yyyy_mm_dd%20DESC&$q=S%26P%20500"
FINRA_MARGIN_URL = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
CBOE_PC_URLS = {
    "totalpc_csv": "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv",
    "daily_json": "https://cdn.cboe.com/data/us/options/market_statistics/daily/{ymd}_daily_options",
}
# 已用真實回傳驗證：CFTC TFF（期貨 only）標普 500 合併合約；CBOE 每日市場統計 JSON
CFTC_TFF_BASE = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
CFTC_MARKET = "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE"
CBOE_DAILY_URL = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{ymd}_daily_options"
# 程式版本：每次更新檔案時加一。每次執行都會把這個版本寫進紀錄，網站「詳細資料」也會顯示，方便確認 GitHub 上跑的是不是最新版。
PIPELINE_VERSION = "v29"
# 財報日探測（只讀）：Nasdaq 個股「下次財報日」、證交所與櫃買中心開放資料裡的法說會／財報相關端點
NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/analyst/{symbol}/earnings-date"
EARNINGS_KEYWORDS = ("法人說明會", "法說", "財務報告", "財報", "營業收入", "重大訊息")
EARNINGS_MAX_AHEAD_DAYS = 200      # 財報日比今天晚超過這麼多天，視為解析錯誤
PRICE_LIMIT = 100000.0            # 單一股價合理上限，超過視為壞資料

JOBS = ("full", "tw_close", "us_breadth", "tw_probe", "us_probe", "tw_backfill")


def fred_key():
    return os.environ.get("FRED_API_KEY", "")


def supabase_url():
    return os.environ.get("SUPABASE_URL", "")


def supabase_key():
    return os.environ.get("SUPABASE_SERVICE_KEY", "")
