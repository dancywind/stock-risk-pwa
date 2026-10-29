# 風險管理 PWA — 第一階段（第 1 步）資料管線

本版做的事：FRED 總經 + 台幣備用鏈 → 驗證 → 真實寫入 Supabase → 留下紀錄。
不含任何真實個人資料；金鑰只放環境變數／GitHub Secrets。全站不寫買賣建議。

## 排程工作（--job）
- full：FRED 總經（10Y、2Y、VIX、WTI、Brent、S&P 500、Nasdaq 綜合、美元廣義指數）+ 台幣 + 證交所加權指數與成交金額
- tw_close：台幣 + 證交所加權指數收盤與成交金額（廣度尚未接）
- tw_probe：只讀探測（不寫市場資料表），用來確認證交所「漲跌家數」的端點與欄位格式，結果存在 scan_status（source 以 probe: 開頭）
- us_breadth：尚未實作，只留一筆「略過」紀錄，不再偷跑全部更新

## 本機測試
    python3 tests/test_pipeline.py          # 不需金鑰、不連網
    python3 pipeline/fetch.py --job full --dry-run   # 抓真資料但不寫資料庫

## 已知限制（照實寫）
- 還沒用真實 FRED／Supabase 金鑰實測過；測試皆為模擬回傳。第一次請用 dry-run，再正式跑一次看 scan_status。
- 台幣第 1、2 層（Yahoo）故意略過，第 4 層（台灣官方）第二階段才接。
- 有任何 failure 程式會回傳結束碼 1，GitHub Actions 會顯示紅燈。
- GitHub 排程可能延遲數分鐘到更久；倉庫 60 天沒動靜，排程會被自動停用，需手動重新啟用。
- 美元廣義指數(DTWEXBGS)不是 ICE DXY，畫面標示不可寫成 DXY。

## 第 2 步新增：台股加權指數與成交金額
- 來源：證交所「每日市場成交資訊」(FMTQIK)，官方、免金鑰；寫入 market_stats（market=TW）。
- 漲跌幅由官方「漲跌點數」換算；成交金額單位為新台幣元。
- 不含漲跌家數（廣度）：尚未接，畫面須顯示「查不到」，不得用其他數字冒充。
- 尚未用 GitHub 排程機器實測連線；若被擋會記為 failure（紅燈），不會編數字。
