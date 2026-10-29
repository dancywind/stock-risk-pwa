# 風險管理 PWA — 第一階段（第 1 步）資料管線

本版做的事：FRED 總經 + 台幣備用鏈 → 驗證 → 真實寫入 Supabase → 留下紀錄。
不含任何真實個人資料；金鑰只放環境變數／GitHub Secrets。全站不寫買賣建議。

## 排程工作（--job）
- full：FRED 總經（10Y、2Y、VIX、WTI、Brent、S&P 500、Nasdaq 綜合、美元廣義指數）+ 台幣
- tw_close：台幣；台股資料第二步才接
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
