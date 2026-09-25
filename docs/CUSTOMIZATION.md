# 客製化設定

共用程式不內嵌部署者的帳戶、主機、使用政策或時區。複製 `examples/bot.env.example` 到自己的私有環境檔，正式服務由 `/etc/crypto-monitor-v2/bot.env` 讀取；修改後重啟協調器。所有偏好由 `BotSettings` 一次解析、驗證並注入 UI／通知／快取／維護，不由普通用戶的備份匯入覆寫。

| 環境變數 | 預設 | 可設範圍／作用 |
|---|---:|---|
| BOT_TITLE | 多鏈地址交易監控 | 主選單標題，1–64 字，當純文字跳脫 HTML |
| BOT_STATUS_TITLE | 全鏈監控 | 運行狀態標題，1–64 字 |
| BOT_USAGE_NOTICE | 空白 | 部署者使用政策，最多 300 字；說明、新增地址及移除提示同步 |
| DISPLAY_TIMEZONE_OFFSET_MINUTES | 0 | UTC 固定偏移，-720～840；不自動處理夏令時間 |
| NOTIFICATION_INTERVAL_SECONDS | 5 | 5–3600 秒；新通知、確認編輯與過載提示共用 |
| TELEGRAM_GLOBAL_RPS | 20 | 1–20 次／秒；只能維持或收緊既有上限 |
| HOUSEKEEPING_INTERVAL_SECONDS | 1 | 1–60 秒；每帳戶背景消息清理／狀態更新 |
| MENU_TTL_SECONDS | 600 | 60–86400 秒；菜單、輸入流程與非通知消息共用期限 |
| FILTER_PAGE_SIZE | 5 | 1–5 筆，限制訊息長度 |
| ADDRESS_PAGE_SIZE | 10 | 1–25 個地址按鈕 |
| ADDRESS_PENDING_LIMIT | 20 | 1–1000 筆；超過才移除該用戶的過載地址，不能以 0 關閉 |
| RECENT_CACHE_MIB | 256 | 1–65536 MiB，容量上限，不預先分配 |
| RECENT_CACHE_MAX_ENTRIES | 50000 | 100–1000000 筆，和容量上限同時生效 |
| NOTIFICATION_WORKERS | 32 | 1–32 個帳戶並行；同帳戶最多一個工作 |
| TOKEN_LOOKUP_WORKERS | 4 | 1–8 個 Token 查詢工作者 |
| TOKEN_LOOKUP_MAX_PENDING | 32 | 1–128 個待查工作，不能小於工作者數 |
| OBSOLETE_RECORD_DAYS | 7 | 1–3650 天；過濾／失效與既有可清理失敗記錄 |
| CONFIRMED_RECORD_DAYS | 30 | 1–3650 天；已確認且不再有有效待發的記錄 |
| MAX_DISPOSABLE_RECORDS | 25000 | 100–1000000 筆可清理記錄上限；數量與天數任一達到即可清理 |

Token 與所有者仍透過 `BOT_TOKEN`、`OWNER_USER_ID` 提供。錯誤設定直接拒絕啟動，不靜默回退。記錄保留只影響本地資料庫，不刪除已發交易消息；既有有效待發不因保留天數被清理。變更保存天數或數量可能在下次維護刪除可清理舊資料，修改前自行備份。增大併發不會繞過 API 硬閘，仍需自己承擔公共 RPC／查詢 API 額度。

舊的已保存冷卻期限不會因新偏好清零；下一次發送按新間隔預約。主動互動仍優先，不另套帳戶速率；遇 Telegram 429 仍必須等待。429 紀錄上限、跨用戶授權、租約、TLS 校驗、大小限制等安全規則不提供關閉開關。

## 鏈與節點

`chains.json` 可配置各鏈 RPC、鏈身分、顯示名稱、狀態頁簡稱 `status_name`（最多顯示 9 個等寬格）、瀏覽器連結、原生幣資訊、確認距離、最低原生幣金額、市場流動性／轉帳價值門檻。`/info` 共用門檻統一顯示；門檻不同則逐鏈列出，過濾原因不再寫死 US$1／US$10,000。

`cluster.json.chains` 只保留需要啟用的鏈；`chains.json` 可保留完整設定目錄，Bot 與掃描器都只使用啟用項。`nodes`、`primary_node`、`preferred_node`、`max_chains` 決定節點與分工；TLS 模式的 IP 可省略，單機／多機均可。協調器可獨立部署，不必出現在 Worker 清單。

心跳預設 5 秒，租約 20 秒；建議保持預設，任意加大間隔可能讓租約過期。systemd CPU／記憶體限制用部署 drop-in；狀態路徑、設定路徑可從 `cluster.py --help` 及各角色 `--help` 查看。憑證、切換流程與增加節點見 [部署文件](DEPLOYMENT.md)。

自用備份系統不在公開範圍；只能在各自私有部署目錄配置。Bot 用戶地址匯入／匯出是共用功能，與伺服器備份分開。

EVM 的 `genesis_optional_rpc_urls` 預設為空，只可填入 `rpc_urls` 中明確信任的完整 URL。列入的端點免查創世區塊，但仍核對鏈 ID、最新區塊時間／高度、完整區塊與 Transfer 日誌；適合不提供創世區塊的官方 RPC。範例只列 Hyperliquid 官方端點，其他端點仍驗證設定的創世 hash。變更此清單會使該鏈資格快取重新驗證，既有冷卻及限流狀態仍保留。
