# Telegram 多鏈交易監控 V2

透過 Telegram 追蹤自己的鏈上地址。一套全鏈掃描服務，多個用戶共用下載結果，各自管理地址、查看記錄及接收通知。支援單機運行，也能把不同鏈分配到多臺伺服器。

## 特性

- **每條鏈共用一次掃描**：下載最新區塊及代幣 Transfer 日誌，在 Worker 本機匹配地址；新增用戶不會為每個地址重複掃描全鏈。
- **多用戶隔離**：所有者授權 Telegram 用戶 ID；地址、過濾記錄、匯入／匯出與通知按用戶隔離，發送前再次核對權限。
- **部署自由**：自行選擇啟用的鏈、Worker 數量、鏈分配及接管節點。協調器可與 Worker 同機，也可獨立部署。
- **RPC 自適應**：按成功吞吐、延遲與錯誤優選，限流端點有序冷卻，背景重新檢查；健康快取跨重啟保存。
- **交易風險過濾**：結合 GoPlus 風險信號與 DEX Screener 流動性／轉帳價值；持久化 Token 與價格快取，減少重複查詢。
- **可恢復的通知**：鏈上未確認交易先通知，確認後更新；每帳戶獨立處理 Telegram 429，互動優先，避免一個帳戶阻塞其他人。
- **直接公網通信**：節點使用雙向 TLS，核對憑證、租約及 epoch；沒有 VPN 或 Tailscale 依賴。Worker 不持有 Bot Token 或用戶 ID。
- **輕量依賴**：Python 標準函式庫、SQLite、Linux systemd，不需要 pip 套件或外部資料庫。

## 監控範圍

| 鏈 | 資產 |
|---|---|
| Ethereum、BNB Smart Chain、Polygon、Avalanche、Arbitrum One、OP Mainnet、Base、HyperEVM | 原生幣直接轉帳、ERC-20 Transfer |
| TRON | TRX、TRC-20 Transfer |

只啟用需要的鏈即可。EVM 合約內部原生幣轉帳、NFT、未上鏈 mempool 不在範圍內。「交易未確認」指已進入區塊、尚未達設定確認距離的交易。

**啟動、重啟及故障接管都從當時最新鏈頭開始，不補停機期間的歷史交易。** 全鏈掃描是即時下載與處理，不是保存整條區塊鏈的完整節點。用途以個人地址為主，不適合交易所熱錢包、高頻量化地址或需要無缺漏歷史帳本的場合。

## 架構與擴充

```text
各鏈 RPC → Worker（每鏈掃描、地址匹配）
                      ↓ HTTPS 雙向 TLS
           協調器（租約、交易重新核對）
                      ↓
           用戶隔離、風險過濾 → Telegram
```

協調器包含 Bot、交易核對與 SQLite；Worker 執行分配給它的鏈。`cluster.json` 的 `chains` 決定實際啟用範圍，`preferred_node` 指定日常分工，`primary_node` 指定其他 Worker 故障時的接管者。節點名稱及部署位置由你決定，不綁定雲端商或固定三臺機器。

加入 Worker 後可把部分鏈移過去；同一條鏈同時由一個租約持有者掃描，不會因增加 Worker 自動拆分同一條鏈。主 Worker 故障時不把它的工作反向轉給小型 Worker；協調器目前是單一服務，沒有自動多主切換。接管節點需為所有啟用鏈保留容量。

## 建議伺服器配置

以下是**留有餘量的起步建議，不是最低要求或容量保證**。以少量個人用戶、一般地址活動為前提，vCPU 性能、鏈上活動與 RPC 限額都會影響結果。

| 部署方式 | 建議 CPU／記憶體 | SSD | 穩定可用頻寬 |
|---|---|---|---|
| 單機、先啟用 1–3 條鏈 | 2–4 vCPU、4 GiB | 30 GiB | 100 Mbps |
| 單機、啟用全部 9 條鏈 | 4–8 vCPU、8 GiB | 60 GiB | 200 Mbps，保留尖峰餘量 |
| 獨立協調器與 Bot | 2 vCPU、2–4 GiB | 30 GiB | 50–100 Mbps |
| 分散式 Worker，每臺數條鏈 | 2–4 vCPU、2–4 GiB | 20–40 GiB | 100 Mbps；重載鏈增加餘量 |

接管 Worker 須按全部鏈的負載配置。使用現代 CPU、穩定路由及足夠 RPC 配額，往往比單純增加候選 RPC 更有效。快取預設上限 256 MiB，可調大，但 systemd 記憶體上限還要留給解析、執行緒與資料庫。SSD 不需容納全鏈歷史，使用量主要由命中交易、保留天數和日誌決定。

## 開始部署

需要 Python 3.10+；正式服務使用 Linux systemd，TLS 憑證與整合測試使用 OpenSSL。

```sh
git clone https://github.com/kk311intl/telegram-multichain-transaction-monitor-bot-V2.git
cd telegram-multichain-transaction-monitor-bot-V2
python3 -m unittest discover -q
python3 build_release.py
```

1. 單機先用 [`examples/cluster.single.example.json`](examples/cluster.single.example.json)，多機用 [`examples/cluster.example.json`](examples/cluster.example.json)。把選定範例及 [`chains.example.json`](examples/chains.example.json) 複製到外部私有設定目錄。
2. 在 `cluster.json` 保留要啟用的鏈並分配節點；在 `chains.json` 調整 RPC、確認距離、原生幣最低金額及市場門檻。範例中的公共 RPC 是候選，不保證容量或一直可用。
3. 只在協調器設定 `BOT_TOKEN`、`OWNER_USER_ID`。為各 Worker 簽發獨立憑證；同機也可透過本機 HTTPS 通信。
4. 按 [部署文件](docs/DEPLOYMENT.md) 安裝、設定權限並啟動相應角色。程式升級與實際設定分離。

| 文件 | 內容 |
|---|---|
| [部署與傳輸](docs/DEPLOYMENT.md) | 單機／多機、TLS、systemd、擴容、升級與回復 |
| [客製化設定](docs/CUSTOMIZATION.md) | 標題、時區、使用政策、通知速度、快取、保留期限及鏈參數 |
| [執行與恢復](docs/RUNTIME.md) | 租約、卡死處理、RPC 優選與冷卻、過濾、Telegram 排程 |

## Bot 使用

未授權用戶發送 `/start`，把收到的用戶 ID 交給所有者。所有者透過 `/user` 授權；普通用戶命令選單只展示 `/start`，仍可呼叫 `/info` 查看技術設定。

主選單提供「新增地址、地址管理、運行狀態、過濾記錄、備份管理、使用說明」。地址新增時驗證格式；狀態依鏈創世時間排列，以簡稱和等寬欄位展示。匯入／匯出只處理自己的地址，不能改寫其他用戶或部署設定。

通知預設每帳戶至少相隔 5 秒，主動互動優先；遇 429 仍遵守 Telegram 指定期限。非通知消息預設 10 分鐘清理、不置頂。單地址有效待發超過 20 筆時移除並保留提示，門檻可由部署者調整。

## 原始碼與授權

`cluster.py`、`lease_store.py` 負責協調；`benchmark.py` 是全鏈掃描引擎；`scanner_adapters.py`、`monitor/` 處理鏈與 RPC；`bot_runtime/` 分離 Telegram、儲存、交易核對及過濾。`release-manifest.json` 明列可封裝的檔案。

採用 [GPL-3.0-only](LICENSE)。文件及 Bot 介面以中文提供；授權條款保留官方原文。個人設定、憑證、執行資料及 `personal/` 不納入 Git。**公開版本保留 Bot 地址匯入／匯出，不包含伺服器定時／異機備份系統。**

公共 RPC 與免費查詢 API 的完整性、容量及可用性不由本程式保證。風險過濾只是輔助信號，兩項查詢均無法判定時會標示提醒；沒有穩定幣白名單。Telegram 發送與本地 SQLite 不具跨系統原子性，不承諾通知恰好一次。
