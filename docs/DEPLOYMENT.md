# 部署與傳輸

每個部署使用自己的節點名稱、主機、用戶 ID、憑證和 RPC 設定。範例網段屬文件保留位址，不能當成實際主機。

## 選擇部署方式

- **單機**：使用 `examples/cluster.single.example.json`，一臺機器同時啟動協調器及 `primary` Worker；範例只啟用 Ethereum，可自行加入其餘鏈並提高 `max_chains`。
- **多機**：使用 `examples/cluster.example.json`，在每臺 Worker 的 `cluster.env` 設定不同的 `CLUSTER_NODE_ID`。只在協調器主機啟動 coordinator 服務，其餘只啟動 worker；需要時同機啟動兩個角色。
- **獨立協調器**：協調器不必列入 `nodes`；該清單只描述 Worker。`primary_node` 是接管掃描的 Worker，不代表協調器所在主機。Bot、交易核對和 SQLite 目前與協調器共同運行。

`cluster.json.chains` 是唯一的啟用鏈清單；`chains.json.chains` 是鏈設定目錄，可保留未啟用項目。未啟用鏈不排程、不在 Bot 中列出；既有地址資料不因停用而刪除。啟用鏈必須存在於鏈設定目錄中。把鏈的 `preferred_node` 設為期望執行它的 Worker，將各節點 `max_chains` 設為足夠容納分配數量；`primary_node` 的容量不得小於全部啟用鏈數。

TLS 模式以每節點憑證識別身分，`ip_address` 可省略，允許同一 NAT 出口；同機多 Worker 需各自的憑證、節點名稱、服務單元及獨立 `--state-dir`。一般同機只需要一個 Worker，可同時管理多條鏈。

## 檔案分工

| 位置 | 內容 |
|---|---|
| `/opt/crypto-monitor-v2` | 共用程式發行目錄的 symlink |
| `/etc/crypto-monitor-v2/cluster.json` | 節點、`primary_node`、容量及鏈分配 |
| `/etc/crypto-monitor-v2/chains.json` | RPC、鏈識別、確認距離及過濾參數 |
| `/etc/crypto-monitor-v2/cluster.env` | 此機節點名稱、協調器 URL、綁定位址 |
| `/etc/crypto-monitor-v2/coordinator-tls.env` | 協調器服務端憑證路徑 |
| `/etc/crypto-monitor-v2/worker-tls.env` | 此 Worker 客戶端憑證路徑 |
| `/etc/crypto-monitor-v2/bot.env` | 僅協調器：Bot Token、所有者 ID、快取容量 |
| `/var/lib/crypto-monitor-v2-coordinator/` | 租約與 Bot SQLite |
| `/var/lib/crypto-monitor-v2/worker/` | Scanner 租約、游標、RPC 快取及日誌 |

`examples/` 只有假資料和空憑證欄位。從範例複製配置到上述外部位置，程式升級不覆寫它們。服務名稱中的 v2 為相容部署識別；Bot UI 不展示此版本字樣。

## 公網雙向 TLS

Worker 的心跳、地址名單與命中回報共用 `CLUSTER_COORDINATOR=https://coordinator.example.org:18765`，直接走公網，服務單元不依賴任何 VPN 或 Tailscale。同機 Worker 可連本機 HTTPS 位址，但憑證 SAN 必須包含該位址。鏈 RPC 也直接連各鏈供應商。TLS 失敗不自動降成 HTTP，重導向被拒絕，不套用系統 HTTP proxy。

協調器要求由私有 CA 簽發的客戶端憑證，憑證中唯一 CN 必須等於 `cluster.json` 的節點名稱，且必須等於請求中的 `node`；通過 TLS 也不能冒用另一節點或租約。客戶端驗證協調器 CA、有效期與 SAN 主機名。最低 TLS 1.2，16 個併發連線、5 秒連線讀取／握手期限，避免慢握手阻塞主接受循環；不構成完整網際網路 DDoS 防護，仍應使用防火牆。

憑證示例（在私有操作目錄執行，不在原始碼目錄執行）：

```sh
# 私有 CA；CA 私鑰離線保存，不分發到 Worker。
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -keyout ca.key -out ca.crt -subj /CN=MonitorCA

# 協調器；替換 SAN 為真實公網網域。
openssl req -newkey rsa:2048 -nodes -keyout coordinator.key -out coordinator.csr -subj /CN=coordinator
printf 'subjectAltName=DNS:coordinator.example.org\nextendedKeyUsage=serverAuth\n' > coordinator.ext
openssl x509 -req -in coordinator.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 90 -extfile coordinator.ext -out coordinator.crt

# 每個節點獨立簽發；CN 必須等於該節點的設定名稱。
openssl req -newkey rsa:2048 -nodes -keyout worker.key -out worker.csr -subj /CN=worker-1
printf 'extendedKeyUsage=clientAuth\n' > worker.ext
openssl x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 90 -extfile worker.ext -out worker.crt
```

同機若同時跑協調器與 Worker，也分開配置服務端與客戶端憑證。憑證 CA 可供兩個服務帳戶讀取；服務端 key 僅 `crypto-monitor-v2-coordinator` 可讀，Worker key 僅 `crypto-monitor-v2` 可讀，例如 root 擁有、對應群組、0640。`bot.env` 只由 systemd 讀取，0600 root 擁有，不給 Worker。憑證路徑由環境檔指定；更新憑證後重啟使用它的程序，SSL context 在程序內快取。沒有自動簽發／續期或 CRL 撤銷機制，必須另行監控有效期及規劃輪換。

Linux 主機與雲端防火牆需放行 HTTPS 叢集埠，宜再限制實際節點出口 IP；勿在公網放行明文相容模式。`cluster.json` 的 `ip_address` 只在明文相容模式必填，且每個節點必須不同；TLS 模式可省略。

單機可把 `CLUSTER_BIND` 設為 `127.0.0.1`，`CLUSTER_COORDINATOR` 設為 `https://localhost:18765`，服務端憑證 SAN 使用 `DNS:localhost`，客戶端憑證 CN 使用單機範例的 `primary`。這種配置不需向公網開放叢集埠。多機則改用真實公網域名和相應 SAN，不能停用主機名驗證。

## 安裝與資源

先本機執行測試及 `python build_release.py`，取得 SHA-256，再自行將 `dist/crypto-monitor-v2.tar.gz` 上傳。校驗雜湊後以 root 執行封裝中的 `install_remote.sh`，提供 `V2_RELEASE=release-時間戳-雜湊前8碼` 與 `V2_ARCHIVE`。安裝器建立服務帳戶、驗證程式與測試、切換程式 symlink、安裝服務單元，但不自動啟動服務。完整 TLS 測試需 OpenSSL，缺少時該整合測試會略過。

首次安裝先填妥外部設定、憑證、檔案權限與防火牆，再啟用協調器與 Worker。`deploy_node_remote.sh` 是可選 Worker 安裝工具，資源參數由呼叫者提供；它不是通用的資料庫遷移工具，也不自動配送私密鏈設定或憑證。

以下在已取得原始碼及本機構建產物的 Linux 主機執行；遠端安裝時把同一封裝、雜湊及安裝腳本傳到該機。`build_release.py` 輸出的第一行是發行名稱，填入 `V2_RELEASE`：

```sh
cd dist
sha256sum -c crypto-monitor-v2.tar.gz.sha256
cd ..
sudo env V2_ARCHIVE="$PWD/dist/crypto-monitor-v2.tar.gz" \
  V2_RELEASE=release-YYYYMMDDTHHMMSSZ-12345678 bash install_remote.sh
```

安裝後將範例複製至 `/etc/crypto-monitor-v2/`，去掉檔名中的 `.example`；單機／多機範例都存成 `cluster.json`。`cluster.env` 填節點名稱、協調器 URL、綁定位址；TLS 路徑放在角色各自的環境檔，不在共用 `cluster.env` 同時放兩個角色的私鑰。`bot.env` 填有效的 Token 與正整數所有者 ID，只放協調器。Worker 主機只配置自己的 TLS 環境檔。

```sh
# 外部設定由 root 管理；Bot Token 只讓 root/systemd 讀取。
sudo chmod 600 /etc/crypto-monitor-v2/*.env
# 若鏈 RPC 含密鑰，建立只供兩個服務帳戶讀取的群組或 ACL，
# 不要為了省事將實際 chains.json 設為所有使用者可讀。

# 單機啟動兩個服務；多機只啟動本機承擔的角色。
sudo systemctl enable --now crypto-monitor-v2-coordinator
sudo systemctl enable --now crypto-monitor-v2-worker
sudo systemctl status crypto-monitor-v2-coordinator crypto-monitor-v2-worker
sudo journalctl -u crypto-monitor-v2-worker -n 50 --no-pager
sudo python3 /opt/crypto-monitor-v2/cluster.py status \
  --db /var/lib/crypto-monitor-v2-coordinator/cluster.sqlite3
```

成功啟動後應看到各 Worker 心跳、每條啟用鏈的租約及持續前進的游標，再在 Telegram 測試 `/start`。全鏈 RPC 必須能取完整交易區塊與未指定地址的 Transfer 日誌；只支援查單一地址的端點不足以支援本方案。

協調器快取預設 256 MiB，可由 `RECENT_CACHE_MIB` 調整。systemd 記憶體／CPU 上限使用 drop-in，例如 `examples/coordinator-resources.conf.example`；需保留程序、RPC 回應和資料庫使用空間，不能把記憶體上限設成剛好等於快取上限。大型全鏈下載的 CPU、頻寬及 RPC 容量須自行量測，三節點範例不是容量保證。

## 擴容、升級與回復

增加 Worker 時先簽發其獨立憑證、填妥外部設定，將節點加入所有主機的同一份 `cluster.json`，調整 `preferred_node`／容量後重啟協調器及受影響 Worker。修改啟用鏈清單也採同樣流程。舊租約會先 drain，待期限和緩衝結束才接手；接手從當時鏈頭開始，切換空窗不補歷史交易。

故障恢復的 Worker 不自動搶回已轉移工作。需要恢復原分工時，在協調器主機執行（替換節點名稱）：

```sh
sudo python3 /opt/crypto-monitor-v2/cluster.py \
  --cluster-config /etc/crypto-monitor-v2/cluster.json rebalance \
  --db /var/lib/crypto-monitor-v2-coordinator/cluster.sqlite3 --node worker-1
```

程式升級先核對封裝雜湊並保留原發行目錄、外部設定及一致的 SQLite 快照，再執行安裝器、重啟本機角色。安裝器切換 symlink，但不會重啟已在執行的程序。回復時指回原發行目錄，再重啟相應服務；若升級包含不相容 schema 變更，資料庫亦須配合一致快照恢復。不要直接複製執行中的 SQLite 主檔而忽略 WAL；快照／伺服器備份由部署者自行管理。

需要從私網明文切換至 TLS 時：

1. 保留既有設定及一致 SQLite 快照；不要把個人快照上傳到公開倉庫。
2. 將原程式目錄內的實際 `cluster.json`、`chains.json` 搬入 `/etc/crypto-monitor-v2/`，不能以公開範例覆蓋真實配置。
3. 先簽發並離線校驗每節點憑證，確認服務帳戶可讀取；準備新 HTTPS 環境檔與防火牆規則。
4. 排定切換，更新協調器與 Worker 後核對憑證身分、心跳、租約、游標前進及權限拒絕。不能先把 Worker 指向仍提供 HTTP 的協調器。
5. 發生故障，還原舊程式 symlink、舊設定／環境檔再重啟。任何重啟仍從當時最新區塊開始，不補停機舊交易。

舊私網 HTTP 必須明確設定 `CLUSTER_ALLOW_PLAINTEXT=1`，僅在受控私網使用來源 IP 授權；同時存在 TLS 配置時禁止客戶端降級。這是舊部署相容功能，不是公網方案。

Bot 用戶備份只處理該用戶自己的地址。伺服器定時／異機備份、SSH 備份帳戶及離線恢復工具不屬公開程式，部署者自行管理。
