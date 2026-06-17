# 三機隊部署指南(白話版)

目標:**面板(hub)放 5090**(配你既有的 cloudflared 隧道 + 網域),5050 與 AWS 各自直連交易所、各自錄製,並把「輕量指標」推回 5090 面板做比較;Parquet 定期彙整到 R2/S3。

> 🔑 鐵則再提醒:**行情擷取每台都直連交易所、不經網域**(經過會增加延遲)。網域/隧道只用在「看面板 + 推指標 + 搬資料」這條冷路徑。Hub 放哪台**不影響**任何一台的擷取/錄製延遲。

---

## 0) 產生 token 並「存進檔案」(不放進指令或網址)

token 是面板鑰匙。**它不該出現在指令參數、shell 歷史或網址**,所以我們把它寫進一個檔案,
放在 repo 外面(git 看不到),三台各放一份**相同內容**的檔。

```powershell
# 在每台機器(這裡示範 5090):把同一把 token 寫到 repo 外的檔案
python -c "import secrets; print(secrets.token_urlsafe(32))" | Out-File -Encoding ascii C:\SheepNode\panel_token.txt
# 第一次在 5090 產生後,把這檔的內容複製到 5050 / AWS 各自的 panel_token.txt(內容要一樣)
# Linux(AWS): echo '貼上同一串' > ~/panel_token.txt && chmod 600 ~/panel_token.txt
```
> 指令裡只會出現「檔案路徑」(`C:\SheepNode\panel_token.txt`),那不是祕密;真正的 token 只在檔案裡。

---

## 1) 5090 = Hub(面板 + 自己也錄)

```powershell
# 在 5090(Windows)
pip install aiohttp pyarrow
python -m bbo_collector.panel_server --exchanges all --out data\bbo --port 8800 --node-id 5090-home --token-file C:\SheepNode\panel_token.txt
```

接著把面板接上網域(用你**既有**的隧道,不要新開):
1. 編輯 `C:\Users\<you>\.cloudflared\config.yml`,**新增**一條 ingress(見 [cloudflared-panel.yml](cloudflared-panel.yml)),保留原本 SheepNode 的條目,catch-all 404 留最後。
2. 設 DNS:`cloudflared tunnel route dns <TUNNEL_ID> panel.<你的網域>`
3. 重啟 cloudflared 服務。

完成後面板網址 = `https://panel.<你的網域>`。

> ⚠ 這跟你 SheepNode 正式站是**不同 port(8800)+ 不同子域**,互不影響;重啟面板不會動到 SheepNode。

---

## 2) 5050 與 AWS = Spoke(各自錄 + 推指標回 5090)

兩台都這樣跑(換 `--node-id`),它們會**各自直連交易所、各自在本地錄 Parquet**,同時每 5 秒把延遲/健康摘要推回 5090 面板:

```bash
# 5050 筆電(token 從檔案讀,不進指令)
python -m bbo_collector.panel_server --exchanges all --out data/bbo --node-id 5050-laptop --hub-url https://panel.<你的網域> --token-file C:\SheepNode\panel_token.txt

# AWS 東京(Linux)
python3 -m bbo_collector.panel_server --exchanges all --out ~/data/bbo --node-id aws-tokyo --hub-url https://panel.<你的網域> --token-file ~/panel_token.txt
```

> 只想比延遲、不想在那台跑採集器?改用零依賴的 latency 探針(一樣讀檔):
> `python node_agent.py --panel-url https://panel.<你的網域> --node-id aws-tokyo --token-file ~/panel_token.txt`

AWS 設成常駐(systemd)範例見本專案 README;5090/5050 用你既有看門狗或 NSSM。

---

## 3) 看面板(登入頁,網址不帶 token)

瀏覽器開:`https://panel.<你的網域>`
→ 沒登入會自動導到 `/login`,在密碼欄貼上你的 token(用 POST 送出,**不會進網址/歷史**),按 unlock。
→ 之後靠 cookie 自動登入(30 天),直接開 `https://panel.<你的網域>` 即可。
→ 「Device comparison」逐交易所綠底標出 **RTT 最低的機器**,並顯示每台 updates/s、live、uptime、drops。

(進階)想用 Email/SSO 登入而不用記 token:Cloudflare Zero Trust → Access → 對 `panel.<你的網域>` 建 policy。
**但要把 `/api/ingest` 設成 Bypass**(否則 5050/AWS 推不進來);app 層 token 仍保護它,所以 CF Access 是可選的額外一層。

---

## 4) Parquet 彙整到 R2(回測一起分析)

1. Cloudflare → R2 → 建 bucket(例 `bbo-archive`)→ 建 API Token(取得 Access Key Id / Secret)。
2. 每台機器各自上傳到**自己的 prefix**(避免撞檔),裝 `pip install boto3`:

```bash
# 例:5090
set AWS_ACCESS_KEY_ID=<R2_KEY> & set AWS_SECRET_ACCESS_KEY=<R2_SECRET>
python sync_parquet.py --root data/bbo --bucket bbo-archive --prefix node=5090 \
  --endpoint-url https://<ACCOUNT_ID>.r2.cloudflarestorage.com --interval 300
# AWS 那台 --prefix node=aws,5050 那台 --prefix node=5050
```

先 `--dry-run` 看會傳什麼;確定後拿掉。要省本地空間可加 `--delete-after`。
回測時三台資料都在 `s3://bbo-archive/node=*/exchange=*/...`,用交易所的 update-id 對齊就能比「誰先看到價格」。

---

## 5) 同步(時鐘)── 比較才準

要比「誰先看到同一筆價格」,三台時鐘要對齊:
- **AWS**:用 Amazon Time Sync(`169.254.169.123`),`chronyc tracking` 看偏移(通常 <1ms)。
- **5090 / 5050(Windows)**:`w32tm /resync`、`w32tm /stripchart /computer:time.google.com /samples:5`。
- 網域對時鐘**毫無幫助**——這是獨立的一步。

---

## 安全檢查清單(對外前必看)
- [ ] 三台都用 `--token-file`(token 在檔案、**不在指令也不在網址**),面板啟動訊息顯示 `Auth: ON`。
- [ ] `panel_token.txt` 放在 repo 外、別 `git add`;Linux 設 `chmod 600`。
- [ ] 瀏覽器走 `/login` 登入頁(token 用 POST 送、不進網址/歷史)。
- [ ] 未設 token 時**只准綁 localhost**,別對外。
- [ ] `/api/ingest` 已被 token header 保護(別讓任何人灌假資料)。
- [ ] R2/S3 金鑰只放需要的機器,別進 git。
- [ ] (可選)Cloudflare Access 加在瀏覽器路徑、把 `/api/ingest` 設 Bypass。
