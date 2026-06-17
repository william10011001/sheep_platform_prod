# 三機隊部署指南(白話版)

目標:**面板(hub)放 5090**(配你既有的 cloudflared 隧道 + 網域),5050 與 AWS 各自直連交易所、各自錄製,並把「輕量指標」推回 5090 面板做比較;Parquet 定期彙整到 R2/S3。

> 🔑 鐵則再提醒:**行情擷取每台都直連交易所、不經網域**(經過會增加延遲)。網域/隧道只用在「看面板 + 推指標 + 搬資料」這條冷路徑。Hub 放哪台**不影響**任何一台的擷取/錄製延遲。

---

## 0) 先產生一把 token(三台共用同一把)

這把 token 就是面板的「鑰匙」。隨便產一串長亂碼,三台都用它:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
# 例: 7Qн...（複製這串,以下用 <TOKEN> 代表）
```

---

## 1) 5090 = Hub(面板 + 自己也錄)

```powershell
# 在 5090(Windows)
pip install aiohttp pyarrow
$env:BBO_PANEL_TOKEN="<TOKEN>"
python -m bbo_collector.panel_server --exchanges all --out data\bbo --port 8800 --node-id 5090-home
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
# 5050 筆電
set BBO_PANEL_TOKEN=<TOKEN>   &  python -m bbo_collector.panel_server --exchanges all --out data/bbo --node-id 5050-laptop --hub-url https://panel.<你的網域> --token <TOKEN>

# AWS 東京(Linux)
BBO_PANEL_TOKEN=<TOKEN> python3 -m bbo_collector.panel_server --exchanges all --out ~/data/bbo --node-id aws-tokyo --hub-url https://panel.<你的網域> --token <TOKEN>
```

> 只想比延遲、不想在那台跑採集器?改用零依賴的 latency 探針:
> `python node_agent.py --panel-url https://panel.<你的網域> --node-id aws-tokyo --token <TOKEN>`

AWS 設成常駐(systemd)範例見本專案 README;5090/5050 用你既有看門狗或 NSSM。

---

## 3) 看面板

瀏覽器開:`https://panel.<你的網域>/?token=<TOKEN>`
→ 第一次帶 `?token=` 會自動種 cookie,之後直接開 `https://panel.<你的網域>` 即可。
→ 「Device comparison」會逐交易所綠底標出 **RTT 最低的機器**,並顯示每台的 updates/s、live、uptime、drops。

(進階)想再加一層登入頁:Cloudflare Zero Trust → Access → 對 `panel.<你的網域>` 建 policy。
**但要把 `/api/ingest` 這條路徑排除**(或改用 service token),否則 5050/AWS 推不進來。app 層的 token 已經保護了所有路徑,所以 CF Access 是「可選的額外一層」。

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
- [ ] 三台都帶 `--token <TOKEN>`(或 `BBO_PANEL_TOKEN`),面板啟動訊息要顯示 `Auth: ON`。
- [ ] 未設 token 時**只准綁 localhost**,別對外。
- [ ] `/api/ingest` 已被 token 保護(別讓任何人灌假資料)。
- [ ] R2/S3 金鑰只放需要的機器,別進 git。
- [ ] (可選)Cloudflare Access 加在瀏覽器路徑、排除 `/api/ingest`。
