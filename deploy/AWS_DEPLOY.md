# AWS 東京採集 + 回拉 5090 回測（程式碼走 R2，非 git）

架構:**AWS東京** 常駐採集(精選幣 L1)→ 本地 Parquet → 每 5 分鐘同步到 **R2 私有桶** → 刪本地;
**5090** 每 15 分鐘從 R2 拉新資料 → 本地長存 → 跑回測。

你的 Cloudflare 帳號 ID = `a435a40ecc31431591a3e691767b485c`,
所以你的 **R2 端點** = `https://a435a40ecc31431591a3e691767b485c.r2.cloudflarestorage.com`

---

## A. R2 準備(一次性)

1. **建私有桶放資料**:Cloudflare → R2 → 建立貯體 → 名稱 `bbo-data`(這個**不要**接公開網域,保持私有)。
2. **建 API 權杖(boto3 要用)**:R2 → Manage R2 API Tokens → Create → 權限 **Object Read & Write** → 建立。
   - 記下 **Access Key ID** 與 **Secret Access Key**(只顯示一次)。
3. **把程式包上傳到公開桶**:R2 → `sheepnode-mining-data`(公開那個)→ Upload → 拖入 `bbo_aws_bundle.tar.gz`(在你桌面)。
   - 確認 `https://data.sheepnode.com/bbo_aws_bundle.tar.gz` 開得到。

---

## B. AWS 東京機器(常駐採集 + 同步)

開正式機(東京 `ap-northeast-1`,**c7g.large**,Ubuntu,30GB gp3),用 EC2 Instance Connect 進終端機,貼(把金鑰換成你的):

```bash
curl -O https://data.sheepnode.com/bbo_aws_bundle.tar.gz
tar xzf bbo_aws_bundle.tar.gz
cd bbo_aws_bundle
sudo R2_ENDPOINT="https://a435a40ecc31431591a3e691767b485c.r2.cloudflarestorage.com" \
     R2_BUCKET="bbo-data" \
     AWS_ACCESS_KEY_ID="你的R2_AccessKeyID" \
     AWS_SECRET_ACCESS_KEY="你的R2_Secret" \
     NODE_ID="aws-tokyo" \
     bash setup_aws.sh
```

跑完它會:裝好環境、把採集器與同步設成 **systemd 常駐(開機自動跑、掛掉自動重啟)**。
驗證:
```bash
systemctl status bbo-collector bbo-sync     # 兩個都要 active(running)
journalctl -u bbo-collector -f              # 看採集即時 stats(Ctrl+C 離開)
chronyc tracking                            # 確認時鐘對時(延遲套利要)
```
幾分鐘後到 R2 `bbo-data` 桶,應看到 `node=aws-tokyo/exchange=.../...parquet` 開始進來。

---

## C. 5090(把資料拉回來長存)

在 5090 開 PowerShell:
```powershell
pip install boto3
cd C:\SheepNode\bbo
git pull   # 或一樣從 R2 抓最新 pull_from_r2.py
$env:AWS_ACCESS_KEY_ID="你的R2_AccessKeyID"
$env:AWS_SECRET_ACCESS_KEY="你的R2_Secret"
python pull_from_r2.py --root data\from_aws --bucket bbo-data --prefix node=aws-tokyo --endpoint-url https://a435a40ecc31431591a3e691767b485c.r2.cloudflarestorage.com --interval 900
```
先加 `--dry-run` 看會拉什麼,沒問題再拿掉。`--interval 900` = 每 15 分鐘自動拉一次(視窗開著就持續)。
要常駐可包成 Windows 排程工作。

---

## D. 在 5090 回測
資料累積一陣後:
```powershell
cd C:\SheepNode\bbo
python arb_deep.py --root data\from_aws --minutes 0
notepad arb_deep_report.txt
```

---

## 重點 / 省錢
- **測試用的新加坡/大阪/小機器記得 Terminate**,只留東京 c7g.large。
- sync 有 `--delete-after`,AWS 本地不會塞滿(只暫存 ~5 分鐘),30GB 綽綽有餘。
- R2 **下載(egress)免費**,所以 5090 拉資料不花錢;只付 R2 儲存(很便宜)。
- 程式包更新版本時:重新打包上傳 R2,AWS 重抓 + `tar xzf` + `sudo bash setup_aws.sh`(會覆蓋並重啟服務)。
