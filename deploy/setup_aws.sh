#!/usr/bin/env bash
set -euo pipefail

R2_ENDPOINT="${R2_ENDPOINT:?set R2_ENDPOINT e.g. https://ACCOUNTID.r2.cloudflarestorage.com}"
R2_BUCKET="${R2_BUCKET:-bbo-data}"
R2_KEY="${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID (R2 token access key id)}"
R2_SECRET="${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY (R2 token secret)}"
NODE_ID="${NODE_ID:-aws-tokyo}"
EXCHANGES="${EXCHANGES:-all}"
QUOTES="${QUOTES:-USDT,USDC,USD,BTC,ETH}"
SYNC_INTERVAL="${SYNC_INTERVAL:-300}"

APP=/opt/bbo
DATA=/data/bbo
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "[1/6] system packages"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip chrony
sudo systemctl enable --now chrony 2>/dev/null || sudo systemctl enable --now chronyd 2>/dev/null || true

echo "[2/6] app dir + code"
sudo mkdir -p "$APP" "$DATA"
sudo cp -r "$HERE/bbo_collector" "$HERE/sync_parquet.py" "$HERE/coins_liquid.txt" "$APP/"

echo "[3/6] python venv + deps"
sudo python3 -m venv "$APP/venv"
sudo "$APP/venv/bin/pip" install --upgrade pip >/dev/null
sudo "$APP/venv/bin/pip" install aiohttp pyarrow boto3 >/dev/null

echo "[4/6] credentials env file"
sudo tee "$APP/bbo.env" >/dev/null <<EOF
AWS_ACCESS_KEY_ID=$R2_KEY
AWS_SECRET_ACCESS_KEY=$R2_SECRET
EOF
sudo chmod 600 "$APP/bbo.env"

echo "[5/6] systemd services"
sudo tee /etc/systemd/system/bbo-collector.service >/dev/null <<EOF
[Unit]
Description=BBO collector (Tokyo)
After=network-online.target
Wants=network-online.target
[Service]
WorkingDirectory=$APP
ExecStart=$APP/venv/bin/python -m bbo_collector.main --exchanges $EXCHANGES --bases-file $APP/coins_liquid.txt --quotes $QUOTES --out $DATA
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/bbo-sync.service >/dev/null <<EOF
[Unit]
Description=BBO sync to R2
After=network-online.target
Wants=network-online.target
[Service]
WorkingDirectory=$APP
EnvironmentFile=$APP/bbo.env
ExecStart=$APP/venv/bin/python $APP/sync_parquet.py --root $DATA --bucket $R2_BUCKET --prefix node=$NODE_ID --endpoint-url $R2_ENDPOINT --interval $SYNC_INTERVAL --min-age 90 --delete-after
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF

echo "[6/6] enable + start"
sudo systemctl daemon-reload
sudo systemctl enable --now bbo-collector bbo-sync

echo ""
echo "DONE. node=$NODE_ID  exchanges=$EXCHANGES  data=$DATA  bucket=$R2_BUCKET"
echo "  status:   systemctl status bbo-collector bbo-sync"
echo "  logs:     journalctl -u bbo-collector -f"
echo "  clock:    chronyc tracking"
