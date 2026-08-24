#!/usr/bin/env bash
# Read-only forensic 15m paper harness on a fresh Ubuntu VPS.
# Does not load a wallet, sign, or send orders.
set -euo pipefail

ROOT="${ROOT:-/root/gabagool-v2-v5}"
SESSIONS="${SESSIONS:-96}"
PAPER_CASH="${PAPER_CASH:-2000}"
ASSETS="${ASSETS:-btc,eth}"
BACKEND="${BACKEND:-public_tape}"

if [[ $EUID -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi
if [[ ! -f "$ROOT/tools/run_forensic_15m_paper.py" ]]; then
  echo "code not found at $ROOT" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  ca-certificates curl git build-essential \
  python3 python3-venv python3-pip

if ! command -v uv >/dev/null 2>&1; then
  curl -fsSL https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi
export PATH="$HOME/.local/bin:$PATH"

uv python install 3.12
cd "$ROOT"
rm -rf .venv
uv venv --python 3.12 .venv
# shellcheck disable=SC1091
source .venv/bin/activate
uv pip install --python .venv/bin/python -r requirements.txt
python -m pytest -q

mkdir -p "$ROOT/data/live_logs"

cat >/etc/systemd/system/gabagool-15m-paper.service <<EOF
[Unit]
Description=Gabagool forensic 15m read-only paper harness
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT
Environment=PYTHONUNBUFFERED=1
Environment=PATH=$ROOT/.venv/bin:/usr/local/bin:/usr/bin
ExecStart=$ROOT/.venv/bin/python -u -m tools.run_forensic_15m_paper \\
  --assets ${ASSETS} \\
  --sessions ${SESSIONS} \\
  --paper-cash ${PAPER_CASH} \\
  --maker-fill-backend ${BACKEND}
Restart=on-failure
RestartSec=20
StandardOutput=append:$ROOT/data/live_logs/paper.systemd.log
StandardError=append:$ROOT/data/live_logs/paper.systemd.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now gabagool-15m-paper.service
sleep 2
systemctl --no-pager --full status gabagool-15m-paper.service || true
echo
echo "logs: journalctl -u gabagool-15m-paper -f"
echo "file: tail -f $ROOT/data/live_logs/paper.systemd.log"
