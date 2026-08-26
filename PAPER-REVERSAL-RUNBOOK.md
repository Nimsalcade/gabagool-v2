# BTC 5-Minute Reversal Complete-Set — Paper-Live Runbook

This mode reads the real Polymarket BTC 5-minute order books and simulates fills. It never sends an order, cancel, merge, redeem, approval, or wallet transaction.

## Strategy locked in this runner

1. BTC 5-minute markets only.
2. One outcome midpoint must first reach at least 65c.
3. The same outcome must later fall to 40c or below and be the cheaper side.
4. The setup is then armed for the rest of that five-minute market.
5. A paper trade occurs only when the same snapshot shows enough best-ask size on both UP and DOWN and the fee-adjusted pair cost is at most 97c.
6. Paper capital per market is capped at $100 from a $1,000 starting balance.
7. The simulated UP and DOWN quantities are equal. Paper mode immediately credits $1 per matched pair as the complete-set merge value.
8. A condition ID can be paper-traded only once, including after process restarts.

## VPS start

```bash
cd ~/gabagool-v2
git pull
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.paper_reversal_pair --config config/reversal_pair_paper.yaml
```

No `.env`, private key, wallet, relayer key, or merge proof is required for paper-live mode.

## Keep it running with systemd

Create `/etc/systemd/system/gabagool-reversal-paper.service`:

```ini
[Unit]
Description=Gabagool BTC 5m reversal complete-set paper-live
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=YOUR_LINUX_USER
WorkingDirectory=/home/YOUR_LINUX_USER/gabagool-v2
ExecStart=/home/YOUR_LINUX_USER/gabagool-v2/.venv/bin/python -m src.paper_reversal_pair --config config/reversal_pair_paper.yaml
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now gabagool-reversal-paper
journalctl -u gabagool-reversal-paper -f
```

Paper trades are appended to:

```text
data/reversal_pair_paper.csv
```

The CSV stores the reversal state, executable asks and sizes, fee rate, simulated matched shares, total cost, complete-set value, locked paper profit, ROI, and running paper balance.
