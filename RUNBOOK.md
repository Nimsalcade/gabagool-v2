# RUNBOOK — Gabagool v2

## Go-live checklist (every session)

1. `set -a; source .env; set +a`
2. `make check` — every line must say PASS. Do not proceed past a FAIL.
3. `.merge_proof` present and for **this** wallet (check_setup verifies; rerun
   `make merge-proof` if it's >72h old or after any credential change).
4. `make dry` for at least one full window. You should see: book reads,
   two-sided `rest BUY` quotes, simulated fills, `[dry-run] would merge`.
5. `make live`. First session: leave defaults (`per_window_cap_usd: 50`).
6. Watch the first real merge land: log line `MERGED n pairs -> $n pUSD | tx=…`
   and the same tx on polygonscan. **If the first 30 minutes produce fills but
   zero merges, stop (`Ctrl-C`) and investigate — do not let inventory build.**

## Stopping

* `Ctrl-C` once → graceful: cancels windows, cancels all orders, force-merges,
  prints the reconciliation report.
* Process killed hard? The heartbeat lapses and **the exchange cancels your
  orders within ~15s** (that's the dead-man's switch working). Then run
  `make positions` and, if pairs remain, `python -m tools.test_merge` style
  recovery isn't needed — just restart the bot or run `make redeem` after the
  windows resolve.

## Daily ops

* `make positions` — `[M·]` flag = mergeable now, `[·R]` = redeemable now.
* `make redeem` — sweeps every redeemable condition (gasless).
* Reconciliation: printed at shutdown; the "wallet Δ" and "internal" lines
  should explain each other. A growing unexplained gap = stop and audit
  `data/gabagool_v2.sqlite` (tables: fills, merges, redeems, balances).

## Troubleshooting — old failure → what it means now

| Symptom (old logs) | Meaning here | Action |
|---|---|---|
| `Expected at most two positions, got N` | Gone — merges are per-condition with `amount="max"` | n/a |
| `401 Unauthorized … polygon-rpc.com` | Gone — no self-managed RPC; relayer executes txs | n/a |
| `AddressEncoder … cannot encode` | Config validation now rejects malformed addresses at startup | Fix `.env` value it names |
| `relayer auth rejected` (new) | Wrong/missing Relayer API key | Recreate at polymarket.com → Settings → API Keys; key AND address |
| `missing approval from the funding wallet` (new) | Approvals not set for this wallet | `make check` (re-runs `setup_trading_approvals`) |
| `MARKET_NOT_READY` spam | Book not open; bot now backs off automatically | None — informational at DEBUG |
| `Size (4) lower than the minimum: 5` | Should never appear; sizes derive from the book's `min_order_size` | If seen, report — book metadata changed |
| Order vanished, no fill recorded | Correct behavior — it was a cancel (heartbeat lapse, post-only reject, our replace) | None |
| `merge submitted but positions did not shrink` | Relayer tx confirmed but chain state unchanged — serious | STOP. Check tx hash on polygonscan; verify wallet address; rerun `make merge-proof` before resuming |
| Kill switch fired | Cash equity fell >10% below session start while mostly in cash | Investigate ledger before restarting; this is not a transient error |

## Scaling rules

* Raise `per_window_cap_usd` only after ≥3 sessions where
  `merged $ ≈ fills $ × (1/combined_avg)` reconciles within a few percent.
* Never raise `combined_budget` above 0.98 — your entire edge is `1.00 − it`.
* `max_concurrent_windows: 2` covers BTC+ETH current windows; raising it adds
  the *next* windows too and doubles capital at work.

## Credential hygiene

* The EOA key signs orders and wallet batches. Keep it in `.env`/secret
  manager only; the code never logs it.
* Relayer API keys are revocable independently — rotate after any incident.
