# Gabagool V4 — Production Complete-Set Accumulation Bot

V4 is the executable bot built from the completed **12,921,881 exact on-chain execution** decode of the reference account.

The strategy is not the old V2 `$0.97` post-only spread farmer. The reconstructed behavior is a **maker-dominant mixed maker/taker inventory engine** that buys both complementary outcomes asynchronously, controls aggregate cost basis and inventory imbalance, continues maker participation into the final seconds, uses selective aggressive BUYs to repair deficient inventory, then settles complete sets after close.

`STRATEGY.md` contains the evidence boundary. Unobservable cancel/queue behavior remains a parameterized implementation choice; measured execution behavior is frozen into the policy.

## Production architecture

```text
current BTC/ETH 5m + 15m Up/Down markets
                  |
                  v
        real order books + wallet state
        - UP/DOWN shares + VWAP
        - imbalance ratio
        - opposite-fill lag
        - seconds to close
                  |
                  v
          execution controller
          /                 \
 post-only maker BUY     FAK taker BUY
      dominant           deficient repair
          \                 /
           aggregate basis guard
                  |
          tight inventory balance
                  |
               close
                  |
        settlement queue
        MERGE -> REDEEM
```

## What V4 fixes

- removes the disproven permanent `UP + DOWN <= 0.97` rule;
- keeps maker execution dominant while permitting evidence-backed taker repair;
- starts maker inventory skew before imbalance becomes large;
- restricts taker activity to the deficient leg and requires meaningful stale-leg/imbalance evidence;
- suppresses aggressive repair near expiry while maker orders continue until ~T-2s;
- uses aggregate side VWAP/cost basis rather than historical maximum fill price;
- settles predominantly after close rather than merging every few seconds;
- records authoritative maker/taker execution events from confirmed account trades;
- reconstructs real wallet positions after a restart instead of assuming an empty account;
- cancels pre-existing untracked orders before a live process begins;
- resumes active conditions from recovered shares and average cost;
- quarantines old/unknown positions into the settlement queue;
- rate-limits MERGE/REDEEM retries and requires holdings proof before exposure is cleared;
- binds the merge proof to the actual wallet and requires it to be recent;
- performs graceful shutdown with `cancel_all` so resting orders are not orphaned.

## Default market universe

The production default is the strongest reconstructed reference universe:

```yaml
assets: [btc, eth]
durations: [300, 900]
```

SOL/XRP remain supported by discovery but are opt-in rather than silently expanding the strategy beyond the core evidence set.

## Capital controls

`config/default.yaml` contains deployment limits, not claims about the reference account's bankroll. The default caps are intentionally conservative and can scale the same policy down:

```yaml
per_window_cap_usd: 250
global_exposure_cap_usd: 1000
max_concurrent_windows: 4
```

The historical representative BTC 5m market acquired roughly $1,357 gross inventory, so these defaults will produce a smaller deployment than the reference account.

## Install

Python 3.11+ is required.

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```

## Credentials

Copy `.env.example` to `.env` and provide the funding-wallet credentials. Never commit the real `.env` file.

```bash
cp .env.example .env
set -a; source .env; set +a
```

Required:

```text
POLY_PRIVATE_KEY
POLY_WALLET
POLY_RELAYER_API_KEY
POLY_RELAYER_API_KEY_ADDRESS
```

The SDK verifies that the private key binds the configured Polymarket funding wallet before the bot can start.

## Live gate

The production process requires a recent split/merge proof for the **same bound wallet**:

```bash
python -m tools.check_setup
python -m tools.test_merge
```

`tools.test_merge` performs a ~$1 split -> merge round trip and writes `.merge_proof`. V4 refuses live startup if the proof is missing, older than seven days, belongs to another wallet, or has no merge transaction hash.

There is no command-line bypass for this gate in V4.

## Run the actual bot

```bash
python -m src.main --live
```

At live startup V4 performs this sequence before any new strategy order:

1. bind and verify the funding wallet;
2. verify wallet-matched merge proof;
3. ensure trading/settlement approvals;
4. inspect and cancel all pre-existing untracked CLOB orders;
5. reconstruct real open positions from the official account data;
6. seed current capital exposure from those positions;
7. resume still-live configured conditions from recovered UP/DOWN inventory;
8. place old/resolved positions into the settlement queue;
9. begin current-market maker/taker execution.

If available pUSD is below the configured trading floor while recovered inventory exists, the process switches to **settlement-only** instead of opening new positions.

## Dry run

```bash
python -m src.main --dry-run
```

Dry-run is a plumbing test with deterministic synthetic fills. It does not submit orders and its P&L/fill ratios are not intended to reproduce historical market behavior.

## Runtime invariants

- routine CLOB execution is BUY-only;
- maker orders are post-only;
- taker repair uses FAK BUYs with a hard maximum price;
- confirmed account trades are the authoritative fill source;
- disappeared/cancelled orders are never inferred to be fills;
- aggregate combined basis is guarded;
- the heavy side is not aggressively repaired;
- settlement does not clear capital exposure until real holdings prove it;
- shutdown cancels every remaining order.

This repository implements the reconstructed mechanism. It does not imply guaranteed profitability: queue priority, other participants, fee/rebate economics and current liquidity can differ from the historical reference period.
