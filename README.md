# Gabagool v2 — Pure Spread Farmer for Polymarket 15-Minute Up/Down Markets

A complete rewrite of the previous bot, built on Polymarket's **current (V2,
post-April 2026)** platform: pUSD collateral, CTF Exchange V2, gasless relayer
transactions, and the official unified `polymarket-client` SDK.

It implements one conservative replica of gabagool22's economic engine: acquire
**both** outcomes, merge matched complete sets back into $1.00 of pUSD, and
redeem winning residual inventory after resolution. No price prediction exists
anywhere in this codebase.

The forensic record does **not** establish this replica's exact quoting policy.
In particular, post-only GTC, fair-split prices, a strict per-quote $0.97 cap,
imbalance thresholds, and merge cadence are implementation choices rather than
measured parameters. See [`STRATEGY.md`](STRATEGY.md) for the locked evidence
baseline, including the 734 15-minute / 182 hourly market split, 94.7% aggregate
matchability, MERGE plus REDEEM exits, and corrected pair-count arithmetic.
UP and DOWN orders are managed independently because they rest in separate
queues and fill asynchronously, often partially. Share matching is an aggregate
inventory outcome used for MERGE, not a requirement that every fresh order have
the same size or that the two legs execute atomically.

---

## Why the old bot lost money (and what changed)

| # | Old behavior | Consequence | This rewrite |
|---|---|---|---|
| 1 | Merge always routed through the **NegRiskAdapter** | 15m Up/Down markets are *not* neg-risk → every merge failed | SDK `merge_positions()` routes standard vs neg-risk correctly via the pUSD collateral adapters |
| 2 | Fallback merge sent `CTF.mergePositions` **from the EOA** | Tokens live in your Polymarket *wallet contract*, not the EOA → guaranteed revert | All on-chain actions execute **from the funding wallet** through Polymarket's gasless relayer |
| 3 | Collateral hardcoded to **USDC.e**, V1 contracts | Platform migrated to **pUSD** + V2 exchanges on 2026-04-22 → wrong everything | All addresses verified against docs.polymarket.com June 2026 (`src/constants.py`) |
| 4 | Relayer client built with wrong constructor + corrupted `.env` value in the key-address field | `AddressEncoder` errors ×1,600 | Plain **Relayer API key** auth (Settings → API Keys); config validates the *shape* of every credential before starting |
| 5 | "Order missing from open-orders ⇒ fully filled" | Cancels booked as fills → phantom inventory → merges "not matching" → fictional PnL (+$661 internal vs −$186 wallet) | Fills come only from `size_matched` + the trades API; **a vanished order with no trades is a cancel, period** (`src/fills.py`) |
| 6 | Momentum **sniper** fired on Binance noise; 57/72 windows ended naked one-sided | Biggest losses: −$109, −$100, −$99 naked bags | Sniper deleted. SELL orders and spread-crossing are structurally impossible (`src/maker_loop.py`) |
| 7 | 16,500 × "425 service not ready"; sub-5-share rejects ×725 | Wasted hours, rejected orders | `accepting_orders` gate + per-market exponential backoff; sizes respect the book's own `min_order_size`/`tick_size` |
| 8 | No heartbeat | Orphan orders if the bot died | V2 heartbeat sidecar = dead-man's switch: if this process dies, the exchange cancels everything |
| 9 | Merge "success" never verified | $0 actually merged all day while logs claimed credits | Every merge is **verified on-chain** (positions must shrink) and recorded with its tx hash |

Intentionally **dropped** from the old repo: the sniper/spike detector, the
Rust pipeline, the backtest folder, and the builder-credential relayer code.
They were either the cause of losses or dead weight around it.

---

## How money flows

```
pUSD ──(small BUY fills arriving asynchronously)──▶ Up + Down shares
 ▲                                                   │
 └────────── merge_positions (gasless) ◀── matched pairs = locked $1.00
                       │
              naked remainder (≤ a few %) ──▶ redeemed after resolution
```

For each matched pair, gross edge = `1.00 − quantity-weighted acquisition
cost`; the replica's default quote budget is 0.97. Actual session PnL must also
include unmatched residuals and redemptions and is derived from the ledger, not
from a headline VWAP multiplication. The edge is realized only when the merge
lands. That's why `--live` is **gated on a real
on-chain merge proof** (see Quickstart step 4).

---

## Quickstart

```bash
# 0. install
make install                      # pip install -r requirements.txt
make test                         # 14 unit/integration tests must pass

# 1. credentials
cp .env.example .env              # fill in; then load:
set -a; source .env; set +a
#    POLY_PRIVATE_KEY            – EOA key that owns your Polymarket wallet
#    POLY_WALLET                 – your Polymarket wallet address (profile page)
#    POLY_RELAYER_API_KEY(+_ADDRESS) – polymarket.com → Settings → API Keys

# 2. fund: hold ≥ $20 pUSD in the wallet (deposit via polymarket.com as usual)

# 3. verify everything
make check                        # python -m tools.check_setup → all PASS

# 4. prove the exit works (≈$1, on-chain, ~1 min)
make merge-proof                  # split $1 → merge back; writes .merge_proof

# 5. watch it run without money
make dry                          # full pipeline against live books, no orders

# 6. go live (small caps are the default: $50/window, $150 global)
make live
```

Panic / ops: `make cancel` (kill all resting orders), `make positions`,
`make redeem` (sweep redeemable leftovers). Full procedures in `RUNBOOK.md`.

---

## Layout

```
src/constants.py      verified V2 addresses & platform limits (single source)
src/config.py         typed config; live mode fails fast on malformed creds
src/sdk.py            client factory: gasless wallet binding + approvals
src/discovery.py      deterministic {asset}-updown-15m-{epoch} slugs + gating
src/quoting.py        pure math: budget-capped fair-split bids (unit-tested)
src/fills.py          fill truth: size_matched + trades API; cancels ≠ fills
src/inventory.py      on-chain position truth (Data API)
src/merge_engine.py   gasless merge, chain-verified, error-classified
src/maker_loop.py     per-window engine: WAIT_READY→FARM→HOLD→DONE
src/window_manager.py concurrency, scheduling, redeem sweeping
src/capital.py        fixed caps + equity kill-switch (no auto-compounding)
src/ops.py            backoff policies + V2 heartbeat dead-man's switch
src/ledger.py         SQLite ledger + internal-vs-wallet reconciliation
tools/                check_setup, test_merge, cancel_all, redeem_all, …
tests/                quoting invariants, fill reconciliation, dry-run E2E
```

---

## Honest expectations

Correct plumbing is necessary, not sufficient. The reference trader's edge is
~2–3¢ per pair **before** competition for queue position; on a ~$200 float,
a good day is measured in single-digit dollars, and quiet books or faster
competitors can make it zero. The unified SDK is officially in beta. Start
with the default small caps, watch the reconciliation report
(`ledger.report()` prints on every shutdown), and scale only what you can
verify on-chain.
