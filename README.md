# Gabagool V3 — Forensic-Calibrated Complete-Set Accumulation Engine

This branch rebuilds the strategy layer around the completed **12.92M exact on-chain
execution decode** while retaining the V2 wallet, fill-reconciliation, merge, relayer,
heartbeat, and ledger plumbing.

The key correction is architectural: Gabagool is not a 100% post-only `$0.97` spread
farmer. The reference behavior is a **maker-dominant mixed maker/taker inventory engine**
that buys both outcomes asynchronously, manages aggregate cost basis and inventory
imbalance, trades almost to expiry, and settles matched inventory mostly after close.

See [`STRATEGY.md`](STRATEGY.md) for the full evidence boundary.

## What changed from V2

- removed the fixed `$0.97` pair-budget strategy invariant;
- added `src/policy.py` with aggregate VWAP, imbalance, opposite-fill lag and expiry controls;
- enabled selective non-post-only BUY repair orders;
- maker quotes continue until ~T-2s; taker aggression stops earlier;
- added 5m + 15m discovery and BTC/ETH/SOL/XRP candidate support;
- removed continuous intra-window merging; closed markets enter a settlement sweep;
- fixed capital accounting so cumulative turnover is not mistaken for permanent global exposure;
- wired authoritative fills into the SQLite `fills` table;
- updated tests to assert measured policy structure instead of the disproven fixed-budget hypothesis.

## Safety boundary

`dry_run: true` remains the default. Capital caps in `config/default.yaml` are **deployment
safety limits, not claims about Gabagool's account size**. The representative historical
BTC 5m market used ~$1,357 of gross spend, so a low live cap will intentionally scale the
behavior down.

The live merge-proof gate, heartbeat, authoritative fill reconciliation, and on-chain
merge verification remain in place.

## Quick start

```bash
make install
make test

cp .env.example .env
set -a; source .env; set +a

make check
make merge-proof
make dry

# live remains explicitly gated
make live
```

## Core architecture

```text
current 5m/15m crypto markets
          |
          v
   market + inventory state
   - UP/DOWN qty + VWAP
   - imbalance ratio
   - opposite-fill lag
   - seconds to close
          |
          v
    execution controller
     /             \
 maker BUY       taker BUY
 dominant        selective
     \             /
      aggregate basis guard
             |
       tight terminal balance
             |
          market close
             |
     settlement sweep
      MERGE + REDEEM
```

## Validation target

A replica is not considered converged merely because it makes money. Paper/live telemetry
must be compared against the reference fingerprints: maker/taker share, taker share by
imbalance, taker share by opposite-fill lag and time remaining, clip distribution, first/
last fill timing, terminal inventory ratio, combined VWAP, distinct executed price levels,
market spend, and settlement timing.
