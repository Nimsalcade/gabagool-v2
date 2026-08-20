# Strategy ground truth — full forensic decode

This document is the evidence boundary for the replica. It supersedes the earlier
3.83-day sample reconstruction.

## Full evidence base

The completed Polygon `OrderFilled` decode contains **12,921,881 exact executions**
across the Gabagool22 activity period. All executions mapped to official Polymarket
market metadata; **12,921,880 were BUY and one was SELL**.

Execution role:

- maker: **11,042,283 (85.45%)**
- taker/aggressive: **1,879,598 (14.55%)**

The public market inventory contains **28,620 conditions**, with 28,586 two-outcome
BUY markets. Full-period terminal combined side VWAP had median **0.985117**, p90
**1.002805**, and p95 **1.010720**. Terminal larger/smaller inventory ratio had median
**1.013584**, p90 **1.055304**, and p95 **1.082311**.

## Timing

First successful fill after market open:

- 5m: median 15s, p90 23s
- 15m: median 20s, p90 35s

Last successful fill age:

- 5m: median 273s, p90 296s, p95 297s
- 15m: median 794s, p90 893s, p95 897s

Therefore the reference trader often continued receiving maker fills within only a
few seconds of expiration. A large fixed pre-close shutdown buffer is not faithful.

## Inventory / taker policy

Taker share rises with pre-fill inventory imbalance:

- ratio <1.05: ~13.17%
- 1.05-1.10: ~16.01%
- 1.10-1.25: ~17.31%
- 1.25-1.50: ~18.42%
- 1.50-2.00: ~18.72%

Taker share also rose as the nearest prior **opposite-outcome** fill became stale:
~9.8% at <=0s, ~13.7% at 1-5s, ~15.7% at 5-15s, ~17.4% at 15-30s, and ~18.2%
at 30-60s. Taker share fell sharply into expiry.

These are conditional fingerprints, not proof of the exact hidden trigger formula.
The replica therefore preserves the monotonic relationships while keeping the
exact trigger implementation explicit and testable.

## Economic policy

The previous replica enforced a permanent `UP bid + DOWN bid <= 0.97` rule. The full
history disproves that as a forensic invariant. The reference trader sometimes took
fills whose simple incremental pair proxy exceeded $1 and still finished with favorable
aggregate inventory economics.

The active replica must therefore manage:

- UP quantity and cost basis;
- DOWN quantity and cost basis;
- aggregate `UP_VWAP + DOWN_VWAP`;
- inventory ratio;
- fill staleness;
- time remaining;
- maker vs taker execution mode.

It must not treat every fill as an atomic complete set.

## Order size / price behavior

Executed clips overwhelmingly fall in the 5-50 share range, with 10-20 shares dominant.
Across 57,240 market-sides the median number of distinct **executed** price levels was
53 (p90 ~97). This proves highly adaptive execution but does not reveal every cancelled
or unfilled quote.

## Settlement

Matched UP+DOWN inventory exits via CTF MERGE, residual winners through REDEEM. The
expanded official history contains about **$71.236M** of MERGE activity. Roughly 99.5%
of merge transactions covered multiple markets; most 5m and 15m merge records occurred
after close. The replica therefore holds matched inventory through the live window and
settles after close rather than merging every 20 seconds.

## Representative 5-minute market

One median-notional BTC 5m market (Feb 14, 2026 2:05-2:10 AM ET) produced:

- 292 exact BUY executions / 227 filled order hashes
- $1,356.685 total spend
- 284 maker / 8 taker fills
- 1,363.536 matched shares
- terminal combined VWAP 0.992764
- terminal inventory ratio 1.004599
- $1,363.517 observed settlement cash
- **+$6.832 gross realized P&L before the final net fee/rebate join**

## Still unknown

`OrderFilled` cannot reveal:

- exact unfilled quote prices;
- cancellation/replacement frequency;
- queue position;
- precise spread-distance rule;
- the exact stochastic/deterministic taker trigger.

Those remaining dimensions must be calibrated in paper/live observation against the
measured fingerprints above. They must not be silently presented as forensic facts.
