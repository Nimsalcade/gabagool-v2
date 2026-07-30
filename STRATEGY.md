# Strategy ground truth

This document separates observations in the gabagool22 activity sample from
implementation hypotheses. It is the evidence baseline for this project; a
behavior should not be described as measured unless the source data establishes
it.

## Evidence base

The sample contains 141,658 executed records over 3.83 days (October 29 through
November 1) for wallet `0x6031b6…f96d`. It covers **916 BTC/ETH Up-or-Down
markets: 734 15-minute markets and 182 hourly markets**. The 916-market total
must not be described as 916 15-minute windows.

## Measured economic behavior

* Participation was two-sided in 99.9% of sampled markets.
* Aggregate potential matched-share coverage was approximately **94.7%**, so
  approximately **5.3%** of purchased outcome shares were unmatched. Median
  market balance was approximately **90.9%**.
* Aggregate UP-plus-DOWN acquisition VWAP was approximately **$0.97**, and 87.8%
  of markets had a combined entry VWAP below $1.00.
* No CLOB `SELL` activity was observed. Matched inventory exited through CTF
  `MERGE`; residual winning inventory exited through `REDEEM` after resolution.
* The first sample recorded approximately **232,415 complete-set merges**. The
  roughly 478,000–500,000 purchased outcome shares are not the same unit as
  complete pairs: one pair consumes one UP share and one DOWN share.
* Execution was frequent and small (approximately 1,537 buys per hour and a
  $3.60 median order).

Matchability is an aggregate result, not evidence of equal-sized fills or equal
inventory in every market. For example, 70 UP shares and 87 DOWN shares produce
70 mergeable pairs and a residual of 17 DOWN shares. A replica must therefore
allow asynchronous fills and temporary, natural asymmetry rather than treating
every action as an atomic equal-quantity pair.

## Economic reconstruction

The strongest reconstruction supported by those measurements is:

1. acquire UP and DOWN inventory asynchronously through many small BUY fills;
2. tolerate temporary quantity imbalance while aggregate cost bases evolve;
3. match an opposite share against roughly 95% of purchased shares;
4. merge matched complete sets for $1 each; and
5. redeem a remaining winning outcome after resolution.

The edge does **not** require simultaneously executable UP and DOWN prices below
$1. A fill of 10 UP at $0.37 at one time and 10 DOWN at $0.58 later constructs
10 complete sets at a $0.95 acquisition cost. Time and inventory are part of
the construction.

A rough scale check using the observed mean pair cost is:

```text
$1.0000 - $0.9715 = $0.0285 gross edge per complete pair
232,415 pairs * $0.0285 = approximately $6,624 gross edge
```

This is not a PnL calculation. Actual PnL must be derived from quantity-weighted
fills, merges, and redemptions at market level, rather than multiplying
headline averages. In particular, multiplying 500,000 *pairs* by an average
edge incorrectly counts outcome shares as pairs and overstates volume by about
two times.

## What remains inferred or unknown

The activity history is consistent with passive liquidity provision, but fills
alone do not establish GTC, post-only or maker status, an exact resting ladder,
queue placement, or latency advantage. Those claims require order-type or
maker/taker metadata.

The sample also does not reveal the policy governing:

* permitted imbalance and inventory by market stage;
* when heavy-side quoting is reduced, retained, or stopped;
* price distribution, ladder depth, and repricing;
* how aggressively the light side is pursued; or
* merge timing.

Consequently, this repository's post-only GTC orders, fair-split targets,
equal share sizing for newly offered legs, combined-budget cap, imbalance
thresholds, and merge cadence are **replica implementation choices**, not
measured gabagool22 parameters. Tests assert the safety properties of those
choices; they do not turn those policies into forensic facts. Equal fresh-order
sizing prevents the bot itself from creating a dollar-sized quantity skew, but
fills remain asynchronous and realized inventory is allowed to be unequal.

## Locked statement

> Gabagool22 is an inventory-centric, market-neutral complete-set liquidity
> strategy. It acquires both outcomes asynchronously through many small BUY
> fills, tolerates temporary quantity imbalance, and achieves roughly 95%
> aggregate share matchability while maintaining an aggregate UP+DOWN
> acquisition VWAP near $0.97. Matched inventory is exited through CTF MERGE
> rather than CLOB SELL; residual winning inventory is redeemed after
> resolution. The available activity history establishes these economic
> behaviors but does not reveal the exact resting-order ladder, repricing
> policy, queue strategy, or latency infrastructure.
