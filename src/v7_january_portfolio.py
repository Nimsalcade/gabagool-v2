"""V7 January-style portfolio controller primitives.

This module encodes the *observable architecture* recovered from the January 17
Gabagool market without claiming to know the trader's private implementation:

* quote both complementary outcomes continuously;
* let inventory drift temporarily rather than serially pairing every fill;
* bias new resting exposure toward the underweight outcome as the gap grows;
* keep at least a small amount of resting exposure on the overweight outcome;
* strengthen balance pressure late in the 15-minute window;
* use portfolio-level VWAP economics instead of a per-lot pair ceiling;
* permit multiple independent parents at the same price.

The profitability cap is an explicit paper-trading guard.  It is not claimed to
be a recovered Gabagool constant.  Public January fills show that his gross
combined acquisition VWAP could exceed $1 in some markets, so a live clone would
need verified rebates/other economics before relaxing this guard above par.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ParentTargets:
    up: int
    down: int
    gap_clips: float
    underweight: str | None
    pressure: float


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def effective_portfolio_cap(
    *,
    age_s: float,
    duration_s: float,
    base_cap: float,
    late_cap: float,
    late_start_s: float,
) -> float:
    """Ramp the portfolio cap from base_cap toward late_cap near expiration."""
    if duration_s <= late_start_s or age_s <= late_start_s:
        return base_cap
    x = clamp((age_s - late_start_s) / (duration_s - late_start_s), 0.0, 1.0)
    # Quadratic ramp keeps the early/middle profitability gate tight while
    # increasingly prioritizing balance as expiry approaches.
    return base_cap + (late_cap - base_cap) * x * x


def projected_side_vwap(*, shares: float, cost: float, add_qty: float, price: float) -> float:
    q = shares + add_qty
    if q <= 0:
        raise ValueError("projected quantity must be positive")
    return (cost + add_qty * price) / q


def max_buy_price_for_portfolio_cap(
    *,
    side: str,
    add_qty: float,
    up_shares: float,
    up_cost: float,
    down_shares: float,
    down_cost: float,
    combined_cap: float,
    absolute_max: float = 0.99,
) -> float:
    """Maximum price for one additional parent under aggregate side-VWAP economics.

    If the opposite side has not filled yet, there is no combined-VWAP anchor;
    only ``absolute_max`` applies.  Once both sides exist, solve exactly for the
    incoming price that keeps projected UP_VWAP + DOWN_VWAP <= combined_cap.
    This is deliberately portfolio-level: no historical lot is assigned to the
    proposed fill.
    """
    side = side.upper()
    if side not in ("UP", "DOWN"):
        raise ValueError("side must be UP or DOWN")
    if add_qty <= 0:
        return 0.0

    if side == "UP":
        own_q, own_c = up_shares, up_cost
        opp_q, opp_c = down_shares, down_cost
    else:
        own_q, own_c = down_shares, down_cost
        opp_q, opp_c = up_shares, up_cost

    if opp_q <= 0:
        return max(0.0, absolute_max)

    opp_vwap = opp_c / opp_q
    own_target_vwap = combined_cap - opp_vwap
    if own_target_vwap <= 0:
        return 0.0

    # (own_c + q*p)/(own_q+q) <= own_target_vwap
    max_p = (own_target_vwap * (own_q + add_qty) - own_c) / add_qty
    return clamp(max_p, 0.0, absolute_max)


def parent_targets(
    *,
    up_shares: float,
    down_shares: float,
    parent_clip: float,
    age_s: float,
    duration_s: float = 900.0,
    base_parents: int = 4,
    max_parents: int = 12,
    min_overweight_parents: int = 1,
    underweight_gain_per_clip: float = 0.55,
    overweight_decay_per_clip: float = 0.22,
    late_pressure_gain: float = 1.50,
    hard_gap_clips: float = 22.0,
) -> ParentTargets:
    """Convert inventory imbalance into simultaneous UP/DOWN parent targets.

    The shape is calibrated to the January forensic fingerprint rather than an
    asserted hidden formula: temporary gaps up to ~22 median clips were observed,
    both sides remained active, and balance pressure strengthened late.
    """
    if parent_clip <= 0:
        return ParentTargets(0, 0, 0.0, None, 1.0)
    if base_parents < 1 or max_parents < base_parents:
        raise ValueError("invalid parent-count bounds")
    if min_overweight_parents < 0 or min_overweight_parents > base_parents:
        raise ValueError("invalid min_overweight_parents")

    gap = up_shares - down_shares
    gap_clips = abs(gap) / parent_clip
    underweight = None if abs(gap) <= 1e-12 else ("DOWN" if gap > 0 else "UP")

    t = clamp(age_s / max(duration_s, 1e-9), 0.0, 1.0)
    pressure = 1.0 + late_pressure_gain * t * t

    if underweight is None:
        return ParentTargets(base_parents, base_parents, 0.0, None, pressure)

    under_n = int(round(base_parents + pressure * underweight_gain_per_clip * gap_clips))
    over_n = int(round(base_parents - pressure * overweight_decay_per_clip * gap_clips))
    under_n = int(clamp(under_n, base_parents, max_parents))
    over_n = int(clamp(over_n, min_overweight_parents, base_parents))

    # At the historical-scale emergency boundary, stop adding to the already
    # overweight side.  This is a safety surface around the observed max gap,
    # not a claim that the source bot used this exact threshold.
    if gap_clips >= hard_gap_clips:
        over_n = 0
        under_n = max_parents

    if underweight == "UP":
        return ParentTargets(under_n, over_n, gap_clips, underweight, pressure)
    return ParentTargets(over_n, under_n, gap_clips, underweight, pressure)


def passive_top(
    *,
    best_bid: float | None,
    best_ask: float | None,
    tick: float,
    max_price: float,
    improve_ticks: int,
    backoff_ticks: int = 0,
) -> float | None:
    """Return a post-only passive bid, optionally improving or backing off."""
    if tick <= 0 or max_price <= 0:
        return None
    if best_ask is None:
        return None

    post_only_max = best_ask - tick
    if post_only_max <= 0:
        return None

    if best_bid is None:
        candidate = post_only_max
    else:
        candidate = best_bid + improve_ticks * tick - backoff_ticks * tick

    candidate = min(candidate, post_only_max, max_price)
    # Put price on the tick grid conservatively.
    units = math.floor((candidate + 1e-12) / tick)
    out = units * tick
    return None if out <= 0 else round(out, 10)


def stacked_ladder(*, top: float | None, tick: float, levels: int, parents: int) -> tuple[float, ...]:
    """Generate adjacent levels with independent same-price parent stacks."""
    if top is None or top <= 0 or tick <= 0 or levels <= 0 or parents <= 0:
        return ()
    prices: list[float] = []
    for i in range(parents):
        level = i % levels
        px = top - level * tick
        if px <= 0:
            break
        prices.append(round(px, 10))
    return tuple(prices)
