"""
Pure quoting math. No I/O, no SDK — every function here is unit-tested.

THE STRATEGY, AS MATH
---------------------
A matched Up+Down pair always merges for exactly $1.00. We therefore only
ever rest BUY orders whose prices satisfy, at all times:

        bid_up + bid_down <= combined_budget  (default 0.97)
        bid_side          <= best_ask_side - tick      (never cross = never take)

Fills then arrive only when sellers cross to us; the pair's gross margin
(1.00 - combined entry) is locked the moment shares are balanced, and is
realized by the merge engine. There is no price prediction anywhere in this
file — the fair-value split only decides how the fixed budget is allocated
between the two sides, never whether to favor one.

HARD INVARIANTS (enforced, not assumed)
---------------------------------------
* Output prices are tick-aligned, in (0, 1).
* Output prices never cross the book.
* The pair of outputs never sums above the budget — including against the
  OTHER side's currently-resting price, so a stale-but-kept order can't
  combine with a fresh one into a >budget pair.
* Sizes respect the exchange minimum share count and minimum notional.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal


def _d(x: float | str | Decimal) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def tick_floor(price: float, tick: float) -> float:
    """Round price DOWN to the market tick. Down, never half-up: rounding a
    bid up can cross the book or breach the pair budget by one tick."""
    t = _d(tick)
    if t <= 0:
        raise ValueError("tick must be positive")
    return float((_d(price) / t).to_integral_value(rounding=ROUND_DOWN) * t)


def tick_decimals(tick: float) -> int:
    return max(0, -_d(tick).as_tuple().exponent)


@dataclass(frozen=True)
class Quote:
    """One side's planned resting bid."""
    price: float
    shares: float

    @property
    def notional(self) -> float:
        return round(self.price * self.shares, 6)


@dataclass(frozen=True)
class BookSide:
    best_bid: float
    best_ask: float


def fair_split_bids(
    up: BookSide,
    down: BookSide,
    combined_budget: float,
    tick: float,
) -> tuple[float, float] | None:
    """
    Allocate `combined_budget` between Up and Down bids in proportion to the
    mid-implied fair values, then clamp each bid strictly below its ask and
    re-shave until the pair satisfies the budget after tick rounding.

    Returns (up_bid, down_bid) or None when no valid two-sided quote exists
    (e.g. a side has no book, or budget can't be met above one tick each).
    """
    if min(up.best_bid, up.best_ask, down.best_bid, down.best_ask) <= 0:
        return None
    if up.best_ask <= up.best_bid or down.best_ask <= down.best_bid:
        # Crossed/locked book snapshot — skip the cycle rather than guess.
        return None

    mid_u = (up.best_bid + up.best_ask) / 2.0
    mid_d = (down.best_bid + down.best_ask) / 2.0
    total = mid_u + mid_d
    if total <= 0:
        return None

    target_u = combined_budget * (mid_u / total)
    target_d = combined_budget * (mid_d / total)

    # Never cross: cap strictly one tick under each ask. tick_floor on the
    # cap keeps "ask - tick" itself tick-aligned for odd book prices.
    bid_u = tick_floor(min(target_u, up.best_ask - tick), tick)
    bid_d = tick_floor(min(target_d, down.best_ask - tick), tick)

    # Budget guarantee after rounding: shave the larger side a tick at a time.
    guard = 0
    while bid_u + bid_d > combined_budget + 1e-12:
        if bid_u >= bid_d:
            bid_u = tick_floor(bid_u - tick, tick)
        else:
            bid_d = tick_floor(bid_d - tick, tick)
        guard += 1
        if guard > 400:  # cannot happen with tick >= 1e-4, but never loop forever
            return None

    if bid_u < tick or bid_d < tick:
        return None
    return (round(bid_u, 6), round(bid_d, 6))


def cap_against_resting(
    my_target: float,
    other_resting_price: float | None,
    other_target: float,
    combined_budget: float,
    tick: float,
) -> float | None:
    """
    Final per-order budget check at POST time.

    The pair constraint must hold against the other side's *actual* resting
    price when one exists (it may be a kept, slightly stale order), falling
    back to that side's fresh target otherwise. This closes the gap where two
    individually-sane quotes combine into a >budget pair.
    """
    other = other_resting_price if other_resting_price is not None else other_target
    capped = tick_floor(min(my_target, combined_budget - other), tick)
    if capped < tick:
        return None
    return capped


def size_for_price(
    price: float,
    rung_dollar_target: float,
    min_order_shares: float,
    min_notional: float,
) -> float:
    """
    Shares for one resting rung: aim for `rung_dollar_target` dollars, then
    clamp UP to every exchange floor. The old bot's hundreds of
    'Size (4) lower than the minimum: 5' rejections came from skipping this.
    """
    if price <= 0:
        raise ValueError("price must be positive")
    shares = max(
        math.ceil(rung_dollar_target / price),
        math.ceil(min_order_shares),
        math.ceil(min_notional / price),
    )
    # Whole shares: the book's min sizes are integral and integral sizes
    # keep merge arithmetic exact in 1e-6 base units.
    return float(int(shares))


def should_requote(resting_price: float | None, target_price: float, drift: float) -> bool:
    """Keep a well-priced resting order (queue position is valuable);
    replace it only when it has drifted from target."""
    if resting_price is None:
        return True
    return abs(resting_price - target_price) > drift + 1e-12


def round_pairs_to_micro(up_shares: float, down_shares: float) -> int:
    """Mergeable pairs in 1e-6 base units = floor(min(up, down) * 1e6).
    Always floor — requesting one micro-unit more than you hold reverts the
    whole merge transaction."""
    pairs = min(_d(up_shares), _d(down_shares))
    if pairs <= 0:
        return 0
    return int((pairs * Decimal(10**6)).to_integral_value(rounding=ROUND_DOWN))


def imbalance_fraction(up_shares: float, down_shares: float) -> float:
    """Signed imbalance: +x means Up-heavy, -x means Down-heavy."""
    denom = max(up_shares, down_shares, 1.0)
    return (up_shares - down_shares) / denom


def conform_price(price: float, tick: float) -> float:
    """Public helper: tick-align an externally provided price (round half-up
    is fine here because callers re-check the cross/budget constraints)."""
    t = _d(tick)
    return float((_d(price) / t).to_integral_value(rounding=ROUND_HALF_UP) * t)
