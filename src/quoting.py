"""Low-level quote/merge arithmetic helpers.

The active strategy policy lives in :mod:`src.policy`. Historical helpers that enforce a
fixed pair budget remain only for backward compatibility with external tooling; the live
MakerLoop no longer uses them as a strategy rule.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal


def _d(x: float | str | Decimal) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def tick_floor(price: float, tick: float) -> float:
    t = _d(tick)
    if t <= 0:
        raise ValueError("tick must be positive")
    return float((_d(price) / t).to_integral_value(rounding=ROUND_DOWN) * t)


def tick_decimals(tick: float) -> int:
    return max(0, -_d(tick).as_tuple().exponent)


@dataclass(frozen=True)
class Quote:
    price: float
    shares: float

    @property
    def notional(self) -> float:
        return round(self.price * self.shares, 6)


@dataclass(frozen=True)
class BookSide:
    best_bid: float
    best_ask: float


def size_for_price(price: float, dollar_target: float, min_order_shares: float, min_notional: float) -> float:
    if price <= 0:
        raise ValueError("price must be positive")
    shares = max(
        math.ceil(dollar_target / price),
        math.ceil(min_order_shares),
        math.ceil(min_notional / price),
    )
    return float(int(shares))


def round_pairs_to_micro(up_shares: float, down_shares: float) -> int:
    pairs = min(_d(up_shares), _d(down_shares))
    if pairs <= 0:
        return 0
    return int((pairs * Decimal(10**6)).to_integral_value(rounding=ROUND_DOWN))


def imbalance_fraction(up_shares: float, down_shares: float) -> float:
    denom = max(up_shares, down_shares, 1.0)
    return (up_shares - down_shares) / denom


def conform_price(price: float, tick: float) -> float:
    t = _d(tick)
    return float((_d(price) / t).to_integral_value(rounding=ROUND_HALF_UP) * t)


# ---------------------------------------------------------------------------
# Legacy compatibility helpers. They are NOT used by MakerLoop V3.
# ---------------------------------------------------------------------------
def fair_split_bids(up: BookSide, down: BookSide, combined_budget: float, tick: float):
    if min(up.best_bid, up.best_ask, down.best_bid, down.best_ask) <= 0:
        return None
    if up.best_ask <= up.best_bid or down.best_ask <= down.best_bid:
        return None
    mid_u = (up.best_bid + up.best_ask) / 2.0
    mid_d = (down.best_bid + down.best_ask) / 2.0
    total = mid_u + mid_d
    if total <= 0:
        return None
    bid_u = tick_floor(min(combined_budget * mid_u / total, up.best_ask - tick), tick)
    bid_d = tick_floor(min(combined_budget * mid_d / total, down.best_ask - tick), tick)
    while bid_u + bid_d > combined_budget + 1e-12:
        if bid_u >= bid_d:
            bid_u = tick_floor(bid_u - tick, tick)
        else:
            bid_d = tick_floor(bid_d - tick, tick)
        if bid_u < tick or bid_d < tick:
            return None
    return round(bid_u, 6), round(bid_d, 6)


def cap_against_resting(my_target, other_resting_price, other_target, combined_budget, tick):
    other = other_resting_price if other_resting_price is not None else other_target
    capped = tick_floor(min(my_target, combined_budget - other), tick)
    return None if capped < tick else capped


def size_pair_for_prices(up_price, down_price, dollar_target, min_order_shares, min_notional):
    return max(
        size_for_price(up_price, dollar_target, min_order_shares, min_notional),
        size_for_price(down_price, dollar_target, min_order_shares, min_notional),
    )


def should_requote(resting_price, target_price, drift):
    return resting_price is None or abs(resting_price - target_price) > drift + 1e-12
