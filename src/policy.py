"""Forensic-calibrated execution policy for Gabagool-style complete-set accumulation.

This module deliberately separates *measured behavior* from still-unknown quote placement.
The full historical decode establishes a maker-dominant mixed execution engine, tight
terminal inventory balance, continued quoting to the final seconds, and aggregate cost-
basis control. It does not reveal cancelled quotes or exact queue placement.

The functions here therefore encode only defensible controls:
- aggregate UP/DOWN VWAP guard, not a per-quote fixed pair budget;
- state-dependent maker skew toward the deficient leg;
- state-dependent taker repair as imbalance/staleness rise;
- taker suppression near expiry while maker quoting continues;
- adaptive 5-40 share clips centered around the observed 10-20 share regime.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class BookSide:
    best_bid: float
    best_ask: float

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid


@dataclass(frozen=True)
class InventoryState:
    up_shares: float
    down_shares: float
    up_cost: float
    down_cost: float
    last_up_fill_ts: float | None
    last_down_fill_ts: float | None
    now_ts: float
    window_start_ts: float
    seconds_to_end: float

    @property
    def up_vwap(self) -> float:
        return self.up_cost / self.up_shares if self.up_shares > 0 else 0.0

    @property
    def down_vwap(self) -> float:
        return self.down_cost / self.down_shares if self.down_shares > 0 else 0.0

    @property
    def combined_vwap(self) -> float | None:
        if self.up_shares <= 0 or self.down_shares <= 0:
            return None
        return self.up_vwap + self.down_vwap

    @property
    def larger_to_smaller_ratio(self) -> float:
        lo = min(self.up_shares, self.down_shares)
        hi = max(self.up_shares, self.down_shares)
        if hi <= 0:
            return 1.0
        if lo <= 0:
            return math.inf
        return hi / lo

    @property
    def deficient_side(self) -> str | None:
        if abs(self.up_shares - self.down_shares) < 1e-9:
            return None
        return "UP" if self.up_shares < self.down_shares else "DOWN"

    def stale_seconds(self, side: str) -> float:
        """Seconds since this side last filled; market age if it has never filled."""
        ts = self.last_up_fill_ts if side == "UP" else self.last_down_fill_ts
        anchor = ts if ts is not None else self.window_start_ts
        return max(0.0, self.now_ts - anchor)

    def opposite_stale_seconds(self, side: str) -> float:
        """Decoded feature: lag since the most recent fill on the opposite outcome."""
        return self.stale_seconds("DOWN" if side == "UP" else "UP")


def tick_floor(price: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("tick must be positive")
    return math.floor((price + 1e-12) / tick) * tick


def maker_target(book: BookSide, *, tick: float, inventory_relation: str, ratio: float) -> float | None:
    """Return a passive bid. Unknown queue policy is represented conservatively.

    Balanced: rest at best bid.
    Deficient leg: improve by one tick when the spread permits it.
    Heavy leg: shade one tick down once imbalance is material.
    """
    if book.best_bid <= 0 or book.best_ask <= book.best_bid or tick <= 0:
        return None
    max_post_only = tick_floor(book.best_ask - tick, tick)
    if max_post_only < tick:
        return None

    px = tick_floor(book.best_bid, tick)
    if inventory_relation == "deficient" and ratio >= 1.05 and book.spread >= 2 * tick - 1e-12:
        px = min(max_post_only, tick_floor(book.best_bid + tick, tick))
    elif inventory_relation == "heavy" and ratio >= 1.10:
        px = max(tick, tick_floor(book.best_bid - tick, tick))
    return min(px, max_post_only)


def projected_combined_vwap(
    state: InventoryState,
    *,
    side: str,
    price: float,
    shares: float,
) -> float | None:
    """Combined side VWAP after a hypothetical BUY on one side."""
    if shares <= 0:
        return state.combined_vwap
    if side == "UP":
        up_sh = state.up_shares + shares
        up_cost = state.up_cost + price * shares
        dn_sh, dn_cost = state.down_shares, state.down_cost
    elif side == "DOWN":
        dn_sh = state.down_shares + shares
        dn_cost = state.down_cost + price * shares
        up_sh, up_cost = state.up_shares, state.up_cost
    else:
        raise ValueError("side must be UP or DOWN")
    if up_sh <= 0 or dn_sh <= 0:
        return None
    return (up_cost / up_sh) + (dn_cost / dn_sh)


def basis_allows(
    state: InventoryState,
    *,
    side: str,
    price: float,
    shares: float,
    max_combined_vwap: float,
    opposite_reference_price: float | None = None,
    initial_pair_ceiling: float = 1.0,
) -> bool:
    """Economic gate based on aggregate cost basis.

    Once both outcomes exist, guard the projected *portfolio* VWAP. Before both legs
    exist, use only a loose initial pair ceiling against an observable opposite
    reference; this avoids recreating the disproven permanent 0.97 pair constraint.
    """
    projected = projected_combined_vwap(state, side=side, price=price, shares=shares)
    if projected is not None:
        return projected <= max_combined_vwap + 1e-12
    if opposite_reference_price is None:
        return True
    return price + opposite_reference_price <= initial_pair_ceiling + 1e-12


def adaptive_clip(
    *,
    base_clip_shares: float,
    max_clip_shares: float,
    min_order_shares: float,
    min_notional: float,
    price: float,
    ratio: float,
    relation: str,
    aggressive: bool,
) -> float:
    """Adaptive whole-share clip calibrated to the observed 5-50 share regime."""
    if price <= 0:
        raise ValueError("price must be positive")
    mult = 1.0
    if relation == "deficient":
        if ratio >= 1.50:
            mult = 2.5
        elif ratio >= 1.25:
            mult = 2.0
        elif ratio >= 1.10:
            mult = 1.5
        elif ratio >= 1.05:
            mult = 1.25
    elif relation == "heavy" and ratio >= 1.10:
        mult = 0.75
    if aggressive:
        mult = max(mult, 1.5)

    raw = base_clip_shares * mult
    floor_shares = max(min_order_shares, math.ceil(min_notional / price))
    shares = max(floor_shares, math.ceil(raw))
    return float(min(math.ceil(max_clip_shares), shares))


def taker_should_fire(
    state: InventoryState,
    *,
    candidate_side: str,
    projected_basis: float | None,
    target_combined_vwap: float,
    max_combined_vwap: float,
    taker_stop_buffer_s: float,
) -> bool:
    """Deterministic taker trigger reflecting the decoded conditional behavior.

    Taker propensity rose with imbalance and opposite-leg staleness but fell sharply
    near expiration. We encode that monotonic structure rather than pretending the
    exact hidden probability function is known.
    """
    if state.seconds_to_end <= taker_stop_buffer_s:
        return False
    ratio = state.larger_to_smaller_ratio
    deficient = state.deficient_side
    # The historical decoder measured nearest-prior *opposite-outcome* fill lag.
    stale = state.opposite_stale_seconds(candidate_side)

    # Exceptionally favorable aggregate economics can justify taking while balanced.
    if projected_basis is not None and projected_basis <= target_combined_vwap - 0.01:
        if ratio < 1.05 and state.seconds_to_end > 60:
            return True

    if deficient != candidate_side:
        return False
    if projected_basis is not None and projected_basis > max_combined_vwap + 1e-12:
        return False

    if math.isinf(ratio):
        threshold = 5.0
    elif ratio >= 1.50:
        threshold = 5.0
    elif ratio >= 1.25:
        threshold = 10.0
    elif ratio >= 1.10:
        threshold = 15.0
    elif ratio >= 1.05:
        threshold = 30.0
    else:
        threshold = 45.0

    # Historical taker share declines into the close: demand stronger evidence.
    if state.seconds_to_end <= 30:
        if ratio < 1.25:
            return False
        threshold = max(threshold, 15.0)
    elif state.seconds_to_end <= 60:
        threshold += 10.0

    return stale >= threshold


def relation_for_side(state: InventoryState, side: str) -> str:
    deficient = state.deficient_side
    if deficient is None:
        return "balanced"
    return "deficient" if deficient == side else "heavy"
