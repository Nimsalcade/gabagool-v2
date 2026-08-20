"""Forensic-calibrated execution policy for Gabagool-style complete-set accumulation.

This module encodes only behavior supported by the completed execution reconstruction:
- BUY both complementary outcomes;
- maker-dominant execution with selective taker repair;
- aggregate cost-basis control instead of a fixed per-quote pair cap;
- tighter maker skew as inventory departs from 1:1;
- taker use concentrated on deficient-leg repair, not generic edge chasing;
- aggressive sizing constrained so small deficits do not flip the imbalance;
- taker urgency rises with imbalance/staleness and falls into expiry;
- 5-40 share clips centered on the observed 10-20 share regime.

The exact cancelled-quote/queue placement algorithm is not observable from OrderFilled,
so maker_target remains a calibrated implementation surface rather than a historical claim.
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

    def side_shares(self, side: str) -> float:
        if side == "UP":
            return self.up_shares
        if side == "DOWN":
            return self.down_shares
        raise ValueError("side must be UP or DOWN")

    def deficit_shares(self, side: str) -> float:
        if side == "UP":
            return max(0.0, self.down_shares - self.up_shares)
        if side == "DOWN":
            return max(0.0, self.up_shares - self.down_shares)
        raise ValueError("side must be UP or DOWN")

    def stale_seconds(self, side: str) -> float:
        ts = self.last_up_fill_ts if side == "UP" else self.last_down_fill_ts
        anchor = ts if ts is not None else self.window_start_ts
        return max(0.0, self.now_ts - anchor)

    def opposite_stale_seconds(self, side: str) -> float:
        return self.stale_seconds("DOWN" if side == "UP" else "UP")


def tick_floor(price: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("tick must be positive")
    return math.floor((price + 1e-12) / tick) * tick


def maker_target(book: BookSide, *, tick: float, inventory_relation: str, ratio: float) -> float | None:
    """Passive BUY target with early inventory skew.

    Terminal inventory is extremely tight historically, so the maker layer begins
    correcting before the taker layer: deficient inventory improves from ~3% imbalance
    when spread permits, while the heavy leg shades down from ~5%.
    """
    if book.best_bid <= 0 or book.best_ask <= book.best_bid or tick <= 0:
        return None
    max_post_only = tick_floor(book.best_ask - tick, tick)
    if max_post_only < tick:
        return None
    px = tick_floor(book.best_bid, tick)
    if inventory_relation == "deficient" and ratio >= 1.03 and book.spread >= 2 * tick - 1e-12:
        px = min(max_post_only, tick_floor(book.best_bid + tick, tick))
    elif inventory_relation == "heavy" and ratio >= 1.05:
        px = max(tick, tick_floor(book.best_bid - tick, tick))
    return min(px, max_post_only)


def projected_combined_vwap(
    state: InventoryState,
    *,
    side: str,
    price: float,
    shares: float,
) -> float | None:
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
    """Whole-share clip schedule centered on the observed 10-20 share regime."""
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
    elif relation == "heavy":
        if ratio >= 1.25:
            mult = 0.50
        elif ratio >= 1.05:
            mult = 0.75

    # Aggressive fills were larger on average, but V3 over-amplified missing-leg
    # states. Keep the normal taker clip in the empirically dominant <=20-share band;
    # 20-50 share fills remain available through configured max clip in future tuning.
    if aggressive:
        mult = min(2.0, max(1.0, mult))

    raw = base_clip_shares * mult
    floor_shares = max(min_order_shares, math.ceil(min_notional / price))
    shares = max(floor_shares, math.ceil(raw))
    return float(min(math.ceil(max_clip_shares), shares))


def repair_clip(
    state: InventoryState,
    *,
    side: str,
    proposed_shares: float,
    min_order_shares: float,
) -> float:
    """Exact-deficit cap for callers that can apply it before sending a FAK."""
    if state.deficient_side != side:
        return 0.0
    deficit = state.deficit_shares(side)
    if deficit + 1e-9 < min_order_shares:
        return 0.0
    return float(max(0.0, min(proposed_shares, deficit)))


def _base_taker_stale_threshold(ratio: float) -> float | None:
    if math.isinf(ratio) or ratio >= 2.0:
        return 30.0
    if ratio >= 1.50:
        return 30.0
    if ratio >= 1.25:
        return 40.0
    if ratio >= 1.10:
        return 50.0
    if ratio >= 1.05:
        return 60.0
    return None


def _minimum_repair_deficit(ratio: float) -> float:
    """Avoid firing a clip larger than a very small live deficit."""
    if math.isinf(ratio) or ratio >= 1.25:
        return 20.0
    if ratio >= 1.10:
        return 15.0
    return 10.0


def taker_should_fire(
    state: InventoryState,
    *,
    candidate_side: str,
    projected_basis: float | None,
    target_combined_vwap: float,
    max_combined_vwap: float,
    taker_stop_buffer_s: float,
) -> bool:
    """Deficient-leg taker repair gate derived from the full-history cross-tabs.

    Price edge alone is not a trigger. Taker use increases with imbalance and stale
    opposite fills, but decreases into the close. Small deficits are left to maker skew
    instead of being over-repaired by a FAK.
    """
    del target_combined_vwap
    if state.seconds_to_end <= taker_stop_buffer_s:
        return False
    if state.deficient_side != candidate_side:
        return False
    if projected_basis is not None and projected_basis > max_combined_vwap + 1e-12:
        return False

    ratio = state.larger_to_smaller_ratio
    threshold = _base_taker_stale_threshold(ratio)
    if threshold is None:
        return False
    if state.deficit_shares(candidate_side) + 1e-9 < _minimum_repair_deficit(ratio):
        return False

    remaining = state.seconds_to_end
    if remaining <= 15:
        if ratio < 1.50:
            return False
        threshold += 20.0
    elif remaining <= 30:
        if ratio < 1.25:
            return False
        threshold += 15.0
    elif remaining <= 60:
        threshold += 10.0
    elif remaining <= 120:
        threshold += 5.0

    return state.opposite_stale_seconds(candidate_side) >= threshold


def relation_for_side(state: InventoryState, side: str) -> str:
    deficient = state.deficient_side
    if deficient is None:
        return "balanced"
    return "deficient" if deficient == side else "heavy"
