"""Maximum-identifiable Gabagool BTC/ETH 15-minute policy.

This module contains only the 15-minute behavior that can be reconstructed from
public executions plus explicitly labelled implementation surfaces where public
fills cannot identify the hidden order lifecycle.

Observed / reconstructed invariants (2025-10-29 fit sample, 150 markets / 16,050 fills):

* BUY both complementary outcomes; no routine SELL path.
* First quoting begins at ~18-23s; quote activity can continue to ~T-1s.
* Parent clip schedule by market age is 10 -> 9 -> 8 -> 7 -> 6 -> 5 shares.
* Executed prices overwhelmingly lie on the integer-cent grid.
* A useful passive base-price reconstruction is the complement of the opposite
  displayed ask: UP_bid ~= 1 - DOWN_ask, DOWN_bid ~= 1 - UP_ask, capped so the
  order remains post-only on its own book.
* Inventory control is soft.  The underweight side keeps four exposure layers;
  the overweight side steps 4 -> 3 -> 2 -> 1 -> 0 layers as the gap grows in
  units of the current parent clip.
* Existing orders must not be assumed to cancel instantaneously when the target
  layer count falls; public fills do not expose cancelled/unfilled orders.
* Eight parent clips is the reconstruction's emergency gap bound.
* Matched inventory is settled as complete sets after the live window; residual
  winning inventory redeems at resolution.

NOT identifiable from public fills:
* exact cancel/requote TTL;
* queue position and queue-ahead size;
* every unfilled quote price;
* the exact hidden trigger for the minority aggressive/taker executions.

Those dimensions belong in the paper/execution adapter, never in this module as
claims about the historical trader.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


DURATION_S = 900
QUOTE_START_AGE_S = 18.0
QUOTE_END_AGE_S = 899.0
UNDERWEIGHT_LAYERS = 4
HARD_SAFETY_GAP_CLIPS = 8.0

# Reconstructed parent-clip schedule. Intervals are [from_age, to_age).
CLIP_SCHEDULE: tuple[tuple[float, float, float], ...] = (
    (18.0, 212.0, 10.0),
    (212.0, 382.0, 9.0),
    (382.0, 522.0, 8.0),
    (522.0, 664.0, 7.0),
    (664.0, 818.0, 6.0),
    (818.0, 899.0, 5.0),
)


@dataclass(frozen=True)
class Inventory:
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0

    @property
    def up_vwap(self) -> float | None:
        return None if self.up_shares <= 0 else self.up_cost / self.up_shares

    @property
    def down_vwap(self) -> float | None:
        return None if self.down_shares <= 0 else self.down_cost / self.down_shares

    @property
    def combined_vwap(self) -> float | None:
        u, d = self.up_vwap, self.down_vwap
        return None if u is None or d is None else u + d

    @property
    def matched(self) -> float:
        return min(self.up_shares, self.down_shares)

    @property
    def signed_gap(self) -> float:
        return self.up_shares - self.down_shares

    @property
    def abs_gap(self) -> float:
        return abs(self.signed_gap)

    @property
    def underweight(self) -> str | None:
        if abs(self.signed_gap) <= 1e-12:
            return None
        return "DOWN" if self.signed_gap > 0 else "UP"

    @property
    def heavy(self) -> str | None:
        under = self.underweight
        if under is None:
            return None
        return "UP" if under == "DOWN" else "DOWN"

    def shares(self, side: str) -> float:
        side = side.upper()
        if side == "UP":
            return self.up_shares
        if side == "DOWN":
            return self.down_shares
        raise ValueError("side must be UP or DOWN")

    def cost(self, side: str) -> float:
        side = side.upper()
        if side == "UP":
            return self.up_cost
        if side == "DOWN":
            return self.down_cost
        raise ValueError("side must be UP or DOWN")


def tick_floor(value: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("tick must be positive")
    if value <= 0:
        return 0.0
    return math.floor((value + 1e-12) / tick) * tick


def clip_for_age(age_s: float) -> float:
    """Return the reconstructed 15m parent clip; 0 outside quoteable ages."""
    age = float(age_s)
    for start, end, shares in CLIP_SCHEDULE:
        if start <= age < end:
            return shares
    return 0.0


def gap_in_clips(inv: Inventory, parent_clip: float) -> float:
    if parent_clip <= 0:
        return math.inf if inv.abs_gap > 0 else 0.0
    return inv.abs_gap / parent_clip


def heavy_layer_count(gap_clips: float) -> int:
    """Reconstructed overweight-side exposure schedule."""
    if gap_clips < 0.5:
        return 4
    if gap_clips < 1.5:
        return 3
    if gap_clips < 2.5:
        return 2
    if gap_clips < 3.5:
        return 1
    return 0


def desired_layer_count(inv: Inventory, side: str, parent_clip: float) -> int:
    """Desired live maker exposure layers for one side.

    Balanced inventory exposes four layers on both sides. Once imbalanced, the
    underweight side remains at four layers while the heavy side progressively
    stops being renewed. This function describes the TARGET exposure only; it
    intentionally says nothing about how fast already-resting orders cancel.
    """
    side = side.upper()
    if side not in ("UP", "DOWN"):
        raise ValueError("side must be UP or DOWN")
    if parent_clip <= 0:
        return 0
    under = inv.underweight
    if under is None or side == under:
        return UNDERWEIGHT_LAYERS
    return heavy_layer_count(gap_in_clips(inv, parent_clip))


def complementary_base_bid(
    *,
    own_best_ask: float,
    opposite_best_ask: float,
    tick: float,
) -> float | None:
    """Reconstructed passive base bid from the opposite displayed ask.

    base = 1 - opposite_ask, floored to the venue tick, then capped one tick
    below the side's own ask so a paper/live implementation stays post-only.
    """
    if tick <= 0 or own_best_ask <= 0 or opposite_best_ask <= 0:
        return None
    post_only_cap = tick_floor(own_best_ask - tick, tick)
    complement = tick_floor(1.0 - opposite_best_ask, tick)
    px = min(post_only_cap, complement)
    if px < tick or px >= 1.0:
        return None
    return round(px, 10)


def layer_prices(base_bid: float | None, *, tick: float, layers: int) -> tuple[float, ...]:
    """Return integer-tick passive layers from the reconstructed base bid.

    Public executions identify multiple executed cent levels but not the exact
    cancelled layer spacing. One-tick spacing is therefore an explicit adapter
    choice; callers should report it as such rather than as a historical fact.
    """
    if base_bid is None or layers <= 0:
        return ()
    out: list[float] = []
    for i in range(int(layers)):
        px = tick_floor(base_bid - i * tick, tick)
        if px >= tick:
            out.append(round(px, 10))
    return tuple(out)


def projected_inventory(
    inv: Inventory,
    *,
    side: str,
    shares: float,
    price: float,
) -> Inventory:
    if shares < 0 or price < 0:
        raise ValueError("shares and price must be non-negative")
    side = side.upper()
    if side == "UP":
        return Inventory(
            up_shares=inv.up_shares + shares,
            down_shares=inv.down_shares,
            up_cost=inv.up_cost + shares * price,
            down_cost=inv.down_cost,
        )
    if side == "DOWN":
        return Inventory(
            up_shares=inv.up_shares,
            down_shares=inv.down_shares + shares,
            up_cost=inv.up_cost,
            down_cost=inv.down_cost + shares * price,
        )
    raise ValueError("side must be UP or DOWN")


def hard_gap_allows(inv: Inventory, *, side: str, shares: float, parent_clip: float) -> bool:
    """Emergency safety gate from the reconstruction (8 current clips)."""
    if parent_clip <= 0:
        return False
    projected = projected_inventory(inv, side=side, shares=shares, price=0.0)
    return gap_in_clips(projected, parent_clip) <= HARD_SAFETY_GAP_CLIPS + 1e-12


def projected_combined_vwap(
    inv: Inventory,
    *,
    side: str,
    shares: float,
    price: float,
) -> float | None:
    return projected_inventory(inv, side=side, shares=shares, price=price).combined_vwap


def settlement_value(inv: Inventory, winner: str) -> float:
    """Gross post-close value after merging matched pairs and redeeming residual winner."""
    winner = winner.upper()
    if winner not in ("UP", "DOWN"):
        raise ValueError("winner must be UP or DOWN")
    matched = inv.matched
    residual = inv.shares(winner) - matched
    return matched + max(0.0, residual)


def acquisition_spend(inv: Inventory) -> float:
    return inv.up_cost + inv.down_cost


def settlement_pnl(inv: Inventory, winner: str) -> float:
    return settlement_value(inv, winner) - acquisition_spend(inv)


def conservative_floor_pnl(inv: Inventory) -> float:
    """P&L if only matched complete sets are credited and all acquisition cost is charged."""
    return inv.matched - acquisition_spend(inv)
