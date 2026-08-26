"""V6 lot-pair accounting and price-ladder helpers.

This module implements the simplified profitability-first hypothesis:

* acquire a small cheap-side seed lot;
* while one side is unmatched, work only the opposite side;
* cap every completion bid from the cost of the currently unmatched opposite lots;
* allow a small amount of completion overbooking so fills can overshoot and rotate
  the unmatched side;
* net fills FIFO so every completed set has an explicit traceable pair cost.

The exact private Gabagool cancel/reprice algorithm is not observable.  V6 is a
clean live-paper experiment of the lot-pair hypothesis, not a historical-source
claim.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Iterable

from src.unmatched_pool import FillNetting

_EPS = 1e-12


@dataclass
class Lot:
    qty: float
    price: float


class FifoLotPool:
    """FIFO unmatched lots with the same public interface as ``UnmatchedPool``.

    The existing paper harness can therefore use this pool without changing its
    accounting/summary code.  ``completed_cost`` is the actual sum of the two
    legs that were paired, so ``completed_qty - completed_cost`` is locked
    complete-set PnL before fees/rebates.
    """

    def __init__(self) -> None:
        self.up_lots: Deque[Lot] = deque()
        self.down_lots: Deque[Lot] = deque()
        self.completed_qty = 0.0
        self.completed_cost = 0.0

    def _lots(self, side: str) -> Deque[Lot]:
        if side == "UP":
            return self.up_lots
        if side == "DOWN":
            return self.down_lots
        raise ValueError(f"side must be UP or DOWN, got {side!r}")

    @staticmethod
    def _qty(lots: Iterable[Lot]) -> float:
        return sum(x.qty for x in lots)

    @staticmethod
    def _cost(lots: Iterable[Lot]) -> float:
        return sum(x.qty * x.price for x in lots)

    @property
    def unmatched_up(self) -> float:
        return self._qty(self.up_lots)

    @property
    def unmatched_down(self) -> float:
        return self._qty(self.down_lots)

    @property
    def unmatched_up_cost(self) -> float:
        return self._cost(self.up_lots)

    @property
    def unmatched_down_cost(self) -> float:
        return self._cost(self.down_lots)

    def unmatched_vwap(self, side: str) -> float | None:
        lots = self._lots(side)
        qty = self._qty(lots)
        if qty <= _EPS:
            return None
        return self._cost(lots) / qty

    def max_unmatched_price(self, side: str) -> float | None:
        lots = self._lots(side)
        if not lots:
            return None
        return max(x.price for x in lots)

    @property
    def completed_vwap(self) -> float | None:
        if self.completed_qty <= _EPS:
            return None
        return self.completed_cost / self.completed_qty

    @property
    def locked_pnl(self) -> float:
        return self.completed_qty - self.completed_cost

    @property
    def residual_cost(self) -> float:
        return self.unmatched_up_cost + self.unmatched_down_cost

    def snapshot_before(self) -> dict[str, float | None]:
        return {
            "unmatched_up": self.unmatched_up,
            "unmatched_down": self.unmatched_down,
            "unmatched_up_vwap": self.unmatched_vwap("UP"),
            "unmatched_down_vwap": self.unmatched_vwap("DOWN"),
        }

    def apply_fill(self, side: str, qty: float, price: float) -> FillNetting:
        if side not in ("UP", "DOWN"):
            raise ValueError(f"side must be UP or DOWN, got {side!r}")
        if qty <= 0:
            raise ValueError("qty must be positive")

        opp = "DOWN" if side == "UP" else "UP"
        own_lots = self._lots(side)
        opp_lots = self._lots(opp)

        before_up = self.unmatched_up
        before_dn = self.unmatched_down
        before_up_vwap = self.unmatched_vwap("UP")
        before_dn_vwap = self.unmatched_vwap("DOWN")

        remaining = float(qty)
        close_qty = 0.0
        consumed_opp_cost = 0.0

        while remaining > _EPS and opp_lots:
            lot = opp_lots[0]
            take = min(remaining, lot.qty)
            close_qty += take
            consumed_opp_cost += take * lot.price
            remaining -= take
            lot.qty -= take
            if lot.qty <= _EPS:
                opp_lots.popleft()

        opp_consumed_vwap = (
            consumed_opp_cost / close_qty if close_qty > _EPS else None
        )
        repair_basis = (
            float(price) + opp_consumed_vwap
            if opp_consumed_vwap is not None
            else None
        )

        if close_qty > _EPS:
            self.completed_qty += close_qty
            self.completed_cost += close_qty * float(price) + consumed_opp_cost

        overshoot = max(0.0, remaining)
        if overshoot > _EPS:
            own_lots.append(Lot(overshoot, float(price)))
        else:
            overshoot = 0.0

        return FillNetting(
            close_qty=close_qty,
            overshoot_qty=overshoot,
            opposite_unmatched_vwap=opp_consumed_vwap,
            repair_basis=repair_basis,
            unmatched_up_before=before_up,
            unmatched_down_before=before_dn,
            unmatched_up_vwap_before=before_up_vwap,
            unmatched_down_vwap_before=before_dn_vwap,
            unmatched_up_after=self.unmatched_up,
            unmatched_down_after=self.unmatched_down,
            unmatched_up_vwap_after=self.unmatched_vwap("UP"),
            unmatched_down_vwap_after=self.unmatched_vwap("DOWN"),
            completed_set_qty_cumulative=self.completed_qty,
            completed_set_cost_vwap_cumulative=self.completed_vwap,
        )


def floor_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("tick must be positive")
    n = math.floor((float(value) + 1e-12) / float(tick))
    return round(n * float(tick), 10)


def completion_ceiling(
    *, pair_cap: float, worst_opposite_lot_price: float, tick: float
) -> float | None:
    """Highest opposite-side bid that keeps every matched lot <= ``pair_cap``.

    V6 deliberately uses the *most expensive* currently unmatched opposite lot.
    That is stricter than VWAP pricing, but it guarantees that any matched share
    produced before the next reconciliation cannot exceed the configured cap.
    """
    raw = float(pair_cap) - float(worst_opposite_lot_price)
    px = floor_to_tick(raw, tick)
    if px < tick - 1e-12:
        return None
    return min(round(1.0 - tick, 10), px)


def passive_bid_top(
    *,
    best_bid: float | None,
    best_ask: float | None,
    tick: float,
    max_price: float | None = None,
) -> float | None:
    """One-tick-improved post-only bid, optionally capped by lot economics."""
    if best_ask is None or tick <= 0:
        return None
    passive_limit = floor_to_tick(float(best_ask) - tick, tick)
    if passive_limit < tick - 1e-12:
        return None

    if best_bid is None:
        top = passive_limit
    else:
        top = min(passive_limit, floor_to_tick(float(best_bid) + tick, tick))

    if max_price is not None:
        top = min(top, floor_to_tick(max_price, tick))
    if top < tick - 1e-12:
        return None
    return round(top, 10)


def descending_ladder(top: float | None, *, tick: float, levels: int) -> tuple[float, ...]:
    if top is None or levels <= 0:
        return ()
    out: list[float] = []
    for i in range(int(levels)):
        px = round(float(top) - i * float(tick), 10)
        if px < tick - 1e-12:
            break
        out.append(px)
    return tuple(out)


def completion_parent_count(
    *, unmatched_qty: float, parent_clip: float, ladder_levels: int, overbook_clips: int
) -> int:
    if unmatched_qty <= _EPS or parent_clip <= _EPS or ladder_levels <= 0:
        return 0
    covered = int(math.ceil((unmatched_qty - _EPS) / parent_clip))
    return max(
        1,
        min(int(ladder_levels), covered + max(0, int(overbook_clips))),
    )
