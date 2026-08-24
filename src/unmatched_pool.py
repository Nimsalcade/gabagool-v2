"""Weighted unmatched-cost pool for complete-set paper accounting.

Strategy-neutral.  This module does not admit, reject, size, or price orders.
It only nets a fill against currently unmatched opposite inventory using a
single weighted cost pool (FIFO/LIFO/nearest produced nearly identical Oct-29
headlines, so no private lot convention is claimed).

Lifetime UP/DOWN totals remain the caller's responsibility.  This pool tracks
only the residual unmatched legs.
"""
from __future__ import annotations

from dataclasses import dataclass


def _vwap(shares: float, cost: float) -> float | None:
    if shares <= 1e-12:
        return None
    return cost / shares


@dataclass
class FillNetting:
    close_qty: float
    overshoot_qty: float
    opposite_unmatched_vwap: float | None
    repair_basis: float | None
    unmatched_up_before: float
    unmatched_down_before: float
    unmatched_up_vwap_before: float | None
    unmatched_down_vwap_before: float | None
    unmatched_up_after: float
    unmatched_down_after: float
    unmatched_up_vwap_after: float | None
    unmatched_down_vwap_after: float | None
    completed_set_qty_cumulative: float
    completed_set_cost_vwap_cumulative: float | None


@dataclass
class UnmatchedPool:
    """Weighted unmatched UP/DOWN cost pools plus completed-set accumulator."""

    unmatched_up: float = 0.0
    unmatched_up_cost: float = 0.0
    unmatched_down: float = 0.0
    unmatched_down_cost: float = 0.0
    completed_qty: float = 0.0
    completed_cost: float = 0.0  # sum(pair_cost * close_qty)

    def unmatched_vwap(self, side: str) -> float | None:
        if side == "UP":
            return _vwap(self.unmatched_up, self.unmatched_up_cost)
        return _vwap(self.unmatched_down, self.unmatched_down_cost)

    @property
    def completed_vwap(self) -> float | None:
        return _vwap(self.completed_qty, self.completed_cost)

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
        before_up, before_dn = self.unmatched_up, self.unmatched_down
        before_up_vwap = self.unmatched_vwap("UP")
        before_dn_vwap = self.unmatched_vwap("DOWN")

        opp_shares = self.unmatched_down if opp == "DOWN" else self.unmatched_up
        opp_cost = self.unmatched_down_cost if opp == "DOWN" else self.unmatched_up_cost
        opp_vwap = _vwap(opp_shares, opp_cost)

        close_qty = min(qty, opp_shares)
        repair_basis: float | None = None
        if close_qty > 1e-12 and opp_vwap is not None:
            repair_basis = price + opp_vwap
            if close_qty + 1e-12 >= opp_shares:
                new_shares, new_cost = 0.0, 0.0
            else:
                new_shares = opp_shares - close_qty
                new_cost = opp_cost - close_qty * opp_vwap
            if opp == "DOWN":
                self.unmatched_down, self.unmatched_down_cost = new_shares, new_cost
            else:
                self.unmatched_up, self.unmatched_up_cost = new_shares, new_cost
            self.completed_qty += close_qty
            self.completed_cost += close_qty * repair_basis
        else:
            close_qty = 0.0

        overshoot = qty - close_qty
        if overshoot > 1e-12:
            if side == "UP":
                self.unmatched_up += overshoot
                self.unmatched_up_cost += overshoot * price
            else:
                self.unmatched_down += overshoot
                self.unmatched_down_cost += overshoot * price
        else:
            overshoot = 0.0

        return FillNetting(
            close_qty=close_qty,
            overshoot_qty=overshoot,
            opposite_unmatched_vwap=opp_vwap,
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
