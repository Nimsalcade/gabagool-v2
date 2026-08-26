"""V6.1 session accounting for the lot-pair paper strategy.

The profitability score is intentionally settlement-independent and merge-only:

    session_cost = cost of every filled UP and DOWN share
    merge_return = matched complete-set quantity * $1
    leftover_cost = cost basis of all unmatched shares
    pnl = merge_return - session_cost

Because session_cost already includes the unmatched shares, leftover_cost is a
breakdown/penalty component, not something added to session_cost a second time.
The accounting identity is:

    session_cost == matched_pair_cost_basis + leftover_cost

and therefore:

    pnl == locked_complete_set_pnl - leftover_cost

This is the strict metric requested for strategy evaluation: any residual shares
are treated as unrecovered cost rather than credited with a future winner value.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.v6_lot_pair import FifoLotPool

_EPS = 1e-9


@dataclass(frozen=True)
class SessionAccounting:
    total_fill_cost: float
    up_fill_cost: float
    down_fill_cost: float
    up_filled_shares: float
    down_filled_shares: float
    total_filled_shares: float
    merge_qty: float
    merge_return: float
    merge_cost_basis: float
    completed_pair_vwap: float | None
    locked_complete_set_pnl: float
    leftover_up_qty: float
    leftover_down_qty: float
    leftover_total_qty: float
    leftover_up_cost: float
    leftover_down_cost: float
    leftover_total_cost: float
    returned_total: float
    pnl: float
    roi_on_session_cost: float | None
    accounting_identity_error: float


def accumulation_cutoff_age(
    *, duration_s: float = 900.0, stop_before_close_s: float = 50.0
) -> float:
    """Return the market age at which new accumulation must stop."""
    duration = float(duration_s)
    stop = float(stop_before_close_s)
    if duration <= 0:
        raise ValueError("duration_s must be positive")
    if stop < 0 or stop >= duration:
        raise ValueError("stop_before_close_s must be in [0, duration_s)")
    return duration - stop


def build_session_accounting(
    *,
    pool: FifoLotPool,
    up_filled_shares: float,
    up_fill_cost: float,
    down_filled_shares: float,
    down_fill_cost: float,
) -> SessionAccounting:
    """Build exact merge-only PnL from all fills and FIFO unmatched lots."""
    up_qty = float(up_filled_shares)
    dn_qty = float(down_filled_shares)
    up_cost = float(up_fill_cost)
    dn_cost = float(down_fill_cost)

    total_cost = up_cost + dn_cost
    merge_qty = float(pool.completed_qty)
    merge_return = merge_qty  # one complete binary set returns exactly $1
    merge_cost_basis = float(pool.completed_cost)

    leftover_up_qty = float(pool.unmatched_up)
    leftover_down_qty = float(pool.unmatched_down)
    leftover_up_cost = float(pool.unmatched_up_cost)
    leftover_down_cost = float(pool.unmatched_down_cost)
    leftover_cost = leftover_up_cost + leftover_down_cost

    returned_total = merge_return
    pnl = returned_total - total_cost
    roi = None if total_cost <= _EPS else pnl / total_cost
    identity_error = total_cost - (merge_cost_basis + leftover_cost)

    return SessionAccounting(
        total_fill_cost=total_cost,
        up_fill_cost=up_cost,
        down_fill_cost=dn_cost,
        up_filled_shares=up_qty,
        down_filled_shares=dn_qty,
        total_filled_shares=up_qty + dn_qty,
        merge_qty=merge_qty,
        merge_return=merge_return,
        merge_cost_basis=merge_cost_basis,
        completed_pair_vwap=pool.completed_vwap,
        locked_complete_set_pnl=merge_return - merge_cost_basis,
        leftover_up_qty=leftover_up_qty,
        leftover_down_qty=leftover_down_qty,
        leftover_total_qty=leftover_up_qty + leftover_down_qty,
        leftover_up_cost=leftover_up_cost,
        leftover_down_cost=leftover_down_cost,
        leftover_total_cost=leftover_cost,
        returned_total=returned_total,
        pnl=pnl,
        roi_on_session_cost=roi,
        accounting_identity_error=identity_error,
    )
