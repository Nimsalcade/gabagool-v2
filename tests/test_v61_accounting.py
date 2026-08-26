from __future__ import annotations

import pytest

from src.v6_lot_pair import FifoLotPool
from src.v61_accounting import accumulation_cutoff_age, build_session_accounting


def test_accumulation_stops_with_50_seconds_remaining() -> None:
    assert accumulation_cutoff_age(duration_s=900.0, stop_before_close_s=50.0) == pytest.approx(850.0)


def test_strict_session_pnl_is_merge_return_minus_all_fill_cost() -> None:
    pool = FifoLotPool()
    pool.apply_fill("DOWN", 10.0, 0.35)
    pool.apply_fill("UP", 20.0, 0.62)

    acct = build_session_accounting(
        pool=pool,
        up_filled_shares=20.0,
        up_fill_cost=20.0 * 0.62,
        down_filled_shares=10.0,
        down_fill_cost=10.0 * 0.35,
    )

    # All filled-share cost: 12.40 + 3.50 = 15.90.
    assert acct.total_fill_cost == pytest.approx(15.90)

    # Ten UP/DOWN pairs merge to exactly $10.
    assert acct.merge_qty == pytest.approx(10.0)
    assert acct.merge_return == pytest.approx(10.0)

    # Ten unmatched UP shares remain at 0.62, cost basis $6.20.
    assert acct.leftover_up_qty == pytest.approx(10.0)
    assert acct.leftover_down_qty == pytest.approx(0.0)
    assert acct.leftover_total_cost == pytest.approx(6.20)

    # Requested strict score: returned - total cost.
    assert acct.pnl == pytest.approx(10.0 - 15.90)
    assert acct.pnl == pytest.approx(-5.90)

    # Same result decomposes as locked pair edge minus leftover cost.
    assert acct.locked_complete_set_pnl == pytest.approx(0.30)
    assert acct.pnl == pytest.approx(acct.locked_complete_set_pnl - acct.leftover_total_cost)

    # Every dollar of every fill is assigned exactly once.
    assert acct.accounting_identity_error == pytest.approx(0.0, abs=1e-12)


def test_no_leftovers_reports_only_complete_set_edge() -> None:
    pool = FifoLotPool()
    pool.apply_fill("DOWN", 10.0, 0.35)
    pool.apply_fill("UP", 10.0, 0.62)

    acct = build_session_accounting(
        pool=pool,
        up_filled_shares=10.0,
        up_fill_cost=6.20,
        down_filled_shares=10.0,
        down_fill_cost=3.50,
    )

    assert acct.total_fill_cost == pytest.approx(9.70)
    assert acct.merge_return == pytest.approx(10.0)
    assert acct.leftover_total_cost == pytest.approx(0.0)
    assert acct.pnl == pytest.approx(0.30)
    assert acct.roi_on_session_cost == pytest.approx(0.30 / 9.70)
    assert acct.accounting_identity_error == pytest.approx(0.0, abs=1e-12)
