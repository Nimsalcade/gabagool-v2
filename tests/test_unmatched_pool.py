"""Weighted unmatched-cost pool: accounting only, no admission filter."""
from __future__ import annotations

from src.unmatched_pool import UnmatchedPool


def test_first_fill_is_all_overshoot():
    p = UnmatchedPool()
    n = p.apply_fill("UP", 10, 0.07)
    assert n.close_qty == 0
    assert abs(n.overshoot_qty - 10) < 1e-12
    assert n.repair_basis is None
    assert abs(n.unmatched_up_after - 10) < 1e-12
    assert n.unmatched_down_after == 0
    assert abs(n.unmatched_up_vwap_after - 0.07) < 1e-12


def test_lagging_fill_closes_cheap_unmatched_at_good_pair():
    p = UnmatchedPool()
    p.apply_fill("UP", 10, 0.07)
    n = p.apply_fill("DOWN", 10, 0.90)
    assert abs(n.close_qty - 10) < 1e-12
    assert n.overshoot_qty == 0
    assert abs(n.opposite_unmatched_vwap - 0.07) < 1e-12
    assert abs(n.repair_basis - 0.97) < 1e-12
    assert n.unmatched_up_after == 0
    assert n.unmatched_down_after == 0
    assert abs(n.completed_set_qty_cumulative - 10) < 1e-12
    assert abs(n.completed_set_cost_vwap_cumulative - 0.97) < 1e-12


def test_parent_clip_residue_crosses_neutral():
    p = UnmatchedPool()
    p.apply_fill("UP", 4, 0.07)
    n = p.apply_fill("DOWN", 10, 0.90)
    assert abs(n.close_qty - 4) < 1e-12
    assert abs(n.overshoot_qty - 6) < 1e-12
    assert abs(n.repair_basis - 0.97) < 1e-12
    assert n.unmatched_up_after == 0
    assert abs(n.unmatched_down_after - 6) < 1e-12
    assert abs(n.unmatched_down_vwap_after - 0.90) < 1e-12


def test_lifetime_vwap_is_not_the_repair_reference():
    """Cheap unmatched lots, not lifetime opposite VWAP, set repair_basis."""
    p = UnmatchedPool()
    p.apply_fill("UP", 90, 0.40)
    p.apply_fill("DOWN", 90, 0.55)  # complete the expensive bulk @ 0.95
    p.apply_fill("UP", 10, 0.07)    # leftover cheap unmatched
    n = p.apply_fill("DOWN", 10, 0.90)
    assert abs(n.repair_basis - 0.97) < 1e-12
    assert abs(n.completed_set_qty_cumulative - 100) < 1e-12
    # Completed VWAP is share-weighted: 90*0.95 + 10*0.97
    assert abs(n.completed_set_cost_vwap_cumulative - 0.952) < 1e-12


def test_exact_close_zeros_the_pool():
    p = UnmatchedPool()
    p.apply_fill("DOWN", 3.5, 0.51)
    n = p.apply_fill("UP", 3.5, 0.48)
    assert n.unmatched_up_after == 0
    assert n.unmatched_down_after == 0
    assert p.unmatched_up_cost == 0
    assert p.unmatched_down_cost == 0


def test_cannot_have_unmatched_on_both_sides():
    p = UnmatchedPool()
    p.apply_fill("UP", 8, 0.40)
    p.apply_fill("DOWN", 5, 0.55)
    assert p.unmatched_up > 0
    assert p.unmatched_down == 0
    p.apply_fill("DOWN", 10, 0.60)
    assert p.unmatched_up == 0
    assert p.unmatched_down > 0


def test_pool_never_rejects_a_bad_repair_basis():
    """Telemetry only: a 1.25 pair is recorded, not refused."""
    p = UnmatchedPool()
    p.apply_fill("UP", 10, 0.35)
    n = p.apply_fill("DOWN", 10, 0.90)
    assert abs(n.repair_basis - 1.25) < 1e-12
    assert abs(n.close_qty - 10) < 1e-12
