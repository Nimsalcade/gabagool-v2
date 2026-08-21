"""Behavioral invariants for the forensic-calibrated policy."""
import math

from src.policy import (
    BookSide,
    InventoryState,
    adaptive_clip,
    basis_allows,
    maker_target,
    projected_combined_vwap,
    repair_clip,
    taker_should_fire,
)


def state(up=100, down=100, up_px=.48, down_px=.50, last_up=95, last_down=95, now=100, end=200):
    return InventoryState(
        up_shares=up,
        down_shares=down,
        up_cost=up * up_px,
        down_cost=down * down_px,
        last_up_fill_ts=last_up,
        last_down_fill_ts=last_down,
        now_ts=now,
        window_start_ts=0,
        seconds_to_end=end,
    )


def test_maker_skews_early_toward_deficient_leg_without_crossing():
    b = BookSide(.45, .48)
    normal = maker_target(b, tick=.01, inventory_relation="balanced", ratio=1.0)
    deficient = maker_target(b, tick=.01, inventory_relation="deficient", ratio=1.06)
    heavy = maker_target(b, tick=.01, inventory_relation="heavy", ratio=1.06)
    assert heavy <= normal <= deficient
    assert deficient <= .47


def test_aggregate_basis_not_fixed_pair_budget():
    s = state(up=100, down=100, up_px=.40, down_px=.54)
    projected = projected_combined_vwap(s, side="DOWN", price=.60, shares=10)
    assert projected is not None and projected < 1.0
    assert basis_allows(
        s, side="DOWN", price=.60, shares=10,
        max_combined_vwap=1.01, opposite_reference_price=.40,
    )


def test_taker_requires_deficient_leg_imbalance_and_staleness():
    mild = state(up=100, down=106, last_up=99, last_down=20, now=100, end=180)
    strong = state(up=100, down=130, last_up=99, last_down=40, now=100, end=180)
    pm = projected_combined_vwap(mild, side="UP", price=.49, shares=10)
    ps = projected_combined_vwap(strong, side="UP", price=.49, shares=20)
    assert not taker_should_fire(
        mild, candidate_side="UP", projected_basis=pm,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )
    assert taker_should_fire(
        strong, candidate_side="UP", projected_basis=ps,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )


def test_balanced_inventory_does_not_taker_chase_edge():
    s = state(up=100, down=100, up_px=.40, down_px=.54, last_up=10, last_down=10, now=100, end=180)
    p = projected_combined_vwap(s, side="UP", price=.41, shares=10)
    assert p is not None and p < .985
    assert not taker_should_fire(
        s, candidate_side="UP", projected_basis=p,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )


def test_small_missing_leg_deficit_is_left_to_maker_skew():
    s = state(up=0, down=10, last_up=None, last_down=50, now=100, end=180)
    p = projected_combined_vwap(s, side="UP", price=.40, shares=20)
    assert not taker_should_fire(
        s, candidate_side="UP", projected_basis=p,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )


def test_large_missing_leg_can_trigger_after_staleness():
    s = state(up=0, down=25, last_up=None, last_down=60, now=100, end=180)
    p = projected_combined_vwap(s, side="UP", price=.40, shares=20)
    assert taker_should_fire(
        s, candidate_side="UP", projected_basis=p,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )


def test_repair_clip_never_overshoots_deficit():
    s = state(up=100, down=118, now=100, end=180)
    assert repair_clip(s, side="UP", proposed_shares=20, min_order_shares=5) == 18
    assert repair_clip(s, side="DOWN", proposed_shares=20, min_order_shares=5) == 0


def test_taker_suppressed_near_close():
    s = state(up=100, down=150, last_up=0, last_down=0, now=100, end=1)
    p = projected_combined_vwap(s, side="UP", price=.49, shares=20)
    assert not taker_should_fire(
        s, candidate_side="UP", projected_basis=p,
        target_combined_vwap=.985, max_combined_vwap=1.01, taker_stop_buffer_s=2,
    )


def test_adaptive_clip_matches_observed_regime():
    base = adaptive_clip(
        base_clip_shares=10, max_clip_shares=40, min_order_shares=5,
        min_notional=1, price=.50, ratio=1.02, relation="balanced", aggressive=False,
    )
    repair = adaptive_clip(
        base_clip_shares=10, max_clip_shares=40, min_order_shares=5,
        min_notional=1, price=.50, ratio=1.30, relation="deficient", aggressive=True,
    )
    assert base == 10
    assert repair == 20


def test_missing_leg_is_infinite_ratio():
    s = state(up=0, down=100, last_up=None, last_down=99, now=100, end=180)
    assert math.isinf(s.larger_to_smaller_ratio)
    assert s.deficient_side == "UP"
