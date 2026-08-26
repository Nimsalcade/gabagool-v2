from __future__ import annotations

import pytest

from src.v6_lot_pair import (
    FifoLotPool,
    completion_ceiling,
    completion_parent_count,
    descending_ladder,
    passive_bid_top,
)


def test_fifo_pair_then_flip() -> None:
    p = FifoLotPool()

    first = p.apply_fill("DOWN", 10.0, 0.35)
    assert first.close_qty == 0
    assert p.unmatched_down == pytest.approx(10.0)

    second = p.apply_fill("UP", 20.0, 0.62)
    assert second.close_qty == pytest.approx(10.0)
    assert second.overshoot_qty == pytest.approx(10.0)
    assert p.completed_qty == pytest.approx(10.0)
    assert p.completed_vwap == pytest.approx(0.97)
    assert p.locked_pnl == pytest.approx(0.30)
    assert p.unmatched_up == pytest.approx(10.0)
    assert p.unmatched_down == pytest.approx(0.0)
    assert p.unmatched_vwap("UP") == pytest.approx(0.62)


def test_fifo_multiple_lots_have_exact_completed_cost() -> None:
    p = FifoLotPool()
    p.apply_fill("DOWN", 10.0, 0.35)
    p.apply_fill("DOWN", 10.0, 0.40)
    out = p.apply_fill("UP", 15.0, 0.59)

    assert out.close_qty == pytest.approx(15.0)
    expected_cost = 10 * (0.35 + 0.59) + 5 * (0.40 + 0.59)
    assert p.completed_cost == pytest.approx(expected_cost)
    assert p.completed_vwap == pytest.approx(expected_cost / 15.0)
    assert p.unmatched_down == pytest.approx(5.0)
    assert p.max_unmatched_price("DOWN") == pytest.approx(0.40)


def test_completion_ceiling_35_cent_lot() -> None:
    assert completion_ceiling(
        pair_cap=0.99,
        worst_opposite_lot_price=0.35,
        tick=0.01,
    ) == pytest.approx(0.64)


def test_completion_ceiling_floors_to_tick() -> None:
    assert completion_ceiling(
        pair_cap=0.985,
        worst_opposite_lot_price=0.351,
        tick=0.01,
    ) == pytest.approx(0.63)


def test_passive_top_is_post_only_and_economic_capped() -> None:
    assert passive_bid_top(
        best_bid=0.62,
        best_ask=0.65,
        tick=0.01,
        max_price=0.64,
    ) == pytest.approx(0.63)

    assert passive_bid_top(
        best_bid=0.64,
        best_ask=0.65,
        tick=0.01,
        max_price=0.64,
    ) == pytest.approx(0.64)


def test_descending_ladder() -> None:
    assert descending_ladder(0.64, tick=0.01, levels=4) == pytest.approx(
        (0.64, 0.63, 0.62, 0.61)
    )


def test_completion_parent_count_allows_controlled_overbook() -> None:
    assert completion_parent_count(
        unmatched_qty=10.0,
        parent_clip=10.0,
        ladder_levels=4,
        overbook_clips=2,
    ) == 3
    assert completion_parent_count(
        unmatched_qty=20.0,
        parent_clip=10.0,
        ladder_levels=4,
        overbook_clips=2,
    ) == 4
