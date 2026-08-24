"""Tests for the maximum-identifiable BTC/ETH 15-minute reconstruction."""
from src.forensic_15m import (
    HARD_SAFETY_GAP_CLIPS,
    Inventory,
    clip_for_age,
    complementary_base_bid,
    conservative_floor_pnl,
    desired_layer_count,
    gap_in_clips,
    hard_gap_allows,
    heavy_layer_count,
    layer_prices,
    projected_combined_vwap,
    settlement_pnl,
    settlement_value,
)


def test_exact_15m_age_clip_schedule():
    assert clip_for_age(17.999) == 0
    assert clip_for_age(18) == 10
    assert clip_for_age(211.999) == 10
    assert clip_for_age(212) == 9
    assert clip_for_age(381.999) == 9
    assert clip_for_age(382) == 8
    assert clip_for_age(521.999) == 8
    assert clip_for_age(522) == 7
    assert clip_for_age(663.999) == 7
    assert clip_for_age(664) == 6
    assert clip_for_age(817.999) == 6
    assert clip_for_age(818) == 5
    assert clip_for_age(898.999) == 5
    assert clip_for_age(899) == 0


def test_complementary_ask_base_bid_is_post_only_and_cent_floored():
    # Opposite ask .57 -> raw complement .43; own ask .46 permits .45 max.
    assert complementary_base_bid(
        own_best_ask=.46, opposite_best_ask=.57, tick=.01
    ) == .43

    # Opposite ask .50 -> raw .50, but own ask .49 means post-only cap .48.
    assert complementary_base_bid(
        own_best_ask=.49, opposite_best_ask=.50, tick=.01
    ) == .48

    # Fractional displayed ask floors to venue tick.
    assert complementary_base_bid(
        own_best_ask=.60, opposite_best_ask=.563, tick=.01
    ) == .43


def test_one_tick_layer_adapter():
    assert layer_prices(.43, tick=.01, layers=4) == (.43, .42, .41, .40)
    assert layer_prices(.02, tick=.01, layers=4) == (.02, .01)
    assert layer_prices(None, tick=.01, layers=4) == ()


def test_reconstructed_inventory_layer_schedule():
    clip = 10
    balanced = Inventory(up_shares=100, down_shares=100)
    assert desired_layer_count(balanced, "UP", clip) == 4
    assert desired_layer_count(balanced, "DOWN", clip) == 4

    # UP is heavy, DOWN is underweight.
    mild = Inventory(up_shares=109, down_shares=100)
    assert gap_in_clips(mild, clip) == .9
    assert desired_layer_count(mild, "DOWN", clip) == 4
    assert desired_layer_count(mild, "UP", clip) == 3

    bigger = Inventory(up_shares=126, down_shares=100)
    assert desired_layer_count(bigger, "DOWN", clip) == 4
    assert desired_layer_count(bigger, "UP", clip) == 1

    stopped = Inventory(up_shares=136, down_shares=100)
    assert desired_layer_count(stopped, "DOWN", clip) == 4
    assert desired_layer_count(stopped, "UP", clip) == 0


def test_heavy_layer_boundaries():
    assert heavy_layer_count(.49) == 4
    assert heavy_layer_count(.50) == 3
    assert heavy_layer_count(1.49) == 3
    assert heavy_layer_count(1.50) == 2
    assert heavy_layer_count(2.49) == 2
    assert heavy_layer_count(2.50) == 1
    assert heavy_layer_count(3.49) == 1
    assert heavy_layer_count(3.50) == 0


def test_eight_clip_hard_safety_gap():
    inv = Inventory(up_shares=100, down_shares=30)
    # 70-share gap, 10-share parent clip. One more UP would make exactly 8 clips.
    assert hard_gap_allows(inv, side="UP", shares=10, parent_clip=10)
    assert not hard_gap_allows(inv, side="UP", shares=11, parent_clip=10)
    assert HARD_SAFETY_GAP_CLIPS == 8.0


def test_projected_combined_vwap_uses_cumulative_side_costs():
    inv = Inventory(
        up_shares=100, down_shares=100,
        up_cost=40, down_cost=54,
    )
    projected = projected_combined_vwap(inv, side="DOWN", shares=10, price=.60)
    expected = .40 + ((54 + 6) / 110)
    assert projected is not None
    assert abs(projected - expected) < 1e-12


def test_complete_set_settlement_identity():
    inv = Inventory(
        up_shares=899.628037,
        down_shares=910.976041,
        up_cost=568.601172,
        down_cost=269.866467,
    )
    # DOWN winner example from the reconstructed day.
    assert abs(settlement_value(inv, "DOWN") - 910.976041) < 1e-9
    assert abs(settlement_pnl(inv, "DOWN") - 72.508402) < 1e-6
    assert abs(conservative_floor_pnl(inv) - 61.160398) < 1e-6
