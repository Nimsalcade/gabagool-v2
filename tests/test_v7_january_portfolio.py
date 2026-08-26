from src.v7_january_portfolio import (
    effective_portfolio_cap,
    max_buy_price_for_portfolio_cap,
    parent_targets,
    passive_top,
    stacked_ladder,
)


def test_portfolio_cap_allows_expensive_underweight_repair_from_cheap_opposite_inventory():
    # 10 UP @ .22 means a fresh DOWN clip can be expensive while still keeping
    # aggregate side VWAP economics below .995.
    px = max_buy_price_for_portfolio_cap(
        side="DOWN",
        add_qty=10,
        up_shares=10,
        up_cost=2.2,
        down_shares=0,
        down_cost=0,
        combined_cap=0.995,
        absolute_max=0.92,
    )
    # First DOWN fill has no own-side history: .995 - .22 = .775.
    assert abs(px - 0.775) < 1e-9


def test_portfolio_cap_uses_existing_side_vwap_not_fifo_lot_pairing():
    # UP VWAP=.40, DOWN VWAP=.50. Another 10 DOWN shares may be bought up to
    # .59 while projected DOWN VWAP becomes .595? Solve exact portfolio bound.
    px = max_buy_price_for_portfolio_cap(
        side="DOWN",
        add_qty=10,
        up_shares=20,
        up_cost=8.0,
        down_shares=10,
        down_cost=5.0,
        combined_cap=0.995,
        absolute_max=0.92,
    )
    # target DOWN VWAP=.595 => (5 + 10*p)/20=.595 => p=.69
    assert abs(px - 0.69) < 1e-9


def test_underweight_side_gets_more_parents_but_overweight_stays_live():
    t = parent_targets(
        up_shares=140,
        down_shares=84,
        parent_clip=14,
        age_s=300,
        base_parents=4,
        max_parents=12,
        min_overweight_parents=1,
        hard_gap_clips=22,
    )
    assert t.underweight == "DOWN"
    assert t.down > t.up
    assert t.up >= 1


def test_hard_gap_turns_off_overweight_side():
    t = parent_targets(
        up_shares=14 * 22,
        down_shares=0,
        parent_clip=14,
        age_s=500,
        base_parents=4,
        max_parents=12,
        min_overweight_parents=1,
        hard_gap_clips=22,
    )
    assert t.underweight == "DOWN"
    assert t.up == 0
    assert t.down == 12


def test_late_pressure_increases_underweight_target_or_holds_at_cap():
    early = parent_targets(
        up_shares=98,
        down_shares=42,
        parent_clip=14,
        age_s=100,
        base_parents=4,
        max_parents=12,
    )
    late = parent_targets(
        up_shares=98,
        down_shares=42,
        parent_clip=14,
        age_s=850,
        base_parents=4,
        max_parents=12,
    )
    assert late.down >= early.down
    assert late.up <= early.up


def test_effective_cap_relaxes_only_late():
    assert effective_portfolio_cap(
        age_s=500, duration_s=900, base_cap=.995, late_cap=1.0, late_start_s=600
    ) == .995
    mid = effective_portfolio_cap(
        age_s=750, duration_s=900, base_cap=.995, late_cap=1.0, late_start_s=600
    )
    end = effective_portfolio_cap(
        age_s=900, duration_s=900, base_cap=.995, late_cap=1.0, late_start_s=600
    )
    assert .995 < mid < 1.0
    assert abs(end - 1.0) < 1e-12


def test_passive_top_respects_post_only_and_economic_max():
    px = passive_top(
        best_bid=.50,
        best_ask=.53,
        tick=.01,
        max_price=.505,
        improve_ticks=1,
    )
    assert px == .50


def test_stacked_ladder_repeats_price_levels_as_independent_parents():
    xs = stacked_ladder(top=.50, tick=.01, levels=3, parents=8)
    assert xs == (.50, .49, .48, .50, .49, .48, .50, .49)
