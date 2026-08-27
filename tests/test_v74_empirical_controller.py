from src.v74_empirical_controller import (
    empirical_underweight_share_target,
    fresh_overweight_allowance_shares,
    fresh_overweight_post_allowed,
)


def test_flow_bias_strengthens_with_gap():
    ps = [
        empirical_underweight_share_target(gap_clips=g, age_s=60).underweight_share_target
        for g in (1, 3, 6, 12, 18)
    ]
    assert ps == sorted(ps)
    assert ps[0] >= 0.55
    assert ps[-1] >= 0.95


def test_late_pressure_strengthens_same_gap():
    early = empirical_underweight_share_target(gap_clips=4, age_s=60)
    late = empirical_underweight_share_target(gap_clips=4, age_s=870)
    assert late.underweight_share_target > early.underweight_share_target
    assert late.overweight_to_underweight_ratio < early.overweight_to_underweight_ratio


def test_hard_gap_shuts_fresh_overweight():
    allowance, flow = fresh_overweight_allowance_shares(
        gap_clips=22,
        age_s=300,
        underweight_filled_since_regime=100,
        parent_clip=14,
    )
    assert flow.hard_stop is True
    assert allowance == 0


def test_no_repair_fills_cannot_replenish_forever():
    allowance, _ = fresh_overweight_allowance_shares(
        gap_clips=3,
        age_s=120,
        underweight_filled_since_regime=0,
        parent_clip=14,
        initial_overweight_parents=1,
    )
    assert allowance == 14
    assert fresh_overweight_post_allowed(
        already_posted_shares=0, next_parent_shares=14, allowance_shares=allowance
    )
    assert not fresh_overweight_post_allowed(
        already_posted_shares=14, next_parent_shares=14, allowance_shares=allowance
    )


def test_underweight_fills_earn_some_overweight_budget():
    a0, _ = fresh_overweight_allowance_shares(
        gap_clips=3,
        age_s=120,
        underweight_filled_since_regime=0,
        parent_clip=14,
    )
    a1, _ = fresh_overweight_allowance_shares(
        gap_clips=3,
        age_s=120,
        underweight_filled_since_regime=42,
        parent_clip=14,
    )
    assert a1 > a0


def test_large_gap_earns_less_overweight_budget_than_small_gap():
    small, _ = fresh_overweight_allowance_shares(
        gap_clips=3,
        age_s=120,
        underweight_filled_since_regime=56,
        parent_clip=14,
    )
    large, _ = fresh_overweight_allowance_shares(
        gap_clips=12,
        age_s=120,
        underweight_filled_since_regime=56,
        parent_clip=14,
    )
    assert large < small
