import math

from src.reversal_pair import (
    Phase,
    ReversalState,
    TopOfBook,
    build_pair_fill,
    estimated_effective_pair_cost,
    taker_fee_usdc,
)


def top(bid, ask, size=500, min_order=5):
    return TopOfBook(bid, ask, size, min_order)


def test_fee_matches_documented_formula():
    # 100 crypto shares at 0.50 with feeRate 0.07 -> $1.75.
    assert taker_fee_usdc(100, 0.50, 0.07) == 1.75
    assert taker_fee_usdc(100, 0.40, 0.07) == 1.68


def test_reversal_gate_tracks_same_side():
    s = ReversalState(leader_threshold=0.65, collapse_threshold=0.40)
    assert s.observe(up_mid=0.66, down_mid=0.34, ts=10)[0].kind == "LEADER"
    assert s.phase is Phase.SEEK_COLLAPSE
    assert s.leader_side == "UP"
    assert s.observe(up_mid=0.52, down_mid=0.48, ts=20) == []
    events = s.observe(up_mid=0.39, down_mid=0.61, ts=30)
    assert events[0].kind == "COLLAPSE"
    assert s.phase is Phase.ARMED
    assert s.collapse_mid == 0.39


def test_other_side_move_does_not_arm_original_leader():
    s = ReversalState(leader_threshold=0.65, collapse_threshold=0.40)
    s.observe(up_mid=0.66, down_mid=0.34, ts=10)
    s.observe(up_mid=0.55, down_mid=0.39, ts=20)
    assert s.phase is Phase.SEEK_COLLAPSE


def test_pair_rejects_cost_above_threshold():
    u = top(0.49, 0.50)
    d = top(0.49, 0.50)
    assert estimated_effective_pair_cost(0.50, 0.50, 0.07) > 1.0
    assert build_pair_fill(
        up=u,
        down=d,
        fee_rate=0.07,
        max_effective_pair_cost=0.97,
        capital_limit_usd=100,
        min_pair_shares=5,
    ) is None


def test_pair_accepts_profitable_full_liquidity_snapshot():
    # Deliberately mispriced example: fee-adjusted cost stays well under 0.97.
    u = top(0.36, 0.37, size=200)
    d = top(0.51, 0.52, size=200)
    fill = build_pair_fill(
        up=u,
        down=d,
        fee_rate=0.07,
        max_effective_pair_cost=0.97,
        capital_limit_usd=100,
        min_pair_shares=5,
    )
    assert fill is not None
    assert fill.total_cost <= 100 + 1e-9
    assert fill.effective_pair_cost <= 0.97
    assert fill.locked_profit > 0
    assert math.isclose(fill.payout_value, fill.shares)


def test_pair_is_capped_by_weakest_ask_size():
    u = top(0.36, 0.37, size=200)
    d = top(0.51, 0.52, size=7.25)
    fill = build_pair_fill(
        up=u,
        down=d,
        fee_rate=0.07,
        max_effective_pair_cost=0.97,
        capital_limit_usd=100,
        min_pair_shares=5,
        share_precision=3,
    )
    assert fill is not None
    assert fill.shares == 7.25


def test_pair_rejects_insufficient_displayed_size():
    u = top(0.36, 0.37, size=4.99)
    d = top(0.51, 0.52, size=100)
    assert build_pair_fill(
        up=u,
        down=d,
        fee_rate=0.07,
        max_effective_pair_cost=0.97,
        capital_limit_usd=100,
        min_pair_shares=5,
    ) is None
