from decimal import Decimal as D

from src.reversal_complete_set import (
    FeeSchedule,
    Level,
    ReversalGate,
    StrategyState,
    buy_execution,
    max_profitable_pair,
    quote_pair,
)


def test_crypto_fee_matches_published_half_price_example():
    fee = FeeSchedule(rate=D("0.07"))
    assert fee.fee(D("100"), D("0.50")) == D("1.75000")


def test_raw_38_55_pair_is_fee_adjusted_before_admission():
    fee = FeeSchedule(rate=D("0.07"))
    q = quote_pair(
        [(D("0.38"), D("200"))],
        [(D("0.55"), D("200"))],
        D("100"),
        fee,
    )
    assert q is not None
    assert q.raw_pair_basis == D("0.93")
    assert q.fee_adjusted_pair_basis == D("0.963817")
    assert q.locked_profit == D("3.618300")


def test_pair_above_fee_adjusted_cap_is_rejected():
    fee = FeeSchedule(rate=D("0.07"))
    q = max_profitable_pair(
        [(D("0.40"), D("200"))],
        [(D("0.55"), D("200"))],
        budget=D("100"),
        pair_cap=D("0.97"),
        fee_schedule=fee,
        min_shares=D("5"),
    )
    assert q is None


def test_max_pair_respects_100_dollar_budget_and_equal_depth():
    fee = FeeSchedule(rate=D("0.07"))
    q = max_profitable_pair(
        [(D("0.38"), D("200"))],
        [(D("0.55"), D("200"))],
        budget=D("100"),
        pair_cap=D("0.97"),
        fee_schedule=fee,
        min_shares=D("5"),
    )
    assert q is not None
    assert q.total_cost <= D("100.0000001")
    assert q.fee_adjusted_pair_basis <= D("0.97")
    assert q.shares > D("103")
    assert q.locked_profit > D("3")


def test_depth_limited_pair_uses_only_common_equal_shares():
    fee = FeeSchedule(rate=D("0.07"))
    q = max_profitable_pair(
        [(D("0.30"), D("12"))],
        [(D("0.60"), D("50"))],
        budget=D("100"),
        pair_cap=D("0.97"),
        fee_schedule=fee,
        min_shares=D("5"),
    )
    assert q is not None
    assert q.shares == D("12.0000")
    assert q.up.filled == q.down.filled == D("12.0000")


def test_gate_requires_same_side_to_reverse():
    gate = ReversalGate()
    events = gate.observe(now=1, up_best_ask=D("0.66"), down_best_ask=D("0.35"))
    assert events == ["HIGH_ARMED"]
    assert gate.high_side == "UP"
    assert gate.state == StrategyState.WAIT_REVERSAL

    events = gate.observe(now=2, up_best_ask=D("0.50"), down_best_ask=D("0.30"))
    assert events == []
    assert gate.state == StrategyState.WAIT_REVERSAL

    events = gate.observe(now=3, up_best_ask=D("0.40"), down_best_ask=D("0.60"))
    assert events == ["REVERSAL_CONFIRMED"]
    assert gate.state == StrategyState.PAIR_HUNT


def test_gate_trade_and_expiry_are_terminal():
    gate = ReversalGate()
    gate.observe(now=1, up_best_ask=D("0.70"), down_best_ask=D("0.31"))
    gate.observe(now=2, up_best_ask=D("0.39"), down_best_ask=D("0.62"))
    gate.mark_traded()
    assert gate.state == StrategyState.TRADED
    assert gate.observe(now=3, up_best_ask=D("0.2"), down_best_ask=D("0.7")) == []

    other = ReversalGate()
    other.expire()
    assert other.state == StrategyState.EXPIRED


def test_buy_execution_walks_depth_and_adds_level_fees():
    fee = FeeSchedule(rate=D("0.07"))
    ex = buy_execution(
        [Level(D("0.30"), D("5")), Level(D("0.32"), D("5"))],
        D("8"),
        fee,
    )
    assert ex.full
    assert ex.filled == D("8")
    assert ex.notional == D("2.46")
    assert ex.worst_price == D("0.32")
    assert ex.fee > 0
    assert ex.total_cost == ex.notional + ex.fee
