"""Unit tests for the pure quoting math — the strategy's safety invariants."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.quoting import (  # noqa: E402
    BookSide,
    cap_against_resting,
    fair_split_bids,
    imbalance_fraction,
    round_pairs_to_micro,
    should_requote,
    size_for_price,
    tick_floor,
)

BUDGET = 0.97
TICK = 0.01


def test_pair_never_exceeds_budget_across_books():
    cases = [
        (BookSide(0.45, 0.47), BookSide(0.52, 0.55)),
        (BookSide(0.10, 0.12), BookSide(0.86, 0.90)),
        (BookSide(0.49, 0.50), BookSide(0.49, 0.50)),
        (BookSide(0.70, 0.76), BookSide(0.20, 0.27)),
        (BookSide(0.01, 0.03), BookSide(0.95, 0.99)),
    ]
    for up, down in cases:
        out = fair_split_bids(up, down, BUDGET, TICK)
        if out is None:
            continue
        bu, bd = out
        assert bu + bd <= BUDGET + 1e-9, (up, down, out)
        assert bu <= up.best_ask - TICK + 1e-9, "crossed UP ask"
        assert bd <= down.best_ask - TICK + 1e-9, "crossed DOWN ask"
        assert bu >= TICK and bd >= TICK


def test_never_crosses_even_when_budget_is_generous():
    up, down = BookSide(0.30, 0.31), BookSide(0.30, 0.31)
    bu, bd = fair_split_bids(up, down, 0.99, TICK)
    assert bu <= 0.30 + 1e-9 and bd <= 0.30 + 1e-9  # ask-1tick caps both


def test_degenerate_books_return_none():
    assert fair_split_bids(BookSide(0, 0.5), BookSide(0.4, 0.6), BUDGET, TICK) is None
    assert fair_split_bids(BookSide(0.5, 0.4), BookSide(0.4, 0.6), BUDGET, TICK) is None  # crossed


def test_cap_against_resting_closes_the_stale_pair_hole():
    # Other side rests at 0.60; my fresh target 0.45 must be cut to <= 0.37
    p = cap_against_resting(0.45, 0.60, 0.30, BUDGET, TICK)
    assert p is not None and p <= BUDGET - 0.60 + 1e-9
    # Other side rests at 0.96 → exactly one tick of budget remains: still valid
    assert cap_against_resting(0.45, 0.96, 0.30, BUDGET, TICK) == 0.01
    # Other side rests at 0.965 → remainder is sub-tick → no valid bid
    assert cap_against_resting(0.45, 0.965, 0.30, BUDGET, TICK) is None


def test_tick_floor_rounds_down_only():
    assert tick_floor(0.4799, 0.01) == 0.47
    assert tick_floor(0.48, 0.01) == 0.48
    assert tick_floor(0.123, 0.001) == 0.123


def test_sizing_respects_all_floors():
    # exchange min shares dominates
    assert size_for_price(0.50, rung_dollar_target=1.0, min_order_shares=5, min_notional=1.0) == 5
    # notional floor dominates at low prices: $1 / 0.05 = 20 shares
    assert size_for_price(0.05, 0.5, 5, 1.0) == 20
    # dollar target dominates when above floors: $4 / 0.40 = 10
    assert size_for_price(0.40, 4.0, 5, 1.0) == 10


def test_requote_logic():
    assert should_requote(None, 0.45, 0.02)
    assert not should_requote(0.45, 0.46, 0.02)   # within drift: keep queue spot
    assert should_requote(0.45, 0.48, 0.02)


def test_pairs_to_micro_floors():
    assert round_pairs_to_micro(10.0, 7.9999999) == 7_999_999
    assert round_pairs_to_micro(0.0, 5.0) == 0
    assert round_pairs_to_micro(3.5, 3.5) == 3_500_000


def test_imbalance_sign():
    assert imbalance_fraction(110, 90) > 0
    assert imbalance_fraction(90, 110) < 0
    assert imbalance_fraction(0, 0) == 0
