"""Tests for low-level arithmetic helpers; active policy tests live in test_policy.py."""
from src.quoting import (
    conform_price,
    imbalance_fraction,
    round_pairs_to_micro,
    size_for_price,
    tick_floor,
)


def test_tick_floor():
    assert tick_floor(.4799, .01) == .47
    assert tick_floor(.48, .01) == .48


def test_size_respects_exchange_floors():
    assert size_for_price(.50, 1.0, 5, 1.0) == 5
    assert size_for_price(.05, .5, 5, 1.0) == 20
    assert size_for_price(.40, 4.0, 5, 1.0) == 10


def test_merge_rounding_floors_micro_units():
    assert round_pairs_to_micro(10.0, 7.9999999) == 7_999_999
    assert round_pairs_to_micro(0.0, 5.0) == 0


def test_imbalance_sign():
    assert imbalance_fraction(110, 90) > 0
    assert imbalance_fraction(90, 110) < 0
    assert imbalance_fraction(0, 0) == 0


def test_conform_price():
    assert conform_price(.476, .01) == .48
