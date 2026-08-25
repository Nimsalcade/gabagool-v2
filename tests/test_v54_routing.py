import math

from src.v54_routing import (
    ContinuationFeatures,
    OnsetFeatures,
    continuation_probability,
    deterministic_uniform,
    hazard_decision,
    onset_probability,
    walk_asks_vwap,
)


def test_onset_probability_reference_points():
    p1 = onset_probability(
        OnsetFeatures(0.4, 100, 0.01, -0.02, "BTC", "UNDERWEIGHT")
    )
    p2 = onset_probability(
        OnsetFeatures(0.4, 100, 0.03, 0.02, "ETH", "OVERWEIGHT")
    )
    assert math.isclose(p1, 0.07769983849735868, rel_tol=0, abs_tol=1e-12)
    assert math.isclose(p2, 0.3062869352506028, rel_tol=0, abs_tol=1e-12)
    assert p2 > p1


def test_onset_missing_price_state_is_finite():
    p = onset_probability(OnsetFeatures(0.2, 0, None, None, "BTC", "BALANCED"))
    assert 0 < p < 1


def test_continuation_probability_reference_points():
    p1 = continuation_probability(
        ContinuationFeatures(
            1,
            0,
            False,
            False,
            0.01,
            -0.02,
            0.01,
            -0.02,
            2.0,
            False,
            False,
            300,
            "BTC",
            "UNDERWEIGHT",
            "UP",
        )
    )
    p2 = continuation_probability(
        ContinuationFeatures(
            2,
            0,
            True,
            False,
            0.02,
            -0.02,
            0.01,
            -0.02,
            3.0,
            False,
            True,
            320,
            "ETH",
            "OVERWEIGHT",
            "DOWN",
        )
    )
    assert math.isclose(p1, 0.6214539283288629, rel_tol=0, abs_tol=1e-12)
    assert math.isclose(p2, 0.7899394265189567, rel_tol=0, abs_tol=1e-12)


def test_hazard_is_deterministic_and_scaled():
    a = hazard_decision(0.2, "m", "UP", 1, seed=5401)
    b = hazard_decision(0.2, "m", "UP", 1, seed=5401)
    assert a == b
    assert math.isclose(a[2], deterministic_uniform("m", "UP", 1, seed=5401))
    _, p, _ = hazard_decision(0.8, "x", seed=1, scale=2.0)
    assert p == 1.0


def test_walk_asks_vwap_finite_volume():
    got = walk_asks_vwap([(0.40, 5), (0.42, 5)], 10)
    assert got is not None
    vwap, worst = got
    assert math.isclose(vwap, 0.41)
    assert math.isclose(worst, 0.42)
    assert walk_asks_vwap([(0.40, 5)], 10) is None
