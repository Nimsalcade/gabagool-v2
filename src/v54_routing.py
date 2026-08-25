"""V5.4 experimental aggressive-episode routing helpers.

The coefficients are copied from the final forensic reconstruction bundle.
They are *proxy* hazard models, not recovered private Gabagool source. V5.4
uses deterministic hash-based Bernoulli draws to turn probabilities into
reproducible paper-routing actions so the state machine can be validated
without imposing a fixed aggressive-share quota.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from typing import Iterable, Sequence


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _z(value: float | None, median: float, iqr: float) -> float:
    x = median if value is None or not math.isfinite(float(value)) else float(value)
    return (x - median) / iqr


def deterministic_uniform(*parts: object, seed: int = 5401) -> float:
    """Stable U[0,1) variate for repeatable paper hazard decisions."""
    payload = "|".join([str(seed), *(str(p) for p in parts)]).encode("utf-8")
    n = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")
    return n / float(1 << 64)


def hazard_decision(
    probability: float,
    *parts: object,
    seed: int = 5401,
    scale: float = 1.0,
) -> tuple[bool, float, float]:
    p = min(1.0, max(0.0, float(probability) * float(scale)))
    u = deterministic_uniform(*parts, seed=seed)
    return u < p, p, u


def walk_asks_vwap(
    asks: Iterable[Sequence[float]], shares: float,
) -> tuple[float, float] | None:
    """Return (VWAP, worst_price) for buying ``shares`` from asks, else None."""
    need = max(0.0, float(shares))
    if need <= 0:
        return None
    cost = 0.0
    worst = 0.0
    for row in asks:
        px, qty = float(row[0]), float(row[1])
        if px <= 0 or qty <= 0:
            continue
        take = min(need, qty)
        cost += take * px
        need -= take
        worst = px
        if need <= 1e-12:
            return cost / float(shares), worst
    return None


@dataclass(frozen=True)
class OnsetFeatures:
    age_fraction: float
    inventory_total_parents: float
    same_price_delta: float | None
    pair_margin: float | None
    asset: str
    role: str


def onset_probability(x: OnsetFeatures) -> float:
    same_known = 0.0 if x.same_price_delta is None else 1.0
    pair_known = 0.0 if x.pair_margin is None else 1.0
    disp = x.same_price_delta
    positive = None if disp is None else max(disp, 0.0)
    interaction = (
        None if disp is None or x.pair_margin is None else disp * x.pair_margin
    )
    pair_above = 0.0 if x.pair_margin is None else float(x.pair_margin > 0.0)

    logit = -2.142963085686646
    logit += -0.04989960502930288 * _z(
        x.age_fraction, 0.4033333333333333, 0.4733333333333334
    )
    logit += -0.4112143760419505 * _z(
        x.inventory_total_parents, 168.8378324, 250.79242033888886
    )
    logit += 0.13893586205941258 * _z(
        disp, 0.0, 0.04000000000000013
    )
    logit += -0.010472985137355686 * _z(
        positive, 0.0, 0.020000000000000004
    )
    logit += -0.14013756159726093 * _z(
        x.pair_margin, -0.030000000000000027, 0.05999999999999994
    )
    logit += -0.04584912532406684 * _z(
        interaction, 0.0002000000000000001, 0.0017999999999999976
    )
    logit += 0.4430354604193743 * same_known
    logit += -0.31670556199113836 * pair_known
    logit += 1.0767146151689009 * pair_above
    if x.asset.upper() == "ETH":
        logit += 0.2853880798699656
    if x.role.upper() == "OVERWEIGHT":
        logit += -0.23775006175999636
    elif x.role.upper() == "UNDERWEIGHT":
        logit += -0.5869363332906078
    return _sigmoid(logit)


@dataclass(frozen=True)
class ContinuationFeatures:
    run_position: int
    elapsed_episode_time: float
    previous_parent_same_second: bool
    previous_transition_switched_side: bool
    current_same_price_delta: float | None
    current_pair_margin: float | None
    episode_start_same_price_delta: float | None
    episode_start_pair_margin: float | None
    gap_clips_after: float
    current_parent_closed_deficit: bool
    current_parent_overshot: bool
    market_age: float
    asset: str
    inventory_role_after: str
    current_side: str


def continuation_probability(x: ContinuationFeatures) -> float:
    """Diagnostic continuation hazard from FINAL_STABLE_HYBRID."""
    disp = (
        0.01 if x.current_same_price_delta is None else float(x.current_same_price_delta)
    )
    pair = -0.02 if x.current_pair_margin is None else float(x.current_pair_margin)
    start_disp = (
        disp
        if x.episode_start_same_price_delta is None
        else float(x.episode_start_same_price_delta)
    )
    start_pair = (
        pair
        if x.episode_start_pair_margin is None
        else float(x.episode_start_pair_margin)
    )
    logit = 0.33663844013475586
    logit += 0.1646726165356667 * math.log1p(max(1, int(x.run_position)))
    logit += 0.05183848418032232 * math.log1p(
        max(0.0, float(x.elapsed_episode_time))
    )
    logit += 0.19330153012592396 * float(bool(x.previous_parent_same_second))
    logit += 0.06510435872054514 * float(
        bool(x.previous_transition_switched_side)
    )
    logit += 1.397159533217787 * disp
    logit += -2.7701798686567685 * pair
    logit += 4.351755935805246 * (disp - start_disp)
    logit += -5.660204324632745 * (pair - start_pair)
    logit += 0.0899791765024862 * math.log1p(
        max(0.0, float(x.gap_clips_after))
    )
    logit += -0.7476646324302176 * float(bool(x.current_parent_closed_deficit))
    logit += 0.9866136387970739 * float(bool(x.current_parent_overshot))
    logit += -7.048599282066556e-05 * float(x.market_age)
    logit += -0.6408376102590423 * float(
        x.current_same_price_delta is not None and x.current_pair_margin is not None
    )
    if x.asset.upper() == "ETH":
        logit += 0.19486200280557125
    role = x.inventory_role_after.upper()
    if role == "OVERWEIGHT":
        logit += -0.15597548347811901
    elif role == "UNDERWEIGHT":
        logit += 0.5014124460015517
    if x.current_side.upper() == "UP":
        logit += 0.03728673157861437
    return _sigmoid(logit)
