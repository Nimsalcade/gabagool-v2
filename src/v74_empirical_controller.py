"""Empirical replenishment helpers for the January V7.4 paper controller.

The Jan-1 and Jan-17 Gabagool reference sessions show a *gradual* inventory
response rather than V7.2/V7.3's binary overweight freeze.  Fresh overweight
exposure is therefore governed by a cumulative budget earned by underweight
fills.  This prevents V7.1-style unlimited replenishment while still allowing
historically observed overweight opportunity flow.

These functions are deliberately pure so the response surface can be tested
independently of the live paper harness.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EmpiricalFlowTarget:
    gap_clips: float
    age_s: float
    underweight_share_target: float
    overweight_to_underweight_ratio: float
    hard_stop: bool


def empirical_underweight_share_target(
    *,
    gap_clips: float,
    age_s: float,
    duration_s: float = 900.0,
    hard_gap_clips: float = 22.0,
    late_pull: float = 0.45,
) -> EmpiricalFlowTarget:
    """Return a conservative empirical underweight-flow target.

    Piecewise anchors are intentionally broad approximations of the combined
    Jan-1/Jan-17 response surface:

      <2 clips   -> 55% underweight flow
      2-4        -> 60%
      4-8        -> 70%
      8-16       -> 82%
      16-hard    -> 95%
      hard+      -> 100% / no new overweight exposure

    The historical fit also showed stronger inventory pressure near expiry, so
    the target is pulled toward 100% by ``late_pull * (age/duration)^2``.
    """
    g = max(0.0, float(gap_clips))
    t = max(0.0, min(float(duration_s), float(age_s)))
    hard = max(1e-9, float(hard_gap_clips))

    if g >= hard:
        return EmpiricalFlowTarget(g, t, 1.0, 0.0, True)
    if g < 2.0:
        base = 0.55
    elif g < 4.0:
        base = 0.60
    elif g < 8.0:
        base = 0.70
    elif g < 16.0:
        base = 0.82
    else:
        base = 0.95

    age_n = t / max(float(duration_s), 1e-9)
    pull = max(0.0, min(1.0, float(late_pull))) * age_n * age_n
    p = base + (1.0 - base) * pull
    p = max(0.500001, min(1.0, p))
    ratio = 0.0 if p >= 1.0 else (1.0 - p) / p
    return EmpiricalFlowTarget(g, t, p, ratio, False)


def fresh_overweight_allowance_shares(
    *,
    gap_clips: float,
    age_s: float,
    underweight_filled_since_regime: float,
    parent_clip: float,
    initial_overweight_parents: float = 1.0,
    duration_s: float = 900.0,
    hard_gap_clips: float = 22.0,
    late_pull: float = 0.45,
) -> tuple[float, EmpiricalFlowTarget]:
    """Cumulative fresh-overweight shares allowed in the current gap regime.

    The allowance is finite when no repair fills occur.  Underweight fills earn
    additional overweight opportunity budget according to the empirical flow
    ratio.  At the hard gap boundary the allowance collapses to zero.
    """
    clip = max(float(parent_clip), 1e-9)
    flow = empirical_underweight_share_target(
        gap_clips=gap_clips,
        age_s=age_s,
        duration_s=duration_s,
        hard_gap_clips=hard_gap_clips,
        late_pull=late_pull,
    )
    if flow.hard_stop:
        return 0.0, flow

    initial = max(0.0, float(initial_overweight_parents)) * clip
    earned = max(0.0, float(underweight_filled_since_regime)) * flow.overweight_to_underweight_ratio
    return initial + earned, flow


def fresh_overweight_post_allowed(
    *,
    already_posted_shares: float,
    next_parent_shares: float,
    allowance_shares: float,
) -> bool:
    """True when another fresh overweight parent fits inside the budget."""
    return (
        max(0.0, float(already_posted_shares))
        + max(0.0, float(next_parent_shares))
        <= max(0.0, float(allowance_shares)) + 1e-9
    )
