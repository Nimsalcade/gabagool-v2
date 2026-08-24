"""Paper-only joint two-sided exposure override.

Not recovered Gabagool source. Evidence-bounded A/B candidate from the Oct-29
15m paired-acquisition envelope:

    same-second UP+DOWN fill pairs: p95 ≈ 1.01, p99 ≈ 1.04, ~0.7% > 1.05
    ±10s pairs:                     p95 ≈ 1.03, ~1.7% > 1.05
    one-leg lagging marginal basis: historically can exceed 1.20

``FRESH_PAIR_CATASTROPHIC = 1.05`` therefore sits outside almost all close
two-sided historical acquisitions without pretending there was a literal < $1
rule.

The pair object is the tick-rounded complementary ANCHORS

    UP_anchor   = tick_floor(1 - DOWN_ask)
    DOWN_anchor = tick_floor(1 - UP_ask)

not the post-only-capped posted bids. Same-snapshot posted complementary
bases never exceed 0.99 by construction of ``complementary_base_bid``; the
1.25 live state (e.g. 0.35 + 0.90) is the uncapped complementary pair.

This module only rewrites TARGET layer counts for NEW exposure. Callers must
not use the result to mass-cancel resting orders.
"""
from __future__ import annotations

from typing import Mapping

from src.forensic_15m import tick_floor

FRESH_PAIR_CATASTROPHIC = 1.05


def complementary_anchor(opposite_best_ask: float, tick: float) -> float | None:
    """Tick-rounded complementary bid *before* the post-only cap."""
    if tick <= 0 or opposite_best_ask <= 0:
        return None
    px = tick_floor(1.0 - float(opposite_best_ask), tick)
    if px < tick or px >= 1.0:
        return None
    return round(px, 10)


def apply_joint_exposure_override(
    *,
    up_base: float | None,
    down_base: float | None,
    layers: Mapping[str, int],
    signed_gap: float,
    cap: float | None = FRESH_PAIR_CATASTROPHIC,
) -> dict[str, int]:
    """If the fresh two-sided pair is catastrophic, suppress NEW leading layers.

    Lagging-side layer counts are preserved so one-leg repair can continue.
    Exactly-balanced inventory posts nothing new rather than a 1.05+ pair.
    ``up_base`` / ``down_base`` are whatever pair object the caller chose
    (anchors, not inventory VWAP).
    """
    out = {"UP": int(layers.get("UP", 0)), "DOWN": int(layers.get("DOWN", 0))}
    if cap is None or cap <= 0:
        return out
    if up_base is None or down_base is None:
        return out
    if float(up_base) + float(down_base) <= float(cap) + 1e-12:
        return out
    if abs(float(signed_gap)) <= 1e-9:
        return {"UP": 0, "DOWN": 0}
    if signed_gap > 0:
        return {"UP": 0, "DOWN": out["DOWN"]}
    return {"UP": out["UP"], "DOWN": 0}
