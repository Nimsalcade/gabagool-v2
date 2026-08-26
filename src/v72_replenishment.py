"""V7.2 replenishment policy for the January portfolio controller.

The January reconstruction distinguishes *resting parents that are allowed to
survive* from *fresh parents that are allowed to be created*.

Once inventory leaves a small neutral band, only the underweight outcome may
receive fresh/replacement BUY parents. Existing overweight-side parents may
remain live (subject to the normal economic/staleness/excess checks) and may
continue to partial-fill, but a filled/cancelled overweight parent is not
replaced until inventory returns to the neutral band or the sign flips.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReplenishmentDecision:
    allow_up: bool
    allow_down: bool
    underweight: str | None
    gap_clips: float
    neutral: bool


def replenishment_decision(
    *,
    up_shares: float,
    down_shares: float,
    parent_clip: float,
    neutral_gap_clips: float = 0.5,
) -> ReplenishmentDecision:
    if parent_clip <= 0:
        return ReplenishmentDecision(False, False, None, 0.0, True)
    if neutral_gap_clips < 0:
        raise ValueError("neutral_gap_clips must be >= 0")

    gap = float(up_shares) - float(down_shares)
    gap_clips = abs(gap) / float(parent_clip)
    neutral = gap_clips < float(neutral_gap_clips)

    if abs(gap) <= 1e-12:
        under = None
    else:
        under = "DOWN" if gap > 0 else "UP"

    if neutral or under is None:
        return ReplenishmentDecision(True, True, under, gap_clips, True)

    return ReplenishmentDecision(
        allow_up=(under == "UP"),
        allow_down=(under == "DOWN"),
        underweight=under,
        gap_clips=gap_clips,
        neutral=False,
    )
