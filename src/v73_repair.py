"""Pure helpers for V7.3 bootstrap risk control and marginal repair execution."""
from __future__ import annotations

import math


def in_bootstrap(
    *,
    up_shares: float,
    down_shares: float,
    parent_clip: float,
    two_sided_threshold_clips: float = 0.5,
) -> bool:
    """Stay in low-exposure bootstrap until both outcomes have meaningful inventory."""
    clip = max(float(parent_clip), 1e-9)
    threshold = max(float(two_sided_threshold_clips), 0.0) * clip
    return min(float(up_shares), float(down_shares)) < threshold - 1e-12


def repair_parent_count(
    *,
    gap_shares: float,
    parent_clip: float,
    requested_parents: int,
    overshoot_parents: int = 1,
) -> int:
    """Bound fresh deficient-side exposure to the live gap plus small overshoot.

    V7.2 could request many repair parents.  During repair we only need enough
    fresh capacity to close the current gap, plus a small configurable overshoot
    allowance for persistence/partial-fill realism.
    """
    clip = max(float(parent_clip), 1e-9)
    gap = max(float(gap_shares), 0.0)
    requested = max(int(requested_parents), 0)
    if requested == 0 or gap <= 1e-12:
        return 0
    needed = int(math.ceil(gap / clip - 1e-12)) + max(int(overshoot_parents), 0)
    return max(1, min(requested, needed))


def repair_maker_top(*, best_ask: float, tick: float, repair_max_price: float) -> float | None:
    """Most aggressive post-only repair price: one tick below ask, capped.

    This deliberately differs from the normal portfolio-basis guard.  Once one
    outcome is already stranded, buying the deficient outcome below $1 improves
    merge-only cash recovery versus leaving that inventory unmatched.  The
    absolute repair cap remains explicit and configurable.
    """
    ask = float(best_ask)
    t = max(float(tick), 1e-9)
    cap = float(repair_max_price)
    raw = min(ask - t, cap)
    if raw <= 0:
        return None
    # Floor to the exchange tick to ensure the quote remains post-only.
    steps = math.floor((raw + 1e-12) / t)
    px = steps * t
    if px <= 0 or px >= ask - 1e-12:
        return None
    return round(px, 10)
