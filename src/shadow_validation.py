"""Strict quality gates for zero-money capture/replay validation.

A replay must not be labelled usable merely because it collected an arbitrary number
of snapshots.  For a short-duration market, temporal coverage matters: a long missing
tail means terminal inventory, last-fill timing, and late execution behavior are not
observable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class CaptureQuality:
    status: str
    usable: bool
    reasons: tuple[str, ...]
    attempts: int
    valid_snapshots: int
    empty_snapshots: int
    book_errors: int
    valid_fraction: float
    first_age_s: float | None
    last_age_s: float | None
    tail_gap_s: float | None
    max_internal_gap_s: float | None
    tape_rows: int
    tape_sell_rows: int


def evaluate_capture_quality(
    *,
    snapshot_times: Sequence[float],
    attempts: int,
    empty_snapshots: int,
    book_errors: int,
    window_start: float,
    window_end: float,
    snapshot_interval_s: float,
    tape_rows: int,
    tape_sell_rows: int,
) -> CaptureQuality:
    """Classify whether one market capture can support behavioral comparison.

    The thresholds are validation requirements, not strategy parameters.  They are
    deliberately strict because terminal-ratio and last-fill comparisons are invalid
    when the final portion of a five-minute book is missing.
    """
    times = sorted(float(x) for x in snapshot_times)
    valid = len(times)
    attempts = max(0, int(attempts))
    empty_snapshots = max(0, int(empty_snapshots))
    book_errors = max(0, int(book_errors))
    interval = max(0.05, float(snapshot_interval_s))

    first_age = times[0] - window_start if times else None
    last_age = times[-1] - window_start if times else None
    tail_gap = max(0.0, window_end - times[-1]) if times else None
    gaps = [b - a for a, b in zip(times, times[1:])]
    max_gap = max(gaps) if gaps else None
    valid_fraction = valid / attempts if attempts > 0 else 0.0

    reasons: list[str] = []
    # A fresh capture should begin close to the start of the actionable window.
    if first_age is None or first_age > 20.0:
        reasons.append("late_or_missing_start")

    # A terminal gap destroys terminal inventory and last-fill inference.
    allowed_gap = max(8.0, interval * 6.0)
    if tail_gap is None or tail_gap > allowed_gap:
        reasons.append("missing_terminal_book_coverage")
    if max_gap is not None and max_gap > allowed_gap:
        reasons.append("large_internal_book_gap")

    # Distinguish one-sided/empty books from transport failures.
    if attempts < 50:
        reasons.append("too_few_book_attempts")
    if valid_fraction < 0.70:
        reasons.append("too_many_empty_or_one_sided_books")
    if book_errors > max(5, int(max(1, attempts) * 0.10)):
        reasons.append("excessive_book_transport_errors")

    if tape_rows < 10 or tape_sell_rows < 5:
        reasons.append("insufficient_official_trade_tape")

    if not reasons:
        status = "USABLE"
        usable = True
    elif "insufficient_official_trade_tape" in reasons:
        status = "INSUFFICIENT_PUBLIC_TAPE"
        usable = False
    elif any(
        r in reasons
        for r in (
            "missing_terminal_book_coverage",
            "large_internal_book_gap",
            "too_many_empty_or_one_sided_books",
        )
    ):
        status = "PARTIAL_WINDOW_BOOK_DATA"
        usable = False
    else:
        status = "CAPTURE_QUALITY_FAIL"
        usable = False

    return CaptureQuality(
        status=status,
        usable=usable,
        reasons=tuple(reasons),
        attempts=attempts,
        valid_snapshots=valid,
        empty_snapshots=empty_snapshots,
        book_errors=book_errors,
        valid_fraction=valid_fraction,
        first_age_s=first_age,
        last_age_s=last_age,
        tail_gap_s=tail_gap,
        max_internal_gap_s=max_gap,
        tape_rows=int(tape_rows),
        tape_sell_rows=int(tape_sell_rows),
    )
