"""Strict zero-money capture/replay validator.

This supersedes the first capture/replay quality label.  It records how many book
requests were attempted, how many returned a fully tradable two-sided snapshot, and
whether valid book coverage extends through the end of the market.  A replay with a
large terminal book gap is explicitly rejected for strategy tuning.

No orders are submitted. No credentials are required.
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymarket import AsyncPublicClient

from src.config import BotConfig
from src.shadow_validation import evaluate_capture_quality
from tools.shadow_capture_replay import (
    ReplayEngine,
    Snapshot,
    choose_fresh_market,
    fetch_snapshot,
    wait_for_stable_tape,
)

REFERENCE_TAKER_SHARE_PCT = 14.545854
REFERENCE_COMBINED_VWAP_MEDIAN = 0.985117405
REFERENCE_TERMINAL_RATIO_MEDIAN = 1.013583649
REFERENCE_TERMINAL_RATIO_P90 = 1.055303820
REFERENCE_5M_FIRST_FILL_MEDIAN_S = 15.0
REFERENCE_5M_LAST_FILL_MEDIAN_S = 273.0


@dataclass(frozen=True)
class CaptureStats:
    snapshots: tuple[Snapshot, ...]
    attempts: int
    empty_snapshots: int
    errors: int


async def capture_books_strict(
    client,
    market,
    *,
    interval: float,
) -> CaptureStats:
    snapshots: list[Snapshot] = []
    attempts = 0
    empty = 0
    errors = 0
    print(
        f"STRICT CAPTURE MARKET: {market.slug}\n"
        f"window: {market.window_start} -> {market.window_end}\n"
        f"sampling official CLOB REST books every {interval:.2f}s; ZERO orders submitted"
    )
    next_print = 0.0
    while market.seconds_to_end > 0:
        attempts += 1
        try:
            snap = await fetch_snapshot(client, market)
            if snap is None:
                empty += 1
            else:
                snapshots.append(snap)
        except Exception as exc:  # noqa: BLE001
            errors += 1
            if errors <= 5 or errors % 10 == 0:
                print(f"book capture warning #{errors}: {type(exc).__name__}: {exc}")
        now = time.time()
        if now >= next_print:
            last_age = (
                snapshots[-1].ts - market.window_start if snapshots else float("nan")
            )
            print(
                f"capture: age={market.age_seconds:6.1f}s remaining={market.seconds_to_end:6.1f}s "
                f"attempts={attempts} valid={len(snapshots)} empty={empty} errors={errors} "
                f"last_valid_age={last_age:.1f}s"
            )
            next_print = now + 30.0
        await asyncio.sleep(max(0.05, interval))
    print(
        f"capture complete: attempts={attempts} valid={len(snapshots)} "
        f"empty={empty} errors={errors}"
    )
    return CaptureStats(tuple(snapshots), attempts, empty, errors)


def _fmt(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None or not math.isfinite(value) else f"{value:.{digits}f}"


def report_strict(engine: ReplayEngine, *, market, capture: CaptureStats, tape, interval: float) -> None:
    sells = [t for t in tape if t.side == "SELL"]
    quality = evaluate_capture_quality(
        snapshot_times=[s.ts for s in capture.snapshots],
        attempts=capture.attempts,
        empty_snapshots=capture.empty_snapshots,
        book_errors=capture.errors,
        window_start=float(market.window_start),
        window_end=float(market.window_end),
        snapshot_interval_s=interval,
        tape_rows=len(tape),
        tape_sell_rows=len(sells),
    )

    fills = engine.up.fill_events + engine.down.fill_events
    maker = engine.up.maker_fill_events + engine.down.maker_fill_events
    taker = engine.up.taker_fill_events + engine.down.taker_fill_events
    taker_share = 100.0 * taker / fills if fills else 0.0
    matched = min(engine.up.shares, engine.down.shares)
    ratio = (
        max(engine.up.shares, engine.down.shares) / min(engine.up.shares, engine.down.shares)
        if min(engine.up.shares, engine.down.shares) > 0
        else math.inf
    )
    combined = engine.up.vwap + engine.down.vwap if matched > 0 else None
    edge = matched * (1.0 - combined) if combined is not None else 0.0
    first_age = engine.first_fill_ts - market.window_start if engine.first_fill_ts else None
    last_age = engine.last_fill_ts - market.window_start if engine.last_fill_ts else None

    if not quality.usable:
        assessment = "DO_NOT_TUNE_FROM_THIS_RUN"
    elif fills < 20:
        assessment = "LOW_FILL_SAMPLE_REPEAT_REQUIRED"
    else:
        assessment = "USABLE_FOR_POLICY_COMPARISON"

    print("\n================ STRICT CAPTURE/REPLAY RESULT ================")
    print("market:", market.slug)
    print("source: CLOB_REST_BOOKS + DATA_API_POSTCLOSE_TAKER_TAPE")
    print(
        f"book_attempts={capture.attempts} valid_snapshots={len(capture.snapshots)} "
        f"empty_or_one_sided={capture.empty_snapshots} book_errors={capture.errors}"
    )
    print(
        f"valid_snapshot_fraction={quality.valid_fraction:.3f} "
        f"first_book_age_s={_fmt(quality.first_age_s)} "
        f"last_book_age_s={_fmt(quality.last_age_s)} "
        f"tail_book_gap_s={_fmt(quality.tail_gap_s)} "
        f"max_internal_gap_s={_fmt(quality.max_internal_gap_s)}"
    )
    print(f"official_taker_rows={len(tape)} official_taker_sell_rows={len(sells)}")
    print(f"quote_events={engine.quote_events} requote_events={engine.requote_events}")
    print(
        f"fill_events={fills} maker={maker} taker={taker} "
        f"taker_share={taker_share:.2f}%"
    )
    print(
        f"UP   shares={engine.up.shares:.6f} spend=${engine.up.cost:.6f} "
        f"vwap={engine.up.vwap:.6f} levels={len(engine.up.prices)}"
    )
    print(
        f"DOWN shares={engine.down.shares:.6f} spend=${engine.down.cost:.6f} "
        f"vwap={engine.down.vwap:.6f} levels={len(engine.down.prices)}"
    )
    print(f"total_spend=${engine.spend:.6f}")
    print("combined_vwap:", "n/a" if combined is None else f"{combined:.6f}")
    print("terminal_ratio:", "inf" if math.isinf(ratio) else f"{ratio:.6f}")
    print(f"matched_pairs={matched:.6f} gross_matched_edge=${edge:.6f}")
    print("first_fill_age_s:", _fmt(first_age))
    print("last_fill_age_s:", _fmt(last_age))
    print("capture_quality:", quality.status)
    print("capture_reasons:", ",".join(quality.reasons) if quality.reasons else "none")
    print("policy_assessment:", assessment)
    print("--- forensic reference deltas (diagnostic, not one-run pass/fail) ---")
    print(
        f"taker_share_delta_pp={taker_share - REFERENCE_TAKER_SHARE_PCT:+.2f} "
        f"reference_overall={REFERENCE_TAKER_SHARE_PCT:.2f}%"
    )
    if combined is not None:
        print(
            f"combined_vwap_delta={combined - REFERENCE_COMBINED_VWAP_MEDIAN:+.6f} "
            f"reference_median={REFERENCE_COMBINED_VWAP_MEDIAN:.6f}"
        )
    if math.isfinite(ratio):
        print(
            f"terminal_ratio_delta={ratio - REFERENCE_TERMINAL_RATIO_MEDIAN:+.6f} "
            f"reference_median={REFERENCE_TERMINAL_RATIO_MEDIAN:.6f} "
            f"reference_p90={REFERENCE_TERMINAL_RATIO_P90:.6f}"
        )
    if market.duration_s == 300:
        if first_age is not None:
            print(
                f"first_fill_delta_s={first_age - REFERENCE_5M_FIRST_FILL_MEDIAN_S:+.1f} "
                f"reference_median={REFERENCE_5M_FIRST_FILL_MEDIAN_S:.1f}"
            )
        if last_age is not None:
            print(
                f"last_fill_delta_s={last_age - REFERENCE_5M_LAST_FILL_MEDIAN_S:+.1f} "
                f"reference_median={REFERENCE_5M_LAST_FILL_MEDIAN_S:.1f}"
            )
    print("===============================================================")


async def amain(args) -> int:
    cfg = BotConfig.load("config/default.yaml")
    client = AsyncPublicClient()
    try:
        market = await choose_fresh_market(
            client,
            args.asset,
            args.duration,
            max_start_age=args.max_start_age,
        )
        capture = await capture_books_strict(
            client,
            market,
            interval=args.snapshot_interval,
        )
        tape = await wait_for_stable_tape(
            client,
            market,
            timeout=args.tape_timeout,
            poll_interval=args.tape_poll_interval,
            stable_polls=args.stable_polls,
        )
        engine = ReplayEngine(market, cfg.strategy, max_spend=args.max_spend)
        print(
            f"replay starting: {len(capture.snapshots)} valid book snapshots + "
            f"{len(tape)} official taker rows"
        )
        engine.replay(list(capture.snapshots), tape)
        report_strict(
            engine,
            market=market,
            capture=capture,
            tape=tape,
            interval=args.snapshot_interval,
        )
        return 0
    finally:
        await client.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="strict zero-money full-window capture/replay validator")
    ap.add_argument("--asset", choices=("btc", "eth", "sol", "xrp"), default="btc")
    ap.add_argument("--duration", type=int, choices=(300, 900), default=300)
    ap.add_argument("--max-spend", type=float, default=2000.0)
    ap.add_argument("--snapshot-interval", type=float, default=1.0)
    ap.add_argument("--max-start-age", type=float, default=12.0)
    ap.add_argument("--tape-timeout", type=float, default=90.0)
    ap.add_argument("--tape-poll-interval", type=float, default=3.0)
    ap.add_argument("--stable-polls", type=int, default=4)
    args = ap.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
