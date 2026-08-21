"""Observe 10 consecutive BTC 5-minute markets without placing any orders.

Purpose
-------
Record the market path that the two-leg complete-set strategy would have seen
before encoding more live execution logic.

The observer is READ-ONLY:
- AsyncPublicClient only
- no wallet
- no private key
- no approvals
- no order placement
- no merge/redeem transactions

Paper model
-----------
LEG1:
  Lock the first side whose best ask is <= FIRST_TRIGGER and for which at least
  TARGET_SHARES are visibly executable at or below FIRST_HARD_CAP. The paper
  LEG1 unit basis is the displayed depth-weighted VWAP for those shares.

LEG2:
  After the paper LEG1 is locked, compute the opposite-side economic ceiling:
      pair_max - LEG1_VWAP
  rounded down to the venue tick. Every poll records how many opposite shares
  are visibly available at/below that ceiling, whether the full target quantity
  is immediately executable, the displayed LEG2 VWAP/worst price, combined
  complete-set basis, gross edge, and time since LEG1.

This is observation, not a fill claim. Displayed liquidity can disappear before
a real order arrives, and a resting order's queue priority is not simulated.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

import httpx
from polymarket import AsyncPublicClient
from polymarket.errors import TransportError as PolymarketTransportError

from src.discovery import resolve_market, window_start_epoch

ASSET = "btc"
DURATION_S = 300
DEFAULT_SESSIONS = 10
DEFAULT_POLL_S = 0.50
DEFAULT_SHARES = Decimal("5")
DEFAULT_FIRST_TRIGGER = Decimal("0.25")
DEFAULT_FIRST_HARD_CAP = Decimal("0.27")
DEFAULT_PAIR_MAX = Decimal("0.999999")


def _d(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc).isoformat()


def _fmt(value: Decimal | None, places: int = 6) -> str:
    return "" if value is None else f"{value:.{places}f}"


def _levels(book: Any, side: str) -> list[tuple[Decimal, Decimal]]:
    raw = getattr(book, side, None) or []
    parsed: list[tuple[Decimal, Decimal]] = []
    for level in raw:
        p = _d(getattr(level, "price", None))
        s = _d(getattr(level, "size", None))
        if p is None or s is None or p <= 0 or s <= 0:
            continue
        parsed.append((p, s))
    if side == "asks":
        parsed.sort(key=lambda x: x[0])
    else:
        parsed.sort(key=lambda x: x[0], reverse=True)
    return parsed


def _best(book: Any, side: str) -> tuple[Decimal, Decimal] | None:
    levels = _levels(book, side)
    return levels[0] if levels else None


@dataclass
class Execution:
    requested: Decimal
    filled: Decimal
    cost: Decimal
    vwap: Decimal | None
    worst_price: Decimal | None
    available: Decimal
    full: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": str(self.requested),
            "filled": str(self.filled),
            "cost": str(self.cost),
            "vwap": None if self.vwap is None else str(self.vwap),
            "worst_price": None if self.worst_price is None else str(self.worst_price),
            "available": str(self.available),
            "full": self.full,
        }


def _buy_execution(book: Any, shares: Decimal, cap: Decimal | None = None) -> Execution:
    remaining = shares
    cost = Decimal(0)
    filled = Decimal(0)
    available = Decimal(0)
    worst: Decimal | None = None

    for price, size in _levels(book, "asks"):
        if cap is not None and price > cap:
            break
        available += size
        if remaining <= 0:
            continue
        take = min(size, remaining)
        if take > 0:
            filled += take
            cost += take * price
            remaining -= take
            worst = price

    full = filled + Decimal("0.000001") >= shares
    vwap = cost / filled if filled > 0 else None
    return Execution(
        requested=shares,
        filled=filled,
        cost=cost,
        vwap=vwap,
        worst_price=worst,
        available=available,
        full=full,
    )


def _floor_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        tick = Decimal("0.01")
    if value <= 0:
        return Decimal(0)
    return (value / tick).to_integral_value(rounding=ROUND_DOWN) * tick


async def _read_retry(label: str, call: Any, *args: Any) -> Any:
    last: BaseException | None = None
    for attempt in range(1, 7):
        try:
            return await call(*args)
        except (PolymarketTransportError, httpx.TransportError, TimeoutError) as exc:
            last = exc
            if attempt == 6:
                break
            delay = min(1.5, 0.15 * (2 ** (attempt - 1)))
            print(f"NET       {label} {type(exc).__name__}; retry {attempt}/6 in {delay:.2f}s")
            await asyncio.sleep(delay)
    raise RuntimeError(f"{label} unavailable after retries") from last


async def _books(client: AsyncPublicClient, up: str, down: str) -> tuple[Any, Any]:
    async def get(token: str) -> Any:
        return await client.get_order_book(token_id=token)

    up_task = asyncio.create_task(_read_retry("UP BOOK", get, up))
    down_task = asyncio.create_task(_read_retry("DOWN BOOK", get, down))
    return await asyncio.gather(up_task, down_task)


@dataclass
class PaperLeg1:
    side: str
    token_id: str
    age_s: float
    recv_ts: float
    best_ask: Decimal
    best_ask_size: Decimal
    vwap: Decimal
    worst_price: Decimal
    cost: Decimal
    shares: Decimal
    venue_min_size: Decimal
    tick: Decimal


@dataclass
class PairPoint:
    age_s: float
    recv_ts: float
    lag_s: float
    hedge_side: str
    best_ask: Decimal | None
    best_ask_size: Decimal | None
    ceiling: Decimal
    available_under_ceiling: Decimal
    leg2_vwap: Decimal
    leg2_worst: Decimal
    combined: Decimal
    gross_edge_per_pair: Decimal
    gross_edge_total: Decimal


CSV_FIELDS = [
    "session",
    "market",
    "condition_id",
    "recv_iso_utc",
    "market_age_s",
    "seconds_to_end",
    "up_best_bid",
    "up_best_bid_size",
    "up_best_ask",
    "up_best_ask_size",
    "up_5_full",
    "up_5_vwap",
    "up_5_worst",
    "up_5_cost",
    "down_best_bid",
    "down_best_bid_size",
    "down_best_ask",
    "down_best_ask_size",
    "down_5_full",
    "down_5_vwap",
    "down_5_worst",
    "down_5_cost",
    "instant_5_pair_basis",
    "leg1_locked",
    "leg1_side",
    "leg1_age_s",
    "leg1_vwap",
    "leg1_worst",
    "leg1_cost",
    "hedge_side",
    "hedge_ceiling",
    "hedge_available_under_ceiling",
    "hedge_full_under_ceiling",
    "hedge_vwap_under_ceiling",
    "hedge_worst_under_ceiling",
    "combined_if_full",
    "gross_edge_per_pair_if_full",
    "gross_edge_total_if_full",
    "seconds_since_leg1",
]


class SessionObserver:
    def __init__(
        self,
        *,
        session_no: int,
        market: Any,
        shares: Decimal,
        first_trigger: Decimal,
        first_hard_cap: Decimal,
        pair_max: Decimal,
        poll_s: float,
        writer: csv.DictWriter,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.shares = shares
        self.first_trigger = first_trigger
        self.first_hard_cap = first_hard_cap
        self.pair_max = pair_max
        self.poll_s = poll_s
        self.writer = writer

        self.leg1: PaperLeg1 | None = None
        self.first_pair: PairPoint | None = None
        self.best_pair: PairPoint | None = None
        self.trigger_observations = 0
        self.trigger_rejected_depth = 0
        self.post_leg1_observations = 0
        self.full_pair_observations = 0
        self.partial_ceiling_observations = 0
        self.read_errors = 0
        self.samples = 0

    def _try_lock_leg1(self, now: float, up_book: Any, down_book: Any) -> None:
        if self.leg1 is not None:
            return

        candidates: list[tuple[str, str, Any, tuple[Decimal, Decimal]]] = []
        for side, token, book in (
            ("UP", self.market.up_token_id, up_book),
            ("DOWN", self.market.down_token_id, down_book),
        ):
            ask = _best(book, "asks")
            if ask is not None and ask[0] <= self.first_trigger:
                candidates.append((side, token, book, ask))

        candidates.sort(key=lambda item: item[3][0])
        for side, token, book, (ask_price, ask_size) in candidates:
            self.trigger_observations += 1
            venue_min = _d(getattr(book, "min_order_size", None)) or self.shares
            qty = max(self.shares, venue_min)
            ex = _buy_execution(book, qty, self.first_hard_cap)
            if not ex.full or ex.vwap is None or ex.worst_price is None:
                self.trigger_rejected_depth += 1
                if self.trigger_rejected_depth <= 3:
                    print(
                        f"LEG1 CAND {side} ask={ask_price} but only "
                        f"{ex.available:.4f}sh displayed <= {self.first_hard_cap}; not locking"
                    )
                continue

            self.leg1 = PaperLeg1(
                side=side,
                token_id=token,
                age_s=now - self.market.window_start,
                recv_ts=now,
                best_ask=ask_price,
                best_ask_size=ask_size,
                vwap=ex.vwap,
                worst_price=ex.worst_price,
                cost=ex.cost,
                shares=qty,
                venue_min_size=venue_min,
                tick=_d(getattr(book, "tick_size", None)) or Decimal("0.01"),
            )
            hedge_side = "DOWN" if side == "UP" else "UP"
            print(
                f"PAPER L1  {side} age={self.leg1.age_s:.1f}s best={ask_price} "
                f"vwap={ex.vwap:.6f} worst={ex.worst_price} "
                f"qty={qty:.6f} cost=${ex.cost:.6f}"
            )
            print(
                f"WATCH L2  {hedge_side}; raw economic room="
                f"{self.pair_max - ex.vwap:.6f} before venue-tick rounding"
            )
            return

    def _observe_pair(self, now: float, up_book: Any, down_book: Any) -> tuple[dict[str, Any], PairPoint | None]:
        blank = {
            "hedge_side": "",
            "hedge_ceiling": "",
            "hedge_available_under_ceiling": "",
            "hedge_full_under_ceiling": "",
            "hedge_vwap_under_ceiling": "",
            "hedge_worst_under_ceiling": "",
            "combined_if_full": "",
            "gross_edge_per_pair_if_full": "",
            "gross_edge_total_if_full": "",
            "seconds_since_leg1": "",
        }
        if self.leg1 is None:
            return blank, None

        self.post_leg1_observations += 1
        other_side = "DOWN" if self.leg1.side == "UP" else "UP"
        other_book = down_book if other_side == "DOWN" else up_book
        best = _best(other_book, "asks")
        tick = _d(getattr(other_book, "tick_size", None)) or Decimal("0.01")
        ceiling = _floor_tick(self.pair_max - self.leg1.vwap, tick)

        while ceiling > 0 and self.leg1.vwap + ceiling > self.pair_max:
            ceiling -= tick

        ex = _buy_execution(other_book, self.leg1.shares, ceiling)
        if ex.available > 0:
            self.partial_ceiling_observations += 1

        row = {
            "hedge_side": other_side,
            "hedge_ceiling": _fmt(ceiling),
            "hedge_available_under_ceiling": _fmt(ex.available),
            "hedge_full_under_ceiling": int(ex.full),
            "hedge_vwap_under_ceiling": _fmt(ex.vwap),
            "hedge_worst_under_ceiling": _fmt(ex.worst_price),
            "combined_if_full": "",
            "gross_edge_per_pair_if_full": "",
            "gross_edge_total_if_full": "",
            "seconds_since_leg1": f"{now - self.leg1.recv_ts:.3f}",
        }

        if not ex.full or ex.vwap is None or ex.worst_price is None:
            return row, None

        combined = self.leg1.vwap + ex.vwap
        if combined >= Decimal("1"):
            return row, None

        edge = Decimal("1") - combined
        point = PairPoint(
            age_s=now - self.market.window_start,
            recv_ts=now,
            lag_s=now - self.leg1.recv_ts,
            hedge_side=other_side,
            best_ask=None if best is None else best[0],
            best_ask_size=None if best is None else best[1],
            ceiling=ceiling,
            available_under_ceiling=ex.available,
            leg2_vwap=ex.vwap,
            leg2_worst=ex.worst_price,
            combined=combined,
            gross_edge_per_pair=edge,
            gross_edge_total=edge * self.leg1.shares,
        )
        self.full_pair_observations += 1
        row.update(
            {
                "combined_if_full": _fmt(combined),
                "gross_edge_per_pair_if_full": _fmt(edge),
                "gross_edge_total_if_full": _fmt(point.gross_edge_total),
            }
        )

        if self.first_pair is None:
            self.first_pair = point
            print(
                f"PAIR FIRST {other_side} age={point.age_s:.1f}s lag={point.lag_s:.1f}s "
                f"bestAsk={_fmt(point.best_ask)} ceiling={ceiling} "
                f"avail={ex.available:.4f}sh vwap={ex.vwap:.6f} "
                f"combined={combined:.6f} edge5=${point.gross_edge_total:.6f}"
            )

        if self.best_pair is None or point.combined < self.best_pair.combined:
            old = self.best_pair.combined if self.best_pair is not None else None
            self.best_pair = point
            if old is None or old - point.combined >= Decimal("0.005"):
                print(
                    f"PAIR BEST  combined={point.combined:.6f} "
                    f"L1={self.leg1.vwap:.6f} L2={point.leg2_vwap:.6f} "
                    f"lag={point.lag_s:.1f}s edge5=${point.gross_edge_total:.6f}"
                )

        return row, point

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        print("\n" + "=" * 78)
        print(f"SESSION   {self.session_no}")
        print(f"MARKET    {self.market.slug}")
        print(f"CONDITION {self.market.condition_id}")
        print(f"WINDOW    {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")

        while time.time() < self.market.window_end:
            started = time.monotonic()
            now = time.time()
            try:
                up_book, down_book = await _books(
                    client, self.market.up_token_id, self.market.down_token_id
                )
            except Exception as exc:  # noqa: BLE001
                self.read_errors += 1
                print(f"READ ERR  {type(exc).__name__}: {exc}")
                await asyncio.sleep(min(1.0, self.poll_s))
                continue

            self.samples += 1
            self._try_lock_leg1(now, up_book, down_book)
            pair_row, _ = self._observe_pair(now, up_book, down_book)

            up_bid = _best(up_book, "bids")
            up_ask = _best(up_book, "asks")
            down_bid = _best(down_book, "bids")
            down_ask = _best(down_book, "asks")
            up_exec = _buy_execution(up_book, self.shares)
            down_exec = _buy_execution(down_book, self.shares)
            instant = (
                up_exec.vwap + down_exec.vwap
                if up_exec.full
                and down_exec.full
                and up_exec.vwap is not None
                and down_exec.vwap is not None
                else None
            )

            row = {
                "session": self.session_no,
                "market": self.market.slug,
                "condition_id": self.market.condition_id,
                "recv_iso_utc": _iso(now),
                "market_age_s": f"{now - self.market.window_start:.3f}",
                "seconds_to_end": f"{self.market.window_end - now:.3f}",
                "up_best_bid": _fmt(None if up_bid is None else up_bid[0]),
                "up_best_bid_size": _fmt(None if up_bid is None else up_bid[1]),
                "up_best_ask": _fmt(None if up_ask is None else up_ask[0]),
                "up_best_ask_size": _fmt(None if up_ask is None else up_ask[1]),
                "up_5_full": int(up_exec.full),
                "up_5_vwap": _fmt(up_exec.vwap),
                "up_5_worst": _fmt(up_exec.worst_price),
                "up_5_cost": _fmt(up_exec.cost),
                "down_best_bid": _fmt(None if down_bid is None else down_bid[0]),
                "down_best_bid_size": _fmt(None if down_bid is None else down_bid[1]),
                "down_best_ask": _fmt(None if down_ask is None else down_ask[0]),
                "down_best_ask_size": _fmt(None if down_ask is None else down_ask[1]),
                "down_5_full": int(down_exec.full),
                "down_5_vwap": _fmt(down_exec.vwap),
                "down_5_worst": _fmt(down_exec.worst_price),
                "down_5_cost": _fmt(down_exec.cost),
                "instant_5_pair_basis": _fmt(instant),
                "leg1_locked": int(self.leg1 is not None),
                "leg1_side": "" if self.leg1 is None else self.leg1.side,
                "leg1_age_s": "" if self.leg1 is None else f"{self.leg1.age_s:.3f}",
                "leg1_vwap": "" if self.leg1 is None else _fmt(self.leg1.vwap),
                "leg1_worst": "" if self.leg1 is None else _fmt(self.leg1.worst_price),
                "leg1_cost": "" if self.leg1 is None else _fmt(self.leg1.cost),
                **pair_row,
            }
            self.writer.writerow(row)

            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self.poll_s - elapsed))

        summary = {
            "session": self.session_no,
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "window_start": self.market.window_start,
            "window_end": self.market.window_end,
            "samples": self.samples,
            "read_errors": self.read_errors,
            "target_shares": str(self.shares),
            "first_trigger": str(self.first_trigger),
            "first_hard_cap": str(self.first_hard_cap),
            "pair_max": str(self.pair_max),
            "trigger_observations": self.trigger_observations,
            "trigger_rejected_for_depth": self.trigger_rejected_depth,
            "leg1": None if self.leg1 is None else {
                **asdict(self.leg1),
                "best_ask": str(self.leg1.best_ask),
                "best_ask_size": str(self.leg1.best_ask_size),
                "vwap": str(self.leg1.vwap),
                "worst_price": str(self.leg1.worst_price),
                "cost": str(self.leg1.cost),
                "shares": str(self.leg1.shares),
                "venue_min_size": str(self.leg1.venue_min_size),
                "tick": str(self.leg1.tick),
            },
            "post_leg1_observations": self.post_leg1_observations,
            "partial_liquidity_under_ceiling_observations": self.partial_ceiling_observations,
            "full_pair_observations": self.full_pair_observations,
            "first_pair": None if self.first_pair is None else self._point_dict(self.first_pair),
            "best_pair": None if self.best_pair is None else self._point_dict(self.best_pair),
            "interpretation": (
                "Read-only displayed-book observation. A paper LEG1/LEG2 event means the "
                "requested quantity was visibly executable in the sampled order book under "
                "the configured price constraint. It does not prove a real order would have "
                "filled because latency, queue priority, cancellations and fees are not modeled."
            ),
        }

        if self.leg1 is None:
            print("END       no qualifying paper LEG1")
        elif self.first_pair is None:
            print(
                f"END       L1={self.leg1.side}@{self.leg1.vwap:.6f}; "
                "no sampled full 5-share LEG2 below $1"
            )
        else:
            print(
                f"END       L1={self.leg1.side}@{self.leg1.vwap:.6f}; "
                f"first pair lag={self.first_pair.lag_s:.1f}s "
                f"best combined={self.best_pair.combined:.6f} "
                f"full-pair samples={self.full_pair_observations}/{self.post_leg1_observations}"
            )
        return summary

    @staticmethod
    def _point_dict(point: PairPoint) -> dict[str, Any]:
        return {
            "age_s": point.age_s,
            "recv_iso_utc": _iso(point.recv_ts),
            "lag_s": point.lag_s,
            "hedge_side": point.hedge_side,
            "best_ask": None if point.best_ask is None else str(point.best_ask),
            "best_ask_size": None if point.best_ask_size is None else str(point.best_ask_size),
            "ceiling": str(point.ceiling),
            "available_under_ceiling": str(point.available_under_ceiling),
            "leg2_vwap": str(point.leg2_vwap),
            "leg2_worst": str(point.leg2_worst),
            "combined": str(point.combined),
            "gross_edge_per_pair": str(point.gross_edge_per_pair),
            "gross_edge_total": str(point.gross_edge_total),
        }


async def _resolve_wait(client: AsyncPublicClient, target_start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, target_start)
        if market is not None:
            return market
        await asyncio.sleep(1.0)


def _aggregate(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    with_leg1 = [s for s in summaries if s["leg1"] is not None]
    paired = [s for s in summaries if s["first_pair"] is not None]
    lags = [float(s["first_pair"]["lag_s"]) for s in paired]
    bests = [Decimal(s["best_pair"]["combined"]) for s in paired]
    return {
        "sessions": len(summaries),
        "sessions_with_leg1": len(with_leg1),
        "sessions_with_sampled_full_sub1_pair": len(paired),
        "pair_rate_given_leg1": None if not with_leg1 else len(paired) / len(with_leg1),
        "median_time_leg1_to_first_pair_s": None if not lags else sorted(lags)[len(lags) // 2],
        "best_combined_across_sessions": None if not bests else str(min(bests)),
        "note": (
            "These are sampled displayed-book observations, not realized fills or P&L. "
            "Use the CSV to study persistence, depth and timing before encoding execution."
        ),
    }


async def amain(args: argparse.Namespace) -> int:
    output_root = Path(args.output)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = output_root / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    csv_path = run_dir / "observations.csv"
    summary_path = run_dir / "summary.json"

    start = window_start_epoch(DURATION_S, time.time()) + DURATION_S
    print("READ ONLY  no wallet, no key, no orders, no merges")
    print(
        f"PLAN       {args.sessions} consecutive BTC 5m sessions | "
        f"{args.shares} paper shares | poll={args.poll:.2f}s"
    )
    print(
        f"LEG1       first displayed ask<={args.first_trigger}, "
        f"requires full size through hard cap {args.first_hard_cap}"
    )
    print(
        f"LEG2       after paper LEG1, observe opposite displayed depth at "
        f"economic ceiling pair<{args.pair_max}"
    )
    print(f"OUTPUT     {run_dir}")

    client = AsyncPublicClient()
    summaries: list[dict[str, Any]] = []
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(f"WAIT       session {idx + 1}/{args.sessions} starts in {wait:.1f}s")
                    await asyncio.sleep(wait)

                market = await _resolve_wait(client, target)
                observer = SessionObserver(
                    session_no=idx + 1,
                    market=market,
                    shares=args.shares,
                    first_trigger=args.first_trigger,
                    first_hard_cap=args.first_hard_cap,
                    pair_max=args.pair_max,
                    poll_s=args.poll,
                    writer=writer,
                )
                summary = await observer.run(client)
                summaries.append(summary)
                fh.flush()

        final = {
            "created_utc": _iso(),
            "config": {
                "asset": ASSET,
                "duration_s": DURATION_S,
                "sessions": args.sessions,
                "poll_s": args.poll,
                "shares": str(args.shares),
                "first_trigger": str(args.first_trigger),
                "first_hard_cap": str(args.first_hard_cap),
                "pair_max": str(args.pair_max),
            },
            "aggregate": _aggregate(summaries),
            "sessions": summaries,
            "files": {
                "observations_csv": str(csv_path),
                "summary_json": str(summary_path),
            },
        }
        summary_path.write_text(json.dumps(final, indent=2, default=str), encoding="utf-8")

        print("\n" + "=" * 78)
        print("10-SESSION READ-ONLY SUMMARY")
        agg = final["aggregate"]
        print(f"SESSIONS   {agg['sessions']}")
        print(f"LEG1       {agg['sessions_with_leg1']}/{agg['sessions']} sessions")
        print(
            f"PAIR<1     {agg['sessions_with_sampled_full_sub1_pair']}/"
            f"{agg['sessions_with_leg1']} sessions with LEG1"
            if agg["sessions_with_leg1"]
            else "PAIR<1     n/a"
        )
        print(f"MEDIAN LAG {agg['median_time_leg1_to_first_pair_s']}")
        print(f"BEST PAIR  {agg['best_combined_across_sessions']}")
        print(f"CSV        {csv_path}")
        print(f"SUMMARY    {summary_path}")
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only 10-session BTC 5m two-leg strategy observer"
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    parser.add_argument("--shares", type=Decimal, default=DEFAULT_SHARES)
    parser.add_argument("--first-trigger", type=Decimal, default=DEFAULT_FIRST_TRIGGER)
    parser.add_argument("--first-hard-cap", type=Decimal, default=DEFAULT_FIRST_HARD_CAP)
    parser.add_argument("--pair-max", type=Decimal, default=DEFAULT_PAIR_MAX)
    parser.add_argument("--output", default="data/metamask_10session_observer")
    args = parser.parse_args()

    if not (1 <= args.sessions <= 50):
        parser.error("--sessions must be between 1 and 50")
    if not (0.10 <= args.poll <= 5.0):
        parser.error("--poll must be between 0.10 and 5.0 seconds")
    if args.shares <= 0:
        parser.error("--shares must be positive")
    if not (Decimal(0) < args.first_trigger < Decimal(1)):
        parser.error("--first-trigger must be between 0 and 1")
    if not (args.first_trigger <= args.first_hard_cap < Decimal(1)):
        parser.error("--first-hard-cap must be >= first-trigger and < 1")
    if not (args.first_hard_cap < args.pair_max < Decimal(1)):
        parser.error("--pair-max must be > first-hard-cap and < 1")

    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
