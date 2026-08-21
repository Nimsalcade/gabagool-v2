"""Record 10 BTC 5-minute markets tick-by-tick, read-only.

This recorder is designed for strategy discovery before any more live-money execution.
It records the underlying Polymarket CLOB market stream at native WebSocket event
frequency and, from the SAME raw feed, derives a MetaMask-Predict-style observation
stream using the 250 ms leading/trailing throttle used by MetaMask Mobile.

Important: MetaMask Predictions' PolymarketProvider consumes the Polymarket CLOB
market WebSocket. These are not two independent liquidity pools. The comparison here
is therefore:

    POLYMARKET_RAW  = every received CLOB price/book event
    METAMASK_VIEW   = the same price stream after MetaMask-style 250 ms coalescing

No wallet, signer, private key, approvals, orders, merges, or transactions are used.

Outputs per run:
  raw_ticks.csv            every individual price_change tick + book top updates
  metamask_view_ticks.csv  MetaMask-style 250 ms leading/trailing price view
  book_depth.csv           every full book snapshot with 5-share displayed VWAP/depth
  trigger_paths.csv        every post-LEG1 tick for the first <= trigger path/session
  room_map.csv             for every observed LEG1 price tick, best later opposite tick
  raw_events/*.jsonl       unmodified WebSocket payloads per market
  summary.json             per-session RAW vs METAMASK comparison

The room map is chronological: the second-leg observation must occur after the
first-leg observation. It is a displayed-price opportunity map, not a fill claim.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import websockets
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch

ASSET = "btc"
DURATION_S = 300
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_SESSIONS = 10
DEFAULT_SHARES = Decimal("5")
DEFAULT_TRIGGER = Decimal("0.25")
METAMASK_THROTTLE_MS = 250
EPS = Decimal("0.000001")


@dataclass
class SideTop:
    bid: Decimal | None = None
    bid_size: Decimal | None = None
    ask: Decimal | None = None
    ask_size: Decimal | None = None


@dataclass
class TickPoint:
    seq: int
    recv_ms: int
    exchange_ms: int | None
    age_s: float
    seconds_to_end: float
    event_type: str
    changed_side: str
    changed_token: str
    change_side: str
    change_price: Decimal | None
    change_size: Decimal | None
    up_bid: Decimal | None
    up_ask: Decimal | None
    down_bid: Decimal | None
    down_ask: Decimal | None


RAW_FIELDS = [
    "session", "market", "condition_id", "seq", "recv_ts_ms", "recv_iso_utc",
    "exchange_ts_ms", "market_age_s", "seconds_to_end", "event_type",
    "changed_side", "changed_token", "change_side", "change_price", "change_size",
    "up_bid", "up_ask", "down_bid", "down_ask", "instant_pair_ask",
    "instant_pair_edge",
]

VIEW_FIELDS = [
    "session", "market", "condition_id", "view_seq", "source_seq", "emit_ts_ms",
    "emit_iso_utc", "market_age_s", "seconds_to_end", "up_bid", "up_ask",
    "down_bid", "down_ask", "instant_pair_ask", "instant_pair_edge",
]

DEPTH_FIELDS = [
    "session", "market", "condition_id", "recv_ts_ms", "recv_iso_utc",
    "exchange_ts_ms", "market_age_s", "seconds_to_end", "side", "token_id",
    "tick_size", "min_order_size", "best_bid", "best_bid_size", "best_ask",
    "best_ask_size", "target_shares", "target_full", "target_vwap", "target_worst",
    "target_cost", "total_ask_depth",
]

PATH_FIELDS = [
    "lens", "session", "market", "condition_id", "leg1_side", "leg1_price",
    "leg1_age_s", "leg2_side", "tick_seq", "tick_age_s", "lag_s", "leg2_ask",
    "combined_top", "gross_edge_per_pair", "gross_edge_5sh", "under_1",
]

ROOM_FIELDS = [
    "lens", "session", "market", "condition_id", "direction", "leg1_side",
    "leg2_side", "leg1_price_tick", "leg1_observations", "leg1_earliest_age_s",
    "leg1_latest_age_s", "best_leg1_age_s", "best_later_leg2_price",
    "best_leg2_age_s", "best_lag_s", "best_combined", "gross_edge_per_pair",
    "gross_edge_5sh", "sub1_possible",
]


def _d(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        x = Decimal(str(value))
        return x if x.is_finite() else None
    except Exception:
        return None


def _iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _s(value: Decimal | None) -> str:
    return "" if value is None else format(value, "f")


def _ms(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        n = int(value)
        return n if n > 10_000_000_000 else n * 1000
    except Exception:
        return None


def _parse_levels(levels: Iterable[Any], *, is_bid: bool) -> list[tuple[Decimal, Decimal]]:
    out: list[tuple[Decimal, Decimal]] = []
    for level in levels or []:
        if not isinstance(level, dict):
            continue
        p = _d(level.get("price"))
        z = _d(level.get("size"))
        if p is None or z is None or p <= 0 or z <= 0:
            continue
        out.append((p, z))
    out.sort(key=lambda x: x[0], reverse=is_bid)
    return out


def _best(levels: list[tuple[Decimal, Decimal]]) -> tuple[Decimal, Decimal] | None:
    return levels[0] if levels else None


def _buy_depth(asks: list[tuple[Decimal, Decimal]], shares: Decimal) -> dict[str, Any]:
    remaining = shares
    filled = Decimal(0)
    cost = Decimal(0)
    worst: Decimal | None = None
    total = sum((z for _, z in asks), Decimal(0))
    for price, size in asks:
        if remaining <= EPS:
            break
        take = min(size, remaining)
        if take <= 0:
            continue
        filled += take
        cost += take * price
        remaining -= take
        worst = price
    full = filled + EPS >= shares
    return {
        "full": full,
        "vwap": (cost / filled) if filled > 0 else None,
        "worst": worst,
        "cost": cost,
        "filled": filled,
        "total": total,
    }


def _pair(a: Decimal | None, b: Decimal | None) -> tuple[Decimal | None, Decimal | None]:
    if a is None or b is None:
        return None, None
    combined = a + b
    return combined, Decimal(1) - combined


def _tick_to_raw_row(session_no: int, market: Any, point: TickPoint) -> dict[str, Any]:
    pair, edge = _pair(point.up_ask, point.down_ask)
    return {
        "session": session_no,
        "market": market.slug,
        "condition_id": market.condition_id,
        "seq": point.seq,
        "recv_ts_ms": point.recv_ms,
        "recv_iso_utc": _iso_ms(point.recv_ms),
        "exchange_ts_ms": point.exchange_ms or "",
        "market_age_s": f"{point.age_s:.6f}",
        "seconds_to_end": f"{point.seconds_to_end:.6f}",
        "event_type": point.event_type,
        "changed_side": point.changed_side,
        "changed_token": point.changed_token,
        "change_side": point.change_side,
        "change_price": _s(point.change_price),
        "change_size": _s(point.change_size),
        "up_bid": _s(point.up_bid),
        "up_ask": _s(point.up_ask),
        "down_bid": _s(point.down_bid),
        "down_ask": _s(point.down_ask),
        "instant_pair_ask": _s(pair),
        "instant_pair_edge": _s(edge),
    }


def _metamask_view(points: list[TickPoint], market: Any) -> list[TickPoint]:
    """Apply MetaMask Mobile's 250 ms leading+trailing price coalescing."""
    if not points:
        return []
    emitted: list[TickPoint] = []
    timer_end: int | None = None
    pending: TickPoint | None = None

    for point in points:
        if timer_end is not None and point.recv_ms >= timer_end:
            if pending is not None:
                emitted.append(TickPoint(
                    **{**pending.__dict__, "recv_ms": timer_end,
                       "age_s": timer_end / 1000 - market.window_start,
                       "seconds_to_end": market.window_end - timer_end / 1000}
                ))
            timer_end = None
            pending = None

        if timer_end is None:
            emitted.append(point)
            timer_end = point.recv_ms + METAMASK_THROTTLE_MS
        else:
            pending = point

    if timer_end is not None and pending is not None:
        emitted.append(TickPoint(
            **{**pending.__dict__, "recv_ms": timer_end,
               "age_s": timer_end / 1000 - market.window_start,
               "seconds_to_end": market.window_end - timer_end / 1000}
        ))
    return emitted


def _write_view_rows(
    writer: csv.DictWriter, *, session_no: int, market: Any, points: list[TickPoint]
) -> None:
    for view_seq, p in enumerate(points, 1):
        pair, edge = _pair(p.up_ask, p.down_ask)
        writer.writerow({
            "session": session_no,
            "market": market.slug,
            "condition_id": market.condition_id,
            "view_seq": view_seq,
            "source_seq": p.seq,
            "emit_ts_ms": p.recv_ms,
            "emit_iso_utc": _iso_ms(p.recv_ms),
            "market_age_s": f"{p.age_s:.6f}",
            "seconds_to_end": f"{p.seconds_to_end:.6f}",
            "up_bid": _s(p.up_bid),
            "up_ask": _s(p.up_ask),
            "down_bid": _s(p.down_bid),
            "down_ask": _s(p.down_ask),
            "instant_pair_ask": _s(pair),
            "instant_pair_edge": _s(edge),
        })


def _first_trigger(points: list[TickPoint], trigger: Decimal) -> tuple[str, Decimal, float, int] | None:
    for p in points:
        candidates: list[tuple[str, Decimal]] = []
        if p.up_ask is not None and p.up_ask <= trigger:
            candidates.append(("UP", p.up_ask))
        if p.down_ask is not None and p.down_ask <= trigger:
            candidates.append(("DOWN", p.down_ask))
        if candidates:
            candidates.sort(key=lambda x: x[1])
            side, price = candidates[0]
            return side, price, p.age_s, p.seq
    return None


def _write_trigger_path(
    writer: csv.DictWriter, *, lens: str, session_no: int, market: Any,
    points: list[TickPoint], trigger: Decimal, shares: Decimal,
) -> dict[str, Any]:
    leg1 = _first_trigger(points, trigger)
    if leg1 is None:
        return {"leg1": None, "sub1_ticks": 0, "best_combined": None}
    side, p1, age1, seq1 = leg1
    other = "DOWN" if side == "UP" else "UP"
    sub1 = 0
    best: Decimal | None = None
    best_age: float | None = None
    first_sub1_age: float | None = None

    for p in points:
        if p.seq < seq1:
            continue
        p2 = p.down_ask if other == "DOWN" else p.up_ask
        if p2 is None:
            continue
        combined = p1 + p2
        under = combined < Decimal(1)
        edge = Decimal(1) - combined
        if under:
            sub1 += 1
            if first_sub1_age is None:
                first_sub1_age = p.age_s
            if best is None or combined < best:
                best = combined
                best_age = p.age_s
        writer.writerow({
            "lens": lens,
            "session": session_no,
            "market": market.slug,
            "condition_id": market.condition_id,
            "leg1_side": side,
            "leg1_price": _s(p1),
            "leg1_age_s": f"{age1:.6f}",
            "leg2_side": other,
            "tick_seq": p.seq,
            "tick_age_s": f"{p.age_s:.6f}",
            "lag_s": f"{p.age_s - age1:.6f}",
            "leg2_ask": _s(p2),
            "combined_top": _s(combined),
            "gross_edge_per_pair": _s(edge),
            "gross_edge_5sh": _s(edge * shares),
            "under_1": int(under),
        })

    return {
        "leg1": {"side": side, "price": str(p1), "age_s": age1},
        "sub1_ticks": sub1,
        "first_sub1_age_s": first_sub1_age,
        "first_sub1_lag_s": None if first_sub1_age is None else first_sub1_age - age1,
        "best_combined": None if best is None else str(best),
        "best_pair_age_s": best_age,
        "best_pair_lag_s": None if best_age is None else best_age - age1,
    }


def _suffix_min(points: list[TickPoint], side: str) -> tuple[list[Decimal | None], list[float | None]]:
    n = len(points)
    vals: list[Decimal | None] = [None] * n
    ages: list[float | None] = [None] * n
    best: Decimal | None = None
    best_age: float | None = None
    for i in range(n - 1, -1, -1):
        # Strictly later: store current best BEFORE considering point i.
        vals[i] = best
        ages[i] = best_age
        value = points[i].up_ask if side == "UP" else points[i].down_ask
        if value is not None and (best is None or value < best):
            best = value
            best_age = points[i].age_s
    return vals, ages


def _write_room_map(
    writer: csv.DictWriter, *, lens: str, session_no: int, market: Any,
    points: list[TickPoint], shares: Decimal,
) -> dict[str, Any]:
    best_overall: Decimal | None = None
    best_direction: str | None = None
    rows_written = 0

    for first_side, second_side in (("UP", "DOWN"), ("DOWN", "UP")):
        later_min, later_age = _suffix_min(points, second_side)
        groups: dict[Decimal, dict[str, Any]] = {}
        for i, p in enumerate(points):
            p1 = p.up_ask if first_side == "UP" else p.down_ask
            if p1 is None:
                continue
            g = groups.setdefault(p1, {
                "count": 0, "earliest": p.age_s, "latest": p.age_s,
                "best_combined": None, "best_leg1_age": None,
                "best_leg2": None, "best_leg2_age": None,
            })
            g["count"] += 1
            g["earliest"] = min(g["earliest"], p.age_s)
            g["latest"] = max(g["latest"], p.age_s)
            p2 = later_min[i]
            a2 = later_age[i]
            if p2 is None or a2 is None:
                continue
            combined = p1 + p2
            if g["best_combined"] is None or combined < g["best_combined"]:
                g["best_combined"] = combined
                g["best_leg1_age"] = p.age_s
                g["best_leg2"] = p2
                g["best_leg2_age"] = a2

        for p1 in sorted(groups):
            g = groups[p1]
            combined = g["best_combined"]
            edge = None if combined is None else Decimal(1) - combined
            sub1 = combined is not None and combined < Decimal(1)
            if sub1 and (best_overall is None or combined < best_overall):
                best_overall = combined
                best_direction = f"{first_side}->{second_side}"
            lag = None
            if g["best_leg1_age"] is not None and g["best_leg2_age"] is not None:
                lag = g["best_leg2_age"] - g["best_leg1_age"]
            writer.writerow({
                "lens": lens,
                "session": session_no,
                "market": market.slug,
                "condition_id": market.condition_id,
                "direction": f"{first_side}->{second_side}",
                "leg1_side": first_side,
                "leg2_side": second_side,
                "leg1_price_tick": _s(p1),
                "leg1_observations": g["count"],
                "leg1_earliest_age_s": f"{g['earliest']:.6f}",
                "leg1_latest_age_s": f"{g['latest']:.6f}",
                "best_leg1_age_s": "" if g["best_leg1_age"] is None else f"{g['best_leg1_age']:.6f}",
                "best_later_leg2_price": _s(g["best_leg2"]),
                "best_leg2_age_s": "" if g["best_leg2_age"] is None else f"{g['best_leg2_age']:.6f}",
                "best_lag_s": "" if lag is None else f"{lag:.6f}",
                "best_combined": _s(combined),
                "gross_edge_per_pair": _s(edge),
                "gross_edge_5sh": _s(None if edge is None else edge * shares),
                "sub1_possible": int(sub1),
            })
            rows_written += 1

    return {
        "room_rows": rows_written,
        "best_sequential_combined": None if best_overall is None else str(best_overall),
        "best_direction": best_direction,
    }


class SessionRecorder:
    def __init__(
        self, *, session_no: int, market: Any, shares: Decimal,
        raw_writer: csv.DictWriter, depth_writer: csv.DictWriter,
        raw_event_dir: Path,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.shares = shares
        self.raw_writer = raw_writer
        self.depth_writer = depth_writer
        self.raw_path = raw_event_dir / f"{market.slug}.jsonl"
        self.token_side = {
            str(market.up_token_id): "UP",
            str(market.down_token_id): "DOWN",
        }
        self.up = SideTop()
        self.down = SideTop()
        self.points: list[TickPoint] = []
        self.seq = 0
        self.raw_messages = 0
        self.price_change_ticks = 0
        self.book_events = 0

    def _top(self, side: str) -> SideTop:
        return self.up if side == "UP" else self.down

    def _append_point(
        self, *, recv_ms: int, exchange_ms: int | None, event_type: str,
        changed_side: str = "", changed_token: str = "", change_side: str = "",
        change_price: Decimal | None = None, change_size: Decimal | None = None,
    ) -> None:
        self.seq += 1
        p = TickPoint(
            seq=self.seq,
            recv_ms=recv_ms,
            exchange_ms=exchange_ms,
            age_s=recv_ms / 1000 - self.market.window_start,
            seconds_to_end=self.market.window_end - recv_ms / 1000,
            event_type=event_type,
            changed_side=changed_side,
            changed_token=changed_token,
            change_side=change_side,
            change_price=change_price,
            change_size=change_size,
            up_bid=self.up.bid,
            up_ask=self.up.ask,
            down_bid=self.down.bid,
            down_ask=self.down.ask,
        )
        self.points.append(p)
        self.raw_writer.writerow(_tick_to_raw_row(self.session_no, self.market, p))

    def _handle_book(self, data: dict[str, Any], recv_ms: int, exchange_ms: int | None) -> None:
        token = str(data.get("asset_id") or "")
        side = self.token_side.get(token)
        if not side:
            return
        bids = _parse_levels(data.get("bids") or [], is_bid=True)
        asks = _parse_levels(data.get("asks") or [], is_bid=False)
        b = _best(bids)
        a = _best(asks)
        top = self._top(side)
        top.bid, top.bid_size = (b if b else (None, None))
        top.ask, top.ask_size = (a if a else (None, None))
        self.book_events += 1

        tick_size = _d(data.get("tick_size"))
        min_size = _d(data.get("min_order_size"))
        ex = _buy_depth(asks, self.shares)
        self.depth_writer.writerow({
            "session": self.session_no,
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "recv_ts_ms": recv_ms,
            "recv_iso_utc": _iso_ms(recv_ms),
            "exchange_ts_ms": exchange_ms or "",
            "market_age_s": f"{recv_ms / 1000 - self.market.window_start:.6f}",
            "seconds_to_end": f"{self.market.window_end - recv_ms / 1000:.6f}",
            "side": side,
            "token_id": token,
            "tick_size": _s(tick_size),
            "min_order_size": _s(min_size),
            "best_bid": _s(None if b is None else b[0]),
            "best_bid_size": _s(None if b is None else b[1]),
            "best_ask": _s(None if a is None else a[0]),
            "best_ask_size": _s(None if a is None else a[1]),
            "target_shares": _s(self.shares),
            "target_full": int(ex["full"]),
            "target_vwap": _s(ex["vwap"]),
            "target_worst": _s(ex["worst"]),
            "target_cost": _s(ex["cost"]),
            "total_ask_depth": _s(ex["total"]),
        })
        self._append_point(
            recv_ms=recv_ms, exchange_ms=exchange_ms, event_type="book",
            changed_side=side, changed_token=token,
        )

    def _handle_price_changes(self, data: dict[str, Any], recv_ms: int, exchange_ms: int | None) -> None:
        for change in data.get("price_changes") or []:
            if not isinstance(change, dict):
                continue
            token = str(change.get("asset_id") or "")
            side = self.token_side.get(token)
            if not side:
                continue
            top = self._top(side)
            bb = _d(change.get("best_bid"))
            ba = _d(change.get("best_ask"))
            if bb is not None:
                top.bid = bb
            if ba is not None:
                top.ask = ba
            self.price_change_ticks += 1
            self._append_point(
                recv_ms=recv_ms,
                exchange_ms=exchange_ms,
                event_type="price_change",
                changed_side=side,
                changed_token=token,
                change_side=str(change.get("side") or ""),
                change_price=_d(change.get("price")),
                change_size=_d(change.get("size")),
            )

    async def run(self) -> list[TickPoint]:
        print("\n" + "=" * 82)
        print(f"SESSION    {self.session_no}")
        print(f"MARKET     {self.market.slug}")
        print(f"WINDOW     {_iso_ms(self.market.window_start * 1000)} -> {_iso_ms(self.market.window_end * 1000)}")
        print(f"RAW FILE   {self.raw_path}")

        with self.raw_path.open("w", encoding="utf-8") as raw_fh:
            while time.time() < self.market.window_end + 1:
                try:
                    async with websockets.connect(
                        MARKET_WS_URL,
                        ping_interval=None,
                        close_timeout=5,
                        max_size=16 * 1024 * 1024,
                    ) as ws:
                        await ws.send(json.dumps({
                            "type": "market",
                            "assets_ids": [self.market.up_token_id, self.market.down_token_id],
                        }))
                        print("STREAM     connected; recording every raw market tick")
                        next_ping = time.monotonic() + 50
                        while time.time() < self.market.window_end + 1:
                            if time.monotonic() >= next_ping:
                                await ws.send("PING")
                                next_ping = time.monotonic() + 50
                            timeout = max(0.05, min(2.0, self.market.window_end + 1 - time.time()))
                            try:
                                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            except asyncio.TimeoutError:
                                continue
                            if not isinstance(message, str):
                                continue
                            text = message.strip()
                            if not text or text in {"PING", "PONG"}:
                                continue
                            recv_ms = int(time.time() * 1000)
                            try:
                                parsed = json.loads(text)
                            except json.JSONDecodeError:
                                continue
                            self.raw_messages += 1
                            raw_fh.write(json.dumps({"recv_ts_ms": recv_ms, "payload": parsed}, separators=(",", ":")) + "\n")
                            events = parsed if isinstance(parsed, list) else [parsed]
                            for data in events:
                                if not isinstance(data, dict):
                                    continue
                                event_type = str(data.get("event_type") or data.get("type") or "")
                                exchange_ms = _ms(data.get("timestamp"))
                                if event_type == "book":
                                    self._handle_book(data, recv_ms, exchange_ms)
                                elif event_type == "price_change":
                                    self._handle_price_changes(data, recv_ms, exchange_ms)
                                else:
                                    # Preserve every raw payload in JSONL; only price/book events
                                    # become normalized tick rows used for strategy math.
                                    continue
                        break
                except Exception as exc:  # noqa: BLE001
                    if time.time() >= self.market.window_end:
                        break
                    print(f"STREAM ERR {type(exc).__name__}: {exc}; reconnecting in 0.5s")
                    await asyncio.sleep(0.5)

        print(
            f"END RAW    messages={self.raw_messages} price_ticks={self.price_change_ticks} "
            f"book_events={self.book_events} normalized_ticks={len(self.points)}"
        )
        return self.points


async def _resolve_wait(client: AsyncPublicClient, start_epoch: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, start_epoch)
        if market is not None:
            return market
        await asyncio.sleep(0.5)


def _lens_summary(
    *, lens: str, session_no: int, market: Any, points: list[TickPoint],
    trigger: Decimal, shares: Decimal, path_writer: csv.DictWriter,
    room_writer: csv.DictWriter,
) -> dict[str, Any]:
    path = _write_trigger_path(
        path_writer, lens=lens, session_no=session_no, market=market,
        points=points, trigger=trigger, shares=shares,
    )
    room = _write_room_map(
        room_writer, lens=lens, session_no=session_no, market=market,
        points=points, shares=shares,
    )
    return {
        "ticks": len(points),
        "trigger_path": path,
        **room,
    }


async def amain(args: argparse.Namespace) -> int:
    root = Path(args.output)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = root / f"run_{stamp}"
    raw_dir = run_dir / "raw_events"
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = run_dir / "raw_ticks.csv"
    view_path = run_dir / "metamask_view_ticks.csv"
    depth_path = run_dir / "book_depth.csv"
    path_path = run_dir / "trigger_paths.csv"
    room_path = run_dir / "room_map.csv"
    summary_path = run_dir / "summary.json"

    # Start with the NEXT full window by default so every session is complete.
    start = window_start_epoch(DURATION_S, time.time())
    if not args.current:
        start += DURATION_S

    print("READ ONLY   no wallet, no key, no orders, no merges")
    print(f"PLAN        {args.sessions} consecutive BTC 5m sessions, native WebSocket ticks")
    print("POLY RAW    every received CLOB price_change/book event")
    print("METAMASK    same CLOB feed emulated with 250ms leading/trailing price throttle")
    print(f"TRIGGER     paper first leg <= {args.trigger}; all later opposite ticks retained")
    print("ROOM MAP    every observed first-leg price tick -> best strictly-later opposite tick")
    print(f"OUTPUT      {run_dir}")

    client = AsyncPublicClient()
    summaries: list[dict[str, Any]] = []
    try:
        with (
            raw_path.open("w", newline="", encoding="utf-8") as raw_fh,
            view_path.open("w", newline="", encoding="utf-8") as view_fh,
            depth_path.open("w", newline="", encoding="utf-8") as depth_fh,
            path_path.open("w", newline="", encoding="utf-8") as path_fh,
            room_path.open("w", newline="", encoding="utf-8") as room_fh,
        ):
            raw_writer = csv.DictWriter(raw_fh, fieldnames=RAW_FIELDS)
            view_writer = csv.DictWriter(view_fh, fieldnames=VIEW_FIELDS)
            depth_writer = csv.DictWriter(depth_fh, fieldnames=DEPTH_FIELDS)
            path_writer = csv.DictWriter(path_fh, fieldnames=PATH_FIELDS)
            room_writer = csv.DictWriter(room_fh, fieldnames=ROOM_FIELDS)
            for w in (raw_writer, view_writer, depth_writer, path_writer, room_writer):
                w.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(f"WAIT        session {idx + 1}/{args.sessions} starts in {wait:.1f}s")
                    await asyncio.sleep(wait)
                market = await _resolve_wait(client, target)
                rec = SessionRecorder(
                    session_no=idx + 1,
                    market=market,
                    shares=args.shares,
                    raw_writer=raw_writer,
                    depth_writer=depth_writer,
                    raw_event_dir=raw_dir,
                )
                raw_points = await rec.run()
                view_points = _metamask_view(raw_points, market)
                _write_view_rows(
                    view_writer, session_no=idx + 1, market=market, points=view_points
                )

                raw_summary = _lens_summary(
                    lens="POLYMARKET_RAW", session_no=idx + 1, market=market,
                    points=raw_points, trigger=args.trigger, shares=args.shares,
                    path_writer=path_writer, room_writer=room_writer,
                )
                view_summary = _lens_summary(
                    lens="METAMASK_250MS", session_no=idx + 1, market=market,
                    points=view_points, trigger=args.trigger, shares=args.shares,
                    path_writer=path_writer, room_writer=room_writer,
                )
                session_summary = {
                    "session": idx + 1,
                    "market": market.slug,
                    "condition_id": market.condition_id,
                    "window_start": market.window_start,
                    "window_end": market.window_end,
                    "raw_messages": rec.raw_messages,
                    "price_change_ticks": rec.price_change_ticks,
                    "book_events": rec.book_events,
                    "polymarket_raw": raw_summary,
                    "metamask_250ms": view_summary,
                }
                summaries.append(session_summary)
                print(
                    "COMPARE    "
                    f"RAW best={raw_summary['best_sequential_combined']} "
                    f"| MM250 best={view_summary['best_sequential_combined']} "
                    f"| raw_ticks={len(raw_points)} mm_ticks={len(view_points)}"
                )
                for fh in (raw_fh, view_fh, depth_fh, path_fh, room_fh):
                    fh.flush()

        final = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "config": {
                "sessions": args.sessions,
                "shares": str(args.shares),
                "trigger": str(args.trigger),
                "metamask_throttle_ms": METAMASK_THROTTLE_MS,
                "source": MARKET_WS_URL,
            },
            "meaning": {
                "polymarket_raw": "Native Polymarket CLOB market WebSocket event stream.",
                "metamask_250ms": (
                    "Same underlying Polymarket price stream after emulating MetaMask Mobile's "
                    "250ms leading/trailing market-price coalescing. Not a separate liquidity pool."
                ),
                "book_depth": (
                    "5-share displayed depth is calculated only from full book snapshots. "
                    "price_change ticks provide faster best-bid/ask timing but not a complete book."
                ),
                "caution": (
                    "Displayed chronological sub-$1 combinations are opportunity observations, "
                    "not guaranteed fills; queue priority, latency, cancellation and fees are separate."
                ),
            },
            "sessions": summaries,
            "files": {
                "raw_ticks": str(raw_path),
                "metamask_view_ticks": str(view_path),
                "book_depth": str(depth_path),
                "trigger_paths": str(path_path),
                "room_map": str(room_path),
                "raw_events_dir": str(raw_dir),
            },
        }
        summary_path.write_text(json.dumps(final, indent=2), encoding="utf-8")
        print("\n" + "=" * 82)
        print("DONE        10-session tick recording complete")
        print(f"RAW TICKS   {raw_path}")
        print(f"MM VIEW     {view_path}")
        print(f"DEPTH       {depth_path}")
        print(f"PATHS       {path_path}")
        print(f"ROOM MAP    {room_path}")
        print(f"SUMMARY     {summary_path}")
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only BTC 5m native-tick Polymarket vs MetaMask observer"
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--shares", type=Decimal, default=DEFAULT_SHARES)
    parser.add_argument("--trigger", type=Decimal, default=DEFAULT_TRIGGER)
    parser.add_argument("--current", action="store_true", help="join current window instead of next full window")
    parser.add_argument("--output", default="data/polymarket_metamask_tick_compare")
    args = parser.parse_args()
    if not (1 <= args.sessions <= 50):
        parser.error("--sessions must be 1..50")
    if args.shares <= 0:
        parser.error("--shares must be positive")
    if not (Decimal(0) < args.trigger < Decimal(1)):
        parser.error("--trigger must be between 0 and 1")
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
