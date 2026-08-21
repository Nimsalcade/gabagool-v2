"""Official-SDK transport for the read-only BTC 5m tick recorder.

This keeps the strategy/room-map analysis from
`polymarket_metamask_10session_tick_recorder.py`, but replaces the hand-written
`websockets.connect()` transport with Polymarket py-sdk's supported realtime
subscription API:

    AsyncPublicClient.subscribe(MarketSpec(...))

The SDK owns heartbeat, reconnect scheduling, subscription replay, frame parsing,
and typed market events. This remains strictly READ ONLY: no wallet, signer, key,
orders, merges, approvals, or transactions.
"""
from __future__ import annotations

import asyncio
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from polymarket import AsyncPublicClient
from polymarket.errors import TransportError
from polymarket.streams import MarketBookEvent, MarketPriceChangeEvent, MarketSpec

from tools import polymarket_metamask_10session_tick_recorder as base


def _ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return base._ms(value)


def _book_to_wire(event: MarketBookEvent) -> dict[str, Any]:
    p = event.payload
    return {
        "event_type": "book",
        "market": str(p.market),
        "asset_id": str(p.token_id),
        "bids": [
            {"price": str(level.price), "size": str(level.size)}
            for level in p.bids
        ],
        "asks": [
            {"price": str(level.price), "size": str(level.size)}
            for level in p.asks
        ],
        "timestamp": _ts_ms(p.timestamp),
        "min_order_size": None if p.min_order_size is None else str(p.min_order_size),
        "tick_size": None if p.tick_size is None else str(p.tick_size),
        "neg_risk": p.neg_risk,
        "last_trade_price": (
            None if p.last_trade_price is None else str(p.last_trade_price)
        ),
    }


def _price_change_to_wire(event: MarketPriceChangeEvent) -> dict[str, Any]:
    p = event.payload
    return {
        "event_type": "price_change",
        "market": str(p.market),
        "timestamp": _ts_ms(p.timestamp),
        "price_changes": [
            {
                "asset_id": str(change.token_id),
                "price": str(change.price),
                "size": str(change.size),
                "side": str(change.side),
                "best_bid": (
                    None if change.best_bid is None else str(change.best_bid)
                ),
                "best_ask": (
                    None if change.best_ask is None else str(change.best_ask)
                ),
                "hash": change.hash,
            }
            for change in p.price_changes
        ],
    }


def _event_json(event: Any) -> Any:
    try:
        return event.model_dump(mode="json", by_alias=True)
    except Exception:
        return repr(event)


async def _sdk_run(self: Any):
    print("\n" + "=" * 82)
    print(f"SESSION    {self.session_no}")
    print(f"MARKET     {self.market.slug}")
    print(
        f"WINDOW     {base._iso_ms(self.market.window_start * 1000)} -> "
        f"{base._iso_ms(self.market.window_end * 1000)}"
    )
    print(f"RAW FILE   {self.raw_path}")
    print("TRANSPORT  official Polymarket AsyncPublicClient.subscribe(MarketSpec)")

    subscribe_attempts = 0
    reconnects_seen = 0
    disconnect_total_s = 0.0
    outage_started: float | None = self.market.window_start
    connected_once = False
    prev_open = False
    handle: Any = None
    sdk_handle_dropped = 0
    sdk_parser_dropped = 0

    with self.raw_path.open("w", encoding="utf-8") as raw_fh:
        async with AsyncPublicClient() as stream_client:
            # Initial subscription retries. Once subscribed, the SDK's own market
            # stream manager handles subsequent reconnects and state replay.
            while time.time() < self.market.window_end + 1 and handle is None:
                subscribe_attempts += 1
                try:
                    handle = await stream_client.subscribe(
                        MarketSpec(
                            token_ids=[
                                str(self.market.up_token_id),
                                str(self.market.down_token_id),
                            ]
                        )
                    )
                    connected_once = True
                    prev_open = True
                    now = time.time()
                    if outage_started is not None:
                        disconnect_total_s += max(0.0, now - outage_started)
                        outage_started = None
                    print(
                        "STREAM     SDK subscription live "
                        f"(attempt={subscribe_attempts})"
                    )
                except TransportError as exc:
                    remaining = max(0.0, self.market.window_end - time.time())
                    if remaining <= 0:
                        break
                    delay = min(8.0, 0.75 * (2 ** min(subscribe_attempts - 1, 4)))
                    delay = min(delay, remaining)
                    print(
                        f"STREAM ERR {type(exc).__name__}: {exc}; "
                        f"SDK subscribe retry in {delay:.2f}s"
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)

            if handle is not None:
                try:
                    while time.time() < self.market.window_end + 1:
                        manager = getattr(stream_client, "_market_manager", None)
                        is_open = bool(manager is not None and manager.is_open)
                        now = time.time()
                        if is_open != prev_open:
                            if not is_open:
                                outage_started = now
                                print("STREAM     SDK socket lost; internal reconnect active")
                            else:
                                if outage_started is not None:
                                    disconnect_total_s += max(0.0, now - outage_started)
                                    outage_started = None
                                reconnects_seen += 1
                                print(
                                    "STREAM     SDK socket restored "
                                    f"(reconnects_seen={reconnects_seen})"
                                )
                            prev_open = is_open

                        timeout = max(
                            0.05,
                            min(1.0, self.market.window_end + 1 - time.time()),
                        )
                        try:
                            event = await asyncio.wait_for(handle.__anext__(), timeout=timeout)
                        except asyncio.TimeoutError:
                            continue
                        except StopAsyncIteration:
                            break

                        recv_ms = int(time.time() * 1000)
                        self.raw_messages += 1
                        raw_fh.write(
                            json.dumps(
                                {
                                    "recv_ts_ms": recv_ms,
                                    "transport": "polymarket_py_sdk",
                                    "event": _event_json(event),
                                },
                                separators=(",", ":"),
                                default=str,
                            )
                            + "\n"
                        )

                        if isinstance(event, MarketBookEvent):
                            data = _book_to_wire(event)
                            self._handle_book(
                                data,
                                recv_ms,
                                _ts_ms(event.payload.timestamp),
                            )
                        elif isinstance(event, MarketPriceChangeEvent):
                            data = _price_change_to_wire(event)
                            self._handle_price_changes(
                                data,
                                recv_ms,
                                _ts_ms(event.payload.timestamp),
                            )
                finally:
                    sdk_handle_dropped = int(getattr(handle, "dropped", 0) or 0)
                    manager = getattr(stream_client, "_market_manager", None)
                    if manager is not None:
                        sdk_parser_dropped = int(
                            getattr(manager, "dropped_events", 0) or 0
                        )
                    await handle.close()

    if outage_started is not None:
        disconnect_total_s += max(
            0.0, min(time.time(), self.market.window_end) - outage_started
        )

    first_age = self.points[0].age_s if self.points else None
    last_age = self.points[-1].age_s if self.points else None
    full_window = bool(
        self.points
        and first_age is not None
        and last_age is not None
        and first_age <= 3.0
        and last_age >= 297.0
        and disconnect_total_s <= 3.0
        and sdk_handle_dropped == 0
        and sdk_parser_dropped == 0
    )

    quality_path = self.raw_path.parent.parent / "connection_quality.csv"
    exists = quality_path.exists()
    with quality_path.open("a", newline="", encoding="utf-8") as qfh:
        fields = [
            "session",
            "market",
            "transport",
            "connected_once",
            "subscribe_attempts",
            "reconnects_seen",
            "disconnect_total_s",
            "sdk_handle_dropped",
            "sdk_parser_dropped",
            "first_tick_age_s",
            "last_tick_age_s",
            "normalized_ticks",
            "full_window_quality",
        ]
        writer = csv.DictWriter(qfh, fieldnames=fields)
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "session": self.session_no,
                "market": self.market.slug,
                "transport": "polymarket_py_sdk",
                "connected_once": int(connected_once),
                "subscribe_attempts": subscribe_attempts,
                "reconnects_seen": reconnects_seen,
                "disconnect_total_s": f"{disconnect_total_s:.3f}",
                "sdk_handle_dropped": sdk_handle_dropped,
                "sdk_parser_dropped": sdk_parser_dropped,
                "first_tick_age_s": "" if first_age is None else f"{first_age:.3f}",
                "last_tick_age_s": "" if last_age is None else f"{last_age:.3f}",
                "normalized_ticks": len(self.points),
                "full_window_quality": int(full_window),
            }
        )

    print(
        f"END RAW    messages={self.raw_messages} price_ticks={self.price_change_ticks} "
        f"book_events={self.book_events} normalized_ticks={len(self.points)}"
    )
    print(
        f"QUALITY    full_window={full_window} attempts={subscribe_attempts} "
        f"reconnects_seen={reconnects_seen} disconnect≈{disconnect_total_s:.2f}s "
        f"handle_dropped={sdk_handle_dropped} parser_dropped={sdk_parser_dropped} "
        f"first_age={first_age} last_age={last_age}"
    )
    return self.points


# Replace only capture transport. Strategy calculations, MetaMask 250ms lens,
# trigger paths, room maps, summaries, CLI flags, and file formats stay the same.
base.SessionRecorder.run = _sdk_run  # type: ignore[method-assign]


if __name__ == "__main__":
    base.main()
