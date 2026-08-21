"""Read-only BTC 5m tick recorder using Polymarket's official Python SDK stream.

This keeps the strategy-analysis outputs from
`polymarket_metamask_10session_tick_recorder.py` but replaces the hand-written
`websockets.connect()` transport with `AsyncPublicClient.subscribe(MarketSpec)`.

Why: the SDK market stream owns heartbeat, reconnect scheduling, subscription
replay after reconnect, event parsing, and malformed-event accounting. That is
safer for a long native-tick capture than repeatedly opening raw sockets.

No wallet, signer, private key, orders, merges, or transactions are used.
"""
from __future__ import annotations

import asyncio
import csv
import json
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

from polymarket import AsyncPublicClient
from polymarket.errors import TransportError
from polymarket.models.clob.market_events import MarketBookEvent, MarketPriceChangeEvent
from polymarket.streams import MarketSpec

from tools import polymarket_metamask_10session_tick_recorder as base


def _dt_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value.timestamp() * 1000)
    except Exception:
        return None


def _book_wire(event: MarketBookEvent) -> dict[str, Any]:
    p = event.payload
    return {
        "event_type": "book",
        "market": p.market,
        "asset_id": str(p.token_id),
        "bids": [
            {"price": str(level.price), "size": str(level.size)} for level in p.bids
        ],
        "asks": [
            {"price": str(level.price), "size": str(level.size)} for level in p.asks
        ],
        "hash": p.hash,
        "timestamp": _dt_ms(p.timestamp),
        "min_order_size": None if p.min_order_size is None else str(p.min_order_size),
        "tick_size": None if p.tick_size is None else str(p.tick_size),
        "neg_risk": p.neg_risk,
        "last_trade_price": None if p.last_trade_price is None else str(p.last_trade_price),
    }


def _price_wire(event: MarketPriceChangeEvent) -> dict[str, Any]:
    p = event.payload
    return {
        "event_type": "price_change",
        "market": p.market,
        "timestamp": _dt_ms(p.timestamp),
        "price_changes": [
            {
                "asset_id": str(change.token_id),
                "price": str(change.price),
                "size": str(change.size),
                "side": change.side,
                "hash": change.hash,
                "best_bid": None if change.best_bid is None else str(change.best_bid),
                "best_ask": None if change.best_ask is None else str(change.best_ask),
            }
            for change in p.price_changes
        ],
    }


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

    connected_once = False
    initial_subscribe_failures = 0
    outage_started = self.market.window_start
    disconnect_total_s = 0.0
    stream_client: AsyncPublicClient | None = None
    handle: Any = None

    with self.raw_path.open("w", encoding="utf-8") as raw_fh:
        # Initial subscribe can fail before the SDK owns a live socket. Retry
        # conservatively; once subscribed, the SDK itself manages reconnects
        # and resends the full subscription state.
        attempt = 0
        while time.time() < self.market.window_end + 1 and handle is None:
            attempt += 1
            try:
                stream_client = AsyncPublicClient()
                handle = await stream_client.subscribe(
                    MarketSpec(
                        token_ids=[
                            str(self.market.up_token_id),
                            str(self.market.down_token_id),
                        ],
                        custom_feature_enabled=False,
                    )
                )
                connected_once = True
                now = time.time()
                disconnect_total_s += max(0.0, now - outage_started)
                outage_started = 0.0
                print("STREAM     subscribed; SDK heartbeat/reconnect active")
            except TransportError as exc:
                initial_subscribe_failures += 1
                if stream_client is not None:
                    with suppress(Exception):
                        await stream_client.close()
                stream_client = None
                delay = min(8.0, 0.75 * (2 ** min(attempt - 1, 4)))
                remaining = max(0.0, self.market.window_end - time.time())
                delay = min(delay, remaining) if remaining else 0.0
                print(
                    f"STREAM ERR {type(exc).__name__}: {exc}; "
                    f"SDK subscribe retry in {delay:.2f}s (failure={attempt})"
                )
                if delay > 0:
                    await asyncio.sleep(delay)

        if handle is not None:
            iterator = handle.__aiter__()
            try:
                while time.time() < self.market.window_end + 1:
                    timeout = max(
                        0.05,
                        min(2.0, self.market.window_end + 1 - time.time()),
                    )
                    try:
                        event = await asyncio.wait_for(iterator.__anext__(), timeout=timeout)
                    except asyncio.TimeoutError:
                        continue
                    except StopAsyncIteration:
                        break

                    recv_ms = int(time.time() * 1000)
                    data: dict[str, Any] | None = None
                    if isinstance(event, MarketBookEvent):
                        data = _book_wire(event)
                    elif isinstance(event, MarketPriceChangeEvent):
                        data = _price_wire(event)
                    if data is None:
                        continue

                    self.raw_messages += 1
                    raw_fh.write(
                        json.dumps(
                            {"recv_ts_ms": recv_ms, "payload": data},
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                    exchange_ms = base._ms(data.get("timestamp"))
                    if data["event_type"] == "book":
                        self._handle_book(data, recv_ms, exchange_ms)
                    else:
                        self._handle_price_changes(data, recv_ms, exchange_ms)
            finally:
                with suppress(Exception):
                    await handle.close()
                if stream_client is not None:
                    with suppress(Exception):
                        await stream_client.close()

    if outage_started:
        disconnect_total_s += max(
            0.0, min(time.time(), self.market.window_end) - outage_started
        )

    first_age = self.points[0].age_s if self.points else None
    last_age = self.points[-1].age_s if self.points else None
    # The SDK may reconnect internally without exposing downtime directly, so
    # quality is judged primarily from edge coverage and tick presence. Initial
    # subscribe delay is still tracked explicitly.
    full_window = bool(
        self.points
        and first_age is not None
        and last_age is not None
        and first_age <= 3.0
        and last_age >= 297.0
        and initial_subscribe_failures == 0
    )

    manager_dropped = 0
    if stream_client is not None:
        try:
            mgr = stream_client._market_manager  # pyright: ignore[reportPrivateUsage]
            if mgr is not None:
                manager_dropped = int(mgr.dropped_events)
        except Exception:
            manager_dropped = 0

    quality_path = self.raw_path.parent.parent / "connection_quality.csv"
    exists = quality_path.exists()
    with quality_path.open("a", newline="", encoding="utf-8") as qfh:
        fields = [
            "session",
            "market",
            "transport",
            "connected_once",
            "initial_subscribe_failures",
            "initial_disconnect_s",
            "first_tick_age_s",
            "last_tick_age_s",
            "normalized_ticks",
            "sdk_dropped_events",
            "full_window_quality",
        ]
        writer = csv.DictWriter(qfh, fieldnames=fields)
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "session": self.session_no,
                "market": self.market.slug,
                "transport": "polymarket_sdk",
                "connected_once": int(connected_once),
                "initial_subscribe_failures": initial_subscribe_failures,
                "initial_disconnect_s": f"{disconnect_total_s:.3f}",
                "first_tick_age_s": "" if first_age is None else f"{first_age:.3f}",
                "last_tick_age_s": "" if last_age is None else f"{last_age:.3f}",
                "normalized_ticks": len(self.points),
                "sdk_dropped_events": manager_dropped,
                "full_window_quality": int(full_window),
            }
        )

    print(
        f"END RAW    messages={self.raw_messages} price_ticks={self.price_change_ticks} "
        f"book_events={self.book_events} normalized_ticks={len(self.points)}"
    )
    print(
        f"QUALITY    full_window={full_window} transport=SDK "
        f"initial_failures={initial_subscribe_failures} "
        f"first_age={first_age} last_age={last_age} dropped={manager_dropped}"
    )
    return self.points


base.SessionRecorder.run = _sdk_run  # type: ignore[method-assign]


if __name__ == "__main__":
    base.main()
