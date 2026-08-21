"""Robust transport wrapper for the read-only 10-session native tick recorder.

Keeps the exact strategy/room-map logic from
`polymarket_metamask_10session_tick_recorder.py`, but hardens the WebSocket
capture path for high-rate BTC 5m books:

- unlimited websocket receive queue (`max_queue=None`)
- longer opening handshake timeout
- exponential reconnect backoff instead of reconnect-hammering every 0.5s
- preserves all data already captured across reconnects
- writes `connection_quality.csv` so incomplete sessions are explicitly marked

Still READ ONLY: no wallet, signer, key, order, merge, or transaction.
"""
from __future__ import annotations

import asyncio
import csv
import json
import time
from pathlib import Path
from typing import Any

import websockets

from tools import polymarket_metamask_10session_tick_recorder as base


async def _robust_run(self: Any):
    print("\n" + "=" * 82)
    print(f"SESSION    {self.session_no}")
    print(f"MARKET     {self.market.slug}")
    print(
        f"WINDOW     {base._iso_ms(self.market.window_start * 1000)} -> "
        f"{base._iso_ms(self.market.window_end * 1000)}"
    )
    print(f"RAW FILE   {self.raw_path}")

    reconnects = 0
    failed_connects = 0
    disconnect_total_s = 0.0
    outage_started = self.market.window_start
    connected_once = False
    consecutive_failures = 0

    with self.raw_path.open("w", encoding="utf-8") as raw_fh:
        while time.time() < self.market.window_end + 1:
            try:
                async with websockets.connect(
                    base.MARKET_WS_URL,
                    ping_interval=None,
                    open_timeout=20,
                    close_timeout=3,
                    max_size=16 * 1024 * 1024,
                    max_queue=None,
                    compression=None,
                ) as ws:
                    now = time.time()
                    if outage_started is not None:
                        disconnect_total_s += max(0.0, now - outage_started)
                        outage_started = None
                    if connected_once:
                        reconnects += 1
                    connected_once = True
                    consecutive_failures = 0

                    await ws.send(
                        json.dumps(
                            {
                                "type": "market",
                                "assets_ids": [
                                    self.market.up_token_id,
                                    self.market.down_token_id,
                                ],
                            }
                        )
                    )
                    print(
                        "STREAM     connected; native ticks live "
                        f"(reconnects={reconnects})"
                    )
                    next_ping = time.monotonic() + 45

                    while time.time() < self.market.window_end + 1:
                        if time.monotonic() >= next_ping:
                            await ws.send("PING")
                            next_ping = time.monotonic() + 45

                        timeout = max(
                            0.05,
                            min(2.0, self.market.window_end + 1 - time.time()),
                        )
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
                        raw_fh.write(
                            json.dumps(
                                {"recv_ts_ms": recv_ms, "payload": parsed},
                                separators=(",", ":"),
                            )
                            + "\n"
                        )

                        events = parsed if isinstance(parsed, list) else [parsed]
                        for data in events:
                            if not isinstance(data, dict):
                                continue
                            event_type = str(
                                data.get("event_type") or data.get("type") or ""
                            )
                            exchange_ms = base._ms(data.get("timestamp"))
                            if event_type == "book":
                                self._handle_book(data, recv_ms, exchange_ms)
                            elif event_type == "price_change":
                                self._handle_price_changes(data, recv_ms, exchange_ms)

                break

            except Exception as exc:  # noqa: BLE001
                if time.time() >= self.market.window_end:
                    break
                if outage_started is None:
                    outage_started = time.time()
                consecutive_failures += 1
                if not connected_once:
                    failed_connects += 1
                # A rapid retry loop can make upstream throttling worse. Back off.
                delay = min(8.0, 0.75 * (2 ** min(consecutive_failures - 1, 4)))
                remaining = max(0.0, self.market.window_end - time.time())
                delay = min(delay, remaining) if remaining else 0.0
                print(
                    f"STREAM ERR {type(exc).__name__}: {exc}; "
                    f"retry in {delay:.2f}s (failure={consecutive_failures})"
                )
                if delay > 0:
                    await asyncio.sleep(delay)

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
    )

    quality_path = self.raw_path.parent.parent / "connection_quality.csv"
    exists = quality_path.exists()
    with quality_path.open("a", newline="", encoding="utf-8") as qfh:
        fields = [
            "session",
            "market",
            "connected_once",
            "failed_connects",
            "reconnects",
            "disconnect_total_s",
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
                "connected_once": int(connected_once),
                "failed_connects": failed_connects,
                "reconnects": reconnects,
                "disconnect_total_s": f"{disconnect_total_s:.3f}",
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
        f"QUALITY    full_window={full_window} reconnects={reconnects} "
        f"failed_connects={failed_connects} disconnect≈{disconnect_total_s:.2f}s "
        f"first_age={first_age} last_age={last_age}"
    )
    return self.points


# Patch only the transport/capture loop. All strategy calculations, raw-vs-MM250
# transforms, room maps, summaries, CLI flags, and output formats remain base code.
base.SessionRecorder.run = _robust_run  # type: ignore[method-assign]


if __name__ == "__main__":
    base.main()
