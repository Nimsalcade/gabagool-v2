"""Record one full MetaMask Predictions BTC 5-minute UP/DOWN session.

This intentionally uses the same market WebSocket that MetaMask Mobile's open-source
Predict WebSocketManager uses:
    wss://ws-subscriptions-clob.polymarket.com/ws/market

and the same subscription payload:
    {"type": "market", "assets_ids": [UP_TOKEN_ID, DOWN_TOKEN_ID]}

The script does not place orders. It captures the live UP/DOWN best bid/ask stream,
raw WebSocket events, top-of-book depth, simultaneous complete-set cost, and the best
chronologically valid sequential UP->DOWN / DOWN->UP acquisition opportunities.

Usage:
    python -m tools.metamask_btc5m_recorder
    python -m tools.metamask_btc5m_recorder --next
    python -m tools.metamask_btc5m_recorder --output data/metamask_capture

`--next` waits for the next BTC 5m window and records it from the opening seconds.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DURATION_S = 300
ASSET = "btc"


@dataclass
class Top:
    bid: float | None = None
    bid_size: float | None = None
    ask: float | None = None
    ask_size: float | None = None
    ts_ms: int | None = None


@dataclass
class CheapAsk:
    price: float = math.inf
    size: float = 0.0
    ts_ms: int = 0
    age_s: float = 0.0


@dataclass
class SequentialBest:
    combined: float = math.inf
    first_side: str = ""
    first_price: float = math.inf
    first_size: float = 0.0
    first_ts_ms: int = 0
    first_age_s: float = 0.0
    second_side: str = ""
    second_price: float = math.inf
    second_size: float = 0.0
    second_ts_ms: int = 0
    second_age_s: float = 0.0

    @property
    def gross_edge(self) -> float:
        return 1.0 - self.combined

    @property
    def executable_pairs_at_top(self) -> float:
        return min(self.first_size, self.second_size)

    @property
    def gross_edge_at_top(self) -> float:
        return self.executable_pairs_at_top * self.gross_edge


CSV_FIELDS = [
    "recv_ts_ms",
    "recv_iso_utc",
    "exchange_ts_ms",
    "market_age_s",
    "seconds_to_end",
    "event_type",
    "changed_token",
    "up_bid",
    "up_bid_size",
    "up_ask",
    "up_ask_size",
    "down_bid",
    "down_bid_size",
    "down_ask",
    "down_ask_size",
    "instant_pair_ask",
    "instant_pair_gross_edge",
    "maker_pair_bid",
    "maker_pair_gross_edge",
]


def _f(value: Any) -> float | None:
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _best_from_book(levels: list[dict[str, Any]] | None, *, is_bid: bool):
    parsed: list[tuple[float, float]] = []
    for level in levels or []:
        price = _f(level.get("price"))
        size = _f(level.get("size"))
        if price is None or size is None or size <= 0:
            continue
        parsed.append((price, size))
    if not parsed:
        return None, None
    return (max(parsed) if is_bid else min(parsed), key=lambda x: x[0])


def _iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


class Recorder:
    def __init__(self, market, output_dir: Path):
        self.market = market
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = output_dir / f"{market.slug}.csv"
        self.raw_path = output_dir / f"{market.slug}.raw.jsonl"
        self.summary_path = output_dir / f"{market.slug}.summary.json"

        self.up = Top()
        self.down = Top()
        self.token_side = {
            market.up_token_id: "UP",
            market.down_token_id: "DOWN",
        }
        self.cheapest = {"UP": CheapAsk(), "DOWN": CheapAsk()}
        self.best_up_down = SequentialBest()
        self.best_down_up = SequentialBest()
        self.min_instant = math.inf
        self.min_instant_ts_ms = 0
        self.rows = 0
        self.raw_events = 0
        self.instant_below_1 = 0
        self.instant_below_099 = 0
        self.sequential_below_1 = 0
        self.stop = asyncio.Event()

    def request_stop(self):
        self.stop.set()

    def _side_top(self, side: str) -> Top:
        return self.up if side == "UP" else self.down

    def _update_book(self, data: dict[str, Any], recv_ms: int):
        token = str(data.get("asset_id") or "")
        side = self.token_side.get(token)
        if not side:
            return None
        top = self._side_top(side)
        bid = _best_from_book(data.get("bids"), is_bid=True)
        ask = _best_from_book(data.get("asks"), is_bid=False)
        top.bid, top.bid_size = bid
        top.ask, top.ask_size = ask
        top.ts_ms = recv_ms
        return side

    def _update_price_changes(self, data: dict[str, Any], recv_ms: int):
        changed: list[str] = []
        for change in data.get("price_changes") or []:
            token = str(change.get("asset_id") or "")
            side = self.token_side.get(token)
            if not side:
                continue
            top = self._side_top(side)
            bid = _f(change.get("best_bid"))
            ask = _f(change.get("best_ask"))
            if bid is not None:
                top.bid = bid
            if ask is not None:
                top.ask = ask
            top.ts_ms = recv_ms
            changed.append(side)
        return changed

    def _observe_sequential(self, recv_ms: int):
        age = recv_ms / 1000 - self.market.window_start
        for side, top in (("UP", self.up), ("DOWN", self.down)):
            if top.ask is None or top.ask_size is None or top.ask_size <= 0:
                continue

            opposite = "DOWN" if side == "UP" else "UP"
            prior = self.cheapest[opposite]
            if prior.price < math.inf and prior.ts_ms <= recv_ms:
                combined = prior.price + top.ask
                target = self.best_up_down if opposite == "UP" else self.best_down_up
                if combined < target.combined:
                    target.combined = combined
                    target.first_side = opposite
                    target.first_price = prior.price
                    target.first_size = prior.size
                    target.first_ts_ms = prior.ts_ms
                    target.first_age_s = prior.age_s
                    target.second_side = side
                    target.second_price = top.ask
                    target.second_size = top.ask_size
                    target.second_ts_ms = recv_ms
                    target.second_age_s = age
                if combined < 1.0:
                    self.sequential_below_1 += 1

            cheapest = self.cheapest[side]
            if top.ask < cheapest.price:
                cheapest.price = top.ask
                cheapest.size = top.ask_size
                cheapest.ts_ms = recv_ms
                cheapest.age_s = age

    def _snapshot_row(self, *, recv_ms: int, exchange_ms: int | None, event_type: str, changed: str):
        instant = None
        instant_edge = None
        maker_pair = None
        maker_edge = None
        if self.up.ask is not None and self.down.ask is not None:
            instant = self.up.ask + self.down.ask
            instant_edge = 1.0 - instant
            if instant < self.min_instant:
                self.min_instant = instant
                self.min_instant_ts_ms = recv_ms
            if instant < 1.0:
                self.instant_below_1 += 1
            if instant < 0.99:
                self.instant_below_099 += 1
        if self.up.bid is not None and self.down.bid is not None:
            maker_pair = self.up.bid + self.down.bid
            maker_edge = 1.0 - maker_pair

        return {
            "recv_ts_ms": recv_ms,
            "recv_iso_utc": _iso_ms(recv_ms),
            "exchange_ts_ms": exchange_ms or "",
            "market_age_s": round(recv_ms / 1000 - self.market.window_start, 6),
            "seconds_to_end": round(self.market.window_end - recv_ms / 1000, 6),
            "event_type": event_type,
            "changed_token": changed,
            "up_bid": self.up.bid if self.up.bid is not None else "",
            "up_bid_size": self.up.bid_size if self.up.bid_size is not None else "",
            "up_ask": self.up.ask if self.up.ask is not None else "",
            "up_ask_size": self.up.ask_size if self.up.ask_size is not None else "",
            "down_bid": self.down.bid if self.down.bid is not None else "",
            "down_bid_size": self.down.bid_size if self.down.bid_size is not None else "",
            "down_ask": self.down.ask if self.down.ask is not None else "",
            "down_ask_size": self.down.ask_size if self.down.ask_size is not None else "",
            "instant_pair_ask": instant if instant is not None else "",
            "instant_pair_gross_edge": instant_edge if instant_edge is not None else "",
            "maker_pair_bid": maker_pair if maker_pair is not None else "",
            "maker_pair_gross_edge": maker_edge if maker_edge is not None else "",
        }

    async def run(self):
        print(f"MARKET   {self.market.slug}")
        print(f"WINDOW   {_iso_ms(self.market.window_start * 1000)} -> {_iso_ms(self.market.window_end * 1000)}")
        print(f"UP       {self.market.up_token_id}")
        print(f"DOWN     {self.market.down_token_id}")
        print(f"WS       {MARKET_WS_URL}")
        print(f"CSV      {self.csv_path}")
        print(f"RAW      {self.raw_path}")

        with self.csv_path.open("w", newline="", encoding="utf-8") as csv_file, self.raw_path.open(
            "w", encoding="utf-8"
        ) as raw_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
            writer.writeheader()

            async with websockets.connect(
                MARKET_WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_size=8 * 1024 * 1024,
            ) as ws:
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

                async def pinger():
                    while not self.stop.is_set():
                        await asyncio.sleep(50)
                        try:
                            await ws.send("PING")
                        except Exception:
                            return

                ping_task = asyncio.create_task(pinger())
                try:
                    while not self.stop.is_set():
                        now = time.time()
                        if now >= self.market.window_end + 2:
                            break
                        timeout = max(0.1, min(5.0, self.market.window_end + 2 - now))
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

                        events = parsed if isinstance(parsed, list) else [parsed]
                        for data in events:
                            if not isinstance(data, dict):
                                continue
                            raw_file.write(json.dumps({"recv_ts_ms": recv_ms, "payload": data}, separators=(",", ":")) + "\n")
                            raw_file.flush()
                            self.raw_events += 1

                            event_type = str(data.get("event_type") or "")
                            changed: list[str] = []
                            if event_type == "book":
                                side = self._update_book(data, recv_ms)
                                if side:
                                    changed.append(side)
                            elif event_type == "price_change":
                                changed.extend(self._update_price_changes(data, recv_ms))
                            else:
                                continue

                            self._observe_sequential(recv_ms)
                            exchange_ms = None
                            try:
                                exchange_ms = int(data.get("timestamp")) if data.get("timestamp") else None
                            except (TypeError, ValueError):
                                pass
                            row = self._snapshot_row(
                                recv_ms=recv_ms,
                                exchange_ms=exchange_ms,
                                event_type=event_type,
                                changed="+".join(sorted(set(changed))),
                            )
                            writer.writerow(row)
                            csv_file.flush()
                            self.rows += 1

                            instant = row["instant_pair_ask"]
                            if instant != "" and float(instant) < 1.0:
                                print(
                                    f"age={row['market_age_s']:>8}  "
                                    f"UP {self.up.bid}/{self.up.ask}  "
                                    f"DOWN {self.down.bid}/{self.down.ask}  "
                                    f"INSTANT={float(instant):.4f}  "
                                    f"EDGE={1-float(instant):+.4f}"
                                )
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

        summary = self.build_summary()
        self.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("\n=== SESSION RESULT ===")
        print(json.dumps(summary, indent=2))
        return summary

    def _seq_dict(self, x: SequentialBest):
        if not math.isfinite(x.combined):
            return None
        return {
            "combined_cost": x.combined,
            "gross_edge_per_pair": x.gross_edge,
            "first_side": x.first_side,
            "first_price": x.first_price,
            "first_size_at_top": x.first_size,
            "first_age_s": x.first_age_s,
            "second_side": x.second_side,
            "second_price": x.second_price,
            "second_size_at_top": x.second_size,
            "second_age_s": x.second_age_s,
            "top_of_book_pair_capacity": x.executable_pairs_at_top,
            "gross_edge_at_top_capacity": x.gross_edge_at_top,
        }

    def build_summary(self):
        return {
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "window_start": self.market.window_start,
            "window_end": self.market.window_end,
            "up_token_id": self.market.up_token_id,
            "down_token_id": self.market.down_token_id,
            "source": MARKET_WS_URL,
            "rows": self.rows,
            "raw_events": self.raw_events,
            "minimum_simultaneous_pair_ask": None if not math.isfinite(self.min_instant) else self.min_instant,
            "minimum_simultaneous_pair_gross_edge": None if not math.isfinite(self.min_instant) else 1.0 - self.min_instant,
            "instant_observations_below_1": self.instant_below_1,
            "instant_observations_below_0_99": self.instant_below_099,
            "sequential_observations_below_1": self.sequential_below_1,
            "best_up_then_down": self._seq_dict(self.best_up_down),
            "best_down_then_up": self._seq_dict(self.best_down_up),
            "interpretation": (
                "Gross complete-set arithmetic only. A sequential opportunity proves that an UP ask and a later DOWN ask "
                "(or vice versa) were displayed at a combined cost below $1. It does not prove a hypothetical order would "
                "have filled at the displayed size; queue priority, latency, fees and slippage must be modeled separately."
            ),
        }


async def _resolve_target(client, use_next: bool):
    current = window_start_epoch(DURATION_S)
    target_start = current + DURATION_S if use_next else current

    if use_next:
        print(f"Waiting for next BTC 5m market: start={target_start} ({_iso_ms(target_start*1000)})")
        while True:
            market = await resolve_market(client, ASSET, DURATION_S, target_start)
            if market is not None:
                # Connect just before opening if the market is already published.
                wait = target_start - time.time() - 1.0
                if wait > 0:
                    await asyncio.sleep(wait)
                return market
            await asyncio.sleep(1.0)

    market = await resolve_market(client, ASSET, DURATION_S, target_start)
    if market is None:
        raise RuntimeError(f"could not resolve current BTC 5m market starting {target_start}")
    return market


async def amain(args):
    client = AsyncPublicClient()
    try:
        market = await _resolve_target(client, args.next)
        recorder = Recorder(market, Path(args.output))

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, recorder.request_stop)
            except NotImplementedError:
                pass

        await recorder.run()
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(description="Record MetaMask/Polymarket BTC 5m UP/DOWN ticks")
    parser.add_argument(
        "--next",
        action="store_true",
        help="wait for the next complete BTC 5m session rather than joining the current one",
    )
    parser.add_argument(
        "--output",
        default="data/metamask_btc5m",
        help="directory for CSV/raw JSONL/summary output",
    )
    args = parser.parse_args()
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
