"""Read-only dual BTC/ETH 15-minute market recorder for Gabagool forensics.

Purpose
-------
Capture one *complete, aligned* BTC + ETH 15-minute Up/Down session from the
public Polymarket market WebSocket. This is an evidence recorder, not a trading
simulator. It records enough information to replay the market later without
counting repeated snapshots as independent opportunities.

Outputs per run
---------------
- raw_market.jsonl       every raw public WebSocket payload
- depth_state.jsonl      normalized full depth after each book/depth change
- top_of_book.csv        synchronized UP/DOWN top-of-book state per asset
- trades.csv             normalized last_trade_price events
- summary.json           capture diagnostics only

The raw/depth files are the primary evidence. Snapshot counts below $1 in the
summary are diagnostics and MUST NOT be interpreted as independent buy cycles.
Liquidity can persist across many snapshots. Independent acquisition counts
should be computed later by a replay that tracks order/fill state and consumes
liquidity/trade evidence once.

Default behavior waits for the next complete aligned 15-minute BTC + ETH window.
Use --current only when intentionally joining an already-running window.

Usage:
    python -m tools.gabagool_dual_15m_recorder
    python -m tools.gabagool_dual_15m_recorder --current
    python -m tools.gabagool_dual_15m_recorder --output data/gabagool_dual_15m
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ASSETS = ("btc", "eth")
DURATION_S = 900
PRECONNECT_S = 2.0
POST_END_S = 2.0

TOP_FIELDS = [
    "recv_ts_ms",
    "recv_iso_utc",
    "exchange_ts_ms",
    "asset",
    "market",
    "market_age_s",
    "seconds_to_end",
    "event_type",
    "changed_outcomes",
    "up_bid",
    "up_bid_size",
    "up_ask",
    "up_ask_size",
    "down_bid",
    "down_bid_size",
    "down_ask",
    "down_ask_size",
    "instant_pair_ask",
    "instant_pair_ask_capacity",
    "instant_pair_gross_edge",
    "maker_pair_bid",
    "maker_pair_bid_capacity",
    "maker_pair_gross_edge",
]

TRADE_FIELDS = [
    "recv_ts_ms",
    "recv_iso_utc",
    "exchange_ts_ms",
    "asset",
    "market",
    "outcome",
    "market_age_s",
    "price",
    "size",
    "side_flag",
    "fee_rate_bps",
    "transaction_hash",
]


def _f(value: Any) -> float | None:
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _event_type(data: dict[str, Any]) -> str:
    return str(data.get("event_type") or data.get("type") or "")


def _payload(data: dict[str, Any]) -> dict[str, Any]:
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else data


def _pick(data: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return None


def _token_id(data: dict[str, Any]) -> str:
    return str(_pick(data, "asset_id", "tokenId", "token_id") or "")


def _exchange_ts_ms(data: dict[str, Any]) -> str:
    value = _pick(data, "timestamp", "exchange_ts_ms")
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        x = float(value)
        if x < 10_000_000_000:
            x *= 1000
        return str(int(x))
    text = str(value)
    if text.isdigit():
        x = int(text)
        if x < 10_000_000_000:
            x *= 1000
        return str(x)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return str(int(dt.timestamp() * 1000))
    except ValueError:
        return text


def _sorted_levels(levels: dict[float, float], *, bids: bool) -> list[dict[str, float]]:
    return [
        {"price": price, "size": size}
        for price, size in sorted(levels.items(), key=lambda item: item[0], reverse=bids)
        if size > 0
    ]


@dataclass
class BookState:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    have_snapshot: bool = False
    last_recv_ms: int | None = None

    def replace(
        self,
        bids: list[dict[str, Any]] | None,
        asks: list[dict[str, Any]] | None,
        recv_ms: int,
    ) -> None:
        new_bids: dict[float, float] = {}
        new_asks: dict[float, float] = {}
        for raw in bids or []:
            price = _f(raw.get("price"))
            size = _f(raw.get("size"))
            if price is not None and size is not None and size > 0:
                new_bids[price] = size
        for raw in asks or []:
            price = _f(raw.get("price"))
            size = _f(raw.get("size"))
            if price is not None and size is not None and size > 0:
                new_asks[price] = size
        self.bids = new_bids
        self.asks = new_asks
        self.have_snapshot = True
        self.last_recv_ms = recv_ms

    def update_level(self, *, side_flag: str, price: float, size: float, recv_ms: int) -> None:
        levels = self.bids if side_flag == "BUY" else self.asks
        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.last_recv_ms = recv_ms

    def best_bid(self) -> tuple[float | None, float | None]:
        if not self.bids:
            return None, None
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self) -> tuple[float | None, float | None]:
        if not self.asks:
            return None, None
        price = min(self.asks)
        return price, self.asks[price]


@dataclass
class MarketStats:
    top_rows: int = 0
    book_events: int = 0
    price_change_events: int = 0
    trade_events: int = 0
    first_event_age_s: float | None = None
    last_event_age_s: float | None = None
    minimum_instant_pair_ask: float = math.inf
    minimum_instant_pair_age_s: float | None = None
    snapshots_below_1: int = 0
    snapshots_below_0995: int = 0
    snapshots_below_099: int = 0

    def observe_age(self, age_s: float) -> None:
        if self.first_event_age_s is None:
            self.first_event_age_s = age_s
        self.last_event_age_s = age_s


class DualRecorder:
    def __init__(self, markets: dict[str, Any], run_dir: Path) -> None:
        self.markets = markets
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.raw_path = run_dir / "raw_market.jsonl"
        self.depth_path = run_dir / "depth_state.jsonl"
        self.top_path = run_dir / "top_of_book.csv"
        self.trades_path = run_dir / "trades.csv"
        self.summary_path = run_dir / "summary.json"

        self.books: dict[tuple[str, str], BookState] = {
            (asset, outcome): BookState()
            for asset in ASSETS
            for outcome in ("UP", "DOWN")
        }
        self.token_map: dict[str, tuple[str, str]] = {}
        for asset, market in markets.items():
            self.token_map[str(market.up_token_id)] = (asset, "UP")
            self.token_map[str(market.down_token_id)] = (asset, "DOWN")

        self.stats = {asset: MarketStats() for asset in ASSETS}
        self.raw_events = 0
        self.depth_records = 0
        self.stop = asyncio.Event()

    def request_stop(self) -> None:
        self.stop.set()

    def _book(self, asset: str, outcome: str) -> BookState:
        return self.books[(asset, outcome)]

    def _full_depth_record(
        self,
        *,
        recv_ms: int,
        exchange_ms: str,
        event_type: str,
        asset: str,
        outcome: str,
    ) -> dict[str, Any]:
        market = self.markets[asset]
        book = self._book(asset, outcome)
        return {
            "recv_ts_ms": recv_ms,
            "recv_iso_utc": _iso_ms(recv_ms),
            "exchange_ts_ms": exchange_ms,
            "asset": asset,
            "market": market.slug,
            "outcome": outcome,
            "token_id": market.up_token_id if outcome == "UP" else market.down_token_id,
            "market_age_s": recv_ms / 1000 - market.window_start,
            "event_type": event_type,
            "have_initial_book": book.have_snapshot,
            "bids": _sorted_levels(book.bids, bids=True),
            "asks": _sorted_levels(book.asks, bids=False),
        }

    def _top_row(
        self,
        *,
        recv_ms: int,
        exchange_ms: str,
        event_type: str,
        asset: str,
        changed_outcomes: set[str],
    ) -> dict[str, Any]:
        market = self.markets[asset]
        up = self._book(asset, "UP")
        down = self._book(asset, "DOWN")
        up_bid, up_bid_size = up.best_bid()
        up_ask, up_ask_size = up.best_ask()
        down_bid, down_bid_size = down.best_bid()
        down_ask, down_ask_size = down.best_ask()

        instant = None
        instant_cap = None
        instant_edge = None
        maker = None
        maker_cap = None
        maker_edge = None

        if up_ask is not None and down_ask is not None:
            instant = up_ask + down_ask
            instant_edge = 1.0 - instant
            if up_ask_size is not None and down_ask_size is not None:
                instant_cap = min(up_ask_size, down_ask_size)

        if up_bid is not None and down_bid is not None:
            maker = up_bid + down_bid
            maker_edge = 1.0 - maker
            if up_bid_size is not None and down_bid_size is not None:
                maker_cap = min(up_bid_size, down_bid_size)

        return {
            "recv_ts_ms": recv_ms,
            "recv_iso_utc": _iso_ms(recv_ms),
            "exchange_ts_ms": exchange_ms,
            "asset": asset,
            "market": market.slug,
            "market_age_s": round(recv_ms / 1000 - market.window_start, 6),
            "seconds_to_end": round(market.window_end - recv_ms / 1000, 6),
            "event_type": event_type,
            "changed_outcomes": "+".join(sorted(changed_outcomes)),
            "up_bid": "" if up_bid is None else up_bid,
            "up_bid_size": "" if up_bid_size is None else up_bid_size,
            "up_ask": "" if up_ask is None else up_ask,
            "up_ask_size": "" if up_ask_size is None else up_ask_size,
            "down_bid": "" if down_bid is None else down_bid,
            "down_bid_size": "" if down_bid_size is None else down_bid_size,
            "down_ask": "" if down_ask is None else down_ask,
            "down_ask_size": "" if down_ask_size is None else down_ask_size,
            "instant_pair_ask": "" if instant is None else instant,
            "instant_pair_ask_capacity": "" if instant_cap is None else instant_cap,
            "instant_pair_gross_edge": "" if instant_edge is None else instant_edge,
            "maker_pair_bid": "" if maker is None else maker,
            "maker_pair_bid_capacity": "" if maker_cap is None else maker_cap,
            "maker_pair_gross_edge": "" if maker_edge is None else maker_edge,
        }

    def _observe_top_stats(self, asset: str, row: dict[str, Any]) -> None:
        stats = self.stats[asset]
        stats.top_rows += 1
        age_s = float(row["market_age_s"])
        stats.observe_age(age_s)
        value = row["instant_pair_ask"]
        if value == "":
            return
        pair = float(value)
        if pair < stats.minimum_instant_pair_ask:
            stats.minimum_instant_pair_ask = pair
            stats.minimum_instant_pair_age_s = age_s
        if pair < 1.0:
            stats.snapshots_below_1 += 1
        if pair <= 0.995:
            stats.snapshots_below_0995 += 1
        if pair <= 0.99:
            stats.snapshots_below_099 += 1

    def _apply_book(self, payload: dict[str, Any], recv_ms: int) -> tuple[str, str] | None:
        token = _token_id(payload)
        mapped = self.token_map.get(token)
        if mapped is None:
            return None
        asset, outcome = mapped
        self._book(asset, outcome).replace(payload.get("bids"), payload.get("asks"), recv_ms)
        self.stats[asset].book_events += 1
        return asset, outcome

    def _price_changes(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        raw = _pick(payload, "price_changes", "priceChanges")
        return [x for x in (raw or []) if isinstance(x, dict)]

    def _apply_price_changes(self, payload: dict[str, Any], recv_ms: int) -> dict[str, set[str]]:
        changed: dict[str, set[str]] = {}
        touched_assets: set[str] = set()
        for change in self._price_changes(payload):
            token = _token_id(change)
            mapped = self.token_map.get(token)
            if mapped is None:
                continue
            asset, outcome = mapped
            price = _f(change.get("price"))
            size = _f(change.get("size"))
            side_flag = str(change.get("side") or "").upper()
            if price is None or size is None or side_flag not in {"BUY", "SELL"}:
                continue
            self._book(asset, outcome).update_level(
                side_flag=side_flag,
                price=price,
                size=size,
                recv_ms=recv_ms,
            )
            changed.setdefault(asset, set()).add(outcome)
            touched_assets.add(asset)
        for asset in touched_assets:
            self.stats[asset].price_change_events += 1
        return changed

    def _trade_row(self, payload: dict[str, Any], recv_ms: int) -> dict[str, Any] | None:
        token = _token_id(payload)
        mapped = self.token_map.get(token)
        if mapped is None:
            return None
        asset, outcome = mapped
        market = self.markets[asset]
        price = _f(payload.get("price"))
        size = _f(payload.get("size"))
        if price is None:
            return None
        self.stats[asset].trade_events += 1
        age_s = recv_ms / 1000 - market.window_start
        self.stats[asset].observe_age(age_s)
        return {
            "recv_ts_ms": recv_ms,
            "recv_iso_utc": _iso_ms(recv_ms),
            "exchange_ts_ms": _exchange_ts_ms(payload),
            "asset": asset,
            "market": market.slug,
            "outcome": outcome,
            "market_age_s": round(age_s, 6),
            "price": price,
            "size": "" if size is None else size,
            "side_flag": str(payload.get("side") or "").upper(),
            "fee_rate_bps": _pick(payload, "fee_rate_bps", "feeRateBps") or "",
            "transaction_hash": _pick(payload, "transaction_hash", "transactionHash") or "",
        }

    async def run(self) -> dict[str, Any]:
        starts = {m.window_start for m in self.markets.values()}
        ends = {m.window_end for m in self.markets.values()}
        if len(starts) != 1 or len(ends) != 1:
            raise RuntimeError("BTC and ETH markets are not aligned to the same window")
        window_start = next(iter(starts))
        window_end = next(iter(ends))

        print("READ ONLY   public market data only; no wallet, key, orders, merge, or redeem")
        print("PURPOSE     record one aligned BTC + ETH 15m session for later no-double-count replay")
        print(f"WINDOW      {_iso_ms(window_start * 1000)} -> {_iso_ms(window_end * 1000)}")
        for asset in ASSETS:
            market = self.markets[asset]
            print(f"{asset.upper():<10} {market.slug}")
            print(f"           UP={market.up_token_id}")
            print(f"           DOWN={market.down_token_id}")
        print(f"WS          {MARKET_WS_URL}")
        print(f"OUTPUT      {self.run_dir}")
        print("NOTE        below-$1 snapshot counts are NOT independent buy counts")

        token_ids = list(self.token_map)

        with (
            self.raw_path.open("w", encoding="utf-8") as raw_fh,
            self.depth_path.open("w", encoding="utf-8") as depth_fh,
            self.top_path.open("w", newline="", encoding="utf-8") as top_fh,
            self.trades_path.open("w", newline="", encoding="utf-8") as trades_fh,
        ):
            top_writer = csv.DictWriter(top_fh, fieldnames=TOP_FIELDS)
            trade_writer = csv.DictWriter(trades_fh, fieldnames=TRADE_FIELDS)
            top_writer.writeheader()
            trade_writer.writeheader()

            async with websockets.connect(
                MARKET_WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_size=16 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))

                async def pinger() -> None:
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
                        if now >= window_end + POST_END_S:
                            break
                        timeout = max(0.1, min(5.0, window_end + POST_END_S - now))
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
                            raw_fh.write(
                                json.dumps({"recv_ts_ms": recv_ms, "payload": data}, separators=(",", ":"))
                                + "\n"
                            )
                            raw_fh.flush()
                            self.raw_events += 1

                            event_type = _event_type(data)
                            payload = _payload(data)
                            exchange_ms = _exchange_ts_ms(payload)
                            changed_by_asset: dict[str, set[str]] = {}

                            if event_type == "book":
                                mapped = self._apply_book(payload, recv_ms)
                                if mapped is not None:
                                    asset, outcome = mapped
                                    changed_by_asset.setdefault(asset, set()).add(outcome)
                                    depth_fh.write(
                                        json.dumps(
                                            self._full_depth_record(
                                                recv_ms=recv_ms,
                                                exchange_ms=exchange_ms,
                                                event_type=event_type,
                                                asset=asset,
                                                outcome=outcome,
                                            ),
                                            separators=(",", ":"),
                                        )
                                        + "\n"
                                    )
                                    depth_fh.flush()
                                    self.depth_records += 1

                            elif event_type == "price_change":
                                changed_by_asset = self._apply_price_changes(payload, recv_ms)
                                for asset, outcomes in changed_by_asset.items():
                                    for outcome in outcomes:
                                        depth_fh.write(
                                            json.dumps(
                                                self._full_depth_record(
                                                    recv_ms=recv_ms,
                                                    exchange_ms=exchange_ms,
                                                    event_type=event_type,
                                                    asset=asset,
                                                    outcome=outcome,
                                                ),
                                                separators=(",", ":"),
                                            )
                                            + "\n"
                                        )
                                        self.depth_records += 1
                                if changed_by_asset:
                                    depth_fh.flush()

                            elif event_type == "last_trade_price":
                                trade_row = self._trade_row(payload, recv_ms)
                                if trade_row is not None:
                                    trade_writer.writerow(trade_row)
                                    trades_fh.flush()
                                    asset = str(trade_row["asset"])
                                    outcome = str(trade_row["outcome"])
                                    changed_by_asset.setdefault(asset, set()).add(outcome)

                            else:
                                continue

                            for asset, changed_outcomes in changed_by_asset.items():
                                row = self._top_row(
                                    recv_ms=recv_ms,
                                    exchange_ms=exchange_ms,
                                    event_type=event_type,
                                    asset=asset,
                                    changed_outcomes=changed_outcomes,
                                )
                                top_writer.writerow(row)
                                top_fh.flush()
                                self._observe_top_stats(asset, row)
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

        summary = self.build_summary()
        self.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("\n=== DUAL 15M CAPTURE RESULT ===")
        for asset in ASSETS:
            item = summary["markets"][asset]
            print(
                f"{asset.upper():<4} rows={item['top_rows']} trades={item['trade_events']} "
                f"minInstant={item['minimum_instant_pair_ask']} "
                f"snap<=.99={item['snapshots_at_or_below_0_99']}"
            )
        print("FILES       raw_market.jsonl | depth_state.jsonl | top_of_book.csv | trades.csv | summary.json")
        print(
            "NEXT        upload the whole run folder/zip; replay will count independent "
            "10-share acquisitions without reusing liquidity"
        )
        return summary

    def build_summary(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "source": MARKET_WS_URL,
            "duration_s": DURATION_S,
            "assets": list(ASSETS),
            "raw_events": self.raw_events,
            "depth_records": self.depth_records,
            "markets": {},
            "files": {
                "raw_market_jsonl": str(self.raw_path),
                "depth_state_jsonl": str(self.depth_path),
                "top_of_book_csv": str(self.top_path),
                "trades_csv": str(self.trades_path),
            },
            "interpretation": (
                "Recorder only. Snapshot counts below a pair-cost threshold are repeated observations, "
                "not independent buy opportunities. Use the raw/depth/trade stream for a chronological "
                "replay that tracks 10-share clip inventory and does not reuse the same liquidity."
            ),
        }
        for asset in ASSETS:
            market = self.markets[asset]
            stats = self.stats[asset]
            result["markets"][asset] = {
                "market": market.slug,
                "condition_id": market.condition_id,
                "window_start": market.window_start,
                "window_end": market.window_end,
                "up_token_id": market.up_token_id,
                "down_token_id": market.down_token_id,
                "top_rows": stats.top_rows,
                "book_events": stats.book_events,
                "price_change_events": stats.price_change_events,
                "trade_events": stats.trade_events,
                "first_event_age_s": stats.first_event_age_s,
                "last_event_age_s": stats.last_event_age_s,
                "minimum_instant_pair_ask": (
                    None
                    if not math.isfinite(stats.minimum_instant_pair_ask)
                    else stats.minimum_instant_pair_ask
                ),
                "minimum_instant_pair_age_s": stats.minimum_instant_pair_age_s,
                "snapshots_below_1": stats.snapshots_below_1,
                "snapshots_at_or_below_0_995": stats.snapshots_below_0995,
                "snapshots_at_or_below_0_99": stats.snapshots_below_099,
            }
        return result


async def _resolve_pair(client: AsyncPublicClient, target_start: int) -> dict[str, Any]:
    while True:
        found: dict[str, Any] = {}
        for asset in ASSETS:
            market = await resolve_market(client, asset, DURATION_S, target_start)
            if market is not None:
                found[asset] = market
        if len(found) == len(ASSETS):
            return found
        await asyncio.sleep(1.0)


async def amain(args: argparse.Namespace) -> int:
    now = time.time()
    current_start = window_start_epoch(DURATION_S, now)
    target_start = current_start if args.current else current_start + DURATION_S

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"

    client = AsyncPublicClient()
    try:
        if not args.current:
            preconnect_at = target_start - PRECONNECT_S
            wait = preconnect_at - time.time()
            if wait > 0:
                print(
                    f"WAIT        next aligned BTC+ETH 15m window starts "
                    f"{_iso_ms(target_start * 1000)}; preconnect in {wait:.1f}s"
                )
                await asyncio.sleep(wait)
        markets = await _resolve_pair(client, target_start)

        if not args.current:
            wait = target_start - time.time()
            if wait > 0.25:
                await asyncio.sleep(max(0.0, wait - 0.25))

        recorder = DualRecorder(markets, run_dir)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, recorder.request_stop)
            except NotImplementedError:
                pass
        await recorder.run()
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Record one aligned BTC + ETH 15m Polymarket Up/Down session "
            "with full depth/trades"
        )
    )
    parser.add_argument(
        "--current",
        action="store_true",
        help="join the current aligned 15m window instead of waiting for the next complete one",
    )
    parser.add_argument(
        "--output",
        default="data/gabagool_dual_15m",
        help="base directory for run_<timestamp> output",
    )
    args = parser.parse_args()
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
