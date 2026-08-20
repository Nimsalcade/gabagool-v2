"""Resilient zero-money live-market shadow runner.

Primary source: official Polymarket CLOB market WebSocket last-trade stream.
Fallback: official Data API public trades polled over HTTP when the WebSocket
opening handshake is unavailable or times out.

No orders are submitted and no credentials are required.

    python -m tools.shadow_market_auto --asset btc --duration 300
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymarket import AsyncPublicClient

from src.config import BotConfig
from src.discovery import discover
from src.shadow import apply_sell_trade
from tools.shadow_market import ShadowMarket


class ResilientShadowMarket(ShadowMarket):
    """ShadowMarket with retry + REST fallback for the public trade tape."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_source = "uninitialized"
        self.http_poll_errors = 0
        self.http_trade_rows = 0
        self._http_seen: set[tuple] = set()
        self._http_activation_ts = 0.0
        self._http_task: asyncio.Task | None = None

    async def start_stream(self) -> None:
        # A transient WebSocket handshake failure must not abort a 5-minute
        # observation. Retry the official stream first because it is the best
        # source for causal queue simulation.
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                print(f"trade tape: opening CLOB WebSocket (attempt {attempt}/3)…")
                await asyncio.wait_for(super().start_stream(), timeout=25.0)
                self.trade_source = "CLOB_WEBSOCKET"
                print("trade tape: CLOB WebSocket connected")
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                print(f"trade tape: WebSocket attempt {attempt} failed: {type(exc).__name__}: {exc}")
                try:
                    await super().stop_stream()
                except Exception:
                    pass
                self.handle = None
                self.listener_task = None
                if attempt < 3:
                    await asyncio.sleep(float(attempt * 2))

        # HTTP fallback is deliberately conservative: delayed trades are never
        # applied to a shadow order that did not yet exist at the trade time.
        self.trade_source = "DATA_API_HTTP_FALLBACK"
        self._http_activation_ts = time.time()
        print(
            "trade tape: WebSocket unavailable after 3 attempts; "
            "using official Data API HTTP fallback (conservative queue estimate)"
        )
        if last_exc is not None:
            print(f"trade tape: final WebSocket error: {type(last_exc).__name__}: {last_exc}")
        self._http_task = asyncio.create_task(self._http_poll_loop(), name="shadow-http-trade-tape")

    async def stop_stream(self) -> None:
        if self.trade_source == "CLOB_WEBSOCKET":
            await super().stop_stream()
            return

        # Give the Data API a short opportunity to expose recently indexed
        # trades, then drain anything that still belongs to a currently-valid
        # shadow order. This never extends hypothetical posting beyond T-2s.
        if self._http_task is not None:
            self._http_task.cancel()
            try:
                await self._http_task
            except asyncio.CancelledError:
                pass
            self._http_task = None
        for _ in range(3):
            try:
                await self._http_poll_once()
                await self.drain_trade_tape()
            except Exception as exc:  # noqa: BLE001
                self.http_poll_errors += 1
                print(f"trade tape: final HTTP poll warning: {type(exc).__name__}: {exc}")
            await asyncio.sleep(1.0)

    @staticmethod
    def _trade_ts(trade) -> float | None:
        ts = getattr(trade, "timestamp", None)
        if ts is None:
            return None
        if hasattr(ts, "timestamp"):
            try:
                return float(ts.timestamp())
            except Exception:
                return None
        try:
            x = float(ts)
            # Accept either seconds or milliseconds defensively.
            return x / 1000.0 if x > 10_000_000_000 else x
        except Exception:
            return None

    @classmethod
    def _trade_key(cls, trade) -> tuple:
        ts = cls._trade_ts(trade)
        return (
            str(getattr(trade, "transaction_hash", "") or ""),
            str(getattr(trade, "token_id", "") or ""),
            str(getattr(trade, "side", "") or "").upper(),
            str(getattr(trade, "price", "") or ""),
            str(getattr(trade, "size", "") or ""),
            round(ts or 0.0, 3),
        )

    async def _http_poll_once(self) -> int:
        now = time.time()
        # Re-query a trailing window because Data API indexing can lag wall time.
        start = int(max(self.market.window_start, now - 30.0))
        end = int(min(self.market.window_end, now) + 1)
        paginator = self.client.list_trades(
            market=[self.market.condition_id],
            start=start,
            end=end,
            page_size=500,
        )
        fresh = []
        page_count = 0
        async for page in paginator:
            page_count += 1
            for trade in page.items:
                key = self._trade_key(trade)
                if key in self._http_seen:
                    continue
                self._http_seen.add(key)
                trade_ts = self._trade_ts(trade)
                if trade_ts is None:
                    continue
                if trade_ts < self._http_activation_ts - 0.250:
                    continue
                if trade_ts > self.market.window_end + 0.250:
                    continue
                if str(getattr(trade, "side", "")).upper() != "SELL":
                    continue
                tok = str(getattr(trade, "token_id", "") or "")
                if tok not in (self.market.up_token_id, self.market.down_token_id):
                    continue
                fresh.append(trade)
            # 30 seconds of one binary market should never require an unbounded
            # walk. Five 500-row pages is already far beyond normal tape density.
            if page_count >= 5:
                break

        fresh.sort(key=lambda t: self._trade_ts(t) or 0.0)
        for trade in fresh:
            await self.trade_q.put(trade)
        self.http_trade_rows += len(fresh)
        return len(fresh)

    async def _http_poll_loop(self) -> None:
        while self.market.seconds_to_end > -3:
            try:
                await self._http_poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.http_poll_errors += 1
                if self.http_poll_errors <= 5 or self.http_poll_errors % 10 == 0:
                    print(f"trade tape: HTTP poll warning #{self.http_poll_errors}: {type(exc).__name__}: {exc}")
            await asyncio.sleep(0.75)

    def _record_fill_at(self, side: str, qty: float, price: float, mode: str, event_ts: float) -> None:
        if qty <= 0:
            return
        t = self.up if side == "UP" else self.down
        t.shares += qty
        t.cost += qty * price
        t.last_fill_ts = event_ts
        t.fill_events += 1
        if mode == "maker":
            t.maker_fill_events += 1
        else:
            t.taker_fill_events += 1
        t.prices.add(round(price, 6))
        self.first_fill_ts = event_ts if self.first_fill_ts is None else min(self.first_fill_ts, event_ts)
        self.last_fill_ts = event_ts if self.last_fill_ts is None else max(self.last_fill_ts, event_ts)
        lag = max(0.0, time.time() - event_ts)
        print(
            f"FILL {mode.upper():5s} {side:4s} {qty:7.3f} @ {price:.4f} | "
            f"UP {self.up.shares:.1f}@{self.up.vwap:.4f} "
            f"DOWN {self.down.shares:.1f}@{self.down.vwap:.4f} tape_lag={lag:.2f}s"
        )

    async def drain_trade_tape(self) -> None:
        while True:
            try:
                p = self.trade_q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if str(getattr(p, "side", "")).upper() != "SELL":
                continue
            tok = str(getattr(p, "token_id", "") or "")
            side = (
                "UP" if tok == self.market.up_token_id
                else "DOWN" if tok == self.market.down_token_id
                else None
            )
            if side is None:
                continue
            o = self.orders.get(side)
            if o is None:
                continue
            event_ts = self._trade_ts(p)
            if event_ts is None:
                event_ts = time.time()
            # Critical for delayed HTTP rows: a historical trade cannot fill an
            # order that was posted after that trade occurred.
            if event_ts + 0.050 < o.posted_ts:
                continue
            try:
                price = float(p.price)
                size = float(p.size or 0)
            except Exception:
                continue
            qty = apply_sell_trade(o, trade_price=price, trade_size=size)
            if qty > 0:
                self._record_fill_at(side, qty, o.price, "maker", event_ts)
            if o.done:
                self.orders[side] = None

    def report(self) -> None:
        super().report()
        print(f"trade_source: {self.trade_source}")
        if self.trade_source == "DATA_API_HTTP_FALLBACK":
            print(f"http_sell_rows_seen: {self.http_trade_rows}")
            print(f"http_poll_errors: {self.http_poll_errors}")
            print(
                "fallback_note: HTTP-indexed trades can arrive late; the runner "
                "rejects pre-order trades, so maker fills are conservative rather than fabricated."
            )


async def amain(args) -> int:
    cfg = BotConfig.load("config/default.yaml")
    client = AsyncPublicClient()
    try:
        markets = await discover(client, (args.asset,), (args.duration,))
        candidates = [m for m in markets if m.seconds_to_end > 20]
        if not candidates:
            print("No suitable current market found. Retry after the next window opens.")
            return 2
        market = candidates[0]
        runner = ResilientShadowMarket(client, market, cfg.strategy, max_spend=args.max_spend)
        await runner.run()
        return 0
    finally:
        await client.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="resilient zero-money live trade-tape shadow runner")
    ap.add_argument("--asset", choices=("btc", "eth", "sol", "xrp"), default="btc")
    ap.add_argument("--duration", type=int, choices=(300, 900), default=300)
    ap.add_argument(
        "--max-spend",
        type=float,
        default=2000.0,
        help="simulation-only spend guard; no funds are used",
    )
    args = ap.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
