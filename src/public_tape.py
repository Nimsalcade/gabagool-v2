"""Public SELL-tape feed and conservative multi-layer shadow execution.

Execution infrastructure only.  This module contains no Gabagool strategy logic.

Primary source:
    Polymarket CLOB ``last_trade_price`` WebSocket events.

Fallback:
    Official public Data API ``list_trades`` polling.

The multi-layer allocator is intentionally finite-volume.  One public SELL print is
consumed once across hypothetical BUY layers in price/time priority; it is never
independently multiplied across every layer.

Equal-price prints:
    visible queue ahead trades first, then our hypothetical order.

Below-bid prints:
    prove the market traded through the hypothetical bid, so queue ahead at that
    higher price is cleared, but only the observed print size is allocated across
    our hypothetical layers.  This is more conservative than independently calling
    the single-order ``apply_sell_trade`` primitive on several layers.
"""
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any, Iterable

from polymarket.streams import MarketSpec

from src.shadow import ShadowOrder


@dataclass(frozen=True)
class TapePrint:
    token_id: str
    side: str
    price: float
    size: float
    event_ts: float
    source: str
    tx_id: str = ""


def atomic_tape_groups(prints: Iterable[TapePrint]) -> list[list[TapePrint]]:
    """Group public prints that belong to one matching event.

    Same transaction hash stays together even if timestamps jitter.  Prints
    without a hash are grouped by millisecond timestamp.  Groups are returned
    in first-seen chronological order.  Callers must process an entire group
    before reconciling inventory-dependent quotes.
    """
    ordered = sorted(
        prints,
        key=lambda p: (p.event_ts, p.tx_id, p.token_id, -p.price),
    )
    buckets: dict[str, list[TapePrint]] = {}
    order: list[str] = []
    for p in ordered:
        key = p.tx_id if p.tx_id else f"ts:{round(p.event_ts, 3)}"
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(p)
    return [buckets[k] for k in order]


def apply_sell_print_to_orders(
    orders: Iterable[ShadowOrder],
    *,
    trade_price: float,
    trade_size: float,
) -> list[tuple[ShadowOrder, float]]:
    """Allocate one aggressive SELL print once across resting BUY layers.

    Orders are processed highest bid first, then oldest first.
    Returns ``[(order, newly_filled_shares), ...]``.
    """
    volume = max(0.0, float(trade_size))
    if volume <= 0:
        return []
    px = float(trade_price)
    eps = 1e-9
    fills: list[tuple[ShadowOrder, float]] = []
    live = sorted(
        (o for o in orders if not o.done),
        key=lambda o: (-o.price, o.posted_ts),
    )
    for o in live:
        if volume <= eps:
            break
        # Sorted descending: once the print is above this bid, it cannot reach
        # this order or any lower-priced hypothetical bid.
        if px > o.price + eps:
            break
        if px < o.price - eps:
            # A lower-price execution indicates the market traded through this
            # higher bid.  Clear queue-ahead, but allocate only finite observed
            # SELL volume rather than fabricating a full fill.
            o.queue_ahead = 0.0
        else:
            # Exact-price execution: external queue ahead must trade first.
            if math.isinf(o.queue_ahead):
                # Unknown/unbounded queue: exact-price tape cannot establish
                # that our hypothetical order was reached.
                break
            queue = max(0.0, float(o.queue_ahead))
            consumed = min(queue, volume)
            o.queue_ahead = queue - consumed
            volume -= consumed
            if volume <= eps:
                break
        delta = min(o.remaining, volume)
        if delta > eps:
            o.filled += delta
            volume -= delta
            fills.append((o, delta))
        # An execution exactly at this price does not prove that lower bid
        # levels traded.  Remaining reported volume belongs at this same price,
        # and V5 maintains unique prices per layer.
        if abs(px - o.price) <= eps:
            break
    return fills


class PublicSellTape:
    """Per-market public SELL tape with WS-first / HTTP-fallback acquisition."""

    def __init__(
        self,
        client: Any,
        market: Any,
        *,
        ws_timeout_s: float = 10.0,
        http_poll_s: float = 0.75,
    ):
        self.client = client
        self.market = market
        self.ws_timeout_s = float(ws_timeout_s)
        self.http_poll_s = float(http_poll_s)
        self.trade_q: asyncio.Queue[TapePrint] = asyncio.Queue()
        self.trade_source = "uninitialized"
        self.handle = None
        self.listener_task: asyncio.Task | None = None
        self.http_task: asyncio.Task | None = None
        self.http_poll_errors = 0
        self.http_trade_rows = 0
        self._http_seen: set[tuple] = set()
        self._activation_ts = 0.0

    @staticmethod
    def _trade_ts(trade: Any) -> float | None:
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
            return x / 1000.0 if x > 10_000_000_000 else x
        except Exception:
            return None

    @classmethod
    def _trade_key(cls, trade: Any) -> tuple:
        ts = cls._trade_ts(trade)
        return (
            str(getattr(trade, "transaction_hash", "") or ""),
            str(getattr(trade, "token_id", "") or ""),
            str(getattr(trade, "side", "") or "").upper(),
            str(getattr(trade, "price", "") or ""),
            str(getattr(trade, "size", "") or ""),
            round(ts or 0.0, 3),
        )

    def _normalize(
        self,
        trade: Any,
        *,
        source: str,
        fallback_ts: float | None = None,
    ) -> TapePrint | None:
        if str(getattr(trade, "side", "") or "").upper() != "SELL":
            return None
        token = str(getattr(trade, "token_id", "") or "")
        if token not in (self.market.up_token_id, self.market.down_token_id):
            return None
        try:
            price = float(getattr(trade, "price"))
            size = float(getattr(trade, "size", 0) or 0)
        except (TypeError, ValueError):
            return None
        if price <= 0 or size <= 0:
            return None
        event_ts = self._trade_ts(trade)
        if event_ts is None:
            event_ts = float(fallback_ts if fallback_ts is not None else time.time())
        tx = (
            str(getattr(trade, "transaction_hash", "") or "")
            or str(getattr(trade, "transactionHash", "") or "")
        )
        return TapePrint(
            token_id=token,
            side="SELL",
            price=price,
            size=size,
            event_ts=event_ts,
            source=source,
            tx_id=tx,
        )

    async def start(self) -> None:
        self._activation_ts = time.time()
        try:
            self.handle = await asyncio.wait_for(
                self.client.subscribe(
                    MarketSpec(
                        token_ids=(
                            self.market.up_token_id,
                            self.market.down_token_id,
                        ),
                        custom_feature_enabled=False,
                    )
                ),
                timeout=self.ws_timeout_s,
            )
            self.trade_source = "CLOB_WEBSOCKET"

            async def listen() -> None:
                async for ev in self.handle:
                    if getattr(ev, "type", None) != "last_trade_price":
                        continue
                    received = time.time()
                    p = self._normalize(
                        ev.payload,
                        source="CLOB_WEBSOCKET",
                        fallback_ts=received,
                    )
                    if p is not None:
                        await self.trade_q.put(p)

            self.listener_task = asyncio.create_task(
                listen(),
                name=f"v5-tape-{self.market.asset}",
            )
            return
        except Exception:
            if self.handle is not None:
                try:
                    await self.handle.close()
                except Exception:
                    pass
            self.handle = None

        # Start fallback within the 18-second V5 pre-quote warm-up rather than
        # spending ~80 seconds retrying WS and missing the beginning of a market.
        self.trade_source = "DATA_API_HTTP_FALLBACK"
        self.http_task = asyncio.create_task(
            self._http_poll_loop(),
            name=f"v5-http-tape-{self.market.asset}",
        )

    async def _http_poll_once(self) -> int:
        now = time.time()
        start = int(max(self.market.window_start, now - 30.0))
        end = int(min(self.market.window_end, now) + 1)
        paginator = self.client.list_trades(
            market=[self.market.condition_id],
            start=start,
            end=end,
            page_size=500,
        )
        fresh: list[TapePrint] = []
        page_count = 0
        async for page in paginator:
            page_count += 1
            for trade in page.items:
                key = self._trade_key(trade)
                if key in self._http_seen:
                    continue
                self._http_seen.add(key)
                event_ts = self._trade_ts(trade)
                if event_ts is None:
                    continue
                if event_ts < self._activation_ts - 0.250:
                    continue
                if event_ts > self.market.window_end + 0.250:
                    continue
                p = self._normalize(
                    trade,
                    source="DATA_API_HTTP_FALLBACK",
                    fallback_ts=event_ts,
                )
                if p is not None:
                    fresh.append(p)
            if page_count >= 5:
                break
        fresh.sort(key=lambda x: x.event_ts)
        for p in fresh:
            await self.trade_q.put(p)
        self.http_trade_rows += len(fresh)
        return len(fresh)

    async def _http_poll_loop(self) -> None:
        while time.time() < self.market.window_end + 3.0:
            try:
                await self._http_poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.http_poll_errors += 1
            await asyncio.sleep(self.http_poll_s)

    async def drain(self) -> list[TapePrint]:
        out: list[TapePrint] = []
        while True:
            try:
                out.append(self.trade_q.get_nowait())
            except asyncio.QueueEmpty:
                break
        out.sort(key=lambda x: x.event_ts)
        return out

    async def stop(self) -> None:
        if self.listener_task is not None:
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass
            self.listener_task = None
        if self.handle is not None:
            try:
                await self.handle.close()
            except Exception:
                pass
            self.handle = None
        if self.http_task is not None:
            self.http_task.cancel()
            try:
                await self.http_task
            except asyncio.CancelledError:
                pass
            self.http_task = None
            # Give delayed official indexing three final opportunities.  The
            # Engine will drain these after stop().
            for _ in range(3):
                try:
                    await self._http_poll_once()
                except Exception:
                    self.http_poll_errors += 1
                await asyncio.sleep(1.0)
