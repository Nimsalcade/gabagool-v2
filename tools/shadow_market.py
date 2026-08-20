"""Zero-money live-market shadow execution.

Runs the V3 policy against a real current Polymarket market without submitting orders.
Maker fills are estimated from the public last-trade stream plus visible queue ahead;
taker FAKs are simulated against the real best ask and displayed depth.

This is materially stronger than `--dry-run`, whose synthetic fills exist only to test
plumbing.  Shadow mode is still an estimator because cancellations/queue priority ahead
of a hypothetical order cannot be observed perfectly.

Example:
    python -m tools.shadow_market --asset btc --duration 300
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymarket import AsyncPublicClient
from polymarket.streams import MarketSpec

from src.config import BotConfig
from src.discovery import discover
from src.policy import (
    BookSide,
    InventoryState,
    adaptive_clip,
    basis_allows,
    maker_target,
    projected_combined_vwap,
    relation_for_side,
    taker_should_fire,
    tick_floor,
)
from src.shadow import ShadowOrder, apply_sell_trade, reduce_queue_from_book
from src.constants import FALLBACK_MIN_ORDER_SHARES, FALLBACK_TICK, MIN_ORDER_NOTIONAL_USD


@dataclass
class Totals:
    shares: float = 0.0
    cost: float = 0.0
    last_fill_ts: float | None = None
    fill_events: int = 0
    maker_fill_events: int = 0
    taker_fill_events: int = 0
    prices: set[float] = field(default_factory=set)

    @property
    def vwap(self) -> float:
        return self.cost / self.shares if self.shares > 0 else 0.0


class ShadowMarket:
    def __init__(self, client, market, cfg, *, max_spend: float):
        self.client = client
        self.market = market
        self.cfg = cfg
        self.max_spend = float(max_spend)
        self.up = Totals()
        self.down = Totals()
        self.orders: dict[str, ShadowOrder | None] = {"UP": None, "DOWN": None}
        self.tick = FALLBACK_TICK
        self.min_shares = FALLBACK_MIN_ORDER_SHARES
        self.trade_q: asyncio.Queue = asyncio.Queue()
        self.listener_task: asyncio.Task | None = None
        self.handle = None
        self.first_fill_ts: float | None = None
        self.last_fill_ts: float | None = None
        self.taker_cooldown = {"UP": 0.0, "DOWN": 0.0}

    @property
    def spend(self) -> float:
        return self.up.cost + self.down.cost

    def state(self) -> InventoryState:
        return InventoryState(
            up_shares=self.up.shares,
            down_shares=self.down.shares,
            up_cost=self.up.cost,
            down_cost=self.down.cost,
            last_up_fill_ts=self.up.last_fill_ts,
            last_down_fill_ts=self.down.last_fill_ts,
            now_ts=time.time(),
            window_start_ts=float(self.market.window_start),
            seconds_to_end=self.market.seconds_to_end,
        )

    async def start_stream(self) -> None:
        self.handle = await self.client.subscribe(
            MarketSpec(
                token_ids=(self.market.up_token_id, self.market.down_token_id),
                custom_feature_enabled=False,
            )
        )

        async def listen():
            async for ev in self.handle:
                if getattr(ev, "type", None) == "last_trade_price":
                    await self.trade_q.put(ev.payload)

        self.listener_task = asyncio.create_task(listen(), name="shadow-trade-tape")

    async def stop_stream(self) -> None:
        if self.listener_task is not None:
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass
        if self.handle is not None:
            try:
                await self.handle.close()
            except Exception:
                pass

    def _record_fill(self, side: str, qty: float, price: float, mode: str) -> None:
        if qty <= 0:
            return
        now = time.time()
        t = self.up if side == "UP" else self.down
        t.shares += qty
        t.cost += qty * price
        t.last_fill_ts = now
        t.fill_events += 1
        if mode == "maker":
            t.maker_fill_events += 1
        else:
            t.taker_fill_events += 1
        t.prices.add(round(price, 6))
        self.first_fill_ts = now if self.first_fill_ts is None else self.first_fill_ts
        self.last_fill_ts = now
        print(
            f"FILL {mode.upper():5s} {side:4s} {qty:7.3f} @ {price:.4f} | "
            f"UP {self.up.shares:.1f}@{self.up.vwap:.4f} "
            f"DOWN {self.down.shares:.1f}@{self.down.vwap:.4f}"
        )

    async def drain_trade_tape(self) -> None:
        while True:
            try:
                p = self.trade_q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if str(getattr(p, "side", "")).upper() != "SELL":
                continue
            tok = str(getattr(p, "token_id", ""))
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
            try:
                price = float(p.price)
                size = float(p.size or 0)
            except Exception:
                continue
            qty = apply_sell_trade(o, trade_price=price, trade_size=size)
            if qty > 0:
                self._record_fill(side, qty, o.price, "maker")
            if o.done:
                self.orders[side] = None

    @staticmethod
    def _book_side(book) -> tuple[BookSide, list[tuple[float, float]], list[tuple[float, float]]] | None:
        bids = [(float(x.price), float(x.size)) for x in (book.bids or [])]
        asks = [(float(x.price), float(x.size)) for x in (book.asks or [])]
        if not bids or not asks:
            return None
        bb = max(p for p, _ in bids)
        ba = min(p for p, _ in asks)
        return BookSide(best_bid=bb, best_ask=ba), bids, asks

    @staticmethod
    def level_size(levels: list[tuple[float, float]], price: float) -> float | None:
        for px, size in levels:
            if abs(px - price) <= 1e-9:
                return size
        return None

    async def fetch_books(self):
        ub, db = await asyncio.gather(
            self.client.get_order_book(token_id=self.market.up_token_id),
            self.client.get_order_book(token_id=self.market.down_token_id),
        )
        u = self._book_side(ub)
        d = self._book_side(db)
        if u is None or d is None:
            return None
        self.tick = float(getattr(ub, "tick_size", None) or FALLBACK_TICK)
        self.min_shares = float(getattr(ub, "min_order_size", None) or FALLBACK_MIN_ORDER_SHARES)

        # Recognize visible queue shrink, never queue growth (new same-price orders
        # may be behind our hypothetical order).
        for side, data in (("UP", u), ("DOWN", d)):
            o = self.orders.get(side)
            if o is not None:
                reduce_queue_from_book(
                    o,
                    visible_size_at_price=self.level_size(data[1], o.price),
                )
        return u, d

    def can_spend(self, dollars: float) -> bool:
        return dollars > 0 and self.spend + dollars <= self.max_spend + 1e-9

    def post_or_requote_maker(
        self,
        side: str,
        token_id: str,
        book: BookSide,
        bids: list[tuple[float, float]],
        other_target: float,
    ) -> None:
        s = self.state()
        relation = relation_for_side(s, side)
        ratio = s.larger_to_smaller_ratio
        held = self.up.shares if side == "UP" else self.down.shares
        if held >= self.cfg.max_shares_per_side:
            self.orders[side] = None
            return
        if relation == "heavy" and ratio >= self.cfg.hard_pause_ratio:
            self.orders[side] = None
            return

        target = maker_target(
            book,
            tick=self.tick,
            inventory_relation=relation,
            ratio=ratio,
        )
        if target is None:
            return
        qty = adaptive_clip(
            base_clip_shares=self.cfg.base_clip_shares,
            max_clip_shares=self.cfg.max_clip_shares,
            min_order_shares=self.min_shares,
            min_notional=MIN_ORDER_NOTIONAL_USD,
            price=target,
            ratio=ratio,
            relation=relation,
            aggressive=False,
        )
        price = target
        while price >= self.tick and not basis_allows(
            s,
            side=side,
            price=price,
            shares=qty,
            max_combined_vwap=self.cfg.max_combined_vwap,
            opposite_reference_price=other_target,
            initial_pair_ceiling=self.cfg.initial_pair_ceiling,
        ):
            price = tick_floor(price - self.tick, self.tick)
        if price < self.tick or not self.can_spend(price * qty):
            return

        old = self.orders.get(side)
        if old is not None and abs(old.price - price) < self.cfg.requote_drift - 1e-12:
            return
        queue = self.level_size(bids, price)
        if queue is None:
            queue = 0.0 if price > book.best_bid + 1e-9 else float("inf")
        self.orders[side] = ShadowOrder(
            side=side,
            token_id=token_id,
            price=price,
            shares=qty,
            queue_ahead=queue,
            posted_ts=time.time(),
        )
        qtxt = "inf" if math.isinf(queue) else f"{queue:.1f}"
        print(f"QUOTE MAKER {side:4s} {qty:.0f} @ {price:.4f} queue_ahead={qtxt}")

    def maybe_taker(self, udata, ddata) -> bool:
        s = self.state()
        if s.up_shares <= 0 and s.down_shares <= 0:
            return False
        deficient = s.deficient_side
        candidates = [deficient] if deficient else ["UP", "DOWN"]
        for side in candidates:
            if side is None or time.time() < self.taker_cooldown[side]:
                continue
            data = udata if side == "UP" else ddata
            book, _, asks = data
            relation = relation_for_side(s, side)
            max_price = tick_floor(book.best_ask, self.tick)
            if max_price <= 0 or max_price >= 1:
                continue
            planned = adaptive_clip(
                base_clip_shares=self.cfg.base_clip_shares,
                max_clip_shares=self.cfg.max_clip_shares,
                min_order_shares=self.min_shares,
                min_notional=MIN_ORDER_NOTIONAL_USD,
                price=max_price,
                ratio=s.larger_to_smaller_ratio,
                relation=relation,
                aggressive=True,
            )
            projected = projected_combined_vwap(s, side=side, price=max_price, shares=planned)
            if not taker_should_fire(
                s,
                candidate_side=side,
                projected_basis=projected,
                target_combined_vwap=self.cfg.target_combined_vwap,
                max_combined_vwap=self.cfg.max_combined_vwap,
                taker_stop_buffer_s=self.cfg.taker_stop_buffer_s,
            ):
                continue
            other_book = ddata[0] if side == "UP" else udata[0]
            if not basis_allows(
                s,
                side=side,
                price=max_price,
                shares=planned,
                max_combined_vwap=self.cfg.max_combined_vwap,
                opposite_reference_price=other_book.best_bid,
                initial_pair_ceiling=self.cfg.initial_pair_ceiling,
            ):
                continue
            displayed = self.level_size(asks, book.best_ask) or 0.0
            qty = min(planned, displayed)
            if qty <= 0 or not self.can_spend(max_price * qty):
                continue
            self.orders[side] = None
            self._record_fill(side, qty, max_price, "taker")
            self.taker_cooldown[side] = time.time() + 2.0
            return True
        return False

    async def run(self) -> None:
        print(f"SHADOW MARKET: {self.market.slug}")
        print(f"window: {self.market.window_start} -> {self.market.window_end}")
        print(f"simulation spend guard: ${self.max_spend:.2f}; ZERO orders will be submitted")
        await self.start_stream()
        try:
            entry_delay = float(self.cfg.entry_delay_by_duration_s.get(self.market.duration_s, 10.0))
            while self.market.seconds_to_end > 0:
                if self.market.age_seconds < entry_delay:
                    await asyncio.sleep(min(0.5, entry_delay - self.market.age_seconds))
                    continue
                await self.drain_trade_tape()
                data = await self.fetch_books()
                if data is None:
                    await asyncio.sleep(0.5)
                    continue
                udata, ddata = data
                if self.market.seconds_to_end <= self.cfg.stop_posting_buffer_s:
                    self.orders = {"UP": None, "DOWN": None}
                    await asyncio.sleep(0.1)
                    continue
                if not self.maybe_taker(udata, ddata):
                    ut = maker_target(
                        udata[0], tick=self.tick,
                        inventory_relation=relation_for_side(self.state(), "UP"),
                        ratio=self.state().larger_to_smaller_ratio,
                    )
                    dt = maker_target(
                        ddata[0], tick=self.tick,
                        inventory_relation=relation_for_side(self.state(), "DOWN"),
                        ratio=self.state().larger_to_smaller_ratio,
                    )
                    if ut is not None and dt is not None:
                        self.post_or_requote_maker(
                            "UP", self.market.up_token_id, udata[0], udata[1], dt
                        )
                        self.post_or_requote_maker(
                            "DOWN", self.market.down_token_id, ddata[0], ddata[1], ut
                        )
                await asyncio.sleep(self.cfg.requote_interval_s)
            await self.drain_trade_tape()
        finally:
            await self.stop_stream()
        self.report()

    def report(self) -> None:
        fills = self.up.fill_events + self.down.fill_events
        maker = self.up.maker_fill_events + self.down.maker_fill_events
        taker = self.up.taker_fill_events + self.down.taker_fill_events
        matched = min(self.up.shares, self.down.shares)
        ratio = (
            max(self.up.shares, self.down.shares) / min(self.up.shares, self.down.shares)
            if min(self.up.shares, self.down.shares) > 0 else math.inf
        )
        combined = self.up.vwap + self.down.vwap if matched > 0 else None
        edge = matched * (1 - combined) if combined is not None else 0.0
        first_age = self.first_fill_ts - self.market.window_start if self.first_fill_ts else None
        last_age = self.last_fill_ts - self.market.window_start if self.last_fill_ts else None
        print("\n================ SHADOW RESULT ================")
        print("market:", self.market.slug)
        print("fill events:", fills, "maker:", maker, "taker:", taker,
              "taker_share:", f"{(100*taker/fills if fills else 0):.2f}%")
        print(f"UP   shares={self.up.shares:.6f} spend=${self.up.cost:.6f} vwap={self.up.vwap:.6f} levels={len(self.up.prices)}")
        print(f"DOWN shares={self.down.shares:.6f} spend=${self.down.cost:.6f} vwap={self.down.vwap:.6f} levels={len(self.down.prices)}")
        print(f"total spend=${self.spend:.6f}")
        print("combined_vwap:", "n/a" if combined is None else f"{combined:.6f}")
        print("terminal_ratio:", "inf" if math.isinf(ratio) else f"{ratio:.6f}")
        print(f"matched_pairs={matched:.6f} gross_matched_edge=${edge:.6f}")
        print("first_fill_age_s:", "n/a" if first_age is None else f"{first_age:.1f}")
        print("last_fill_age_s:", "n/a" if last_age is None else f"{last_age:.1f}")
        print("================================================")


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
        runner = ShadowMarket(client, market, cfg.strategy, max_spend=args.max_spend)
        await runner.run()
        return 0
    finally:
        await client.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="zero-money live trade-tape shadow runner")
    ap.add_argument("--asset", choices=("btc", "eth", "sol", "xrp"), default="btc")
    ap.add_argument("--duration", type=int, choices=(300, 900), default=300)
    ap.add_argument(
        "--max-spend", type=float, default=2000.0,
        help="simulation-only spend guard; no funds are used",
    )
    args = ap.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
