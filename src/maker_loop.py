"""Per-market mixed maker/taker accumulation engine.

Measured historical behavior this loop is designed to reproduce:
- almost exclusively BUY both outcomes;
- maker-dominant execution with a material taker repair layer;
- aggregate cost-basis management rather than a fixed per-quote pair cap;
- tight terminal inventory balance;
- maker quoting continues to the final seconds while taker aggression fades earlier;
- no intra-window MERGE. Settlement is owned by WindowManager after close.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from enum import Enum

from .constants import FALLBACK_MIN_ORDER_SHARES, FALLBACK_TICK, MIN_ORDER_NOTIONAL_USD
from .discovery import UpDownMarket
from .fills import FillTracker
from .ops import Backoff, classify_order_error
from .policy import (
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

log = logging.getLogger("maker_loop")


class State(Enum):
    WAIT_READY = "WAIT_READY"
    FARM = "FARM"
    HOLD = "HOLD"
    DONE = "DONE"


@dataclass
class WindowResult:
    market: UpDownMarket
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        return self.up_cost + self.down_cost


@dataclass
class _SideQuote:
    order_id: str | None = None
    price: float | None = None
    mode: str = "maker"
    last_post: float = 0.0


class MakerLoop:
    def __init__(self, client, market: UpDownMarket, *, cfg, capital, ledger=None, dry_run=True):
        self.client = client
        self.market = market
        self.cfg = cfg
        self.capital = capital
        self.ledger = ledger
        self.dry_run = dry_run
        self.tracker = FillTracker(condition_id=market.condition_id, ledger=ledger)
        self.backoff = Backoff()
        self._sides = {"UP": _SideQuote(), "DOWN": _SideQuote()}
        self._tick = FALLBACK_TICK
        self._min_shares = FALLBACK_MIN_ORDER_SHARES
        self._dry_cycle = 0
        self.log = logging.getLogger(
            f"maker.{market.asset}.{market.duration_s}.{market.window_start % 100000}"
        )

    async def run(self) -> WindowResult:
        m = self.market
        self.log.info("window start | %s | neg_risk=%s", m, m.neg_risk)
        state = State.WAIT_READY
        ts_open = time.time()
        try:
            while state is not State.DONE:
                remaining = m.seconds_to_end
                if remaining <= 0:
                    state = State.DONE
                    break

                entry_delay = float(self.cfg.entry_delay_by_duration_s.get(m.duration_s, 10.0))
                if m.age_seconds < entry_delay:
                    await asyncio.sleep(min(1.0, entry_delay - m.age_seconds))
                    continue

                if remaining <= self.cfg.stop_posting_buffer_s and state is State.FARM:
                    await self._enter_hold()
                    state = State.HOLD

                if state is State.WAIT_READY:
                    state = await self._wait_ready_tick(state)
                elif state is State.FARM:
                    await self._farm_tick()
                elif state is State.HOLD:
                    if not self.dry_run:
                        filled_notional = await self.tracker.reconcile(self.client)
                        if filled_notional:
                            self.capital.on_spend(m.condition_id, filled_notional)
                    await asyncio.sleep(min(0.25, max(0.05, remaining)))
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.log.info("cancelled — cleaning up")
            raise
        finally:
            await self._cancel_all_safe()
            if not self.dry_run:
                filled_notional = await self.tracker.reconcile(self.client)
                if filled_notional:
                    self.capital.on_spend(m.condition_id, filled_notional)
        return self._finish(ts_open)

    async def _wait_ready_tick(self, state: State) -> State:
        if not self.backoff.ready("ready"):
            await asyncio.sleep(min(1.0, self.backoff.seconds_left("ready")))
            return state
        pair = await self._fetch_books()
        if pair is not None:
            self.backoff.success("ready")
            self.log.info("book live — FARM")
            return State.FARM
        delay = self.backoff.failure("ready", factor=2.0)
        await asyncio.sleep(min(delay, 2.0))
        return state

    def _state(self) -> InventoryState:
        t = self.tracker
        return InventoryState(
            up_shares=t.up.shares,
            down_shares=t.down.shares,
            up_cost=t.up.cost,
            down_cost=t.down.cost,
            last_up_fill_ts=t.up.last_fill_ts,
            last_down_fill_ts=t.down.last_fill_ts,
            now_ts=time.time(),
            window_start_ts=float(self.market.window_start),
            seconds_to_end=self.market.seconds_to_end,
        )

    async def _farm_tick(self) -> None:
        pair = await self._fetch_books()
        if pair is None:
            await asyncio.sleep(0.5)
            return
        up_book, down_book = pair

        if not self.dry_run:
            filled_notional = await self.tracker.reconcile(self.client)
            if filled_notional:
                self.capital.on_spend(self.market.condition_id, filled_notional)

        self.capital.update_resting(
            self.market.condition_id, self.tracker.resting_notional()
        )
        state = self._state()

        # Repair first. A marketable limit is permitted only when aggregate basis
        # and inventory/staleness state justify it.
        if self.cfg.taker_enabled and await self._maybe_taker(state, up_book, down_book):
            await asyncio.sleep(self.cfg.requote_interval_s)
            return

        state = self._state()
        up_target = maker_target(
            up_book,
            tick=self._tick,
            inventory_relation=relation_for_side(state, "UP"),
            ratio=state.larger_to_smaller_ratio,
        )
        down_target = maker_target(
            down_book,
            tick=self._tick,
            inventory_relation=relation_for_side(state, "DOWN"),
            ratio=state.larger_to_smaller_ratio,
        )

        if up_target is not None and down_target is not None:
            await self._manage_maker("UP", self.market.up_token_id, up_target, down_target)
            await self._manage_maker("DOWN", self.market.down_token_id, down_target, up_target)

        await asyncio.sleep(self.cfg.requote_interval_s)

    async def _maybe_taker(
        self, state: InventoryState, up_book: BookSide, down_book: BookSide
    ) -> bool:
        # Build the initial two-sided base passively.
        if state.up_shares <= 0 and state.down_shares <= 0:
            return False

        deficient = state.deficient_side
        candidates = [deficient] if deficient else ["UP", "DOWN"]
        for side in candidates:
            if side is None:
                continue
            book = up_book if side == "UP" else down_book
            relation = relation_for_side(state, side)
            if relation == "heavy" and not (
                state.combined_vwap is not None
                and state.combined_vwap <= self.cfg.target_combined_vwap - 0.01
            ):
                continue
            price = tick_floor(book.best_ask, self._tick)
            if price <= 0 or price >= 1:
                continue
            shares = adaptive_clip(
                base_clip_shares=self.cfg.base_clip_shares,
                max_clip_shares=self.cfg.max_clip_shares,
                min_order_shares=self._min_shares,
                min_notional=MIN_ORDER_NOTIONAL_USD,
                price=price,
                ratio=state.larger_to_smaller_ratio,
                relation=relation,
                aggressive=True,
            )
            projected = projected_combined_vwap(
                state, side=side, price=price, shares=shares
            )
            if not taker_should_fire(
                state,
                candidate_side=side,
                projected_basis=projected,
                target_combined_vwap=self.cfg.target_combined_vwap,
                max_combined_vwap=self.cfg.max_combined_vwap,
                taker_stop_buffer_s=self.cfg.taker_stop_buffer_s,
            ):
                continue
            if not basis_allows(
                state,
                side=side,
                price=price,
                shares=shares,
                max_combined_vwap=self.cfg.max_combined_vwap,
                opposite_reference_price=(
                    down_book.best_bid if side == "UP" else up_book.best_bid
                ),
                initial_pair_ceiling=self.cfg.initial_pair_ceiling,
            ):
                continue
            if not self.capital.can_commit(
                self.market.condition_id,
                self.tracker.total_cost(),
                self.tracker.resting_notional(),
                price * shares,
            ):
                continue
            held = self.tracker.up.shares if side == "UP" else self.tracker.down.shares
            if held + shares > self.cfg.max_shares_per_side:
                continue

            await self._cancel_side(side)
            token_id = self.market.up_token_id if side == "UP" else self.market.down_token_id
            oid = await self._post_buy(
                token_id,
                side,
                price,
                shares,
                post_only=False,
                mode="taker",
            )
            if oid:
                self._sides[side] = _SideQuote(oid, price, "taker", time.time())
                self.tracker.register(
                    oid,
                    side,
                    token_id,
                    price,
                    shares,
                    mode="taker",
                )
                if self.ledger is not None:
                    self.ledger.record_order(
                        oid, self.market.condition_id, side, price, shares
                    )
                self.capital.update_resting(
                    self.market.condition_id, self.tracker.resting_notional()
                )
                if self.dry_run:
                    self._dry_fill(oid)
                self.log.info(
                    "TAKER repair %s %.0f sh @ %.3f ratio=%s stale=%.1fs basis=%s",
                    side,
                    shares,
                    price,
                    "inf"
                    if math.isinf(state.larger_to_smaller_ratio)
                    else f"{state.larger_to_smaller_ratio:.3f}",
                    state.opposite_stale_seconds(side),
                    "n/a" if projected is None else f"{projected:.4f}",
                )
                return True
        return False

    async def _manage_maker(
        self, side: str, token_id: str, target: float, other_target: float
    ) -> None:
        state = self._state()
        relation = relation_for_side(state, side)
        ratio = state.larger_to_smaller_ratio
        held = self.tracker.up.shares if side == "UP" else self.tracker.down.shares

        # Only a true runaway imbalance hard-pauses the heavy side. Normal imbalance
        # is handled by price/size skew and the taker repair layer.
        if relation == "heavy" and ratio >= self.cfg.hard_pause_ratio:
            await self._cancel_side(side)
            return
        if held >= self.cfg.max_shares_per_side:
            await self._cancel_side(side)
            return

        sq = self._sides[side]
        if sq.order_id and sq.mode == "taker":
            await self._cancel_side(side)
            sq = self._sides[side]

        price = target
        shares = adaptive_clip(
            base_clip_shares=self.cfg.base_clip_shares,
            max_clip_shares=self.cfg.max_clip_shares,
            min_order_shares=self._min_shares,
            min_notional=MIN_ORDER_NOTIONAL_USD,
            price=price,
            ratio=ratio,
            relation=relation,
            aggressive=False,
        )

        # Shave only as much as aggregate basis requires; no fixed 0.97 pair cap.
        while price >= self._tick and not basis_allows(
            state,
            side=side,
            price=price,
            shares=shares,
            max_combined_vwap=self.cfg.max_combined_vwap,
            opposite_reference_price=other_target,
            initial_pair_ceiling=self.cfg.initial_pair_ceiling,
        ):
            price = tick_floor(price - self._tick, self._tick)
        if price < self._tick:
            await self._cancel_side(side)
            return

        if sq.order_id is not None and sq.price is not None:
            if abs(sq.price - price) < self.cfg.requote_drift - 1e-12:
                return

        notional = price * shares
        if not self.capital.can_commit(
            self.market.condition_id,
            self.tracker.total_cost(),
            self.tracker.resting_notional(),
            notional,
        ):
            return

        if sq.order_id is not None:
            await self._cancel_order(sq.order_id)
            self._sides[side] = _SideQuote()

        oid = await self._post_buy(
            token_id, side, price, shares, post_only=True, mode="maker"
        )
        if oid:
            self._sides[side] = _SideQuote(oid, price, "maker", time.time())
            self.tracker.register(
                oid, side, token_id, price, shares, mode="maker"
            )
            if self.ledger is not None:
                self.ledger.record_order(
                    oid, self.market.condition_id, side, price, shares
                )
            self.capital.update_resting(
                self.market.condition_id, self.tracker.resting_notional()
            )
            self.backoff.success(f"post:{side}")

    async def _post_buy(
        self,
        token_id: str,
        side: str,
        price: float,
        shares: float,
        *,
        post_only: bool,
        mode: str,
    ) -> str | None:
        assert side in ("UP", "DOWN")
        if self.dry_run:
            return (
                f"dry_{mode}_{side}_{int(price*1000)}_"
                f"{int(time.time()*1e6)%1000000}"
            )
        if not self.backoff.ready(f"post:{side}"):
            return None
        try:
            resp = await self.client.place_limit_order(
                token_id=token_id,
                side="BUY",
                price=str(price),
                size=str(shares),
                post_only=post_only,
            )
        except Exception as exc:  # noqa: BLE001
            self._order_error(side, str(exc))
            return None
        if getattr(resp, "ok", False):
            return str(resp.order_id)
        self._order_error(
            side, f"{getattr(resp, 'code', '')} {getattr(resp, 'message', '')}"
        )
        return None

    def _order_error(self, side: str, msg: str) -> None:
        category, factor = classify_order_error(msg)
        delay = self.backoff.failure(f"post:{side}", factor=factor)
        lvl = logging.WARNING if category in ("constraint", "balance") else logging.INFO
        self.log.log(
            lvl,
            "order rejected [%s] %s — backoff %.1fs (%s)",
            category,
            side,
            delay,
            msg[:160],
        )

    async def _enter_hold(self) -> None:
        self.log.info(
            "T−%ds HOLD — cancel quotes; settlement is post-close",
            self.cfg.stop_posting_buffer_s,
        )
        await self._cancel_all_safe()
        if not self.dry_run:
            filled_notional = await self.tracker.reconcile(self.client)
            if filled_notional:
                self.capital.on_spend(self.market.condition_id, filled_notional)

    async def _fetch_books(self) -> tuple[BookSide, BookSide] | None:
        try:
            up_book, down_book = await asyncio.gather(
                self.client.get_order_book(token_id=self.market.up_token_id),
                self.client.get_order_book(token_id=self.market.down_token_id),
            )
        except Exception as exc:  # noqa: BLE001
            cat, factor = classify_order_error(str(exc))
            self.backoff.failure("ready", factor=factor)
            self.log.debug("book fetch failed [%s]: %s", cat, exc)
            return None

        def side_of(book) -> BookSide | None:
            bids = [float(l.price) for l in (book.bids or [])]
            asks = [float(l.price) for l in (book.asks or [])]
            if not bids or not asks:
                return None
            return BookSide(best_bid=max(bids), best_ask=min(asks))

        u, d = side_of(up_book), side_of(down_book)
        if u is None or d is None:
            return None
        self._tick = float(up_book.tick_size or FALLBACK_TICK)
        self._min_shares = float(
            up_book.min_order_size or FALLBACK_MIN_ORDER_SHARES
        )
        if self.dry_run:
            self._simulate_maker_fills()
        return u, d

    def _simulate_maker_fills(self) -> None:
        """Deterministic plumbing simulation; not a profitability model."""
        self._dry_cycle += 1
        for i, side in enumerate(("UP", "DOWN")):
            sq = self._sides[side]
            if (
                sq.order_id
                and sq.mode == "maker"
                and (self._dry_cycle + i * 3) % 7 == 0
            ):
                self._dry_fill(sq.order_id)
                self._sides[side] = _SideQuote()

    def _dry_fill(self, order_id: str) -> None:
        o = self.tracker.orders.get(order_id)
        if not o or not o.open:
            return
        delta = o.shares - o.filled
        if delta <= 0:
            return
        o.filled = o.shares
        o.open = False
        tot = self.tracker.up if o.side == "UP" else self.tracker.down
        tot.shares += delta
        tot.cost += delta * o.price
        tot.max_price = max(tot.max_price, o.price)
        tot.last_fill_ts = time.time()
        self.capital.on_spend(self.market.condition_id, delta * o.price)
        if self.ledger is not None:
            self.ledger.record_fill(
                self.market.condition_id, o.order_id, o.side, o.price, delta
            )
        self.log.info(
            "[dry] FILL %s %.0f sh @ %.3f [%s]",
            o.side,
            delta,
            o.price,
            o.mode,
        )

    async def _cancel_order(self, order_id: str) -> None:
        o = self.tracker.orders.get(order_id)
        if self.dry_run or order_id.startswith("dry_"):
            if o:
                o.open = False
            return
        try:
            await self.client.cancel_order(order_id=order_id)
        except Exception as exc:  # noqa: BLE001
            self.log.debug("cancel %s failed: %s", order_id[:10], exc)

    async def _cancel_side(self, side: str) -> None:
        for oid, o in list(self.tracker.orders.items()):
            if o.open and o.side == side:
                await self._cancel_order(oid)
        self._sides[side] = _SideQuote()
        self.capital.update_resting(
            self.market.condition_id, self.tracker.resting_notional()
        )

    async def _cancel_all_safe(self) -> None:
        if self.dry_run:
            for o in self.tracker.orders.values():
                o.open = False
            for side in self._sides:
                self._sides[side] = _SideQuote()
            self.capital.update_resting(self.market.condition_id, 0.0)
            return
        try:
            await self.client.cancel_market_orders(
                market=self.market.condition_id
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("bulk cancel failed (%s); falling back", exc)
            for oid in self.tracker.open_order_ids():
                await self._cancel_order(oid)
        for side in self._sides:
            self._sides[side] = _SideQuote()
        self.capital.update_resting(self.market.condition_id, 0.0)

    def _finish(self, ts_open: float) -> WindowResult:
        t = self.tracker
        res = WindowResult(
            market=self.market,
            up_shares=t.up.shares,
            down_shares=t.down.shares,
            up_cost=t.up.cost,
            down_cost=t.down.cost,
        )
        combined = t.combined_avg()
        ratio = (
            max(t.up.shares, t.down.shares) / min(t.up.shares, t.down.shares)
            if min(t.up.shares, t.down.shares)
            else math.inf
        )
        self.log.info(
            "window done | UP %.3f@%.3f DOWN %.3f@%.3f | combined=%s ratio=%s cost=$%.2f",
            t.up.shares,
            t.up.avg_price,
            t.down.shares,
            t.down.avg_price,
            "n/a" if combined is None else f"{combined:.4f}",
            "inf" if math.isinf(ratio) else f"{ratio:.4f}",
            t.total_cost(),
        )
        if self.ledger is not None:
            self.ledger.record_window(
                self.market.condition_id,
                self.market.slug,
                ts_open,
                time.time(),
                t.up.shares,
                t.down.shares,
                t.up.cost,
                t.down.cost,
                0.0,
            )
        return res
