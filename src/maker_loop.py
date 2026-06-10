"""
Per-window execution engine. One instance per live 15-minute market.

STATE MACHINE
-------------
    WAIT_READY ──(book accepting orders)──▶ FARM ──(T−buffer)──▶ HOLD ──▶ DONE
                                              │
                                              └─ every cycle: quote → reconcile → merge

WHAT IS STRUCTURALLY IMPOSSIBLE HERE
------------------------------------
* SELL orders: the order helper hardcodes side="BUY" and asserts it.
* Crossing the spread: prices are capped at best_ask − tick AND post_only=True,
  so a marketable order is rejected by the exchange instead of taking.
* A pair costing more than the budget: every post is capped against the OTHER
  side's actual resting price (see quoting.cap_against_resting).
* Directional entries: there is no signal input. The only asymmetry allowed
  is the imbalance brake, which can only PAUSE the heavy side — it can never
  add to it.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum

from .constants import FALLBACK_MIN_ORDER_SHARES, FALLBACK_TICK, MIN_ORDER_NOTIONAL_USD
from .discovery import UpDownMarket
from .fills import FillTracker
from .merge_engine import MergeEngine
from .ops import Backoff, classify_order_error
from .quoting import (
    BookSide,
    cap_against_resting,
    fair_split_bids,
    imbalance_fraction,
    should_requote,
    size_for_price,
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
    merged_pairs: float = 0.0

    @property
    def total_cost(self) -> float:
        return self.up_cost + self.down_cost


@dataclass
class _SideQuote:
    order_id: str | None = None
    price: float | None = None
    paused: bool = False
    last_post: float = 0.0


class MakerLoop:
    def __init__(
        self,
        client,
        market: UpDownMarket,
        *,
        cfg,                       # StrategyConfig
        capital,                   # CapitalManager
        merge_engine: MergeEngine,
        ledger=None,
        dry_run: bool = True,
    ):
        self.client = client
        self.market = market
        self.cfg = cfg
        self.capital = capital
        self.merges = merge_engine
        self.ledger = ledger
        self.dry_run = dry_run
        self.tracker = FillTracker(condition_id=market.condition_id)
        self.backoff = Backoff()
        self._sides = {"UP": _SideQuote(), "DOWN": _SideQuote()}
        self._tick = FALLBACK_TICK
        self._min_shares = FALLBACK_MIN_ORDER_SHARES
        self.log = logging.getLogger(f"maker.{market.asset}.{market.window_start % 100000}")

    # ============================================================= main loop
    async def run(self) -> WindowResult:
        m = self.market
        self.log.info("window start | %s | neg_risk=%s", m, m.neg_risk)
        state = State.WAIT_READY if not m.accepting_orders else State.FARM
        ts_open = time.time()

        try:
            while state is not State.DONE:
                remaining = m.seconds_to_end
                if remaining <= 0:
                    state = State.DONE
                    break

                if remaining <= self.cfg.stop_posting_buffer_s and state is State.FARM:
                    await self._enter_hold()
                    state = State.HOLD

                if state is State.WAIT_READY:
                    state = await self._wait_ready_tick(state)
                elif state is State.FARM:
                    await self._farm_tick()
                elif state is State.HOLD:
                    # Keep reconciling + merging the tail; never post.
                    await self.tracker.reconcile(self.client)
                    await self.merges.merge_condition(
                        m.condition_id, interval_s=self.cfg.merge_interval_s
                    )
                    await asyncio.sleep(2.0)

                await asyncio.sleep(0)  # cooperative
        except asyncio.CancelledError:
            self.log.info("cancelled — cleaning up")
            raise
        finally:
            await self._cancel_all_safe()
            # Last reconcile + force merge of whatever pairs exist on-chain.
            await self.tracker.reconcile(self.client)
            await self.merges.merge_condition(m.condition_id, force=True)

        return self._finish(ts_open)

    # ============================================================ state ticks
    async def _wait_ready_tick(self, state: State) -> State:
        """Poll the book gently until the market accepts orders."""
        if not self.backoff.ready("ready"):
            await asyncio.sleep(min(2.0, self.backoff.seconds_left("ready")))
            return state
        book_pair = await self._fetch_books()
        if book_pair is not None:
            self.log.info("book is live — entering FARM")
            self.backoff.success("ready")
            return State.FARM
        delay = self.backoff.failure("ready", factor=2.0)
        self.log.debug("book not ready; next check in %.1fs", delay)
        return state

    async def _farm_tick(self) -> None:
        cfg = self.cfg
        pair = await self._fetch_books()
        if pair is None:
            await asyncio.sleep(1.0)
            return
        up_side, down_side = pair

        # 1) authoritative fills first — quoting decisions use real inventory
        filled_notional = await self.tracker.reconcile(self.client)
        if filled_notional:
            self.capital.on_spend(filled_notional)

        # 2) imbalance brake (pause-only; can never buy the heavy side more)
        self._apply_imbalance_brake()

        # 3) merge matched pairs continuously (locked $1s recycle as capital)
        res = await self.merges.merge_condition(
            self.market.condition_id, interval_s=cfg.merge_interval_s
        )
        if res.success:
            self.capital.on_merge_return(res.pairs)

        # 4) compute fresh two-sided targets under the budget
        targets = fair_split_bids(up_side, down_side, cfg.combined_budget, self._tick)
        if targets is None:
            await asyncio.sleep(cfg.requote_interval_s)
            return
        target_up, target_down = targets

        # 5) re-quote each unpaused side that drifted
        await self._requote("UP", self.market.up_token_id, target_up, target_down)
        await self._requote("DOWN", self.market.down_token_id, target_down, target_up)

        await asyncio.sleep(cfg.requote_interval_s)

    async def _enter_hold(self) -> None:
        self.log.info("T−%ds — HOLD: cancel all, final merge", self.cfg.stop_posting_buffer_s)
        await self._cancel_all_safe()
        await self.tracker.reconcile(self.client)
        await self.merges.merge_condition(self.market.condition_id, force=True)

    # ============================================================== quoting
    async def _requote(self, side: str, token_id: str, my_target: float, other_target: float) -> None:
        sq = self._sides[side]
        if sq.paused or not self.backoff.ready(f"post:{side}"):
            return
        cfg = self.cfg

        held = self.tracker.up.shares if side == "UP" else self.tracker.down.shares
        if held >= cfg.max_shares_per_side:
            return

        other_resting = self.tracker.resting_price("DOWN" if side == "UP" else "UP")
        # When the other side is paused (no resting order), cap against its
        # actual average fill price, not the theoretical mid-based target.
        # Using the target would let the active side quote too high when the
        # market moved away from the paused side's actual entry, producing
        # pairs whose combined cost exceeds the budget.
        effective_other = other_resting
        if effective_other is None:
            other_leg = self.tracker.down if side == "UP" else self.tracker.up
            if other_leg.shares > 0 and other_leg.max_price > 0:
                effective_other = other_leg.max_price
            else:
                effective_other = other_target
        price = cap_against_resting(
            my_target, effective_other, other_target, cfg.combined_budget, self._tick
        )
        if price is None:
            return
        if not should_requote(sq.price if sq.order_id else None, price, cfg.requote_drift):
            return

        shares = size_for_price(
            price, cfg.rung_dollar_target, self._min_shares, MIN_ORDER_NOTIONAL_USD
        )
        notional = price * shares
        if not self.capital.can_commit(self.tracker.total_cost(),
                                       self.tracker.resting_notional(), notional):
            return

        # replace: cancel the stale order first so budget math stays honest
        if sq.order_id is not None:
            await self._cancel_order(sq.order_id)
            sq.order_id, sq.price = None, None

        order_id = await self._post_buy(token_id, side, price, shares)
        if order_id:
            sq.order_id, sq.price, sq.last_post = order_id, price, time.time()
            self.tracker.register(order_id, side, token_id, price, shares)
            if self.ledger is not None:
                self.ledger.record_order(order_id, self.market.condition_id, side, price, shares)
            self.backoff.success(f"post:{side}")

    async def _post_buy(self, token_id: str, side: str, price: float, shares: float) -> str | None:
        """The ONLY order entry point in the codebase. BUY, post-only, GTC."""
        assert side in ("UP", "DOWN")
        if self.dry_run:
            oid = f"dry_{side}_{int(price*1000)}_{int(time.time()*1000) % 100000}"
            self.log.info("[dry] rest BUY %s %.0f sh @ %.3f", side, shares, price)
            return oid
        try:
            resp = await self.client.place_limit_order(
                token_id=token_id,
                side="BUY",                # hardcoded by design — see module docstring
                price=str(price),
                size=str(shares),
                post_only=True,            # marketable ⇒ rejected, never taken
            )
        except Exception as exc:  # noqa: BLE001
            self._order_error(side, str(exc))
            return None

        if getattr(resp, "ok", False):
            return str(resp.order_id)
        msg = f"{getattr(resp, 'code', '')} {getattr(resp, 'message', '')}"
        self._order_error(side, msg)
        return None

    def _order_error(self, side: str, msg: str) -> None:
        category, factor = classify_order_error(msg)
        delay = self.backoff.failure(f"post:{side}", factor=factor)
        lvl = logging.WARNING if category in ("constraint", "balance") else logging.INFO
        self.log.log(lvl, "order rejected [%s] %s — backing off %.1fs (%s)",
                     category, side, delay, msg.strip()[:160])
        if category == "post_only_mode":
            # engine restart window: also slow the opposite side
            self.backoff.failure("post:UP", factor=factor)
            self.backoff.failure("post:DOWN", factor=factor)

    # ============================================================ imbalance
    def _apply_imbalance_brake(self) -> None:
        cfg = self.cfg
        up, dn = self.tracker.up.shares, self.tracker.down.shares
        frac = imbalance_fraction(up, dn)
        for side, heavy in (("UP", frac), ("DOWN", -frac)):
            sq = self._sides[side]
            gap = abs(up - dn)
            if not sq.paused and heavy > cfg.imbalance_pause_at and gap > cfg.imbalance_min_gap_shares:
                sq.paused = True
                self.log.info("imbalance brake: pausing %s (%.0f vs %.0f)", side, up, dn)
                asyncio.get_event_loop().create_task(self._cancel_side(side))
            elif sq.paused and heavy < cfg.imbalance_resume_at:
                sq.paused = False
                self.log.info("imbalance brake: resuming %s", side)

    # ============================================================== plumbing
    async def _fetch_books(self) -> tuple[BookSide, BookSide] | None:
        try:
            up_book, down_book = await asyncio.gather(
                self.client.get_order_book(token_id=self.market.up_token_id),
                self.client.get_order_book(token_id=self.market.down_token_id),
            )
        except Exception as exc:  # noqa: BLE001 — includes MARKET_NOT_READY
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

        # authoritative per-market constraints ride along on the book
        self._tick = float(up_book.tick_size or FALLBACK_TICK)
        self._min_shares = float(up_book.min_order_size or FALLBACK_MIN_ORDER_SHARES)
        if self.dry_run:
            u, d = self._simulate_fills(u, d)
        return (u, d)

    def _simulate_fills(self, u: BookSide, d: BookSide) -> tuple[BookSide, BookSide]:
        """Dry-run: each cycle, every resting order has a small chance a
        seller crosses to it. Keeps the whole pipeline exercised end-to-end."""
        for side in ("UP", "DOWN"):
            sq = self._sides[side]
            if sq.order_id and sq.order_id.startswith("dry_") and random.random() < 0.18:
                o = self.tracker.orders.get(sq.order_id)
                if o and o.open:
                    o.open = False
                    o.filled = o.shares
                    tot = self.tracker.up if side == "UP" else self.tracker.down
                    tot.shares += o.shares
                    tot.cost += o.shares * o.price
                    if o.price > tot.max_price:
                        tot.max_price = o.price
                    self.log.info("[dry] FILL %s %.0f sh @ %.3f", side, o.shares, o.price)
                    sq.order_id, sq.price = None, None
        return (u, d)

    async def _cancel_order(self, order_id: str) -> None:
        if self.dry_run or order_id.startswith("dry_"):
            o = self.tracker.orders.get(order_id)
            if o:
                o.open = False
            return
        try:
            await self.client.cancel_order(order_id=order_id)
        except Exception as exc:  # noqa: BLE001 — reconcile() will learn the truth
            self.log.debug("cancel %s failed: %s", order_id[:10], exc)

    async def _cancel_side(self, side: str) -> None:
        for oid, o in list(self.tracker.orders.items()):
            if o.open and o.side == side:
                await self._cancel_order(oid)
        self._sides[side].order_id = None
        self._sides[side].price = None

    async def _cancel_all_safe(self) -> None:
        if self.dry_run:
            for o in self.tracker.orders.values():
                o.open = o.open and False
            return
        try:
            await self.client.cancel_market_orders(market=self.market.condition_id)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("bulk cancel failed (%s); falling back to per-order", exc)
            for oid in self.tracker.open_order_ids():
                await self._cancel_order(oid)
        for sq in self._sides.values():
            sq.order_id, sq.price = None, None

    # ================================================================ finish
    def _finish(self, ts_open: float) -> WindowResult:
        t = self.tracker
        res = WindowResult(
            market=self.market,
            up_shares=t.up.shares, down_shares=t.down.shares,
            up_cost=t.up.cost, down_cost=t.down.cost,
            merged_pairs=0.0,  # session total lives in MergeEngine; per-window in ledger
        )
        combined = t.combined_avg()
        self.log.info(
            "window done | UP %.0f@%.3f DOWN %.0f@%.3f | combined=%s | cost=$%.2f",
            t.up.shares, t.up.avg_price, t.down.shares, t.down.avg_price,
            f"{combined:.4f}" if combined else "n/a", t.total_cost(),
        )
        if self.ledger is not None:
            self.ledger.record_window(
                self.market.condition_id, self.market.slug, ts_open, time.time(),
                t.up.shares, t.down.shares, t.up.cost, t.down.cost, res.merged_pairs,
            )
        return res
