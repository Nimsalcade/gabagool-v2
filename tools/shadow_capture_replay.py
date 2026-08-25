"""Post-close REST capture/replay validator for the Gabagool V3 policy.

Why this exists
---------------
The public CLOB WebSocket can be unavailable on some networks, while the Data API
can index public trades several seconds after they occur.  Applying delayed rows to
a *live* hypothetical order creates a causal problem: by the time the row arrives,
the policy may already have made later decisions without that fill.

This tool avoids that error entirely:

1. It waits for a fresh 5m/15m market when necessary.
2. During the live market it records official CLOB REST order-book snapshots only.
3. After close it waits for the official Data API taker tape to stabilize.
4. It replays the policy chronologically against the captured books + completed tape.

No orders are submitted. No credentials are required. No synthetic random fills are
used. Maker fills remain an estimate because a hypothetical order's exact queue
priority/cancellations cannot be known, but the replay is causal and conservative at
the quoted price.

Example:
    python -m tools.shadow_capture_replay --asset btc --duration 300
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymarket import AsyncPublicClient

from src.config import BotConfig
from src.constants import FALLBACK_MIN_ORDER_SHARES, FALLBACK_TICK, MIN_ORDER_NOTIONAL_USD
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


@dataclass(frozen=True)
class Snapshot:
    ts: float
    up_bids: tuple[tuple[float, float], ...]
    up_asks: tuple[tuple[float, float], ...]
    down_bids: tuple[tuple[float, float], ...]
    down_asks: tuple[tuple[float, float], ...]
    tick: float
    min_shares: float


@dataclass(frozen=True)
class TapeTrade:
    ts: float
    token_id: str
    side: str
    price: float
    size: float
    transaction_hash: str
    occurrence: int = 0


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


def level_size(levels: Iterable[tuple[float, float]], price: float) -> float | None:
    for px, size in levels:
        if abs(px - price) <= 1e-9:
            return size
    return None


def book_side(
    bids: tuple[tuple[float, float], ...], asks: tuple[tuple[float, float], ...]
) -> BookSide | None:
    if not bids or not asks:
        return None
    return BookSide(best_bid=max(p for p, _ in bids), best_ask=min(p for p, _ in asks))


def occurrence_key_rows(raw_rows: list[tuple]) -> list[tuple[tuple, int]]:
    """Preserve genuine identical rows while making repeated full-tape polls comparable.

    Data API Trade rows do not expose a unique trade id.  A Counter-style occurrence
    suffix prevents two genuinely identical fills in one response from being collapsed,
    while the same response on a later poll produces the same identities.
    """
    seen: Counter = Counter()
    out: list[tuple[tuple, int]] = []
    for base in raw_rows:
        idx = seen[base]
        seen[base] += 1
        out.append((base, idx))
    return out


class ReplayEngine:
    def __init__(self, market, cfg, *, max_spend: float):
        self.market = market
        self.cfg = cfg
        self.max_spend = float(max_spend)
        self.up = Totals()
        self.down = Totals()
        self.orders: dict[str, ShadowOrder | None] = {"UP": None, "DOWN": None}
        self.first_fill_ts: float | None = None
        self.last_fill_ts: float | None = None
        self.taker_cooldown = {"UP": 0.0, "DOWN": 0.0}
        self.current_ts = float(market.window_start)
        self.tick = FALLBACK_TICK
        self.min_shares = FALLBACK_MIN_ORDER_SHARES
        self.quote_events = 0
        self.requote_events = 0
        self.tape_sell_events_applied = 0

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
            now_ts=self.current_ts,
            window_start_ts=float(self.market.window_start),
            seconds_to_end=max(0.0, float(self.market.window_end) - self.current_ts),
        )

    def can_spend(self, dollars: float) -> bool:
        return dollars > 0 and self.spend + dollars <= self.max_spend + 1e-9

    def record_fill(self, side: str, qty: float, price: float, mode: str, ts: float) -> None:
        if qty <= 0:
            return
        t = self.up if side == "UP" else self.down
        t.shares += qty
        t.cost += qty * price
        t.last_fill_ts = ts
        t.fill_events += 1
        if mode == "maker":
            t.maker_fill_events += 1
        else:
            t.taker_fill_events += 1
        t.prices.add(round(price, 6))
        self.first_fill_ts = ts if self.first_fill_ts is None else min(self.first_fill_ts, ts)
        self.last_fill_ts = ts if self.last_fill_ts is None else max(self.last_fill_ts, ts)
        print(
            f"REPLAY FILL {mode.upper():5s} {side:4s} {qty:7.3f} @ {price:.4f} "
            f"age={ts-self.market.window_start:6.1f}s | "
            f"UP {self.up.shares:.1f}@{self.up.vwap:.4f} "
            f"DOWN {self.down.shares:.1f}@{self.down.vwap:.4f}"
        )

    def apply_trade(self, tr: TapeTrade) -> None:
        if tr.side != "SELL":
            return
        side = (
            "UP" if tr.token_id == self.market.up_token_id
            else "DOWN" if tr.token_id == self.market.down_token_id
            else None
        )
        if side is None:
            return
        order = self.orders.get(side)
        if order is None:
            return
        # Second-resolution Data API timestamps can tie with a quote.  Require the
        # tape event not to precede the hypothetical post by more than one second;
        # otherwise the trade cannot causally fill this order.
        if tr.ts + 1.0 < order.posted_ts:
            return
        qty = apply_sell_trade(order, trade_price=tr.price, trade_size=tr.size)
        self.tape_sell_events_applied += 1
        if qty > 0:
            self.record_fill(side, qty, order.price, "maker", tr.ts)
        if order.done:
            self.orders[side] = None

    def reduce_queues(self, snap: Snapshot) -> None:
        for side, bids in (("UP", snap.up_bids), ("DOWN", snap.down_bids)):
            order = self.orders.get(side)
            if order is None:
                continue
            reduce_queue_from_book(order, visible_size_at_price=level_size(bids, order.price))

    def maybe_taker(self, snap: Snapshot) -> bool:
        state = self.state()
        if state.up_shares <= 0 and state.down_shares <= 0:
            return False
        ubook = book_side(snap.up_bids, snap.up_asks)
        dbook = book_side(snap.down_bids, snap.down_asks)
        if ubook is None or dbook is None:
            return False
        deficient = state.deficient_side
        candidates = [deficient] if deficient else ["UP", "DOWN"]
        for side in candidates:
            if side is None or self.current_ts < self.taker_cooldown[side]:
                continue
            book = ubook if side == "UP" else dbook
            asks = snap.up_asks if side == "UP" else snap.down_asks
            relation = relation_for_side(state, side)
            if relation == "heavy" and not (
                state.combined_vwap is not None
                and state.combined_vwap <= self.cfg.target_combined_vwap - 0.01
            ):
                continue
            max_price = tick_floor(book.best_ask, self.tick)
            if max_price <= 0 or max_price >= 1:
                continue
            planned = adaptive_clip(
                base_clip_shares=self.cfg.base_clip_shares,
                max_clip_shares=self.cfg.max_clip_shares,
                min_order_shares=self.min_shares,
                min_notional=MIN_ORDER_NOTIONAL_USD,
                price=max_price,
                ratio=state.larger_to_smaller_ratio,
                relation=relation,
                aggressive=True,
            )
            projected = projected_combined_vwap(
                state, side=side, price=max_price, shares=planned
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
            other_book = dbook if side == "UP" else ubook
            if not basis_allows(
                state,
                side=side,
                price=max_price,
                shares=planned,
                max_combined_vwap=self.cfg.max_combined_vwap,
                opposite_reference_price=other_book.best_bid,
                initial_pair_ceiling=self.cfg.initial_pair_ceiling,
            ):
                continue
            displayed = level_size(asks, book.best_ask) or 0.0
            qty = min(planned, displayed)
            if qty <= 0 or not self.can_spend(max_price * qty):
                continue
            self.orders[side] = None
            self.record_fill(side, qty, max_price, "taker", self.current_ts)
            self.taker_cooldown[side] = self.current_ts + 2.0
            return True
        return False

    def manage_maker(
        self,
        side: str,
        token_id: str,
        book: BookSide,
        bids: tuple[tuple[float, float], ...],
        other_target: float,
    ) -> None:
        state = self.state()
        relation = relation_for_side(state, side)
        ratio = state.larger_to_smaller_ratio
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
            state,
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
        queue = level_size(bids, price)
        if queue is None:
            queue = 0.0 if price > book.best_bid + 1e-9 else float("inf")
        if old is not None:
            self.requote_events += 1
        self.quote_events += 1
        self.orders[side] = ShadowOrder(
            side=side,
            token_id=token_id,
            price=price,
            shares=qty,
            queue_ahead=queue,
            posted_ts=self.current_ts,
        )

    def on_snapshot(self, snap: Snapshot) -> None:
        self.current_ts = snap.ts
        self.tick = snap.tick
        self.min_shares = snap.min_shares
        self.reduce_queues(snap)

        entry_delay = float(self.cfg.entry_delay_by_duration_s.get(self.market.duration_s, 10.0))
        age = self.current_ts - self.market.window_start
        remaining = self.market.window_end - self.current_ts
        if age < entry_delay:
            return
        if remaining <= self.cfg.stop_posting_buffer_s:
            self.orders = {"UP": None, "DOWN": None}
            return

        ubook = book_side(snap.up_bids, snap.up_asks)
        dbook = book_side(snap.down_bids, snap.down_asks)
        if ubook is None or dbook is None:
            return

        if self.cfg.taker_enabled and self.maybe_taker(snap):
            return

        state = self.state()
        ut = maker_target(
            ubook,
            tick=self.tick,
            inventory_relation=relation_for_side(state, "UP"),
            ratio=state.larger_to_smaller_ratio,
        )
        dt = maker_target(
            dbook,
            tick=self.tick,
            inventory_relation=relation_for_side(state, "DOWN"),
            ratio=state.larger_to_smaller_ratio,
        )
        if ut is None or dt is None:
            return
        self.manage_maker("UP", self.market.up_token_id, ubook, snap.up_bids, dt)
        self.manage_maker("DOWN", self.market.down_token_id, dbook, snap.down_bids, ut)

    def replay(self, snapshots: list[Snapshot], tape: list[TapeTrade]) -> None:
        tape = sorted(tape, key=lambda x: (x.ts, x.transaction_hash, x.occurrence))
        idx = 0
        for snap in snapshots:
            # Apply prints that happened since the previous snapshot before making
            # the next policy decision. Data timestamps are second-resolution.
            while idx < len(tape) and tape[idx].ts <= snap.ts + 1e-9:
                self.current_ts = max(self.current_ts, tape[idx].ts)
                self.apply_trade(tape[idx])
                idx += 1
            self.on_snapshot(snap)

        # Consume any final in-window tape prints after the last captured snapshot.
        while idx < len(tape) and tape[idx].ts <= self.market.window_end:
            self.current_ts = max(self.current_ts, tape[idx].ts)
            self.apply_trade(tape[idx])
            idx += 1

    def report(self, *, snapshots: int, book_errors: int, tape_total: int, tape_sells: int) -> None:
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

        print("\n================ CAPTURE/REPLAY RESULT ================")
        print("market:", self.market.slug)
        print("source: CLOB_REST_BOOKS + DATA_API_POSTCLOSE_TAKER_TAPE")
        print(f"book_snapshots={snapshots} book_errors={book_errors}")
        print(f"official_taker_rows={tape_total} official_taker_sell_rows={tape_sells}")
        print(f"quote_events={self.quote_events} requote_events={self.requote_events}")
        print(
            "fill events:", fills,
            "maker:", maker,
            "taker:", taker,
            "taker_share:", f"{(100*taker/fills if fills else 0):.2f}%",
        )
        print(
            f"UP   shares={self.up.shares:.6f} spend=${self.up.cost:.6f} "
            f"vwap={self.up.vwap:.6f} levels={len(self.up.prices)}"
        )
        print(
            f"DOWN shares={self.down.shares:.6f} spend=${self.down.cost:.6f} "
            f"vwap={self.down.vwap:.6f} levels={len(self.down.prices)}"
        )
        print(f"total spend=${self.spend:.6f}")
        print("combined_vwap:", "n/a" if combined is None else f"{combined:.6f}")
        print("terminal_ratio:", "inf" if math.isinf(ratio) else f"{ratio:.6f}")
        print(f"matched_pairs={matched:.6f} gross_matched_edge=${edge:.6f}")
        print("first_fill_age_s:", "n/a" if first_age is None else f"{first_age:.1f}")
        print("last_fill_age_s:", "n/a" if last_age is None else f"{last_age:.1f}")
        valid = snapshots >= 100 and tape_total >= 10
        print("validation_quality:", "USABLE" if valid else "INSUFFICIENT_PUBLIC_TAPE")
        print(
            "estimator_note: exact hypothetical queue priority is unknowable; equal-price "
            "maker fills require visible queue ahead to be consumed, while lower-price "
            "SELL prints imply price-priority fill."
        )
        print("=======================================================")


async def fetch_snapshot(client, market) -> Snapshot | None:
    ub, db = await asyncio.gather(
        client.get_order_book(token_id=market.up_token_id),
        client.get_order_book(token_id=market.down_token_id),
    )
    up_bids = tuple((float(x.price), float(x.size)) for x in (ub.bids or []))
    up_asks = tuple((float(x.price), float(x.size)) for x in (ub.asks or []))
    dn_bids = tuple((float(x.price), float(x.size)) for x in (db.bids or []))
    dn_asks = tuple((float(x.price), float(x.size)) for x in (db.asks or []))
    if not up_bids or not up_asks or not dn_bids or not dn_asks:
        return None
    tick = float(getattr(ub, "tick_size", None) or FALLBACK_TICK)
    min_shares = float(getattr(ub, "min_order_size", None) or FALLBACK_MIN_ORDER_SHARES)
    return Snapshot(
        ts=time.time(),
        up_bids=up_bids,
        up_asks=up_asks,
        down_bids=dn_bids,
        down_asks=dn_asks,
        tick=tick,
        min_shares=min_shares,
    )


async def capture_books(client, market, *, interval: float) -> tuple[list[Snapshot], int]:
    snapshots: list[Snapshot] = []
    errors = 0
    print(
        f"CAPTURE MARKET: {market.slug}\n"
        f"window: {market.window_start} -> {market.window_end}\n"
        f"sampling official CLOB REST books every {interval:.2f}s; ZERO orders submitted"
    )
    next_print = 0.0
    while market.seconds_to_end > 0:
        try:
            snap = await fetch_snapshot(client, market)
            if snap is not None:
                snapshots.append(snap)
        except Exception as exc:  # noqa: BLE001
            errors += 1
            if errors <= 5 or errors % 10 == 0:
                print(f"book capture warning #{errors}: {type(exc).__name__}: {exc}")
        now = time.time()
        if now >= next_print:
            print(
                f"capture: age={market.age_seconds:6.1f}s remaining={market.seconds_to_end:6.1f}s "
                f"snapshots={len(snapshots)} errors={errors}"
            )
            next_print = now + 30.0
        await asyncio.sleep(max(0.05, interval))
    print(f"capture complete: {len(snapshots)} snapshots, {errors} errors")
    return snapshots, errors


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
        return x / 1000.0 if x > 10_000_000_000 else x
    except Exception:
        return None


async def fetch_taker_tape_once(client, market) -> list[TapeTrade]:
    paginator = client.list_trades(
        market=[market.condition_id],
        taker_only=True,
        start=int(market.window_start),
        end=int(market.window_end + 1),
        page_size=10_000,
    )
    raw: list[tuple] = []
    objects = []
    async for page in paginator:
        for tr in page.items:
            ts = _trade_ts(tr)
            if ts is None:
                continue
            token = str(getattr(tr, "token_id", "") or "")
            if token not in (market.up_token_id, market.down_token_id):
                continue
            side = str(getattr(tr, "side", "") or "").upper()
            try:
                price = float(tr.price)
                size = float(tr.size or 0)
            except Exception:
                continue
            tx = str(getattr(tr, "transaction_hash", "") or "")
            wallet = str(getattr(tr, "wallet", "") or "")
            base = (round(ts, 3), token, side, round(price, 9), round(size, 9), tx, wallet)
            raw.append(base)
            objects.append((ts, token, side, price, size, tx))

    identities = occurrence_key_rows(raw)
    out = []
    for (_, occurrence), obj in zip(identities, objects, strict=True):
        ts, token, side, price, size, tx = obj
        out.append(
            TapeTrade(
                ts=ts,
                token_id=token,
                side=side,
                price=price,
                size=size,
                transaction_hash=tx,
                occurrence=occurrence,
            )
        )
    out.sort(key=lambda x: (x.ts, x.transaction_hash, x.occurrence))
    return out


def tape_signature(rows: list[TapeTrade]) -> tuple:
    if not rows:
        return (0, 0.0, 0.0, 0)
    return (
        len(rows),
        round(max(r.ts for r in rows), 3),
        round(sum(r.size for r in rows), 6),
        sum(1 for r in rows if r.side == "SELL"),
    )


async def wait_for_stable_tape(
    client,
    market,
    *,
    timeout: float,
    poll_interval: float,
    stable_polls: int,
) -> list[TapeTrade]:
    print("post-close: waiting for official Data API taker tape to stabilize…")
    deadline = time.time() + timeout
    last_sig = None
    stable = 0
    best: list[TapeTrade] = []
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            rows = await fetch_taker_tape_once(client, market)
        except Exception as exc:  # noqa: BLE001
            print(f"tape poll {attempt}: {type(exc).__name__}: {exc}")
            await asyncio.sleep(poll_interval)
            continue
        sig = tape_signature(rows)
        if len(rows) > len(best):
            best = rows
        if sig == last_sig and sig[0] > 0:
            stable += 1
        else:
            stable = 0
            last_sig = sig
        print(
            f"tape poll {attempt}: rows={sig[0]} sells={sig[3]} "
            f"latest_ts={sig[1]:.3f} stable={stable}/{stable_polls}"
        )
        if stable >= stable_polls:
            return rows
        await asyncio.sleep(poll_interval)
    print(
        f"tape stabilization timeout after {timeout:.0f}s; using largest observed "
        f"response ({len(best)} rows)"
    )
    return best


async def choose_fresh_market(client, asset: str, duration: int, *, max_start_age: float):
    while True:
        markets = await discover(client, (asset,), (duration,))
        candidates = [m for m in markets if m.seconds_to_end > 20]
        if candidates:
            m = candidates[0]
            if m.age_seconds <= max_start_age:
                return m
            wait = max(1.0, m.seconds_to_end + 2.0)
            print(
                f"current {duration//60}m market is already {m.age_seconds:.1f}s old; "
                f"waiting {wait:.1f}s for a fresh window"
            )
            await asyncio.sleep(wait)
            continue
        # Discovery can briefly lag immediately around a boundary.
        print("fresh market not visible yet; retrying in 2s")
        await asyncio.sleep(2.0)


async def amain(args) -> int:
    cfg = BotConfig.load("config/default.yaml")
    client = AsyncPublicClient()
    try:
        market = await choose_fresh_market(
            client,
            args.asset,
            args.duration,
            max_start_age=args.max_start_age,
        )
        snapshots, book_errors = await capture_books(
            client,
            market,
            interval=args.snapshot_interval,
        )
        tape = await wait_for_stable_tape(
            client,
            market,
            timeout=args.tape_timeout,
            poll_interval=args.tape_poll_interval,
            stable_polls=args.stable_polls,
        )
        sells = [t for t in tape if t.side == "SELL"]
        engine = ReplayEngine(market, cfg.strategy, max_spend=args.max_spend)
        print(
            f"replay starting: {len(snapshots)} book snapshots + {len(tape)} official "
            f"taker rows ({len(sells)} SELL aggressors)"
        )
        engine.replay(snapshots, tape)
        engine.report(
            snapshots=len(snapshots),
            book_errors=book_errors,
            tape_total=len(tape),
            tape_sells=len(sells),
        )
        return 0
    finally:
        await client.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="zero-money post-close CLOB-book + official-tape capture/replay validator"
    )
    ap.add_argument("--asset", choices=("btc", "eth", "sol", "xrp"), default="btc")
    ap.add_argument("--duration", type=int, choices=(300, 900), default=300)
    ap.add_argument("--max-spend", type=float, default=2000.0)
    ap.add_argument("--snapshot-interval", type=float, default=1.0)
    ap.add_argument("--max-start-age", type=float, default=12.0)
    ap.add_argument("--tape-timeout", type=float, default=90.0)
    ap.add_argument("--tape-poll-interval", type=float, default=3.0)
    ap.add_argument("--stable-polls", type=int, default=4)
    args = ap.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
