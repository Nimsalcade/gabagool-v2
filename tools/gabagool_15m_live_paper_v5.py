"""Read-only live paper runner for the reconstructed Gabagool 15-minute policy.

This tool consumes REAL current Polymarket BTC/ETH 15-minute order books but NEVER
creates, signs, cancels, merges, or redeems a real order/transaction.

It implements the maximum-identifiable 15m policy in :mod:`src.forensic_15m`:

* BTC/ETH 15-minute Up/Down markets.
* quote from age ~18s through ~899s;
* BUY-only maker exposure on both outcomes;
* 10 -> 9 -> 8 -> 7 -> 6 -> 5 parent clips by market age;
* passive base bid = 1 - opposite displayed ask, capped post-only;
* four underweight layers and 4/3/2/1/0 heavy-side layers by gap-in-clips;
* 8-clip emergency inventory bound;
* no intrawindow paper merge; matched complete sets settle after close;
* residual winning shares redeem at resolution.

Execution model
---------------
Public fills cannot reveal the reference wallet's exact queue position or cancel
lifecycle. This runner therefore uses a deliberately conservative maker-fill proxy:
a hypothetical bid must already be resting, and a LATER real order-book snapshot
must display enough ask liquidity at or below that bid to fill the entire remaining
paper order. Visible liquidity is consumed once per snapshot across our paper orders.

Cancel/requote TTL is an IMPLEMENTATION SURFACE, not a claimed Gabagool constant.
The default is 10 seconds. When the desired heavy-side layer count falls, existing
paper layers are allowed to live until TTL; they are not mass-cancelled immediately.
That reproduces the reconstruction's observed soft overshoot without pretending we
know the private cancellation schedule.

Aggressive repair
-----------------
The completed full-history decode found a maker-dominant execution mix with a
minority of aggressive fills. ``--taker-mode evidence`` enables the repository's
existing evidence-calibrated deficient-leg repair gate. The existence and monotonic
relationships are evidence-backed; the exact hidden trigger is not identifiable.
Use ``--taker-mode off`` to measure the pure 15m maker reconstruction separately.

Example
-------
    python -m tools.gabagool_15m_live_paper_v5 \
        --assets btc,eth --sessions 1 --paper-cash 500

Outputs CSV event logs plus a JSON session summary under ``data/``.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from src.forensic_15m import (
    DURATION_S,
    QUOTE_END_AGE_S,
    QUOTE_START_AGE_S,
    Inventory as PolicyInventory,
    acquisition_spend,
    clip_for_age,
    complementary_base_bid,
    conservative_floor_pnl,
    desired_layer_count,
    hard_gap_allows,
    layer_prices,
    settlement_pnl,
    settlement_value,
)
from src.policy import (
    InventoryState as RepairInventoryState,
    projected_combined_vwap as repair_projected_combined_vwap,
    taker_should_fire,
)
from tools.metamask_10session_strategy_observer import _best, _books, _levels


DEFAULT_ASSETS = ("btc", "eth")
DEFAULT_POLL_S = 0.50
DEFAULT_QUOTE_TTL_S = 10.0
DEFAULT_PAPER_CASH = 500.0
DEFAULT_MAX_COMBINED_VWAP = 1.01
DEFAULT_RESOLUTION_TIMEOUT_S = 240.0
DEFAULT_OUTPUT_PREFIX = "data/gabagool_15m_live_v5"

EVENT_FIELDS = [
    "utc", "session", "asset", "market", "age_s", "event", "side", "order_id",
    "qty", "price", "cost", "reason", "cash", "reserved", "up_shares", "up_vwap",
    "down_shares", "down_vwap", "combined_vwap", "gap_shares", "clip", "up_layers",
    "down_layers", "maker_fills", "taker_fills",
]


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc).isoformat()


def _tick(book: Any) -> float:
    try:
        return float(getattr(book, "tick_size", None) or 0.01)
    except (TypeError, ValueError):
        return 0.01


def _ask(book: Any) -> tuple[float, float] | None:
    best = _best(book, "asks")
    if best is None:
        return None
    return float(best[0]), float(best[1])


def _asks(book: Any) -> list[tuple[float, float]]:
    return [(float(p), float(s)) for p, s in _levels(book, "asks")]


@dataclass
class PaperOrder:
    order_id: str
    side: str
    price: float
    shares: float
    created_ts: float
    expires_ts: float


@dataclass
class MutableInventory:
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    last_up_fill_ts: float | None = None
    last_down_fill_ts: float | None = None

    def policy(self) -> PolicyInventory:
        return PolicyInventory(
            up_shares=self.up_shares,
            down_shares=self.down_shares,
            up_cost=self.up_cost,
            down_cost=self.down_cost,
        )

    def add(self, side: str, shares: float, price: float, now: float) -> None:
        if side == "UP":
            self.up_shares += shares
            self.up_cost += shares * price
            self.last_up_fill_ts = now
        elif side == "DOWN":
            self.down_shares += shares
            self.down_cost += shares * price
            self.last_down_fill_ts = now
        else:
            raise ValueError("side must be UP or DOWN")

    def shares(self, side: str) -> float:
        return self.up_shares if side == "UP" else self.down_shares

    def vwap(self, side: str) -> float | None:
        q = self.shares(side)
        if q <= 0:
            return None
        cost = self.up_cost if side == "UP" else self.down_cost
        return cost / q

    @property
    def underweight(self) -> str | None:
        if abs(self.up_shares - self.down_shares) <= 1e-9:
            return None
        return "UP" if self.up_shares < self.down_shares else "DOWN"

    @property
    def deficit(self) -> float:
        return abs(self.up_shares - self.down_shares)

    @property
    def ratio(self) -> float:
        lo = min(self.up_shares, self.down_shares)
        hi = max(self.up_shares, self.down_shares)
        if hi <= 0:
            return 1.0
        if lo <= 0:
            return math.inf
        return hi / lo


@dataclass
class MarketStats:
    asset: str
    market: str
    condition_id: str
    first_fill_age_s: float | None = None
    last_fill_age_s: float | None = None
    maker_fills: int = 0
    taker_fills: int = 0
    quote_posts: int = 0
    quote_expiries: int = 0
    max_gap_shares: float = 0.0
    max_gap_clips: float = 0.0
    winner: str | None = None
    resolution_found: bool = False
    settlement_value: float | None = None
    settlement_pnl: float | None = None
    conservative_floor_pnl: float | None = None


class PaperMarket:
    def __init__(
        self,
        *,
        session_no: int,
        market: Any,
        writer: csv.DictWriter,
        paper_cash: float,
        poll_s: float,
        quote_ttl_s: float,
        taker_mode: str,
        max_combined_vwap: float,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.writer = writer
        self.starting_cash = float(paper_cash)
        self.cash = float(paper_cash)
        self.poll_s = float(poll_s)
        self.quote_ttl_s = float(quote_ttl_s)
        self.taker_mode = taker_mode
        self.max_combined_vwap = float(max_combined_vwap)
        self.inv = MutableInventory()
        self.orders: dict[str, PaperOrder] = {}
        self._next_oid = 1
        self.stats = MarketStats(
            asset=market.asset,
            market=market.slug,
            condition_id=market.condition_id,
        )
        self._last_clip = 0.0

    def _reserved(self) -> float:
        return sum(o.price * o.shares for o in self.orders.values())

    def _free_cash(self) -> float:
        return self.cash - self._reserved()

    def _layer_counts(self, clip: float) -> tuple[int, int]:
        p = self.inv.policy()
        return (
            desired_layer_count(p, "UP", clip),
            desired_layer_count(p, "DOWN", clip),
        )

    def _event(
        self,
        *,
        now: float,
        event: str,
        side: str = "",
        order_id: str = "",
        qty: float | None = None,
        price: float | None = None,
        cost: float | None = None,
        reason: str = "",
        clip: float | None = None,
    ) -> None:
        p = self.inv.policy()
        c = float(clip if clip is not None else self._last_clip)
        up_layers, down_layers = self._layer_counts(c) if c > 0 else (0, 0)
        self.writer.writerow({
            "utc": _iso(now),
            "session": self.session_no,
            "asset": self.market.asset,
            "market": self.market.slug,
            "age_s": f"{now - self.market.window_start:.3f}",
            "event": event,
            "side": side,
            "order_id": order_id,
            "qty": "" if qty is None else f"{qty:.9f}",
            "price": "" if price is None else f"{price:.9f}",
            "cost": "" if cost is None else f"{cost:.9f}",
            "reason": reason,
            "cash": f"{self.cash:.9f}",
            "reserved": f"{self._reserved():.9f}",
            "up_shares": f"{p.up_shares:.9f}",
            "up_vwap": "" if p.up_vwap is None else f"{p.up_vwap:.9f}",
            "down_shares": f"{p.down_shares:.9f}",
            "down_vwap": "" if p.down_vwap is None else f"{p.down_vwap:.9f}",
            "combined_vwap": "" if p.combined_vwap is None else f"{p.combined_vwap:.9f}",
            "gap_shares": f"{p.abs_gap:.9f}",
            "clip": f"{c:.3f}",
            "up_layers": up_layers,
            "down_layers": down_layers,
            "maker_fills": self.stats.maker_fills,
            "taker_fills": self.stats.taker_fills,
        })

    def _new_order(self, side: str, price: float, shares: float, now: float) -> None:
        need = price * shares
        if need > self._free_cash() + 1e-9:
            return
        oid = f"P{self.session_no}-{self.market.asset.upper()}-{self._next_oid}"
        self._next_oid += 1
        self.orders[oid] = PaperOrder(
            order_id=oid,
            side=side,
            price=price,
            shares=shares,
            created_ts=now,
            expires_ts=now + self.quote_ttl_s,
        )
        self.stats.quote_posts += 1
        self._event(
            now=now,
            event="QUOTE",
            side=side,
            order_id=oid,
            qty=shares,
            price=price,
            cost=need,
            reason="forensic complementary-ask maker layer",
            clip=shares,
        )

    def _expire_orders(self, now: float) -> None:
        for oid, order in list(self.orders.items()):
            if now < order.expires_ts:
                continue
            self.orders.pop(oid, None)
            self.stats.quote_expiries += 1
            self._event(
                now=now,
                event="EXPIRE",
                side=order.side,
                order_id=oid,
                qty=order.shares,
                price=order.price,
                reason=f"paper quote TTL {self.quote_ttl_s:.1f}s (unobservable historical surface)",
                clip=order.shares,
            )

    def _record_fill(
        self,
        *,
        now: float,
        side: str,
        shares: float,
        price: float,
        kind: str,
        order_id: str = "",
        reason: str,
    ) -> bool:
        cost = shares * price
        if cost > self.cash + 1e-9:
            return False
        self.cash -= cost
        self.inv.add(side, shares, price, now)
        age = now - self.market.window_start
        if self.stats.first_fill_age_s is None:
            self.stats.first_fill_age_s = age
        self.stats.last_fill_age_s = age
        if kind == "MAKER_FILL":
            self.stats.maker_fills += 1
        else:
            self.stats.taker_fills += 1
        clip = max(self._last_clip, 1e-9)
        gap = self.inv.policy().abs_gap
        self.stats.max_gap_shares = max(self.stats.max_gap_shares, gap)
        self.stats.max_gap_clips = max(self.stats.max_gap_clips, gap / clip)
        self._event(
            now=now,
            event=kind,
            side=side,
            order_id=order_id,
            qty=shares,
            price=price,
            cost=cost,
            reason=reason,
            clip=self._last_clip,
        )
        print(
            f"{self.market.asset.upper():3} {kind:10} {side:4} {shares:5.1f}@{price:.3f} "
            f"U={self.inv.up_shares:.1f}@{(self.inv.vwap('UP') or 0):.4f} "
            f"D={self.inv.down_shares:.1f}@{(self.inv.vwap('DOWN') or 0):.4f} "
            f"basis={(self.inv.policy().combined_vwap or 0):.4f} cash=${self.cash:.2f}"
        )
        return True

    def _strict_maker_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        """Consume one real later ask snapshot across hypothetical resting bids."""
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            visible = _asks(book)
            if not visible:
                continue
            # Highest bid receives executable visible liquidity first.
            orders = sorted(
                (o for o in self.orders.values() if o.side == side and now > o.created_ts + 1e-6),
                key=lambda o: (-o.price, o.created_ts),
            )
            # Mutable visible ask quantities for this ONE snapshot. This prevents
            # the same displayed size from filling several hypothetical orders.
            levels = [[p, q] for p, q in visible]
            for order in orders:
                if not hard_gap_allows(
                    self.inv.policy(),
                    side=side,
                    shares=order.shares,
                    parent_clip=max(self._last_clip, order.shares),
                ):
                    self.orders.pop(order.order_id, None)
                    self._event(
                        now=now,
                        event="CANCEL_SAFETY",
                        side=side,
                        order_id=order.order_id,
                        qty=order.shares,
                        price=order.price,
                        reason="8-clip hard safety gap",
                        clip=self._last_clip,
                    )
                    continue

                available = sum(q for p, q in levels if p <= order.price + 1e-12)
                if available + 1e-9 < order.shares:
                    continue  # full-order-only conservative proxy

                need = order.shares
                for level in levels:
                    p, q = level
                    if p > order.price + 1e-12 or need <= 1e-12:
                        continue
                    take = min(q, need)
                    level[1] -= take
                    need -= take
                if need > 1e-8:
                    continue

                self.orders.pop(order.order_id, None)
                self._record_fill(
                    now=now,
                    side=side,
                    shares=order.shares,
                    price=order.price,
                    kind="MAKER_FILL",
                    order_id=order.order_id,
                    reason=f"later real ask snapshot fully crossed resting bid after {now-order.created_ts:.2f}s",
                )

    def _desired_prices(self, up_book: Any, down_book: Any, clip: float) -> dict[str, tuple[float, ...]]:
        ua, da = _ask(up_book), _ask(down_book)
        if ua is None or da is None:
            return {"UP": (), "DOWN": ()}
        up_tick, dn_tick = _tick(up_book), _tick(down_book)
        p = self.inv.policy()
        up_base = complementary_base_bid(
            own_best_ask=ua[0], opposite_best_ask=da[0], tick=up_tick
        )
        dn_base = complementary_base_bid(
            own_best_ask=da[0], opposite_best_ask=ua[0], tick=dn_tick
        )
        return {
            "UP": layer_prices(
                up_base,
                tick=up_tick,
                layers=desired_layer_count(p, "UP", clip),
            ),
            "DOWN": layer_prices(
                dn_base,
                tick=dn_tick,
                layers=desired_layer_count(p, "DOWN", clip),
            ),
        }

    def _renew_layers(self, now: float, up_book: Any, down_book: Any, clip: float) -> None:
        desired = self._desired_prices(up_book, down_book, clip)
        # Critical forensic behavior: when heavy-side target layers fall, already
        # resting layers are NOT immediately mass-cancelled here. They naturally
        # expire on the explicitly configurable TTL. We only stop renewing them.
        for side in ("UP", "DOWN"):
            want = desired[side]
            target_n = len(want)
            active = [o for o in self.orders.values() if o.side == side]
            if len(active) >= target_n:
                continue
            occupied = {round(o.price, 10) for o in active}
            for px in want:
                if len([o for o in self.orders.values() if o.side == side]) >= target_n:
                    break
                if round(px, 10) in occupied:
                    continue
                if not hard_gap_allows(
                    self.inv.policy(), side=side, shares=clip, parent_clip=clip
                ):
                    break
                self._new_order(side, px, clip, now)
                occupied.add(round(px, 10))

    def _repair_state(self, now: float) -> RepairInventoryState:
        return RepairInventoryState(
            up_shares=self.inv.up_shares,
            down_shares=self.inv.down_shares,
            up_cost=self.inv.up_cost,
            down_cost=self.inv.down_cost,
            last_up_fill_ts=self.inv.last_up_fill_ts,
            last_down_fill_ts=self.inv.last_down_fill_ts,
            now_ts=now,
            window_start_ts=float(self.market.window_start),
            seconds_to_end=float(self.market.window_end - now),
        )

    def _maybe_evidence_taker(self, now: float, up_book: Any, down_book: Any, clip: float) -> None:
        if self.taker_mode != "evidence":
            return
        under = self.inv.underweight
        if under is None:
            return
        deficit = self.inv.deficit
        # Historical aggressive fills were typically somewhat larger than maker
        # clips. Cap at two current parent clips AND exact live deficit.
        planned = min(deficit, max(clip, 2.0 * clip))
        if planned + 1e-9 < clip:
            return
        book = up_book if under == "UP" else down_book
        asks = _asks(book)
        remaining = planned
        cost = 0.0
        worst = 0.0
        for price, qty in asks:
            if remaining <= 1e-12:
                break
            take = min(qty, remaining)
            if take > 0:
                cost += take * price
                remaining -= take
                worst = price
        if remaining > 1e-8 or planned <= 0:
            return
        vwap = cost / planned
        state = self._repair_state(now)
        projected = repair_projected_combined_vwap(
            state, side=under, price=vwap, shares=planned
        )
        if not taker_should_fire(
            state,
            candidate_side=under,
            projected_basis=projected,
            target_combined_vwap=0.985,
            max_combined_vwap=self.max_combined_vwap,
            taker_stop_buffer_s=2.0,
        ):
            return
        if cost > self._free_cash() + 1e-9:
            return
        # Cancel paper maker orders on the deficient side before immediate repair
        # so reserved cash is not double counted.
        for oid, o in list(self.orders.items()):
            if o.side == under:
                self.orders.pop(oid, None)
                self._event(
                    now=now,
                    event="CANCEL_FOR_REPAIR",
                    side=under,
                    order_id=oid,
                    qty=o.shares,
                    price=o.price,
                    reason="evidence-calibrated deficient-leg aggressive repair",
                    clip=clip,
                )
        self._record_fill(
            now=now,
            side=under,
            shares=planned,
            price=vwap,
            kind="TAKER_FILL",
            reason=(
                "evidence-calibrated repair; exact historical trigger unidentifiable | "
                f"ratio={self.inv.ratio:.4f} deficit={deficit:.2f} projected={projected}"
            ),
        )

    async def run(self, client: AsyncPublicClient) -> MarketStats:
        print("\n" + "=" * 100)
        print(f"PAPER {self.market.asset.upper()} | {self.market.slug}")
        print(f"WINDOW {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")
        print(f"MODE   real books / conservative maker proxy / taker={self.taker_mode}")
        self._event(now=time.time(), event="SESSION_START", reason="read-only live 15m forensic paper")

        while time.time() < self.market.window_end:
            cycle = time.monotonic()
            now = time.time()
            age = now - self.market.window_start
            clip = clip_for_age(age)
            self._last_clip = clip

            try:
                up_book, down_book = await _books(
                    client, self.market.up_token_id, self.market.down_token_id
                )
            except Exception as exc:  # noqa: BLE001
                self._event(now=now, event="READ_ERROR", reason=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(min(1.0, self.poll_s))
                continue

            self._expire_orders(now)
            self._strict_maker_fills(now, up_book, down_book)

            if QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S and clip > 0:
                self._maybe_evidence_taker(now, up_book, down_book, clip)
                self._renew_layers(now, up_book, down_book, clip)

            elapsed = time.monotonic() - cycle
            await asyncio.sleep(max(0.0, self.poll_s - elapsed))

        now = time.time()
        for oid, order in list(self.orders.items()):
            self.orders.pop(oid, None)
            self._event(
                now=now,
                event="CANCEL_CLOSE",
                side=order.side,
                order_id=oid,
                qty=order.shares,
                price=order.price,
                reason="market closed; settlement is post-close",
                clip=self._last_clip,
            )

        self.stats.conservative_floor_pnl = conservative_floor_pnl(self.inv.policy())
        self._event(
            now=now,
            event="WINDOW_CLOSE",
            reason=f"matched={self.inv.policy().matched:.6f} floorPnL={self.stats.conservative_floor_pnl:.6f}",
        )
        return self.stats


async def _winner_from_gamma(slug: str, timeout_s: float) -> str | None:
    deadline = time.time() + timeout_s
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    async with httpx.AsyncClient(timeout=10.0) as hc:
        while time.time() < deadline:
            try:
                r = await hc.get(url)
                if r.status_code == 200:
                    m = r.json()
                    outcomes = m.get("outcomes", [])
                    prices = m.get("outcomePrices", [])
                    if isinstance(outcomes, str):
                        outcomes = json.loads(outcomes)
                    if isinstance(prices, str):
                        prices = json.loads(prices)
                    vals = []
                    for i, raw in enumerate(prices):
                        try:
                            vals.append((float(raw), i))
                        except (TypeError, ValueError):
                            pass
                    if vals:
                        val, idx = max(vals)
                        if val >= 0.99 and idx < len(outcomes):
                            label = str(outcomes[idx]).strip().upper()
                            if label in ("UP", "YES"):
                                return "UP"
                            if label in ("DOWN", "NO"):
                                return "DOWN"
            except Exception:
                pass
            await asyncio.sleep(2.0)
    return None


async def _get_market_for_session(
    client: AsyncPublicClient,
    *,
    asset: str,
    session_index: int,
    join_current: bool,
) -> Any:
    while True:
        now = time.time()
        current_start = window_start_epoch(DURATION_S, now)
        age = now - current_start
        # For session 0, --join-current can attach to the live window. Otherwise
        # use the next clean 15m boundary whenever we are already past quote start.
        start = current_start
        if session_index > 0 or (not join_current and age > QUOTE_START_AGE_S):
            start = current_start + DURATION_S
        if start > now:
            wait = start - now
            print(f"WAIT   {asset.upper()} next 15m market in {wait:.1f}s")
            await asyncio.sleep(min(wait, 5.0))
            continue
        m = await resolve_market(client, asset, DURATION_S, int(start))
        if m is not None and m.seconds_to_end > 0:
            return m
        await asyncio.sleep(1.0)


async def run(args: argparse.Namespace) -> int:
    assets = tuple(x.strip().lower() for x in args.assets.split(",") if x.strip())
    bad = [a for a in assets if a not in ("btc", "eth")]
    if bad:
        raise SystemExit(f"15m forensic mode is calibrated for btc,eth only; unsupported: {bad}")

    prefix = Path(args.out)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = prefix.parent / f"{prefix.name}_{stamp}_events.csv"
    json_path = prefix.parent / f"{prefix.name}_{stamp}_summary.json"

    client = AsyncPublicClient()
    results: list[dict[str, Any]] = []
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=EVENT_FIELDS)
            writer.writeheader()

            for session_no in range(1, args.sessions + 1):
                markets = await asyncio.gather(
                    *(
                        _get_market_for_session(
                            client,
                            asset=a,
                            session_index=session_no - 1,
                            join_current=args.join_current,
                        )
                        for a in assets
                    )
                )
                engines = [
                    PaperMarket(
                        session_no=session_no,
                        market=m,
                        writer=writer,
                        paper_cash=args.paper_cash,
                        poll_s=args.poll,
                        quote_ttl_s=args.quote_ttl,
                        taker_mode=args.taker_mode,
                        max_combined_vwap=args.max_combined_vwap,
                    )
                    for m in markets
                ]
                await asyncio.gather(*(e.run(client) for e in engines))
                fh.flush()

                # Resolve actual outcomes after close, then compute real paper PnL.
                winners = await asyncio.gather(
                    *(
                        _winner_from_gamma(e.market.slug, args.resolution_timeout)
                        for e in engines
                    )
                )
                for engine, winner in zip(engines, winners, strict=True):
                    engine.stats.winner = winner
                    engine.stats.resolution_found = winner is not None
                    p = engine.inv.policy()
                    if winner is not None:
                        engine.stats.settlement_value = settlement_value(p, winner)
                        engine.stats.settlement_pnl = settlement_pnl(p, winner)
                    row = {
                        **asdict(engine.stats),
                        "up_shares": p.up_shares,
                        "down_shares": p.down_shares,
                        "up_vwap": p.up_vwap,
                        "down_vwap": p.down_vwap,
                        "combined_vwap": p.combined_vwap,
                        "matched_shares": p.matched,
                        "gross_spend": acquisition_spend(p),
                        "ending_unsettled_cash": engine.cash,
                        "starting_paper_cash": engine.starting_cash,
                        "maker_share": (
                            engine.stats.maker_fills
                            / max(1, engine.stats.maker_fills + engine.stats.taker_fills)
                        ),
                    }
                    results.append(row)
                    print("-" * 100)
                    print(json.dumps(row, indent=2, sort_keys=True))

        summary = {
            "generated_utc": _iso(),
            "mode": "READ_ONLY_REAL_15M_BOOKS",
            "fill_model": "later-snapshot full-depth cross-through; no queue-priority claim",
            "cancel_surface": f"paper quote TTL={args.quote_ttl}s; not a historical claim",
            "taker_mode": args.taker_mode,
            "assets": list(assets),
            "sessions": args.sessions,
            "markets": results,
            "aggregate": {
                "markets": len(results),
                "resolved": sum(bool(r["resolution_found"]) for r in results),
                "maker_fills": sum(int(r["maker_fills"]) for r in results),
                "taker_fills": sum(int(r["taker_fills"]) for r in results),
                "gross_spend": sum(float(r["gross_spend"]) for r in results),
                "settlement_pnl": sum(
                    float(r["settlement_pnl"])
                    for r in results if r["settlement_pnl"] is not None
                ),
            },
        }
        json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nEVENTS  {csv_path}")
        print(f"SUMMARY {json_path}")
        return 0
    finally:
        try:
            await client.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Read-only real-market Gabagool 15m forensic paper runner"
    )
    ap.add_argument("--assets", default="btc,eth", help="btc,eth or one of them")
    ap.add_argument("--sessions", type=int, default=1, help="consecutive 15m windows")
    ap.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    ap.add_argument("--quote-ttl", type=float, default=DEFAULT_QUOTE_TTL_S)
    ap.add_argument("--paper-cash", type=float, default=DEFAULT_PAPER_CASH)
    ap.add_argument(
        "--taker-mode",
        choices=("evidence", "off"),
        default="evidence",
        help="evidence = minority deficient-leg repair; off = pure maker reconstruction",
    )
    ap.add_argument("--max-combined-vwap", type=float, default=DEFAULT_MAX_COMBINED_VWAP)
    ap.add_argument("--resolution-timeout", type=float, default=DEFAULT_RESOLUTION_TIMEOUT_S)
    ap.add_argument("--out", default=DEFAULT_OUTPUT_PREFIX)
    ap.add_argument(
        "--join-current",
        action="store_true",
        help="join the current 15m window even if it is already past the 18s start; default waits for a clean window",
    )
    args = ap.parse_args()
    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if args.poll <= 0 or args.quote_ttl <= 0 or args.paper_cash <= 0:
        ap.error("poll, quote-ttl, and paper-cash must be positive")
    return args


def main() -> None:
    raise SystemExit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
