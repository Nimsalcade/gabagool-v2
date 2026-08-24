"""Run the reconstructed Gabagool 15-minute policy against REAL live books.

READ ONLY. This program uses ``AsyncPublicClient`` only. It never loads a wallet,
never signs an order, and never submits/cancels/merges/redeems anything on-chain.

The strategy rules come from :mod:`src.forensic_15m`. The two dimensions public
fills cannot identify exactly -- queue priority and private cancel/requote timing --
are intentionally kept outside the strategy and exposed as conservative paper-model
parameters.

Default maker fill proxy (`--maker-fill-backend snapshot_cross`):
  A hypothetical BUY must already be resting. A later real order-book snapshot must
  show enough total ask size at/below the paper bid to fill the WHOLE paper order.
  The same displayed liquidity is consumed only once per snapshot across our orders.

Alternate maker fill proxy (`--maker-fill-backend public_tape`):
  Each V5 layer is a hypothetical resting BUY with visible queue-ahead. Public SELL
  prints (CLOB WebSocket ``last_trade_price``, Data API fallback) consume finite
  tape volume once across layers in price/time priority. Book snapshots only shrink
  queue-ahead; they never independently create fills. Partial fills are recorded.

Default quote TTL:
  10 seconds. This is NOT claimed to be Gabagool's private cancel timer.
  ``snapshot_cross`` still cancel/reposts at TTL (full-parent proxy).
  ``public_tape`` treats TTL as a same-price keepalive: if the resting bid is
  still at or below the current allowed complementary base and within the
  inventory layer count, the existing ShadowOrder is kept (oid, posted_ts,
  queue_ahead, filled/remaining preserved) and only ``expires`` is extended.

V5.1 tape reconciliation:
  Public SELL prints that share a transaction hash (else the same millisecond)
  are one atomic execution group. Inventory updates for the whole group, then
  the quote ladder is reevaluated. Same-price queue state is preserved.

V5.2 sticky ladder (public_tape paper candidate, not recovered source):
  After each atomic group and on renew/TTL, existing bids that are still at or
  below the current allowed base KEEP FIFO priority when the complementary
  anchor rises. Vacancies replenish at the current anchor. Inventory 4→0 still
  drops extra layers. We do not chase a higher complementary tick merely
  because the anchor moved.

V5.2a one-tick hysteresis (paper experiment, not recovered source):
  A resting bid exactly one tick above the current allowed base is kept
  (``HYSTERESIS_KEEP_1T``) to preserve FIFO. Bids 2 ticks too aggressive
  emit ``REPRICE_BACKOFF_2T``; 3+ ticks emit ``REPRICE_BACKOFF_3PLUS``.

Optional aggressive repair:
  ``--taker-mode evidence`` uses the repository's full-history, evidence-calibrated
  deficient-leg repair gate. Its existence/monotonic fingerprints are observed; the
  exact historical trigger formula remains unidentifiable. Use ``--taker-mode off``
  to isolate the pure maker reconstruction.

Optional joint-exposure paper candidate (`--fresh-pair-cap`, default 0 / off):
  PARKED. Same-snapshot post-only complementary bids already sum to ≤ 0.99;
  a 1.05 cap on uncapped anchors is a new dislocation filter, not a recovered
  Gabagool rule. Pass a positive value only for an explicit paper experiment.
  Resting orders are never mass-cancelled by this gate; keepalive still uses
  ordinary V5 desired prices.

Unmatched-cost pool (accounting / telemetry only):
  Every paper fill nets against a weighted unmatched UP/DOWN cost pool. Repair
  basis = fill_price + opposite_unmatched_vwap. Parent-clip residue that crosses
  neutral becomes new unmatched inventory. This is NOT an admission filter.

V5.1 tape reconciliation:
  Public SELL prints that share a transaction hash (else the same millisecond)
  are one atomic execution group. Inventory updates for the whole group, then
  V5.2 sticky-ladder reconciliation runs (no upward chase; 1-tick hysteresis;
  2+ tick backoff). Same-price queue state is preserved. No new admission threshold.

Example:
  python -m tools.run_forensic_15m_paper --assets btc,eth --sessions 1
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import time
from dataclasses import asdict, dataclass
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
from src.joint_exposure import (
    FRESH_PAIR_CATASTROPHIC,
    apply_joint_exposure_override,
    complementary_anchor,
)
from src.policy import (
    InventoryState as RepairState,
    projected_combined_vwap as repair_projected_vwap,
    taker_should_fire,
)
from src.public_tape import (
    PublicSellTape,
    TapePrint,
    apply_sell_print_to_orders,
    atomic_tape_groups,
)
from src.shadow import ShadowOrder, reduce_queue_from_book
from src.unmatched_pool import UnmatchedPool
from src.sticky_ladder import plan_sticky_side
from tools.metamask_10session_strategy_observer import _best, _books, _levels


EVENT_FIELDS = [
    "utc", "session", "asset", "market", "age_s", "event", "side", "order_id",
    "qty", "price", "cost", "reason", "cash", "reserved", "up_shares", "up_vwap",
    "down_shares", "down_vwap", "combined_vwap", "gap_shares", "clip", "up_layers",
    "down_layers", "maker_fills", "taker_fills",
    "unmatched_up_before", "unmatched_down_before",
    "unmatched_up_vwap_before", "unmatched_down_vwap_before",
    "closing_qty", "overshoot_qty", "repair_basis",
    "unmatched_up_after", "unmatched_down_after",
    "completed_set_qty_cumulative", "completed_set_cost_vwap_cumulative",
]


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc).isoformat()


def _best_ask(book: Any) -> tuple[float, float] | None:
    x = _best(book, "asks")
    return None if x is None else (float(x[0]), float(x[1]))


def _asks(book: Any) -> list[tuple[float, float]]:
    return [(float(p), float(q)) for p, q in _levels(book, "asks")]


def _bids(book: Any) -> list[tuple[float, float]]:
    return [(float(p), float(q)) for p, q in _levels(book, "bids")]


def _level_size(levels: list[tuple[float, float]], price: float) -> float | None:
    for p, q in levels:
        if abs(p - price) <= 1e-9:
            return q
    return None


def _tick(book: Any) -> float:
    try:
        x = float(getattr(book, "tick_size", None) or 0.01)
        return x if x > 0 else 0.01
    except (TypeError, ValueError):
        return 0.01


@dataclass
class Order:
    oid: str
    side: str
    price: float
    shares: float
    created: float
    expires: float
    shadow: ShadowOrder | None = None

    @property
    def remaining(self) -> float:
        if self.shadow is not None:
            return self.shadow.remaining
        return self.shares


@dataclass
class Inventory:
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    last_up_fill: float | None = None
    last_down_fill: float | None = None

    def policy(self) -> PolicyInventory:
        return PolicyInventory(self.up_shares, self.down_shares, self.up_cost, self.down_cost)

    def add(self, side: str, shares: float, price: float, now: float) -> None:
        if side == "UP":
            self.up_shares += shares
            self.up_cost += shares * price
            self.last_up_fill = now
        else:
            self.down_shares += shares
            self.down_cost += shares * price
            self.last_down_fill = now

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
        lo, hi = min(self.up_shares, self.down_shares), max(self.up_shares, self.down_shares)
        if hi <= 0:
            return 1.0
        return math.inf if lo <= 0 else hi / lo


@dataclass
class Result:
    session: int
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
    resolved: bool = False
    settlement_value: float | None = None
    settlement_pnl: float | None = None
    conservative_floor_pnl: float | None = None
    completed_set_qty: float = 0.0
    completed_set_vwap: float | None = None
    unmatched_up_end: float = 0.0
    unmatched_down_end: float = 0.0
    unmatched_up_vwap_end: float | None = None
    unmatched_down_vwap_end: float | None = None


class Engine:
    def __init__(self, *, session: int, market: Any, writer: csv.DictWriter, args: argparse.Namespace):
        self.session = session
        self.market = market
        self.writer = writer
        self.args = args
        self.cash = float(args.paper_cash)
        self.inv = Inventory()
        self.pool = UnmatchedPool()
        self.orders: dict[str, Order] = {}
        self.oid_seq = 1
        self.clip = 0.0
        self.result = Result(session, market.asset, market.slug, market.condition_id)
        self._last_no_quote_diag: str | None = None
        self._last_joint_diag: str | None = None
        self.tape: PublicSellTape | None = None
        self._last_up_book: Any = None
        self._last_down_book: Any = None
        self._last_sticky_diag: dict[str, str | None] = {"UP": None, "DOWN": None}
        self._hysteresis_emitted: set[str] = set()

    def _use_tape(self) -> bool:
        return getattr(self.args, "maker_fill_backend", "snapshot_cross") == "public_tape"

    def reserved(self, *, exclude_side: str | None = None) -> float:
        return sum(o.price * o.remaining for o in self.orders.values() if o.side != exclude_side)

    def emit(self, now: float, event: str, *, side: str = "", order: Order | None = None,
             qty: float | None = None, price: float | None = None,
             cost: float | None = None, reason: str = "",
             extra: dict[str, Any] | None = None) -> None:
        p = self.inv.policy()
        c = self.clip
        up_layers = desired_layer_count(p, "UP", c) if c > 0 else 0
        dn_layers = desired_layer_count(p, "DOWN", c) if c > 0 else 0
        row = {
            "utc": _iso(now), "session": self.session, "asset": self.market.asset,
            "market": self.market.slug, "age_s": f"{now-self.market.window_start:.3f}",
            "event": event, "side": side, "order_id": "" if order is None else order.oid,
            "qty": "" if qty is None else f"{qty:.9f}",
            "price": "" if price is None else f"{price:.9f}",
            "cost": "" if cost is None else f"{cost:.9f}", "reason": reason,
            "cash": f"{self.cash:.9f}", "reserved": f"{self.reserved():.9f}",
            "up_shares": f"{p.up_shares:.9f}",
            "up_vwap": "" if p.up_vwap is None else f"{p.up_vwap:.9f}",
            "down_shares": f"{p.down_shares:.9f}",
            "down_vwap": "" if p.down_vwap is None else f"{p.down_vwap:.9f}",
            "combined_vwap": "" if p.combined_vwap is None else f"{p.combined_vwap:.9f}",
            "gap_shares": f"{p.abs_gap:.9f}", "clip": f"{c:.3f}",
            "up_layers": up_layers, "down_layers": dn_layers,
            "maker_fills": self.result.maker_fills, "taker_fills": self.result.taker_fills,
            "unmatched_up_before": "", "unmatched_down_before": "",
            "unmatched_up_vwap_before": "", "unmatched_down_vwap_before": "",
            "closing_qty": "", "overshoot_qty": "", "repair_basis": "",
            "unmatched_up_after": "", "unmatched_down_after": "",
            "completed_set_qty_cumulative": "", "completed_set_cost_vwap_cumulative": "",
        }
        if extra:
            row.update(extra)
        self.writer.writerow(row)

    def post(self, side: str, price: float, now: float, *,
             up_book: Any = None, down_book: Any = None) -> None:
        notional = price * self.clip
        available = self.cash - self.reserved()
        if notional > available + 1e-9:
            self.emit(
                now, "POST_REJECT", side=side, qty=self.clip, price=price,
                cost=notional,
                reason=f"cash/reserved gate; available={available:.6f}",
            )
            return
        shadow = None
        reason = "1-opposite-ask passive layer"
        if self._use_tape():
            book = up_book if side == "UP" else down_book
            token = self.market.up_token_id if side == "UP" else self.market.down_token_id
            queue, kind, best_bid, same_sz = self.classify_queue_init(book, price)
            shadow = ShadowOrder(
                side=side, token_id=str(token), price=price, shares=self.clip,
                queue_ahead=queue, posted_ts=now,
            )
            qtxt = "inf" if math.isinf(queue) else f"{queue:.6f}"
            btxt = "None" if best_bid is None else f"{best_bid:.6f}"
            stxt = "None" if same_sz is None else f"{same_sz:.6f}"
            reason = (
                "1-opposite-ask passive layer; "
                f"queue_init_kind={kind}; "
                f"queue_ahead={qtxt}; "
                f"paper_bid={price:.6f}; "
                f"real_best_bid={btxt}; "
                f"same_price_real_size={stxt}"
            )
        o = Order(
            oid=f"P{self.session}-{self.market.asset.upper()}-{self.oid_seq}",
            side=side, price=price, shares=self.clip, created=now,
            expires=now + self.args.quote_ttl, shadow=shadow,
        )
        self.oid_seq += 1
        self.orders[o.oid] = o
        self.result.quote_posts += 1
        self.emit(now, "QUOTE", side=side, order=o, qty=o.shares, price=o.price,
                  cost=notional, reason=reason)

    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        plans: dict[str, Any] = {}
        if self._use_tape() and up_book is not None and down_book is not None:
            ua, da = _best_ask(up_book), _best_ask(down_book)
            if ua is not None and da is not None:
                age = now - self.market.window_start
                clip = clip_for_age(age) if QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S else 0.0
                p = self.inv.policy()
                for side, own, opp, tick in (
                    ("UP", ua, da, _tick(up_book)),
                    ("DOWN", da, ua, _tick(down_book)),
                ):
                    base = complementary_base_bid(
                        own_best_ask=own[0], opposite_best_ask=opp[0], tick=tick,
                    )
                    n = desired_layer_count(p, side, clip) if clip > 0 else 0
                    active = [
                        (o.oid, o.price, o.created)
                        for o in self.orders.values() if o.side == side
                    ]
                    plans[side] = plan_sticky_side(
                        orders=active, current_base=base, desired_n=n, tick=tick,
                    )
        keep_desired: dict[str, tuple[float, ...]] | None = None
        if not plans and self._use_tape() and up_book is not None and down_book is not None:
            age = now - self.market.window_start
            if QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S and clip_for_age(age) > 0:
                keep_desired = self.desired(up_book, down_book)
        for oid, o in list(self.orders.items()):
            if now < o.expires:
                continue
            keep = False
            kind = "EXPIRE"
            if o.side in plans:
                plan = plans[o.side]
                if oid in plan.keep_oids:
                    keep = True
                    if oid in plan.hysteresis_1t_oids:
                        kind = "HYSTERESIS_KEEP_1T"
                    elif oid in plan.sticky_keep_oids:
                        kind = "STICKY_KEEP"
                    else:
                        kind = "QUEUE_KEEP"
                elif oid in plan.backoff_3plus_oids:
                    kind = "REPRICE_BACKOFF_3PLUS"
                elif oid in plan.backoff_2t_oids or oid in plan.backoff_oids:
                    kind = "REPRICE_BACKOFF_2T"
                elif oid in plan.drop_oids:
                    kind = "INVENTORY_LAYER_DROP"
            elif keep_desired is not None:
                wanted = {round(px, 10) for px in keep_desired.get(o.side, ())}
                still_wanted = round(o.price, 10) in wanted
                safe = hard_gap_allows(
                    self.inv.policy(), side=o.side, shares=o.remaining,
                    parent_clip=max(self.clip, o.shares) if max(self.clip, o.shares) > 0 else o.shares,
                )
                if still_wanted and safe:
                    keep = True
                    kind = "QUEUE_KEEP"
            if keep:
                o.expires = now + self.args.quote_ttl
                q = o.shadow.queue_ahead if o.shadow is not None else float("nan")
                qtxt = "inf" if math.isinf(q) else f"{q:.6f}"
                self.emit(
                    now, kind, side=o.side, order=o, qty=o.remaining,
                    price=o.price,
                    reason=(
                        f"sticky keepalive; remaining={o.remaining:.9f}; "
                        f"queue_ahead={qtxt}; posted_ts={o.created:.3f}"
                    ),
                )
                continue
            del self.orders[oid]
            self.result.quote_expiries += 1
            self.emit(now, kind, side=o.side, order=o, qty=o.remaining, price=o.price,
                      reason=f"paper TTL={self.args.quote_ttl}s; {kind.lower()}")

    def fill(self, now: float, side: str, shares: float, price: float, kind: str,
             reason: str, order: Order | None = None) -> bool:
        cost = shares * price
        if cost > self.cash + 1e-9:
            return False
        self.cash -= cost
        net = self.pool.apply_fill(side, shares, price)
        self.inv.add(side, shares, price, now)
        age = now - self.market.window_start
        if self.result.first_fill_age_s is None:
            self.result.first_fill_age_s = age
        self.result.last_fill_age_s = age
        if kind == "MAKER_FILL":
            self.result.maker_fills += 1
        else:
            self.result.taker_fills += 1
        p = self.inv.policy()
        self.result.max_gap_shares = max(self.result.max_gap_shares, p.abs_gap)
        if self.clip > 0:
            self.result.max_gap_clips = max(self.result.max_gap_clips, p.abs_gap / self.clip)
        self.result.completed_set_qty = net.completed_set_qty_cumulative
        self.result.completed_set_vwap = net.completed_set_cost_vwap_cumulative
        self.result.unmatched_up_end = net.unmatched_up_after
        self.result.unmatched_down_end = net.unmatched_down_after
        self.result.unmatched_up_vwap_end = net.unmatched_up_vwap_after
        self.result.unmatched_down_vwap_end = net.unmatched_down_vwap_after

        def fmt(x: float | None) -> str:
            return "" if x is None else f"{x:.9f}"

        extra = {
            "unmatched_up_before": f"{net.unmatched_up_before:.9f}",
            "unmatched_down_before": f"{net.unmatched_down_before:.9f}",
            "unmatched_up_vwap_before": fmt(net.unmatched_up_vwap_before),
            "unmatched_down_vwap_before": fmt(net.unmatched_down_vwap_before),
            "closing_qty": f"{net.close_qty:.9f}",
            "overshoot_qty": f"{net.overshoot_qty:.9f}",
            "repair_basis": fmt(net.repair_basis),
            "unmatched_up_after": f"{net.unmatched_up_after:.9f}",
            "unmatched_down_after": f"{net.unmatched_down_after:.9f}",
            "completed_set_qty_cumulative": f"{net.completed_set_qty_cumulative:.9f}",
            "completed_set_cost_vwap_cumulative": fmt(net.completed_set_cost_vwap_cumulative),
        }
        self.emit(now, kind, side=side, order=order, qty=shares, price=price,
                  cost=cost, reason=reason, extra=extra)
        rb = "" if net.repair_basis is None else f"{net.repair_basis:.4f}"
        print(
            f"{self.market.asset.upper():3} {kind:10} {side:4} {shares:5.1f}@{price:.3f} | "
            f"U={p.up_shares:.1f}@{(p.up_vwap or 0):.4f} "
            f"D={p.down_shares:.1f}@{(p.down_vwap or 0):.4f} "
            f"basis={(p.combined_vwap or 0):.4f} "
            f"close={net.close_qty:.2f} over={net.overshoot_qty:.2f} "
            f"repair={rb or '-'} "
            f"uU={net.unmatched_up_after:.1f} uD={net.unmatched_down_after:.1f} "
            f"set={(net.completed_set_cost_vwap_cumulative or 0):.4f} "
            f"cash=${self.cash:.2f}"
        )
        return True

    def strict_maker_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            levels = [[p, q] for p, q in _asks(book)]
            if not levels:
                continue
            orders = sorted(
                [o for o in self.orders.values() if o.side == side and now > o.created],
                key=lambda o: (-o.price, o.created),
            )
            for o in orders:
                if not hard_gap_allows(self.inv.policy(), side=side, shares=o.shares,
                                       parent_clip=max(self.clip, o.shares)):
                    self.orders.pop(o.oid, None)
                    self.emit(now, "CANCEL_SAFETY", side=side, order=o, qty=o.shares,
                              price=o.price, reason="8-clip emergency gap")
                    continue
                available = sum(q for p, q in levels if p <= o.price + 1e-12)
                if available + 1e-9 < o.shares:
                    continue
                need = o.shares
                for level in levels:
                    p, q = level
                    if p > o.price + 1e-12 or need <= 1e-12:
                        continue
                    take = min(q, need)
                    level[1] -= take
                    need -= take
                if need > 1e-8:
                    continue
                self.orders.pop(o.oid, None)
                depth_ratio = available / o.shares if o.shares > 0 else float("inf")
                self.fill(
                    now, side, o.shares, o.price, "MAKER_FILL",
                    (
                        f"later real ask snapshot crossed paper bid after {now-o.created:.2f}s; "
                        f"cross_depth={available:.9f}; "
                        f"depth_ratio={depth_ratio:.6f}; "
                        f"cross_excess={available-o.shares:.9f}"
                    ),
                    o,
                )

    def classify_queue_init(
        self, book: Any, price: float,
    ) -> tuple[float, str, float | None, float | None]:
        """Return (queue_ahead, kind, real_best_bid, same_price_real_size).

        queue_ahead uses the inherited shadow_market rule unchanged:
          exact displayed level → that size
          improve best bid      → 0
          otherwise             → inf
        kind is observational only.
        """
        if book is None:
            return float("inf"), "EMPTY_AT_BEST", None, None
        bids = _bids(book)
        same = _level_size(bids, price)
        best = max((p for p, _ in bids), default=None)
        if same is not None:
            return float(same), "EXACT_LEVEL", best, float(same)
        if best is None:
            return float("inf"), "EMPTY_AT_BEST", None, None
        if price > best + 1e-9:
            return 0.0, "IMPROVE_BEST", best, None
        if price < best - 1e-9:
            return float("inf"), "EMPTY_BELOW_BEST", best, None
        return float("inf"), "EMPTY_AT_BEST", best, None

    def _queue_ahead_at(self, book: Any, price: float) -> float:
        return self.classify_queue_init(book, price)[0]

    def reduce_queues(self, up_book: Any, down_book: Any) -> None:
        for o in self.orders.values():
            if o.shadow is None:
                continue
            book = up_book if o.side == "UP" else down_book
            reduce_queue_from_book(
                o.shadow,
                visible_size_at_price=_level_size(_bids(book), o.price),
            )

    def tape_maker_fills(
        self, now: float, prints: list[TapePrint],
        up_book: Any = None, down_book: Any = None,
    ) -> None:
        up_book = up_book if up_book is not None else self._last_up_book
        down_book = down_book if down_book is not None else self._last_down_book
        for group in atomic_tape_groups(prints):
            filled = False
            last_ts = now
            for p in group:
                if self._apply_one_tape_print(now, p):
                    filled = True
                    last_ts = max(float(p.event_ts), last_ts)
            if filled:
                self.reconcile_desired_after_fills(last_ts, up_book, down_book)

    def _apply_one_tape_print(self, now: float, p: TapePrint) -> bool:
        if p.token_id == self.market.up_token_id:
            side = "UP"
        elif p.token_id == self.market.down_token_id:
            side = "DOWN"
        else:
            return False
        candidates: list[Order] = []
        for o in list(self.orders.values()):
            if o.side != side or o.shadow is None or o.shadow.done:
                continue
            if p.event_ts + 0.050 < o.created:
                continue
            if not hard_gap_allows(
                self.inv.policy(), side=side, shares=o.remaining,
                parent_clip=max(self.clip, o.shares),
            ):
                self.orders.pop(o.oid, None)
                self.emit(
                    now, "CANCEL_SAFETY", side=side, order=o, qty=o.remaining,
                    price=o.price, reason="8-clip emergency gap",
                )
                continue
            candidates.append(o)
        if not candidates:
            return False
        by_shadow = {id(o.shadow): o for o in candidates if o.shadow is not None}
        fills = apply_sell_print_to_orders(
            [o.shadow for o in candidates if o.shadow is not None],
            trade_price=p.price, trade_size=p.size,
        )
        any_fill = False
        for sh, qty in fills:
            o = by_shadow.get(id(sh))
            if o is None:
                continue
            if not hard_gap_allows(
                self.inv.policy(), side=side, shares=qty,
                parent_clip=max(self.clip, o.shares),
            ):
                sh.filled = max(0.0, sh.filled - qty)
                self.orders.pop(o.oid, None)
                self.emit(
                    now, "CANCEL_SAFETY", side=side, order=o, qty=o.remaining,
                    price=o.price, reason="8-clip emergency gap",
                )
                continue
            q = sh.queue_ahead
            qtxt = "inf" if math.isinf(q) else f"{q:.6f}"
            fill_ts = max(float(p.event_ts), o.created)
            tx = p.tx_id or ""
            grp = tx if tx else f"ts:{round(p.event_ts, 3)}"
            ok = self.fill(
                fill_ts, side, qty, o.price, "MAKER_FILL",
                (
                    f"public SELL {p.source} {p.size:.9f}@{p.price:.4f}; "
                    f"tx_id={tx or 'none'}; group={grp}; "
                    f"queue_ahead={qtxt}; remaining={sh.remaining:.9f}"
                ),
                o,
            )
            if not ok:
                sh.filled = max(0.0, sh.filled - qty)
                continue
            any_fill = True
            if sh.done:
                self.orders.pop(o.oid, None)
        return any_fill

    def reconcile_desired_after_fills(
        self, now: float, up_book: Any, down_book: Any,
    ) -> None:
        """Sticky-ladder reevaluation after an atomic tape group.

        Back off too-aggressive bids, drop extra inventory layers, keep valid
        resting FIFO when the anchor rises. Vacancies replenish at the current
        complementary base. Does not chase a higher tick while the side already
        has its required layer count.
        """
        if up_book is None or down_book is None:
            return
        age = now - self.market.window_start
        clip = clip_for_age(age)
        if clip > 0:
            self.clip = clip
        self.apply_sticky_ladder(now, up_book, down_book, replenish=True)

    def apply_sticky_ladder(
        self, now: float, up_book: Any, down_book: Any, *, replenish: bool,
    ) -> None:
        if up_book is None or down_book is None:
            return
        ua, da = _best_ask(up_book), _best_ask(down_book)
        p = self.inv.policy()
        want_new = self.desired_new_exposure(now, up_book, down_book)
        for side, own, opp, tick in (
            ("UP", ua, da, _tick(up_book)),
            ("DOWN", da, ua, _tick(down_book)),
        ):
            if own is None or opp is None or self.clip <= 0:
                base = None
                n = 0 if self.clip <= 0 else desired_layer_count(p, side, self.clip)
            else:
                base = complementary_base_bid(
                    own_best_ask=own[0], opposite_best_ask=opp[0], tick=tick,
                )
                n = desired_layer_count(p, side, self.clip)
            active = [
                (o.oid, o.price, o.created)
                for o in self.orders.values() if o.side == side
            ]
            plan = plan_sticky_side(
                orders=active, current_base=base, desired_n=n, tick=tick,
            )
            for oid in plan.backoff_2t_oids:
                o = self.orders.pop(oid, None)
                if o is None:
                    continue
                self._hysteresis_emitted.discard(oid)
                self.emit(
                    now, "REPRICE_BACKOFF_2T", side=side, order=o,
                    qty=o.remaining, price=o.price,
                    reason=f"bid {o.price:.4f} is 2 ticks > allowed base {base}; sticky backoff",
                )
            for oid in plan.backoff_3plus_oids:
                o = self.orders.pop(oid, None)
                if o is None:
                    continue
                self._hysteresis_emitted.discard(oid)
                self.emit(
                    now, "REPRICE_BACKOFF_3PLUS", side=side, order=o,
                    qty=o.remaining, price=o.price,
                    reason=f"bid {o.price:.4f} is 3+ ticks > allowed base {base}; sticky backoff",
                )
            for oid in plan.hysteresis_1t_oids:
                if oid in self._hysteresis_emitted:
                    continue
                o = self.orders.get(oid)
                if o is None:
                    continue
                q = o.shadow.queue_ahead if o.shadow is not None else float("nan")
                qtxt = "inf" if math.isinf(q) else f"{q:.6f}"
                self.emit(
                    now, "HYSTERESIS_KEEP_1T", side=side, order=o,
                    qty=o.remaining, price=o.price,
                    reason=(
                        f"1-tick adverse keep; base={base}; "
                        f"remaining={o.remaining:.9f}; queue_ahead={qtxt}"
                    ),
                )
                self._hysteresis_emitted.add(oid)
            live_hyst = set(plan.hysteresis_1t_oids)
            self._hysteresis_emitted = {
                oid for oid in self._hysteresis_emitted
                if oid in live_hyst or (
                    oid in self.orders and self.orders[oid].side != side
                )
            }
            for oid in plan.drop_oids:
                o = self.orders.pop(oid, None)
                if o is None:
                    continue
                self.emit(
                    now, "INVENTORY_LAYER_DROP", side=side, order=o,
                    qty=o.remaining, price=o.price,
                    reason=(
                        f"sticky 4→0 drop; desired_n={n} "
                        f"gap={p.abs_gap:.6f} clip={self.clip:.3f}"
                    ),
                )
            if plan.skipped_higher:
                reason = (
                    f"no upward chase; keep {len(plan.keep_oids)}/{n} "
                    f"base={base} skip={plan.skipped_higher}"
                )
                if reason != self._last_sticky_diag.get(side):
                    self.emit(now, "STICKY_KEEP", side=side, reason=reason)
                    self._last_sticky_diag[side] = reason
            else:
                self._last_sticky_diag[side] = None
            if not replenish:
                continue
            n_post = len(want_new.get(side, ()))
            for px in plan.replenish_prices:
                live_n = sum(1 for o in self.orders.values() if o.side == side)
                if live_n >= n_post:
                    break
                if not hard_gap_allows(
                    self.inv.policy(), side=side, shares=self.clip,
                    parent_clip=self.clip,
                ):
                    break
                self.post(side, px, now, up_book=up_book, down_book=down_book)
                self.emit(
                    now, "VACANCY_REPLENISH", side=side, qty=self.clip, price=px,
                    reason=f"sticky vacancy at current anchor {px:.4f}; base={base}",
                )

    def desired(self, up_book: Any, down_book: Any) -> dict[str, tuple[float, ...]]:
        ua, da = _best_ask(up_book), _best_ask(down_book)
        if ua is None or da is None or self.clip <= 0:
            return {"UP": (), "DOWN": ()}
        p = self.inv.policy()
        up_tick, dn_tick = _tick(up_book), _tick(down_book)
        ub = complementary_base_bid(
            own_best_ask=ua[0], opposite_best_ask=da[0], tick=up_tick
        )
        db = complementary_base_bid(
            own_best_ask=da[0], opposite_best_ask=ua[0], tick=dn_tick
        )
        return {
            "UP": layer_prices(ub, tick=up_tick,
                               layers=desired_layer_count(p, "UP", self.clip)),
            "DOWN": layer_prices(db, tick=dn_tick,
                                 layers=desired_layer_count(p, "DOWN", self.clip)),
        }

    def _fresh_pair_cap(self) -> float:
        return float(getattr(self.args, "fresh_pair_cap", 0.0))

    def desired_new_exposure(
        self, now: float, up_book: Any, down_book: Any
    ) -> dict[str, tuple[float, ...]]:
        """V5 layers, then paper joint-exposure override for NEW posts only."""
        ua, da = _best_ask(up_book), _best_ask(down_book)
        if ua is None or da is None or self.clip <= 0:
            return {"UP": (), "DOWN": ()}
        p = self.inv.policy()
        up_tick, dn_tick = _tick(up_book), _tick(down_book)
        ub = complementary_base_bid(
            own_best_ask=ua[0], opposite_best_ask=da[0], tick=up_tick
        )
        db = complementary_base_bid(
            own_best_ask=da[0], opposite_best_ask=ua[0], tick=dn_tick
        )
        v5 = {
            "UP": desired_layer_count(p, "UP", self.clip),
            "DOWN": desired_layer_count(p, "DOWN", self.clip),
        }
        anchor_up = complementary_anchor(da[0], up_tick)
        anchor_dn = complementary_anchor(ua[0], dn_tick)
        cap = self._fresh_pair_cap()
        layers = apply_joint_exposure_override(
            up_base=anchor_up,
            down_base=anchor_dn,
            layers=v5,
            signed_gap=p.signed_gap,
            cap=cap,
        )
        if layers != v5:
            posted_pair = (
                None if ub is None or db is None else round(ub + db, 10)
            )
            anchor_pair = (
                None if anchor_up is None or anchor_dn is None
                else round(anchor_up + anchor_dn, 10)
            )
            reason = (
                f"fresh_pair_cap={cap:.4f} "
                f"anchor={anchor_up}/{anchor_dn} sum={anchor_pair} "
                f"posted={ub}/{db} sum={posted_pair} "
                f"v5_layers={v5['UP']}/{v5['DOWN']} "
                f"new_layers={layers['UP']}/{layers['DOWN']} "
                f"signed_gap={p.signed_gap:.6f}"
            )
            if reason != self._last_joint_diag:
                self.emit(now, "JOINT_EXPOSURE", reason=reason)
                self._last_joint_diag = reason
        else:
            self._last_joint_diag = None
        return {
            "UP": layer_prices(ub, tick=up_tick, layers=layers["UP"]),
            "DOWN": layer_prices(db, tick=dn_tick, layers=layers["DOWN"]),
        }

    def renew(self, now: float, up_book: Any, down_book: Any) -> None:
        ua, da = _best_ask(up_book), _best_ask(down_book)
        p = self.inv.policy()
        up_layers = desired_layer_count(p, "UP", self.clip)
        down_layers = desired_layer_count(p, "DOWN", self.clip)
        if ua is None or da is None or self.clip <= 0:
            ub = db = None
        else:
            ub = complementary_base_bid(
                own_best_ask=ua[0], opposite_best_ask=da[0], tick=_tick(up_book),
            )
            db = complementary_base_bid(
                own_best_ask=da[0], opposite_best_ask=ua[0], tick=_tick(down_book),
            )
        self.apply_sticky_ladder(now, up_book, down_book, replenish=True)
        active_up = sum(1 for o in self.orders.values() if o.side == "UP")
        active_down = sum(1 for o in self.orders.values() if o.side == "DOWN")
        if not active_up and not active_down:

            def fmt(x: Any) -> str:
                if x is None:
                    return "None"
                if isinstance(x, tuple):
                    return f"{x[0]:.6f}@{x[1]:.6f}"
                return f"{float(x):.6f}"

            reason = (
                f"ua={fmt(ua)} da={fmt(da)} "
                f"ub={fmt(ub)} db={fmt(db)} "
                f"desired_layers={up_layers}/{down_layers} "
                f"active={active_up}/{active_down} "
                f"gap={p.abs_gap:.6f} clip={self.clip:.3f}"
            )
            if reason != self._last_no_quote_diag:
                self.emit(now, "NO_QUOTE_DIAG", reason=reason)
                self._last_no_quote_diag = reason
        else:
            self._last_no_quote_diag = None

    def repair_state(self, now: float) -> RepairState:
        return RepairState(
            up_shares=self.inv.up_shares, down_shares=self.inv.down_shares,
            up_cost=self.inv.up_cost, down_cost=self.inv.down_cost,
            last_up_fill_ts=self.inv.last_up_fill, last_down_fill_ts=self.inv.last_down_fill,
            now_ts=now, window_start_ts=float(self.market.window_start),
            seconds_to_end=float(self.market.window_end-now),
        )

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        if self.args.taker_mode != "evidence" or self.clip <= 0:
            return
        side = self.inv.underweight
        if side is None:
            return
        deficit = self.inv.deficit
        planned = min(deficit, self.clip * 2.0)
        if planned + 1e-9 < self.clip:
            return
        book = up_book if side == "UP" else down_book
        remaining, cost = planned, 0.0
        for px, qty in _asks(book):
            if remaining <= 1e-12:
                break
            take = min(qty, remaining)
            cost += take * px
            remaining -= take
        if remaining > 1e-8:
            return
        vwap = cost / planned
        state = self.repair_state(now)
        projected = repair_projected_vwap(state, side=side, price=vwap, shares=planned)
        if not taker_should_fire(
            state, candidate_side=side, projected_basis=projected,
            target_combined_vwap=.985, max_combined_vwap=self.args.max_combined_vwap,
            taker_stop_buffer_s=2.0,
        ):
            return
        # Orders on the deficient side are about to be replaced by the immediate
        # paper repair, so only reserve the opposite side when checking cash.
        if cost > self.cash - self.reserved(exclude_side=side) + 1e-9:
            return
        for oid, o in list(self.orders.items()):
            if o.side == side:
                del self.orders[oid]
                self.emit(now, "CANCEL_FOR_REPAIR", side=side, order=o, qty=o.remaining,
                          price=o.price, reason="deficient-leg aggressive repair")
        self.fill(now, side, planned, vwap, "TAKER_FILL",
                  f"evidence repair ratio={self.inv.ratio:.4f} deficit={deficit:.2f} projected={projected}")

    async def run(self, client: AsyncPublicClient) -> Result:
        print("\n" + "=" * 96)
        print(f"PAPER {self.market.asset.upper()} | {self.market.slug}")
        print(f"WINDOW {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")
        print(f"FILL   backend={getattr(self.args, 'maker_fill_backend', 'snapshot_cross')}")
        print(f"JOINT  fresh_pair_cap={self._fresh_pair_cap():.4f} (paper candidate, not recovered)")
        tape: PublicSellTape | None = None
        if self._use_tape():
            tape = PublicSellTape(client, self.market)
            await tape.start()
            self.tape = tape
            print(f"TAPE   {self.market.asset.upper()} source={tape.trade_source}")
        self.emit(
            time.time(), "SESSION_START",
            reason=(
                "real-book read-only forensic 15m "
                f"backend={getattr(self.args, 'maker_fill_backend', 'snapshot_cross')}"
                + (f" tape={tape.trade_source}" if tape is not None else "")
            ),
        )
        try:
            while time.time() < self.market.window_end:
                started = time.monotonic()
                now = time.time()
                age = now - self.market.window_start
                self.clip = clip_for_age(age)
                try:
                    up_book, down_book = await _books(
                        client, self.market.up_token_id, self.market.down_token_id
                    )
                except Exception as exc:  # noqa: BLE001
                    self.emit(now, "READ_ERROR", reason=f"{type(exc).__name__}: {exc}")
                    await asyncio.sleep(min(1.0, self.args.poll))
                    continue
                self._last_up_book, self._last_down_book = up_book, down_book
                self.expire(now, up_book, down_book)
                if tape is not None:
                    self.reduce_queues(up_book, down_book)
                    self.tape_maker_fills(now, await tape.drain(), up_book, down_book)
                else:
                    self.strict_maker_fills(now, up_book, down_book)
                if QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S and self.clip > 0:
                    self.maybe_taker(now, up_book, down_book)
                    self.renew(now, up_book, down_book)
                await asyncio.sleep(max(0.0, self.args.poll-(time.monotonic()-started)))
        finally:
            if tape is not None:
                await tape.stop()
                leftover = await tape.drain()
                if leftover:
                    self.tape_maker_fills(
                        time.time(), leftover, self._last_up_book, self._last_down_book,
                    )
                self.tape = None

        now = time.time()
        for oid, o in list(self.orders.items()):
            del self.orders[oid]
            self.emit(now, "CANCEL_CLOSE", side=o.side, order=o, qty=o.remaining,
                      price=o.price, reason="post-close settlement lifecycle")
        self.result.conservative_floor_pnl = conservative_floor_pnl(self.inv.policy())
        self.emit(now, "WINDOW_CLOSE",
                  reason=f"matched={self.inv.policy().matched:.6f} floorPnL={self.result.conservative_floor_pnl:.6f}")
        return self.result


async def gamma_winner(slug: str, timeout_s: float) -> str | None:
    deadline = time.time() + timeout_s
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    async with httpx.AsyncClient(timeout=10.0) as hc:
        while time.time() < deadline:
            try:
                r = await hc.get(url)
                if r.status_code == 200:
                    m = r.json()
                    outcomes, prices = m.get("outcomes", []), m.get("outcomePrices", [])
                    if isinstance(outcomes, str):
                        outcomes = json.loads(outcomes)
                    if isinstance(prices, str):
                        prices = json.loads(prices)
                    ranked = []
                    for i, raw in enumerate(prices):
                        try:
                            ranked.append((float(raw), i))
                        except (TypeError, ValueError):
                            pass
                    if ranked:
                        value, idx = max(ranked)
                        if value >= .99 and idx < len(outcomes):
                            label = str(outcomes[idx]).upper()
                            if label in ("UP", "YES"):
                                return "UP"
                            if label in ("DOWN", "NO"):
                                return "DOWN"
            except Exception:
                pass
            await asyncio.sleep(2.0)
    return None


async def choose_market(client: AsyncPublicClient, asset: str, *, clean_start: bool) -> Any:
    while True:
        now = time.time()
        start = window_start_epoch(DURATION_S, now)
        age = now - start
        if clean_start and age > QUOTE_START_AGE_S:
            start += DURATION_S
        if start > now:
            wait = start - now
            print(f"WAIT  {asset.upper()} clean 15m window in {wait:.1f}s")
            await asyncio.sleep(min(5.0, wait))
            continue
        m = await resolve_market(client, asset, DURATION_S, int(start))
        if m is not None and m.seconds_to_end > 0:
            return m
        await asyncio.sleep(1.0)


async def amain(args: argparse.Namespace) -> int:
    assets = tuple(a.strip().lower() for a in args.assets.split(",") if a.strip())
    if not assets or any(a not in ("btc", "eth") for a in assets):
        raise SystemExit("--assets must contain only btc and/or eth")

    prefix = Path(args.out)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    events_path = prefix.parent / f"{prefix.name}_{stamp}_events.csv"
    summary_path = prefix.parent / f"{prefix.name}_{stamp}_summary.json"

    client = AsyncPublicClient()
    final_rows: list[dict[str, Any]] = []
    try:
        with events_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=EVENT_FIELDS)
            writer.writeheader()
            for session in range(1, args.sessions+1):
                clean = (session == 1 and not args.join_current)
                markets = await asyncio.gather(
                    *(choose_market(client, asset, clean_start=clean) for asset in assets)
                )
                engines = [Engine(session=session, market=m, writer=writer, args=args) for m in markets]
                await asyncio.gather(*(e.run(client) for e in engines))
                fh.flush()
                winners = await asyncio.gather(
                    *(gamma_winner(e.market.slug, args.resolution_timeout) for e in engines)
                )
                for e, winner in zip(engines, winners, strict=True):
                    e.result.winner = winner
                    e.result.resolved = winner is not None
                    p = e.inv.policy()
                    if winner:
                        e.result.settlement_value = settlement_value(p, winner)
                        e.result.settlement_pnl = settlement_pnl(p, winner)
                    row = {
                        **asdict(e.result),
                        "up_shares": p.up_shares, "down_shares": p.down_shares,
                        "up_vwap": p.up_vwap, "down_vwap": p.down_vwap,
                        "combined_vwap": p.combined_vwap, "matched_shares": p.matched,
                        "gross_spend": acquisition_spend(p),
                        "paper_cash_after_buys": e.cash,
                        "maker_share": e.result.maker_fills / max(1, e.result.maker_fills+e.result.taker_fills),
                        "completed_set_qty": e.pool.completed_qty,
                        "completed_set_vwap": e.pool.completed_vwap,
                        "unmatched_up_end": e.pool.unmatched_up,
                        "unmatched_down_end": e.pool.unmatched_down,
                        "unmatched_up_vwap_end": e.pool.unmatched_vwap("UP"),
                        "unmatched_down_vwap_end": e.pool.unmatched_vwap("DOWN"),
                    }
                    final_rows.append(row)
                    print("-" * 96)
                    print(json.dumps(row, indent=2, sort_keys=True))

        summary = {
            "generated_utc": _iso(),
            "mode": "READ_ONLY_REAL_15M_BOOKS",
            "strategy": "maximum-identifiable Gabagool 15m reconstruction",
            "maker_fill_proxy": (
                "public SELL tape + per-layer queue-ahead"
                if args.maker_fill_backend == "public_tape"
                else "later-snapshot full-depth cross-through"
            ),
            "quote_ttl_s": args.quote_ttl,
            "quote_ttl_is_historical_claim": False,
            "fresh_pair_cap": args.fresh_pair_cap,
            "fresh_pair_cap_is_historical_claim": False,
            "unmatched_pool": "weighted cost pool; telemetry only; not an admission filter",
            "taker_mode": args.taker_mode,
            "markets": final_rows,
            "aggregate": {
                "markets": len(final_rows),
                "resolved": sum(bool(r["resolved"]) for r in final_rows),
                "maker_fills": sum(int(r["maker_fills"]) for r in final_rows),
                "taker_fills": sum(int(r["taker_fills"]) for r in final_rows),
                "gross_spend": sum(float(r["gross_spend"]) for r in final_rows),
                "settlement_pnl": sum(float(r["settlement_pnl"]) for r in final_rows if r["settlement_pnl"] is not None),
            },
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nEVENTS  {events_path}")
        print(f"SUMMARY {summary_path}")
        return 0
    finally:
        try:
            await client.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-only real-market Gabagool 15m paper harness")
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--poll", type=float, default=.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0, help="independent cash per market")
    ap.add_argument("--taker-mode", choices=("evidence", "off"), default="evidence")
    ap.add_argument(
        "--maker-fill-backend",
        choices=("snapshot_cross", "public_tape"),
        default="snapshot_cross",
        help="snapshot_cross is the current full-parent book-cross proxy; "
             "public_tape uses real SELL prints + per-layer queue-ahead",
    )
    ap.add_argument("--max-combined-vwap", type=float, default=1.01)
    ap.add_argument(
        "--fresh-pair-cap",
        type=float,
        default=0.0,
        help="PARKED paper-only joint-exposure cap on complementary ANCHORS. "
             "<=0 disables (default). Not recovered source.",
    )
    ap.add_argument("--resolution-timeout", type=float, default=240.0)
    ap.add_argument("--join-current", action="store_true",
                    help="join current 15m window; default waits for a clean window if age>18s")
    ap.add_argument("--out", default="data/gabagool_15m_live_v5")
    args = ap.parse_args()
    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(args.poll, args.quote_ttl, args.paper_cash, args.resolution_timeout) <= 0:
        ap.error("poll/quote-ttl/paper-cash/resolution-timeout must be positive")
    return args


def main() -> None:
    raise SystemExit(asyncio.run(amain(parse_args())))


if __name__ == "__main__":
    main()
