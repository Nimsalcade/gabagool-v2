"""Read-only Gabagool forensic V3 execution validator.

V3 freezes the V2 quote/inventory strategy and investigates the execution layer.

The V2 engine remains the CONTROL strategy. Its REST ask-cross fills are the only
fills allowed to change the control inventory and therefore the future quote
stream. Two shadow execution models observe exactly those same posted/cancelled
quotes but NEVER feed fills back into the strategy:

STRICT
    Existing V2 rule: a later sampled ask book must make the full resting clip
    executable at or below the bid. This is the control model.

TOUCH
    Uses the public Polymarket market WebSocket last_trade_price event. A SELL
    trade at or below a resting BUY bid is treated as evidence that the bid was
    touched. Trade size is used as shadow fill capacity.

QUEUE
    Uses the same SELL trade events, but first consumes displayed size observed
    ahead of our hypothetical order at the exact bid price. A SELL trade below
    our bid implies higher bid levels, including ours, were cleared.

Important: TOUCH and QUEUE are execution-forensics shadows, not alternative
strategies. Their shadow fills do not change V2 quotes. Their inventory/PnL
statistics are therefore diagnostics only. If one shadow execution model matches
the chain fingerprint, it can be promoted into a later independent strategy run.

No wallet, private key, signature, order, merge, redeem, or authenticated user
channel is used.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import websockets
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from tools import gabagool_forensic_v1 as v1
from tools import gabagool_forensic_v2 as v2
from tools.metamask_10session_strategy_observer import _d, _iso

ASSET = v2.ASSET
DURATION_S = v2.DURATION_S
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DEFAULT_SESSIONS = 10
DEFAULT_POLL_S = v2.DEFAULT_POLL_S
DEFAULT_CLIP = v2.DEFAULT_CLIP
DEFAULT_QUOTE_PAIR_TARGET = v2.DEFAULT_QUOTE_PAIR_TARGET
DEFAULT_INVENTORY_PAIR_MAX = v2.DEFAULT_INVENTORY_PAIR_MAX
DEFAULT_REQUOTE_S = v2.DEFAULT_REQUOTE_S
DEFAULT_MAX_GAP = v2.DEFAULT_MAX_GAP
DEFAULT_STOP_NEW_SEED_S = v2.DEFAULT_STOP_NEW_SEED_S
DEFAULT_SKEW_TICKS = v2.DEFAULT_SKEW_TICKS

EXECUTION_FIELDS = [
    "utc",
    "session",
    "market",
    "age_s",
    "model",
    "event",
    "side",
    "qty",
    "order_price",
    "trade_price",
    "trade_size",
    "trade_side",
    "transaction_hash",
    "order_age_s",
    "queue_ahead_before",
    "queue_ahead_after",
    "remaining_after",
    "reason",
    "up_shares",
    "up_avg",
    "down_shares",
    "down_avg",
    "gap",
    "matched",
    "pair_basis",
]

ZERO = Decimal("0")


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _median(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return None if not clean else statistics.median(clean)


@dataclass
class ShadowOrder:
    side: str
    price: Decimal
    size: Decimal
    created_ts: float
    reason: str
    remaining: Decimal
    queue_ahead: Decimal | None = None
    queue_initialized_ts: float | None = None


@dataclass
class ShadowLedger:
    name: str
    market_start: float
    inv: v1.Inventory = field(default_factory=v1.Inventory)
    orders: dict[str, ShadowOrder | None] = field(
        default_factory=lambda: {"UP": None, "DOWN": None}
    )
    fill_events: int = 0
    completed_orders: int = 0
    partial_fill_events: int = 0
    gross_spend: Decimal = ZERO
    sides_bought: set[str] = field(default_factory=set)
    first_fill_age_s: float | None = None
    last_fill_age_s: float | None = None
    max_gap: Decimal = ZERO
    imbalanced_before_fill_count: int = 0
    underweight_fill_count: int = 0
    heavy_fill_count: int = 0
    opposite_sums: list[float] = field(default_factory=list)
    opposite_lags: list[float] = field(default_factory=list)
    last_fill_side: str | None = None
    last_fill_price: Decimal | None = None
    last_fill_ts: float | None = None
    quote_posts: int = 0
    quote_cancels: int = 0
    trade_candidates: int = 0
    queue_unknown_posts: int = 0

    def post(
        self,
        *,
        side: str,
        price: Decimal,
        size: Decimal,
        now: float,
        reason: str,
        queue_ahead: Decimal | None = None,
    ) -> None:
        self.orders[side] = ShadowOrder(
            side=side,
            price=price,
            size=size,
            created_ts=now,
            reason=reason,
            remaining=size,
            queue_ahead=queue_ahead,
            queue_initialized_ts=now if queue_ahead is not None else None,
        )
        self.quote_posts += 1
        if self.name == "QUEUE" and queue_ahead is None:
            self.queue_unknown_posts += 1

    def cancel(self, side: str) -> ShadowOrder | None:
        order = self.orders.get(side)
        if order is not None:
            self.orders[side] = None
            self.quote_cancels += 1
        return order

    def record_fill(
        self,
        *,
        side: str,
        qty: Decimal,
        price: Decimal,
        now: float,
    ) -> None:
        if qty <= 0:
            return
        pre_under = self.inv.underweight()
        pre_heavy = self.inv.heavy()
        if pre_under is not None:
            self.imbalanced_before_fill_count += 1
            if side == pre_under:
                self.underweight_fill_count += 1
            elif side == pre_heavy:
                self.heavy_fill_count += 1

        self.inv.add(side, qty, qty * price)
        self.gross_spend += qty * price
        self.sides_bought.add(side)
        self.fill_events += 1

        age = now - self.market_start
        if self.first_fill_age_s is None:
            self.first_fill_age_s = age
        self.last_fill_age_s = age
        self.max_gap = max(self.max_gap, self.inv.gap())

        if (
            self.last_fill_side is not None
            and self.last_fill_side != side
            and self.last_fill_price is not None
            and self.last_fill_ts is not None
        ):
            self.opposite_sums.append(float(self.last_fill_price + price))
            self.opposite_lags.append(now - self.last_fill_ts)

        self.last_fill_side = side
        self.last_fill_price = price
        self.last_fill_ts = now

    def summary(self) -> dict[str, Any]:
        matched = self.inv.matched()
        basis = self.inv.pair_basis()
        edge = ZERO if basis is None else matched * (Decimal("1") - basis)
        under_rate = (
            None
            if self.imbalanced_before_fill_count == 0
            else self.underweight_fill_count / self.imbalanced_before_fill_count
        )
        return {
            "model": self.name,
            "fill_events": self.fill_events,
            "completed_orders": self.completed_orders,
            "partial_fill_events": self.partial_fill_events,
            "quote_posts_seen": self.quote_posts,
            "quote_cancels_seen": self.quote_cancels,
            "trade_candidates": self.trade_candidates,
            "queue_unknown_posts": self.queue_unknown_posts,
            "sides_bought": sorted(self.sides_bought),
            "both_sides": self.sides_bought == {"UP", "DOWN"},
            "gross_spend": str(self.gross_spend),
            "up_shares": str(self.inv.up_shares),
            "up_cost": str(self.inv.up_cost),
            "up_avg": None if self.inv.avg("UP") is None else str(self.inv.avg("UP")),
            "down_shares": str(self.inv.down_shares),
            "down_cost": str(self.inv.down_cost),
            "down_avg": None if self.inv.avg("DOWN") is None else str(self.inv.avg("DOWN")),
            "matched_shares": str(matched),
            "pair_basis": None if basis is None else str(basis),
            "matched_gross_edge": str(edge),
            "residual_up_shares": str(self.inv.up_shares - matched),
            "residual_down_shares": str(self.inv.down_shares - matched),
            "first_fill_age_s": self.first_fill_age_s,
            "last_fill_age_s": self.last_fill_age_s,
            "max_gap_shares": str(self.max_gap),
            "imbalanced_before_fill_count": self.imbalanced_before_fill_count,
            "underweight_fill_count": self.underweight_fill_count,
            "heavy_fill_count": self.heavy_fill_count,
            "underweight_fill_rate": under_rate,
            "opposite_transition_count": len(self.opposite_sums),
            "opposite_sum_mean": (
                None if not self.opposite_sums else statistics.mean(self.opposite_sums)
            ),
            "opposite_sum_median": (
                None if not self.opposite_sums else statistics.median(self.opposite_sums)
            ),
            "opposite_lag_median_s": (
                None if not self.opposite_lags else statistics.median(self.opposite_lags)
            ),
        }


class ExecutionForensicEngine(v2.InventoryAwareForensicEngine):
    """V2 control strategy plus TOUCH and QUEUE shadow execution observers."""

    def __init__(
        self,
        *,
        execution_writer: csv.DictWriter,
        raw_ws_path: Path,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.execution_writer = execution_writer
        self.raw_ws_path = raw_ws_path

        self.shadow = {
            "TOUCH": ShadowLedger("TOUCH", self.market.window_start),
            "QUEUE": ShadowLedger("QUEUE", self.market.window_start),
        }
        self.bid_levels: dict[str, dict[Decimal, Decimal]] = {"UP": {}, "DOWN": {}}
        self.ask_levels: dict[str, dict[Decimal, Decimal]] = {"UP": {}, "DOWN": {}}
        self.token_side = {
            str(self.market.up_token_id): "UP",
            str(self.market.down_token_id): "DOWN",
        }

        self.ws_stop = asyncio.Event()
        self.ws_connected = 0
        self.ws_reconnects = 0
        self.ws_errors = 0
        self.ws_raw_events = 0
        self.ws_book_events = 0
        self.ws_price_change_events = 0
        self.ws_trade_events = 0
        self.ws_sell_trade_events = 0

    def _level_size(self, side: str, price: Decimal) -> Decimal | None:
        levels = self.bid_levels.get(side) or {}
        if price in levels:
            return levels[price]
        # An initialized book with no exact level means no displayed quantity ahead.
        if levels:
            return ZERO
        return None

    def _execution_row(
        self,
        *,
        now: float,
        model: str,
        event: str,
        side: str = "",
        qty: Decimal | None = None,
        order_price: Decimal | None = None,
        trade_price: Decimal | None = None,
        trade_size: Decimal | None = None,
        trade_side: str = "",
        transaction_hash: str = "",
        order_age_s: float | None = None,
        queue_before: Decimal | None = None,
        queue_after: Decimal | None = None,
        remaining_after: Decimal | None = None,
        reason: str = "",
    ) -> None:
        if model == "STRICT":
            inv = self.inv
        else:
            inv = self.shadow[model].inv
        pair = inv.pair_basis()
        self.execution_writer.writerow(
            {
                "utc": _iso(now),
                "session": self.session_no,
                "market": self.market.slug,
                "age_s": f"{now - self.market.window_start:.6f}",
                "model": model,
                "event": event,
                "side": side,
                "qty": "" if qty is None else str(qty),
                "order_price": "" if order_price is None else str(order_price),
                "trade_price": "" if trade_price is None else str(trade_price),
                "trade_size": "" if trade_size is None else str(trade_size),
                "trade_side": trade_side,
                "transaction_hash": transaction_hash,
                "order_age_s": "" if order_age_s is None else f"{order_age_s:.6f}",
                "queue_ahead_before": "" if queue_before is None else str(queue_before),
                "queue_ahead_after": "" if queue_after is None else str(queue_after),
                "remaining_after": "" if remaining_after is None else str(remaining_after),
                "reason": reason,
                "up_shares": str(inv.up_shares),
                "up_avg": "" if inv.avg("UP") is None else str(inv.avg("UP")),
                "down_shares": str(inv.down_shares),
                "down_avg": "" if inv.avg("DOWN") is None else str(inv.avg("DOWN")),
                "gap": str(inv.gap()),
                "matched": str(inv.matched()),
                "pair_basis": "" if pair is None else str(pair),
            }
        )

    def _post(self, side: str, price: Decimal, now: float, reason: str) -> None:
        super()._post(side, price, now, reason)
        # If super rejected the quote, there is no control order to mirror.
        control = self.orders.get(side)
        if control is None or control.created_ts != now or control.price != price:
            return

        self.shadow["TOUCH"].post(
            side=side,
            price=price,
            size=self.clip,
            now=now,
            reason=reason,
        )
        queue_ahead = self._level_size(side, price)
        self.shadow["QUEUE"].post(
            side=side,
            price=price,
            size=self.clip,
            now=now,
            reason=reason,
            queue_ahead=queue_ahead,
        )
        self._execution_row(
            now=now,
            model="TOUCH",
            event="QUOTE",
            side=side,
            qty=self.clip,
            order_price=price,
            remaining_after=self.clip,
            reason=reason,
        )
        self._execution_row(
            now=now,
            model="QUEUE",
            event="QUOTE",
            side=side,
            qty=self.clip,
            order_price=price,
            queue_after=queue_ahead,
            remaining_after=self.clip,
            reason=reason,
        )

    def _cancel(self, side: str, now: float, reason: str, *, quiet: bool = True) -> None:
        control = self.orders.get(side)
        super()._cancel(side, now, reason, quiet=quiet)
        if control is None:
            return
        for model in ("TOUCH", "QUEUE"):
            order = self.shadow[model].cancel(side)
            if order is None:
                continue
            self._execution_row(
                now=now,
                model=model,
                event="CANCEL",
                side=side,
                qty=order.remaining,
                order_price=order.price,
                order_age_s=now - order.created_ts,
                queue_before=order.queue_ahead if model == "QUEUE" else None,
                remaining_after=ZERO,
                reason=reason,
            )

    def _record_fill(
        self,
        side: str,
        qty: Decimal,
        price: Decimal,
        now: float,
        reason: str,
    ) -> None:
        order = self.orders.get(side)
        created = None if order is None else order.created_ts

        # A STRICT fill removes the control order from the quote stream. Shadow
        # models must therefore stop observing that specific resting order too,
        # even if their alternative execution rule had not credited a fill.
        for model in ("TOUCH", "QUEUE"):
            shadow_order = self.shadow[model].cancel(side)
            if shadow_order is not None:
                self._execution_row(
                    now=now,
                    model=model,
                    event="CONTROL_REMOVE",
                    side=side,
                    qty=shadow_order.remaining,
                    order_price=shadow_order.price,
                    order_age_s=now - shadow_order.created_ts,
                    queue_before=(
                        shadow_order.queue_ahead if model == "QUEUE" else None
                    ),
                    remaining_after=ZERO,
                    reason="control STRICT fill removed resting quote",
                )

        super()._record_fill(side, qty, price, now, reason)
        self._execution_row(
            now=now,
            model="STRICT",
            event="FILL",
            side=side,
            qty=qty,
            order_price=price,
            order_age_s=None if created is None else now - created,
            remaining_after=ZERO,
            reason=reason,
        )

    def _update_book(self, data: dict[str, Any], now: float) -> None:
        token = str(data.get("asset_id") or "")
        side = self.token_side.get(token)
        if side is None:
            return
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for raw in data.get("bids") or []:
            p = _dec(raw.get("price"))
            q = _dec(raw.get("size"))
            if p is not None and q is not None and q > 0:
                bids[p] = q
        for raw in data.get("asks") or []:
            p = _dec(raw.get("price"))
            q = _dec(raw.get("size"))
            if p is not None and q is not None and q > 0:
                asks[p] = q
        self.bid_levels[side] = bids
        self.ask_levels[side] = asks
        self._initialize_unknown_queue(side, now)

    def _update_price_changes(self, data: dict[str, Any], now: float) -> None:
        for change in data.get("price_changes") or []:
            token = str(change.get("asset_id") or "")
            side = self.token_side.get(token)
            if side is None:
                continue
            price = _dec(change.get("price"))
            size = _dec(change.get("size"))
            book_side = str(change.get("side") or "").upper()
            if price is None or size is None:
                continue
            levels = self.bid_levels[side] if book_side == "BUY" else self.ask_levels[side]
            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            if book_side == "BUY":
                self._initialize_unknown_queue(side, now)

    def _initialize_unknown_queue(self, side: str, now: float) -> None:
        order = self.shadow["QUEUE"].orders.get(side)
        if order is None or order.queue_ahead is not None:
            return
        observed = self._level_size(side, order.price)
        if observed is None:
            return
        order.queue_ahead = observed
        order.queue_initialized_ts = now
        self._execution_row(
            now=now,
            model="QUEUE",
            event="QUEUE_INIT",
            side=side,
            qty=ZERO,
            order_price=order.price,
            order_age_s=now - order.created_ts,
            queue_after=observed,
            remaining_after=order.remaining,
            reason="first exact-price displayed bid snapshot after quote",
        )

    def _apply_shadow_fill(
        self,
        *,
        model: str,
        order: ShadowOrder,
        qty: Decimal,
        now: float,
        trade_price: Decimal,
        trade_size: Decimal,
        trade_side: str,
        transaction_hash: str,
        reason: str,
        queue_before: Decimal | None = None,
        queue_after: Decimal | None = None,
    ) -> None:
        qty = min(qty, order.remaining)
        if qty <= 0:
            return
        ledger = self.shadow[model]
        before_remaining = order.remaining
        order.remaining -= qty
        if qty < before_remaining:
            ledger.partial_fill_events += 1
        ledger.record_fill(side=order.side, qty=qty, price=order.price, now=now)

        completed = order.remaining <= 0
        if completed:
            ledger.completed_orders += 1
            ledger.orders[order.side] = None

        self._execution_row(
            now=now,
            model=model,
            event="FILL",
            side=order.side,
            qty=qty,
            order_price=order.price,
            trade_price=trade_price,
            trade_size=trade_size,
            trade_side=trade_side,
            transaction_hash=transaction_hash,
            order_age_s=now - order.created_ts,
            queue_before=queue_before,
            queue_after=queue_after,
            remaining_after=max(ZERO, order.remaining),
            reason=reason,
        )
        print(
            f"{model}_FILL {order.side} {qty}@{order.price:.6f} "
            f"trade={trade_side} {trade_size}@{trade_price:.6f} "
            f"age={now - order.created_ts:.2f}s"
        )

    def _process_sell_trade(self, data: dict[str, Any], recv_ts: float) -> None:
        token = str(data.get("asset_id") or "")
        side = self.token_side.get(token)
        if side is None:
            return
        trade_side = str(data.get("side") or "").upper()
        if trade_side != "SELL":
            return

        trade_price = _dec(data.get("price"))
        trade_size = _dec(data.get("size"))
        if trade_price is None or trade_size is None or trade_size <= 0:
            return
        tx_hash = str(data.get("transaction_hash") or "")
        self.ws_sell_trade_events += 1

        # TOUCH: if a SELL execution prints at/below our resting BUY bid, the
        # hypothetical bid was at least economically touchable. Exact-price
        # volume is used as capacity; a print below the bid implies the bid level
        # was cleared, so the remaining clip is credited.
        touch = self.shadow["TOUCH"]
        t_order = touch.orders.get(side)
        if t_order is not None and recv_ts >= t_order.created_ts and trade_price <= t_order.price:
            touch.trade_candidates += 1
            qty = t_order.remaining if trade_price < t_order.price else min(
                t_order.remaining, trade_size
            )
            self._apply_shadow_fill(
                model="TOUCH",
                order=t_order,
                qty=qty,
                now=recv_ts,
                trade_price=trade_price,
                trade_size=trade_size,
                trade_side=trade_side,
                transaction_hash=tx_hash,
                reason=(
                    "SELL trade below bid cleared level"
                    if trade_price < t_order.price
                    else "SELL trade touched exact bid; no queue deduction"
                ),
            )

        # QUEUE: same evidence, but exact-price SELL volume must first consume
        # displayed quantity observed ahead of the hypothetical order.
        queue = self.shadow["QUEUE"]
        q_order = queue.orders.get(side)
        if q_order is None or recv_ts < q_order.created_ts or trade_price > q_order.price:
            return
        queue.trade_candidates += 1

        if trade_price < q_order.price:
            before = q_order.queue_ahead
            q_order.queue_ahead = ZERO
            self._apply_shadow_fill(
                model="QUEUE",
                order=q_order,
                qty=q_order.remaining,
                now=recv_ts,
                trade_price=trade_price,
                trade_size=trade_size,
                trade_side=trade_side,
                transaction_hash=tx_hash,
                reason="SELL trade printed below bid; bid level necessarily cleared",
                queue_before=before,
                queue_after=ZERO,
            )
            return

        # Exact price. Unknown queue is intentionally conservative: initialize
        # from the latest exact-price displayed level if possible; otherwise do
        # not grant a fill from this print.
        if q_order.queue_ahead is None:
            observed = self._level_size(side, q_order.price)
            if observed is None:
                self._execution_row(
                    now=recv_ts,
                    model="QUEUE",
                    event="QUEUE_UNKNOWN",
                    side=side,
                    qty=ZERO,
                    order_price=q_order.price,
                    trade_price=trade_price,
                    trade_size=trade_size,
                    trade_side=trade_side,
                    transaction_hash=tx_hash,
                    order_age_s=recv_ts - q_order.created_ts,
                    remaining_after=q_order.remaining,
                    reason="exact-price SELL trade but no displayed queue snapshot",
                )
                return
            q_order.queue_ahead = observed
            q_order.queue_initialized_ts = recv_ts

        before = q_order.queue_ahead
        volume = trade_size
        if before > 0:
            consumed = min(before, volume)
            q_order.queue_ahead -= consumed
            volume -= consumed
        after = q_order.queue_ahead

        if volume <= 0:
            self._execution_row(
                now=recv_ts,
                model="QUEUE",
                event="QUEUE_CONSUME",
                side=side,
                qty=ZERO,
                order_price=q_order.price,
                trade_price=trade_price,
                trade_size=trade_size,
                trade_side=trade_side,
                transaction_hash=tx_hash,
                order_age_s=recv_ts - q_order.created_ts,
                queue_before=before,
                queue_after=after,
                remaining_after=q_order.remaining,
                reason="exact-price SELL volume consumed displayed queue ahead",
            )
            return

        self._apply_shadow_fill(
            model="QUEUE",
            order=q_order,
            qty=min(q_order.remaining, volume),
            now=recv_ts,
            trade_price=trade_price,
            trade_size=trade_size,
            trade_side=trade_side,
            transaction_hash=tx_hash,
            reason="exact-price SELL volume exceeded displayed queue ahead",
            queue_before=before,
            queue_after=after,
        )

    async def _ws_loop(self) -> None:
        self.raw_ws_path.parent.mkdir(parents=True, exist_ok=True)
        with self.raw_ws_path.open("w", encoding="utf-8") as raw:
            while not self.ws_stop.is_set() and time.time() < self.market.window_end:
                try:
                    async with websockets.connect(
                        MARKET_WS_URL,
                        ping_interval=None,
                        close_timeout=5,
                        max_size=8 * 1024 * 1024,
                    ) as ws:
                        self.ws_connected += 1
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

                        async def pinger() -> None:
                            while not self.ws_stop.is_set():
                                await asyncio.sleep(10)
                                try:
                                    await ws.send("PING")
                                except Exception:
                                    return

                        ping_task = asyncio.create_task(pinger())
                        try:
                            while (
                                not self.ws_stop.is_set()
                                and time.time() < self.market.window_end
                            ):
                                timeout = max(
                                    0.1,
                                    min(5.0, self.market.window_end - time.time()),
                                )
                                try:
                                    message = await asyncio.wait_for(
                                        ws.recv(), timeout=timeout
                                    )
                                except asyncio.TimeoutError:
                                    continue
                                if not isinstance(message, str):
                                    continue
                                text = message.strip()
                                if not text or text in {"PING", "PONG"}:
                                    continue
                                recv_ts = time.time()
                                try:
                                    parsed = json.loads(text)
                                except json.JSONDecodeError:
                                    continue
                                events = parsed if isinstance(parsed, list) else [parsed]
                                for data in events:
                                    if not isinstance(data, dict):
                                        continue
                                    self.ws_raw_events += 1
                                    raw.write(
                                        json.dumps(
                                            {
                                                "recv_ts": recv_ts,
                                                "session": self.session_no,
                                                "market": self.market.slug,
                                                "payload": data,
                                            },
                                            separators=(",", ":"),
                                        )
                                        + "\n"
                                    )
                                    raw.flush()

                                    event_type = str(data.get("event_type") or "")
                                    if event_type == "book":
                                        self.ws_book_events += 1
                                        self._update_book(data, recv_ts)
                                    elif event_type == "price_change":
                                        self.ws_price_change_events += 1
                                        self._update_price_changes(data, recv_ts)
                                    elif event_type == "last_trade_price":
                                        self.ws_trade_events += 1
                                        self._process_sell_trade(data, recv_ts)
                        finally:
                            ping_task.cancel()
                            await asyncio.gather(ping_task, return_exceptions=True)
                except Exception as exc:  # noqa: BLE001
                    self.ws_errors += 1
                    if self.ws_stop.is_set() or time.time() >= self.market.window_end:
                        break
                    self.ws_reconnects += 1
                    print(f"WS ERR     {type(exc).__name__}: {exc}; reconnecting")
                    await asyncio.sleep(0.25)

    def _strict_execution_summary(self, base: dict[str, Any]) -> dict[str, Any]:
        return {
            "model": "STRICT",
            "fill_events": int(base.get("maker_proxy_fills", 0)),
            "completed_orders": int(base.get("maker_proxy_fills", 0)),
            "partial_fill_events": 0,
            "sides_bought": base.get("sides_bought", []),
            "both_sides": set(base.get("sides_bought", [])) == {"UP", "DOWN"},
            "gross_spend": base.get("gross_spend"),
            "up_shares": base.get("up_shares"),
            "up_cost": base.get("up_cost"),
            "up_avg": base.get("up_avg"),
            "down_shares": base.get("down_shares"),
            "down_cost": base.get("down_cost"),
            "down_avg": base.get("down_avg"),
            "matched_shares": base.get("matched_shares"),
            "pair_basis": base.get("pair_basis"),
            "matched_gross_edge": base.get("harvest_gross_pnl"),
            "residual_up_shares": base.get("residual_up_shares"),
            "residual_down_shares": base.get("residual_down_shares"),
            "first_fill_age_s": base.get("first_fill_age_s"),
            "last_fill_age_s": base.get("last_fill_age_s"),
            "max_gap_shares": base.get("max_gap_shares"),
            "underweight_fill_rate": base.get("underweight_fill_rate"),
            "opposite_transition_count": base.get("opposite_transition_count"),
            "opposite_sum_mean": base.get("opposite_sum_mean"),
            "opposite_sum_median": base.get("opposite_sum_median"),
            "opposite_lag_median_s": base.get("opposite_lag_median_s"),
        }

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        ws_task = asyncio.create_task(self._ws_loop())
        await asyncio.sleep(0)
        try:
            base = await super().run(client)
        finally:
            self.ws_stop.set()
            await asyncio.gather(ws_task, return_exceptions=True)

        models = {
            "STRICT": self._strict_execution_summary(base),
            "TOUCH": self.shadow["TOUCH"].summary(),
            "QUEUE": self.shadow["QUEUE"].summary(),
        }
        base["execution_models"] = models
        base["websocket"] = {
            "url": MARKET_WS_URL,
            "connections": self.ws_connected,
            "reconnects": self.ws_reconnects,
            "errors": self.ws_errors,
            "raw_events": self.ws_raw_events,
            "book_events": self.ws_book_events,
            "price_change_events": self.ws_price_change_events,
            "last_trade_price_events": self.ws_trade_events,
            "sell_trade_events": self.ws_sell_trade_events,
            "raw_path": str(self.raw_ws_path),
        }

        print("EXECUTION   same V2 quote stream, shadow fills do not feed back")
        for name in ("STRICT", "TOUCH", "QUEUE"):
            m = models[name]
            print(
                f"{name:<10} fills={m['fill_events']} completed={m['completed_orders']} "
                f"bothSides={m['both_sides']} first={m['first_fill_age_s']} "
                f"last={m['last_fill_age_s']} matched={m['matched_shares']} "
                f"basis={m['pair_basis']}"
            )
        print(
            f"WS          trades={self.ws_trade_events} sellTrades={self.ws_sell_trade_events} "
            f"errors={self.ws_errors} reconnects={self.ws_reconnects}"
        )
        return base


async def _resolve_wait(client: AsyncPublicClient, target_start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, target_start)
        if market is not None:
            return market
        await asyncio.sleep(1.0)


def _aggregate_model(
    summaries: list[dict[str, Any]],
    model: str,
) -> dict[str, Any]:
    rows = [s["execution_models"][model] for s in summaries]
    with_fills = [r for r in rows if int(r.get("fill_events", 0)) > 0]
    both = [r for r in rows if bool(r.get("both_sides"))]

    matched_total = sum(Decimal(str(r.get("matched_shares") or "0")) for r in rows)
    matched_cost = ZERO
    for r in rows:
        if r.get("pair_basis") is None:
            continue
        matched = Decimal(str(r.get("matched_shares") or "0"))
        matched_cost += matched * Decimal(str(r["pair_basis"]))
    weighted_basis = None if matched_total <= 0 else matched_cost / matched_total

    return {
        "model": model,
        "sessions": len(rows),
        "sessions_with_fills": len(with_fills),
        "both_sides_markets": len(both),
        "both_sides_rate": None if not rows else len(both) / len(rows),
        "total_fill_events": sum(int(r.get("fill_events", 0)) for r in rows),
        "total_completed_orders": sum(int(r.get("completed_orders", 0)) for r in rows),
        "total_partial_fill_events": sum(int(r.get("partial_fill_events", 0)) for r in rows),
        "total_matched_shares": str(matched_total),
        "pair_weighted_combined_basis": (
            None if weighted_basis is None else str(weighted_basis)
        ),
        "median_first_fill_age_s": _median(
            [r.get("first_fill_age_s") for r in with_fills]
        ),
        "median_last_fill_age_s": _median(
            [r.get("last_fill_age_s") for r in with_fills]
        ),
        "median_fill_events_per_active_market": _median(
            [float(r.get("fill_events", 0)) for r in with_fills]
        ),
        "median_completed_orders_per_active_market": _median(
            [float(r.get("completed_orders", 0)) for r in with_fills]
        ),
        "median_opposite_sum": _median(
            [r.get("opposite_sum_median") for r in with_fills]
        ),
    }


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    ws_dir = run_dir / "ws_raw"
    ws_dir.mkdir(parents=True, exist_ok=True)

    activity_path = run_dir / "activity.csv"
    execution_path = run_dir / "execution.csv"
    summary_path = run_dir / "summary.json"

    print("READ ONLY   no wallet, no key, no orders, no real merge/redeem")
    print("OBJECTIVE   isolate execution model while freezing V2 strategy")
    print("VERSION     forensic V3: STRICT vs TOUCH vs QUEUE")
    print(
        f"PLAN        {args.sessions} consecutive BTC 5m markets | "
        f"clip={args.clip}sh | poll={args.poll:.2f}s"
    )
    print(
        f"STRATEGY    V2 control unchanged | soft={args.quote_pair_target} | "
        f"hard={args.inventory_pair_max} | gap={args.max_gap} | skew={args.skew_ticks}"
    )
    print("EXECUTION   STRICT controls quotes; TOUCH/QUEUE are non-feedback shadows")
    print(f"WS          {MARKET_WS_URL} | public market channel | raw capture enabled")
    print(f"OUTPUT      {run_dir}")

    client = AsyncPublicClient()
    summaries: list[dict[str, Any]] = []
    start = window_start_epoch(DURATION_S, time.time()) + DURATION_S

    try:
        with (
            activity_path.open("w", newline="", encoding="utf-8") as activity_fh,
            execution_path.open("w", newline="", encoding="utf-8") as execution_fh,
        ):
            activity_writer = csv.DictWriter(
                activity_fh, fieldnames=v1.ACTIVITY_FIELDS
            )
            activity_writer.writeheader()
            execution_writer = csv.DictWriter(
                execution_fh, fieldnames=EXECUTION_FIELDS
            )
            execution_writer.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(
                        f"WAIT        session {idx + 1}/{args.sessions} "
                        f"starts in {wait:.1f}s"
                    )
                    await asyncio.sleep(wait)

                market = await _resolve_wait(client, target)
                raw_ws_path = ws_dir / f"{idx + 1:03d}_{market.slug}.jsonl"
                engine = ExecutionForensicEngine(
                    session_no=idx + 1,
                    market=market,
                    writer=activity_writer,
                    execution_writer=execution_writer,
                    raw_ws_path=raw_ws_path,
                    clip=args.clip,
                    poll_s=args.poll,
                    quote_pair_target=args.quote_pair_target,
                    inventory_pair_max=args.inventory_pair_max,
                    requote_s=args.requote,
                    max_gap=args.max_gap,
                    stop_new_seed_s=args.stop_new_seed,
                    skew_ticks=args.skew_ticks,
                )
                summaries.append(await engine.run(client))
                activity_fh.flush()
                execution_fh.flush()

        aggregates = {
            model: _aggregate_model(summaries, model)
            for model in ("STRICT", "TOUCH", "QUEUE")
        }
        ws_total = {
            key: sum(int(s["websocket"].get(key, 0)) for s in summaries)
            for key in (
                "connections",
                "reconnects",
                "errors",
                "raw_events",
                "book_events",
                "price_change_events",
                "last_trade_price_events",
                "sell_trade_events",
            )
        }

        result = {
            "created_utc": _iso(),
            "version": "gabagool_forensic_v3",
            "config": {
                "asset": ASSET,
                "duration_s": DURATION_S,
                "sessions": args.sessions,
                "poll_s": args.poll,
                "clip": str(args.clip),
                "quote_pair_target": str(args.quote_pair_target),
                "inventory_pair_max": str(args.inventory_pair_max),
                "requote_s": args.requote,
                "max_gap": str(args.max_gap),
                "stop_new_seed_s": args.stop_new_seed,
                "skew_ticks": args.skew_ticks,
                "control_execution_model": "STRICT",
                "shadow_models_feed_back": False,
                "taker_repair": False,
                "mid_market_merge": False,
            },
            "execution_model_definitions": {
                "STRICT": (
                    "V2 control: full clip credited only when later sampled ask "
                    "book is executable at/below resting bid."
                ),
                "TOUCH": (
                    "Shadow: public last_trade_price SELL print at/below a resting "
                    "BUY bid. Exact-price trade size limits fill quantity; a lower "
                    "print clears the remaining hypothetical bid."
                ),
                "QUEUE": (
                    "Shadow: TOUCH evidence plus displayed exact-price bid size "
                    "ahead of the hypothetical order. Exact-price SELL volume first "
                    "consumes queue ahead; a lower print clears the bid."
                ),
            },
            "aggregate_by_execution_model": aggregates,
            "websocket_totals": ws_total,
            "sessions": summaries,
            "files": {
                "activity_csv": str(activity_path),
                "execution_csv": str(execution_path),
                "summary_json": str(summary_path),
                "ws_raw_dir": str(ws_dir),
            },
            "interpretation": (
                "V3 is an execution-forensics experiment. V2 STRICT fills alone "
                "drive strategy inventory and future quotes. TOUCH and QUEUE receive "
                "the same quote/cancel stream but their fills never feed back. Their "
                "inventory economics are diagnostics, not standalone strategy PnL. "
                "The purpose is to determine whether public trade/queue evidence "
                "better reproduces chain fill density and timing than the strict "
                "ask-cross proxy before changing the strategy."
            ),
        }
        summary_path.write_text(
            json.dumps(result, indent=2, default=str), encoding="utf-8"
        )

        print("\n" + "=" * 96)
        print("GABAGOOL FORENSIC V3 SUMMARY")
        print(f"SESSIONS    {args.sessions}")
        for model in ("STRICT", "TOUCH", "QUEUE"):
            a = aggregates[model]
            rate = a["both_sides_rate"]
            rate_text = "n/a" if rate is None else f"{rate:.1%}"
            print(
                f"{model:<10} fills={a['total_fill_events']} "
                f"completed={a['total_completed_orders']} "
                f"twoSided={a['both_sides_markets']}/{a['sessions']} ({rate_text}) "
                f"medianFirst={a['median_first_fill_age_s']} "
                f"medianLast={a['median_last_fill_age_s']} "
                f"medianActiveFills={a['median_fill_events_per_active_market']} "
                f"oppMedian={a['median_opposite_sum']}"
            )
        print(
            f"WS          trades={ws_total['last_trade_price_events']} "
            f"sellTrades={ws_total['sell_trade_events']} "
            f"errors={ws_total['errors']} reconnects={ws_total['reconnects']}"
        )
        print(f"ACTIVITY    {activity_path}")
        print(f"EXECUTION   {execution_path}")
        print(f"SUMMARY     {summary_path}")
        print(f"RAW WS      {ws_dir}")
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only Gabagool V3 execution-forensics validator"
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    parser.add_argument("--clip", type=Decimal, default=DEFAULT_CLIP)
    parser.add_argument(
        "--quote-pair-target",
        type=Decimal,
        default=DEFAULT_QUOTE_PAIR_TARGET,
    )
    parser.add_argument(
        "--inventory-pair-max",
        type=Decimal,
        default=DEFAULT_INVENTORY_PAIR_MAX,
    )
    parser.add_argument("--requote", type=float, default=DEFAULT_REQUOTE_S)
    parser.add_argument("--max-gap", type=Decimal, default=DEFAULT_MAX_GAP)
    parser.add_argument(
        "--stop-new-seed",
        type=float,
        default=DEFAULT_STOP_NEW_SEED_S,
    )
    parser.add_argument("--skew-ticks", type=int, default=DEFAULT_SKEW_TICKS)
    parser.add_argument(
        "--output",
        default="data/gabagool_forensic_v3",
    )
    args = parser.parse_args()

    if not (1 <= args.sessions <= 200):
        parser.error("--sessions must be 1..200")
    if not (0.1 <= args.poll <= 5.0):
        parser.error("--poll must be 0.1..5.0")
    if args.clip <= 0:
        parser.error("--clip must be positive")
    if not (Decimal("0.95") <= args.quote_pair_target < Decimal("1")):
        parser.error("--quote-pair-target must be in [0.95, 1)")
    if not (
        args.quote_pair_target <= args.inventory_pair_max < Decimal("1")
    ):
        parser.error("--inventory-pair-max must be >= quote target and < 1")
    if args.requote <= 0:
        parser.error("--requote must be positive")
    if args.max_gap < args.clip:
        parser.error("--max-gap must be >= clip")
    if args.stop_new_seed < 0 or args.stop_new_seed >= DURATION_S:
        parser.error("--stop-new-seed must be in [0, 300)")
    if args.skew_ticks < 0 or args.skew_ticks > 10:
        parser.error("--skew-ticks must be 0..10")

    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
