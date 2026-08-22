"""Read-only Gabagool forensic V3 execution validator.

V3 freezes the V2 strategy logic and changes the validation layer only.

The same V2 quote/cancel stream is audited under four execution interpretations:

1. strict_ask_cross
   Exactly the V2 paper-fill rule. A resting BUY is credited only when a later
   polled ask book makes the full clip executable at or below the resting bid.

2. trade_touch_any
   Uses public CLOB ``last_trade_price`` executions. A trade below the resting
   bid implies the quote would have been swept; a trade exactly at the bid
   credits up to the printed trade size. The event's ``side`` flag is ignored.

3. trade_touch_sell_flag
   Same trade-touch rule, but only for ``last_trade_price`` events whose public
   ``side`` field is SELL. This is reported separately because the public event
   exposes a side flag but V3 does not assume undocumented aggressor semantics.

4. queue_conservative_sell_flag
   Uses SELL-flagged trade prints plus the displayed external BUY size already
   resting at our quote price when the V2 quote was posted. Same-price prints
   consume that queue-ahead estimate before our hypothetical quote receives a
   fill. A print below our bid is treated as a sweep-through and fills the
   remaining hypothetical quote. Cancellations ahead are NOT credited, making
   this queue model deliberately conservative.

Important: the shadow models do not feed fills back into strategy decisions.
They audit one frozen V2 quote stream so execution assumptions can be compared
without changing the quote-generation strategy. This is an execution-forensics
experiment, not a self-consistent alternative trading engine.

No wallet, private key, signature, order, merge, or redeem is ever requested.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import websockets
from polymarket import AsyncPublicClient

from src.discovery import window_start_epoch
from tools import gabagool_forensic_v1 as v1
from tools import gabagool_forensic_v2 as v2
from tools.metamask_btc5m_recorder import MARKET_WS_URL

ASSET = v2.ASSET
DURATION_S = v2.DURATION_S

DEFAULT_SESSIONS = 10
DEFAULT_POLL_S = v2.DEFAULT_POLL_S
DEFAULT_CLIP = v2.DEFAULT_CLIP
DEFAULT_QUOTE_PAIR_TARGET = v2.DEFAULT_QUOTE_PAIR_TARGET
DEFAULT_INVENTORY_PAIR_MAX = v2.DEFAULT_INVENTORY_PAIR_MAX
DEFAULT_REQUOTE_S = v2.DEFAULT_REQUOTE_S
DEFAULT_MAX_GAP = v2.DEFAULT_MAX_GAP
DEFAULT_STOP_NEW_SEED_S = v2.DEFAULT_STOP_NEW_SEED_S
DEFAULT_SKEW_TICKS = v2.DEFAULT_SKEW_TICKS
DEFAULT_PRECONNECT_S = 2.0
DEFAULT_RESOLUTION_GRACE_S = 90.0

MODEL_STRICT = "strict_ask_cross"
MODEL_TOUCH = "trade_touch_any"
MODEL_SELL_TOUCH = "trade_touch_sell_flag"
MODEL_QUEUE = "queue_conservative_sell_flag"
MODEL_NAMES = (MODEL_STRICT, MODEL_TOUCH, MODEL_SELL_TOUCH, MODEL_QUEUE)

EXECUTION_FIELDS = [
    "utc",
    "session",
    "market",
    "market_age_s",
    "event",
    "model",
    "quote_id",
    "side",
    "qty",
    "quote_price",
    "trade_price",
    "trade_size",
    "trade_side_flag",
    "quote_age_s",
    "queue_ahead_before",
    "queue_ahead_after",
    "exchange_ts_ms",
    "transaction_hash",
    "reason",
    "up_shares",
    "up_avg",
    "down_shares",
    "down_avg",
    "gap",
    "matched",
    "pair_basis",
    "gross_spend",
]

EPS = Decimal("0.0000000001")


def _d(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _median(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return None if not clean else statistics.median(clean)


def _mean(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return None if not clean else statistics.mean(clean)


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).isoformat()


@dataclass
class ShadowInventory:
    up_shares: Decimal = Decimal(0)
    up_cost: Decimal = Decimal(0)
    down_shares: Decimal = Decimal(0)
    down_cost: Decimal = Decimal(0)

    def shares(self, side: str) -> Decimal:
        return self.up_shares if side == "UP" else self.down_shares

    def avg(self, side: str) -> Decimal | None:
        q = self.shares(side)
        if q <= 0:
            return None
        cost = self.up_cost if side == "UP" else self.down_cost
        return cost / q

    def add(self, side: str, qty: Decimal, price: Decimal) -> None:
        if side == "UP":
            self.up_shares += qty
            self.up_cost += qty * price
        else:
            self.down_shares += qty
            self.down_cost += qty * price

    def matched(self) -> Decimal:
        return min(self.up_shares, self.down_shares)

    def gap_signed(self) -> Decimal:
        return self.up_shares - self.down_shares

    def gap(self) -> Decimal:
        return abs(self.gap_signed())

    def underweight(self) -> str | None:
        gap = self.gap_signed()
        if gap > 0:
            return "DOWN"
        if gap < 0:
            return "UP"
        return None

    def heavy(self) -> str | None:
        gap = self.gap_signed()
        if gap > 0:
            return "UP"
        if gap < 0:
            return "DOWN"
        return None

    def pair_basis(self) -> Decimal | None:
        if self.matched() <= 0:
            return None
        up = self.avg("UP")
        down = self.avg("DOWN")
        if up is None or down is None:
            return None
        return up + down


@dataclass
class ModelQuoteState:
    remaining: Decimal
    queue_ahead: Decimal | None = None


@dataclass
class QuoteLifecycle:
    quote_id: int
    side: str
    price: Decimal
    size: Decimal
    created_ts: float
    reason: str
    models: dict[str, ModelQuoteState] = field(default_factory=dict)


@dataclass
class ModelStats:
    name: str
    inv: ShadowInventory = field(default_factory=ShadowInventory)
    fill_events: int = 0
    fill_shares: Decimal = Decimal(0)
    gross_spend: Decimal = Decimal(0)
    sides_bought: set[str] = field(default_factory=set)
    first_fill_age: float | None = None
    last_fill_age: float | None = None
    max_gap_seen: Decimal = Decimal(0)
    imbalanced_before_fill_count: int = 0
    underweight_fill_count: int = 0
    heavy_fill_count: int = 0
    last_fill_side: str | None = None
    last_fill_price: Decimal | None = None
    last_fill_ts: float | None = None
    opposite_sums: list[float] = field(default_factory=list)
    opposite_lags: list[float] = field(default_factory=list)

    def record_fill(
        self,
        *,
        side: str,
        qty: Decimal,
        price: Decimal,
        now: float,
        window_start: float,
    ) -> tuple[Decimal | None, float | None]:
        if qty <= 0:
            return None, None

        pre_under = self.inv.underweight()
        pre_heavy = self.inv.heavy()
        if pre_under is not None:
            self.imbalanced_before_fill_count += 1
            if side == pre_under:
                self.underweight_fill_count += 1
            elif side == pre_heavy:
                self.heavy_fill_count += 1

        self.inv.add(side, qty, price)
        self.fill_events += 1
        self.fill_shares += qty
        self.gross_spend += qty * price
        self.sides_bought.add(side)

        age = now - window_start
        if self.first_fill_age is None:
            self.first_fill_age = age
        self.last_fill_age = age
        self.max_gap_seen = max(self.max_gap_seen, self.inv.gap())

        opposite_sum: Decimal | None = None
        opposite_lag: float | None = None
        if (
            self.last_fill_side is not None
            and self.last_fill_side != side
            and self.last_fill_price is not None
            and self.last_fill_ts is not None
        ):
            opposite_sum = self.last_fill_price + price
            opposite_lag = now - self.last_fill_ts
            self.opposite_sums.append(float(opposite_sum))
            self.opposite_lags.append(opposite_lag)

        self.last_fill_side = side
        self.last_fill_price = price
        self.last_fill_ts = now
        return opposite_sum, opposite_lag

    def summary(self, winner: str | None) -> dict[str, Any]:
        matched = self.inv.matched()
        basis = self.inv.pair_basis()
        residual_up = self.inv.up_shares - matched
        residual_down = self.inv.down_shares - matched
        harvest_gross = Decimal(0) if basis is None else matched * (Decimal(1) - basis)

        settlement = None
        if winner in {"UP", "DOWN"}:
            winner_residual = residual_up if winner == "UP" else residual_down
            proceeds = matched + winner_residual
            settlement = {
                "winner": winner,
                "matched_merge_proceeds": str(matched),
                "winner_residual_redeem": str(winner_residual),
                "total_proceeds": str(proceeds),
                "gross_acquisition_spend": str(self.gross_spend),
                "paper_total_pnl": str(proceeds - self.gross_spend),
            }

        under_rate = (
            None
            if self.imbalanced_before_fill_count == 0
            else self.underweight_fill_count / self.imbalanced_before_fill_count
        )

        return {
            "model": self.name,
            "fill_events": self.fill_events,
            "fill_shares": str(self.fill_shares),
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
            "matched_harvest_gross_pnl": str(harvest_gross),
            "residual_up_shares": str(residual_up),
            "residual_down_shares": str(residual_down),
            "first_fill_age_s": self.first_fill_age,
            "last_fill_age_s": self.last_fill_age,
            "max_gap_shares": str(self.max_gap_seen),
            "imbalanced_before_fill_count": self.imbalanced_before_fill_count,
            "underweight_fill_count": self.underweight_fill_count,
            "heavy_fill_count": self.heavy_fill_count,
            "underweight_fill_rate": under_rate,
            "opposite_transition_count": len(self.opposite_sums),
            "opposite_sum_mean": None if not self.opposite_sums else statistics.mean(self.opposite_sums),
            "opposite_sum_median": None if not self.opposite_sums else statistics.median(self.opposite_sums),
            "opposite_lag_median_s": None if not self.opposite_lags else statistics.median(self.opposite_lags),
            "settlement": settlement,
        }


class ExecutionAudit:
    def __init__(
        self,
        *,
        session_no: int,
        market: Any,
        writer: csv.DictWriter,
        execution_fh: Any,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.writer = writer
        self.execution_fh = execution_fh

        self.models = {name: ModelStats(name) for name in MODEL_NAMES}
        self.current_quotes: dict[str, QuoteLifecycle | None] = {"UP": None, "DOWN": None}
        self.next_quote_id = 1

        self.bid_levels: dict[str, dict[Decimal, Decimal]] = {"UP": {}, "DOWN": {}}
        self.ask_levels: dict[str, dict[Decimal, Decimal]] = {"UP": {}, "DOWN": {}}
        self.have_book: dict[str, bool] = {"UP": False, "DOWN": False}

        self.token_side = {
            str(market.up_token_id): "UP",
            str(market.down_token_id): "DOWN",
        }
        self.trade_events = 0
        self.trade_events_deduped = 0
        self.book_events = 0
        self.price_change_events = 0
        self.ws_events = 0
        self.ws_reconnects = 0
        self._seen_trade_keys: set[tuple[str, ...]] = set()

    def _emit_execution(
        self,
        *,
        now: float,
        event: str,
        model: str = "",
        quote: QuoteLifecycle | None = None,
        side: str = "",
        qty: Decimal | None = None,
        trade_price: Decimal | None = None,
        trade_size: Decimal | None = None,
        trade_side_flag: str = "",
        queue_before: Decimal | None = None,
        queue_after: Decimal | None = None,
        exchange_ts_ms: str = "",
        transaction_hash: str = "",
        reason: str = "",
    ) -> None:
        stats = self.models.get(model)
        inv = None if stats is None else stats.inv
        pair_basis = None if inv is None else inv.pair_basis()

        self.writer.writerow(
            {
                "utc": _iso(now),
                "session": self.session_no,
                "market": self.market.slug,
                "market_age_s": f"{now - self.market.window_start:.6f}",
                "event": event,
                "model": model,
                "quote_id": "" if quote is None else quote.quote_id,
                "side": side or ("" if quote is None else quote.side),
                "qty": "" if qty is None else str(qty),
                "quote_price": "" if quote is None else str(quote.price),
                "trade_price": "" if trade_price is None else str(trade_price),
                "trade_size": "" if trade_size is None else str(trade_size),
                "trade_side_flag": trade_side_flag,
                "quote_age_s": "" if quote is None else f"{now - quote.created_ts:.6f}",
                "queue_ahead_before": "" if queue_before is None else str(queue_before),
                "queue_ahead_after": "" if queue_after is None else str(queue_after),
                "exchange_ts_ms": exchange_ts_ms,
                "transaction_hash": transaction_hash,
                "reason": reason,
                "up_shares": "" if inv is None else str(inv.up_shares),
                "up_avg": "" if inv is None or inv.avg("UP") is None else str(inv.avg("UP")),
                "down_shares": "" if inv is None else str(inv.down_shares),
                "down_avg": "" if inv is None or inv.avg("DOWN") is None else str(inv.avg("DOWN")),
                "gap": "" if inv is None else str(inv.gap()),
                "matched": "" if inv is None else str(inv.matched()),
                "pair_basis": "" if pair_basis is None else str(pair_basis),
                "gross_spend": "" if stats is None else str(stats.gross_spend),
            }
        )
        self.execution_fh.flush()

    def _queue_ahead_for(self, side: str, price: Decimal) -> Decimal | None:
        if not self.have_book[side]:
            return None
        return self.bid_levels[side].get(price, Decimal(0))

    def on_quote(
        self,
        *,
        side: str,
        qty: Decimal,
        price: Decimal,
        now: float,
        reason: str,
    ) -> None:
        quote = QuoteLifecycle(
            quote_id=self.next_quote_id,
            side=side,
            price=price,
            size=qty,
            created_ts=now,
            reason=reason,
        )
        self.next_quote_id += 1

        queue_ahead = self._queue_ahead_for(side, price)
        for name in MODEL_NAMES:
            quote.models[name] = ModelQuoteState(
                remaining=qty,
                queue_ahead=queue_ahead if name == MODEL_QUEUE else None,
            )

        self.current_quotes[side] = quote
        self._emit_execution(
            now=now,
            event="QUOTE_LIFECYCLE_START",
            quote=quote,
            side=side,
            qty=qty,
            queue_before=queue_ahead,
            queue_after=queue_ahead,
            reason=reason,
        )

    def on_lifecycle_end(self, *, side: str, now: float, reason: str) -> None:
        quote = self.current_quotes.get(side)
        if quote is None:
            return
        self._emit_execution(
            now=now,
            event="QUOTE_LIFECYCLE_END",
            quote=quote,
            side=side,
            reason=reason,
        )
        self.current_quotes[side] = None

    def _record_model_fill(
        self,
        *,
        model: str,
        quote: QuoteLifecycle,
        qty: Decimal,
        now: float,
        trade_price: Decimal | None,
        trade_size: Decimal | None,
        trade_side_flag: str,
        queue_before: Decimal | None,
        queue_after: Decimal | None,
        exchange_ts_ms: str,
        transaction_hash: str,
        reason: str,
    ) -> None:
        if qty <= 0:
            return

        state = quote.models[model]
        qty = min(qty, state.remaining)
        if qty <= 0:
            return
        state.remaining -= qty

        stats = self.models[model]
        stats.record_fill(
            side=quote.side,
            qty=qty,
            price=quote.price,
            now=now,
            window_start=self.market.window_start,
        )
        self._emit_execution(
            now=now,
            event="MODEL_FILL",
            model=model,
            quote=quote,
            side=quote.side,
            qty=qty,
            trade_price=trade_price,
            trade_size=trade_size,
            trade_side_flag=trade_side_flag,
            queue_before=queue_before,
            queue_after=queue_after,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
            reason=reason,
        )

    def on_strict_fill(
        self,
        *,
        side: str,
        qty: Decimal,
        price: Decimal,
        now: float,
        reason: str,
    ) -> None:
        quote = self.current_quotes.get(side)
        if quote is None:
            quote = QuoteLifecycle(
                quote_id=-1,
                side=side,
                price=price,
                size=qty,
                created_ts=now,
                reason="synthetic strict lifecycle recovery",
                models={
                    name: ModelQuoteState(remaining=qty)
                    for name in MODEL_NAMES
                },
            )

        state = quote.models[MODEL_STRICT]
        strict_qty = min(qty, state.remaining)
        self._record_model_fill(
            model=MODEL_STRICT,
            quote=quote,
            qty=strict_qty,
            now=now,
            trade_price=None,
            trade_size=None,
            trade_side_flag="",
            queue_before=None,
            queue_after=None,
            exchange_ts_ms="",
            transaction_hash="",
            reason=reason,
        )
        self.on_lifecycle_end(side=side, now=now, reason="strict V2 engine removed quote after fill")

    def update_book(self, data: dict[str, Any]) -> None:
        side = self.token_side.get(str(data.get("asset_id") or ""))
        if side is None:
            return
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for raw in data.get("bids") or []:
            p = _d(raw.get("price"))
            q = _d(raw.get("size"))
            if p is not None and q is not None and q > 0:
                bids[p] = q
        for raw in data.get("asks") or []:
            p = _d(raw.get("price"))
            q = _d(raw.get("size"))
            if p is not None and q is not None and q > 0:
                asks[p] = q
        self.bid_levels[side] = bids
        self.ask_levels[side] = asks
        self.have_book[side] = True
        self.book_events += 1

    def update_price_changes(self, data: dict[str, Any]) -> None:
        for change in data.get("price_changes") or []:
            side = self.token_side.get(str(change.get("asset_id") or ""))
            if side is None:
                continue
            price = _d(change.get("price"))
            size = _d(change.get("size"))
            book_side = str(change.get("side") or "").upper()
            if price is None or size is None:
                continue
            levels = self.bid_levels[side] if book_side == "BUY" else self.ask_levels[side]
            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = size
        self.price_change_events += 1

    def _trade_key(self, data: dict[str, Any]) -> tuple[str, ...]:
        return (
            str(data.get("asset_id") or ""),
            str(data.get("timestamp") or ""),
            str(data.get("transaction_hash") or ""),
            str(data.get("price") or ""),
            str(data.get("size") or ""),
            str(data.get("side") or ""),
        )

    def on_trade(self, data: dict[str, Any], recv_ts: float) -> None:
        key = self._trade_key(data)
        if key in self._seen_trade_keys:
            self.trade_events_deduped += 1
            return
        self._seen_trade_keys.add(key)
        self.trade_events += 1

        side = self.token_side.get(str(data.get("asset_id") or ""))
        trade_price = _d(data.get("price"))
        trade_size = _d(data.get("size"))
        trade_side_flag = str(data.get("side") or "").upper()
        if side is None or trade_price is None or trade_size is None or trade_size <= 0:
            return

        quote = self.current_quotes.get(side)
        if quote is None or recv_ts + 1e-9 < quote.created_ts:
            return

        exchange_ts_ms = str(data.get("timestamp") or "")
        transaction_hash = str(data.get("transaction_hash") or "")

        self._touch_fill(
            model=MODEL_TOUCH,
            quote=quote,
            trade_price=trade_price,
            trade_size=trade_size,
            recv_ts=recv_ts,
            trade_side_flag=trade_side_flag,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
            require_sell_flag=False,
        )
        self._touch_fill(
            model=MODEL_SELL_TOUCH,
            quote=quote,
            trade_price=trade_price,
            trade_size=trade_size,
            recv_ts=recv_ts,
            trade_side_flag=trade_side_flag,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
            require_sell_flag=True,
        )
        self._queue_fill(
            quote=quote,
            trade_price=trade_price,
            trade_size=trade_size,
            recv_ts=recv_ts,
            trade_side_flag=trade_side_flag,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
        )

    def _touch_fill(
        self,
        *,
        model: str,
        quote: QuoteLifecycle,
        trade_price: Decimal,
        trade_size: Decimal,
        recv_ts: float,
        trade_side_flag: str,
        exchange_ts_ms: str,
        transaction_hash: str,
        require_sell_flag: bool,
    ) -> None:
        state = quote.models[model]
        if state.remaining <= 0:
            return
        if require_sell_flag and trade_side_flag != "SELL":
            return
        if trade_price > quote.price + EPS:
            return

        if trade_price < quote.price - EPS:
            fill_qty = state.remaining
            reason = "public trade printed below resting bid; sweep-through proxy"
        else:
            fill_qty = min(state.remaining, trade_size)
            reason = "public trade printed at resting bid; touch proxy ignores queue"

        self._record_model_fill(
            model=model,
            quote=quote,
            qty=fill_qty,
            now=recv_ts,
            trade_price=trade_price,
            trade_size=trade_size,
            trade_side_flag=trade_side_flag,
            queue_before=None,
            queue_after=None,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
            reason=reason,
        )

    def _queue_fill(
        self,
        *,
        quote: QuoteLifecycle,
        trade_price: Decimal,
        trade_size: Decimal,
        recv_ts: float,
        trade_side_flag: str,
        exchange_ts_ms: str,
        transaction_hash: str,
    ) -> None:
        if trade_side_flag != "SELL":
            return
        state = quote.models[MODEL_QUEUE]
        if state.remaining <= 0:
            return
        if trade_price > quote.price + EPS:
            return

        before = state.queue_ahead

        if trade_price < quote.price - EPS:
            fill_qty = state.remaining
            reason = "SELL-flagged trade printed below bid; conservative sweep-through"
            after = Decimal(0) if before is not None else None
            state.queue_ahead = after
        else:
            if state.queue_ahead is None:
                return
            available = trade_size
            consumed_ahead = min(state.queue_ahead, available)
            state.queue_ahead -= consumed_ahead
            available -= consumed_ahead
            after = state.queue_ahead
            fill_qty = min(state.remaining, max(Decimal(0), available))
            reason = "SELL-flagged same-price volume exceeded conservative queue-ahead"

        if fill_qty <= 0:
            self._emit_execution(
                now=recv_ts,
                event="QUEUE_PROGRESS",
                model=MODEL_QUEUE,
                quote=quote,
                side=quote.side,
                qty=Decimal(0),
                trade_price=trade_price,
                trade_size=trade_size,
                trade_side_flag=trade_side_flag,
                queue_before=before,
                queue_after=state.queue_ahead,
                exchange_ts_ms=exchange_ts_ms,
                transaction_hash=transaction_hash,
                reason="SELL-flagged trade consumed queue ahead only",
            )
            return

        self._record_model_fill(
            model=MODEL_QUEUE,
            quote=quote,
            qty=fill_qty,
            now=recv_ts,
            trade_price=trade_price,
            trade_size=trade_size,
            trade_side_flag=trade_side_flag,
            queue_before=before,
            queue_after=state.queue_ahead,
            exchange_ts_ms=exchange_ts_ms,
            transaction_hash=transaction_hash,
            reason=reason,
        )

    def summary(self, winner: str | None) -> dict[str, Any]:
        return {
            "ws_event_counts": {
                "total": self.ws_events,
                "book": self.book_events,
                "price_change": self.price_change_events,
                "last_trade_price": self.trade_events,
                "deduped_last_trade_price": self.trade_events_deduped,
                "reconnects": self.ws_reconnects,
            },
            "models": {
                name: self.models[name].summary(winner)
                for name in MODEL_NAMES
            },
        }


class ExecutionForensicEngine(v2.InventoryAwareForensicEngine):
    def __init__(
        self,
        *,
        execution_writer: csv.DictWriter,
        execution_fh: Any,
        ws_raw_dir: Path,
        resolution_grace_s: float,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.audit = ExecutionAudit(
            session_no=self.session_no,
            market=self.market,
            writer=execution_writer,
            execution_fh=execution_fh,
        )
        self.ws_raw_path = ws_raw_dir / f"{self.market.slug}.jsonl"
        self.resolution_grace_s = resolution_grace_s
        self.ws_ready = asyncio.Event()
        self.ws_resolved = asyncio.Event()
        self.winning_asset_id: str | None = None
        self.winning_outcome: str | None = None
        self.winner_side: str | None = None
        self.resolution_source: str | None = None
        self.resolution_timestamp_ms: str | None = None
        self._stream_task: asyncio.Task[Any] | None = None
        self.strategy_summary: dict[str, Any] | None = None

    def _emit(self, **kwargs: Any) -> None:
        event = str(kwargs.get("event") or "")
        now = float(kwargs.get("now") or time.time())
        side = str(kwargs.get("side") or "")
        qty = kwargs.get("qty")
        price = kwargs.get("price")
        reason = str(kwargs.get("reason") or "")

        if event == "QUOTE" and side in {"UP", "DOWN"} and qty is not None and price is not None:
            self.audit.on_quote(
                side=side,
                qty=Decimal(qty),
                price=Decimal(price),
                now=now,
                reason=reason,
            )
        elif event == "CANCEL" and side in {"UP", "DOWN"}:
            self.audit.on_lifecycle_end(side=side, now=now, reason=f"strategy cancel: {reason}")
        elif event == "MAKER_FILL" and side in {"UP", "DOWN"} and qty is not None and price is not None:
            self.audit.on_strict_fill(
                side=side,
                qty=Decimal(qty),
                price=Decimal(price),
                now=now,
                reason=reason,
            )

        super()._emit(**kwargs)

    async def preconnect(self) -> None:
        if self._stream_task is not None:
            return
        self._stream_task = asyncio.create_task(self._market_stream_loop())
        try:
            await asyncio.wait_for(self.ws_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            print("WS WARN    market stream not ready within 5s; strategy will continue")

    def _handle_resolution(self, data: dict[str, Any], source: str) -> None:
        winning_asset_id = str(data.get("winning_asset_id") or "")
        if not winning_asset_id:
            return
        self.winning_asset_id = winning_asset_id
        self.winning_outcome = str(data.get("winning_outcome") or "") or None
        self.resolution_timestamp_ms = str(data.get("timestamp") or "") or None
        if winning_asset_id == str(self.market.up_token_id):
            self.winner_side = "UP"
        elif winning_asset_id == str(self.market.down_token_id):
            self.winner_side = "DOWN"
        else:
            return
        self.resolution_source = source
        self.ws_resolved.set()

    async def _market_stream_loop(self) -> None:
        deadline = self.market.window_end + self.resolution_grace_s
        self.ws_raw_path.parent.mkdir(parents=True, exist_ok=True)

        with self.ws_raw_path.open("w", encoding="utf-8") as raw_fh:
            backoff = 0.25
            while time.time() < deadline and not self.ws_resolved.is_set():
                try:
                    async with websockets.connect(
                        MARKET_WS_URL,
                        ping_interval=None,
                        close_timeout=3,
                        max_size=8 * 1024 * 1024,
                    ) as ws:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "market",
                                    "assets_ids": [
                                        self.market.up_token_id,
                                        self.market.down_token_id,
                                    ],
                                    "custom_feature_enabled": True,
                                }
                            )
                        )
                        self.ws_ready.set()
                        backoff = 0.25

                        async def pinger() -> None:
                            while time.time() < deadline and not self.ws_resolved.is_set():
                                await asyncio.sleep(8.0)
                                try:
                                    await ws.send("PING")
                                except Exception:
                                    return

                        ping_task = asyncio.create_task(pinger())
                        try:
                            while time.time() < deadline and not self.ws_resolved.is_set():
                                timeout = max(0.1, min(5.0, deadline - time.time()))
                                try:
                                    message = await asyncio.wait_for(ws.recv(), timeout=timeout)
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
                                    self.audit.ws_events += 1
                                    raw_fh.write(
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
                                    raw_fh.flush()

                                    event_type = str(data.get("event_type") or "")
                                    if event_type == "book":
                                        self.audit.update_book(data)
                                    elif event_type == "price_change":
                                        self.audit.update_price_changes(data)
                                    elif event_type == "last_trade_price":
                                        self.audit.on_trade(data, recv_ts)
                                    elif event_type == "market_resolved":
                                        self._handle_resolution(data, "market_ws")
                        finally:
                            ping_task.cancel()
                            await asyncio.gather(ping_task, return_exceptions=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    self.audit.ws_reconnects += 1
                    if not self.ws_ready.is_set():
                        print(f"WS WARN    initial stream error {type(exc).__name__}: {exc}")
                    await asyncio.sleep(min(backoff, max(0.0, deadline - time.time())))
                    backoff = min(4.0, backoff * 2)

        self.ws_ready.set()

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        if self._stream_task is None:
            await self.preconnect()
        self.strategy_summary = await super().run(client)
        return self.strategy_summary

    async def finalize_resolution(self) -> None:
        if self._stream_task is not None and not self._stream_task.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._stream_task),
                    timeout=max(0.1, self.market.window_end + self.resolution_grace_s - time.time() + 0.5),
                )
            except asyncio.TimeoutError:
                self._stream_task.cancel()
                await asyncio.gather(self._stream_task, return_exceptions=True)

        if self.winner_side is None:
            resolved = await asyncio.to_thread(_gamma_resolution, self.market.condition_id)
            if resolved is not None:
                winning_asset_id, winning_outcome = resolved
                self._handle_resolution(
                    {
                        "winning_asset_id": winning_asset_id,
                        "winning_outcome": winning_outcome,
                        "timestamp": "",
                    },
                    "gamma_fallback",
                )

    def full_summary(self) -> dict[str, Any]:
        return {
            "session": self.session_no,
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "strategy_v2": self.strategy_summary,
            "resolution": {
                "resolved": self.winner_side is not None,
                "winner_side": self.winner_side,
                "winning_asset_id": self.winning_asset_id,
                "winning_outcome": self.winning_outcome,
                "source": self.resolution_source,
                "timestamp_ms": self.resolution_timestamp_ms,
            },
            "execution_audit": self.audit.summary(self.winner_side),
            "ws_raw_jsonl": str(self.ws_raw_path),
        }


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _gamma_resolution(condition_id: str) -> tuple[str, str | None] | None:
    """Best-effort public Gamma fallback after the WebSocket grace period."""
    query = urllib.parse.urlencode(
        {
            "condition_ids": condition_id,
            "closed": "true",
            "limit": 5,
        }
    )
    url = f"https://gamma-api.polymarket.com/markets?{query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "gabagool-forensic-v3/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:  # noqa: BLE001
        return None

    markets = payload if isinstance(payload, list) else []
    if not markets:
        return None
    market = markets[0]
    token_ids = [str(x) for x in _json_array(market.get("clobTokenIds"))]
    outcomes = [str(x) for x in _json_array(market.get("outcomes"))]
    prices_raw = _json_array(market.get("outcomePrices"))
    prices: list[Decimal] = []
    for raw in prices_raw:
        value = _d(raw)
        if value is None:
            return None
        prices.append(value)
    if not token_ids or len(token_ids) != len(prices):
        return None

    idx = max(range(len(prices)), key=lambda i: prices[i])
    if prices[idx] < Decimal("0.99"):
        return None
    outcome = outcomes[idx] if idx < len(outcomes) else None
    return token_ids[idx], outcome


def _aggregate_models(sessions: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for model in MODEL_NAMES:
        rows = [s["execution_audit"]["models"][model] for s in sessions]
        active = [r for r in rows if r["fill_events"] > 0]
        both = sum(1 for r in rows if r["both_sides"])
        matched_total = sum((Decimal(r["matched_shares"]) for r in rows), Decimal(0))
        spend_total = sum((Decimal(r["gross_spend"]) for r in rows), Decimal(0))
        fill_shares = sum((Decimal(r["fill_shares"]) for r in rows), Decimal(0))
        harvest_total = sum((Decimal(r["matched_harvest_gross_pnl"]) for r in rows), Decimal(0))

        weighted_basis_num = Decimal(0)
        for r in rows:
            basis = r.get("pair_basis")
            matched = Decimal(r["matched_shares"])
            if basis is not None and matched > 0:
                weighted_basis_num += matched * Decimal(str(basis))
        weighted_basis = None if matched_total <= 0 else weighted_basis_num / matched_total

        imbalanced = sum(int(r["imbalanced_before_fill_count"]) for r in rows)
        underweight = sum(int(r["underweight_fill_count"]) for r in rows)
        settled = [r["settlement"] for r in rows if r.get("settlement") is not None]
        settlement_pnl = sum(
            (Decimal(x["paper_total_pnl"]) for x in settled),
            Decimal(0),
        )

        result[model] = {
            "sessions": len(rows),
            "sessions_with_fills": len(active),
            "markets_buying_both_sides": both,
            "both_sides_rate": 0.0 if not rows else both / len(rows),
            "fill_events": sum(int(r["fill_events"]) for r in rows),
            "fill_shares": str(fill_shares),
            "gross_spend": str(spend_total),
            "matched_shares": str(matched_total),
            "pair_weighted_combined_basis": None if weighted_basis is None else str(weighted_basis),
            "matched_harvest_gross_pnl": str(harvest_total),
            "median_first_fill_age_s": _median([r.get("first_fill_age_s") for r in active]),
            "median_last_fill_age_s": _median([r.get("last_fill_age_s") for r in active]),
            "median_max_gap_shares": _median(
                [float(Decimal(r["max_gap_shares"])) for r in active]
            ),
            "aggregate_underweight_fill_rate": (
                None if imbalanced == 0 else underweight / imbalanced
            ),
            "mean_session_opposite_sum": _mean([r.get("opposite_sum_mean") for r in active]),
            "median_session_opposite_sum": _median([r.get("opposite_sum_median") for r in active]),
            "resolved_sessions": len(settled),
            "paper_total_pnl_resolved_sessions": str(settlement_pnl),
        }
    return result


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    ws_raw_dir = run_dir / "ws_raw"
    ws_raw_dir.mkdir(parents=True, exist_ok=True)

    activity_path = run_dir / "activity.csv"
    execution_path = run_dir / "execution_events.csv"
    summary_path = run_dir / "summary.json"

    print("READ ONLY   no wallet, no key, no orders, no real merge/redeem")
    print("OBJECTIVE   isolate execution assumptions on one frozen V2 quote stream")
    print("VERSION     forensic V3: strict + trade-touch + queue execution audit")
    print(
        f"PLAN        {args.sessions} consecutive BTC 5m markets | clip={args.clip}sh "
        f"| poll={args.poll:.2f}s"
    )
    print(
        f"STRATEGY    FROZEN V2 | soft={args.quote_pair_target} | "
        f"hard pair max={args.inventory_pair_max} | skew={args.skew_ticks} | maxGap={args.max_gap}"
    )
    print("MODELS      strict_ask_cross | trade_touch_any | trade_touch_sell_flag | queue_conservative_sell_flag")
    print(
        f"STREAM      public CLOB market websocket | last_trade_price + book + resolution | "
        f"resolution grace={args.resolution_grace:.0f}s"
    )
    print(f"OUTPUT      {run_dir}")

    client = AsyncPublicClient()
    engines: list[ExecutionForensicEngine] = []

    start = window_start_epoch(DURATION_S, time.time()) + DURATION_S

    try:
        with (
            activity_path.open("w", newline="", encoding="utf-8") as activity_fh,
            execution_path.open("w", newline="", encoding="utf-8") as execution_fh,
        ):
            activity_writer = csv.DictWriter(activity_fh, fieldnames=v1.ACTIVITY_FIELDS)
            activity_writer.writeheader()
            execution_writer = csv.DictWriter(execution_fh, fieldnames=EXECUTION_FIELDS)
            execution_writer.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                preconnect_at = target - args.preconnect
                wait = preconnect_at - time.time()
                if wait > 0:
                    print(
                        f"WAIT        session {idx + 1}/{args.sessions} preconnects in {wait:.1f}s "
                        f"(market starts {_iso(target)})"
                    )
                    await asyncio.sleep(wait)

                market = await v2._resolve_wait(client, target)
                engine = ExecutionForensicEngine(
                    session_no=idx + 1,
                    market=market,
                    writer=activity_writer,
                    clip=args.clip,
                    poll_s=args.poll,
                    quote_pair_target=args.quote_pair_target,
                    inventory_pair_max=args.inventory_pair_max,
                    requote_s=args.requote,
                    max_gap=args.max_gap,
                    stop_new_seed_s=args.stop_new_seed,
                    skew_ticks=args.skew_ticks,
                    execution_writer=execution_writer,
                    execution_fh=execution_fh,
                    ws_raw_dir=ws_raw_dir,
                    resolution_grace_s=args.resolution_grace,
                )
                engines.append(engine)
                await engine.preconnect()

                wait_to_start = target - time.time()
                if wait_to_start > 0:
                    await asyncio.sleep(wait_to_start)

                await engine.run(client)
                activity_fh.flush()
                execution_fh.flush()

        print("\nRESOLUTION  waiting for public resolution events / Gamma fallback...")
        await asyncio.gather(*(engine.finalize_resolution() for engine in engines))

        sessions = [engine.full_summary() for engine in engines]
        aggregates = _aggregate_models(sessions)
        resolved_count = sum(1 for s in sessions if s["resolution"]["resolved"])

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
                "preconnect_s": args.preconnect,
                "resolution_grace_s": args.resolution_grace,
                "taker_repair": False,
                "mid_market_merge": False,
                "post_fill_opposite_reprice": True,
                "strategy_logic": "frozen V2",
            },
            "execution_model_definitions": {
                MODEL_STRICT: (
                    "V2 strict proxy: full clip only when a later polled ask book is executable "
                    "at or below the resting bid."
                ),
                MODEL_TOUCH: (
                    "Public last_trade_price: below-bid print sweeps full remaining quote; "
                    "same-price print fills up to printed size; side flag ignored."
                ),
                MODEL_SELL_TOUCH: (
                    "Same trade-touch rule, restricted to public last_trade_price events whose "
                    "side field equals SELL. V3 does not assume undocumented aggressor semantics."
                ),
                MODEL_QUEUE: (
                    "SELL-flagged trades plus displayed BUY size at our price when quote was posted. "
                    "Same-price volume consumes queue ahead first; cancellations ahead are not credited."
                ),
            },
            "aggregate_execution_models": aggregates,
            "resolved_sessions": resolved_count,
            "sessions": sessions,
            "files": {
                "strategy_activity_csv": str(activity_path),
                "execution_events_csv": str(execution_path),
                "ws_raw_dir": str(ws_raw_dir),
                "summary_json": str(summary_path),
            },
            "interpretation": (
                "Execution-forensics only. All shadow models observe the same quote lifecycles produced "
                "by the strict V2 strategy path and do not feed hypothetical fills back into quote decisions. "
                "This isolates fill-model sensitivity but means touch/queue results are counterfactual audits, "
                "not self-consistent alternative strategies. PnL settlement ignores fees and maker rebates."
            ),
        }
        summary_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

        print("\n" + "=" * 112)
        print("GABAGOOL FORENSIC V3 EXECUTION SUMMARY")
        print(f"SESSIONS    {args.sessions} | RESOLVED {resolved_count}/{args.sessions}")
        for model in MODEL_NAMES:
            row = aggregates[model]
            print("-" * 112)
            print(f"MODEL       {model}")
            print(
                f"FILLS       events={row['fill_events']} shares={row['fill_shares']} | "
                f"two-sided={row['markets_buying_both_sides']}/{row['sessions']} "
                f"({row['both_sides_rate']:.1%})"
            )
            print(
                f"TIMING      firstMed={row['median_first_fill_age_s']}s | "
                f"lastMed={row['median_last_fill_age_s']}s"
            )
            print(
                f"ECON        matched={row['matched_shares']} | basis={row['pair_weighted_combined_basis']} | "
                f"matchedEdge=${Decimal(row['matched_harvest_gross_pnl']):.6f}"
            )
            print(
                f"BEHAVIOR    underweight={row['aggregate_underweight_fill_rate']} | "
                f"oppMedian={row['median_session_opposite_sum']} | "
                f"medianMaxGap={row['median_max_gap_shares']}"
            )
            print(
                f"SETTLED PNL ${Decimal(row['paper_total_pnl_resolved_sessions']):.6f} "
                f"across {row['resolved_sessions']} resolved sessions"
            )

        print("-" * 112)
        print("CHAIN REF   BTC5m first≈14s | last≈205s | both sides≈100% | fill rows median≈59.5")
        print("FILES       strategy activity:", activity_path)
        print("            execution events:", execution_path)
        print("            raw websocket:", ws_raw_dir)
        print("            summary:", summary_path)
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
    parser.add_argument("--preconnect", type=float, default=DEFAULT_PRECONNECT_S)
    parser.add_argument(
        "--resolution-grace",
        type=float,
        default=DEFAULT_RESOLUTION_GRACE_S,
    )
    parser.add_argument("--output", default="data/gabagool_forensic_v3")
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
        args.quote_pair_target <= args.inventory_pair_max < Decimal("1.05")
    ):
        parser.error("--inventory-pair-max must be >= quote target and < 1.05")
    if args.requote <= 0:
        parser.error("--requote must be positive")
    if args.max_gap < args.clip:
        parser.error("--max-gap must be >= clip")
    if args.stop_new_seed < 0 or args.stop_new_seed >= DURATION_S:
        parser.error("--stop-new-seed must be in [0, 300)")
    if args.skew_ticks < 0 or args.skew_ticks > 10:
        parser.error("--skew-ticks must be 0..10")
    if args.preconnect < 0 or args.preconnect > 10:
        parser.error("--preconnect must be in [0, 10]")
    if args.resolution_grace < 0 or args.resolution_grace > 600:
        parser.error("--resolution-grace must be in [0, 600]")

    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
