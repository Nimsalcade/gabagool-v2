"""Authoritative fill tracking with exact taker trade-cost reconciliation.

Rules:
- an order disappearing from open orders is never inferred as a fill;
- maker fills use exchange `size_matched` while open, then confirmed trade attribution;
- FAK taker orders are re-checked against the trades API for a short finalization window
  because they disappear immediately and trade indexing can lag the order response;
- taker cost uses actual trade price, not the planning max-price.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

log = logging.getLogger("fills")
_COUNTABLE = {"MATCHED", "MINED", "CONFIRMED"}
_TAKER_FINALIZE_S = 30.0


@dataclass
class TrackedOrder:
    order_id: str
    side: str
    token_id: str
    price: float                 # maker limit or taker max-price planning value
    shares: float                # maker size or taker planning share estimate
    mode: str = "maker"          # maker | taker
    filled: float = 0.0
    filled_cost: float = 0.0
    open: bool = True
    created_ts: float = field(default_factory=time.time)
    finalized: bool = False


@dataclass
class SideTotals:
    shares: float = 0.0
    cost: float = 0.0
    max_price: float = 0.0
    last_fill_ts: float | None = None

    @property
    def avg_price(self) -> float:
        return self.cost / self.shares if self.shares else 0.0


@dataclass(frozen=True)
class FillAggregate:
    shares: float = 0.0
    cost: float = 0.0


@dataclass
class FillTracker:
    condition_id: str
    ledger: object | None = None
    orders: dict[str, TrackedOrder] = field(default_factory=dict)
    up: SideTotals = field(default_factory=SideTotals)
    down: SideTotals = field(default_factory=SideTotals)

    def register(
        self,
        order_id: str,
        side: str,
        token_id: str,
        price: float,
        shares: float,
        *,
        mode: str = "maker",
    ) -> None:
        self.orders[order_id] = TrackedOrder(
            order_id, side, token_id, price, shares, mode=mode
        )

    def resting_price(self, side: str) -> float | None:
        prices = [
            o.price for o in self.orders.values()
            if o.open and o.side == side and o.mode == "maker"
        ]
        return max(prices) if prices else None

    def resting_notional(self) -> float:
        return sum(
            o.price * max(0.0, o.shares - o.filled)
            for o in self.orders.values() if o.open and o.mode == "maker"
        )

    def open_order_ids(self) -> list[str]:
        return [oid for oid, o in self.orders.items() if o.open and o.mode == "maker"]

    async def reconcile(self, client) -> float:
        if not self.orders:
            return 0.0

        now = time.time()
        open_matched: dict[str, float] = {}
        open_ids: set[str] = set()
        try:
            paginator = client.list_open_orders(market=self.condition_id)
            async for page in paginator:
                for oo in page.items:
                    oid = str(oo.id)
                    open_ids.add(oid)
                    open_matched[oid] = float(oo.size_matched or 0)
        except Exception as exc:  # noqa: BLE001
            log.debug("open-orders poll failed (%s); keeping prior state", exc)
            return 0.0

        needs_trades = any(
            (o.open and o.order_id not in open_ids)
            or (o.mode == "taker" and not o.finalized and now - o.created_ts <= _TAKER_FINALIZE_S)
            for o in self.orders.values()
        )
        traded = await self._fills_from_trades(client) if needs_trades else {}

        new_notional = 0.0
        for o in self.orders.values():
            if o.mode == "taker":
                agg = traded.get(o.order_id)
                if agg is not None:
                    target_shares = agg.shares
                    target_cost = agg.cost
                    new_notional += self._apply_target(o, target_shares, target_cost)
                o.open = False  # FAK never rests after the request completes.
                if now - o.created_ts > _TAKER_FINALIZE_S:
                    o.finalized = True
                continue

            if not o.open:
                continue
            if o.order_id in open_ids:
                target_shares = min(open_matched.get(o.order_id, o.filled), o.shares)
                target_cost = target_shares * o.price
            else:
                agg = traded.get(o.order_id)
                target_shares = min(agg.shares if agg is not None else o.filled, o.shares)
                target_cost = target_shares * o.price
                o.open = False
                o.finalized = True
                if target_shares == 0 and o.filled == 0:
                    log.debug("order %s cancelled/unfilled", o.order_id[:10])
            new_notional += self._apply_target(o, target_shares, target_cost)

        return new_notional

    def _apply_target(self, o: TrackedOrder, target_shares: float, target_cost: float) -> float:
        delta_shares = target_shares - o.filled
        delta_cost = target_cost - o.filled_cost
        if delta_shares <= 1e-9 and delta_cost <= 1e-9:
            return 0.0
        if delta_shares < -1e-9 or delta_cost < -1e-7:
            # Confirmed trade aggregates should be monotonic. Never rewrite inventory
            # backwards on an inconsistent transient API response.
            log.warning("non-monotonic fill aggregate for %s; ignoring", o.order_id[:10])
            return 0.0
        if delta_shares <= 1e-9:
            return 0.0

        fill_price = delta_cost / delta_shares if delta_cost > 0 else o.price
        o.filled = target_shares
        o.filled_cost = target_cost
        tot = self.up if o.side == "UP" else self.down
        tot.shares += delta_shares
        tot.cost += delta_cost if delta_cost > 0 else delta_shares * o.price
        tot.max_price = max(tot.max_price, fill_price)
        tot.last_fill_ts = time.time()
        notional = delta_cost if delta_cost > 0 else delta_shares * o.price
        if self.ledger is not None:
            self.ledger.record_fill(
                self.condition_id, o.order_id, o.side, fill_price, delta_shares
            )
        log.info(
            "FILL %s %s %.3f sh @ %.4f [%s] order=%s",
            self.condition_id[:10], o.side, delta_shares, fill_price, o.mode, o.order_id[:10],
        )
        return notional

    async def _fills_from_trades(self, client) -> dict[str, FillAggregate]:
        shares: dict[str, float] = {}
        costs: dict[str, float] = {}
        try:
            paginator = client.list_account_trades(market=self.condition_id)
            async for page in paginator:
                for tr in page.items:
                    status = str(getattr(tr, "status", "")).upper()
                    if status not in _COUNTABLE:
                        continue
                    tr_size = float(getattr(tr, "size", 0) or 0)
                    tr_price = float(getattr(tr, "price", 0) or 0)
                    tid = str(getattr(tr, "taker_order_id", "") or "")
                    if tid in self.orders:
                        shares[tid] = shares.get(tid, 0.0) + tr_size
                        costs[tid] = costs.get(tid, 0.0) + tr_size * tr_price
                    for mo in getattr(tr, "maker_orders", None) or []:
                        moid = str(getattr(mo, "order_id", "") or "")
                        if moid in self.orders:
                            amt = float(getattr(mo, "matched_amount", 0) or 0)
                            shares[moid] = shares.get(moid, 0.0) + amt
                            # Maker limit price is authoritative for its leg.
                            costs[moid] = costs.get(moid, 0.0) + amt * self.orders[moid].price
        except Exception as exc:  # noqa: BLE001
            log.debug("trades poll failed (%s); no new information", exc)
        return {
            oid: FillAggregate(qty, costs.get(oid, 0.0))
            for oid, qty in shares.items()
        }

    def matched_pairs(self) -> float:
        return min(self.up.shares, self.down.shares)

    def combined_avg(self) -> float | None:
        if self.up.shares and self.down.shares:
            return self.up.avg_price + self.down.avg_price
        return None

    def total_cost(self) -> float:
        return self.up.cost + self.down.cost
