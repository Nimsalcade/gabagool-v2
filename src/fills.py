"""Authoritative fill tracking with decision-state timestamps.

Orders disappearing from the open-order list are never inferred as fills. Fill size is
accepted only from exchange-reported matched size or confirmed trade attribution.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

log = logging.getLogger("fills")
_COUNTABLE = {"MATCHED", "MINED", "CONFIRMED"}


@dataclass
class TrackedOrder:
    order_id: str
    side: str
    token_id: str
    price: float
    shares: float
    mode: str = "maker"  # maker | taker
    filled: float = 0.0
    open: bool = True


@dataclass
class SideTotals:
    shares: float = 0.0
    cost: float = 0.0
    max_price: float = 0.0
    last_fill_ts: float | None = None

    @property
    def avg_price(self) -> float:
        return self.cost / self.shares if self.shares else 0.0


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
            for o in self.orders.values() if o.open
        )

    def open_order_ids(self) -> list[str]:
        return [oid for oid, o in self.orders.items() if o.open]

    async def reconcile(self, client) -> float:
        if not self.orders:
            return 0.0

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

        gone = [o for o in self.orders.values() if o.open and o.order_id not in open_ids]
        traded: dict[str, float] = {}
        if gone:
            traded = await self._fills_from_trades(client)

        new_notional = 0.0
        for o in self.orders.values():
            if not o.open:
                continue
            if o.order_id in open_ids:
                target = min(open_matched.get(o.order_id, o.filled), o.shares)
            else:
                target = min(traded.get(o.order_id, o.filled), o.shares)
                o.open = False
                if target == 0 and o.filled == 0:
                    log.debug("order %s cancelled unfilled", o.order_id[:10])

            delta = target - o.filled
            if delta > 1e-9:
                o.filled = target
                tot = self.up if o.side == "UP" else self.down
                tot.shares += delta
                tot.cost += delta * o.price
                tot.max_price = max(tot.max_price, o.price)
                tot.last_fill_ts = time.time()
                new_notional += delta * o.price
                if self.ledger is not None:
                    self.ledger.record_fill(
                        self.condition_id, o.order_id, o.side, o.price, delta
                    )
                log.info(
                    "FILL %s %s %.3f sh @ %.3f [%s] order=%s",
                    self.condition_id[:10], o.side, delta, o.price, o.mode, o.order_id[:10],
                )
        return new_notional

    async def _fills_from_trades(self, client) -> dict[str, float]:
        sizes: dict[str, float] = {}
        try:
            paginator = client.list_account_trades(market=self.condition_id)
            async for page in paginator:
                for tr in page.items:
                    status = str(getattr(tr, "status", "")).upper()
                    if status not in _COUNTABLE:
                        continue
                    tid = str(getattr(tr, "taker_order_id", "") or "")
                    if tid in self.orders:
                        sizes[tid] = sizes.get(tid, 0.0) + float(tr.size or 0)
                    for mo in getattr(tr, "maker_orders", None) or []:
                        moid = str(getattr(mo, "order_id", "") or "")
                        if moid in self.orders:
                            sizes[moid] = sizes.get(moid, 0.0) + float(
                                getattr(mo, "matched_amount", 0) or 0
                            )
        except Exception as exc:  # noqa: BLE001
            log.debug("trades poll failed (%s); no new information", exc)
        return sizes

    def matched_pairs(self) -> float:
        return min(self.up.shares, self.down.shares)

    def combined_avg(self) -> float | None:
        if self.up.shares and self.down.shares:
            return self.up.avg_price + self.down.avg_price
        return None

    def total_cost(self) -> float:
        return self.up.cost + self.down.cost
