"""
Fill tracking that cannot hallucinate.

THE BUG THIS REPLACES
---------------------
The old `_reconcile_fills()` declared any order missing from the open-orders
list to be FULLY FILLED. But orders also vanish when they are cancelled — by
us, by the exchange (post-only mode, market close, heartbeat lapse), or by a
restart. Every such cancel was booked as a fill, so the bot's inventory said
"you hold matched pairs" when the chain said otherwise. Merges then reverted
('script is not matching'), PnL was fiction (+$661 internal vs −$186 wallet),
and 'naked' losses appeared out of nowhere.

THE FIX
-------
Fills come ONLY from authoritative sources, never inferred from absence:

  1. While an order is open: `OpenOrder.size_matched` (exchange-reported).
  2. After it leaves the book: the trades API (`list_account_trades`),
     summing CONFIRMED/MINED/MATCHED trade sizes attributed to that order.

An order that disappears with zero attributed trades is a CANCEL and adds
exactly nothing to inventory.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger("fills")

# Trade statuses that count toward filled size. FAILED is terminal-bad and
# RETRYING resolves into one of the others; we recount on every poll so a
# RETRYING->FAILED transition self-corrects.
_COUNTABLE = {"MATCHED", "MINED", "CONFIRMED"}


@dataclass
class TrackedOrder:
    order_id: str
    side: str            # "UP" | "DOWN"
    token_id: str
    price: float
    shares: float
    filled: float = 0.0  # best-known matched size
    open: bool = True


@dataclass
class SideTotals:
    shares: float = 0.0
    cost: float = 0.0
    max_price: float = 0.0

    @property
    def avg_price(self) -> float:
        return self.cost / self.shares if self.shares else 0.0


@dataclass
class FillTracker:
    """Per-window order/fill ledger for one market."""
    condition_id: str
    orders: dict[str, TrackedOrder] = field(default_factory=dict)
    up: SideTotals = field(default_factory=SideTotals)
    down: SideTotals = field(default_factory=SideTotals)

    # ----------------------------------------------------------- bookkeeping
    def register(self, order_id: str, side: str, token_id: str, price: float, shares: float) -> None:
        self.orders[order_id] = TrackedOrder(order_id, side, token_id, price, shares)

    def resting_price(self, side: str) -> float | None:
        prices = [o.price for o in self.orders.values() if o.open and o.side == side]
        return max(prices) if prices else None

    def resting_notional(self) -> float:
        return sum(o.price * (o.shares - o.filled) for o in self.orders.values() if o.open)

    def open_order_ids(self) -> list[str]:
        return [oid for oid, o in self.orders.items() if o.open]

    # -------------------------------------------------------------- reconcile
    async def reconcile(self, client) -> float:
        """Pull authoritative fill sizes; return newly-filled notional ($)."""
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
        except Exception as exc:  # noqa: BLE001 — transient API failure: change nothing
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
                # Left the book. Trades API is the only truth. No trades ⇒
                # it was a CANCEL ⇒ filled stays exactly as last reported.
                target = min(traded.get(o.order_id, o.filled), o.shares)
                o.open = False
                if target == 0 and o.filled == 0:
                    log.debug("order %s cancelled unfilled (not a fill)", o.order_id[:10])

            delta = target - o.filled
            if delta > 1e-9:
                o.filled = target
                tot = self.up if o.side == "UP" else self.down
                tot.shares += delta
                tot.cost += delta * o.price
                if o.price > tot.max_price:
                    tot.max_price = o.price
                new_notional += delta * o.price
                log.info(
                    "FILL %s %s %.0f sh @ %.3f (order %s)",
                    self.condition_id[:10], o.side, delta, o.price, o.order_id[:10],
                )
        return new_notional

    async def _fills_from_trades(self, client) -> dict[str, float]:
        """order_id -> filled size, from this market's trade history.

        As a passive quoter we are the MAKER: our order ids appear inside each
        trade's maker_orders array, keyed by `matched_amount`. (If we ever
        appeared as taker, taker_order_id covers it too.)"""
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
                            amt = float(getattr(mo, "matched_amount", 0) or 0)
                            sizes[moid] = sizes.get(moid, 0.0) + amt
        except Exception as exc:  # noqa: BLE001
            log.debug("trades poll failed (%s); treating as no-new-information", exc)
        return sizes

    # ------------------------------------------------------------- accounting
    def matched_pairs(self) -> float:
        return min(self.up.shares, self.down.shares)

    def combined_avg(self) -> float | None:
        if self.up.shares and self.down.shares:
            return self.up.avg_price + self.down.avg_price
        return None

    def total_cost(self) -> float:
        return self.up.cost + self.down.cost
