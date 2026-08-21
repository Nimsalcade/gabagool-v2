"""Regression: maker fill indexing may lag cancellation/disappearance."""
from __future__ import annotations

import asyncio

from src.fills import FillTracker


class _Page:
    def __init__(self, items):
        self.items = items


class _Paginator:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        self.done = False
        return self

    async def __anext__(self):
        if self.done:
            raise StopAsyncIteration
        self.done = True
        return _Page(self.items)


class _MakerOrder:
    def __init__(self, order_id, matched_amount):
        self.order_id = order_id
        self.matched_amount = matched_amount


class _Trade:
    def __init__(self, order_id, amount):
        self.status = "CONFIRMED"
        self.taker_order_id = ""
        self.size = amount
        self.price = .70
        self.maker_orders = [_MakerOrder(order_id, amount)]


class Client:
    def __init__(self):
        self.open = []
        self.trades = []

    def list_open_orders(self, market=None):
        return _Paginator(self.open)

    def list_account_trades(self, market=None):
        return _Paginator(self.trades)


def run(coro):
    return asyncio.run(coro)


def test_maker_order_disappears_then_trade_indexes_later():
    c = Client()
    t = FillTracker(condition_id="cid")
    t.register("m1", "UP", "tok", .45, 10, mode="maker")

    # Order disappears; trade endpoint has not indexed its final execution yet.
    run(t.reconcile(c))
    assert not t.orders["m1"].open
    assert not t.orders["m1"].finalized
    assert t.up.shares == 0
    assert t.resting_notional() == 0

    # A later reconcile sees the authoritative maker trade and recovers the fill.
    c.trades = [_Trade("m1", 6)]
    got = run(t.reconcile(c))
    assert abs(got - 2.70) < 1e-9
    assert abs(t.up.shares - 6) < 1e-9
    assert abs(t.up.cost - 2.70) < 1e-9
