"""Fill reconciliation tests: cancels are NEVER fills; partials are exact."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.fills import FillTracker  # noqa: E402


class _Page:
    def __init__(self, items):
        self.items = items


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def __aiter__(self):
        self._it = iter(self._pages)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class _OpenOrder:
    def __init__(self, id, size_matched):
        self.id, self.size_matched = id, size_matched


class _MakerOrder:
    def __init__(self, order_id, matched_amount):
        self.order_id, self.matched_amount = order_id, matched_amount


class _Trade:
    def __init__(self, maker_orders, status="CONFIRMED", taker_order_id="", size=0):
        self.maker_orders = maker_orders
        self.status = status
        self.taker_order_id = taker_order_id
        self.size = size


class FakeClient:
    def __init__(self):
        self.open_orders = []
        self.trades = []

    def list_open_orders(self, market=None):
        return _Paginator([_Page(self.open_orders)])

    def list_account_trades(self, market=None):
        return _Paginator([_Page(self.trades)])


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_cancel_is_not_a_fill():
    c = FakeClient()
    t = FillTracker(condition_id="0xc0nd")
    t.register("o1", "UP", "tokU", 0.45, 10)
    # order vanished from the book, zero trades attributed → cancel
    c.open_orders, c.trades = [], []
    got = run(t.reconcile(c))
    assert got == 0.0
    assert t.up.shares == 0.0 and not t.orders["o1"].open


def test_partial_fill_via_size_matched_then_full_via_trades():
    c = FakeClient()
    t = FillTracker(condition_id="0xc0nd")
    t.register("o1", "UP", "tokU", 0.45, 10)

    c.open_orders = [_OpenOrder("o1", 4)]            # partial while resting
    run(t.reconcile(c))
    assert abs(t.up.shares - 4) < 1e-9

    c.open_orders = []                                # left the book…
    c.trades = [_Trade([_MakerOrder("o1", 10)])]      # …trades say 10 total
    run(t.reconcile(c))
    assert abs(t.up.shares - 10) < 1e-9
    assert abs(t.up.cost - 4.5) < 1e-9


def test_failed_trades_do_not_count():
    c = FakeClient()
    t = FillTracker(condition_id="0xc0nd")
    t.register("o2", "DOWN", "tokD", 0.50, 8)
    c.open_orders = []
    c.trades = [_Trade([_MakerOrder("o2", 8)], status="FAILED")]
    run(t.reconcile(c))
    assert t.down.shares == 0.0


def test_matched_pairs_math():
    t = FillTracker(condition_id="x")
    t.up.shares, t.down.shares = 12.0, 9.5
    assert t.matched_pairs() == 9.5
