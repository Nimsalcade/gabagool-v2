"""Production restart-recovery tests."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.recovery import recover_wallet_state


class _Page:
    def __init__(self, items):
        self.items = tuple(items)


class _Paginator:
    def __init__(self, items):
        self.items = list(items)

    async def first_page(self):
        return _Page(self.items)

    def __aiter__(self):
        self._done = False
        return self

    async def __anext__(self):
        if self._done:
            raise StopAsyncIteration
        self._done = True
        return _Page(self.items)


class _Pos:
    def __init__(
        self,
        *,
        condition_id,
        outcome,
        outcome_index,
        size,
        avg_price,
        slug,
        redeemable=False,
        mergeable=False,
    ):
        self.condition_id = condition_id
        self.outcome = outcome
        self.outcome_index = outcome_index
        self.size = size
        self.avg_price = avg_price
        self.slug = slug
        self.redeemable = redeemable
        self.mergeable = mergeable


class FakeClient:
    def __init__(self, *, positions=(), open_orders=(), end_by_slug=None):
        self.positions = list(positions)
        self.open_orders = list(open_orders)
        self.end_by_slug = dict(end_by_slug or {})
        self.cancel_calls = 0

    def list_open_orders(self):
        return _Paginator(self.open_orders)

    async def cancel_all(self):
        self.cancel_calls += 1
        self.open_orders = []
        return SimpleNamespace(canceled=())

    def list_positions(self, **kwargs):
        assert kwargs.get("size_threshold") == 0.0
        return _Paginator(self.positions)

    async def get_market(self, *, slug):
        if slug not in self.end_by_slug:
            raise RuntimeError("unknown slug")
        return SimpleNamespace(
            state=SimpleNamespace(
                end_date=datetime.fromtimestamp(self.end_by_slug[slug], tz=timezone.utc)
            )
        )


def run(coro):
    return asyncio.run(coro)


def test_recovery_cancels_stale_orders_and_seeds_active_inventory(monkeypatch):
    import src.recovery as recovery

    monkeypatch.setattr(recovery.time, "time", lambda: 1_000.0)
    positions = [
        _Pos(
            condition_id="cid1", outcome="Up", outcome_index=0, size=10,
            avg_price=.41, slug="btc-updown-5m-1",
        ),
        _Pos(
            condition_id="cid1", outcome="Down", outcome_index=1, size=9,
            avg_price=.55, slug="btc-updown-5m-1",
        ),
    ]
    client = FakeClient(
        positions=positions,
        open_orders=[SimpleNamespace(id="old")],
        end_by_slug={"btc-updown-5m-1": 1_200.0},
    )
    got = run(recover_wallet_state(client))
    assert client.cancel_calls == 1
    assert got.canceled_orders == 1
    assert not got.settlement_due
    seed = got.active["cid1"]
    assert seed.up_shares == 10
    assert seed.down_shares == 9
    assert abs(seed.up_cost - 4.10) < 1e-9
    assert abs(seed.down_cost - 4.95) < 1e-9
    assert abs(got.committed_cost - 9.05) < 1e-9


def test_recovery_routes_redeemable_position_to_settlement(monkeypatch):
    import src.recovery as recovery

    monkeypatch.setattr(recovery.time, "time", lambda: 1_000.0)
    client = FakeClient(
        positions=[
            _Pos(
                condition_id="cid2", outcome="Up", outcome_index=0, size=7,
                avg_price=.48, slug="btc-updown-5m-2", redeemable=True,
            )
        ],
        end_by_slug={"btc-updown-5m-2": 900.0},
    )
    got = run(recover_wallet_state(client))
    assert "cid2" in got.settlement_due
    assert "cid2" not in got.active


def test_recovery_quarantines_unresolved_unknown_market(monkeypatch):
    import src.recovery as recovery

    monkeypatch.setattr(recovery.time, "time", lambda: 1_000.0)
    client = FakeClient(
        positions=[
            _Pos(
                condition_id="cid3", outcome="Down", outcome_index=1, size=5,
                avg_price=.52, slug="missing-slug",
            )
        ]
    )
    got = run(recover_wallet_state(client))
    assert "cid3" in got.settlement_due
    assert "cid3" not in got.active
