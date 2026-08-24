"""Public-tape TTL is a same-price keepalive, not a cancel/repost."""
from __future__ import annotations

import argparse
import csv
import io
from types import SimpleNamespace

from src.shadow import ShadowOrder
from tools.run_forensic_15m_paper import EVENT_FIELDS, Engine, Order


def _engine(*, backend: str = "public_tape", window_start: float = 1_000_000.0) -> Engine:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EVENT_FIELDS)
    writer.writeheader()
    args = argparse.Namespace(
        paper_cash=2_000.0,
        quote_ttl=10.0,
        maker_fill_backend=backend,
        taker_mode="off",
        max_combined_vwap=1.01,
        poll=0.5,
    )
    market = SimpleNamespace(
        asset="btc",
        slug="btc-updown-15m-test",
        condition_id="0xcond",
        window_start=window_start,
        window_end=window_start + 900,
        up_token_id="tokU",
        down_token_id="tokD",
    )
    eng = Engine(session=1, market=market, writer=writer, args=args)
    eng.clip = 10.0
    eng._events_buf = buf
    return eng


def _events(eng: Engine) -> list[dict[str, str]]:
    eng._events_buf.seek(0)
    return list(csv.DictReader(eng._events_buf))


def _order(
    eng: Engine,
    *,
    side: str = "UP",
    price: float = 0.47,
    shares: float = 10.0,
    created: float | None = None,
    queue: float = 190.0,
    filled: float = 0.0,
    oid: str = "P1-BTC-1",
) -> Order:
    posted = eng.market.window_start + 50.0 if created is None else created
    shadow = ShadowOrder(
        side=side,
        token_id=eng.market.up_token_id if side == "UP" else eng.market.down_token_id,
        price=price,
        shares=shares,
        queue_ahead=queue,
        posted_ts=posted,
        filled=filled,
    )
    order = Order(
        oid=oid,
        side=side,
        price=price,
        shares=shares,
        created=posted,
        expires=posted + eng.args.quote_ttl,
        shadow=shadow,
    )
    eng.orders[order.oid] = order
    return order


def test_ttl_keep_same_desired_price_preserves_shadow_identity():
    e = _engine()
    o = _order(e, queue=190.0)
    posted_ts = o.shadow.posted_ts
    e.desired = lambda *_: {"UP": (0.47,), "DOWN": ()}  # type: ignore[method-assign]
    now = o.expires
    e.expire(now, object(), object())

    assert o.oid in e.orders
    kept = e.orders[o.oid]
    assert kept is o
    assert kept.shadow is o.shadow
    assert kept.shadow.posted_ts == posted_ts
    assert kept.shadow.queue_ahead == 190.0
    assert kept.shadow.filled == 0.0
    assert kept.created == posted_ts
    assert kept.expires == now + 10.0
    assert kept.shares == 10.0
    kinds = [r["event"] for r in _events(e)]
    assert "QUEUE_KEEP" in kinds
    assert "EXPIRE" not in kinds


def test_ttl_cancel_when_desired_price_moves_creates_room_for_new_shadow():
    e = _engine()
    o = _order(e, price=0.47, queue=190.0)
    old_shadow = o.shadow
    e.desired = lambda *_: {"UP": (0.46,), "DOWN": ()}  # type: ignore[method-assign]
    now = o.expires
    e.expire(now, object(), object())

    assert o.oid not in e.orders
    assert old_shadow.queue_ahead == 190.0
    kinds = [r["event"] for r in _events(e)]
    assert "EXPIRE" in kinds
    assert "QUEUE_KEEP" not in kinds


def test_ttl_cancel_when_inventory_removes_layer():
    e = _engine()
    o = _order(e, price=0.44, queue=80.0)
    e.desired = lambda *_: {"UP": (0.47, 0.46, 0.45), "DOWN": ()}  # type: ignore[method-assign]
    e.expire(o.expires, object(), object())
    assert o.oid not in e.orders
    assert any(r["event"] == "EXPIRE" for r in _events(e))


def test_ttl_keep_partial_fill_does_not_reset_parent_size():
    e = _engine()
    o = _order(e, shares=10.0, filled=3.0, queue=83.4)
    e.desired = lambda *_: {"UP": (0.47,), "DOWN": ()}  # type: ignore[method-assign]
    now = o.expires
    e.expire(now, object(), object())

    kept = e.orders[o.oid]
    assert kept.shares == 10.0
    assert abs(kept.remaining - 7.0) < 1e-9
    assert abs(kept.shadow.filled - 3.0) < 1e-9
    assert kept.shadow.queue_ahead == 83.4
    row = [r for r in _events(e) if r["event"] == "QUEUE_KEEP"][0]
    assert abs(float(row["qty"]) - 7.0) < 1e-9


def test_clip_schedule_change_does_not_resize_or_cancel_resting_parent():
    e = _engine()
    o = _order(e, shares=10.0, queue=120.0)
    e.clip = 9.0
    e.desired = lambda *_: {"UP": (0.47,), "DOWN": ()}  # type: ignore[method-assign]
    now = o.expires
    e.expire(now, object(), object())

    kept = e.orders[o.oid]
    assert kept.shares == 10.0
    assert kept.shadow.shares == 10.0
    assert kept.shadow.posted_ts == o.created
    assert any(r["event"] == "QUEUE_KEEP" for r in _events(e))


def test_snapshot_cross_ttl_still_cancels_even_if_price_desired():
    e = _engine(backend="snapshot_cross")
    o = _order(e, queue=190.0)
    e.desired = lambda *_: {"UP": (0.47,), "DOWN": ()}  # type: ignore[method-assign]
    e.expire(o.expires, object(), object())
    assert o.oid not in e.orders
    assert any(r["event"] == "EXPIRE" for r in _events(e))
