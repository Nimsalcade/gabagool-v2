"""Atomic tape-group fill then ordinary V5 layer reconciliation."""
from __future__ import annotations

import argparse
import csv
import io
from types import SimpleNamespace

from src.public_tape import TapePrint, atomic_tape_groups
from src.shadow import ShadowOrder
from tools.run_forensic_15m_paper import EVENT_FIELDS, Engine, Order


def _lvl(price: float, size: float = 100.0) -> SimpleNamespace:
    return SimpleNamespace(price=price, size=size)


def _book(ask: float, bid: float | None = None, tick: float = 0.01) -> SimpleNamespace:
    if bid is None:
        bid = max(tick, round(ask - tick, 10))
    return SimpleNamespace(tick_size=tick, asks=[_lvl(ask)], bids=[_lvl(bid)])


def _engine() -> Engine:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EVENT_FIELDS)
    writer.writeheader()
    args = argparse.Namespace(
        paper_cash=2_000.0,
        quote_ttl=10.0,
        maker_fill_backend="public_tape",
        taker_mode="off",
        max_combined_vwap=1.01,
        poll=0.5,
        fresh_pair_cap=0.0,
    )
    market = SimpleNamespace(
        asset="btc",
        slug="btc-updown-15m-test",
        condition_id="0xcond",
        window_start=1_000_000.0,
        window_end=1_000_900.0,
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


def _resting(eng: Engine, side: str, price: float, *, oid: str, shares: float = 10.0) -> Order:
    posted = eng.market.window_start + 20.0
    shadow = ShadowOrder(
        side=side,
        token_id=eng.market.up_token_id if side == "UP" else eng.market.down_token_id,
        price=price,
        shares=shares,
        queue_ahead=0.0,
        posted_ts=posted,
    )
    order = Order(
        oid=oid, side=side, price=price, shares=shares,
        created=posted, expires=posted + 10.0, shadow=shadow,
    )
    eng.orders[oid] = order
    return order


def _print(eng: Engine, *, token: str, price: float, size: float, ts: float, tx: str = "") -> TapePrint:
    return TapePrint(
        token_id=token, side="SELL", price=price, size=size,
        event_ts=ts, source="test", tx_id=tx,
    )


def test_atomic_groups_share_transaction_then_timestamp():
    a = TapePrint("tokU", "SELL", 0.49, 10, 1.000, "ws", "0xaaa")
    b = TapePrint("tokU", "SELL", 0.48, 10, 1.001, "ws", "0xaaa")
    c = TapePrint("tokU", "SELL", 0.47, 10, 1.000, "ws", "0xbbb")
    d = TapePrint("tokD", "SELL", 0.50, 4, 2.000, "ws", "")
    e = TapePrint("tokD", "SELL", 0.51, 4, 2.000, "ws", "")
    groups = atomic_tape_groups([c, d, b, e, a])
    assert [ [p.tx_id or round(p.event_ts, 3) for p in g] for g in groups ] == [
        ["0xaaa", "0xaaa"],
        ["0xbbb"],
        [2.0, 2.0],
    ]


def test_leading_layer_removed_after_fill_crosses_3_5_clip_gap():
    e = _engine()
    # 3.4 clips heavy UP → still 1 leading layer. One 10-share fill → 4.4 → 0.
    e.inv.add("UP", 34.0, 0.49, e.market.window_start + 20)
    for i, px in enumerate((0.49, 0.48, 0.47, 0.46)):
        _resting(e, "UP", px, oid=f"U{i}")
    _resting(e, "DOWN", 0.49, oid="D0")
    now = e.market.window_start + 50.0
    up_book, down_book = _book(0.50), _book(0.50)
    e.tape_maker_fills(
        now,
        [_print(e, token="tokU", price=0.49, size=10, ts=now)],
        up_book, down_book,
    )
    assert e.inv.up_shares == 44.0
    assert abs(e.inv.policy().abs_gap / 10.0 - 4.4) < 1e-9
    assert all(o.side != "UP" for o in e.orders.values())
    assert any(o.oid == "D0" for o in e.orders.values())
    assert any(r["event"] == "INVENTORY_LAYER_DROP" for r in _events(e))


def test_same_atomic_group_fills_before_reconcile():
    e = _engine()
    e.inv.add("UP", 34.0, 0.49, e.market.window_start + 20)
    top = _resting(e, "UP", 0.49, oid="U0")
    deep = _resting(e, "UP", 0.48, oid="U1")
    leftover = _resting(e, "UP", 0.47, oid="U2")
    now = e.market.window_start + 50.0
    up_book, down_book = _book(0.50), _book(0.50)
    prints = [
        _print(e, token="tokU", price=0.49, size=10, ts=now, tx="0xsame"),
        _print(e, token="tokU", price=0.48, size=10, ts=now, tx="0xsame"),
    ]
    e.tape_maker_fills(now, prints, up_book, down_book)
    fills = [r for r in _events(e) if r["event"] == "MAKER_FILL"]
    assert len(fills) == 2
    assert {round(float(r["price"]), 2) for r in fills} == {0.49, 0.48}
    assert e.inv.up_shares == 54.0
    assert top.oid not in e.orders
    assert deep.oid not in e.orders
    assert leftover.oid not in e.orders
    rec = [r for r in _events(e) if r["event"] == "INVENTORY_LAYER_DROP"]
    assert rec and rec[0]["order_id"] == "U2"
    rec_idx = [i for i, r in enumerate(_events(e)) if r["event"] == "INVENTORY_LAYER_DROP"]
    fill_idx = [i for i, r in enumerate(_events(e)) if r["event"] == "MAKER_FILL"]
    assert min(rec_idx) > max(fill_idx)


def test_later_print_does_not_fill_leading_layer_cancelled_after_prior_group():
    e = _engine()
    e.inv.add("UP", 34.0, 0.49, e.market.window_start + 20)
    _resting(e, "UP", 0.49, oid="U0")
    deep = _resting(e, "UP", 0.48, oid="U1")
    now = e.market.window_start + 50.0
    up_book, down_book = _book(0.50), _book(0.50)
    e.tape_maker_fills(
        now,
        [
            _print(e, token="tokU", price=0.49, size=10, ts=now, tx="0x1"),
            _print(e, token="tokU", price=0.48, size=10, ts=now + 1.0, tx="0x2"),
        ],
        up_book, down_book,
    )
    fills = [r for r in _events(e) if r["event"] == "MAKER_FILL"]
    assert len(fills) == 1
    assert round(float(fills[0]["price"]), 2) == 0.49
    assert e.inv.up_shares == 44.0
    assert deep.oid not in e.orders
    assert deep.shadow is not None and deep.shadow.filled == 0.0


def test_same_price_survives_reconcile_when_still_desired():
    e = _engine()
    o = _resting(e, "UP", 0.49, oid="U0")
    now = e.market.window_start + 50.0
    up_book, down_book = _book(0.50), _book(0.50)
    e.tape_maker_fills(
        now,
        [_print(e, token="tokU", price=0.49, size=3, ts=now)],
        up_book, down_book,
    )
    assert o.oid in e.orders
    assert o.shadow is not None
    assert abs(o.shadow.filled - 3) < 1e-9
    assert abs(o.shadow.queue_ahead) < 1e-9
    assert not any(
        r["event"] in (
            "INVENTORY_LAYER_DROP", "REPRICE_BACKOFF",
            "REPRICE_BACKOFF_2T", "REPRICE_BACKOFF_3PLUS",
        ) and r["order_id"] == "U0" for r in _events(e)
    )
