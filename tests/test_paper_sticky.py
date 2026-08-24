"""V5.2 sticky ladder: no upward chase, backoff when too aggressive."""
from __future__ import annotations

import argparse
import csv
import io
from types import SimpleNamespace

from src.shadow import ShadowOrder
from src.sticky_ladder import plan_sticky_side
from tools.run_forensic_15m_paper import EVENT_FIELDS, Engine, Order


def test_plan_anchor_rise_keeps_old_ladder_and_skips_higher():
    old = [
        ("a", 0.49, 1.0),
        ("b", 0.48, 1.1),
        ("c", 0.47, 1.2),
        ("d", 0.46, 1.3),
    ]
    plan = plan_sticky_side(
        orders=old, current_base=0.51, desired_n=4, tick=0.01,
    )
    assert plan.keep_oids == ("a", "b", "c", "d")
    assert plan.backoff_oids == ()
    assert plan.drop_oids == ()
    assert plan.replenish_prices == ()
    assert plan.skipped_higher == (0.51, 0.50)
    assert set(plan.sticky_keep_oids) == {"c", "d"}


def test_plan_anchor_fall_backs_off_too_aggressive():
    old = [
        ("a", 0.49, 1.0),
        ("b", 0.48, 1.1),
        ("c", 0.47, 1.2),
        ("d", 0.46, 1.3),
    ]
    plan = plan_sticky_side(
        orders=old, current_base=0.47, desired_n=4, tick=0.01,
    )
    # V5.2a: 0.49 is 2 ticks → backoff; 0.48 is 1 tick → hysteresis keep.
    assert set(plan.backoff_oids) == {"a"}
    assert plan.backoff_2t_oids == ("a",)
    assert plan.backoff_3plus_oids == ()
    assert plan.hysteresis_1t_oids == ("b",)
    assert set(plan.keep_oids) == {"b", "c", "d"}
    assert plan.replenish_prices == (0.45,)


def test_plan_1_tick_hysteresis_keeps_fifo():
    old = [("a", 0.48, 1.0), ("b", 0.47, 1.1), ("c", 0.46, 1.2), ("d", 0.45, 1.3)]
    plan = plan_sticky_side(
        orders=old, current_base=0.47, desired_n=4, tick=0.01,
    )
    assert plan.backoff_oids == ()
    assert plan.hysteresis_1t_oids == ("a",)
    assert plan.keep_oids == ("a", "b", "c", "d")
    assert plan.replenish_prices == ()


def test_plan_3plus_ticks_backs_off():
    old = [("a", 0.50, 1.0), ("b", 0.47, 1.1)]
    plan = plan_sticky_side(
        orders=old, current_base=0.47, desired_n=2, tick=0.01,
    )
    assert plan.backoff_3plus_oids == ("a",)
    assert plan.backoff_2t_oids == ()
    assert plan.keep_oids == ("b",)


def test_plan_inventory_4_to_2_drops_deepest():
    old = [
        ("a", 0.49, 1.0),
        ("b", 0.48, 1.1),
        ("c", 0.47, 1.2),
        ("d", 0.46, 1.3),
    ]
    plan = plan_sticky_side(
        orders=old, current_base=0.49, desired_n=2, tick=0.01,
    )
    assert plan.keep_oids == ("a", "b")
    assert plan.drop_oids == ("c", "d")
    assert plan.replenish_prices == ()


def test_plan_vacancy_replenishes_at_current_anchor():
    remain = [
        ("a", 0.49, 1.0),
        ("b", 0.48, 1.1),
        ("c", 0.47, 1.2),
    ]
    plan = plan_sticky_side(
        orders=remain, current_base=0.51, desired_n=4, tick=0.01,
    )
    assert plan.keep_oids == ("a", "b", "c")
    assert plan.replenish_prices == (0.51,)
    assert 0.50 in plan.skipped_higher


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


def _resting(eng: Engine, side: str, price: float, *, oid: str, shares: float = 10.0,
             queue: float = 80.0, created: float | None = None) -> Order:
    posted = eng.market.window_start + 20.0 if created is None else created
    shadow = ShadowOrder(
        side=side,
        token_id=eng.market.up_token_id if side == "UP" else eng.market.down_token_id,
        price=price,
        shares=shares,
        queue_ahead=queue,
        posted_ts=posted,
    )
    order = Order(
        oid=oid, side=side, price=price, shares=shares,
        created=posted, expires=posted + 10.0, shadow=shadow,
    )
    eng.orders[oid] = order
    return order


def test_engine_anchor_rise_preserves_oids_and_does_not_post_higher():
    e = _engine()
    oids = []
    for i, px in enumerate((0.49, 0.48, 0.47, 0.46)):
        oids.append(_resting(e, "UP", px, oid=f"U{i}", queue=160.0 - 10 * i).oid)
    for i, px in enumerate((0.48, 0.47, 0.46, 0.45)):
        _resting(e, "DOWN", px, oid=f"D{i}", queue=90.0)
    # complementary base from DOWN ask 0.49 → 0.51, own ask 0.52 cap 0.51
    up_book, down_book = _book(0.52), _book(0.49)
    now = e.market.window_start + 50.0
    e.apply_sticky_ladder(now, up_book, down_book, replenish=True)
    live = {o.oid: o for o in e.orders.values() if o.side == "UP"}
    assert set(live) == set(oids)
    assert all(live[oid].shadow.queue_ahead == 160.0 - 10 * i for i, oid in enumerate(oids))
    assert all(live[oid].created == e.market.window_start + 20.0 for oid in oids)
    kinds = [r["event"] for r in _events(e)]
    assert "STICKY_KEEP" in kinds
    up_quotes = [
        r for r in _events(e)
        if r["event"] in ("QUOTE", "VACANCY_REPLENISH") and r["side"] == "UP"
    ]
    assert up_quotes == []
    assert not any(abs(float(o.price) - 0.51) < 1e-9 for o in e.orders.values() if o.side == "UP")
    assert not any(abs(float(o.price) - 0.50) < 1e-9 for o in e.orders.values() if o.side == "UP")


def test_engine_anchor_fall_backs_off_overly_aggressive():
    e = _engine()
    hi = _resting(e, "UP", 0.49, oid="U0")
    mid = _resting(e, "UP", 0.48, oid="U1")
    ok = _resting(e, "UP", 0.47, oid="U2")
    deep = _resting(e, "UP", 0.46, oid="U3")
    # DOWN ask 0.53 → complement 0.47; own ask 0.50 cap 0.49 → base 0.47
    up_book, down_book = _book(0.50), _book(0.53)
    now = e.market.window_start + 50.0
    e.apply_sticky_ladder(now, up_book, down_book, replenish=True)
    assert hi.oid not in e.orders
    assert mid.oid in e.orders  # 1-tick hysteresis
    assert ok.oid in e.orders
    assert deep.oid in e.orders
    kinds = [r["event"] for r in _events(e)]
    assert "REPRICE_BACKOFF_2T" in kinds
    assert "HYSTERESIS_KEEP_1T" in kinds
    assert "REPRICE_BACKOFF_3PLUS" not in kinds
    hyst = [r for r in _events(e) if r["event"] == "HYSTERESIS_KEEP_1T"]
    assert hyst and hyst[0]["order_id"] == "U1"


def test_engine_3plus_tick_backoff_event():
    e = _engine()
    hi = _resting(e, "UP", 0.50, oid="U0")
    ok = _resting(e, "UP", 0.47, oid="U1")
    up_book, down_book = _book(0.50), _book(0.53)  # base 0.47
    e.apply_sticky_ladder(e.market.window_start + 50.0, up_book, down_book, replenish=False)
    assert hi.oid not in e.orders
    assert ok.oid in e.orders
    assert any(r["event"] == "REPRICE_BACKOFF_3PLUS" for r in _events(e))


def test_engine_fill_vacancy_replenishes_at_current_anchor():
    e = _engine()
    _resting(e, "UP", 0.49, oid="U0")
    _resting(e, "UP", 0.48, oid="U1")
    _resting(e, "UP", 0.47, oid="U2")
    up_book, down_book = _book(0.52), _book(0.49)  # base 0.51
    now = e.market.window_start + 50.0
    e.apply_sticky_ladder(now, up_book, down_book, replenish=True)
    prices = sorted(round(o.price, 2) for o in e.orders.values() if o.side == "UP")
    assert 0.51 in prices
    assert 0.50 not in prices  # one vacancy only
    assert {0.49, 0.48, 0.47}.issubset(set(prices))
    assert any(r["event"] == "VACANCY_REPLENISH" for r in _events(e))


def test_engine_inventory_4_to_2_drops_deepest_only():
    e = _engine()
    e.inv.add("UP", 20.0, 0.49, e.market.window_start + 20)
    # gap 2.0 clips → 2 leading UP layers
    a = _resting(e, "UP", 0.49, oid="U0")
    b = _resting(e, "UP", 0.48, oid="U1")
    c = _resting(e, "UP", 0.47, oid="U2")
    d = _resting(e, "UP", 0.46, oid="U3")
    up_book, down_book = _book(0.50), _book(0.50)  # base 0.49
    e.apply_sticky_ladder(e.market.window_start + 50.0, up_book, down_book, replenish=True)
    live = {o.oid for o in e.orders.values() if o.side == "UP"}
    assert live == {a.oid, b.oid}
    assert c.oid not in e.orders and d.oid not in e.orders
    assert any(r["event"] == "INVENTORY_LAYER_DROP" for r in _events(e))


def test_atomic_fills_complete_before_sticky_reconcile():
    from src.public_tape import TapePrint
    e = _engine()
    e.inv.add("UP", 34.0, 0.49, e.market.window_start + 20)
    _resting(e, "UP", 0.49, oid="U0", queue=0.0)
    _resting(e, "UP", 0.48, oid="U1", queue=0.0)
    leftover = _resting(e, "UP", 0.47, oid="U2", queue=0.0)
    now = e.market.window_start + 50.0
    prints = [
        TapePrint("tokU", "SELL", 0.49, 10, now, "test", "0xsame"),
        TapePrint("tokU", "SELL", 0.48, 10, now, "test", "0xsame"),
    ]
    e.tape_maker_fills(now, prints, _book(0.50), _book(0.50))
    fills = [r for r in _events(e) if r["event"] == "MAKER_FILL"]
    assert len(fills) == 2
    rec = [i for i, r in enumerate(_events(e))
           if r["event"] in (
               "INVENTORY_LAYER_DROP", "REPRICE_BACKOFF",
               "REPRICE_BACKOFF_2T", "REPRICE_BACKOFF_3PLUS",
           )]
    fill_i = [i for i, r in enumerate(_events(e)) if r["event"] == "MAKER_FILL"]
    assert rec and min(rec) > max(fill_i)
    assert leftover.oid not in e.orders
