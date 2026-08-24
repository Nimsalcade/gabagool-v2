"""Paper-only joint-exposure override. Not a recovered Gabagool constant."""
from __future__ import annotations

import argparse
import csv
import io
from types import SimpleNamespace

from src.forensic_15m import complementary_base_bid
from src.joint_exposure import (
    FRESH_PAIR_CATASTROPHIC,
    apply_joint_exposure_override,
    complementary_anchor,
)
from src.shadow import ShadowOrder
from tools.run_forensic_15m_paper import EVENT_FIELDS, Engine, Order


def test_posted_complementary_bases_never_exceed_one():
    max_pair = 0.0
    n_valid = 0
    for ai in range(1, 100):
        for di in range(1, 100):
            a, d = ai / 100, di / 100
            ub = complementary_base_bid(own_best_ask=a, opposite_best_ask=d, tick=0.01)
            db = complementary_base_bid(own_best_ask=d, opposite_best_ask=a, tick=0.01)
            if ub is None or db is None:
                continue
            n_valid += 1
            max_pair = max(max_pair, ub + db)
    assert n_valid > 9000
    assert max_pair <= 0.99 + 1e-12


def test_complementary_anchors_can_reproduce_the_1_25_state():
    # UP ask 0.10, DOWN ask 0.65 → anchors 0.90 + 0.35 = 1.25
    assert complementary_anchor(0.65, 0.01) == 0.35
    assert complementary_anchor(0.10, 0.01) == 0.90
    posted_up = complementary_base_bid(
        own_best_ask=0.10, opposite_best_ask=0.65, tick=0.01
    )
    posted_dn = complementary_base_bid(
        own_best_ask=0.65, opposite_best_ask=0.10, tick=0.01
    )
    assert posted_up is not None and posted_dn is not None
    assert posted_up + posted_dn <= 0.99 + 1e-12
    assert complementary_anchor(0.65, 0.01) + complementary_anchor(0.10, 0.01) == 1.25


def test_pair_at_or_below_cap_leaves_layers_unchanged():
    layers = {"UP": 4, "DOWN": 3}
    out = apply_joint_exposure_override(
        up_base=0.48, down_base=0.50, layers=layers, signed_gap=12.0, cap=1.05
    )
    assert out == {"UP": 4, "DOWN": 3}


def test_catastrophic_pair_suppresses_leading_only():
    layers = {"UP": 4, "DOWN": 4}
    up_heavy = apply_joint_exposure_override(
        up_base=0.35, down_base=0.90, layers=layers, signed_gap=20.0, cap=1.05
    )
    assert up_heavy == {"UP": 0, "DOWN": 4}
    dn_heavy = apply_joint_exposure_override(
        up_base=0.35, down_base=0.90, layers=layers, signed_gap=-20.0, cap=1.05
    )
    assert dn_heavy == {"UP": 4, "DOWN": 0}


def test_catastrophic_pair_when_balanced_posts_nothing_new():
    out = apply_joint_exposure_override(
        up_base=0.35, down_base=0.90, layers={"UP": 4, "DOWN": 4},
        signed_gap=0.0, cap=1.05,
    )
    assert out == {"UP": 0, "DOWN": 0}


def test_missing_base_or_disabled_cap_is_noop():
    layers = {"UP": 2, "DOWN": 4}
    assert apply_joint_exposure_override(
        up_base=None, down_base=0.90, layers=layers, signed_gap=10.0, cap=1.05
    ) == layers
    assert apply_joint_exposure_override(
        up_base=0.35, down_base=0.90, layers=layers, signed_gap=10.0, cap=0.0
    ) == layers


def _lvl(price: float, size: float = 100.0) -> SimpleNamespace:
    return SimpleNamespace(price=price, size=size)


def _book(ask: float, bid: float | None = None, tick: float = 0.01) -> SimpleNamespace:
    if bid is None:
        bid = max(tick, round(ask - tick, 10))
    return SimpleNamespace(tick_size=tick, asks=[_lvl(ask)], bids=[_lvl(bid)])


def _engine(*, cash: float = 2000.0, cap: float = 1.05) -> Engine:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EVENT_FIELDS)
    writer.writeheader()
    args = argparse.Namespace(
        paper_cash=cash,
        quote_ttl=10.0,
        maker_fill_backend="public_tape",
        taker_mode="off",
        max_combined_vwap=1.01,
        poll=0.5,
        fresh_pair_cap=cap,
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


def test_renew_does_not_replenish_leading_when_anchor_pair_catastrophic():
    e = _engine()
    e.inv.add("UP", 20.0, 0.30, e.market.window_start + 20)
    # UP ask 0.10, DOWN ask 0.65 → anchors 0.90 + 0.35 = 1.25; UP is leading.
    up_book, down_book = _book(0.10), _book(0.65)
    now = e.market.window_start + 30.0
    e.renew(now, up_book, down_book)
    sides = {o.side for o in e.orders.values()}
    assert "UP" not in sides
    assert "DOWN" in sides
    assert any(r["event"] == "JOINT_EXPOSURE" for r in _events(e))


def test_keepalive_does_not_cancel_resting_leading_on_temporary_1_05():
    e = _engine()
    e.inv.add("UP", 20.0, 0.30, e.market.window_start + 20)
    shadow = ShadowOrder(
        side="UP", token_id="tokU", price=0.47, shares=10.0,
        queue_ahead=100.0, posted_ts=e.market.window_start + 25, filled=0.0,
    )
    order = Order(
        oid="P1-BTC-KEEP", side="UP", price=0.47, shares=10.0,
        created=shadow.posted_ts, expires=shadow.posted_ts + 10.0, shadow=shadow,
    )
    e.orders[order.oid] = order
    # Ordinary V5 still wants 0.47 (near-touching books); override is irrelevant
    # to keepalive. Polarized books would move the complementary price and V5
    # itself would drop the order — that is not this gate.
    up_book, down_book = _book(0.48), _book(0.52)
    e.desired = lambda *_: {"UP": (0.47, 0.46, 0.45, 0.44), "DOWN": (0.51,)}  # type: ignore[method-assign]
    e.expire(order.expires, up_book, down_book)
    assert order.oid in e.orders
    assert any(r["event"] == "QUEUE_KEEP" for r in _events(e))


def test_harness_default_fresh_pair_cap_is_disabled():
    assert FRESH_PAIR_CATASTROPHIC == 1.05
    from tools.run_forensic_15m_paper import parse_args
    import sys
    old = sys.argv
    try:
        sys.argv = ["run_forensic_15m_paper.py"]
        args = parse_args()
    finally:
        sys.argv = old
    assert args.fresh_pair_cap == 0.0
