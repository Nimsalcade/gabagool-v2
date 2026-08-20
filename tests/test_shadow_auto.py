from datetime import datetime, timezone
from types import SimpleNamespace

from tools.shadow_market_auto import ResilientShadowMarket


def trade(*, ts=1000.0, tx="0xabc", token="tok", side="SELL", price="0.45", size="10"):
    return SimpleNamespace(
        timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
        transaction_hash=tx,
        token_id=token,
        side=side,
        price=price,
        size=size,
    )


def test_trade_ts_datetime_to_epoch():
    t = trade(ts=1234.5)
    assert ResilientShadowMarket._trade_ts(t) == 1234.5


def test_trade_key_is_stable_for_same_public_trade():
    a = trade()
    b = trade()
    assert ResilientShadowMarket._trade_key(a) == ResilientShadowMarket._trade_key(b)


def test_trade_key_changes_for_distinct_fill_fields():
    a = trade(size="10")
    b = trade(size="11")
    assert ResilientShadowMarket._trade_key(a) != ResilientShadowMarket._trade_key(b)


def test_trade_ts_accepts_millisecond_numeric_timestamp():
    t = SimpleNamespace(timestamp=1_700_000_000_000)
    assert ResilientShadowMarket._trade_ts(t) == 1_700_000_000.0
