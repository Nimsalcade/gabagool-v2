from src.public_tape import apply_sell_print_to_orders
from src.shadow import ShadowOrder


def order(price, shares=10.0, queue=0.0, posted=1.0):
    return ShadowOrder(
        side="UP",
        token_id="tok",
        price=price,
        shares=shares,
        queue_ahead=queue,
        posted_ts=posted,
    )


def test_trade_above_bid_does_not_fill():
    o = order(.50)
    assert apply_sell_print_to_orders([o], trade_price=.51, trade_size=100) == []
    assert o.filled == 0


def test_equal_price_consumes_queue_then_partial_fills():
    o = order(.50, shares=10, queue=7)
    fills = apply_sell_print_to_orders([o], trade_price=.50, trade_size=12)
    assert len(fills) == 1
    assert abs(fills[0][1] - 5) < 1e-9
    assert abs(o.queue_ahead) < 1e-9
    assert abs(o.filled - 5) < 1e-9
    assert abs(o.remaining - 5) < 1e-9


def test_below_bid_volume_is_not_multiplied_across_layers():
    high = order(.52, shares=10, posted=1)
    low = order(.51, shares=10, posted=2)
    fills = apply_sell_print_to_orders(
        [low, high],
        trade_price=.50,
        trade_size=12,
    )
    assert [(round(o.price, 2), qty) for o, qty in fills] == [
        (.52, 10),
        (.51, 2),
    ]
    assert high.done
    assert abs(low.filled - 2) < 1e-9
    assert abs(low.remaining - 8) < 1e-9


def test_higher_price_has_priority_over_older_lower_price():
    high = order(.53, shares=8, posted=10)
    low = order(.52, shares=8, posted=1)
    fills = apply_sell_print_to_orders(
        [low, high],
        trade_price=.50,
        trade_size=10,
    )
    assert [(round(o.price, 2), qty) for o, qty in fills] == [
        (.53, 8),
        (.52, 2),
    ]
