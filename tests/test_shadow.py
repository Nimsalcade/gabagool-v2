from src.shadow import ShadowOrder, apply_sell_trade, reduce_queue_from_book


def order(price=0.45, shares=10, queue=20):
    return ShadowOrder(
        side="UP", token_id="tok", price=price, shares=shares,
        queue_ahead=queue, posted_ts=0.0,
    )


def test_trade_above_bid_does_not_fill():
    o = order()
    assert apply_sell_trade(o, trade_price=0.46, trade_size=100) == 0
    assert o.filled == 0


def test_equal_price_consumes_queue_first():
    o = order(queue=20)
    assert apply_sell_trade(o, trade_price=0.45, trade_size=15) == 0
    assert o.queue_ahead == 5
    assert apply_sell_trade(o, trade_price=0.45, trade_size=8) == 3
    assert o.filled == 3
    assert o.queue_ahead == 0


def test_trade_below_bid_implies_price_priority_fill():
    o = order(queue=100)
    assert apply_sell_trade(o, trade_price=0.44, trade_size=1) == 10
    assert o.done


def test_book_shrink_can_reduce_but_not_increase_queue():
    o = order(queue=20)
    reduce_queue_from_book(o, visible_size_at_price=12)
    assert o.queue_ahead == 12
    reduce_queue_from_book(o, visible_size_at_price=30)
    assert o.queue_ahead == 12
