from polymarket.models.clob.market_events import MarketBestBidAskEvent, MarketPriceChangeEvent

from src.paper_reversal_pair_ws import _apply_event, _replace_quote
from src.reversal_pair import TopOfBook


def test_replace_quote_preserves_depth_fields():
    old = TopOfBook(best_bid=0.49, best_ask=0.51, ask_size=123.0, min_order_size=5.0)
    new = _replace_quote(old, best_bid=0.64, best_ask=0.66)
    assert new is not None
    assert new.best_bid == 0.64
    assert new.best_ask == 0.66
    assert new.ask_size == 123.0
    assert new.min_order_size == 5.0


def test_best_bid_ask_event_updates_only_matching_token():
    up = TopOfBook(0.49, 0.51, 100.0, 5.0)
    down = TopOfBook(0.49, 0.51, 90.0, 5.0)
    event = MarketBestBidAskEvent(
        type="best_bid_ask",
        payload={
            "market": "m",
            "asset_id": "UPTOKEN",
            "best_bid": "0.64",
            "best_ask": "0.66",
            "spread": "0.02",
        },
    )
    new_up, new_down, _, _ = _apply_event(
        event,
        up_token_id="UPTOKEN",
        down_token_id="DNTOKEN",
        up=up,
        down=down,
    )
    assert new_up is not None and new_down is not None
    assert new_up.best_bid == 0.64
    assert new_up.best_ask == 0.66
    assert new_up.ask_size == 100.0
    assert new_down == down


def test_price_change_uses_streamed_bbo_for_both_tokens():
    up = TopOfBook(0.49, 0.51, 100.0, 5.0)
    down = TopOfBook(0.49, 0.51, 90.0, 5.0)
    event = MarketPriceChangeEvent(
        type="price_change",
        payload={
            "market": "m",
            "price_changes": [
                {
                    "asset_id": "UPTOKEN",
                    "price": "0.65",
                    "size": "10",
                    "side": "BUY",
                    "best_bid": "0.65",
                    "best_ask": "0.67",
                },
                {
                    "asset_id": "DNTOKEN",
                    "price": "0.33",
                    "size": "10",
                    "side": "SELL",
                    "best_bid": "0.32",
                    "best_ask": "0.34",
                },
            ],
        },
    )
    new_up, new_down, _, _ = _apply_event(
        event,
        up_token_id="UPTOKEN",
        down_token_id="DNTOKEN",
        up=up,
        down=down,
    )
    assert new_up is not None and new_down is not None
    assert (new_up.best_bid, new_up.best_ask) == (0.65, 0.67)
    assert (new_down.best_bid, new_down.best_ask) == (0.32, 0.34)
