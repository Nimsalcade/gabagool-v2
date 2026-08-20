"""Public-tape shadow execution primitives.

This module is deliberately isolated from live order placement.  It estimates whether a
hypothetical resting BUY would have filled from *real* CLOB last-trade events while
respecting visible queue ahead at the posted price.

It is still an estimator: cancellations ahead of us and hidden queue priority are not
fully observable.  The model is conservative at equal price (visible queue must trade
through first) and deterministic when the tape trades below our bid (our higher-priced
bid would have had priority).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ShadowOrder:
    side: str
    token_id: str
    price: float
    shares: float
    queue_ahead: float
    posted_ts: float
    filled: float = 0.0

    @property
    def remaining(self) -> float:
        return max(0.0, self.shares - self.filled)

    @property
    def done(self) -> bool:
        return self.remaining <= 1e-9


def apply_sell_trade(order: ShadowOrder, *, trade_price: float, trade_size: float) -> float:
    """Apply an aggressive SELL print to a hypothetical resting BUY.

    Returns newly filled shares.

    - trade above our bid: cannot fill us;
    - trade exactly at our bid: consume visible queue ahead, then us;
    - trade below our bid: price priority implies our resting bid must have been
      consumed before the lower-price execution, so fill the remaining order.
    """
    if order.done or trade_size <= 0:
        return 0.0
    eps = 1e-9
    if trade_price > order.price + eps:
        return 0.0

    if trade_price < order.price - eps:
        delta = order.remaining
        order.filled += delta
        order.queue_ahead = 0.0
        return delta

    size_left = float(trade_size)
    if order.queue_ahead > 0:
        consumed = min(order.queue_ahead, size_left)
        order.queue_ahead -= consumed
        size_left -= consumed
    if size_left <= 0:
        return 0.0

    delta = min(order.remaining, size_left)
    order.filled += delta
    return delta


def reduce_queue_from_book(order: ShadowOrder, *, visible_size_at_price: float | None) -> None:
    """Conservatively reduce queue-ahead when visible external size shrinks.

    New size at the same price may have joined behind us, so increases never increase
    queue_ahead.  A decrease can represent fills/cancels ahead and is safe to recognize.
    """
    if visible_size_at_price is None or visible_size_at_price < 0:
        return
    order.queue_ahead = min(order.queue_ahead, float(visible_size_at_price))
