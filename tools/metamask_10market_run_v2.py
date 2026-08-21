"""10-market MetaMask Predictions run using exact-share marketable limit BUYs.

Polymarket marketable BUY orders enforce a $1 collateral minimum. That distorted the
tiny test when five shares at a cheap price cost less than $1. This wrapper keeps the
same 10-market strategy but replaces BUY execution with a crossing limit order sized
in shares, then cancels any unfilled remainder quickly. This preserves the intended
~5-share first leg and exact deficient-share hedge without forcing a $1 minimum BUY.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_DOWN
from typing import Any

from tools import metamask_10market_run as run
from tools import metamask_tiny_order as tiny


async def _exact_share_buy(
    client: Any,
    *,
    token_id: str,
    amount: Decimal,
    max_price: Decimal,
    label: str,
) -> Any:
    """Cross the book with an exact-share limit BUY and cancel any remainder."""
    amount = Decimal(str(amount))
    max_price = Decimal(str(max_price))
    if amount <= 0 or max_price <= 0:
        raise RuntimeError(f"{label}: invalid amount/price")

    # tiny._robust_trade_window computes amount = max_price * desired_shares.
    # Recover that desired share quantity here so cheap orders below $1 remain valid.
    size = (amount / max_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    if size <= 0:
        raise RuntimeError(f"{label}: computed zero share size")

    print(
        f"{label}  LIMIT-BUY size={size:.6f} price_cap={max_price:.6f} "
        f"notional_cap=${amount:.6f}"
    )
    response = await client.place_limit_order(
        token_id=token_id,
        price=str(max_price),
        size=str(size),
        side="BUY",
        post_only=False,
    )

    if not bool(getattr(response, "ok", False)):
        code = getattr(response, "code", "unknown")
        message = getattr(response, "message", "order rejected")
        raise RuntimeError(f"{label} rejected [{code}]: {message}")

    # A crossing GTC limit can leave an unfilled remainder resting. Give immediate
    # matching a brief chance, then cancel the remainder. Already-filled orders may
    # reject cancellation; that is harmless and is intentionally ignored.
    order_id = str(getattr(response, "order_id", "") or "")
    await asyncio.sleep(0.25)
    if order_id:
        try:
            await client.cancel_order(order_id=order_id)
        except Exception:  # noqa: BLE001
            pass

    return response


def main() -> None:
    tiny._safe_buy = _exact_share_buy  # type: ignore[method-assign]
    print(
        "EXECUTION exact-share crossing LIMIT BUYs; no $1 market-BUY minimum; "
        "unfilled remainder auto-cancelled"
    )
    run.main()


if __name__ == "__main__":
    main()
