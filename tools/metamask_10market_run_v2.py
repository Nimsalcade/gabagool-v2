"""10-market MetaMask Predictions run using exact-share marketable limit BUYs.

Polymarket marketable BUY orders enforce a $1 collateral minimum. That distorted the
tiny test when five shares at a cheap price cost less than $1. This wrapper keeps the
same 10-market strategy but replaces BUY execution with a crossing limit order sized
in shares, then cancels any unfilled remainder quickly. This preserves the intended
~5-share first leg and exact deficient-share hedge without forcing a $1 minimum BUY.

This experiment is a plumbing/logic validation, not an optimized economic policy. The
only pair-selection invariant is that the projected matched complete-set acquisition
basis must remain strictly below $1. Profit buffers, fee reserves, maker/taker policy,
and optimal entry thresholds are intentionally left for later tuning.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace
from typing import Any

import httpx
from polymarket.errors import TransportError as PolymarketTransportError

from tools import metamask_10market_run as run
from tools import metamask_tiny_live_test as live
from tools import metamask_tiny_order as tiny

# Six-decimal sentinel representing strict < $1 for the current controller's <= check.
# Market tick sizes normally make the executable ceiling lower than this naturally.
PAIR_MAX = Decimal("0.999999")
WINDOW_SPEND_CAP = Decimal("5.00")

# Keep the original function before main() installs the retrying wrapper.
_ORIGINAL_BOOKS = live._books  # pyright: ignore[reportPrivateUsage]


async def _resilient_books(client: Any, up: str, down: str) -> tuple[Any, Any]:
    """Retry transient read-only order-book failures inside the same 5-minute market.

    Book reads are idempotent, unlike order submissions, so they are safe to retry.
    A short transport outage should not consume an entire validation session.
    """
    last_error: BaseException | None = None
    for attempt in range(1, 6):
        try:
            return await _ORIGINAL_BOOKS(client, up, down)
        except (PolymarketTransportError, httpx.TransportError) as exc:
            last_error = exc
            if attempt == 5:
                break
            delay = min(1.0, 0.15 * (2 ** (attempt - 1)))
            print(
                f"NET       BOOKS transient {type(exc).__name__}; "
                f"retry {attempt}/5 in {delay:.2f}s"
            )
            await asyncio.sleep(delay)
    raise RuntimeError(
        "order-book transport unavailable after 5 retries; ending only this session"
    ) from last_error


async def _cancel_after_ambiguous_submit(client: Any, label: str) -> None:
    """Fail closed after an ambiguous POST by cancelling every open test order.

    A transport exception can occur before the request reaches the exchange or after
    the exchange accepted it but before the response reached us. Blindly retrying the
    BUY could therefore double the intended position. This runner never intentionally
    leaves resting orders, so cancel_all() is the safest reconciliation action here.
    Filled shares are not undone; the outer trade loop detects them from on-chain
    position/cash deltas after this function returns.
    """
    last_error: BaseException | None = None
    for attempt in range(1, 4):
        try:
            await asyncio.sleep(0.20 * attempt)
            await client.cancel_all()
            print(
                f"RECOVER   {label} transport result was ambiguous; open orders cancelled, "
                "reconciling actual position before any retry"
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(
                f"RECOVER   {label} cancel-all attempt {attempt}/3 failed: "
                f"{type(exc).__name__}: {exc}"
            )
    raise RuntimeError(
        f"{label}: ambiguous order submission and unable to confirm cancellation; "
        "refusing another order until exposure is reconciled"
    ) from last_error


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
    try:
        response = await client.place_limit_order(
            token_id=token_id,
            price=str(max_price),
            size=str(size),
            side="BUY",
            post_only=False,
        )
    except (PolymarketTransportError, httpx.TransportError) as exc:
        # Do not blindly resubmit. The exchange may have accepted the order even
        # though the HTTP response was lost. Cancel any possible remainder, then
        # let the caller reconcile the real position/cash delta.
        print(f"NET       {label} {type(exc).__name__}: response unknown")
        await _cancel_after_ambiguous_submit(client, label)
        return SimpleNamespace(ok=True, order_id="", status="transport-reconciled")

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
        except (PolymarketTransportError, httpx.TransportError) as exc:
            print(f"NET       {label} cancel {type(exc).__name__}: reconciling with cancel-all")
            await _cancel_after_ambiguous_submit(client, label)
        except Exception:  # noqa: BLE001
            # The order may already be fully filled/cancelled. The outer loop checks
            # actual holdings, so a normal terminal-order cancel rejection is benign.
            pass

    return response


def main() -> None:
    tiny._safe_buy = _exact_share_buy  # type: ignore[method-assign]
    live._books = _resilient_books  # type: ignore[method-assign]

    # Override the base experiment's old arbitrary pair threshold. For this phase,
    # accept any projected matched pair strictly below $1 and let the exchange tick
    # size determine the nearest executable price. Five matched shares require < $5.
    run.PAIR_MAX = PAIR_MAX
    run.MAX_TOTAL_SPEND = WINDOW_SPEND_CAP
    run.MIN_CASH_TO_START_WINDOW = WINDOW_SPEND_CAP

    print(
        "EXECUTION exact-share crossing LIMIT BUYs; no $1 market-BUY minimum; "
        "unfilled remainder auto-cancelled"
    )
    print(
        "PAIR RULE validation mode: projected matched basis must be <1.000000; "
        "LEG2 ceiling = 1.000000 - LEG1 unit cost (subject to executable tick)"
    )
    print(
        "RECOVERY  ambiguous network submits are never blindly retried; possible "
        "resting orders are cancelled and actual holdings are reconciled first"
    )
    print(
        "NETWORK   transient read-only book failures retry inside the same market; "
        "they no longer waste a whole 5-minute session"
    )
    print(
        "GOAL      confirm repeated sub-$1 two-leg acquisition + matched-share MERGE; "
        "economic buffers/tuning come later"
    )
    run.main()


if __name__ == "__main__":
    main()
