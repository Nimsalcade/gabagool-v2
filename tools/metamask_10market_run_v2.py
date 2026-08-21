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
PAIR_MAX = Decimal("0.999999")
WINDOW_SPEND_CAP = Decimal("5.00")

# Preserve originals before main() installs validation/retry wrappers.
_ORIGINAL_BOOKS = live._books  # pyright: ignore[reportPrivateUsage]
_ORIGINAL_POSITIONS = live._positions  # pyright: ignore[reportPrivateUsage]
_ORIGINAL_PUSD_BALANCE = live._pusd_balance  # pyright: ignore[reportPrivateUsage]
_ORIGINAL_BEST_ASK = live._best_ask  # pyright: ignore[reportPrivateUsage]
_ORIGINAL_CTF_BALANCE = live._ctf_balance  # pyright: ignore[reportPrivateUsage]

# Filled LEG1 state used only to widen LEG2's crossing limit to the full economically
# admissible price band. It is reset automatically when the books switch markets.
_LEG1_TOKEN: str | None = None
_LEG1_UNIT: Decimal | None = None


async def _read_retry(label: str, call: Any, *args: Any) -> Any:
    """Retry idempotent network reads without turning a brief outage into exposure."""
    last_error: BaseException | None = None
    for attempt in range(1, 8):
        try:
            return await call(*args)
        except (PolymarketTransportError, httpx.TransportError) as exc:
            last_error = exc
            if attempt == 7:
                break
            delay = min(1.25, 0.15 * (2 ** (attempt - 1)))
            print(
                f"NET       {label} transient {type(exc).__name__}; "
                f"retry {attempt}/7 in {delay:.2f}s"
            )
            await asyncio.sleep(delay)
    raise RuntimeError(f"{label} unavailable after 7 retries") from last_error


async def _resilient_books(client: Any, up: str, down: str) -> tuple[Any, Any]:
    global _LEG1_TOKEN, _LEG1_UNIT
    # A new pair of token ids means a new five-minute market. Never let the prior
    # market's LEG1 cost influence this market's entry book.
    if _LEG1_TOKEN is not None and _LEG1_TOKEN not in {str(up), str(down)}:
        _LEG1_TOKEN = None
        _LEG1_UNIT = None
    return await _read_retry("BOOKS", _ORIGINAL_BOOKS, client, up, down)


async def _resilient_positions(
    ctf: str,
    wallet: str,
    up: str,
    down: str,
) -> tuple[Decimal, Decimal]:
    return await _read_retry("POSITIONS", _ORIGINAL_POSITIONS, ctf, wallet, up, down)


async def _resilient_pusd_balance(token: str, wallet: str) -> Decimal:
    return await _read_retry("CASH", _ORIGINAL_PUSD_BALANCE, token, wallet)


def _validation_best_ask(book: Any) -> tuple[Decimal, Decimal] | None:
    """Expose the full admissible LEG2 crossing cap to the base controller.

    The base controller normally crosses only one tick above the observed ask. That is
    too timid for this validation: an ask at .70 can move to .72 before the order gets
    there even when LEG1=.24 and every fill through .75 still leaves the pair < $1.

    Once LEG1's actual all-in unit cost is known, synthesize the controller-facing ask
    so its existing `ask + tick` rule becomes the highest valid exchange tick below
    PAIR_MAX - LEG1. The real ask must already be at or below that cap; otherwise the
    untouched real ask is returned and no invalid hedge is attempted.
    """
    real = _ORIGINAL_BEST_ASK(book)
    if real is None or _LEG1_TOKEN is None or _LEG1_UNIT is None:
        return real

    token_id = str(getattr(book, "token_id", ""))
    if token_id == _LEG1_TOKEN:
        return real

    real_price, real_size = real
    tick = Decimal(str(getattr(book, "tick_size", None) or "0.01"))
    raw_cap = PAIR_MAX - _LEG1_UNIT
    if tick <= 0 or raw_cap <= 0:
        return real

    cap = (raw_cap / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    # Guard strictness against any decimal/tick edge case.
    while cap > 0 and _LEG1_UNIT + cap > PAIR_MAX:
        cap -= tick

    if cap <= 0 or real_price > cap:
        return real

    # tiny._robust_trade_window subsequently adds one tick to the ask. Feed it
    # cap-tick so the actual limit submitted is exactly `cap`, consuming any asks
    # from the real best ask through the highest still-valid sub-$1 price.
    controller_ask = max(tick, cap - tick)
    if controller_ask != real_price:
        print(
            f"LEG2 CAP  real_ask={real_price} full_sub1_cap={cap} "
            f"(LEG1 all-in={_LEG1_UNIT:.6f})"
        )
    return controller_ask, real_size


async def _cancel_after_ambiguous_submit(client: Any, label: str) -> None:
    """Fail closed after an ambiguous POST by cancelling every open test order."""
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
    global _LEG1_TOKEN, _LEG1_UNIT

    amount = Decimal(str(amount))
    max_price = Decimal(str(max_price))
    if amount <= 0 or max_price <= 0:
        raise RuntimeError(f"{label}: invalid amount/price")

    size = (amount / max_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    if size <= 0:
        raise RuntimeError(f"{label}: computed zero share size")

    # Snapshot LEG1 locally so the validation book wrapper can later calculate the
    # widest LEG2 price that still leaves the actual acquired pair below $1.
    leg1_before_cash: Decimal | None = None
    leg1_before_shares: Decimal | None = None
    collateral = ""
    ctf = ""
    wallet = ""
    if label == "LEG1":
        _LEG1_TOKEN = str(token_id)
        _LEG1_UNIT = None
        ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
        wallet = str(client.wallet)
        collateral = str(ctx.environment_config.collateral_token)
        ctf = str(ctx.environment_config.conditional_tokens)
        leg1_before_cash = await _resilient_pusd_balance(collateral, wallet)
        leg1_before_shares = await _read_retry(
            "LEG1 TOKEN", _ORIGINAL_CTF_BALANCE, ctf, wallet, str(token_id)
        )

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
        print(f"NET       {label} {type(exc).__name__}: response unknown")
        await _cancel_after_ambiguous_submit(client, label)
        response = SimpleNamespace(ok=True, order_id="", status="transport-reconciled")

    if not bool(getattr(response, "ok", False)):
        code = getattr(response, "code", "unknown")
        message = getattr(response, "message", "order rejected")
        raise RuntimeError(f"{label} rejected [{code}]: {message}")

    order_id = str(getattr(response, "order_id", "") or "")
    await asyncio.sleep(0.25)
    if order_id:
        try:
            await client.cancel_order(order_id=order_id)
        except (PolymarketTransportError, httpx.TransportError) as exc:
            print(f"NET       {label} cancel {type(exc).__name__}: reconciling with cancel-all")
            await _cancel_after_ambiguous_submit(client, label)
        except Exception:  # noqa: BLE001
            pass

    if (
        label == "LEG1"
        and leg1_before_cash is not None
        and leg1_before_shares is not None
    ):
        # Give settlement a short chance; this does not submit anything and is safe to
        # retry. The outer controller performs its own definitive reconciliation too.
        for _ in range(8):
            after_cash = await _resilient_pusd_balance(collateral, wallet)
            after_shares = await _read_retry(
                "LEG1 TOKEN", _ORIGINAL_CTF_BALANCE, ctf, wallet, str(token_id)
            )
            acquired = after_shares - leg1_before_shares
            spent = leg1_before_cash - after_cash
            if acquired > Decimal("0.000001") and spent > 0:
                _LEG1_UNIT = spent / acquired
                print(
                    f"LEG2 ROOM actual LEG1 all-in={_LEG1_UNIT:.6f}; "
                    f"raw opposite ceiling={PAIR_MAX - _LEG1_UNIT:.6f}"
                )
                break
            await asyncio.sleep(0.20)

    return response


def main() -> None:
    tiny._safe_buy = _exact_share_buy  # type: ignore[method-assign]
    live._books = _resilient_books  # type: ignore[method-assign]
    live._positions = _resilient_positions  # type: ignore[method-assign]
    live._pusd_balance = _resilient_pusd_balance  # type: ignore[method-assign]
    live._best_ask = _validation_best_ask  # type: ignore[method-assign]

    run.PAIR_MAX = PAIR_MAX
    run.MAX_TOTAL_SPEND = WINDOW_SPEND_CAP
    run.MIN_CASH_TO_START_WINDOW = WINDOW_SPEND_CAP

    print(
        "EXECUTION exact-share crossing LIMIT BUYs; no $1 market-BUY minimum; "
        "unfilled remainder auto-cancelled"
    )
    print(
        "PAIR RULE validation mode: projected matched basis must be <1.000000; "
        "LEG2 uses the FULL remaining sub-$1 price room, not merely ask+1 tick"
    )
    print(
        "RECOVERY  ambiguous network submits are never blindly retried; possible "
        "resting orders are cancelled and actual holdings are reconciled first"
    )
    print(
        "NETWORK   book, position, and pUSD reads retry inside the same market so a "
        "transient timeout after LEG1 does not abandon the hedge loop"
    )
    print(
        "GOAL      confirm repeated sub-$1 two-leg acquisition + matched-share MERGE; "
        "economic buffers/tuning come later"
    )
    run.main()


if __name__ == "__main__":
    main()
