"""Tightly capped automated MetaMask Predictions BTC 5m live execution test.

The runner resolves the active MetaMask Predictions Deposit Wallet, verifies the
locally-entered owner key, fails closed on geo restrictions, tolerates transient FAK
no-fill races, pairs only at the configured complete-set basis, and merges matched
UP/DOWN shares through the MetaMask Predictions relayer.

No private key is printed, written to disk, or accepted as a command-line argument.
"""
from __future__ import annotations

import argparse
import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import httpx
from eth_account import Account
from eth_utils import to_checksum_address
from polymarket import AsyncSecureClient

from tools import metamask_predict_wallet as wallet_tool
from tools import metamask_tiny_live_test as live

DEFAULT_OWNER = "0x349aFa6115f4fa35C4dC8B998bce3F6d4C659E1f"
GEO_URL = "https://polymarket.com/api/geoblock"
FIRST_EXECUTION_SLIPPAGE = Decimal("0.02")


async def _geo_preflight() -> None:
    """Fail closed if Polymarket says the current network location is blocked."""
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as session:
            response = await session.get(
                GEO_URL,
                headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            data = response.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"geoblock preflight failed; refusing live order: {exc}") from exc

    if not isinstance(data, dict) or "blocked" not in data:
        raise RuntimeError(
            f"geoblock preflight returned an unexpected response; refusing live order: {data!r}"
        )
    if data.get("blocked") is True:
        country = str(data.get("country") or "restricted region")
        raise RuntimeError(
            f"Polymarket reports this connection as geo-restricted ({country}); refusing live order"
        )
    print(f"GEO       eligible ({data.get('country') or 'country not reported'})")


def _resolve_wallet(owner: str) -> tuple[str, bool]:
    owner = to_checksum_address(owner)
    beacon = wallet_tool.read_beacon(wallet_tool.POLYGON_RPC)
    uups = wallet_tool.derive_uups(owner)
    uups_deployed = wallet_tool.code_exists(uups, wallet_tool.POLYGON_RPC)

    if beacon:
        beacon_wallet = wallet_tool.derive_beacon(owner, beacon)
        beacon_deployed = wallet_tool.code_exists(beacon_wallet, wallet_tool.POLYGON_RPC)
    else:
        beacon_wallet = None
        beacon_deployed = False

    resolved = uups if uups_deployed or not beacon_wallet else beacon_wallet
    deployed = uups_deployed if resolved == uups else beacon_deployed
    return to_checksum_address(resolved), bool(deployed)


def _is_no_fill(exc: BaseException) -> bool:
    text = str(exc).lower()
    return (
        "no orders found to match" in text
        or "fak_not_filled" in text
        or "fak order" in text and "not" in text and "fill" in text
        or "fok_not_filled" in text
    )


async def _safe_buy(
    client: Any,
    *,
    token_id: str,
    amount: Decimal,
    max_price: Decimal,
    label: str,
) -> Any:
    """Use the current SDK BUY signature and surface rejected responses."""
    print(f"{label}  BUY amount=${amount:.6f} max_price={max_price:.6f}")
    response = await client.place_market_order(
        token_id=token_id,
        side="BUY",
        amount=str(amount),
        max_price=str(max_price),
        order_type="FAK",
    )
    if not bool(getattr(response, "ok", False)):
        code = getattr(response, "code", "unknown")
        message = getattr(response, "message", "order rejected")
        raise RuntimeError(f"{label} rejected [{code}]: {message}")
    return response


async def _robust_trade_window(
    client: AsyncSecureClient,
    market: Any,
    *,
    first_max: Decimal,
    pair_max: Decimal,
    emergency_pair_max: Decimal,
    max_total_spend: Decimal,
    test_shares: Decimal,
) -> str:
    """Trade one window, treating a transient FAK miss as a market race, not fatal."""
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    wallet = str(client.wallet)
    collateral = str(ctx.environment_config.collateral_token)
    ctf = str(ctx.environment_config.conditional_tokens)
    up = market.up_token_id
    down = market.down_token_id

    if market.neg_risk:
        print("SKIP      BTC 5m unexpectedly marked neg-risk")
        return "no-trade"

    initial_pos = await live._positions(ctf, wallet, up, down)  # pyright: ignore[reportPrivateUsage]
    if initial_pos != (Decimal(0), Decimal(0)):
        raise RuntimeError(f"refusing test: pre-existing position on {market.slug}: {initial_pos}")

    initial_cash = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
    print(f"CASH      ${initial_cash:.6f}")

    first_side: str | None = None
    first_token = ""
    first_after = initial_pos
    first_cash_after = initial_cash

    while market.seconds_to_end > live.ENTRY_STOP_S:
        up_book, down_book = await live._books(client, up, down)  # pyright: ignore[reportPrivateUsage]
        up_ask = live._best_ask(up_book)  # pyright: ignore[reportPrivateUsage]
        down_ask = live._best_ask(down_book)  # pyright: ignore[reportPrivateUsage]
        if up_ask and down_ask:
            candidates = [
                ("UP", up, up_ask, up_book),
                ("DOWN", down, down_ask, down_book),
            ]
            candidates.sort(key=lambda item: item[2][0])
            side, token, (ask, _size), book = candidates[0]
            if ask <= first_max:
                minimum = Decimal(str(getattr(book, "min_order_size", None) or test_shares))
                shares = max(test_shares, minimum)
                tick = Decimal(str(getattr(book, "tick_size", None) or "0.01"))
                # Trigger remains <= first_max; the tiny hard execution cap permits
                # two cents for the REST/order latency race that caused the first miss.
                max_price = min(
                    Decimal("0.99"),
                    first_max + FIRST_EXECUTION_SLIPPAGE,
                    ask + max(tick, FIRST_EXECUTION_SLIPPAGE),
                )
                amount = max_price * shares
                if amount > max_total_spend:
                    print(
                        f"SKIP      first-leg minimum would cost ${amount:.4f} "
                        f"> cap ${max_total_spend}"
                    )
                    return "no-trade"

                print(
                    f"TRIGGER   first={side} ask={ask} shares_target={shares} "
                    f"hard_max={max_price} T-{market.seconds_to_end:.1f}s"
                )
                cash_before = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
                try:
                    await _safe_buy(
                        client,
                        token_id=token,
                        amount=amount,
                        max_price=max_price,
                        label="LEG1",
                    )
                except Exception as exc:  # noqa: BLE001
                    if _is_no_fill(exc):
                        print("MISS      LEG1 book moved before FAK matched; continuing to watch")
                        await asyncio.sleep(0.15)
                        continue
                    raise

                candidate_after = await live._wait_position_change(  # pyright: ignore[reportPrivateUsage]
                    ctf, wallet, up, down, initial_pos, timeout_s=4
                )
                candidate_cash = await live._wait_cash_below(  # pyright: ignore[reportPrivateUsage]
                    collateral, wallet, cash_before, timeout_s=4
                )
                idx = 0 if side == "UP" else 1
                if candidate_after[idx] <= initial_pos[idx] or candidate_cash >= cash_before:
                    print("MISS      LEG1 accepted but no settled position change; continuing to watch")
                    await asyncio.sleep(0.15)
                    continue

                first_side = side
                first_token = token
                first_after = candidate_after
                first_cash_after = candidate_cash
                break
        await asyncio.sleep(live.POLL_S)

    if first_side is None:
        print("NO TRADE  no first-leg fill before entry stop")
        return "no-trade"

    first_idx = 0 if first_side == "UP" else 1
    first_shares = first_after[first_idx] - initial_pos[first_idx]
    first_spend = initial_cash - first_cash_after
    if first_shares <= 0 or first_spend <= 0:
        print(f"LEG1 FAIL position={first_after} spend={first_spend}; stopping")
        return "exposure-unknown"

    first_unit = first_spend / first_shares
    other_side = "DOWN" if first_side == "UP" else "UP"
    other_token = down if first_side == "UP" else up
    other_idx = 1 - first_idx
    print(
        f"LEG1 FILL {first_side} shares={first_shares:.6f} "
        f"spend=${first_spend:.6f} all-in/unit=${first_unit:.6f}; "
        f"{other_side} target <= ${pair_max - first_unit:.6f}"
    )

    while market.seconds_to_end > 1.5:
        up_book, down_book = await live._books(client, up, down)  # pyright: ignore[reportPrivateUsage]
        other_book = down_book if other_side == "DOWN" else up_book
        ask_info = live._best_ask(other_book)  # pyright: ignore[reportPrivateUsage]
        if not ask_info:
            await asyncio.sleep(live.POLL_S)
            continue

        ask, _ = ask_info
        current = await live._positions(ctf, wallet, up, down)  # pyright: ignore[reportPrivateUsage]
        deficit = current[first_idx] - current[other_idx]
        if deficit <= Decimal("0.000001"):
            break

        tick = Decimal(str(getattr(other_book, "tick_size", None) or "0.01"))
        max_price = min(Decimal("0.99"), ask + tick)
        combined = first_unit + max_price
        if combined <= pair_max:
            cash_now = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
            spent = initial_cash - cash_now
            budget = max_total_spend - spent
            amount = min(max_price * deficit, budget)
            if amount <= Decimal("0.01"):
                break

            print(
                f"PAIR      {other_side} ask={ask} max={max_price} "
                f"combined≈{combined:.6f} deficit={deficit:.6f}"
            )
            before = current
            try:
                await _safe_buy(
                    client,
                    token_id=other_token,
                    amount=amount,
                    max_price=max_price,
                    label="LEG2",
                )
            except Exception as exc:  # noqa: BLE001
                if _is_no_fill(exc):
                    print("MISS      LEG2 book moved before FAK matched; continuing hedge watch")
                    await asyncio.sleep(0.15)
                    continue
                raise
            await live._wait_position_change(  # pyright: ignore[reportPrivateUsage]
                ctf, wallet, up, down, before, timeout_s=4
            )
            await asyncio.sleep(0.15)
            continue

        if market.seconds_to_end <= live.EMERGENCY_S:
            # The emergency basis is intentionally equal to pair_max in this wrapper,
            # so we do not deliberately complete an unprofitable pair. Flatten the
            # unmatched first leg at the available bid instead.
            first_book = up_book if first_side == "UP" else down_book
            bid_info = live._best_bid(first_book)  # pyright: ignore[reportPrivateUsage]
            current = await live._positions(ctf, wallet, up, down)  # pyright: ignore[reportPrivateUsage]
            excess = current[first_idx] - current[other_idx]
            if bid_info and excess > Decimal("0.000001"):
                bid, _ = bid_info
                first_tick = Decimal(str(getattr(first_book, "tick_size", None) or "0.01"))
                min_price = max(Decimal("0.001"), bid - first_tick)
                print(
                    f"FLATTEN   no pair <= {emergency_pair_max}; "
                    f"selling excess {first_side} at min {min_price}"
                )
                try:
                    await live._sell(  # pyright: ignore[reportPrivateUsage]
                        client,
                        token_id=first_token,
                        shares=excess,
                        min_price=min_price,
                        label="EXIT",
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"EXIT MISS flatten order failed: {exc}")
                await asyncio.sleep(1)
                final_cash = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
                print(f"FLAT PNL  ${final_cash - initial_cash:+.6f}")
                return "flattened"
            break

        await asyncio.sleep(live.POLL_S)

    final_pos = await live._positions(ctf, wallet, up, down)  # pyright: ignore[reportPrivateUsage]
    pair_shares = min(final_pos)
    cash_premerge = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
    total_spend = initial_cash - cash_premerge
    locked_floor = pair_shares - total_spend

    if pair_shares <= Decimal("0.000001"):
        print(f"STOP      no matched pair; positions={final_pos}")
        return "exposure-unknown"

    print(
        f"LOCKED    UP={final_pos[0]:.6f} DOWN={final_pos[1]:.6f} "
        f"spent=${total_spend:.6f} gross_floor=${locked_floor:+.6f}"
    )

    tx_hash = await live._merge_via_metamask(  # pyright: ignore[reportPrivateUsage]
        client,
        condition_id=market.condition_id,
        pair_shares=pair_shares,
    )
    await asyncio.sleep(2)
    await live._best_effort_flatten_residual(client, market, initial_cash)  # pyright: ignore[reportPrivateUsage]
    cash_after = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
    pos_after = await live._positions(ctf, wallet, up, down)  # pyright: ignore[reportPrivateUsage]
    print(f"RESULT    merge_tx={tx_hash}")
    print(
        f"RESULT    pUSD ${initial_cash:.6f} -> ${cash_after:.6f} "
        f"| net=${cash_after - initial_cash:+.6f}"
    )
    print(f"RESULT    residual UP={pos_after[0]:.6f} DOWN={pos_after[1]:.6f}")
    return "merged"


async def amain(owner: str) -> int:
    owner = to_checksum_address(owner)
    await _geo_preflight()

    wallet, deployed = _resolve_wallet(owner)
    print(f"OWNER     {owner}")
    print(f"PREDICT   {wallet} deployed={deployed}")
    if not deployed:
        raise RuntimeError(
            "resolved MetaMask Predictions deposit wallet is not deployed; refusing live order"
        )

    private_key = live._private_key()  # pyright: ignore[reportPrivateUsage]
    signer = to_checksum_address(Account.from_key(private_key).address)
    if signer.lower() != owner.lower():
        raise RuntimeError(
            f"private key belongs to {signer}, expected MetaMask owner {owner}; refusing live order"
        )

    # Reuse the already verified in-memory key and replace the live module's window
    # trader with the no-fill-tolerant implementation above.
    live._private_key = lambda: private_key  # type: ignore[method-assign]
    live._trade_window = _robust_trade_window  # type: ignore[method-assign]

    args = SimpleNamespace(
        wallet=wallet,
        first_max=Decimal("0.25"),
        pair_max=Decimal("0.90"),
        emergency_pair_max=Decimal("0.90"),
        max_total_spend=Decimal("4.75"),
        test_shares=Decimal("5"),
        max_windows=12,
    )

    print("LIMITS    trigger<=0.25 LEG1-hard<=0.27 pair<=0.90 spend<=4.75 target=5sh")
    print("MODE      FAK misses are retried; no deliberate losing emergency pair")
    return await live.amain(args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one tightly capped MetaMask Predictions BTC 5m live test"
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="MetaMask owner EOA")
    args = parser.parse_args()
    try:
        rc = asyncio.run(amain(args.owner))
    except KeyboardInterrupt:
        rc = 130
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL     {type(exc).__name__}: {exc}")
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
