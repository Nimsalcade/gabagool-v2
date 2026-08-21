"""Automated tiny live BTC 5m complete-set test for a MetaMask Predictions wallet.

This is intentionally one-shot and tightly capped. It watches fresh BTC 5-minute
Up/Down markets, opens at most one cheap first leg, acquires the opposite leg when
the combined basis is acceptable, merges matched pairs through MetaMask's Predictions
relayer, and exits. Near expiry it either completes a near-flat pair or sells the
first leg back out instead of carrying an unintended directional bet through expiry.

The private key is never printed or stored by this tool. If POLY_PRIVATE_KEY is not
already present in the environment, the tool asks for it with a hidden terminal prompt.
Never paste a private key into chat.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

import httpx
from eth_abi.abi import decode as abi_decode
from eth_abi.abi import encode as abi_encode
from eth_utils import keccak, to_checksum_address
from polymarket import AsyncSecureClient
from polymarket._internal.actions import account as _account_actions
from polymarket._internal.actions.orders.context import resolve_exchange_address
from polymarket._internal.actions.relayer.calls import (
    TransactionCall,
    erc1155_set_approval_for_all_call,
    erc20_approval_call,
    merge_positions_call,
)
from polymarket._internal.actions.relayer.signing.deposit_wallet import (
    sign_deposit_wallet_batch,
)
from polymarket._internal.wallet import signature_type_for
from polymarket.types import EvmAddress

from src.discovery import resolve_market, window_start_epoch

MM_RELAYER = "https://predict.api.cx.metamask.io/transaction"
RPC_URL = "https://polygon.drpc.org"
CHAIN_ID = 137
MAX_UINT256 = (1 << 256) - 1
MICRO = 1_000_000

DEFAULT_FIRST_MAX = Decimal("0.35")
DEFAULT_PAIR_MAX = Decimal("0.90")
DEFAULT_EMERGENCY_PAIR_MAX = Decimal("1.03")
DEFAULT_MAX_TOTAL_SPEND = Decimal("6.00")
DEFAULT_TEST_SHARES = Decimal("5")
DEFAULT_MAX_WINDOWS = 12
POLL_S = 0.50
ENTRY_STOP_S = 60.0
EMERGENCY_S = 15.0

_ERC20_ALLOWANCE = keccak(text="allowance(address,address)")[:4]
_ERC20_BALANCE_OF = keccak(text="balanceOf(address)")[:4]
_ERC1155_BALANCE_OF = keccak(text="balanceOf(address,uint256)")[:4]
_ERC1155_IS_APPROVED = keccak(text="isApprovedForAll(address,address)")[:4]


def _private_key() -> str:
    key = (os.getenv("POLY_PRIVATE_KEY") or "").strip()
    if not key:
        key = getpass.getpass(
            "MetaMask owner private key (hidden; used only in memory, never printed): "
        ).strip()
    if key.startswith("0x"):
        key = key[2:]
    if len(key) != 64:
        raise SystemExit("Private key must be 32 bytes / 64 hex characters.")
    int(key, 16)
    return "0x" + key


def _fmt(value: Decimal | float) -> str:
    return f"{Decimal(str(value)):.6f}"


async def _rpc(method: str, params: list[Any]) -> Any:
    async with httpx.AsyncClient(
        timeout=15,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
    ) as session:
        response = await session.post(
            RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        response.raise_for_status()
        body = response.json()
    if "error" in body:
        raise RuntimeError(f"RPC {method} failed: {body['error']}")
    return body.get("result")


async def _eth_call(to: str, data: str) -> str:
    return await _rpc(
        "eth_call",
        [{"to": to_checksum_address(to), "data": data}, "latest"],
    )


def _call_data(selector: bytes, types: list[str], values: list[Any]) -> str:
    return "0x" + (selector + abi_encode(types, values)).hex()


async def _pusd_balance(token: str, wallet: str) -> Decimal:
    data = _call_data(_ERC20_BALANCE_OF, ["address"], [wallet])
    raw = await _eth_call(token, data)
    return Decimal(int(raw, 16)) / MICRO


async def _wait_cash_below(token: str, wallet: str, before: Decimal, timeout_s: float = 8) -> Decimal:
    deadline = time.time() + timeout_s
    last = before
    while time.time() < deadline:
        last = await _pusd_balance(token, wallet)
        if last < before:
            return last
        await asyncio.sleep(0.35)
    return last


async def _erc20_allowance(token: str, owner: str, spender: str) -> int:
    data = _call_data(_ERC20_ALLOWANCE, ["address", "address"], [owner, spender])
    raw = await _eth_call(token, data)
    return int(raw, 16)


async def _erc1155_approved(token: str, owner: str, operator: str) -> bool:
    data = _call_data(_ERC1155_IS_APPROVED, ["address", "address"], [owner, operator])
    raw = await _eth_call(token, data)
    return bool(abi_decode(["bool"], bytes.fromhex(raw[2:]))[0])


async def _ctf_balance(token: str, wallet: str, token_id: str) -> Decimal:
    data = _call_data(
        _ERC1155_BALANCE_OF,
        ["address", "uint256"],
        [wallet, int(token_id)],
    )
    raw = await _eth_call(token, data)
    return Decimal(int(raw, 16)) / MICRO


async def _positions(ctf: str, wallet: str, up: str, down: str) -> tuple[Decimal, Decimal]:
    values = await asyncio.gather(
        _ctf_balance(ctf, wallet, up),
        _ctf_balance(ctf, wallet, down),
    )
    return values[0], values[1]


async def _mm_proxy(envelope: dict[str, Any]) -> Any:
    async with httpx.AsyncClient(timeout=20) as session:
        response = await session.post(
            MM_RELAYER,
            headers={"Content-Type": "application/json"},
            json=envelope,
        )
        text = response.text
        if not response.is_success:
            raise RuntimeError(
                f"MetaMask relayer HTTP {response.status_code}: {text}"
            )
    if not text:
        raise RuntimeError("MetaMask relayer returned an empty response")
    data = json.loads(text)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"MetaMask relayer: {data['error']}")
    return data


async def _mm_nonce(owner: str) -> str:
    data = await _mm_proxy(
        {
            "path": "/nonce",
            "method": "GET",
            "query": {"address": owner, "type": "WALLET"},
        }
    )
    nonce = str(data.get("nonce", "")) if isinstance(data, dict) else ""
    if not nonce:
        raise RuntimeError(f"MetaMask relayer nonce missing: {data!r}")
    return nonce


async def _mm_submit_batch(
    client: AsyncSecureClient,
    calls: list[TransactionCall],
    label: str,
) -> str:
    if not calls:
        raise ValueError("empty MetaMask batch")

    # These SDK internals implement the exact DepositWallet EIP-712 signature used
    # by MetaMask/Polymarket. The submit itself goes through MetaMask's Predictions
    # relayer proxy, so no separate Polymarket Relayer API key is required here.
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    owner = str(client.signer)
    wallet = str(client.wallet)
    nonce = await _mm_nonce(owner)
    deadline = str(int(time.time()) + 300)

    signature = sign_deposit_wallet_batch(
        ctx.signer,
        wallet=ctx.wallet,
        calls=calls,
        nonce=nonce,
        deadline=deadline,
        chain_id=CHAIN_ID,
    )

    result = await _mm_proxy(
        {
            "path": "/submit",
            "method": "POST",
            "body": {
                "type": "WALLET",
                "from": owner,
                "to": str(
                    ctx.environment_config.wallet_derivation.deposit_wallet_factory
                ),
                "nonce": nonce,
                "signature": signature,
                "depositWalletParams": {
                    "depositWallet": wallet,
                    "deadline": deadline,
                    "calls": [
                        {
                            "target": str(call.to),
                            "value": str(call.value),
                            "data": call.data,
                        }
                        for call in calls
                    ],
                },
            },
        }
    )

    txid = ""
    if isinstance(result, dict):
        txid = str(result.get("transactionID") or result.get("id") or "")
    if not txid:
        raise RuntimeError(
            f"{label}: relayer response missing transaction id: {result!r}"
        )

    for _ in range(30):
        polled = await _mm_proxy(
            {
                "path": "/transaction",
                "method": "GET",
                "query": {"id": txid},
            }
        )
        tx = polled[0] if isinstance(polled, list) and polled else polled
        if isinstance(tx, dict):
            state = str(tx.get("state") or "")
            tx_hash = str(tx.get("transactionHash") or "")
            if state in {"STATE_FAILED", "STATE_INVALID"}:
                raise RuntimeError(
                    f"{label}: relayer transaction {txid} {state}"
                )
            if state == "STATE_CONFIRMED" and tx_hash:
                print(f"{label}: confirmed {tx_hash}")
                return tx_hash
        await asyncio.sleep(1)

    raise RuntimeError(
        f"{label}: timed out waiting for MetaMask relayer transaction {txid}"
    )


async def _refresh_clob_allowance(
    client: AsyncSecureClient,
    asset_type: str,
    token_id: str | None,
) -> None:
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    signature_type = signature_type_for(ctx.wallet_type)
    path, params = _account_actions.build_update_balance_allowance_request(
        asset_type=asset_type,
        token_id=token_id,
        signature_type=signature_type,
    )
    await ctx.secure_clob.get_bytes(path, params=params)


async def _ensure_standard_trade_approvals(
    client: AsyncSecureClient,
    *,
    max_total_spend: Decimal,
) -> None:
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    wallet = str(client.wallet)
    collateral = str(ctx.environment_config.collateral_token)
    ctf = str(ctx.environment_config.conditional_tokens)
    exchange = str(resolve_exchange_address(ctx.environment_config, False))

    calls: list[TransactionCall] = []
    required = int(max_total_spend * MICRO)
    allowance = await _erc20_allowance(collateral, wallet, exchange)
    approved = await _erc1155_approved(ctf, wallet, exchange)

    if allowance < required:
        calls.append(
            erc20_approval_call(
                token_address=EvmAddress(collateral),
                spender=EvmAddress(exchange),
                amount=MAX_UINT256,
            )
        )
    if not approved:
        calls.append(
            erc1155_set_approval_for_all_call(
                token_address=EvmAddress(ctf),
                operator=EvmAddress(exchange),
                approved=True,
            )
        )

    if calls:
        print(
            f"PRECHECK  setting {len(calls)} missing standard trading approval(s) "
            "through MetaMask relayer"
        )
        await _mm_submit_batch(client, calls, "APPROVALS")
        await asyncio.sleep(1)
        await _refresh_clob_allowance(client, "COLLATERAL", None)
    else:
        print("PRECHECK  standard BUY/SELL approvals already present")


def _best_ask(book: Any) -> tuple[Decimal, Decimal] | None:
    levels = [
        (Decimal(str(level.price)), Decimal(str(level.size)))
        for level in (getattr(book, "asks", None) or [])
    ]
    return min(levels, key=lambda item: item[0]) if levels else None


def _best_bid(book: Any) -> tuple[Decimal, Decimal] | None:
    levels = [
        (Decimal(str(level.price)), Decimal(str(level.size)))
        for level in (getattr(book, "bids", None) or [])
    ]
    return max(levels, key=lambda item: item[0]) if levels else None


async def _books(
    client: AsyncSecureClient,
    up: str,
    down: str,
) -> tuple[Any, Any]:
    values = await asyncio.gather(
        client.get_order_book(token_id=up),
        client.get_order_book(token_id=down),
    )
    return values[0], values[1]


async def _wait_position_change(
    ctf: str,
    wallet: str,
    up: str,
    down: str,
    before: tuple[Decimal, Decimal],
    *,
    timeout_s: float = 10,
) -> tuple[Decimal, Decimal]:
    deadline = time.time() + timeout_s
    last = before
    while time.time() < deadline:
        last = await _positions(ctf, wallet, up, down)
        if last != before:
            return last
        await asyncio.sleep(0.4)
    return last


async def _buy(
    client: AsyncSecureClient,
    *,
    token_id: str,
    amount: Decimal,
    max_price: Decimal,
    label: str,
) -> Any:
    print(
        f"{label}  BUY amount=${_fmt(amount)} max_price={_fmt(max_price)}"
    )
    return await client.place_market_order(
        token_id=token_id,
        side="BUY",
        amount=str(amount),
        max_spend=str(amount),
        max_price=str(max_price),
        order_type="FAK",
    )


async def _sell(
    client: AsyncSecureClient,
    *,
    token_id: str,
    shares: Decimal,
    min_price: Decimal,
    label: str,
) -> Any:
    print(
        f"{label}  SELL shares={_fmt(shares)} min_price={_fmt(min_price)}"
    )
    return await client.place_market_order(
        token_id=token_id,
        side="SELL",
        shares=str(shares),
        min_price=str(min_price),
        order_type="FAK",
    )


async def _merge_via_metamask(
    client: AsyncSecureClient,
    *,
    condition_id: str,
    pair_shares: Decimal,
) -> str:
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    position_ctx = await client._resolve_market_position_context(  # pyright: ignore[reportPrivateUsage]
        condition_id=condition_id
    )
    micro = int((pair_shares * MICRO).to_integral_value(rounding=ROUND_DOWN))
    if micro <= 0:
        raise RuntimeError("no mergeable micro-shares")
    call = merge_positions_call(
        target=position_ctx.adapter_address,
        collateral=EvmAddress(str(ctx.environment_config.collateral_token)),
        condition_id=condition_id,
        amount=micro,
    )
    print(f"MERGE     {Decimal(micro) / MICRO:.6f} matched shares")
    return await _mm_submit_batch(client, [call], "MERGE")


async def _resolve_fresh_market(
    client: AsyncSecureClient,
    start: int,
) -> Any:
    while time.time() < start + 20:
        market = await resolve_market(client, "btc", 300, start)
        if market is not None and market.accepting_orders:
            return market
        await asyncio.sleep(0.5)
    return None


async def _best_effort_flatten_residual(
    client: AsyncSecureClient,
    market: Any,
    initial_cash: Decimal,
) -> None:
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    ctf = str(ctx.environment_config.conditional_tokens)
    wallet = str(client.wallet)
    up, down = market.up_token_id, market.down_token_id
    pos = await _positions(ctf, wallet, up, down)
    if max(pos) <= Decimal("0.000001"):
        return

    up_book, down_book = await _books(client, up, down)
    for side, token, shares, book in (
        ("UP", up, pos[0], up_book),
        ("DOWN", down, pos[1], down_book),
    ):
        if shares <= Decimal("0.000001"):
            continue
        bid_info = _best_bid(book)
        if not bid_info:
            print(f"CLEANUP   residual {side}={shares:.6f}; no bid available")
            continue
        bid, _ = bid_info
        tick = Decimal(str(getattr(book, "tick_size", None) or "0.01"))
        min_price = max(Decimal("0.001"), bid - tick)
        try:
            await _sell(
                client,
                token_id=token,
                shares=shares,
                min_price=min_price,
                label="CLEANUP",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"CLEANUP   residual {side} sell failed: {exc}")

    await asyncio.sleep(1)
    cash = await _pusd_balance(str(ctx.environment_config.collateral_token), wallet)
    pos_after = await _positions(ctf, wallet, up, down)
    print(
        f"CLEANUP   pUSD delta=${cash - initial_cash:+.6f} "
        f"residual UP={pos_after[0]:.6f} DOWN={pos_after[1]:.6f}"
    )


async def _trade_window(
    client: AsyncSecureClient,
    market: Any,
    *,
    first_max: Decimal,
    pair_max: Decimal,
    emergency_pair_max: Decimal,
    max_total_spend: Decimal,
    test_shares: Decimal,
) -> str:
    ctx = client._ctx  # pyright: ignore[reportPrivateUsage]
    wallet = str(client.wallet)
    collateral = str(ctx.environment_config.collateral_token)
    ctf = str(ctx.environment_config.conditional_tokens)
    up = market.up_token_id
    down = market.down_token_id

    if market.neg_risk:
        print(
            "SKIP      BTC 5m unexpectedly marked neg-risk; this tiny tester "
            "only handles standard binary markets"
        )
        return "no-trade"

    initial_pos = await _positions(ctf, wallet, up, down)
    if initial_pos != (Decimal(0), Decimal(0)):
        raise RuntimeError(
            f"refusing test: pre-existing position on {market.slug}: {initial_pos}"
        )

    initial_cash = await _pusd_balance(collateral, wallet)
    print(f"CASH      ${initial_cash:.6f}")

    first_side: str | None = None
    first_token = ""
    first_after = initial_pos
    first_cash_after = initial_cash

    while market.seconds_to_end > ENTRY_STOP_S:
        up_book, down_book = await _books(client, up, down)
        up_ask, down_ask = _best_ask(up_book), _best_ask(down_book)
        if up_ask and down_ask:
            candidates = [
                ("UP", up, up_ask, up_book),
                ("DOWN", down, down_ask, down_book),
            ]
            candidates.sort(key=lambda item: item[2][0])
            side, token, (ask, _size), book = candidates[0]
            if ask <= first_max:
                minimum = Decimal(
                    str(getattr(book, "min_order_size", None) or test_shares)
                )
                shares = max(test_shares, minimum)
                tick = Decimal(str(getattr(book, "tick_size", None) or "0.01"))
                max_price = min(Decimal("0.99"), ask + tick)
                amount = max_price * shares
                if amount > max_total_spend:
                    print(
                        f"SKIP      first-leg minimum would cost ${amount:.4f} "
                        f"> cap ${max_total_spend}"
                    )
                    return "no-trade"

                first_side, first_token = side, token
                print(
                    f"TRIGGER   first={side} ask={ask} shares_target={shares} "
                    f"T-{market.seconds_to_end:.1f}s"
                )
                cash_before = await _pusd_balance(collateral, wallet)
                await _buy(
                    client,
                    token_id=token,
                    amount=amount,
                    max_price=max_price,
                    label="LEG1",
                )
                first_after = await _wait_position_change(
                    ctf,
                    wallet,
                    up,
                    down,
                    initial_pos,
                )
                first_cash_after = await _wait_cash_below(
                    collateral,
                    wallet,
                    cash_before,
                )
                break
        await asyncio.sleep(POLL_S)

    if first_side is None:
        print("NO TRADE  no first-leg ask <= threshold before entry stop")
        return "no-trade"

    first_idx = 0 if first_side == "UP" else 1
    first_shares = first_after[first_idx] - initial_pos[first_idx]
    first_spend = initial_cash - first_cash_after
    if first_shares <= 0 or first_spend <= 0:
        print(
            f"LEG1 FAIL position={first_after} spend={first_spend}; stopping"
        )
        return "exposure-unknown"

    first_unit = first_spend / first_shares
    other_side = "DOWN" if first_side == "UP" else "UP"
    other_token = down if first_side == "UP" else up
    other_idx = 1 - first_idx
    profitable_max = pair_max - first_unit
    print(
        f"LEG1 FILL {first_side} shares={first_shares:.6f} "
        f"spend=${first_spend:.6f} all-in/unit=${first_unit:.6f}; "
        f"{other_side} target <= ${profitable_max:.6f}"
    )

    while market.seconds_to_end > 1.5:
        up_book, down_book = await _books(client, up, down)
        other_book = down_book if other_side == "DOWN" else up_book
        ask_info = _best_ask(other_book)
        if not ask_info:
            await asyncio.sleep(POLL_S)
            continue

        ask, _ = ask_info
        current = await _positions(ctf, wallet, up, down)
        deficit = current[first_idx] - current[other_idx]
        if deficit <= Decimal("0.000001"):
            break

        tick = Decimal(str(getattr(other_book, "tick_size", None) or "0.01"))
        max_price = min(Decimal("0.99"), ask + tick)
        combined = first_unit + max_price

        profitable = combined <= pair_max
        emergency = (
            market.seconds_to_end <= EMERGENCY_S
            and combined <= emergency_pair_max
        )

        if profitable or emergency:
            cash_now = await _pusd_balance(collateral, wallet)
            spent = initial_cash - cash_now
            budget = max_total_spend - spent
            amount = min(max_price * deficit, budget)
            if amount <= Decimal("0.01"):
                break

            reason = "PAIR" if profitable else "EMERGENCY-PAIR"
            print(
                f"{reason}   {other_side} ask={ask} max={max_price} "
                f"combined≈{combined:.6f} deficit={deficit:.6f}"
            )
            before = current
            await _buy(
                client,
                token_id=other_token,
                amount=amount,
                max_price=max_price,
                label="LEG2",
            )
            await _wait_position_change(ctf, wallet, up, down, before)
            await asyncio.sleep(0.35)
            continue

        if market.seconds_to_end <= EMERGENCY_S:
            # No acceptable complete-set hedge. Flatten the unmatched first leg
            # rather than carry directional exposure through settlement.
            first_book = up_book if first_side == "UP" else down_book
            bid_info = _best_bid(first_book)
            current = await _positions(ctf, wallet, up, down)
            excess = current[first_idx] - current[other_idx]
            if bid_info and excess > Decimal("0.000001"):
                bid, _ = bid_info
                first_tick = Decimal(
                    str(getattr(first_book, "tick_size", None) or "0.01")
                )
                min_price = max(Decimal("0.001"), bid - first_tick)
                print(
                    f"FLATTEN   no pair <= {emergency_pair_max}; "
                    f"selling excess {first_side} at min {min_price}"
                )
                await _sell(
                    client,
                    token_id=first_token,
                    shares=excess,
                    min_price=min_price,
                    label="EXIT",
                )
                await asyncio.sleep(1)
                final_cash = await _pusd_balance(collateral, wallet)
                print(f"FLAT PNL  ${final_cash - initial_cash:+.6f}")
                return "flattened"
            break

        await asyncio.sleep(POLL_S)

    final_pos = await _positions(ctf, wallet, up, down)
    pair_shares = min(final_pos)
    cash_premerge = await _pusd_balance(collateral, wallet)
    total_spend = initial_cash - cash_premerge
    locked_floor = pair_shares - total_spend

    if pair_shares <= Decimal("0.000001"):
        print(f"STOP      no matched pair; positions={final_pos}")
        return "exposure-unknown"

    print(
        f"LOCKED    UP={final_pos[0]:.6f} DOWN={final_pos[1]:.6f} "
        f"spent=${total_spend:.6f} gross_floor=${locked_floor:+.6f}"
    )

    tx_hash = await _merge_via_metamask(
        client,
        condition_id=market.condition_id,
        pair_shares=pair_shares,
    )

    await asyncio.sleep(2)
    await _best_effort_flatten_residual(client, market, initial_cash)
    cash_after = await _pusd_balance(collateral, wallet)
    pos_after = await _positions(ctf, wallet, up, down)
    print(f"RESULT    merge_tx={tx_hash}")
    print(
        f"RESULT    pUSD ${initial_cash:.6f} -> ${cash_after:.6f} "
        f"| net=${cash_after - initial_cash:+.6f}"
    )
    print(
        f"RESULT    residual UP={pos_after[0]:.6f} "
        f"DOWN={pos_after[1]:.6f}"
    )
    return "merged"


async def amain(args: argparse.Namespace) -> int:
    private_key = _private_key()
    wallet = to_checksum_address(args.wallet)

    # AsyncSecureClient derives the CLOB API credentials from the owner key and
    # classifies the supplied wallet as the current Deposit Wallet. No relayer API
    # key is needed for CLOB orders in this tester.
    client = await AsyncSecureClient.create(
        private_key=private_key,
        wallet=wallet,
    )

    try:
        print(f"OWNER     {client.signer}")
        print(f"WALLET    {client.wallet} ({client.wallet_type})")
        if str(client.wallet).lower() != wallet.lower():
            raise RuntimeError(
                f"SDK bound {client.wallet}, expected {wallet}"
            )
        if "DEPOSIT" not in str(client.wallet_type).upper():
            raise RuntimeError(
                f"expected Deposit Wallet, got {client.wallet_type}"
            )

        balance = await _pusd_balance(
            str(client._ctx.environment_config.collateral_token),  # pyright: ignore[reportPrivateUsage]
            wallet,
        )
        if balance < args.max_total_spend:
            raise RuntimeError(
                f"pUSD ${balance:.4f} is below configured tiny-test cap "
                f"${args.max_total_spend}"
            )

        await _ensure_standard_trade_approvals(
            client,
            max_total_spend=args.max_total_spend,
        )

        now = time.time()
        start = window_start_epoch(300, now) + 300

        for index in range(args.max_windows):
            target = start + index * 300
            wait = target - time.time()
            if wait > 0:
                print(
                    f"WAIT      window {index + 1}/{args.max_windows} "
                    f"starts in {wait:.1f}s"
                )
                await asyncio.sleep(wait)

            market = await _resolve_fresh_market(client, target)
            if market is None:
                print(f"SKIP      market unavailable for start={target}")
                continue

            print(f"MARKET    {market.slug}")
            print(f"CONDITION {market.condition_id}")
            print(f"UP        {market.up_token_id}")
            print(f"DOWN      {market.down_token_id}")

            result = await _trade_window(
                client,
                market,
                first_max=args.first_max,
                pair_max=args.pair_max,
                emergency_pair_max=args.emergency_pair_max,
                max_total_spend=args.max_total_spend,
                test_shares=args.test_shares,
            )
            if result == "merged":
                return 0
            if result == "exposure-unknown":
                return 3
            if result == "flattened":
                print(
                    "RETRY     first leg was flattened; moving to the next "
                    "fresh 5m window"
                )

        print(
            "DONE      no qualifying complete-set test within configured windows"
        )
        return 2
    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automated tiny MetaMask Predictions BTC 5m live test"
    )
    parser.add_argument(
        "--wallet",
        required=True,
        help="resolved MetaMask Predictions deposit wallet",
    )
    parser.add_argument(
        "--first-max",
        type=Decimal,
        default=DEFAULT_FIRST_MAX,
    )
    parser.add_argument(
        "--pair-max",
        type=Decimal,
        default=DEFAULT_PAIR_MAX,
    )
    parser.add_argument(
        "--emergency-pair-max",
        type=Decimal,
        default=DEFAULT_EMERGENCY_PAIR_MAX,
    )
    parser.add_argument(
        "--max-total-spend",
        type=Decimal,
        default=DEFAULT_MAX_TOTAL_SPEND,
    )
    parser.add_argument(
        "--test-shares",
        type=Decimal,
        default=DEFAULT_TEST_SHARES,
    )
    parser.add_argument(
        "--max-windows",
        type=int,
        default=DEFAULT_MAX_WINDOWS,
    )
    args = parser.parse_args()

    if not (Decimal("0") < args.first_max < Decimal("1")):
        parser.error("--first-max must be between 0 and 1")
    if not (args.first_max < args.pair_max <= Decimal("1")):
        parser.error("--pair-max must be above first-max and <= 1")
    if not (
        args.pair_max
        <= args.emergency_pair_max
        <= Decimal("1.05")
    ):
        parser.error(
            "--emergency-pair-max must be between pair-max and 1.05"
        )
    if not (
        Decimal("0")
        < args.max_total_spend
        <= Decimal("10")
    ):
        parser.error("--max-total-spend must be > 0 and <= $10")
    if not (Decimal("0") < args.test_shares <= Decimal("10")):
        parser.error("--test-shares must be > 0 and <= 10")
    if not (1 <= args.max_windows <= 24):
        parser.error("--max-windows must be between 1 and 24")
    return args


def main() -> None:
    args = parse_args()
    try:
        rc = asyncio.run(amain(args))
    except KeyboardInterrupt:
        rc = 130
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL     {type(exc).__name__}: {exc}")
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
