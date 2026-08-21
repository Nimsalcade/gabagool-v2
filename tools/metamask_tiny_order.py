"""Safe one-shot wrapper for the MetaMask Predictions BTC 5m tiny live test.

This wrapper fixes the current SDK BUY call, resolves the active MetaMask Predictions
Deposit Wallet from the owner EOA, verifies the locally-entered private key belongs to
that owner, fails closed on Polymarket geo restrictions, and applies conservative tiny
limits before delegating to ``metamask_tiny_live_test``.

No private key is printed, written to disk, or accepted as a command-line argument.
"""
from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import httpx
from eth_account import Account
from eth_utils import to_checksum_address

from tools import metamask_predict_wallet as wallet_tool
from tools import metamask_tiny_live_test as live

DEFAULT_OWNER = "0x349aFa6115f4fa35C4dC8B998bce3F6d4C659E1f"
GEO_URL = "https://polymarket.com/api/geoblock"


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


async def _safe_buy(
    client: Any,
    *,
    token_id: str,
    amount: Decimal,
    max_price: Decimal,
    label: str,
) -> Any:
    """Correct current SDK BUY signature; reject silently-partial API failures."""
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

    # Hidden prompt from the existing tester. Verify the key before any authenticated
    # client or order request is created, then keep it only in process memory.
    private_key = live._private_key()  # pyright: ignore[reportPrivateUsage]
    signer = to_checksum_address(Account.from_key(private_key).address)
    if signer.lower() != owner.lower():
        raise RuntimeError(
            f"private key belongs to {signer}, expected MetaMask owner {owner}; refusing live order"
        )

    # The existing tester calls _private_key() inside amain(). Return the already
    # verified in-memory key rather than prompting a second time. Also replace its
    # obsolete BUY helper (which passed the removed max_spend= keyword).
    live._private_key = lambda: private_key  # type: ignore[method-assign]
    live._buy = _safe_buy  # type: ignore[method-assign]

    args = SimpleNamespace(
        wallet=wallet,
        first_max=Decimal("0.25"),
        pair_max=Decimal("0.90"),
        emergency_pair_max=Decimal("0.90"),
        max_total_spend=Decimal("4.75"),
        test_shares=Decimal("5"),
        max_windows=12,
    )

    print("LIMITS    first<=0.25 pair<=0.90 spend<=4.75 target=5sh windows<=12")
    print("MODE      one tiny first-leg attempt per window; no deliberate losing emergency pair")
    return await live.amain(args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one tightly capped MetaMask Predictions BTC 5m tiny live test"
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
