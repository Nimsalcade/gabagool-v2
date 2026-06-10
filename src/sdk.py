"""
Client factory around the official unified `polymarket-client` SDK.

This module is the ONLY place that constructs SDK clients. It guarantees:

  * The client is bound to the correct funding wallet (deposit wallet / Safe /
    proxy) — `setup_gasless_wallet()` is idempotent and is what binds the
    client, so we always call it.
  * Relayer credentials are attached, because split / merge / redeem all run
    gaslessly through Polymarket's relayer in this codebase.
  * Trading approvals exist before the first order (pUSD -> exchanges,
    ERC-1155 -> exchanges + collateral adapters). `setup_trading_approvals()`
    is also idempotent.

WHY THE OLD MERGE FAILED (for context, so nobody reintroduces it)
-----------------------------------------------------------------
1. poly_merger.py sent CTF.mergePositions() FROM THE EOA, but the outcome
   tokens live in the Polymarket wallet contract. The EOA holds nothing, so
   the burn reverts every time, on every RPC.
2. merge_engine.py routed every merge through the NegRiskAdapter — wrong
   contract for the 15-minute Up/Down markets (neg_risk = False).
3. Both used USDC.e/legacy assumptions after the April 2026 pUSD migration.
4. The relayer client was constructed with builder-credential kwargs that
   don't match the documented interface, and the api-key address field
   contained a corrupted .env string.

The unified SDK's `merge_positions()` fixes all four at once: it executes
from the bound wallet, routes standard vs neg-risk automatically, is
pUSD-native (via the CtfCollateralAdapter), and authenticates the relayer
with a plain Relayer API key.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from polymarket import AsyncSecureClient, RelayerApiKey

from .config import BotConfig, ConfigError

log = logging.getLogger("sdk")


@dataclass
class BoundClient:
    client: AsyncSecureClient
    wallet: str          # the funding wallet actually bound (checksum address)
    wallet_type: str     # SDK-reported wallet type (deposit/safe/proxy/eoa)
    signer: str          # EOA derived from the private key


async def build_secure_client(cfg: BotConfig) -> BoundClient:
    """Create, bind, and verify an authenticated client. Raises on mismatch."""
    api_key = None
    if cfg.relayer_api_key:
        api_key = RelayerApiKey(
            key=cfg.relayer_api_key,
            address=cfg.relayer_api_key_address,
        )

    client = await AsyncSecureClient.create(
        private_key=cfg.private_key,
        wallet=cfg.wallet or None,
        api_key=api_key,
    )

    # Binding step: idempotent. Deploys the deposit wallet if this signer has
    # none, otherwise just returns a client bound to the gasless wallet.
    try:
        bound = await client.setup_gasless_wallet()
        if bound is not client:
            await client.close()
            client = bound
    except Exception as exc:  # noqa: BLE001 - surfaced with context below
        await client.close()
        raise ConfigError(
            f"setup_gasless_wallet() failed: {exc}. "
            "Check POLY_RELAYER_API_KEY / POLY_RELAYER_API_KEY_ADDRESS — the "
            "relayer rejects requests with missing or mismatched key/address."
        ) from exc

    wallet = str(client.wallet)
    wtype = str(getattr(client, "wallet_type", "unknown"))
    signer = str(getattr(client, "signer", ""))

    if cfg.wallet and wallet.lower() != cfg.wallet.lower():
        await client.close()
        raise ConfigError(
            f"Configured POLY_WALLET={cfg.wallet} but the SDK bound {wallet}. "
            "Your private key does not control the configured wallet. Trading "
            "with this mismatch is how funds get stranded — refusing to start."
        )

    log.info("SDK bound | wallet=%s type=%s signer=%s", wallet, wtype, signer)
    return BoundClient(client=client, wallet=wallet, wallet_type=wtype, signer=signer)


async def ensure_trading_approvals(bc: BoundClient) -> None:
    """Idempotently set pUSD + conditional-token approvals from the wallet.

    Raises ConfigError only for fatal failures (wallet binding / core exchange
    approvals). Non-fatal failures (e.g. CtfAutoRedeem operator not yet in the
    relayer allowlist) are logged as warnings and do not block trading.
    """
    log.info("Ensuring trading approvals (idempotent)…")
    try:
        handle = await bc.client.setup_trading_approvals()
        outcome = await handle.wait()
        tx = getattr(outcome, "transaction_hash", None)
        log.info("Trading approvals confirmed%s", f" | tx={tx}" if tx else "")
    except Exception as exc:
        msg = str(exc)
        if "not in the allowed list" in msg:
            log.warning(
                "Non-fatal: relayer blocked an approval operator not yet in its "
                "allowlist (%s). Core trading approvals (exchange, collateral "
                "adapter) are likely already set from the web UI. Split / merge "
                "/ order placement should still work.",
                msg[:200],
            )
            return
        raise
