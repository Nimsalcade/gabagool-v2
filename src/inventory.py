"""
On-chain inventory truth.

RULE: merge amounts are computed from what the wallet ACTUALLY HOLDS
(Data-API positions, which index the chain), never from the bot's internal
fill ledger. The internal ledger drives quoting decisions; the chain drives
money movements. Keeping these two roles separate is what makes a merge
mathematically unable to 'not match'.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .constants import PUSD

log = logging.getLogger("inventory")


@dataclass(frozen=True)
class ConditionHolding:
    condition_id: str
    up_shares: float
    down_shares: float
    up_token_id: str | None
    down_token_id: str | None
    redeemable: bool
    mergeable_flag: bool      # Data-API's own mergeable signal (belt)
    neg_risk: bool

    @property
    def pairs(self) -> float:  # …and suspenders: our own min() (must agree)
        return min(self.up_shares, self.down_shares)


async def fetch_holding(client, condition_id: str) -> ConditionHolding:
    """Aggregate both outcome legs of one condition from list_positions."""
    up_sh = down_sh = 0.0
    up_tok = down_tok = None
    redeemable = False
    mergeable = False
    neg_risk = False
    try:
        paginator = client.list_positions(market=[condition_id], size_threshold=0.0)
        async for page in paginator:
            for pos in page.items:
                size = float(pos.size or 0)
                outcome = (pos.outcome or "").strip().lower()
                idx = getattr(pos, "outcome_index", None)
                is_up = outcome == "up" or (outcome not in ("up", "down") and idx == 0)
                if is_up:
                    up_sh += size
                    up_tok = str(pos.token_id) if pos.token_id else up_tok
                else:
                    down_sh += size
                    down_tok = str(pos.token_id) if pos.token_id else down_tok
                redeemable = redeemable or bool(getattr(pos, "redeemable", False))
                mergeable = mergeable or bool(getattr(pos, "mergeable", False))
                neg_risk = neg_risk or bool(getattr(pos, "negative_risk", False))
    except Exception as exc:  # noqa: BLE001 — caller treats zeros as "unknown, skip"
        log.warning("positions fetch failed for %s: %s", condition_id[:12], exc)

    return ConditionHolding(
        condition_id=condition_id,
        up_shares=up_sh,
        down_shares=down_sh,
        up_token_id=up_tok,
        down_token_id=down_tok,
        redeemable=redeemable,
        mergeable_flag=mergeable,
        neg_risk=neg_risk,
    )


async def fetch_pusd_balance(client) -> float:
    """Wallet's spendable pUSD (whole dollars). CLOB API first, on-chain fallback.

    New Polymarket Deposit Wallets require signature_type=3 (POLY_1271) for
    the CLOB API to report the correct balance. If the SDK uses the wrong type,
    the CLOB reports $0 even when on-chain funds exist. The fallback queries
    the pUSD ERC-20 contract directly via a public Polygon RPC.
    """
    # 1. CLOB API (primary — handles proxy/safe wallets correctly)
    try:
        ba = await client.get_balance_allowance(asset_type="COLLATERAL")
        bal = float(ba.balance) / 1e6
        if bal > 0:
            return bal
    except Exception as exc:  # noqa: BLE001
        log.warning("CLOB balance fetch failed: %s", exc)

    # 2. On-chain fallback for deposit wallets (signature_type mismatch fix)
    wallet = getattr(client, "wallet", "") or ""
    if wallet:
        onchain = await _fetch_pusd_onchain(wallet)
        if onchain is not None:
            log.info("On-chain pUSD balance: $%.2f", onchain)
            return onchain

    log.warning("Could not determine pUSD balance (CLOB + on-chain both failed)")
    return float("nan")


async def _fetch_pusd_onchain(wallet_address: str) -> float | None:
    """Query pUSD ERC-20 balance directly from Polygon via eth_call."""
    try:
        import httpx
    except ImportError:
        log.warning("httpx not available for on-chain balance fallback")
        return None

    # keccak256("balanceOf(address)")[:4]
    data = "0x70a08231" + wallet_address[2:].lower().rjust(64, "0")
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": PUSD, "data": data}, "latest"],
        "id": 1,
    }
    for rpc_url in ("https://polygon-rpc.com", "https://rpc.ankr.com/polygon"):
        try:
            async with httpx.AsyncClient(timeout=10) as session:
                resp = await session.post(rpc_url, json=payload)
                result = resp.json()
                raw = result.get("result", "")
                if raw:
                    return int(raw, 16) / 1e6
        except Exception:  # noqa: BLE001 — try next RPC
            continue

    return None
