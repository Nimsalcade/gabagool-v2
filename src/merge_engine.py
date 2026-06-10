"""
Merge engine — the heart of the strategy, rebuilt on the documented V2 path.

WHAT A CORRECT MERGE LOOKS LIKE ON POLYMARKET TODAY
---------------------------------------------------
1. Amount comes from the CHAIN: pairs = min(up_held, down_held) per the
   Data-API positions for the funding wallet. (We pass `amount="max"` and let
   the SDK compute the same number from live balances — and we cross-check.)
2. Execution happens FROM THE FUNDING WALLET (deposit wallet / Safe / proxy),
   not from the EOA. The unified SDK submits the wallet transaction through
   Polymarket's relayer (gasless) using your Relayer API key.
3. Routing is pUSD-native: standard markets go through the
   CtfCollateralAdapter, neg-risk markets through the NegRiskCtfCollateral-
   Adapter. The SDK picks the route; we only assert our 15-minute markets are
   the standard kind.
4. Success is VERIFIED on-chain: positions for the condition must shrink by
   the merged amount. A tx hash is recorded either way.

Every numbered point above is the negation of a specific failure in the
old codebase (EOA-sent merges, NegRiskAdapter-for-everything, USDC.e
collateral, fictional 'Merged: $X' credits with merge_count=0).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from polymarket import PolymarketError, RateLimitError, TimeoutError as PmTimeoutError
from polymarket import TransactionFailedError

from .inventory import fetch_holding
from .quoting import round_pairs_to_micro

log = logging.getLogger("merge_engine")


@dataclass
class MergeResult:
    success: bool
    pairs: float = 0.0           # pairs merged == pUSD returned
    tx_hash: str | None = None
    error: str | None = None
    retry_in_s: float = 0.0      # caller hint; 0 = no retry needed


class MergeEngine:
    """One instance per process; condition_id is passed per call."""

    def __init__(self, client, *, dry_run: bool, min_pairs: float, ledger=None):
        self.client = client
        self.dry_run = dry_run
        self.min_pairs = float(min_pairs)
        self.ledger = ledger
        self.total_merged_usd = 0.0
        self.merge_count = 0
        self._last_attempt: dict[str, float] = {}

    # ------------------------------------------------------------------ API
    async def merge_condition(
        self,
        condition_id: str,
        *,
        force: bool = False,
        interval_s: float = 20.0,
    ) -> MergeResult:
        """Merge all currently-held matched pairs for one condition."""
        now = time.time()
        if not force and now - self._last_attempt.get(condition_id, 0.0) < interval_s:
            return MergeResult(False, error="cooldown")
        self._last_attempt[condition_id] = now

        # 1) Chain truth decides the amount.
        holding = await fetch_holding(self.client, condition_id)
        pairs = holding.pairs
        micro = round_pairs_to_micro(holding.up_shares, holding.down_shares)
        if micro <= 0:
            return MergeResult(False, error="no matched pairs on-chain")
        if pairs < self.min_pairs and not force:
            return MergeResult(
                False, error=f"below threshold ({pairs:.2f} < {self.min_pairs})"
            )

        if self.dry_run:
            log.info("[dry-run] would merge %.2f pairs on %s", pairs, condition_id[:12])
            self.total_merged_usd += pairs
            self.merge_count += 1
            return MergeResult(True, pairs=pairs, tx_hash="dry-run")

        # 2) Execute gaslessly from the funding wallet. amount='max' lets the
        #    SDK size from live balances — immune to any drift between our
        #    positions snapshot and the chain at execution time.
        try:
            handle = await self.client.merge_positions(
                condition_id=condition_id, amount="max"
            )
            outcome = await handle.wait()
            tx = str(
                getattr(outcome, "transaction_hash", None)
                or getattr(handle, "transaction_hash", None)
                or ""
            ) or None
        except TransactionFailedError as exc:
            return self._fail(condition_id, f"relayer tx failed onchain: {exc}", 30)
        except RateLimitError as exc:
            return self._fail(condition_id, f"relayer rate-limited: {exc}", 60)
        except PmTimeoutError as exc:
            # Submitted but unconfirmed: verification below decides truth.
            log.warning("merge confirmation timed out (%s); verifying on-chain", exc)
            tx = None
        except PolymarketError as exc:
            return self._fail(condition_id, _classify(exc), 30)
        except Exception as exc:  # noqa: BLE001
            return self._fail(condition_id, f"unexpected: {exc!r}", 30)

        # 3) Verify against the chain. Positions index within a few seconds.
        merged = await self._verify_shrunk(condition_id, before_pairs=pairs)
        if merged <= 0:
            return self._fail(
                condition_id,
                "merge submitted but positions did not shrink "
                f"(tx={tx}); investigate before retrying blindly",
                60,
            )

        self.total_merged_usd += merged
        self.merge_count += 1
        log.info(
            "MERGED %.2f pairs -> $%.2f pUSD | %s | tx=%s",
            merged, merged, condition_id[:12], tx,
        )
        if self.ledger is not None:
            self.ledger.record_merge(condition_id, merged, tx)
        return MergeResult(True, pairs=merged, tx_hash=tx)

    # ------------------------------------------------------------- internals
    async def _verify_shrunk(self, condition_id: str, *, before_pairs: float) -> float:
        """Poll positions until the matched-pair count drops; return pairs
        actually merged (0 on no observable change within the window)."""
        for delay in (2, 3, 5, 8, 12):
            await asyncio.sleep(delay)
            after = await fetch_holding(self.client, condition_id)
            shrunk = before_pairs - after.pairs
            if shrunk > 1e-6:
                return shrunk
        return 0.0

    def _fail(self, condition_id: str, msg: str, retry_s: float) -> MergeResult:
        log.error("MERGE FAILED %s | %s", condition_id[:12], msg)
        if self.ledger is not None:
            self.ledger.record_merge_failure(condition_id, msg)
        return MergeResult(False, error=msg, retry_in_s=retry_s)

    def session_summary(self) -> str:
        return (
            f"MERGE SESSION | merged=${self.total_merged_usd:.2f} "
            f"across {self.merge_count} merges"
        )


def _classify(exc: Exception) -> str:
    """Turn SDK/relayer errors into operator-actionable messages."""
    text = str(exc)
    low = text.lower()
    if "401" in text or "unauthorized" in low or "api key" in low:
        return (
            "relayer auth rejected — verify POLY_RELAYER_API_KEY and "
            "POLY_RELAYER_API_KEY_ADDRESS (Settings → API Keys on polymarket.com). "
            f"Raw: {text}"
        )
    if "allowance" in low or "approval" in low:
        return (
            "missing approval from the funding wallet — run "
            "`python -m tools.check_setup` to re-apply setup_trading_approvals(). "
            f"Raw: {text}"
        )
    if "nonce" in low:
        return f"relayer nonce conflict (another tx in flight?) — safe to retry. Raw: {text}"
    if "balance" in low or "insufficient" in low:
        return (
            "wallet holds fewer tokens than requested — positions changed "
            f"mid-flight; will recompute from chain on retry. Raw: {text}"
        )
    return text
