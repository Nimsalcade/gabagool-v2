"""Chain-verified complete-set MERGE engine."""
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
    pairs: float = 0.0
    tx_hash: str | None = None
    error: str | None = None
    retry_in_s: float = 0.0


class MergeEngine:
    def __init__(self, client, *, dry_run: bool, min_pairs: float, ledger=None):
        self.client = client
        self.dry_run = dry_run
        self.min_pairs = float(min_pairs)
        self.ledger = ledger
        self.total_merged_usd = 0.0
        self.merge_count = 0
        self._last_attempt: dict[str, float] = {}

    async def merge_condition(
        self,
        condition_id: str,
        *,
        force: bool = False,
        interval_s: float = 20.0,
    ) -> MergeResult:
        now = time.time()
        if not force and now - self._last_attempt.get(condition_id, 0.0) < interval_s:
            return MergeResult(False, error="cooldown")
        self._last_attempt[condition_id] = now

        holding = await fetch_holding(self.client, condition_id)
        if not holding.valid:
            return self._fail(condition_id, "inventory read failed; merge not attempted", 15)
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
            log.warning("merge confirmation timed out (%s); verifying holdings", exc)
            tx = None
        except PolymarketError as exc:
            return self._fail(condition_id, _classify(exc), 30)
        except Exception as exc:  # noqa: BLE001
            return self._fail(condition_id, f"unexpected: {exc!r}", 30)

        merged = await self._verify_shrunk(condition_id, before_pairs=pairs)
        if merged <= 0:
            return self._fail(
                condition_id,
                "merge submitted but no valid holdings read proved positions shrank "
                f"(tx={tx}); do not credit/retry blindly",
                60,
            )

        self.total_merged_usd += merged
        self.merge_count += 1
        log.info(
            "MERGED %.6f pairs -> $%.6f pUSD | %s | tx=%s",
            merged, merged, condition_id[:12], tx,
        )
        if self.ledger is not None:
            self.ledger.record_merge(condition_id, merged, tx)
        return MergeResult(True, pairs=merged, tx_hash=tx)

    async def _verify_shrunk(self, condition_id: str, *, before_pairs: float) -> float:
        """Only a successful inventory read may prove a merge."""
        for delay in (2, 3, 5, 8, 12):
            await asyncio.sleep(delay)
            after = await fetch_holding(self.client, condition_id)
            if not after.valid:
                continue
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
    text = str(exc)
    low = text.lower()
    if "401" in text or "unauthorized" in low or "api key" in low:
        return (
            "relayer auth rejected — verify POLY_RELAYER_API_KEY and "
            f"POLY_RELAYER_API_KEY_ADDRESS. Raw: {text}"
        )
    if "allowance" in low or "approval" in low:
        return f"missing funding-wallet approval. Raw: {text}"
    if "nonce" in low:
        return f"relayer nonce conflict; safe to re-check before retry. Raw: {text}"
    if "balance" in low or "insufficient" in low:
        return f"wallet balance changed before merge. Raw: {text}"
    return text
