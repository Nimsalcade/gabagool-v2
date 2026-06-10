"""
Operational guards: per-market backoff and the CLOB heartbeat sidecar.

BACKOFF — the 16,500-error lesson
---------------------------------
The old bot retried `425 service not ready` in a hot loop for two straight
hours. Policy here: every failure category maps to an explicit, capped,
exponential per-market backoff, and MARKET_NOT_READY additionally flips the
market to "not yet quotable" so the loop waits instead of spamming.

HEARTBEAT — the V2 dead-man's switch
------------------------------------
CLOB V2 cancels ALL open orders if a heartbeat session lapses (>10s, 5s
grace). For a maker bot this is a feature: we send a beat every 5s, and if
this process dies, the exchange sweeps our resting orders for us — no orphan
bids left to be picked off. The unified SDK doesn't expose the endpoint yet,
so the sidecar drives `py-clob-client-v2` with the same key and the L2
credentials the SDK already derived.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field

from .constants import CHAIN_ID, CLOB_HOST, HEARTBEAT_INTERVAL_S

log = logging.getLogger("ops")


# ============================================================ backoff policy
@dataclass
class Backoff:
    base_s: float = 2.0
    cap_s: float = 60.0
    _until: dict[str, float] = field(default_factory=dict)
    _streak: dict[str, int] = field(default_factory=dict)

    def ready(self, key: str) -> bool:
        return time.time() >= self._until.get(key, 0.0)

    def seconds_left(self, key: str) -> float:
        return max(0.0, self._until.get(key, 0.0) - time.time())

    def failure(self, key: str, *, factor: float = 1.0) -> float:
        n = self._streak.get(key, 0) + 1
        self._streak[key] = n
        delay = min(self.cap_s, self.base_s * factor * (2 ** (n - 1)))
        delay *= 0.8 + 0.4 * random.random()  # jitter to de-sync loops
        self._until[key] = time.time() + delay
        return delay

    def success(self, key: str) -> None:
        self._streak.pop(key, None)
        self._until.pop(key, None)


def classify_order_error(message: str) -> tuple[str, float]:
    """(category, backoff_factor) for an order placement error string."""
    m = (message or "").upper()
    if "MARKET_NOT_READY" in m or "SERVICE NOT READY" in m or "425" in m:
        return ("not_ready", 3.0)        # book isn't open yet — wait, don't spam
    if "POST_ONLY" in m or "POST-ONLY" in m:
        return ("post_only_mode", 2.0)   # engine restart window — pause quoting
    if "MIN_SIZE" in m or "MIN_TICK" in m:
        return ("constraint", 1.0)       # our bug — log loudly, requote next cycle
    if "NOT_ENOUGH_BALANCE" in m or "INSUFFICIENT" in m:
        return ("balance", 2.0)
    if "RATE" in m and "LIMIT" in m or "429" in m:
        return ("rate_limit", 4.0)
    return ("other", 1.0)


# ============================================================ heartbeat task
class Heartbeat:
    """Async wrapper around py-clob-client-v2's post_heartbeat()."""

    def __init__(self, private_key: str, creds, funder: str, signature_type: int | None):
        self._private_key = private_key
        self._creds = creds        # polymarket SDK ApiKeyCreds (key/secret/passphrase)
        self._funder = funder
        self._sig_type = signature_type
        self._task: asyncio.Task | None = None
        self._client = None

    def _build_sync_client(self):
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds

        creds = ApiCreds(
            api_key=str(getattr(self._creds, "api_key", "") or getattr(self._creds, "key", "")),
            api_secret=str(getattr(self._creds, "api_secret", "") or getattr(self._creds, "secret", "")),
            api_passphrase=str(
                getattr(self._creds, "api_passphrase", "") or getattr(self._creds, "passphrase", "")
            ),
        )
        return ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=self._private_key,
            creds=creds,
            funder=self._funder,
            signature_type=self._sig_type,
        )

    async def start(self) -> None:
        if self._task is not None:
            return
        try:
            self._client = await asyncio.to_thread(self._build_sync_client)
        except Exception as exc:  # noqa: BLE001 — heartbeat is best-effort safety
            log.warning("heartbeat disabled (client build failed: %s)", exc)
            return
        self._task = asyncio.create_task(self._loop(), name="heartbeat")
        log.info("heartbeat started (every %.0fs — orders auto-cancel if we die)",
                 HEARTBEAT_INTERVAL_S)

    async def _loop(self) -> None:
        heartbeat_id = ""
        while True:
            try:
                resp = await asyncio.to_thread(self._client.post_heartbeat, heartbeat_id)
                heartbeat_id = (resp or {}).get("heartbeat_id", heartbeat_id) or heartbeat_id
            except Exception as exc:  # noqa: BLE001
                # A 400 returns the correct id in-band per docs; generic
                # failures just try again next tick — never crash the bot.
                log.debug("heartbeat tick failed: %s", exc)
                heartbeat_id = ""
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
