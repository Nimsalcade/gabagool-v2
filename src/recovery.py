"""Production startup recovery for the live Gabagool engine.

A process restart must not pretend the wallet is empty.  This module:
- cancels untracked resting orders before the strategy resumes;
- scans the wallet's real open positions;
- resolves exact market end-times from each position slug when possible;
- returns live conditions as seed inventory so MakerLoop resumes from real state;
- returns closed/resolved conditions for the post-close settlement queue;
- computes currently committed acquisition cost for capital accounting.

No strategy decisions are made here; this is state reconciliation only.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

log = logging.getLogger("recovery")


@dataclass
class SeedInventory:
    condition_id: str
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    slug: str | None = None
    window_end: float | None = None
    redeemable: bool = False
    mergeable: bool = False

    @property
    def total_cost(self) -> float:
        return self.up_cost + self.down_cost

    @property
    def pairs(self) -> float:
        return min(self.up_shares, self.down_shares)

    @property
    def has_inventory(self) -> bool:
        return self.up_shares > 1e-9 or self.down_shares > 1e-9


@dataclass
class RecoveryState:
    active: dict[str, SeedInventory] = field(default_factory=dict)
    settlement_due: dict[str, SeedInventory] = field(default_factory=dict)
    canceled_orders: int = 0
    position_rows: int = 0

    @property
    def committed_cost(self) -> float:
        return sum(x.total_cost for x in self.active.values()) + sum(
            x.total_cost for x in self.settlement_due.values()
        )


def _is_up(pos) -> bool:
    outcome = str(getattr(pos, "outcome", "") or "").strip().lower()
    idx = getattr(pos, "outcome_index", None)
    return outcome == "up" or (outcome not in ("up", "down") and idx == 0)


async def _resolve_window_end(client, slug: str | None) -> float | None:
    if not slug:
        return None
    try:
        market = await client.get_market(slug=slug)
        state = getattr(market, "state", None)
        end = getattr(state, "end_date", None)
        if end is not None:
            return float(end.timestamp())
    except Exception as exc:  # noqa: BLE001
        log.debug("could not resolve end time for %s: %s", slug, exc)
    return None


async def cancel_untracked_orders(client) -> int:
    """Cancel every pre-existing order before this process begins tracking orders."""
    try:
        before = await client.list_open_orders().first_page()
        n = len(before.items)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"cannot inspect startup open orders: {exc}") from exc

    if n == 0:
        return 0

    log.warning("startup found %d untracked open orders; cancelling all", n)
    try:
        await client.cancel_all()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"startup cancel_all failed: {exc}") from exc

    # Require observable zero-open-order state before continuing.
    for _ in range(12):
        await asyncio.sleep(0.25)
        try:
            page = await client.list_open_orders().first_page()
        except Exception:
            continue
        if not page.items:
            return n
    raise RuntimeError("startup cancel_all returned but open orders remain visible")


async def recover_wallet_state(client) -> RecoveryState:
    out = RecoveryState()
    out.canceled_orders = await cancel_untracked_orders(client)

    grouped: dict[str, SeedInventory] = {}
    try:
        paginator = client.list_positions(size_threshold=0.0, page_size=500)
        async for page in paginator:
            for pos in page.items:
                size = float(getattr(pos, "size", 0) or 0)
                if size <= 1e-9:
                    continue
                cid = str(getattr(pos, "condition_id", "") or "")
                if not cid:
                    continue
                out.position_rows += 1
                seed = grouped.setdefault(cid, SeedInventory(condition_id=cid))
                seed.slug = str(getattr(pos, "slug", "") or "") or seed.slug
                seed.redeemable = seed.redeemable or bool(getattr(pos, "redeemable", False))
                seed.mergeable = seed.mergeable or bool(getattr(pos, "mergeable", False))
                avg = float(getattr(pos, "avg_price", 0) or 0)
                cost = max(0.0, size * avg)
                if _is_up(pos):
                    seed.up_shares += size
                    seed.up_cost += cost
                else:
                    seed.down_shares += size
                    seed.down_cost += cost
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"cannot recover wallet positions: {exc}") from exc

    if not grouped:
        log.info("startup recovery: no open positions")
        return out

    # Resolve market end-times concurrently; do not classify an unknown market as
    # active merely because the process cannot resolve metadata.
    items = list(grouped.values())
    ends = await asyncio.gather(
        *(_resolve_window_end(client, x.slug) for x in items), return_exceptions=True
    )
    now = time.time()
    for seed, end in zip(items, ends, strict=True):
        if isinstance(end, Exception):
            seed.window_end = None
        else:
            seed.window_end = end

        closed = seed.redeemable or seed.mergeable
        if seed.window_end is not None:
            closed = closed or now >= seed.window_end
        elif not closed:
            # Unknown timing + unresolved inventory is unsafe to trade through.
            # Keep it in settlement/recovery rather than double-acquiring it.
            closed = True
            log.warning(
                "startup position %s has unknown market end; quarantining from new trading",
                seed.condition_id[:12],
            )

        if closed:
            out.settlement_due[seed.condition_id] = seed
        else:
            out.active[seed.condition_id] = seed

    log.info(
        "startup recovery: rows=%d active_conditions=%d settlement_conditions=%d committed_cost=$%.2f",
        out.position_rows,
        len(out.active),
        len(out.settlement_due),
        out.committed_cost,
    )
    return out
