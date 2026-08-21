"""Market scheduling, restart recovery, and post-close settlement orchestration."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from .capital import KillSwitch
from .discovery import UpDownMarket, discover
from .inventory import fetch_holding
from .maker_loop import MakerLoop
from .merge_engine import MergeEngine

log = logging.getLogger("window_manager")


@dataclass
class _PendingSettlement:
    condition_id: str
    due_at: float
    market: UpDownMarket | None = None
    first_seen: float = 0.0
    last_stale_warning: float = 0.0
    last_merge_attempt: float = 0.0
    last_redeem_attempt: float = 0.0


class SettlementManager:
    """Sweep closed conditions without ever abandoning unresolved inventory.

    The official SDK merge action is per-condition, so the implementation cannot
    reproduce the reference wallet's single multi-market transaction shape. It does
    reproduce the evidence-backed lifecycle: no live-window merge loop, then a queue of
    closed conditions is settled after close. Retries are idempotent and rate-limited.
    """

    def __init__(self, client, *, cfg, capital, ledger=None, dry_run=True):
        self.client = client
        self.cfg = cfg
        self.capital = capital
        self.ledger = ledger
        self.dry_run = dry_run
        self.merges = MergeEngine(
            client,
            dry_run=dry_run,
            min_pairs=cfg.min_pairs_to_merge,
            ledger=ledger,
        )
        self._pending: dict[str, _PendingSettlement] = {}
        self._last_sweep = 0.0

    def enqueue_market(self, market: UpDownMarket) -> None:
        cid = market.condition_id
        existing = self._pending.get(cid)
        due = market.window_end + self.cfg.merge_after_close_s
        if existing is None:
            self._pending[cid] = _PendingSettlement(
                condition_id=cid,
                due_at=due,
                market=market,
                first_seen=time.time(),
            )
        else:
            existing.due_at = min(existing.due_at, due)
            existing.market = market

    def enqueue_condition(self, condition_id: str, *, due_at: float | None = None) -> None:
        if condition_id in self._pending:
            return
        self._pending[condition_id] = _PendingSettlement(
            condition_id=condition_id,
            due_at=time.time() if due_at is None else float(due_at),
            first_seen=time.time(),
        )

    def contains(self, condition_id: str) -> bool:
        return condition_id in self._pending

    async def sweep(self) -> None:
        now = time.time()
        if now - self._last_sweep < self.cfg.settlement_sweep_interval_s:
            return
        self._last_sweep = now
        due = [p for p in self._pending.values() if now >= p.due_at]
        if not due:
            return
        log.info("settlement sweep: %d due conditions", len(due))

        for p in due:
            cid = p.condition_id
            if self.dry_run:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                continue

            holding = await fetch_holding(self.client, cid)
            if not holding.valid:
                log.warning("settlement read unavailable for %s; keeping pending", cid[:12])
                continue
            if holding.up_shares <= 1e-9 and holding.down_shares <= 1e-9:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                log.info("settlement clear %s", cid[:12])
                continue

            if holding.pairs > 1e-9 and now - p.last_merge_attempt >= 30.0:
                p.last_merge_attempt = now
                res = await self.merges.merge_condition(cid, force=True)
                if res.success:
                    # MergeEngine only returns success after a valid holdings read
                    # proves pair inventory shrank, so this amount is safe to credit.
                    self.capital.on_settlement_return(cid, res.pairs)
                    holding = await fetch_holding(self.client, cid)
                    if not holding.valid:
                        log.warning(
                            "merge verified for %s but refreshed holdings unavailable; keeping pending",
                            cid[:12],
                        )
                        continue

            if holding.up_shares <= 1e-9 and holding.down_shares <= 1e-9:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                log.info("settlement clear %s", cid[:12])
                continue

            if holding.redeemable and now - p.last_redeem_attempt >= 30.0:
                p.last_redeem_attempt = now
                try:
                    handle = await self.client.redeem_positions(condition_id=cid)
                    outcome = await handle.wait()
                    tx = str(getattr(outcome, "transaction_hash", "") or "") or None
                    if self.ledger is not None:
                        self.ledger.record_redeem(cid, tx, True)
                    log.info(
                        "REDEEM submitted %s tx=%s; awaiting zero-holding proof",
                        cid[:12], tx,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("redeem failed for %s: %s", cid[:12], exc)
                    if self.ledger is not None:
                        self.ledger.record_redeem(cid, None, False, exc)
                continue

            # Never drop unresolved exposure because resolution/indexing is slow.
            if now - p.first_seen > 3600 and now - p.last_stale_warning > 900:
                p.last_stale_warning = now
                log.warning(
                    "settlement pending >1h for %s: UP %.6f DOWN %.6f; retaining until proven clear",
                    cid[:12], holding.up_shares, holding.down_shares,
                )


class WindowManager:
    def __init__(self, client, *, cfg, capital, ledger=None):
        self.client = client
        self.cfg = cfg
        self.capital = capital
        self.ledger = ledger
        self.settlement = SettlementManager(
            client,
            cfg=cfg.strategy,
            capital=capital,
            ledger=ledger,
            dry_run=cfg.dry_run,
        )
        self._active: dict[str, asyncio.Task] = {}
        self._active_markets: dict[str, UpDownMarket] = {}
        self._seed_inventory: dict[str, object] = {}

    def apply_recovery(self, recovery) -> None:
        """Install wallet state recovered before the scheduler starts."""
        for cid, seed in recovery.active.items():
            self._seed_inventory[cid] = seed
            self.capital.seed_condition(cid, seed.total_cost)
        for cid, seed in recovery.settlement_due.items():
            self.capital.seed_condition(cid, seed.total_cost)
            self.settlement.enqueue_condition(cid, due_at=time.time())
        if recovery.active or recovery.settlement_due:
            log.info(
                "recovery installed | active=%d settlement=%d committed=$%.2f",
                len(recovery.active),
                len(recovery.settlement_due),
                recovery.committed_cost,
            )

    async def run_forever(self) -> None:
        scfg = self.cfg.strategy
        mode = "DRY-RUN/SYNTHETIC-FILLS (PLUMBING ONLY)" if self.cfg.dry_run else "LIVE"
        log.info(
            "window manager | assets=%s durations=%s target_basis=%.3f max_basis=%.3f | %s",
            ",".join(scfg.assets),
            ",".join(str(d) for d in scfg.durations),
            scfg.target_combined_vwap,
            scfg.max_combined_vwap,
            mode,
        )
        try:
            while True:
                await self._reap_finished()
                await self.settlement.sweep()
                await self._launch_new()
                await self.capital.check_kill_switch(
                    self.client, dry_run=self.cfg.dry_run
                )
                await asyncio.sleep(2.0)
        except KillSwitch as ks:
            log.critical("KILL SWITCH: %s", ks)
            await self.shutdown()
            raise
        except asyncio.CancelledError:
            await self.shutdown()
            raise

    def _hydrate_loop(self, loop: MakerLoop, condition_id: str) -> None:
        seed = self._seed_inventory.pop(condition_id, None)
        if seed is None:
            return
        loop.tracker.up.shares = float(seed.up_shares)
        loop.tracker.up.cost = float(seed.up_cost)
        loop.tracker.up.max_price = float(seed.up_cost / seed.up_shares) if seed.up_shares else 0.0
        loop.tracker.down.shares = float(seed.down_shares)
        loop.tracker.down.cost = float(seed.down_cost)
        loop.tracker.down.max_price = (
            float(seed.down_cost / seed.down_shares) if seed.down_shares else 0.0
        )
        # Treat recovered inventory as already-held state. New fill timing begins
        # with this process; the stale timer will naturally become conservative.
        now = time.time()
        if seed.up_shares > 0:
            loop.tracker.up.last_fill_ts = now
        if seed.down_shares > 0:
            loop.tracker.down.last_fill_ts = now
        log.info(
            "resuming %s with recovered UP %.3f@$%.4f DOWN %.3f@$%.4f",
            condition_id[:12],
            seed.up_shares,
            seed.up_cost / seed.up_shares if seed.up_shares else 0.0,
            seed.down_shares,
            seed.down_cost / seed.down_shares if seed.down_shares else 0.0,
        )

    async def _launch_new(self) -> None:
        if len(self._active) >= self.cfg.capital.max_concurrent_windows:
            return
        try:
            markets = await discover(
                self.client,
                self.cfg.strategy.assets,
                self.cfg.strategy.durations,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("discovery failed: %s", exc)
            return

        for m in markets:
            if m.condition_id in self._active or self.settlement.contains(m.condition_id):
                continue
            if len(self._active) >= self.cfg.capital.max_concurrent_windows:
                break
            if m.seconds_to_end <= self.cfg.strategy.stop_posting_buffer_s + 3:
                continue
            loop = MakerLoop(
                self.client,
                m,
                cfg=self.cfg.strategy,
                capital=self.capital,
                ledger=self.ledger,
                dry_run=self.cfg.dry_run,
            )
            self._hydrate_loop(loop, m.condition_id)
            task = asyncio.create_task(loop.run(), name=f"window:{m.slug}")
            self._active[m.condition_id] = task
            self._active_markets[m.condition_id] = m
            log.info("launched %s (%d active)", m.slug, len(self._active))

    async def _reap_finished(self) -> None:
        for condition_id, task in list(self._active.items()):
            if not task.done():
                continue
            self._active.pop(condition_id, None)
            market = self._active_markets.pop(condition_id, None)
            try:
                result = task.result()
                self.settlement.enqueue_market(result.market)
            except asyncio.CancelledError:
                if market is not None and market.seconds_to_end <= 0:
                    self.settlement.enqueue_market(market)
            except Exception as exc:  # noqa: BLE001
                log.error("window task crashed: %s", exc, exc_info=True)
                # A strategy-task exception must never orphan wallet inventory.
                if market is not None:
                    self.settlement.enqueue_market(market)

    async def shutdown(self) -> None:
        log.info("shutdown: cancelling %d active windows", len(self._active))
        for task in self._active.values():
            task.cancel()
        if self._active:
            await asyncio.gather(*self._active.values(), return_exceptions=True)
        self._active.clear()
        self._active_markets.clear()
        if not self.cfg.dry_run:
            try:
                await self.client.cancel_all()
                log.info("cancel_all confirmed")
            except Exception as exc:  # noqa: BLE001
                log.error("cancel_all failed: %s", exc)
        log.info(self.settlement.merges.session_summary())
