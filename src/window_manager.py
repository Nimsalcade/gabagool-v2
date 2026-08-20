"""Market scheduling and post-close settlement orchestration."""
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
    market: UpDownMarket
    first_seen: float
    last_stale_warning: float = 0.0


class SettlementManager:
    """Sweep closed conditions without ever abandoning unresolved inventory.

    The SDK merge action is per condition, so the implementation cannot reproduce the
    reference wallet's single multi-market transaction shape. It does reproduce the
    evidence-backed timing policy: no live-window merge loop; multiple closed conditions
    become due together and are swept after close.
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

    def enqueue(self, market: UpDownMarket) -> None:
        self._pending[market.condition_id] = _PendingSettlement(market, time.time())

    def contains(self, condition_id: str) -> bool:
        return condition_id in self._pending

    async def sweep(self) -> None:
        now = time.time()
        if now - self._last_sweep < self.cfg.settlement_sweep_interval_s:
            return
        self._last_sweep = now
        due = [
            p for p in self._pending.values()
            if now >= p.market.window_end + self.cfg.merge_after_close_s
        ]
        if not due:
            return
        log.info("settlement sweep: %d closed conditions", len(due))

        for p in due:
            cid = p.market.condition_id
            if self.dry_run:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                continue

            holding = await fetch_holding(self.client, cid)
            if not holding.valid:
                log.warning("settlement read unavailable for %s; keeping pending", cid[:12])
                continue
            if holding.up_shares <= 0 and holding.down_shares <= 0:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                continue

            if holding.pairs > 0:
                before_pairs = holding.pairs
                res = await self.merges.merge_condition(cid, force=True)
                if res.success:
                    post = await fetch_holding(self.client, cid)
                    if not post.valid:
                        log.warning(
                            "merge submitted for %s but post-merge holdings are unavailable; "
                            "keeping pending and NOT crediting capital",
                            cid[:12],
                        )
                        continue
                    returned = max(0.0, before_pairs - post.pairs)
                    if returned <= 1e-9:
                        log.warning(
                            "merge for %s is not yet reflected in holdings; keeping pending and NOT crediting capital",
                            cid[:12],
                        )
                        continue
                    self.capital.on_settlement_return(cid, returned)
                    holding = post

            if holding.up_shares <= 0 and holding.down_shares <= 0:
                self.capital.close_condition(cid)
                self._pending.pop(cid, None)
                continue

            if holding.redeemable:
                try:
                    handle = await self.client.redeem_positions(condition_id=cid)
                    outcome = await handle.wait()
                    tx = str(getattr(outcome, "transaction_hash", "") or "") or None
                    if self.ledger is not None:
                        self.ledger.record_redeem(cid, tx, True)
                    # Transaction acceptance is not the same as zero inventory. A
                    # later sweep must prove holdings are gone before exposure closes.
                    log.info("REDEEM submitted %s tx=%s; awaiting zero-holding proof", cid[:12], tx)
                except Exception as exc:  # noqa: BLE001
                    log.warning("redeem failed for %s: %s", cid[:12], exc)
                    if self.ledger is not None:
                        self.ledger.record_redeem(cid, None, False, exc)
                continue

            # Never drop unresolved exposure just because indexing/resolution is slow.
            if now > p.market.window_end + 3600 and now - p.last_stale_warning > 900:
                p.last_stale_warning = now
                log.warning(
                    "settlement still pending >1h for %s: UP %.6f DOWN %.6f; retaining until proven clear",
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
                await self._launch_new()
                await self.settlement.sweep()
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
            task = asyncio.create_task(loop.run(), name=f"window:{m.slug}")
            self._active[m.condition_id] = task
            log.info("launched %s (%d active)", m.slug, len(self._active))

    async def _reap_finished(self) -> None:
        for condition_id, task in list(self._active.items()):
            if not task.done():
                continue
            self._active.pop(condition_id, None)
            try:
                result = task.result()
                self.settlement.enqueue(result.market)
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                log.error("window task crashed: %s", exc, exc_info=True)

    async def shutdown(self) -> None:
        log.info("shutdown: cancelling %d active windows", len(self._active))
        for task in self._active.values():
            task.cancel()
        if self._active:
            await asyncio.gather(*self._active.values(), return_exceptions=True)
        self._active.clear()
        if not self.cfg.dry_run:
            try:
                await self.client.cancel_all()
                log.info("cancel_all confirmed")
            except Exception as exc:  # noqa: BLE001
                log.error("cancel_all failed: %s", exc)
        log.info(self.settlement.merges.session_summary())
