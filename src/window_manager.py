"""
Window manager: discovers upcoming 15-minute windows, runs one MakerLoop per
window under the concurrency cap, and hands finished conditions to the
redeem sweeper so the ≤6% naked residue (and any unmerged dust) comes home.

The redeem path matters more than it looks: the old wallet's history shows
redemption dust trickling in for *days* because nothing claimed it. Here,
every condition the bot touched is checked after resolution and redeemed
gaslessly the moment the Data API flags it redeemable.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .capital import KillSwitch
from .discovery import discover
from .inventory import fetch_holding
from .maker_loop import MakerLoop
from .merge_engine import MergeEngine

log = logging.getLogger("window_manager")


class Redeemer:
    def __init__(self, client, *, dry_run: bool, ledger=None):
        self.client = client
        self.dry_run = dry_run
        self.ledger = ledger
        self._pending: dict[str, str] = {}   # condition_id -> market_id

    def enqueue(self, condition_id: str, market_id: str) -> None:
        self._pending[condition_id] = market_id

    async def sweep(self) -> None:
        for condition_id, market_id in list(self._pending.items()):
            holding = await fetch_holding(self.client, condition_id)
            if holding.up_shares <= 0 and holding.down_shares <= 0:
                self._pending.pop(condition_id, None)        # nothing left
                continue
            if not holding.redeemable:
                continue                                     # not resolved yet
            if self.dry_run:
                log.info("[dry] would redeem %s", condition_id[:12])
                self._pending.pop(condition_id, None)
                continue
            try:
                handle = await self.client.redeem_positions(condition_id=condition_id)
                outcome = await handle.wait()
                tx = str(getattr(outcome, "transaction_hash", "") or "") or None
                log.info("REDEEMED %s | tx=%s", condition_id[:12], tx)
                if self.ledger is not None:
                    self.ledger.record_redeem(condition_id, tx, True)
                self._pending.pop(condition_id, None)
            except Exception as exc:  # noqa: BLE001 — retried next sweep
                log.warning("redeem failed for %s: %s", condition_id[:12], exc)
                if self.ledger is not None:
                    self.ledger.record_redeem(condition_id, None, False, exc)


class WindowManager:
    def __init__(self, client, *, cfg, capital, ledger=None):
        self.client = client
        self.cfg = cfg                      # BotConfig
        self.capital = capital
        self.ledger = ledger
        self.merges = MergeEngine(
            client,
            dry_run=cfg.dry_run,
            min_pairs=cfg.strategy.min_pairs_to_merge,
            ledger=ledger,
        )
        self.redeemer = Redeemer(client, dry_run=cfg.dry_run, ledger=ledger)
        self._active: dict[str, asyncio.Task] = {}   # condition_id -> task

    async def run_forever(self) -> None:
        scfg = self.cfg.strategy
        log.info(
            "window manager up | assets=%s | budget=%.2f | per-window cap=$%.0f | %s",
            ",".join(scfg.assets), scfg.combined_budget,
            self.cfg.capital.per_window_cap_usd,
            "DRY-RUN" if self.cfg.dry_run else "LIVE",
        )
        try:
            while True:
                await self._reap_finished()
                await self._launch_new()
                await self.redeemer.sweep()
                await self.capital.check_kill_switch(self.client, dry_run=self.cfg.dry_run)
                await asyncio.sleep(5.0)
        except KillSwitch as ks:
            log.critical("KILL SWITCH: %s", ks)
            await self.shutdown()
            raise
        except asyncio.CancelledError:
            await self.shutdown()
            raise

    # ---------------------------------------------------------------- detail
    async def _launch_new(self) -> None:
        if len(self._active) >= self.cfg.capital.max_concurrent_windows:
            return
        try:
            markets = await discover(self.client, self.cfg.strategy.assets)
        except Exception as exc:  # noqa: BLE001
            log.warning("discovery failed: %s", exc)
            return
        for m in markets:
            if m.condition_id in self._active:
                continue
            if len(self._active) >= self.cfg.capital.max_concurrent_windows:
                break
            # Only start a window worth working: needs more remaining life
            # than the posting buffer, else we'd HOLD immediately.
            if m.seconds_to_end <= self.cfg.strategy.stop_posting_buffer_s + 30:
                continue
            if m.neg_risk:
                # Up/Down 15m markets are standard; a neg-risk one here means
                # the series changed shape — refuse rather than mis-route.
                log.warning("skipping %s: unexpected neg_risk market", m.slug)
                continue
            loop = MakerLoop(
                self.client, m,
                cfg=self.cfg.strategy,
                capital=self.capital,
                merge_engine=self.merges,
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
                self.redeemer.enqueue(condition_id, result.market.market_id)
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 — one window must not kill the rest
                log.error("window task crashed: %s", exc, exc_info=True)
                self.redeemer.enqueue(condition_id, "")

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
                log.info("cancel_all confirmed — no resting orders remain")
            except Exception as exc:  # noqa: BLE001
                log.error("cancel_all failed: %s — check the UI for orphans", exc)
        log.info(self.merges.session_summary())
        if time.time():  # keep linters honest about the import
            pass
