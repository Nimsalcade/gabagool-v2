"""
Capital manager: caps that bind BEFORE an order leaves the process, and a
kill-switch that watches real equity (wallet pUSD), not internal estimates.

The old bot's CapitalManager auto-compounded 100% and let cycle caps float up
to the whole wallet; at 13:38 on May 29 it had $1.56 of $346 left in cash with
everything else deployed. Here:
  * caps are absolute dollars, fixed for the session;
  * nothing compounds unless the operator changes the config;
  * the kill-switch cancels everything and halts on a configured drawdown.
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("capital")


class KillSwitch(RuntimeError):
    """Raised when the session drawdown limit is breached."""


class CapitalManager:
    def __init__(self, cfg, ledger=None):
        self.cfg = cfg                       # CapitalConfig
        self.ledger = ledger
        self.session_spent = 0.0             # gross fill notional this session
        self.session_merged = 0.0            # pUSD returned by merges
        self.start_equity: float | None = None
        self._last_balance: float | None = None
        self._last_balance_ts = 0.0

    # ----------------------------------------------------------- per-window
    def can_commit(self, window_cost: float, window_resting: float, new_notional: float) -> bool:
        """May this window add `new_notional` of resting exposure?"""
        c = self.cfg
        if window_cost + window_resting + new_notional > c.per_window_cap_usd:
            return False
        global_at_work = self.session_spent - self.session_merged
        if global_at_work + new_notional > c.global_exposure_cap_usd:
            log.debug("global exposure cap binding (%.2f at work)", global_at_work)
            return False
        return True

    def on_spend(self, notional: float) -> None:
        self.session_spent += notional

    def on_merge_return(self, pairs: float) -> None:
        self.session_merged += pairs

    # ------------------------------------------------------------- equity KS
    async def check_kill_switch(self, client, *, dry_run: bool) -> None:
        """Snapshot wallet pUSD; raise KillSwitch on configured drawdown.
        Conservative by design: it compares CASH equity and therefore only
        evaluates while exposure is small relative to the wallet — it will
        never 'rescue' a bad day, only stop a catastrophic one."""
        if dry_run:
            return
        now = time.time()
        if now - self._last_balance_ts < 30:
            return
        from .inventory import fetch_pusd_balance  # local import: avoid cycle

        bal = await fetch_pusd_balance(client)
        self._last_balance_ts = now
        if bal != bal:  # NaN — fetch failed; do nothing on missing data
            return
        if self.ledger is not None:
            self.ledger.record_balance(bal)
        if self.start_equity is None:
            self.start_equity = bal
            log.info("session start equity: $%.2f pUSD", bal)
            return
        at_work = max(0.0, self.session_spent - self.session_merged)
        equity_floor = self.start_equity * (1.0 - self.cfg.session_drawdown_kill)
        # Only judge when most capital is back in cash; otherwise a normal
        # deployed window would look like a drawdown.
        if at_work < 0.25 * self.start_equity and bal < equity_floor:
            raise KillSwitch(
                f"equity ${bal:.2f} fell below floor ${equity_floor:.2f} "
                f"(start ${self.start_equity:.2f}, limit "
                f"{self.cfg.session_drawdown_kill:.0%}) — halting session"
            )
        self._last_balance = bal
