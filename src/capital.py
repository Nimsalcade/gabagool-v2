"""Exposure accounting and kill-switch.

Unlike the prior implementation, global exposure is tracked per condition and can be
released when settlement closes that condition. This prevents cumulative turnover from
being mistaken for live capital at risk.
"""
from __future__ import annotations

import logging
import time

log = logging.getLogger("capital")


class KillSwitch(RuntimeError):
    pass


class CapitalManager:
    def __init__(self, cfg, ledger=None):
        self.cfg = cfg
        self.ledger = ledger
        self._condition_spend: dict[str, float] = {}
        self._condition_returns: dict[str, float] = {}
        self._condition_resting: dict[str, float] = {}
        self.start_equity: float | None = None
        self._last_balance: float | None = None
        self._last_balance_ts = 0.0

    def condition_at_work(self, condition_id: str) -> float:
        return max(
            0.0,
            self._condition_spend.get(condition_id, 0.0)
            - self._condition_returns.get(condition_id, 0.0),
        )

    def global_at_work(self) -> float:
        ids = set(self._condition_spend) | set(self._condition_returns)
        return sum(self.condition_at_work(cid) for cid in ids)

    def update_resting(self, condition_id: str, notional: float) -> None:
        self._condition_resting[condition_id] = max(0.0, float(notional))

    def global_resting(self) -> float:
        return sum(self._condition_resting.values())

    def can_commit(
        self,
        condition_id: str,
        window_cost: float,
        window_resting: float,
        new_notional: float,
    ) -> bool:
        c = self.cfg
        if window_cost + window_resting + new_notional > c.per_window_cap_usd:
            return False
        existing_current = self._condition_resting.get(condition_id, 0.0)
        other_resting = max(0.0, self.global_resting() - existing_current)
        if self.global_at_work() + other_resting + window_resting + new_notional > c.global_exposure_cap_usd:
            log.debug(
                "global exposure cap binding (%.2f filled, %.2f resting)",
                self.global_at_work(), self.global_resting(),
            )
            return False
        return True

    def on_spend(self, condition_id: str, notional: float) -> None:
        self._condition_spend[condition_id] = self._condition_spend.get(condition_id, 0.0) + notional

    def on_settlement_return(self, condition_id: str, amount: float) -> None:
        self._condition_returns[condition_id] = self._condition_returns.get(condition_id, 0.0) + amount

    def close_condition(self, condition_id: str) -> None:
        self._condition_spend.pop(condition_id, None)
        self._condition_returns.pop(condition_id, None)
        self._condition_resting.pop(condition_id, None)

    async def check_kill_switch(self, client, *, dry_run: bool) -> None:
        if dry_run:
            return
        now = time.time()
        if now - self._last_balance_ts < 30:
            return
        from .inventory import fetch_pusd_balance

        bal = await fetch_pusd_balance(client)
        self._last_balance_ts = now
        if bal != bal:
            return
        if self.ledger is not None:
            self.ledger.record_balance(bal)
        if self.start_equity is None:
            self.start_equity = bal
            log.info("session start equity: $%.2f pUSD", bal)
            return
        at_work = self.global_at_work()
        equity_floor = self.start_equity * (1.0 - self.cfg.session_drawdown_kill)
        if at_work < 0.25 * self.start_equity and bal < equity_floor:
            raise KillSwitch(
                f"equity ${bal:.2f} below floor ${equity_floor:.2f} "
                f"(start ${self.start_equity:.2f})"
            )
        self._last_balance = bal
