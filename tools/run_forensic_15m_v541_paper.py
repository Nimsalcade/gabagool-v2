"""Read-only V5.4.1 Gabagool 15m paper harness.

V5.4.1 is a state-machine correction on top of V5.4. It keeps the exact same
hazard models, parent sizing, stack planner, pair-margin safety guard and public
tape fill model, but fixes two free-running conversion errors observed in the
first V5.4 live paper window:

1. An onset hazard may be sampled at most once per planner replenishment batch.
   Additional same-batch vacancies post passively unless an already-started
   aggressive episode continues through its continuation hazard.
2. Once a continuation hazard stops an aggressive episode, onset is blocked
   until an actual passive maker execution intervenes. Merely posting a passive
   parent does not start a new episode.

This remains an experimental maximum-identifiable paper approximation, not a
claim to recovered private Gabagool routing logic.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v54_paper as v54


class V541Engine(v54.V54Engine):
    """V5.4 engine with corrected onset batching and episode re-arm semantics."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v541_batch_onset_consumed = False

    def _onset_p(self, snap: dict[str, Any]) -> float:
        # This method is reached only when V5.4 actually has a causal executable
        # snapshot and is about to evaluate an onset hazard. Mark that single
        # draw as consumed for the current planner batch.
        self._v541_batch_onset_consumed = True
        return super()._onset_p(snap)

    def _route_or_post(
        self,
        side: str,
        px: float,
        now: float,
        up_book: Any,
        down_book: Any,
    ) -> None:
        # V5.4 incorrectly re-sampled onset once per same-batch stack vacancy.
        # After the first real onset evaluation, later vacancies in the same
        # replenishment batch are passive unless an AGGRESSIVE episode is
        # already active (in which case the V5.4 continuation path still runs).
        if self._v54_state == "PASSIVE" and self._v541_batch_onset_consumed:
            return base.Engine.post(
                self, side, px, now, up_book=up_book, down_book=down_book
            )
        return super()._route_or_post(side, px, now, up_book, down_book)

    def _apply_plan(
        self,
        now: float,
        side: str,
        plan: Any,
        current_base: float | None,
        slots: tuple[int, ...],
        up_book: Any,
        down_book: Any,
        *,
        replenish: bool,
    ) -> None:
        # One independent onset opportunity per planner replenishment batch.
        self._v541_batch_onset_consumed = False
        try:
            return super()._apply_plan(
                now,
                side,
                plan,
                current_base,
                slots,
                up_book,
                down_book,
                replenish=replenish,
            )
        finally:
            self._v541_batch_onset_consumed = False

    def _end_episode(self, now: float, reason: str) -> None:
        # Let V5.4 emit/reset the episode first, then alter only the re-arm state.
        super()._end_episode(now, reason)

        block = (
            reason.startswith("continuation hazard stopped")
            or reason.startswith("observed-max paper guard")
        )
        if block:
            self._v54_state = "BLOCKED"
            self.emit(
                now,
                "AGGRESSIVE_ONSET_BLOCK",
                reason=(
                    "V5.4.1 continuation ended; onset blocked until a real "
                    "passive maker execution intervenes"
                ),
            )

    def fill(
        self,
        now: float,
        side: str,
        shares: float,
        price: float,
        kind: str,
        reason: str,
        order: Any = None,
    ) -> bool:
        ok = super().fill(now, side, shares, price, kind, reason, order)
        if ok and kind == "MAKER_FILL" and self._v54_state == "BLOCKED":
            self._v54_state = "PASSIVE"
            self.emit(
                now,
                "AGGRESSIVE_ONSET_UNBLOCK",
                side=side,
                qty=shares,
                price=price,
                reason=(
                    "V5.4.1 passive maker execution intervened; onset re-armed"
                ),
            )
        return ok


def parse_args() -> argparse.Namespace:
    # Keep every V5.4 CLI/default unchanged so V5.4 vs V5.4.1 is an A/B of the
    # state-machine correction only.
    return v54.parse_args()


async def _run(args: argparse.Namespace) -> int:
    base.Engine = V541Engine
    base.apply_sell_print_to_orders = v54.apply_sell_print_to_multi_orders
    original_choose = base.choose_market

    async def choose_clean(client: Any, asset: str, *, clean_start: bool) -> Any:
        return await original_choose(
            client, asset, clean_start=(not args.join_current)
        )

    base.choose_market = choose_clean

    async def no_blocking_resolution(_slug: str, _timeout_s: float) -> None:
        return None

    base.gamma_winner = no_blocking_resolution
    return await base.amain(args)


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
