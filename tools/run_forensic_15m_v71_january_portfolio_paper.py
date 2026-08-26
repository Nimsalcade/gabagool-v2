"""V7.1 runtime wrapper for the January portfolio paper controller.

This keeps V7's continuous two-sided portfolio router and replaces the inherited
8-clip V5 tape safety with the January-calibrated configurable hard gap (default
22 clips).  It also restores the fixed January parent clip before every tape
fill pass so max-gap telemetry uses the same unit as routing.
"""
from __future__ import annotations

import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v7_january_portfolio_paper as v7
from src.v53_execution import apply_sell_print_to_multi_orders


class V71JanuaryPortfolioEngine(v7.V7JanuaryPortfolioEngine):
    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        self._refresh_clip(now)
        return super().expire(now, up_book, down_book)


async def _run(args):
    # Base public-tape fill plumbing historically had an 8-parent emergency gap.
    # January 17 reached ~22.19 median fill clips, so V7 needs a wider safety
    # envelope while still refusing fills that increase an already-excessive gap.
    def january_gap_allows(inv, *, side: str, shares: float, parent_clip: float) -> bool:
        clip = max(float(parent_clip), float(args.v7_parent_clip), 1e-9)
        limit = float(args.v7_hard_gap_clips) * clip
        old_gap = float(inv.signed_gap)
        new_gap = old_gap + (float(shares) if side == "UP" else -float(shares))
        if abs(new_gap) <= limit + 1e-9:
            return True
        # Always allow a deficient-side fill that reduces an already-large gap.
        return abs(new_gap) < abs(old_gap) - 1e-9

    base.hard_gap_allows = january_gap_allows
    base.Result = v7.V7Result
    base.Engine = V71JanuaryPortfolioEngine
    base.apply_sell_print_to_orders = apply_sell_print_to_multi_orders

    original_choose = base.choose_market

    async def choose_clean(client: Any, asset: str, *, clean_start: bool) -> Any:
        return await original_choose(client, asset, clean_start=(not args.join_current))

    base.choose_market = choose_clean

    async def no_blocking_resolution(_slug: str, _timeout_s: float) -> None:
        return None

    base.gamma_winner = no_blocking_resolution
    return await base.amain(args)


def main() -> None:
    raise SystemExit(asyncio.run(_run(v7.parse_args())))


if __name__ == "__main__":
    main()
