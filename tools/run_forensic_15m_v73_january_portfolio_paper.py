"""Read-only V7.3 January portfolio paper harness.

V7.3 keeps V7.2's overweight-side replenishment freeze and fixes the next two
failure modes exposed by live paper sessions:

1. BOOTSTRAP RISK
   A balanced market previously opened four 14-share parents per side. A single
   directional tape burst could therefore consume ~56 shares on one outcome
   before inventory steering reacted. V7.3 starts with one parent per side until
   both outcomes have at least 0.5 parent clips of inventory.

2. MARGINAL REPAIR ECONOMICS
   V7.2 used the normal projected UP_VWAP + DOWN_VWAP <= portfolio-cap rule even
   when one side was already stranded. Under the strict merge-only accounting
   test, that is the wrong marginal decision: once an overweight share is sunk,
   any deficient-side share bought below $1 increases recoverable merge cash
   versus leaving the overweight share unmatched. V7.3 therefore separates
   accumulation economics from repair economics. Outside the neutral band, the
   underweight side quotes one tick below the live ask, capped by an explicit
   repair maximum (default 0.92), while fresh repair exposure is bounded to the
   current inventory gap plus one parent.

The normal <=0.995 portfolio guard still governs balanced accumulation. V7.3 is
maker-only and read-only; it does not load a wallet or submit real orders.
"""
from __future__ import annotations

import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v7_january_portfolio_paper as v7
import tools.run_forensic_15m_v72_january_portfolio_paper as v72
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v7_january_portfolio import stacked_ladder
from src.v73_repair import in_bootstrap, repair_maker_top, repair_parent_count


def _inventory_signed_gap(inv: Any) -> float:
    """Return UP-DOWN shares for the common paper Inventory implementation.

    V7.3 previously referenced ``inv.signed_gap``, but the shared paper Inventory
    exposes ``up_shares`` and ``down_shares`` only. Keeping this calculation here
    avoids coupling the January controller to an attribute that does not exist at
    runtime and is inherited by V7.4.
    """
    return float(inv.up_shares) - float(inv.down_shares)


class V73JanuaryPortfolioEngine(v72.V72JanuaryPortfolioEngine):
    def _desired_side(
        self,
        *,
        now: float,
        side: str,
        book: Any,
        target_count: int,
        targets: Any,
        portfolio_cap: float,
    ) -> tuple[tuple[float, ...], float | None, str]:
        d = self._replenishment()
        bootstrap = in_bootstrap(
            up_shares=self.inv.up_shares,
            down_shares=self.inv.down_shares,
            parent_clip=self.clip,
            two_sided_threshold_clips=float(self.args.v73_bootstrap_two_sided_clips),
        )

        # While truly balanced but not yet seeded on both outcomes, keep opening
        # exposure tiny. This changes the worst immediate one-side burst from
        # four parents (~56 shares) to one parent (~14 shares by default).
        if d.neutral and bootstrap:
            target_count = min(int(target_count), int(self.args.v73_bootstrap_parents))

        # Once outside neutral, only V7.2's underweight side is allowed to create
        # new parents. For that deficient side, use marginal repair economics
        # instead of the normal aggregate portfolio-basis cap.
        if (not d.neutral) and d.underweight == side:
            ask = base._best_ask(book)
            tick = base._tick(book)
            if ask is None:
                return (), None, "v73_repair missing best ask"

            gap_shares = abs(_inventory_signed_gap(self.inv))
            repair_count = repair_parent_count(
                gap_shares=gap_shares,
                parent_clip=self.clip,
                requested_parents=int(target_count),
                overshoot_parents=int(self.args.v73_repair_overshoot_parents),
            )
            top = repair_maker_top(
                best_ask=ask[0],
                tick=tick,
                repair_max_price=float(self.args.v73_repair_max_price),
            )
            prices = stacked_ladder(
                top=top,
                tick=tick,
                levels=int(self.args.v7_ladder_levels),
                parents=repair_count,
            )
            return (
                prices,
                float(self.args.v73_repair_max_price),
                (
                    f"V73_REPAIR target={repair_count}/{target_count}; under={d.underweight}; "
                    f"gap_shares={gap_shares:.3f}; gap_clips={d.gap_clips:.3f}; "
                    f"ask={ask[0]:.4f}; repair_top={top}; "
                    f"repair_max={self.args.v73_repair_max_price:.4f}; "
                    f"portfolio_cap_ignored_for_repair={portfolio_cap:.5f}"
                ),
            )

        prices, max_price, reason = super()._desired_side(
            now=now,
            side=side,
            book=book,
            target_count=target_count,
            targets=targets,
            portfolio_cap=portfolio_cap,
        )
        if d.neutral and bootstrap:
            reason = (
                f"V73_BOOTSTRAP parents={target_count}; threshold="
                f"{self.args.v73_bootstrap_two_sided_clips:.3f} clips; {reason}"
            )
        return prices, max_price, reason


async def _run(args):
    # Keep the January-scale emergency fill envelope from V7.2. Bootstrap and
    # repair targeting now control ordinary inventory risk before this boundary.
    def january_gap_allows(inv, *, side: str, shares: float, parent_clip: float) -> bool:
        clip = max(float(parent_clip), float(args.v7_parent_clip), 1e-9)
        limit = float(args.v7_hard_gap_clips) * clip
        old_gap = _inventory_signed_gap(inv)
        new_gap = old_gap + (float(shares) if side == "UP" else -float(shares))
        if abs(new_gap) <= limit + 1e-9:
            return True
        return abs(new_gap) < abs(old_gap) - 1e-9

    base.hard_gap_allows = january_gap_allows
    base.Result = v7.V7Result
    base.Engine = V73JanuaryPortfolioEngine
    base.apply_sell_print_to_orders = apply_sell_print_to_multi_orders

    original_choose = base.choose_market

    async def choose_clean(client: Any, asset: str, *, clean_start: bool) -> Any:
        return await original_choose(client, asset, clean_start=(not args.join_current))

    base.choose_market = choose_clean

    async def no_blocking_resolution(_slug: str, _timeout_s: float) -> None:
        return None

    base.gamma_winner = no_blocking_resolution
    return await base.amain(args)


def parse_args():
    args = v72.parse_args()
    args.v73_bootstrap_parents = 1
    args.v73_bootstrap_two_sided_clips = 0.5
    args.v73_repair_overshoot_parents = 1
    args.v73_repair_max_price = 0.92
    return args


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
