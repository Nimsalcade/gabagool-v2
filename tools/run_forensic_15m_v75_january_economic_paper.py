"""Read-only V7.5 January economic-repair paper harness.

V7.5 keeps V7.4's gradual empirical inventory replenishment but fixes the
failure demonstrated by completed V7.4 paper sessions: repeated deficient-side
repair was allowed up to 0.92 regardless of the already-owned side's cost basis,
which produced completed-set VWAPs around 1.08-1.31 and large strict losses.

Changes from V7.4
-----------------
1. Economic repair cap
   Deficient-side maker quotes are capped by projected aggregate
   UP_VWAP + DOWN_VWAP, using 1.005 early and a quadratic late ramp to 1.010.
   When the live ask is too expensive, V7.5 rests at the economic ceiling instead
   of chasing one tick below ask.

2. No extra repair parent
   Repair capacity covers the live gap only (ceil(gap / clip)); V7.3/V7.4's
   additional overshoot parent is removed to reduce balance-sign ping-pong.

3. No free overweight budget on every imbalance regime
   V7.4 granted one fresh overweight parent each time the inventory sign changed.
   Across repeated sign flips that was not globally bounded.  V7.5 starts each
   regime with zero free overweight budget; further overweight opportunity must
   be earned from underweight fills through V7.4's empirical flow ratio.

4. Runtime-safe hard-gap callback
   Uses the shared Inventory's actual ``up_shares`` / ``down_shares`` fields.

Paper-only/read-only: no wallet is loaded and no live order is submitted.
"""
from __future__ import annotations

import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v7_january_portfolio_paper as v7
import tools.run_forensic_15m_v74_january_empirical_paper as v74
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v7_january_portfolio import stacked_ladder
from src.v73_repair import repair_maker_top, repair_parent_count
from src.v75_economic_repair import economic_repair_price


class V75JanuaryEconomicEngine(v74.V74JanuaryEmpiricalEngine):
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

        # Intercept the deficient-side repair path before V7.3 can apply its
        # unconditional single-side 0.92 ceiling.
        if (not d.neutral) and d.underweight == side:
            ask = base._best_ask(book)
            tick = base._tick(book)
            if ask is None:
                return (), None, "v75_repair missing best ask"

            gap_shares = abs(float(self.inv.up_shares) - float(self.inv.down_shares))
            repair_count = repair_parent_count(
                gap_shares=gap_shares,
                parent_clip=self.clip,
                requested_parents=int(target_count),
                overshoot_parents=0,
            )
            if repair_count <= 0:
                return (), None, "v75_repair gap already closed"

            age = max(0.0, float(now) - float(self.market.window_start))
            economics = economic_repair_price(
                side=side,
                add_qty=self.clip,
                up_shares=self.inv.up_shares,
                up_cost=self.inv.up_cost,
                down_shares=self.inv.down_shares,
                down_cost=self.inv.down_cost,
                age_s=age,
                absolute_max=float(self.args.v73_repair_max_price),
                duration_s=900.0,
                base_cap=float(self.args.v75_repair_pair_cap),
                late_cap=float(self.args.v75_late_repair_pair_cap),
                late_start_s=float(self.args.v75_late_repair_start),
            )
            top = repair_maker_top(
                best_ask=ask[0],
                tick=tick,
                repair_max_price=economics.max_buy_price,
            )
            prices = stacked_ladder(
                top=top,
                tick=tick,
                levels=int(self.args.v7_ladder_levels),
                parents=repair_count,
            )
            return (
                prices,
                economics.max_buy_price,
                (
                    f"V75_ECON_REPAIR target={repair_count}/{target_count}; "
                    f"under={d.underweight}; gap_shares={gap_shares:.3f}; "
                    f"gap_clips={d.gap_clips:.3f}; ask={ask[0]:.4f}; "
                    f"pair_cap={economics.pair_cap:.5f}; "
                    f"economic_max={economics.max_buy_price:.4f}; top={top}; "
                    f"absolute_max={self.args.v73_repair_max_price:.4f}; "
                    f"normal_accumulation_cap={portfolio_cap:.5f}"
                ),
            )

        return super()._desired_side(
            now=now,
            side=side,
            book=book,
            target_count=target_count,
            targets=targets,
            portfolio_cap=portfolio_cap,
        )


async def _run(args):
    def economic_gap_allows(inv, *, side: str, shares: float, parent_clip: float) -> bool:
        clip = max(float(parent_clip), float(args.v7_parent_clip), 1e-9)
        limit = float(args.v7_hard_gap_clips) * clip
        old_gap = float(inv.up_shares) - float(inv.down_shares)
        new_gap = old_gap + (float(shares) if side == "UP" else -float(shares))
        if abs(new_gap) <= limit + 1e-9:
            return True
        # Outside the emergency envelope, only fills that reduce absolute
        # imbalance are accepted by the public-tape proxy.
        return abs(new_gap) < abs(old_gap) - 1e-9

    base.hard_gap_allows = economic_gap_allows
    base.Result = v7.V7Result
    base.Engine = V75JanuaryEconomicEngine
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
    args = v74.parse_args()

    # V7.5 intentionally removes the per-regime free overweight parent and the
    # extra repair parent.  Existing valid parents still persist normally.
    args.v74_initial_overweight_budget_parents = 0.0
    args.v73_repair_overshoot_parents = 0

    # Six supplied historical sessions ended with aggregate side-VWAP sums
    # between ~0.9694 and ~1.0059.  Keep a tight explicit gross repair budget.
    args.v75_repair_pair_cap = 1.005
    args.v75_late_repair_pair_cap = 1.010
    args.v75_late_repair_start = 600.0
    return args


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
