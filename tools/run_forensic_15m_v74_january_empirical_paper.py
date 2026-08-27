"""Read-only V7.4 January empirical-controller paper harness.

V7.4 keeps V7.3's small bootstrap and marginal underweight repair pricing, but
replaces V7.2/V7.3's binary ``gap > 0.5 clips => no fresh overweight parents``
rule with the gradual response observed across the complete Jan-1 and Jan-17
Gabagool sessions.

Core change
-----------
Fresh overweight replenishment is controlled by a cumulative *budget* inside
an imbalance regime:

* existing resting parents are still preserved when valid;
* a newly established imbalance gets only a small finite overweight budget;
* fills on the underweight side earn additional overweight opportunity budget;
* the earned ratio falls as the inventory gap grows;
* late in the market the ratio falls further (stronger convergence pressure);
* at the observed ~22-clip outer envelope, no fresh gap-increasing exposure is
  permitted.

This fixes both previous extremes:

* V7.1 could replenish a positive overweight parent target forever;
* V7.2/V7.3 shut fresh overweight opportunity off almost immediately.

Balanced accumulation still uses the normal portfolio economics.  Deficient-
side repair still uses V7.3's marginal-repair maker pricing with a 0.92 ceiling.
The program is paper-only/read-only and never loads a wallet or submits orders.
"""
from __future__ import annotations

import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v7_january_portfolio_paper as v7
import tools.run_forensic_15m_v73_january_portfolio_paper as v73
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v74_empirical_controller import (
    fresh_overweight_allowance_shares,
    fresh_overweight_post_allowed,
)


class V74JanuaryEmpiricalEngine(v73.V73JanuaryPortfolioEngine):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v74_regime_underweight: str | None = None
        self._v74_regime_underweight_start_qty = 0.0
        self._v74_fresh_overweight_posted_shares = 0.0
        self._v74_last_budget_sig: str | None = None

    def _v74_sync_regime(self, now: float):
        d = self._replenishment()
        if d.neutral or d.underweight is None:
            if self._v74_regime_underweight is not None:
                self.emit(
                    now,
                    "V74_REPLENISH_REGIME_RESET",
                    reason=(
                        f"returned_to_neutral; previous_underweight={self._v74_regime_underweight}; "
                        f"fresh_overweight_posted={self._v74_fresh_overweight_posted_shares:.3f}"
                    ),
                )
            self._v74_regime_underweight = None
            self._v74_regime_underweight_start_qty = 0.0
            self._v74_fresh_overweight_posted_shares = 0.0
            return d

        if d.underweight != self._v74_regime_underweight:
            self._v74_regime_underweight = d.underweight
            self._v74_regime_underweight_start_qty = (
                float(self.inv.up_shares) if d.underweight == "UP" else float(self.inv.down_shares)
            )
            self._v74_fresh_overweight_posted_shares = 0.0
            self._v74_last_budget_sig = None
            self.emit(
                now,
                "V74_REPLENISH_REGIME_START",
                reason=(
                    f"underweight={d.underweight}; gap_clips={d.gap_clips:.3f}; "
                    f"initial_overweight_budget_parents={self.args.v74_initial_overweight_budget_parents}"
                ),
            )
        return d

    def _v74_budget(self, now: float):
        d = self._v74_sync_regime(now)
        if d.neutral or d.underweight is None:
            return d, None, 0.0

        current_under = (
            float(self.inv.up_shares) if d.underweight == "UP" else float(self.inv.down_shares)
        )
        repaired = max(0.0, current_under - self._v74_regime_underweight_start_qty)
        age = max(0.0, float(now) - float(self.market.window_start))
        allowance, flow = fresh_overweight_allowance_shares(
            gap_clips=d.gap_clips,
            age_s=age,
            underweight_filled_since_regime=repaired,
            parent_clip=self.clip,
            initial_overweight_parents=float(self.args.v74_initial_overweight_budget_parents),
            duration_s=900.0,
            hard_gap_clips=float(self.args.v7_hard_gap_clips),
            late_pull=float(self.args.v74_late_inventory_pull),
        )

        sig = (
            f"{d.underweight}|{d.gap_clips:.2f}|{repaired:.1f}|{allowance:.1f}|"
            f"{self._v74_fresh_overweight_posted_shares:.1f}|{flow.underweight_share_target:.3f}"
        )
        if sig != self._v74_last_budget_sig:
            self._v74_last_budget_sig = sig
            self.emit(
                now,
                "V74_REPLENISH_BUDGET",
                reason=(
                    f"underweight={d.underweight}; gap_clips={d.gap_clips:.3f}; "
                    f"underweight_filled_regime={repaired:.3f}; "
                    f"underweight_flow_target={flow.underweight_share_target:.4f}; "
                    f"over_to_under_ratio={flow.overweight_to_underweight_ratio:.4f}; "
                    f"fresh_overweight_allowance={allowance:.3f}; "
                    f"fresh_overweight_posted={self._v74_fresh_overweight_posted_shares:.3f}; "
                    f"hard_stop={flow.hard_stop}"
                ),
            )
        return d, flow, allowance

    def _post_v7(
        self,
        *,
        now: float,
        side: str,
        price: float,
        up_book: Any,
        down_book: Any,
        reason: str,
    ) -> bool:
        d, flow, allowance = self._v74_budget(now)

        # Outside neutral, only the *fresh overweight* side is budgeted.  The
        # underweight side is free to replenish according to V7.3 repair targets.
        is_fresh_overweight = (
            (not d.neutral)
            and d.underweight is not None
            and side != d.underweight
        )
        if is_fresh_overweight:
            if not fresh_overweight_post_allowed(
                already_posted_shares=self._v74_fresh_overweight_posted_shares,
                next_parent_shares=self.clip,
                allowance_shares=allowance,
            ):
                self.emit(
                    now,
                    "V74_BLOCK_FRESH_OVERWEIGHT",
                    side=side,
                    qty=self.clip,
                    price=price,
                    reason=(
                        f"underweight={d.underweight}; gap_clips={d.gap_clips:.3f}; "
                        f"allowance={allowance:.3f}; "
                        f"already_posted={self._v74_fresh_overweight_posted_shares:.3f}; "
                        f"flow_target={None if flow is None else flow.underweight_share_target:.4f}; "
                        f"{reason}"
                    ),
                )
                return False

        # Bypass V7.2's binary block explicitly.  The original V7 posting
        # implementation is used after V7.4's budget admission decision.
        ok = v7.V7JanuaryPortfolioEngine._post_v7(
            self,
            now=now,
            side=side,
            price=price,
            up_book=up_book,
            down_book=down_book,
            reason=(
                f"V74_EMPIRICAL; {reason}; "
                f"fresh_overweight={is_fresh_overweight}; allowance={allowance:.3f}; "
                f"posted={self._v74_fresh_overweight_posted_shares:.3f}"
            ),
        )
        if ok and is_fresh_overweight:
            self._v74_fresh_overweight_posted_shares += float(self.clip)
        return ok

    def _reconcile_side(
        self,
        *,
        now: float,
        side: str,
        desired_prices: tuple[float, ...],
        max_price: float | None,
        reason: str,
        up_book: Any,
        down_book: Any,
    ) -> None:
        self._v74_sync_regime(now)
        # Bypass V7.2's all-or-nothing fresh-side guard.  V7.4 gates individual
        # fresh overweight parents in _post_v7 using the empirical budget.
        return v7.V7JanuaryPortfolioEngine._reconcile_side(
            self,
            now=now,
            side=side,
            desired_prices=desired_prices,
            max_price=max_price,
            reason=f"V74_EMPIRICAL; {reason}",
            up_book=up_book,
            down_book=down_book,
        )


async def _run(args):
    def january_gap_allows(inv, *, side: str, shares: float, parent_clip: float) -> bool:
        clip = max(float(parent_clip), float(args.v7_parent_clip), 1e-9)
        limit = float(args.v7_hard_gap_clips) * clip
        old_gap = float(inv.signed_gap)
        new_gap = old_gap + (float(shares) if side == "UP" else -float(shares))
        if abs(new_gap) <= limit + 1e-9:
            return True
        return abs(new_gap) < abs(old_gap) - 1e-9

    base.hard_gap_allows = january_gap_allows
    base.Result = v7.V7Result
    base.Engine = V74JanuaryEmpiricalEngine
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
    args = v73.parse_args()
    args.v74_initial_overweight_budget_parents = 1.0
    args.v74_late_inventory_pull = 0.45
    return args


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
