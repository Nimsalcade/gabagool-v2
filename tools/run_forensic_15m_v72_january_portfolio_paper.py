"""Read-only V7.2 January portfolio paper harness.

V7.2 keeps V7.1's continuous two-sided inventory controller and January-scale
22-clip emergency envelope, but fixes the replenishment defect found in the
first live-paper window:

* inside a 0.5-clip neutral band, both sides may create/replenish parents;
* outside that band, ONLY the underweight side may create fresh/replacement
  parents;
* overweight-side parents already resting are preserved subject to the normal
  economic, staleness and excess-count checks;
* when an overweight parent fills or is cancelled, it is NOT replaced until
  inventory returns to neutral or the inventory sign flips.

This preserves queue priority and historical-looking overshoot without allowing
"one overweight parent" to become unlimited cumulative overweight accumulation.
No wallet is loaded and no real order is sent.
"""
from __future__ import annotations

import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v7_january_portfolio_paper as v7
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v72_replenishment import replenishment_decision


class V72JanuaryPortfolioEngine(v7.V7JanuaryPortfolioEngine):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v72_block_new_side: str | None = None
        self._v72_last_replenish_sig: str | None = None

    def _refresh_clip(self, now: float) -> float:
        self.clip = float(self.args.v7_parent_clip)
        return self.clip

    def _replenishment(self):
        return replenishment_decision(
            up_shares=self.inv.up_shares,
            down_shares=self.inv.down_shares,
            parent_clip=max(self.clip, float(self.args.v7_parent_clip), 1e-9),
            neutral_gap_clips=float(self.args.v72_neutral_gap_clips),
        )

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
        if self._v72_block_new_side == side:
            return False
        return super()._post_v7(
            now=now,
            side=side,
            price=price,
            up_book=up_book,
            down_book=down_book,
            reason=reason,
        )

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
        """Preserve old overweight parents but prohibit fresh replacements."""
        d = self._replenishment()
        allow_new = d.allow_up if side == "UP" else d.allow_down

        sig = (
            f"{d.underweight}|{d.gap_clips:.3f}|{d.neutral}|"
            f"{d.allow_up}|{d.allow_down}"
        )
        if sig != self._v72_last_replenish_sig:
            self._v72_last_replenish_sig = sig
            self.emit(
                now,
                "V72_REPLENISH_STATE",
                reason=(
                    f"underweight={d.underweight}; gap_clips={d.gap_clips:.3f}; "
                    f"neutral={d.neutral}; allow_new_UP={d.allow_up}; "
                    f"allow_new_DOWN={d.allow_down}; "
                    f"neutral_band={self.args.v72_neutral_gap_clips:.3f} clips"
                ),
            )

        previous = self._v72_block_new_side
        self._v72_block_new_side = None if allow_new else side
        try:
            # Parent implementation still performs all validation and cancels
            # economic/stale/newest-excess parents.  Only creation is blocked.
            return super()._reconcile_side(
                now=now,
                side=side,
                desired_prices=desired_prices,
                max_price=max_price,
                reason=(
                    f"{reason}; v72_allow_new={allow_new}; "
                    f"v72_gap_clips={d.gap_clips:.3f}"
                ),
                up_book=up_book,
                down_book=down_book,
            )
        finally:
            self._v72_block_new_side = previous

    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        self._refresh_clip(now)
        return super().expire(now, up_book, down_book)


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
    base.Engine = V72JanuaryPortfolioEngine
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
    args = v7.parse_args()
    # argparse is owned by the V7 harness.  Keep V7.2's one new parameter as a
    # runtime attribute so the production-surface change stays isolated.
    args.v72_neutral_gap_clips = 0.5
    return args


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
