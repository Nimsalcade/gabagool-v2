"""Read-only V6.1 profitability-accounting paper harness.

V6.1 keeps the V6 rolling lot-pair router unchanged and adds the exact success
measurement requested for each 15-minute session:

* every fill records qty, price, fill cost and cumulative session cost;
* accumulation stops with 50 seconds remaining (market age 850s);
* all still-resting paper orders are canceled at the cutoff;
* matched UP/DOWN shares are valued as complete sets returning $1 each;
* all unmatched residual shares and their exact cost basis are reported;
* strict session PnL = merge_return - total_fill_cost.

Residual winner settlement is deliberately NOT credited to this metric.  This
makes the score a strict test of the complete-set accumulation strategy itself.
No wallet is loaded and no real order is submitted.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v6_lot_pair_paper as v6
from src.forensic_15m import DURATION_S
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v61_accounting import accumulation_cutoff_age, build_session_accounting

STOP_BEFORE_CLOSE_S = 50.0


@dataclass
class V61Result(base.Result):
    accumulation_stop_before_close_s: float = STOP_BEFORE_CLOSE_S
    accumulation_cutoff_age_s: float = DURATION_S - STOP_BEFORE_CLOSE_S
    total_fill_cost: float = 0.0
    up_fill_cost: float = 0.0
    down_fill_cost: float = 0.0
    total_filled_shares: float = 0.0
    merge_qty: float = 0.0
    merge_return: float = 0.0
    merge_cost_basis: float = 0.0
    locked_complete_set_pnl: float = 0.0
    leftover_up_qty: float = 0.0
    leftover_down_qty: float = 0.0
    leftover_total_qty: float = 0.0
    leftover_up_cost: float = 0.0
    leftover_down_cost: float = 0.0
    leftover_total_cost: float = 0.0
    returned_total: float = 0.0
    strict_session_pnl: float = 0.0
    strict_session_roi: float | None = None
    accounting_identity_error: float = 0.0


class V61ProfitEngine(v6.V6LotPairEngine):
    """V6 router plus cutoff enforcement and exact session-cost accounting."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v61_cutoff_emitted = False

    @property
    def _v61_cutoff_age(self) -> float:
        return accumulation_cutoff_age(
            duration_s=float(DURATION_S),
            stop_before_close_s=STOP_BEFORE_CLOSE_S,
        )

    def _reconcile(self, now: float, up_book: Any, down_book: Any) -> None:
        age = now - self.market.window_start
        if age >= self._v61_cutoff_age:
            for oid in list(self.orders):
                self._cancel(
                    now,
                    oid,
                    "V61_CANCEL_CUTOFF",
                    (
                        f"accumulation stopped with {STOP_BEFORE_CLOSE_S:.0f}s remaining; "
                        f"cutoff_age={self._v61_cutoff_age:.3f}s"
                    ),
                )
            if not self._v61_cutoff_emitted:
                self._v61_cutoff_emitted = True
                total_cost = self.inv.up_cost + self.inv.down_cost
                self.emit(
                    now,
                    "V61_ACCUMULATION_STOP",
                    reason=(
                        f"cutoff_age={self._v61_cutoff_age:.3f}; "
                        f"stop_before_close={STOP_BEFORE_CLOSE_S:.3f}; "
                        f"fills={self.result.maker_fills + self.result.taker_fills}; "
                        f"up_qty={self.inv.up_shares:.9f}; up_cost={self.inv.up_cost:.9f}; "
                        f"down_qty={self.inv.down_shares:.9f}; down_cost={self.inv.down_cost:.9f}; "
                        f"total_fill_cost={total_cost:.9f}"
                    ),
                )
            return
        return super()._reconcile(now, up_book, down_book)

    def _apply_one_tape_print(self, now: float, p: Any) -> bool:
        # The public tape can be drained slightly after the 850s wall-clock
        # boundary.  Honor a fill if the actual print happened before cutoff;
        # ignore prints whose event timestamp is at/after the cutoff.
        event_age = float(p.event_ts) - float(self.market.window_start)
        if event_age >= self._v61_cutoff_age:
            return False
        return super()._apply_one_tape_print(now, p)

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
        if not ok:
            return False
        fill_cost = float(shares) * float(price)
        total_cost = self.inv.up_cost + self.inv.down_cost
        self.emit(
            now,
            "V61_FILL_ACCOUNT",
            side=side,
            order=order,
            qty=shares,
            price=price,
            cost=fill_cost,
            reason=(
                f"fill_cost={fill_cost:.9f}; cumulative_total_fill_cost={total_cost:.9f}; "
                f"cumulative_up_cost={self.inv.up_cost:.9f}; "
                f"cumulative_down_cost={self.inv.down_cost:.9f}; "
                f"up_shares={self.inv.up_shares:.9f}; down_shares={self.inv.down_shares:.9f}"
            ),
        )
        return True

    async def run(self, client: Any):
        result = await super().run(client)
        acct = build_session_accounting(
            pool=self.pool,
            up_filled_shares=self.inv.up_shares,
            up_fill_cost=self.inv.up_cost,
            down_filled_shares=self.inv.down_shares,
            down_fill_cost=self.inv.down_cost,
        )

        result.accumulation_stop_before_close_s = STOP_BEFORE_CLOSE_S
        result.accumulation_cutoff_age_s = self._v61_cutoff_age
        result.total_fill_cost = acct.total_fill_cost
        result.up_fill_cost = acct.up_fill_cost
        result.down_fill_cost = acct.down_fill_cost
        result.total_filled_shares = acct.total_filled_shares
        result.merge_qty = acct.merge_qty
        result.merge_return = acct.merge_return
        result.merge_cost_basis = acct.merge_cost_basis
        result.locked_complete_set_pnl = acct.locked_complete_set_pnl
        result.leftover_up_qty = acct.leftover_up_qty
        result.leftover_down_qty = acct.leftover_down_qty
        result.leftover_total_qty = acct.leftover_total_qty
        result.leftover_up_cost = acct.leftover_up_cost
        result.leftover_down_cost = acct.leftover_down_cost
        result.leftover_total_cost = acct.leftover_total_cost
        result.returned_total = acct.returned_total
        result.strict_session_pnl = acct.pnl
        result.strict_session_roi = acct.roi_on_session_cost
        result.accounting_identity_error = acct.accounting_identity_error

        self.emit(
            __import__("time").time(),
            "V61_SESSION_ACCOUNTING",
            reason=(
                f"TOTAL_FILL_COST={acct.total_fill_cost:.9f}; "
                f"UP={acct.up_filled_shares:.9f}sh/${acct.up_fill_cost:.9f}; "
                f"DOWN={acct.down_filled_shares:.9f}sh/${acct.down_fill_cost:.9f}; "
                f"MERGE_QTY={acct.merge_qty:.9f}; MERGE_RETURN={acct.merge_return:.9f}; "
                f"MERGE_COST_BASIS={acct.merge_cost_basis:.9f}; "
                f"LEFTOVER_UP={acct.leftover_up_qty:.9f}sh/${acct.leftover_up_cost:.9f}; "
                f"LEFTOVER_DOWN={acct.leftover_down_qty:.9f}sh/${acct.leftover_down_cost:.9f}; "
                f"LEFTOVER_COST={acct.leftover_total_cost:.9f}; "
                f"RETURNED_TOTAL={acct.returned_total:.9f}; "
                f"PNL={acct.pnl:.9f}; ROI={acct.roi_on_session_cost}; "
                f"IDENTITY_ERROR={acct.accounting_identity_error:.12f}"
            ),
        )
        print("=" * 96)
        print(f"V6.1 SESSION ACCOUNTING | {self.market.asset.upper()} | {self.market.slug}")
        print(
            f"FILLS  UP {acct.up_filled_shares:.3f} sh cost=${acct.up_fill_cost:.4f} | "
            f"DOWN {acct.down_filled_shares:.3f} sh cost=${acct.down_fill_cost:.4f} | "
            f"TOTAL COST=${acct.total_fill_cost:.4f}"
        )
        print(
            f"MERGE  {acct.merge_qty:.3f} sets -> RETURN=${acct.merge_return:.4f} | "
            f"matched cost=${acct.merge_cost_basis:.4f} | "
            f"pair VWAP={(acct.completed_pair_vwap or 0):.4f}"
        )
        print(
            f"LEFT   UP {acct.leftover_up_qty:.3f} sh cost=${acct.leftover_up_cost:.4f} | "
            f"DOWN {acct.leftover_down_qty:.3f} sh cost=${acct.leftover_down_cost:.4f} | "
            f"TOTAL LEFT COST=${acct.leftover_total_cost:.4f}"
        )
        print(
            f"PNL    RETURN ${acct.returned_total:.4f} - COST ${acct.total_fill_cost:.4f} "
            f"= ${acct.pnl:+.4f} | ROI={(acct.roi_on_session_cost or 0)*100:+.3f}%"
        )
        print(
            f"CHECK  total_cost - (matched_cost + leftover_cost) "
            f"= {acct.accounting_identity_error:+.12f}"
        )
        print("=" * 96)
        return result


def parse_args() -> argparse.Namespace:
    return v6.parse_args()


async def _run(args: argparse.Namespace) -> int:
    # Result is monkeypatched so base.amain's dataclass/asdict summary includes
    # all V6.1 profitability fields automatically.
    base.Result = V61Result
    base.Engine = V61ProfitEngine
    base.apply_sell_print_to_orders = apply_sell_print_to_multi_orders

    original_choose = base.choose_market

    async def choose_clean(client: Any, asset: str, *, clean_start: bool) -> Any:
        return await original_choose(
            client,
            asset,
            clean_start=(not args.join_current),
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
