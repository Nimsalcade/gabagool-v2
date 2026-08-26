"""Read-only V7 January-style portfolio accumulation paper harness.

V7 replaces V6's serial SEED -> COMPLETE -> FLIP router with a continuous
portfolio controller motivated by the January 17 forensic reconstruction:

* both UP and DOWN maintain resting BUY parents throughout the live window;
* the underweight side gets progressively more parents as inventory drifts;
* the overweight side is throttled, but normally remains quoted to preserve
  price opportunity and queue priority;
* old valid parents keep priority; newest excess parents are cancelled first;
* multiple independent parents may stack at the same cent;
* individual fills are NOT assigned a local 0.99 complementary-price gate;
* aggregate projected UP_VWAP + DOWN_VWAP is the profitability guard;
* balance pressure and allowable portfolio basis relax toward par near expiry;
* quoting stops at age 894s (six seconds before close), matching the observed
  last-fill age of the January 17 sample while keeping the paper test explicit.

No wallet is loaded and no real order is sent.  This is a paper reconstruction
of the observed architecture, not a claim that the private Gabagool source used
these exact hidden parameters.
"""
from __future__ import annotations

import argparse
import asyncio
import math
from dataclasses import dataclass
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v53_paper as v53
import tools.run_forensic_15m_v6_lot_pair_paper as v6
from src.forensic_15m import DURATION_S, QUOTE_START_AGE_S
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v61_accounting import build_session_accounting
from src.v7_january_portfolio import (
    effective_portfolio_cap,
    max_buy_price_for_portfolio_cap,
    parent_targets,
    passive_top,
    stacked_ladder,
)

JANUARY_QUOTE_END_AGE_S = 894.0


def _best_bid(book: Any) -> tuple[float, float] | None:
    bids = base._bids(book)
    if not bids:
        return None
    return max(bids, key=lambda x: x[0])


@dataclass
class V7Result(base.Result):
    accumulation_cutoff_age_s: float = JANUARY_QUOTE_END_AGE_S
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
    end_balance_pct: float | None = None
    end_combined_vwap: float | None = None


class V7JanuaryPortfolioEngine(v6.V6LotPairEngine):
    """Continuous two-sided inventory-controlled portfolio accumulator."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v7_last_sig: str | None = None
        self._v7_cutoff_emitted = False

    def _refresh_clip(self, now: float) -> float:
        # The Jan-17 sample's median observed fill clip was 14.025 shares.  We
        # expose a fixed paper parent size so this can be tested independently
        # from the Oct/Nov/Dec schedules used by earlier reconstructions.
        self.clip = float(self.args.v7_parent_clip)
        return self.clip

    def _portfolio_cap(self, age: float) -> float:
        return effective_portfolio_cap(
            age_s=age,
            duration_s=float(DURATION_S),
            base_cap=float(self.args.v7_portfolio_cap),
            late_cap=float(self.args.v7_late_portfolio_cap),
            late_start_s=float(self.args.v7_late_cap_start),
        )

    def _targets(self, age: float):
        return parent_targets(
            up_shares=self.inv.up_shares,
            down_shares=self.inv.down_shares,
            parent_clip=self.clip,
            age_s=age,
            duration_s=float(DURATION_S),
            base_parents=self.args.v7_base_parents,
            max_parents=self.args.v7_max_parents,
            min_overweight_parents=self.args.v7_min_overweight_parents,
            underweight_gain_per_clip=self.args.v7_underweight_gain,
            overweight_decay_per_clip=self.args.v7_overweight_decay,
            late_pressure_gain=self.args.v7_late_pressure_gain,
            hard_gap_clips=self.args.v7_hard_gap_clips,
        )

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
        if target_count <= 0:
            return (), None, "target_count=0"

        bid = _best_bid(book)
        ask = base._best_ask(book)
        tick = base._tick(book)
        if ask is None:
            return (), None, "missing best ask"

        max_price = max_buy_price_for_portfolio_cap(
            side=side,
            add_qty=self.clip,
            up_shares=self.inv.up_shares,
            up_cost=self.inv.up_cost,
            down_shares=self.inv.down_shares,
            down_cost=self.inv.down_cost,
            combined_cap=portfolio_cap,
            absolute_max=self.args.v7_absolute_max_price,
        )

        # Inventory steering is implemented through both parent count and quote
        # aggressiveness. Underweight side improves the best bid by one tick;
        # overweight exposure backs away as gap grows but remains live unless the
        # hard gap boundary is reached.
        under = targets.underweight
        if under == side:
            improve_ticks = self.args.v7_underweight_improve_ticks
            backoff_ticks = 0
        elif under is None:
            improve_ticks = self.args.v7_balanced_improve_ticks
            backoff_ticks = 0
        else:
            improve_ticks = 0
            backoff_ticks = min(
                self.args.v7_max_overweight_backoff_ticks,
                int(targets.gap_clips // max(self.args.v7_gap_clips_per_backoff_tick, 1e-9)),
            )

        top = passive_top(
            best_bid=None if bid is None else bid[0],
            best_ask=ask[0],
            tick=tick,
            max_price=max_price,
            improve_ticks=improve_ticks,
            backoff_ticks=backoff_ticks,
        )
        prices = stacked_ladder(
            top=top,
            tick=tick,
            levels=self.args.v7_ladder_levels,
            parents=target_count,
        )
        return (
            prices,
            max_price,
            (
                f"target={target_count}; under={under}; gap_clips={targets.gap_clips:.3f}; "
                f"pressure={targets.pressure:.3f}; portfolio_cap={portfolio_cap:.5f}; "
                f"max_buy={max_price:.4f}; improve={improve_ticks}; backoff={backoff_ticks}"
            ),
        )

    def _cancel_v7(self, now: float, oid: str, event: str, reason: str) -> None:
        o = self.orders.pop(oid, None)
        if o is None:
            return
        self.emit(now, event, side=o.side, order=o, qty=o.remaining, price=o.price, reason=reason)

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
        before = set(self.orders)
        base.Engine.post(self, side, price, now, up_book=up_book, down_book=down_book)
        new_ids = set(self.orders) - before
        if not new_ids:
            return False
        oid = next(iter(new_ids))
        o = self.orders[oid]

        # If another own parent was created at the same price in this exact
        # reconciliation burst, only the first copy inherits external queue
        # ahead; later copies are behind our own first parent.
        siblings = [
            x for x in self.orders.values()
            if x.oid != oid and x.side == side and abs(x.price - price) <= 1e-9
            and abs(x.created - now) <= 1e-6
        ]
        if siblings and o.shadow is not None:
            o.shadow.queue_ahead = 0.0

        self.emit(
            now,
            "V7_STACK_POST" if siblings else "V7_QUOTE_POST",
            side=side,
            order=o,
            qty=o.remaining,
            price=o.price,
            reason=reason,
        )
        return True

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
        desired_count = len(desired_prices)
        book = up_book if side == "UP" else down_book
        tick = base._tick(book)
        top = max(desired_prices) if desired_prices else None
        stale_floor = (
            None
            if top is None
            else max(0.0, top - self.args.v7_stale_keep_ticks * tick)
        )

        candidates = sorted(
            [o for o in self.orders.values() if o.side == side],
            key=lambda o: (o.created, -o.price),
        )
        valid = []
        for o in candidates:
            if desired_count <= 0:
                self._cancel_v7(now, o.oid, "V7_CANCEL_SIDE_OFF", reason)
                continue
            if max_price is not None and o.price > max_price + 1e-9:
                self._cancel_v7(
                    now, o.oid, "V7_CANCEL_ECONOMIC",
                    f"resting={o.price:.4f} > portfolio max={max_price:.4f}; {reason}",
                )
                continue
            if stale_floor is not None and o.price < stale_floor - 1e-9:
                self._cancel_v7(
                    now, o.oid, "V7_CANCEL_DEEP_STALE",
                    f"resting={o.price:.4f} < stale_floor={stale_floor:.4f}; {reason}",
                )
                continue
            valid.append(o)

        # When inventory pressure reduces target exposure, preserve the oldest
        # parents/queue positions and cancel newest excess parents first.
        if len(valid) > desired_count:
            keep = sorted(valid, key=lambda o: (o.created, -o.price))[:desired_count]
            keep_ids = {o.oid for o in keep}
            for o in valid:
                if o.oid not in keep_ids:
                    self._cancel_v7(
                        now, o.oid, "V7_CANCEL_NEWEST_EXCESS",
                        f"live={len(valid)} target={desired_count}; preserve older queue; {reason}",
                    )
            valid = keep

        if desired_count <= len(valid):
            return

        wanted: dict[float, int] = {}
        for px in desired_prices:
            key = round(px, 10)
            wanted[key] = wanted.get(key, 0) + 1
        live_counts: dict[float, int] = {}
        for o in valid:
            key = round(o.price, 10)
            live_counts[key] = live_counts.get(key, 0) + 1

        missing = desired_count - len(valid)
        for px in desired_prices:
            if missing <= 0:
                break
            key = round(px, 10)
            if live_counts.get(key, 0) >= wanted.get(key, 0):
                continue
            if self._post_v7(
                now=now,
                side=side,
                price=px,
                up_book=up_book,
                down_book=down_book,
                reason=reason,
            ):
                live_counts[key] = live_counts.get(key, 0) + 1
                missing -= 1

        # If sticky older parents occupy different valid levels, fill any final
        # parent-count vacancies at the current desired ladder without forcing a
        # mass reprice.
        if missing > 0:
            for px in desired_prices:
                if missing <= 0:
                    break
                if self._post_v7(
                    now=now,
                    side=side,
                    price=px,
                    up_book=up_book,
                    down_book=down_book,
                    reason=reason,
                ):
                    missing -= 1

    def _cancel_all_cutoff(self, now: float) -> None:
        for oid in list(self.orders):
            self._cancel_v7(
                now,
                oid,
                "V7_CANCEL_CUTOFF",
                f"January paper cutoff at age={JANUARY_QUOTE_END_AGE_S:.1f}s",
            )
        if not self._v7_cutoff_emitted:
            self._v7_cutoff_emitted = True
            self.emit(
                now,
                "V7_ACCUMULATION_STOP",
                reason=(
                    f"cutoff_age={JANUARY_QUOTE_END_AGE_S:.1f}; "
                    f"UP={self.inv.up_shares:.6f}; DOWN={self.inv.down_shares:.6f}; "
                    f"gap={self.inv.up_shares-self.inv.down_shares:+.6f}"
                ),
            )

    def _reconcile(self, now: float, up_book: Any, down_book: Any) -> None:
        age = now - self.market.window_start
        if age >= JANUARY_QUOTE_END_AGE_S:
            self._cancel_all_cutoff(now)
            return
        if age < QUOTE_START_AGE_S:
            return

        self._refresh_clip(now)
        targets = self._targets(age)
        cap = self._portfolio_cap(age)

        up_prices, up_max, up_reason = self._desired_side(
            now=now,
            side="UP",
            book=up_book,
            target_count=targets.up,
            targets=targets,
            portfolio_cap=cap,
        )
        dn_prices, dn_max, dn_reason = self._desired_side(
            now=now,
            side="DOWN",
            book=down_book,
            target_count=targets.down,
            targets=targets,
            portfolio_cap=cap,
        )

        self._reconcile_side(
            now=now, side="UP", desired_prices=up_prices, max_price=up_max,
            reason=up_reason, up_book=up_book, down_book=down_book,
        )
        self._reconcile_side(
            now=now, side="DOWN", desired_prices=dn_prices, max_price=dn_max,
            reason=dn_reason, up_book=up_book, down_book=down_book,
        )

        sig = (
            f"{targets.up}|{targets.down}|{targets.underweight}|"
            f"{targets.gap_clips:.2f}|{cap:.4f}|"
            f"{max(up_prices) if up_prices else None}|{max(dn_prices) if dn_prices else None}"
        )
        if sig != self._v7_last_sig:
            self._v7_last_sig = sig
            self.emit(
                now,
                "V7_PORTFOLIO_STATE",
                reason=(
                    f"UP_parents={targets.up}; DOWN_parents={targets.down}; "
                    f"underweight={targets.underweight}; gap_clips={targets.gap_clips:.3f}; "
                    f"pressure={targets.pressure:.3f}; cap={cap:.5f}; "
                    f"UP_top={max(up_prices) if up_prices else None}; "
                    f"DOWN_top={max(dn_prices) if dn_prices else None}"
                ),
            )

    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        if now - self.market.window_start >= JANUARY_QUOTE_END_AGE_S:
            self._cancel_all_cutoff(now)
            return
        # Preserve queue age. Reconciliation decides whether a parent is still
        # economically/sizing valid; TTL itself is only a keepalive in tape mode.
        for o in self.orders.values():
            if now >= o.expires:
                o.expires = now + self.args.quote_ttl

    def reconcile_desired_after_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        self._reconcile(now, up_book, down_book)

    def renew(self, now: float, up_book: Any, down_book: Any) -> None:
        self._reconcile(now, up_book, down_book)

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        return None

    def _apply_one_tape_print(self, now: float, p: Any) -> bool:
        event_age = float(p.event_ts) - float(self.market.window_start)
        if event_age >= JANUARY_QUOTE_END_AGE_S:
            return False
        return super()._apply_one_tape_print(now, p)

    async def run(self, client: Any):
        # Bypass V6's end-of-window lot-pair metrics, but retain the common live
        # tape engine inherited by V53/base.
        result = await v53.V53Engine.run(self, client)
        acct = build_session_accounting(
            pool=self.pool,
            up_filled_shares=self.inv.up_shares,
            up_fill_cost=self.inv.up_cost,
            down_filled_shares=self.inv.down_shares,
            down_fill_cost=self.inv.down_cost,
        )

        result.accumulation_cutoff_age_s = JANUARY_QUOTE_END_AGE_S
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

        larger = max(self.inv.up_shares, self.inv.down_shares)
        result.end_balance_pct = (
            100.0 * min(self.inv.up_shares, self.inv.down_shares) / larger
            if larger > 0 else None
        )
        result.end_combined_vwap = self.inv.policy().combined_vwap

        self.emit(
            __import__("time").time(),
            "V7_SESSION_ACCOUNTING",
            reason=(
                f"TOTAL_COST={acct.total_fill_cost:.9f}; MERGE={acct.merge_qty:.9f}; "
                f"RETURN={acct.merge_return:.9f}; LEFT_COST={acct.leftover_total_cost:.9f}; "
                f"PNL={acct.pnl:.9f}; ROI={acct.roi_on_session_cost}; "
                f"BALANCE={result.end_balance_pct}; COMBINED_VWAP={result.end_combined_vwap}; "
                f"IDENTITY_ERROR={acct.accounting_identity_error:.12f}"
            ),
        )

        print("=" * 104)
        print(f"V7 JANUARY PORTFOLIO | {self.market.asset.upper()} | {self.market.slug}")
        print(
            f"FILLS   UP {acct.up_filled_shares:.3f} sh / ${acct.up_fill_cost:.4f} | "
            f"DOWN {acct.down_filled_shares:.3f} sh / ${acct.down_fill_cost:.4f} | "
            f"TOTAL {acct.total_filled_shares:.3f} sh / ${acct.total_fill_cost:.4f}"
        )
        print(
            f"MERGE   {acct.merge_qty:.3f} sets -> ${acct.merge_return:.4f} | "
            f"matched cost=${acct.merge_cost_basis:.4f} | pair VWAP={(acct.completed_pair_vwap or 0):.4f}"
        )
        print(
            f"LEFT    UP {acct.leftover_up_qty:.3f}/${acct.leftover_up_cost:.4f} | "
            f"DOWN {acct.leftover_down_qty:.3f}/${acct.leftover_down_cost:.4f} | "
            f"left cost=${acct.leftover_total_cost:.4f}"
        )
        print(
            f"BALANCE {(result.end_balance_pct or 0):.2f}% | "
            f"COMBINED VWAP={(result.end_combined_vwap or 0):.4f} | "
            f"LAST FILL AGE={result.last_fill_age_s}"
        )
        print(
            f"STRICT  RETURN ${acct.returned_total:.4f} - COST ${acct.total_fill_cost:.4f} "
            f"= ${acct.pnl:+.4f} | ROI={(acct.roi_on_session_cost or 0)*100:+.3f}%"
        )
        print("=" * 104)
        return result


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Read-only V7 January-style continuous portfolio paper harness"
    )
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=96)
    ap.add_argument("--poll", type=float, default=0.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0)
    ap.add_argument("--maker-fill-backend", choices=("public_tape",), default="public_tape")
    ap.add_argument("--v53-regime", choices=("oct", "nov", "dec"), default="dec")
    ap.add_argument("--resolution-timeout", type=float, default=1.0)
    ap.add_argument("--join-current", action="store_true")
    ap.add_argument("--out", default="data/gabagool_v7_january_validation")

    ap.add_argument("--v7-parent-clip", type=float, default=14.0)
    ap.add_argument("--v7-base-parents", type=int, default=4)
    ap.add_argument("--v7-max-parents", type=int, default=12)
    ap.add_argument("--v7-min-overweight-parents", type=int, default=1)
    ap.add_argument("--v7-ladder-levels", type=int, default=4)
    ap.add_argument("--v7-underweight-gain", type=float, default=0.55)
    ap.add_argument("--v7-overweight-decay", type=float, default=0.22)
    ap.add_argument("--v7-late-pressure-gain", type=float, default=1.50)
    ap.add_argument("--v7-hard-gap-clips", type=float, default=22.0)
    ap.add_argument("--v7-portfolio-cap", type=float, default=0.995)
    ap.add_argument("--v7-late-portfolio-cap", type=float, default=1.000)
    ap.add_argument("--v7-late-cap-start", type=float, default=600.0)
    ap.add_argument("--v7-absolute-max-price", type=float, default=0.92)
    ap.add_argument("--v7-underweight-improve-ticks", type=int, default=1)
    ap.add_argument("--v7-balanced-improve-ticks", type=int, default=1)
    ap.add_argument("--v7-gap-clips-per-backoff-tick", type=float, default=2.0)
    ap.add_argument("--v7-max-overweight-backoff-ticks", type=int, default=4)
    ap.add_argument("--v7-stale-keep-ticks", type=int, default=8)

    args = ap.parse_args()

    # Compatibility fields expected by inherited V5/V5.3 surfaces.  V7 disables
    # taker routing and does not use V6's serial pair-cap parameters.
    args.taker_mode = "off"
    args.max_combined_vwap = 1.01
    args.fresh_pair_cap = 0.0
    args.v53_aggressive_mode = "off"
    args.v53_aggressive_cooldown = 0.0
    args.v53_repair_basis_cap = 1.0
    args.v53_aggressive_pair_cap = 1.0
    args.v53_aggressive_headroom = 0.0

    # V6 attributes are retained only because V7 subclasses the implementation
    # for the FIFO lot pool / public-tape plumbing. They are not routing gates.
    args.v6_pair_cap = 0.99
    args.v6_seed_max_price = 0.49
    args.v6_seed_parents = 4
    args.v6_ladder_levels = 4
    args.v6_overbook_clips = 2

    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(args.poll, args.quote_ttl, args.paper_cash, args.v7_parent_clip) <= 0:
        ap.error("poll/quote-ttl/paper-cash/v7-parent-clip must be positive")
    if not (0 < args.v7_portfolio_cap <= args.v7_late_portfolio_cap <= 1.0):
        ap.error("require 0 < v7-portfolio-cap <= v7-late-portfolio-cap <= 1")
    if not (0 < args.v7_absolute_max_price < 1):
        ap.error("--v7-absolute-max-price must be between 0 and 1")
    if args.v7_base_parents < 1 or args.v7_max_parents < args.v7_base_parents:
        ap.error("invalid V7 parent counts")
    if not (0 <= args.v7_min_overweight_parents <= args.v7_base_parents):
        ap.error("invalid --v7-min-overweight-parents")
    if args.v7_ladder_levels < 1:
        ap.error("--v7-ladder-levels must be >=1")
    if args.v7_hard_gap_clips <= 0:
        ap.error("--v7-hard-gap-clips must be positive")
    return args


async def _run(args: argparse.Namespace) -> int:
    base.Result = V7Result
    base.Engine = V7JanuaryPortfolioEngine
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
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
