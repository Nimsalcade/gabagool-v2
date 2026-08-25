"""Read-only V5.3 Gabagool 15m paper harness.

Adds evidence-backed same-cent parent stacks, signed-parent regime profiles and
full-parent aggressive BUY routing on top of V5.2a. Hidden stack-count and taker
trigger formulas remain explicit paper-model surfaces, not recovered source.
"""
from __future__ import annotations

import argparse
import asyncio
import math
import time
from typing import Any

import tools.run_forensic_15m_paper as base
from src.forensic_15m import (
    QUOTE_END_AGE_S,
    QUOTE_START_AGE_S,
    complementary_base_bid,
    desired_layer_count,
    hard_gap_allows,
)
from src.v53_execution import (
    aggressive_candidate,
    apply_sell_print_to_multi_orders,
    parent_clip_for,
    plan_multi_parent_side,
    stack_targets,
)


class V53Engine(base.Engine):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v53_last_aggressive = {"UP": -math.inf, "DOWN": -math.inf}
        self._v53_hyst: set[str] = set()

    def _refresh_clip(self, now: float) -> float:
        self.clip = parent_clip_for(
            now - self.market.window_start,
            asset=self.market.asset,
            regime=self.args.v53_regime,
        )
        return self.clip

    def _plan(self, now: float, side: str, up_book: Any, down_book: Any):
        ua, da = base._best_ask(up_book), base._best_ask(down_book)
        p = self.inv.policy()
        if side == "UP":
            own, opp, tick = ua, da, base._tick(up_book)
        else:
            own, opp, tick = da, ua, base._tick(down_book)
        if own is None or opp is None or self.clip <= 0:
            current_base, n = None, 0
        else:
            current_base = complementary_base_bid(
                own_best_ask=own[0], opposite_best_ask=opp[0], tick=tick
            )
            n = desired_layer_count(p, side, self.clip)
            # Preserve the parked V5 fresh-pair override for NEW logical layers.
            n = min(n, len(self.desired_new_exposure(now, up_book, down_book)[side]))
        slots = stack_targets(
            p, side=side, parent_clip=self.clip, logical_layers=n,
            regime=self.args.v53_regime,
        )
        active = [(o.oid, o.price, o.created) for o in self.orders.values() if o.side == side]
        plan = plan_multi_parent_side(
            orders=active, current_base=current_base, desired_layers=n,
            stack_slots=slots, tick=tick,
        )
        return plan, current_base, slots

    def _apply_plan(
        self, now: float, side: str, plan: Any, current_base: float | None,
        slots: tuple[int, ...], up_book: Any, down_book: Any, *, replenish: bool,
    ) -> None:
        for kind, oids in (
            ("REPRICE_BACKOFF_2T", plan.backoff_2t_oids),
            ("REPRICE_BACKOFF_3PLUS", plan.backoff_3plus_oids),
            ("INVENTORY_LAYER_DROP", plan.drop_oids),
        ):
            for oid in oids:
                o = self.orders.pop(oid, None)
                if o is None:
                    continue
                self._v53_hyst.discard(oid)
                self.emit(
                    now, kind, side=side, order=o, qty=o.remaining, price=o.price,
                    reason=f"V5.3 multi-parent plan; base={current_base}; slots={slots}",
                )

        for oid in plan.hysteresis_1t_oids:
            if oid in self._v53_hyst or oid not in self.orders:
                continue
            o = self.orders[oid]
            self.emit(
                now, "HYSTERESIS_KEEP_1T", side=side, order=o,
                qty=o.remaining, price=o.price,
                reason=f"V5.3 one-tick FIFO keep; base={current_base}",
            )
            self._v53_hyst.add(oid)
        live = set(plan.hysteresis_1t_oids)
        self._v53_hyst = {
            oid for oid in self._v53_hyst
            if oid in live or (oid in self.orders and self.orders[oid].side != side)
        }

        if not replenish:
            return
        for px in plan.replenish_prices:
            if not hard_gap_allows(
                self.inv.policy(), side=side, shares=self.clip, parent_clip=self.clip
            ):
                break
            same_before = [
                o for o in self.orders.values()
                if o.side == side and abs(o.price - px) <= 1e-9
            ]
            before_oids = set(self.orders)
            super().post(side, px, now, up_book=up_book, down_book=down_book)
            new = [self.orders[oid] for oid in set(self.orders) - before_oids]
            if new and same_before and any(abs(o.created - now) <= 1e-6 for o in same_before):
                # Only the first new copy in a reconciliation burst carries the
                # external visible queue; later own copies are FIFO behind it.
                if new[0].shadow is not None:
                    new[0].shadow.queue_ahead = 0.0
            if new:
                self.emit(
                    now,
                    "STACK_REPLENISH" if same_before else "VACANCY_REPLENISH",
                    side=side, qty=self.clip, price=px,
                    reason=f"V5.3 same-cent target; copies_before={len(same_before)} slots={slots}",
                )

    def apply_sticky_ladder(self, now: float, up_book: Any, down_book: Any, *, replenish: bool) -> None:
        if up_book is None or down_book is None:
            return
        self._refresh_clip(now)
        for side in ("UP", "DOWN"):
            plan, current_base, slots = self._plan(now, side, up_book, down_book)
            self._apply_plan(
                now, side, plan, current_base, slots, up_book, down_book,
                replenish=replenish,
            )

    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        self._refresh_clip(now)
        if not self._use_tape() or up_book is None or down_book is None:
            return super().expire(now, up_book, down_book)
        self.apply_sticky_ladder(now, up_book, down_book, replenish=False)
        for o in list(self.orders.values()):
            if now < o.expires:
                continue
            o.expires = now + self.args.quote_ttl
            self.emit(
                now, "QUEUE_KEEP", side=o.side, order=o, qty=o.remaining, price=o.price,
                reason="V5.3 same-cent/FIFO keepalive",
            )

    def reconcile_desired_after_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        self._refresh_clip(now)
        self.apply_sticky_ladder(now, up_book, down_book, replenish=True)

    def renew(self, now: float, up_book: Any, down_book: Any) -> None:
        self._refresh_clip(now)
        self.apply_sticky_ladder(now, up_book, down_book, replenish=True)

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        """Route one normal parent aggressively; never deficit-clip it."""
        self._refresh_clip(now)
        if self.args.v53_aggressive_mode == "off" or self.clip <= 0:
            return
        age = now - self.market.window_start
        if not (QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S):
            return

        books = {"UP": up_book, "DOWN": down_book}
        choices = []
        inv = self.inv.policy()
        for side in ("UP", "DOWN"):
            if now - self._v53_last_aggressive[side] < self.args.v53_aggressive_cooldown:
                continue
            if not hard_gap_allows(inv, side=side, shares=self.clip, parent_clip=self.clip):
                continue
            opp = "DOWN" if side == "UP" else "UP"
            own_asks, opp_asks = base._asks(books[side]), base._asks(books[opp])
            if not own_asks or not opp_asks:
                continue
            opp_q = self.pool.unmatched_up if opp == "UP" else self.pool.unmatched_down
            c = aggressive_candidate(
                side=side, shares=self.clip, own_asks=own_asks, opposite_asks=opp_asks,
                opposite_unmatched_qty=opp_q,
                opposite_unmatched_vwap=self.pool.unmatched_vwap(opp),
                repair_basis_cap=self.args.v53_repair_basis_cap,
                fresh_pair_cap=self.args.v53_aggressive_pair_cap,
                headroom=self.args.v53_aggressive_headroom,
            )
            if c is not None:
                choices.append((c.score - (0.001 if self.inv.underweight == side else 0.0), c))
        if not choices:
            return
        _, c = min(choices, key=lambda x: x[0])
        cost = c.execution_vwap * c.shares
        if cost > self.cash - self.reserved() + 1e-9:
            return
        if self.fill(
            now, c.side, c.shares, c.execution_vwap, "TAKER_FILL",
            (
                f"V5.3 {c.reason}; parent={c.shares:.3f}; signed_limit~{c.limit_price:.4f}; "
                f"repair_basis={c.repair_basis}; fresh_pair={c.fresh_pair_basis}; "
                f"closes_unmatched={c.closes_unmatched:.3f}; no-deficit-clipping"
            ),
        ):
            self._v53_last_aggressive[c.side] = now


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-only Gabagool V5.3 multi-parent 15m paper harness")
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--poll", type=float, default=.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0)
    ap.add_argument("--maker-fill-backend", choices=("public_tape",), default="public_tape")
    ap.add_argument("--v53-regime", choices=("oct", "nov", "dec"), default="oct")
    ap.add_argument("--v53-aggressive-mode", choices=("evidence", "off"), default="evidence")
    ap.add_argument("--v53-repair-basis-cap", type=float, default=1.05)
    ap.add_argument("--v53-aggressive-pair-cap", type=float, default=1.00)
    ap.add_argument("--v53-aggressive-headroom", type=float, default=0.0)
    ap.add_argument("--v53-aggressive-cooldown", type=float, default=8.0)
    ap.add_argument("--max-combined-vwap", type=float, default=1.01)
    ap.add_argument("--fresh-pair-cap", type=float, default=0.0)
    ap.add_argument("--resolution-timeout", type=float, default=1.0)
    ap.add_argument("--join-current", action="store_true")
    ap.add_argument("--out", default="data/gabagool_15m_live_v53")
    args = ap.parse_args()
    args.taker_mode = "v53" if args.v53_aggressive_mode == "evidence" else "off"
    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(
        args.poll, args.quote_ttl, args.paper_cash, args.v53_repair_basis_cap,
        args.v53_aggressive_pair_cap, args.v53_aggressive_cooldown,
    ) <= 0:
        ap.error("positive timing/capital/basis parameters required")
    if args.v53_aggressive_headroom < 0:
        ap.error("--v53-aggressive-headroom must be >= 0")
    return args


async def _run(args: argparse.Namespace) -> int:
    # Process-local monkeypatches keep the V5.2a branch code untouched.
    base.Engine = V53Engine
    base.apply_sell_print_to_orders = apply_sell_print_to_multi_orders

    original_choose = base.choose_market

    async def choose_clean(client: Any, asset: str, *, clean_start: bool) -> Any:
        return await original_choose(client, asset, clean_start=(not args.join_current))

    base.choose_market = choose_clean

    # Resolution never blocks the strategy scheduler.
    async def no_blocking_resolution(_slug: str, _timeout_s: float) -> None:
        return None

    base.gamma_winner = no_blocking_resolution
    return await base.amain(args)


def main() -> None:
    raise SystemExit(asyncio.run(_run(parse_args())))


if __name__ == "__main__":
    main()
