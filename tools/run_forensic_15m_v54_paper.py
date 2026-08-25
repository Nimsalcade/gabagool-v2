"""Read-only V5.4 Gabagool 15m paper harness.

V5.4 keeps the V5.3 multi-parent maker architecture but replaces the poll-based
aggressive gate with a parent-opportunity finite-state router:

PASSIVE -> probabilistic onset hazard -> AGGRESSIVE episode -> continuation
hazard -> PASSIVE.

The hazard equations come from the final forensic reconstruction. Converting
those probabilities into actions with deterministic hash-based Bernoulli draws
is an explicit paper experiment, not recovered private Gabagool logic.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v53_paper as v53
from src.forensic_15m import hard_gap_allows
from src.v53_execution import apply_sell_print_to_multi_orders
from src.v54_routing import (
    ContinuationFeatures,
    OnsetFeatures,
    continuation_probability,
    hazard_decision,
    onset_probability,
    walk_asks_vwap,
)


class V54Engine(v53.V53Engine):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._v54_state = "PASSIVE"
        self._v54_episode_side: str | None = None
        self._v54_episode_id = 0
        self._v54_episode_parent_index = 0
        self._v54_episode_start_ts: float | None = None
        self._v54_episode_start_disp: float | None = None
        self._v54_episode_start_pair: float | None = None
        self._v54_last_aggressive_ts: float | None = None
        self._v54_last_aggressive_side: str | None = None
        self._v54_next_continue_p: float | None = None
        self._v54_opportunity_seq = 0
        self._v54_started = 0
        self._v54_continued = 0
        self._v54_terminated = 0

    def _role_for_side(self, side: str) -> str:
        p = self.inv.policy()
        if p.abs_gap <= 1e-9:
            return "BALANCED"
        return "UNDERWEIGHT" if self.inv.underweight == side else "OVERWEIGHT"

    def _role_after_for_side(self, side: str) -> str:
        p = self.inv.policy()
        if p.abs_gap <= 1e-9:
            return "BALANCED"
        if side == "UP":
            return "OVERWEIGHT" if p.up_shares > p.down_shares else "UNDERWEIGHT"
        return "OVERWEIGHT" if p.down_shares > p.up_shares else "UNDERWEIGHT"

    def _end_episode(self, now: float, reason: str) -> None:
        if self._v54_state != "AGGRESSIVE":
            return
        self.emit(
            now,
            "AGGRESSIVE_EPISODE_STOP",
            side=self._v54_episode_side or "",
            reason=(
                f"V5.4 episode={self._v54_episode_id} "
                f"parents={self._v54_episode_parent_index}; {reason}"
            ),
        )
        self._v54_terminated += 1
        self._v54_state = "PASSIVE"
        self._v54_episode_side = None
        self._v54_episode_parent_index = 0
        self._v54_episode_start_ts = None
        self._v54_episode_start_disp = None
        self._v54_episode_start_pair = None
        self._v54_last_aggressive_ts = None
        self._v54_last_aggressive_side = None
        self._v54_next_continue_p = None

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
        if ok and kind == "MAKER_FILL" and self._v54_state == "AGGRESSIVE":
            self._end_episode(now, "passive execution intervened")
        return ok

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        # V5.4 routes only when the planner creates a new signed-parent opportunity.
        return

    def _route_snapshot(
        self,
        now: float,
        side: str,
        intended_price: float,
        up_book: Any,
        down_book: Any,
    ) -> dict[str, Any] | None:
        book = up_book if side == "UP" else down_book
        walked = walk_asks_vwap(base._asks(book), self.clip)
        if walked is None:
            return None
        execution_vwap, worst_price = walked
        same_delta = execution_vwap - float(intended_price)
        opp = "DOWN" if side == "UP" else "UP"
        opp_unmatched_qty = (
            self.pool.unmatched_down if side == "UP" else self.pool.unmatched_up
        )
        opp_unmatched_vwap = self.pool.unmatched_vwap(opp)
        pair_margin = None
        if opp_unmatched_qty > 1e-9 and opp_unmatched_vwap is not None:
            pair_margin = execution_vwap + opp_unmatched_vwap - 1.0
        age = now - self.market.window_start
        return {
            "execution_vwap": execution_vwap,
            "worst_price": worst_price,
            "same_delta": same_delta,
            "pair_margin": pair_margin,
            "age": age,
            "role": self._role_for_side(side),
            "inventory_total_parents": (
                (self.inv.up_shares + self.inv.down_shares) / self.clip
                if self.clip > 0
                else 0.0
            ),
            "opp_unmatched_qty": opp_unmatched_qty,
        }

    def _onset_p(self, snap: dict[str, Any]) -> float:
        return onset_probability(
            OnsetFeatures(
                age_fraction=max(0.0, snap["age"]) / 900.0,
                inventory_total_parents=snap["inventory_total_parents"],
                same_price_delta=snap["same_delta"],
                pair_margin=snap["pair_margin"],
                asset=self.market.asset,
                role=snap["role"],
            )
        )

    def _after_aggressive(
        self,
        now: float,
        side: str,
        snap: dict[str, Any],
        *,
        pre_gap: float,
        pre_underweight: str | None,
        opp_unmatched_before: float,
    ) -> None:
        self._v54_episode_parent_index += 1
        idx = self._v54_episode_parent_index
        if idx == 1:
            self._v54_episode_start_ts = now
            self._v54_episode_start_disp = snap["same_delta"]
            self._v54_episode_start_pair = snap["pair_margin"]

        closed = (
            pre_gap > 1e-9
            and pre_underweight == side
            and self.clip + 1e-9 >= pre_gap
        )
        overshot = self.clip > opp_unmatched_before + 1e-9
        same_second = (
            self._v54_last_aggressive_ts is not None
            and int(now) == int(self._v54_last_aggressive_ts)
        )
        switched = (
            self._v54_last_aggressive_side is not None
            and side != self._v54_last_aggressive_side
        )
        elapsed = (
            0.0
            if self._v54_episode_start_ts is None
            else max(0.0, now - self._v54_episode_start_ts)
        )
        p = continuation_probability(
            ContinuationFeatures(
                run_position=idx,
                elapsed_episode_time=elapsed,
                previous_parent_same_second=same_second,
                previous_transition_switched_side=switched,
                current_same_price_delta=snap["same_delta"],
                current_pair_margin=snap["pair_margin"],
                episode_start_same_price_delta=self._v54_episode_start_disp,
                episode_start_pair_margin=self._v54_episode_start_pair,
                gap_clips_after=(
                    self.inv.deficit / self.clip if self.clip > 0 else 0.0
                ),
                current_parent_closed_deficit=closed,
                current_parent_overshot=overshot,
                market_age=snap["age"],
                asset=self.market.asset,
                inventory_role_after=self._role_after_for_side(side),
                current_side=side,
            )
        )
        self._v54_next_continue_p = p
        self._v54_last_aggressive_ts = now
        self._v54_last_aggressive_side = side
        self.emit(
            now,
            "AGGRESSIVE_CONTINUE_SCORE",
            side=side,
            qty=self.clip,
            price=snap["execution_vwap"],
            reason=(
                f"V5.4 episode={self._v54_episode_id} parent={idx}; "
                f"p_continue={p:.6f}; delta={snap['same_delta']:.6f}; "
                f"pair_margin={snap['pair_margin']}; closed={int(closed)} "
                f"overshot={int(overshot)}"
            ),
        )

    def _execute_aggressive(
        self,
        now: float,
        side: str,
        snap: dict[str, Any],
        *,
        event: str,
        p: float,
        u: float,
    ) -> bool:
        # Safety guard only; not claimed as recovered routing logic.
        if (
            snap["pair_margin"] is not None
            and snap["pair_margin"] > self.args.v54_max_pair_margin
        ):
            return False
        cost = snap["execution_vwap"] * self.clip
        if cost > self.cash - self.reserved() + 1e-9:
            return False
        pre_gap = self.inv.deficit
        pre_underweight = self.inv.underweight
        opp_unmatched_before = snap["opp_unmatched_qty"]
        self.emit(
            now,
            event,
            side=side,
            qty=self.clip,
            price=snap["execution_vwap"],
            cost=cost,
            reason=(
                f"V5.4 episode={self._v54_episode_id}; p={p:.6f}; u={u:.6f}; "
                f"passive_ref_delta={snap['same_delta']:.6f}; "
                f"pair_margin={snap['pair_margin']}; "
                f"worst_ask={snap['worst_price']:.4f}; "
                "deterministic-hazard paper action"
            ),
        )
        ok = super().fill(
            now,
            side,
            self.clip,
            snap["execution_vwap"],
            "TAKER_FILL",
            (
                f"V5.4 {event}; episode={self._v54_episode_id}; "
                f"full scheduled parent; p={p:.6f}; u={u:.6f}; "
                "no deficit clipping"
            ),
        )
        if ok:
            self._after_aggressive(
                now,
                side,
                snap,
                pre_gap=pre_gap,
                pre_underweight=pre_underweight,
                opp_unmatched_before=opp_unmatched_before,
            )
        return ok

    def _route_or_post(
        self,
        side: str,
        px: float,
        now: float,
        up_book: Any,
        down_book: Any,
    ) -> None:
        self._v54_opportunity_seq += 1
        seq = self._v54_opportunity_seq
        snap = self._route_snapshot(now, side, px, up_book, down_book)
        if snap is None:
            return base.Engine.post(
                self, side, px, now, up_book=up_book, down_book=down_book
            )

        if self._v54_state == "AGGRESSIVE" and side == self._v54_episode_side:
            if self._v54_episode_parent_index >= self.args.v54_max_episode_parents:
                self._end_episode(now, "observed-max paper guard")
            else:
                p0 = (
                    self._v54_next_continue_p
                    if self._v54_next_continue_p is not None
                    else 0.0
                )
                go, p, u = hazard_decision(
                    p0,
                    self.market.slug,
                    self._v54_episode_id,
                    self._v54_episode_parent_index + 1,
                    "continue",
                    seed=self.args.v54_seed,
                    scale=self.args.v54_continue_scale,
                )
                if go and self._execute_aggressive(
                    now,
                    side,
                    snap,
                    event="AGGRESSIVE_EPISODE_CONTINUE",
                    p=p,
                    u=u,
                ):
                    self._v54_continued += 1
                    return
                self._end_episode(
                    now, f"continuation hazard stopped p={p:.6f} u={u:.6f}"
                )

        if self._v54_state == "PASSIVE":
            p0 = self._onset_p(snap)
            go, p, u = hazard_decision(
                p0,
                self.market.slug,
                side,
                seq,
                "start",
                seed=self.args.v54_seed,
                scale=self.args.v54_start_scale,
            )
            self.emit(
                now,
                "AGGRESSIVE_ONSET_SCORE",
                side=side,
                qty=self.clip,
                price=snap["execution_vwap"],
                reason=(
                    f"V5.4 p_start={p:.6f}; raw={p0:.6f}; u={u:.6f}; "
                    f"delta={snap['same_delta']:.6f}; "
                    f"pair_margin={snap['pair_margin']}; role={snap['role']} "
                    f"seq={seq}"
                ),
            )
            if go:
                self._v54_episode_id += 1
                self._v54_state = "AGGRESSIVE"
                self._v54_episode_side = side
                self._v54_episode_parent_index = 0
                if self._execute_aggressive(
                    now,
                    side,
                    snap,
                    event="AGGRESSIVE_EPISODE_START",
                    p=p,
                    u=u,
                ):
                    self._v54_started += 1
                    return
                self._end_episode(now, "start selected but execution/safety failed")

        base.Engine.post(
            self, side, px, now, up_book=up_book, down_book=down_book
        )

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
                    now,
                    kind,
                    side=side,
                    order=o,
                    qty=o.remaining,
                    price=o.price,
                    reason=(
                        f"V5.4/V5.3 multi-parent plan; base={current_base}; "
                        f"slots={slots}"
                    ),
                )
        for oid in plan.hysteresis_1t_oids:
            if oid in self._v53_hyst or oid not in self.orders:
                continue
            o = self.orders[oid]
            self.emit(
                now,
                "HYSTERESIS_KEEP_1T",
                side=side,
                order=o,
                qty=o.remaining,
                price=o.price,
                reason=f"V5.4 one-tick FIFO keep; base={current_base}",
            )
            self._v53_hyst.add(oid)
        live = set(plan.hysteresis_1t_oids)
        self._v53_hyst = {
            oid
            for oid in self._v53_hyst
            if oid in live or (oid in self.orders and self.orders[oid].side != side)
        }
        if not replenish:
            return
        for px in plan.replenish_prices:
            if not hard_gap_allows(
                self.inv.policy(),
                side=side,
                shares=self.clip,
                parent_clip=self.clip,
            ):
                break
            same_before = [
                o
                for o in self.orders.values()
                if o.side == side and abs(o.price - px) <= 1e-9
            ]
            before_oids = set(self.orders)
            self._route_or_post(side, px, now, up_book, down_book)
            new = [self.orders[oid] for oid in set(self.orders) - before_oids]
            if (
                new
                and same_before
                and any(abs(o.created - now) <= 1e-6 for o in same_before)
            ):
                if new[0].shadow is not None:
                    new[0].shadow.queue_ahead = 0.0
            if new:
                self.emit(
                    now,
                    "STACK_REPLENISH" if same_before else "VACANCY_REPLENISH",
                    side=side,
                    qty=self.clip,
                    price=px,
                    reason=(
                        f"V5.4 same-cent target; copies_before={len(same_before)} "
                        f"slots={slots}"
                    ),
                )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Read-only Gabagool V5.4 aggressive-episode 15m paper harness"
    )
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--poll", type=float, default=.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0)
    ap.add_argument(
        "--maker-fill-backend", choices=("public_tape",), default="public_tape"
    )
    ap.add_argument("--v53-regime", choices=("oct", "nov", "dec"), default="oct")
    ap.add_argument("--max-combined-vwap", type=float, default=1.01)
    ap.add_argument("--fresh-pair-cap", type=float, default=0.0)
    ap.add_argument("--resolution-timeout", type=float, default=1.0)
    ap.add_argument("--join-current", action="store_true")
    ap.add_argument("--out", default="data/gabagool_15m_live_v54")
    ap.add_argument("--v54-seed", type=int, default=5401)
    ap.add_argument("--v54-start-scale", type=float, default=1.0)
    ap.add_argument("--v54-continue-scale", type=float, default=1.0)
    ap.add_argument("--v54-max-pair-margin", type=float, default=0.10)
    ap.add_argument("--v54-max-episode-parents", type=int, default=19)
    args = ap.parse_args()
    args.taker_mode = "off"
    args.v53_aggressive_mode = "off"
    args.v53_repair_basis_cap = 1.05
    args.v53_aggressive_pair_cap = 1.00
    args.v53_aggressive_headroom = 0.0
    args.v53_aggressive_cooldown = 8.0
    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(
        args.poll,
        args.quote_ttl,
        args.paper_cash,
        args.v54_start_scale,
        args.v54_continue_scale,
    ) <= 0:
        ap.error("positive timing/capital/hazard-scale parameters required")
    if args.v54_max_pair_margin < 0:
        ap.error("--v54-max-pair-margin must be >= 0")
    if args.v54_max_episode_parents < 1:
        ap.error("--v54-max-episode-parents must be >= 1")
    return args


async def _run(args: argparse.Namespace) -> int:
    base.Engine = V54Engine
    base.apply_sell_print_to_orders = apply_sell_print_to_multi_orders
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
