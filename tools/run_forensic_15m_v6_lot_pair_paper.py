"""Read-only V6 lot-pair rolling-ladder paper harness.

V6 deliberately removes the V5.4 probabilistic aggressive router.  It tests the
simpler profitability hypothesis directly against live Polymarket books:

1. When flat, seed the cheaper side with a small passive ladder.
2. As soon as one side is unmatched, cancel the old-side acquisition ladder.
3. Compute the opposite-side maximum price from the unmatched lot cost and the
   configured pair ceiling (default 0.99).
4. Maintain several small passive completion parents below that ceiling.
5. After every atomic public-tape fill group, immediately cancel invalid orders,
   preserve still-valid FIFO orders, and replenish vacancies.
6. If completion orders overshoot, the excess becomes the new unmatched side and
   the ladder automatically flips.

No wallet is loaded and no real order is submitted.  This is a live-book paper
experiment, not a claim that Gabagool used these exact hidden cancellation rules.
"""
from __future__ import annotations

import argparse
import asyncio
import math
from typing import Any

import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v53_paper as v53
from src.forensic_15m import QUOTE_END_AGE_S, QUOTE_START_AGE_S
from src.v53_execution import apply_sell_print_to_multi_orders, parent_clip_for
from src.v6_lot_pair import (
    FifoLotPool,
    completion_ceiling,
    completion_parent_count,
    descending_ladder,
    passive_bid_top,
)


def _best_bid(book: Any) -> tuple[float, float] | None:
    bids = base._bids(book)
    if not bids:
        return None
    return max(bids, key=lambda x: x[0])


class V6LotPairEngine(v53.V53Engine):
    """Profitability-first rolling lot-pair ladder."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.pool = FifoLotPool()
        self._v6_last_state: str | None = None

    def _refresh_clip(self, now: float) -> float:
        self.clip = parent_clip_for(
            now - self.market.window_start,
            asset=self.market.asset,
            regime=self.args.v53_regime,
        )
        return self.clip

    def _state(self) -> tuple[str, str | None]:
        up = self.pool.unmatched_up
        dn = self.pool.unmatched_down
        if up > 1e-9 and dn > 1e-9:
            return "CONFLICT", None
        if up > 1e-9:
            return "COMPLETE", "DOWN"
        if dn > 1e-9:
            return "COMPLETE", "UP"
        return "SEED", None

    def _emit_state(
        self,
        now: float,
        *,
        state: str,
        desired_side: str | None,
        top: float | None,
        desired_count: int,
        reason: str,
    ) -> None:
        sig = (
            f"{state}|{desired_side}|{top}|{desired_count}|"
            f"{self.pool.unmatched_up:.6f}|{self.pool.unmatched_down:.6f}"
        )
        if sig == self._v6_last_state:
            return
        self._v6_last_state = sig
        self.emit(
            now,
            "V6_ROUTE_STATE",
            side=desired_side or "",
            price=top,
            reason=(
                f"{reason}; state={state}; pair_cap={self.args.v6_pair_cap:.4f}; "
                f"uU={self.pool.unmatched_up:.6f}@{self.pool.unmatched_vwap('UP')}; "
                f"uD={self.pool.unmatched_down:.6f}@{self.pool.unmatched_vwap('DOWN')}; "
                f"locked_pnl={self.pool.locked_pnl:.6f}; residual_cost={self.pool.residual_cost:.6f}"
            ),
        )

    def _desired(
        self, now: float, up_book: Any, down_book: Any
    ) -> tuple[str | None, tuple[float, ...], str]:
        if self.clip <= 0:
            return None, (), "clip=0"

        state, completion_side = self._state()
        if state == "CONFLICT":
            return None, (), "both unmatched sides nonzero; conservative stop"

        books = {"UP": up_book, "DOWN": down_book}

        if state == "SEED":
            ua = base._best_ask(up_book)
            da = base._best_ask(down_book)
            if ua is None or da is None:
                return None, (), "missing best ask"

            if ua[0] < da[0] - 1e-12:
                side = "UP"
            elif da[0] < ua[0] - 1e-12:
                side = "DOWN"
            else:
                ub = _best_bid(up_book)
                db = _best_bid(down_book)
                upb = None if ub is None else ub[0]
                dnb = None if db is None else db[0]
                side = (
                    "UP"
                    if dnb is None or (upb is not None and upb <= dnb)
                    else "DOWN"
                )

            book = books[side]
            bid = _best_bid(book)
            ask = base._best_ask(book)
            tick = base._tick(book)
            top = passive_bid_top(
                best_bid=None if bid is None else bid[0],
                best_ask=None if ask is None else ask[0],
                tick=tick,
                max_price=self.args.v6_seed_max_price,
            )
            prices = descending_ladder(
                top,
                tick=tick,
                levels=self.args.v6_seed_parents,
            )
            return (
                side,
                prices,
                (
                    f"seed cheaper side; ua={ua[0]:.4f} da={da[0]:.4f}; "
                    f"seed_max={self.args.v6_seed_max_price:.4f}"
                ),
            )

        assert completion_side in ("UP", "DOWN")
        side = completion_side
        opp = "DOWN" if side == "UP" else "UP"
        worst = self.pool.max_unmatched_price(opp)
        if worst is None:
            return None, (), "completion state without opposite lot"

        book = books[side]
        bid = _best_bid(book)
        ask = base._best_ask(book)
        tick = base._tick(book)
        economic_cap = completion_ceiling(
            pair_cap=self.args.v6_pair_cap,
            worst_opposite_lot_price=worst,
            tick=tick,
        )
        if economic_cap is None:
            return side, (), f"no positive completion ceiling against {opp}@{worst:.4f}"

        top = passive_bid_top(
            best_bid=None if bid is None else bid[0],
            best_ask=None if ask is None else ask[0],
            tick=tick,
            max_price=economic_cap,
        )
        opp_qty = self.pool.unmatched_down if opp == "DOWN" else self.pool.unmatched_up
        count = completion_parent_count(
            unmatched_qty=opp_qty,
            parent_clip=self.clip,
            ladder_levels=self.args.v6_ladder_levels,
            overbook_clips=self.args.v6_overbook_clips,
        )
        prices = descending_ladder(top, tick=tick, levels=count)
        return (
            side,
            prices,
            (
                f"completion ladder against {opp}; worst_lot={worst:.4f}; "
                f"economic_cap={economic_cap:.4f}; unmatched={opp_qty:.4f}; "
                f"overbook={self.args.v6_overbook_clips}"
            ),
        )

    def _cancel(self, now: float, oid: str, event: str, reason: str) -> None:
        o = self.orders.pop(oid, None)
        if o is None:
            return
        self.emit(
            now,
            event,
            side=o.side,
            order=o,
            qty=o.remaining,
            price=o.price,
            reason=reason,
        )

    def _post_one(
        self,
        now: float,
        side: str,
        price: float,
        up_book: Any,
        down_book: Any,
        *,
        state: str,
    ) -> bool:
        before = set(self.orders)
        base.Engine.post(
            self,
            side,
            price,
            now,
            up_book=up_book,
            down_book=down_book,
        )
        new_ids = set(self.orders) - before
        if not new_ids:
            return False
        oid = next(iter(new_ids))
        o = self.orders[oid]
        self.emit(
            now,
            "V6_SEED_POST" if state == "SEED" else "V6_COMPLETION_POST",
            side=side,
            order=o,
            qty=o.remaining,
            price=o.price,
            reason=(
                f"rolling lot-pair ladder; pair_cap={self.args.v6_pair_cap:.4f}"
            ),
        )
        return True

    def _reconcile(self, now: float, up_book: Any, down_book: Any) -> None:
        age = now - self.market.window_start
        if not (QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S):
            return
        self._refresh_clip(now)
        if self.clip <= 0:
            return

        state, _ = self._state()
        desired_side, desired_prices, reason = self._desired(now, up_book, down_book)

        if desired_side is None or not desired_prices:
            for oid in list(self.orders):
                self._cancel(now, oid, "V6_CANCEL_NO_TARGET", reason)
            self._emit_state(
                now,
                state=state,
                desired_side=desired_side,
                top=None,
                desired_count=0,
                reason=reason,
            )
            return

        book = up_book if desired_side == "UP" else down_book
        top = max(desired_prices)
        bottom = min(desired_prices)

        for oid, o in list(self.orders.items()):
            if o.side != desired_side:
                self._cancel(
                    now,
                    oid,
                    "V6_CANCEL_FLIP",
                    (
                        f"router wants {desired_side}; old {o.side} inventory order "
                        f"canceled immediately; {reason}"
                    ),
                )

        candidates = sorted(
            [o for o in self.orders.values() if o.side == desired_side],
            key=lambda o: (-o.price, o.created),
        )

        valid = []
        for o in candidates:
            if o.price > top + 1e-9:
                self._cancel(
                    now,
                    o.oid,
                    "V6_CANCEL_UNSAFE",
                    f"price={o.price:.4f} > safe_top={top:.4f}; {reason}",
                )
                continue
            if o.price < bottom - 1e-9:
                self._cancel(
                    now,
                    o.oid,
                    "V6_CANCEL_STALE",
                    f"price={o.price:.4f} < active_bottom={bottom:.4f}; {reason}",
                )
                continue
            valid.append(o)

        desired_count = len(desired_prices)
        if len(valid) > desired_count:
            keep = sorted(valid, key=lambda o: (o.created, -o.price))[:desired_count]
            keep_ids = {o.oid for o in keep}
            for o in valid:
                if o.oid not in keep_ids:
                    self._cancel(
                        now,
                        o.oid,
                        "V6_CANCEL_EXCESS",
                        f"valid parents>{desired_count}; {reason}",
                    )
            valid = keep

        counts: dict[float, int] = {}
        for o in valid:
            key = round(o.price, 10)
            counts[key] = counts.get(key, 0) + 1

        missing = max(0, desired_count - len(valid))
        if missing:
            for px in desired_prices:
                if missing <= 0:
                    break
                key = round(px, 10)
                if counts.get(key, 0) >= 1:
                    continue
                if self._post_one(
                    now,
                    desired_side,
                    px,
                    up_book,
                    down_book,
                    state=state,
                ):
                    counts[key] = counts.get(key, 0) + 1
                    missing -= 1

        if missing > 0:
            for px in desired_prices:
                if missing <= 0:
                    break
                if self._post_one(
                    now,
                    desired_side,
                    px,
                    up_book,
                    down_book,
                    state=state,
                ):
                    missing -= 1

        self._emit_state(
            now,
            state=state,
            desired_side=desired_side,
            top=top,
            desired_count=desired_count,
            reason=reason,
        )

    def expire(self, now: float, up_book: Any = None, down_book: Any = None) -> None:
        for o in self.orders.values():
            if now >= o.expires:
                o.expires = now + self.args.quote_ttl

    def reconcile_desired_after_fills(
        self, now: float, up_book: Any, down_book: Any
    ) -> None:
        self._reconcile(now, up_book, down_book)

    def renew(self, now: float, up_book: Any, down_book: Any) -> None:
        self._reconcile(now, up_book, down_book)

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        return None

    async def run(self, client: Any):
        result = await super().run(client)
        locked = self.pool.locked_pnl
        residual = self.pool.residual_cost
        set_vwap = self.pool.completed_vwap
        self.emit(
            __import__("time").time(),
            "V6_WINDOW_METRICS",
            reason=(
                f"completed={self.pool.completed_qty:.6f}; "
                f"set_vwap={set_vwap}; locked_set_pnl={locked:.6f}; "
                f"residual_cost={residual:.6f}; "
                f"conservative_net={locked-residual:.6f}; "
                f"pair_cap={self.args.v6_pair_cap:.4f}"
            ),
        )
        print(
            f"V6 {self.market.asset.upper()} | sets={self.pool.completed_qty:.2f} "
            f"set_vwap={(set_vwap or 0):.4f} locked=${locked:.4f} "
            f"residual=${residual:.4f} conservative=${locked-residual:.4f}"
        )
        return result


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Read-only V6 rolling lot-pair live-book paper harness"
    )
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=96)
    ap.add_argument("--poll", type=float, default=0.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0)
    ap.add_argument("--maker-fill-backend", choices=("public_tape",), default="public_tape")
    ap.add_argument("--v53-regime", choices=("oct", "nov", "dec"), default="oct")
    ap.add_argument("--v6-pair-cap", type=float, default=0.99)
    ap.add_argument("--v6-seed-max-price", type=float, default=0.49)
    ap.add_argument("--v6-seed-parents", type=int, default=4)
    ap.add_argument("--v6-ladder-levels", type=int, default=4)
    ap.add_argument("--v6-overbook-clips", type=int, default=2)
    ap.add_argument("--resolution-timeout", type=float, default=1.0)
    ap.add_argument("--join-current", action="store_true")
    ap.add_argument("--out", default="data/gabagool_v6_lot_pair_validation")
    args = ap.parse_args()

    args.taker_mode = "off"
    args.max_combined_vwap = 1.01
    args.fresh_pair_cap = 0.0

    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(args.poll, args.quote_ttl, args.paper_cash) <= 0:
        ap.error("poll/quote-ttl/paper-cash must be positive")
    if not (0.50 <= args.v6_pair_cap < 1.0):
        ap.error("--v6-pair-cap must be in [0.50, 1.00)")
    if not (0.01 <= args.v6_seed_max_price < args.v6_pair_cap):
        ap.error("--v6-seed-max-price must be positive and below pair cap")
    if min(args.v6_seed_parents, args.v6_ladder_levels) < 1:
        ap.error("seed parents and ladder levels must be >= 1")
    if args.v6_overbook_clips < 0:
        ap.error("--v6-overbook-clips must be >= 0")
    return args


async def _run(args: argparse.Namespace) -> int:
    base.Engine = V6LotPairEngine
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
