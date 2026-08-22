"""Read-only Gabagool forensic V2 validator.

V1 successfully reproduced the observed underweight-side tendency, but a live
smoke test falsified its quote-only budget model: a 0.985 intended quote-pair
budget still accumulated a 1.0591 pooled basis because stale/current-mid quotes
were not constrained against already-filled inventory.

V2 keeps the evidence-backed structure and changes one thing: every resting bid
is constrained by the projected pooled inventory economics. The soft quote
budget remains 0.985; the hard projected pooled pair ceiling is 0.995, matching
the forensic high-.98/low-.99 target band. No taker repair and no mid-market
merge are introduced.

This remains a proof harness, not a PnL optimizer. It never requests a wallet,
private key, signature, order, merge, or redeem.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from tools import gabagool_forensic_v1 as v1
from tools.metamask_10session_strategy_observer import (
    _buy_execution,
    _d,
    _floor_tick,
    _iso,
)

ASSET = v1.ASSET
DURATION_S = v1.DURATION_S

DEFAULT_SESSIONS = 50
DEFAULT_POLL_S = v1.DEFAULT_POLL_S
DEFAULT_CLIP = v1.DEFAULT_CLIP
DEFAULT_QUOTE_PAIR_TARGET = v1.DEFAULT_QUOTE_PAIR_TARGET
DEFAULT_INVENTORY_PAIR_MAX = Decimal("0.995")
DEFAULT_REQUOTE_S = v1.DEFAULT_REQUOTE_S
DEFAULT_MAX_GAP = v1.DEFAULT_MAX_GAP
DEFAULT_STOP_NEW_SEED_S = v1.DEFAULT_STOP_NEW_SEED_S
DEFAULT_SKEW_TICKS = v1.DEFAULT_SKEW_TICKS


class InventoryAwareForensicEngine(v1.ForensicMarketEngine):
    """V1 engine with an inventory-aware maker quote guard.

    The V1 failure mode was not the two-sided/maker hypothesis itself. The
    failure was pricing each fresh quote from current mids without enforcing
    what that quote would do to the cost basis of inventory already held.

    V2 therefore:
      1. starts from the same V1 mid-proportional quote hypothesis;
      2. clamps each quote so a hypothetical fill cannot push pooled pair basis
         above inventory_pair_max;
      3. checks the joint case where both resting quotes fill together;
      4. after a one-sided observed fill, cancels the opposite stale quote and
         immediately lets the normal reconciliation pass rebuild it from the
         new inventory state.
    """

    def __init__(self, *, inventory_pair_max: Decimal, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.inventory_pair_max = inventory_pair_max
        self.unsafe_joint_crosses = 0
        self.post_fill_reprices = 0

    def _projected_pair_basis(
        self,
        additions: dict[str, tuple[Decimal, Decimal]],
    ) -> Decimal | None:
        """Return pooled UP avg + DOWN avg after hypothetical fills.

        additions maps side -> (qty, price).  The same pooled-average accounting
        is used by V1's harvest summary, so the guard and the scorecard evaluate
        the same economic object.
        """
        up_q = self.inv.up_shares
        up_c = self.inv.up_cost
        down_q = self.inv.down_shares
        down_c = self.inv.down_cost

        for side, (qty, price) in additions.items():
            if side == "UP":
                up_q += qty
                up_c += qty * price
            else:
                down_q += qty
                down_c += qty * price

        if up_q <= 0 or down_q <= 0:
            return None
        return (up_c / up_q) + (down_c / down_q)

    def _clamp_single(
        self,
        side: str,
        price: Decimal | None,
        tick: Decimal,
    ) -> Decimal | None:
        if price is None or price <= 0:
            return None

        candidate = _floor_tick(price, tick)
        while candidate > 0:
            projected = self._projected_pair_basis(
                {side: (self.clip, candidate)}
            )
            # With no opposite inventory yet there is no paired basis to guard;
            # the joint quote check below still protects the two-new-fill case.
            if projected is None or projected <= self.inventory_pair_max:
                return candidate
            candidate = _floor_tick(candidate - tick, tick)
        return None

    def _clamp_joint(
        self,
        up_price: Decimal | None,
        down_price: Decimal | None,
        up_tick: Decimal,
        down_tick: Decimal,
    ) -> tuple[Decimal | None, Decimal | None]:
        if up_price is None or down_price is None:
            return up_price, down_price

        up_q = up_price
        down_q = down_price
        under = self.inv.underweight()

        # If both currently resting bids were to fill in the same sampled move,
        # require the resulting pooled basis to remain inside the hard ceiling.
        for _ in range(250):
            projected = self._projected_pair_basis(
                {
                    "UP": (self.clip, up_q),
                    "DOWN": (self.clip, down_q),
                }
            )
            if projected is None or projected <= self.inventory_pair_max:
                return up_q, down_q

            # Preserve the underweight-side quote where possible. Lowering the
            # heavy-side bid both improves economics and makes further imbalance
            # less likely. If balanced, reduce the more expensive quote first.
            if under == "UP" and down_q > 0:
                down_q = _floor_tick(down_q - down_tick, down_tick)
            elif under == "DOWN" and up_q > 0:
                up_q = _floor_tick(up_q - up_tick, up_tick)
            elif up_q >= down_q and up_q > 0:
                up_q = _floor_tick(up_q - up_tick, up_tick)
            elif down_q > 0:
                down_q = _floor_tick(down_q - down_tick, down_tick)

            if up_q <= 0:
                up_q = Decimal(0)
            if down_q <= 0:
                down_q = Decimal(0)
            if up_q <= 0 or down_q <= 0:
                break

        return (
            up_q if up_q > 0 else None,
            down_q if down_q > 0 else None,
        )

    def _desired_quotes(self, up_book: Any, down_book: Any, now: float) -> dict[str, Decimal | None]:
        desired = super()._desired_quotes(up_book, down_book, now)

        up_tick = _d(getattr(up_book, "tick_size", None)) or Decimal("0.01")
        down_tick = _d(getattr(down_book, "tick_size", None)) or Decimal("0.01")

        up_price = self._clamp_single("UP", desired.get("UP"), up_tick)
        down_price = self._clamp_single("DOWN", desired.get("DOWN"), down_tick)
        up_price, down_price = self._clamp_joint(
            up_price,
            down_price,
            up_tick,
            down_tick,
        )

        return {"UP": up_price, "DOWN": down_price}

    def _detect_resting_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        """Detect fills, then invalidate stale opposite quote after one-sided fill.

        If both orders are already executable in the same snapshot, both are
        credited: a real resting maker could have received both fills before a
        cancellation reached the venue. The joint quote guard is designed to
        make that case economically safe at quote time.
        """
        crossed: list[tuple[str, Any]] = []
        books = {"UP": up_book, "DOWN": down_book}

        for side in ("UP", "DOWN"):
            order = self.orders.get(side)
            if order is None:
                continue
            execution = _buy_execution(books[side], order.size, order.price)
            if execution.full:
                crossed.append((side, order))

        if not crossed:
            return

        if len(crossed) == 2:
            projected = self._projected_pair_basis(
                {
                    side: (order.size, order.price)
                    for side, order in crossed
                }
            )
            if projected is not None and projected > self.inventory_pair_max:
                # Do not hide an unsafe paper event. If it ever happens, credit
                # both fills and count it as a validator failure signal.
                self.unsafe_joint_crosses += 1
                print(
                    f"UNSAFE      simultaneous resting crosses project pair={projected:.6f} "
                    f"> {self.inventory_pair_max}"
                )

            # Prefer the currently underweight side first only for deterministic
            # inventory-state logging. Both fills have the same sampled timestamp.
            under = self.inv.underweight()
            crossed.sort(key=lambda item: 0 if item[0] == under else 1)
            for side, order in crossed:
                self.orders[side] = None
                self._record_fill(
                    side,
                    order.size,
                    order.price,
                    now,
                    f"resting bid crossed; joint snapshot; projected_pair={projected}",
                )
            return

        side, order = crossed[0]
        self.orders[side] = None
        self._record_fill(
            side,
            order.size,
            order.price,
            now,
            "resting bid crossed; force inventory-aware opposite reprice",
        )

        other = "DOWN" if side == "UP" else "UP"
        if self.orders.get(other) is not None:
            self._cancel(other, now, "post-fill inventory-aware reprice", quiet=True)
            self.post_fill_reprices += 1

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        summary = await super().run(client)
        summary["inventory_pair_max"] = str(self.inventory_pair_max)
        summary["unsafe_joint_crosses"] = self.unsafe_joint_crosses
        summary["post_fill_reprices"] = self.post_fill_reprices
        return summary


async def _resolve_wait(client: AsyncPublicClient, target_start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, target_start)
        if market is not None:
            return market
        await asyncio.sleep(1.0)


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    activity_path = run_dir / "activity.csv"
    summary_path = run_dir / "summary.json"

    print("READ ONLY   no wallet, no key, no orders, no real merge/redeem")
    print("OBJECTIVE   falsify/confirm Gabagool maker + inventory hypothesis")
    print("VERSION     forensic V2: inventory-aware quote guard")
    print(
        f"PLAN        {args.sessions} consecutive BTC 5m markets | clip={args.clip}sh "
        f"| poll={args.poll:.2f}s"
    )
    print(
        f"QUOTING     soft combined target={args.quote_pair_target} | "
        f"hard projected pair max={args.inventory_pair_max} | skew={args.skew_ticks} ticks"
    )
    print(
        f"INVENTORY   max gap={args.max_gap}sh | post-fill opposite reprice | "
        f"no mid-market merge | stop new balanced seeds T-{args.stop_new_seed:.0f}s"
    )
    print("TAKER       disabled")
    print(f"OUTPUT      {run_dir}")

    client = AsyncPublicClient()
    summaries: list[dict[str, Any]] = []
    start = window_start_epoch(DURATION_S, time.time()) + DURATION_S

    try:
        with activity_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=v1.ACTIVITY_FIELDS)
            writer.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(f"WAIT        session {idx + 1}/{args.sessions} starts in {wait:.1f}s")
                    await asyncio.sleep(wait)

                market = await _resolve_wait(client, target)
                engine = InventoryAwareForensicEngine(
                    session_no=idx + 1,
                    market=market,
                    writer=writer,
                    clip=args.clip,
                    poll_s=args.poll,
                    quote_pair_target=args.quote_pair_target,
                    inventory_pair_max=args.inventory_pair_max,
                    requote_s=args.requote,
                    max_gap=args.max_gap,
                    stop_new_seed_s=args.stop_new_seed,
                    skew_ticks=args.skew_ticks,
                )
                summaries.append(await engine.run(client))
                fh.flush()

        aggregate = v1._aggregate(summaries)
        unsafe = sum(int(s.get("unsafe_joint_crosses", 0)) for s in summaries)
        reprices = sum(int(s.get("post_fill_reprices", 0)) for s in summaries)

        result = {
            "created_utc": _iso(),
            "version": "gabagool_forensic_v2",
            "config": {
                "asset": ASSET,
                "duration_s": DURATION_S,
                "sessions": args.sessions,
                "poll_s": args.poll,
                "clip": str(args.clip),
                "quote_pair_target": str(args.quote_pair_target),
                "inventory_pair_max": str(args.inventory_pair_max),
                "requote_s": args.requote,
                "max_gap": str(args.max_gap),
                "stop_new_seed_s": args.stop_new_seed,
                "skew_ticks": args.skew_ticks,
                "taker_repair": False,
                "mid_market_merge": False,
                "post_fill_opposite_reprice": True,
            },
            "aggregate": {
                **aggregate,
                "unsafe_joint_crosses": unsafe,
                "post_fill_reprices": reprices,
            },
            "sessions": summaries,
            "files": {
                "activity_csv": str(activity_path),
                "summary_json": str(summary_path),
            },
            "interpretation": (
                "Read-only forward paper proof. V2 adds a hard projected pooled pair-basis "
                "guard to the V1 maker/inventory hypothesis and cancels the opposite stale "
                "quote after a one-sided observed fill. Resting maker fills still require a "
                "later sampled ask book to show the configured clip executable at or below "
                "the bid. Queue priority, maker rebates, taker fees, and real transaction "
                "latency are not modeled. Matched inventory is harvested only after the window."
            ),
        }
        summary_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

        print("\n" + "=" * 96)
        print("GABAGOOL FORENSIC V2 SUMMARY")
        print(f"SESSIONS    {aggregate['sessions']}")
        print(
            f"TWO-SIDED   {aggregate['markets_buying_both_sides']}/"
            f"{aggregate['sessions']} ({aggregate['both_sides_rate']:.1%})"
        )
        print(f"FILLS       {aggregate['total_maker_proxy_fills']}")
        print(f"MATCHED     {aggregate['total_matched_shares']} shares")
        print(f"SPEND       ${Decimal(aggregate['total_gross_spend']):.6f}")
        print(f"HARVEST PNL ${Decimal(aggregate['total_harvest_gross_pnl']):.6f} gross")
        print(f"FIRST FILL  median={aggregate['median_first_fill_age_s']}s | chain ref≈14s")
        print(f"LAST FILL   median={aggregate['median_last_fill_age_s']}s | chain ref≈205s")
        print(
            f"PAIR BASIS  weighted={aggregate['pair_weighted_combined_basis']} "
            f"| target band≈0.985-0.995"
        )
        print(
            f"UNDERWEIGHT aggregate={aggregate['aggregate_underweight_fill_rate']} "
            f"| chain ref≈0.68"
        )
        print(
            f"OPP SUM     session-median={aggregate['median_session_opposite_sum']} "
            f"| chain ref≈0.99"
        )
        print(f"UNSAFE      joint-cross violations={unsafe} | target=0")
        print(f"REPRICES    post-fill opposite cancels={reprices}")
        print(f"ACTIVITY    {activity_path}")
        print(f"SUMMARY     {summary_path}")
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only inventory-aware Gabagool forensic V2 validator"
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    parser.add_argument("--clip", type=Decimal, default=DEFAULT_CLIP)
    parser.add_argument(
        "--quote-pair-target",
        type=Decimal,
        default=DEFAULT_QUOTE_PAIR_TARGET,
    )
    parser.add_argument(
        "--inventory-pair-max",
        type=Decimal,
        default=DEFAULT_INVENTORY_PAIR_MAX,
    )
    parser.add_argument("--requote", type=float, default=DEFAULT_REQUOTE_S)
    parser.add_argument("--max-gap", type=Decimal, default=DEFAULT_MAX_GAP)
    parser.add_argument(
        "--stop-new-seed",
        type=float,
        default=DEFAULT_STOP_NEW_SEED_S,
    )
    parser.add_argument("--skew-ticks", type=int, default=DEFAULT_SKEW_TICKS)
    parser.add_argument("--output", default="data/gabagool_forensic_v2")
    args = parser.parse_args()

    if not (1 <= args.sessions <= 200):
        parser.error("--sessions must be 1..200")
    if not (0.1 <= args.poll <= 5.0):
        parser.error("--poll must be 0.1..5.0")
    if args.clip <= 0:
        parser.error("--clip must be positive")
    if not (Decimal("0.95") <= args.quote_pair_target < Decimal("1")):
        parser.error("--quote-pair-target must be in [0.95, 1)")
    if not (args.quote_pair_target <= args.inventory_pair_max < Decimal("1")):
        parser.error("--inventory-pair-max must be >= quote-pair-target and < 1")
    if args.requote <= 0:
        parser.error("--requote must be positive")
    if args.max_gap < args.clip:
        parser.error("--max-gap must be >= clip")
    if args.stop_new_seed < 0 or args.stop_new_seed >= DURATION_S:
        parser.error("--stop-new-seed must be in [0, 300)")
    if args.skew_ticks < 0 or args.skew_ticks > 10:
        parser.error("--skew-ticks must be 0..10")

    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
