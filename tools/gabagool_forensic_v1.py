"""Read-only forward Gabagool forensic strategy validator.

Purpose
-------
Test a falsifiable, evidence-derived hypothesis about the observable strategy:

- continuously maintain small maker BUY quotes on both outcomes;
- keep intended paired quote budget near a fixed combined target;
- allow temporary inventory imbalance;
- bias subsequent quotes toward the underweight side;
- do NOT merge mid-market;
- batch-harvest matched inventory only after the market window;
- never use a wallet, private key, real order, signature, merge, or redeem.

This is a proof harness, not a PnL optimizer. Parameters must remain frozen for a
validation run. Maker fills are conservative paper proxies: a resting bid is
counted only when a later sampled ask book shows the full configured clip
executable at or below that resting bid. Queue priority is not modeled, so a
paper fill is not proof that a live maker order would have filled.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from tools.metamask_10session_strategy_observer import (
    _best,
    _books,
    _buy_execution,
    _d,
    _floor_tick,
    _iso,
)

ASSET = "btc"
DURATION_S = 300

DEFAULT_SESSIONS = 50
DEFAULT_POLL_S = 0.50
DEFAULT_CLIP = Decimal("10")
DEFAULT_QUOTE_PAIR_TARGET = Decimal("0.985")
DEFAULT_REQUOTE_S = 3.0
DEFAULT_MAX_GAP = Decimal("40")
DEFAULT_STOP_NEW_SEED_S = 45.0
DEFAULT_SKEW_TICKS = 2

# Chain-forensic reference fingerprints. These are comparison targets only;
# they are not used to make quote/fill decisions.
REF_FIRST_FILL_MEDIAN_S = 14.0
REF_LAST_FILL_MEDIAN_S = 205.0
REF_OPPOSITE_SUM_MEDIAN = 0.99
REF_OPPOSITE_SUM_MEAN = 0.985
REF_UNDERWEIGHT_TENDENCY = 0.68
REF_PAIR_BASIS_LOW = 0.985
REF_PAIR_BASIS_HIGH = 0.995

ACTIVITY_FIELDS = [
    "utc",
    "session",
    "market",
    "age_s",
    "event",
    "side",
    "qty",
    "price",
    "cost",
    "reason",
    "up_shares",
    "up_avg",
    "down_shares",
    "down_avg",
    "gap",
    "matched",
    "pair_basis",
    "opposite_sum",
    "opposite_lag_s",
    "gross_spend",
]


@dataclass
class RestingOrder:
    side: str
    price: Decimal
    size: Decimal
    created_ts: float
    reason: str


@dataclass
class Inventory:
    up_shares: Decimal = Decimal(0)
    up_cost: Decimal = Decimal(0)
    down_shares: Decimal = Decimal(0)
    down_cost: Decimal = Decimal(0)

    def shares(self, side: str) -> Decimal:
        return self.up_shares if side == "UP" else self.down_shares

    def cost(self, side: str) -> Decimal:
        return self.up_cost if side == "UP" else self.down_cost

    def avg(self, side: str) -> Decimal | None:
        qty = self.shares(side)
        return None if qty <= 0 else self.cost(side) / qty

    def add(self, side: str, qty: Decimal, cost: Decimal) -> None:
        if side == "UP":
            self.up_shares += qty
            self.up_cost += cost
        else:
            self.down_shares += qty
            self.down_cost += cost

    def matched(self) -> Decimal:
        return min(self.up_shares, self.down_shares)

    def gap_signed(self) -> Decimal:
        return self.up_shares - self.down_shares

    def gap(self) -> Decimal:
        return abs(self.gap_signed())

    def underweight(self) -> str | None:
        gap = self.gap_signed()
        if gap > 0:
            return "DOWN"
        if gap < 0:
            return "UP"
        return None

    def heavy(self) -> str | None:
        gap = self.gap_signed()
        if gap > 0:
            return "UP"
        if gap < 0:
            return "DOWN"
        return None

    def pair_basis(self) -> Decimal | None:
        if self.matched() <= 0:
            return None
        up = self.avg("UP")
        down = self.avg("DOWN")
        if up is None or down is None:
            return None
        return up + down


class ForensicMarketEngine:
    def __init__(
        self,
        *,
        session_no: int,
        market: Any,
        writer: csv.DictWriter,
        clip: Decimal,
        poll_s: float,
        quote_pair_target: Decimal,
        requote_s: float,
        max_gap: Decimal,
        stop_new_seed_s: float,
        skew_ticks: int,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.writer = writer
        self.clip = clip
        self.poll_s = poll_s
        self.quote_pair_target = quote_pair_target
        self.requote_s = requote_s
        self.max_gap = max_gap
        self.stop_new_seed_s = stop_new_seed_s
        self.skew_ticks = skew_ticks

        self.inv = Inventory()
        self.orders: dict[str, RestingOrder | None] = {"UP": None, "DOWN": None}

        self.samples = 0
        self.read_errors = 0
        self.quote_posts = 0
        self.quote_cancels = 0
        self.fill_count = 0
        self.sides_bought: set[str] = set()
        self.gross_spend = Decimal(0)
        self.first_fill_age: float | None = None
        self.last_fill_age: float | None = None
        self.max_gap_seen = Decimal(0)

        self.last_fill_side: str | None = None
        self.last_fill_price: Decimal | None = None
        self.last_fill_ts: float | None = None
        self.opposite_sums: list[float] = []
        self.opposite_lags: list[float] = []

        self.imbalanced_before_fill_count = 0
        self.underweight_fill_count = 0
        self.heavy_fill_count = 0

        self.harvest_matched = Decimal(0)
        self.harvest_basis: Decimal | None = None
        self.harvest_gross_pnl = Decimal(0)

    def _state_row(
        self,
        *,
        now: float,
        event: str,
        side: str = "",
        qty: Decimal | None = None,
        price: Decimal | None = None,
        cost: Decimal | None = None,
        reason: str = "",
        opposite_sum: Decimal | None = None,
        opposite_lag_s: float | None = None,
    ) -> dict[str, Any]:
        pair_basis = self.inv.pair_basis()
        return {
            "utc": _iso(now),
            "session": self.session_no,
            "market": self.market.slug,
            "age_s": f"{now - self.market.window_start:.3f}",
            "event": event,
            "side": side,
            "qty": "" if qty is None else str(qty),
            "price": "" if price is None else str(price),
            "cost": "" if cost is None else str(cost),
            "reason": reason,
            "up_shares": str(self.inv.up_shares),
            "up_avg": "" if self.inv.avg("UP") is None else str(self.inv.avg("UP")),
            "down_shares": str(self.inv.down_shares),
            "down_avg": "" if self.inv.avg("DOWN") is None else str(self.inv.avg("DOWN")),
            "gap": str(self.inv.gap()),
            "matched": str(self.inv.matched()),
            "pair_basis": "" if pair_basis is None else str(pair_basis),
            "opposite_sum": "" if opposite_sum is None else str(opposite_sum),
            "opposite_lag_s": "" if opposite_lag_s is None else f"{opposite_lag_s:.6f}",
            "gross_spend": str(self.gross_spend),
        }

    def _emit(self, **kwargs: Any) -> None:
        self.writer.writerow(self._state_row(**kwargs))

    def _cancel(self, side: str, now: float, reason: str, *, quiet: bool = True) -> None:
        order = self.orders.get(side)
        if order is None:
            return
        self.orders[side] = None
        self.quote_cancels += 1
        self._emit(
            now=now,
            event="CANCEL",
            side=side,
            qty=order.size,
            price=order.price,
            cost=order.price * order.size,
            reason=reason,
        )
        if not quiet:
            print(f"CANCEL     {side} {order.size}@{order.price} | {reason}")

    def _post(self, side: str, price: Decimal, now: float, reason: str) -> None:
        if price <= 0 or price >= 1:
            return
        self.orders[side] = RestingOrder(side, price, self.clip, now, reason)
        self.quote_posts += 1
        self._emit(
            now=now,
            event="QUOTE",
            side=side,
            qty=self.clip,
            price=price,
            cost=price * self.clip,
            reason=reason,
        )

    @staticmethod
    def _mid(book: Any) -> Decimal | None:
        bid = _best(book, "bids")
        ask = _best(book, "asks")
        if bid and ask:
            return (bid[0] + ask[0]) / Decimal(2)
        if ask:
            return ask[0]
        if bid:
            return bid[0]
        return None

    def _desired_quotes(self, up_book: Any, down_book: Any, now: float) -> dict[str, Decimal | None]:
        up_ask = _best(up_book, "asks")
        down_ask = _best(down_book, "asks")
        if up_ask is None or down_ask is None:
            return {"UP": None, "DOWN": None}

        up_tick = _d(getattr(up_book, "tick_size", None)) or Decimal("0.01")
        down_tick = _d(getattr(down_book, "tick_size", None)) or Decimal("0.01")
        up_mid = self._mid(up_book)
        down_mid = self._mid(down_book)
        if up_mid is None or down_mid is None or up_mid + down_mid <= 0:
            return {"UP": None, "DOWN": None}

        # Explicit hypothesis: allocate the combined budget in proportion to current mids,
        # then cap each quote one tick below the current displayed ask.
        total_mid = up_mid + down_mid
        raw_up = self.quote_pair_target * (up_mid / total_mid)
        raw_down = self.quote_pair_target - raw_up

        up_cap = max(Decimal(0), up_ask[0] - up_tick)
        down_cap = max(Decimal(0), down_ask[0] - down_tick)
        up_q = min(_floor_tick(raw_up, up_tick), _floor_tick(up_cap, up_tick))
        down_q = min(_floor_tick(raw_down, down_tick), _floor_tick(down_cap, down_tick))

        under = self.inv.underweight()
        if under == "UP" and down_q > down_tick:
            for _ in range(self.skew_ticks):
                if up_q + up_tick <= up_cap and down_q - down_tick > 0:
                    up_q += up_tick
                    down_q -= down_tick
        elif under == "DOWN" and up_q > up_tick:
            for _ in range(self.skew_ticks):
                if down_q + down_tick <= down_cap and up_q - up_tick > 0:
                    down_q += down_tick
                    up_q -= up_tick

        heavy = self.inv.heavy()
        if self.inv.gap() >= self.max_gap and heavy:
            if heavy == "UP":
                up_q = Decimal(0)
            else:
                down_q = Decimal(0)

        seconds_to_end = self.market.window_end - now
        if seconds_to_end <= self.stop_new_seed_s:
            # Near close, never add to the heavy side. If balanced, stop seeding
            # altogether. Underweight-side quotes may continue to reduce residual risk.
            if heavy == "UP":
                up_q = Decimal(0)
            elif heavy == "DOWN":
                down_q = Decimal(0)
            else:
                up_q = Decimal(0)
                down_q = Decimal(0)

        while up_q > 0 and down_q > 0 and up_q + down_q > self.quote_pair_target:
            # Preserve the underweight side first when rounding causes a budget breach.
            if under == "UP":
                down_q = max(Decimal(0), down_q - down_tick)
            elif under == "DOWN":
                up_q = max(Decimal(0), up_q - up_tick)
            elif up_q >= down_q:
                up_q = max(Decimal(0), up_q - up_tick)
            else:
                down_q = max(Decimal(0), down_q - down_tick)

        return {
            "UP": up_q if up_q > 0 else None,
            "DOWN": down_q if down_q > 0 else None,
        }

    def _record_fill(self, side: str, qty: Decimal, price: Decimal, now: float, reason: str) -> None:
        pre_under = self.inv.underweight()
        pre_heavy = self.inv.heavy()
        if pre_under is not None:
            self.imbalanced_before_fill_count += 1
            if side == pre_under:
                self.underweight_fill_count += 1
            elif side == pre_heavy:
                self.heavy_fill_count += 1

        cost = qty * price
        self.inv.add(side, qty, cost)
        self.gross_spend += cost
        self.sides_bought.add(side)
        self.fill_count += 1

        age = now - self.market.window_start
        if self.first_fill_age is None:
            self.first_fill_age = age
        self.last_fill_age = age
        self.max_gap_seen = max(self.max_gap_seen, self.inv.gap())

        opposite_sum: Decimal | None = None
        opposite_lag: float | None = None
        if (
            self.last_fill_side is not None
            and self.last_fill_side != side
            and self.last_fill_price is not None
            and self.last_fill_ts is not None
        ):
            opposite_sum = self.last_fill_price + price
            opposite_lag = now - self.last_fill_ts
            self.opposite_sums.append(float(opposite_sum))
            self.opposite_lags.append(opposite_lag)

        self._emit(
            now=now,
            event="MAKER_FILL",
            side=side,
            qty=qty,
            price=price,
            cost=cost,
            reason=reason,
            opposite_sum=opposite_sum,
            opposite_lag_s=opposite_lag,
        )
        print(
            f"MAKER_FILL {side} {qty}@{price:.6f} cost=${cost:.4f} | "
            f"U={self.inv.up_shares}@{(self.inv.avg('UP') or Decimal(0)):.4f} "
            f"D={self.inv.down_shares}@{(self.inv.avg('DOWN') or Decimal(0)):.4f} "
            f"gap={self.inv.gap()}"
        )

        self.last_fill_side = side
        self.last_fill_price = price
        self.last_fill_ts = now

    def _detect_resting_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            order = self.orders.get(side)
            if order is None:
                continue
            execution = _buy_execution(book, order.size, order.price)
            if not execution.full:
                continue
            self.orders[side] = None
            self._record_fill(
                side,
                order.size,
                order.price,
                now,
                f"resting bid crossed after {now - order.created_ts:.2f}s",
            )

    def _reconcile_quotes(self, now: float, up_book: Any, down_book: Any) -> None:
        desired = self._desired_quotes(up_book, down_book, now)
        ticks = {
            "UP": _d(getattr(up_book, "tick_size", None)) or Decimal("0.01"),
            "DOWN": _d(getattr(down_book, "tick_size", None)) or Decimal("0.01"),
        }

        for side in ("UP", "DOWN"):
            want = desired[side]
            current = self.orders.get(side)
            if want is None:
                self._cancel(side, now, "quote disabled")
                continue
            if current is not None:
                stale = now - current.created_ts >= self.requote_s
                moved = abs(current.price - want) + Decimal("0.0000001") >= ticks[side]
                if stale or moved:
                    self._cancel(side, now, "stale/reprice")

        under = self.inv.underweight()
        order_sides = ["UP", "DOWN"]
        if under:
            order_sides = [under, "DOWN" if under == "UP" else "UP"]
        else:
            candidates = [(side, desired[side]) for side in order_sides if desired[side] is not None]
            candidates.sort(key=lambda item: item[1])
            seen = {side for side, _ in candidates}
            order_sides = [side for side, _ in candidates] + [
                side for side in ("UP", "DOWN") if side not in seen
            ]

        for side in order_sides:
            want = desired[side]
            if want is None or self.orders.get(side) is not None:
                continue
            reason = "balanced two-sided maker"
            if under == side:
                reason = "underweight-side maker bias"
            elif under is not None:
                reason = "heavy-side reduced maker"
            self._post(side, want, now, reason)

    def _harvest(self, now: float) -> None:
        matched = self.inv.matched()
        basis = self.inv.pair_basis()
        if matched <= 0 or basis is None:
            self._emit(now=now, event="HARVEST", reason="no matched inventory")
            return

        self.harvest_matched = matched
        self.harvest_basis = basis
        self.harvest_gross_pnl = matched * (Decimal(1) - basis)
        self._emit(
            now=now,
            event="HARVEST",
            qty=matched,
            price=basis,
            cost=matched * basis,
            reason="paper batch merge after market window; no mid-market merge",
        )
        print(
            f"HARVEST    matched={matched} basis={basis:.6f} "
            f"grossEdge=${self.harvest_gross_pnl:.6f}"
        )

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        print("\n" + "=" * 96)
        print(f"SESSION    {self.session_no}")
        print(f"MARKET     {self.market.slug}")
        print(f"WINDOW     {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")

        self._emit(now=time.time(), event="SESSION_START", reason="forensic maker/inventory proof")

        while time.time() < self.market.window_end:
            cycle = time.monotonic()
            now = time.time()
            try:
                up_book, down_book = await _books(
                    client,
                    self.market.up_token_id,
                    self.market.down_token_id,
                )
            except Exception as exc:  # noqa: BLE001
                self.read_errors += 1
                print(f"READ ERR   {type(exc).__name__}: {exc}")
                await asyncio.sleep(min(1.0, self.poll_s))
                continue

            self.samples += 1
            self._detect_resting_fills(now, up_book, down_book)
            self._reconcile_quotes(now, up_book, down_book)

            elapsed = time.monotonic() - cycle
            await asyncio.sleep(max(0.0, self.poll_s - elapsed))

        now = time.time()
        self._cancel("UP", now, "market close")
        self._cancel("DOWN", now, "market close")
        self._harvest(now)
        self._emit(now=now, event="SESSION_END", reason="residual inventory retained for analysis")

        pair_basis = self.inv.pair_basis()
        underweight_rate = (
            None
            if self.imbalanced_before_fill_count == 0
            else self.underweight_fill_count / self.imbalanced_before_fill_count
        )
        opposite_mean = None if not self.opposite_sums else statistics.mean(self.opposite_sums)
        opposite_median = None if not self.opposite_sums else statistics.median(self.opposite_sums)
        opposite_lag_median = (
            None if not self.opposite_lags else statistics.median(self.opposite_lags)
        )

        summary = {
            "session": self.session_no,
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "samples": self.samples,
            "read_errors": self.read_errors,
            "quote_posts": self.quote_posts,
            "quote_cancels": self.quote_cancels,
            "maker_proxy_fills": self.fill_count,
            "sides_bought": sorted(self.sides_bought),
            "gross_spend": str(self.gross_spend),
            "up_shares": str(self.inv.up_shares),
            "up_cost": str(self.inv.up_cost),
            "up_avg": None if self.inv.avg("UP") is None else str(self.inv.avg("UP")),
            "down_shares": str(self.inv.down_shares),
            "down_cost": str(self.inv.down_cost),
            "down_avg": None if self.inv.avg("DOWN") is None else str(self.inv.avg("DOWN")),
            "matched_shares": str(self.inv.matched()),
            "pair_basis": None if pair_basis is None else str(pair_basis),
            "harvest_gross_pnl": str(self.harvest_gross_pnl),
            "residual_up_shares": str(self.inv.up_shares - self.inv.matched()),
            "residual_down_shares": str(self.inv.down_shares - self.inv.matched()),
            "first_fill_age_s": self.first_fill_age,
            "last_fill_age_s": self.last_fill_age,
            "max_gap_shares": str(self.max_gap_seen),
            "imbalanced_before_fill_count": self.imbalanced_before_fill_count,
            "underweight_fill_count": self.underweight_fill_count,
            "heavy_fill_count": self.heavy_fill_count,
            "underweight_fill_rate": underweight_rate,
            "opposite_transition_count": len(self.opposite_sums),
            "opposite_sum_mean": opposite_mean,
            "opposite_sum_median": opposite_median,
            "opposite_lag_median_s": opposite_lag_median,
        }

        print(
            f"END        fills={self.fill_count} bothSides={set(self.sides_bought) == {'UP', 'DOWN'}} "
            f"first={self.first_fill_age} last={self.last_fill_age} maxGap={self.max_gap_seen}"
        )
        print(
            f"PAIR       matched={self.inv.matched()} basis="
            f"{'n/a' if pair_basis is None else f'{pair_basis:.6f}'} "
            f"oppMedian={'n/a' if opposite_median is None else f'{opposite_median:.4f}'} "
            f"underweightRate={'n/a' if underweight_rate is None else f'{underweight_rate:.3f}'}"
        )
        return summary


async def _resolve_wait(client: AsyncPublicClient, target_start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, target_start)
        if market is not None:
            return market
        await asyncio.sleep(1.0)


def _median(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return None if not clean else statistics.median(clean)


def _mean(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return None if not clean else statistics.mean(clean)


def _aggregate(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    with_fills = [s for s in summaries if int(s["maker_proxy_fills"]) > 0]
    both = [s for s in summaries if set(s.get("sides_bought", [])) == {"UP", "DOWN"}]
    pairable = [s for s in summaries if s.get("pair_basis") is not None]

    matched_total = sum(Decimal(s["matched_shares"]) for s in summaries)
    matched_cost = Decimal(0)
    for s in pairable:
        matched = Decimal(s["matched_shares"])
        basis = Decimal(s["pair_basis"])
        matched_cost += matched * basis
    weighted_pair_basis = None if matched_total <= 0 else matched_cost / matched_total

    under_counts = sum(int(s["underweight_fill_count"]) for s in summaries)
    imbalanced_counts = sum(int(s["imbalanced_before_fill_count"]) for s in summaries)
    under_rate = None if imbalanced_counts == 0 else under_counts / imbalanced_counts

    return {
        "sessions": len(summaries),
        "sessions_with_fills": len(with_fills),
        "markets_buying_both_sides": len(both),
        "both_sides_rate": None if not summaries else len(both) / len(summaries),
        "total_maker_proxy_fills": sum(int(s["maker_proxy_fills"]) for s in summaries),
        "total_gross_spend": str(sum(Decimal(s["gross_spend"]) for s in summaries)),
        "total_matched_shares": str(matched_total),
        "pair_weighted_combined_basis": (
            None if weighted_pair_basis is None else str(weighted_pair_basis)
        ),
        "total_harvest_gross_pnl": str(
            sum(Decimal(s["harvest_gross_pnl"]) for s in summaries)
        ),
        "median_first_fill_age_s": _median([s.get("first_fill_age_s") for s in with_fills]),
        "median_last_fill_age_s": _median([s.get("last_fill_age_s") for s in with_fills]),
        "median_max_gap_shares": _median(
            [float(Decimal(s["max_gap_shares"])) for s in with_fills]
        ),
        "aggregate_underweight_fill_rate": under_rate,
        "mean_session_opposite_sum": _mean(
            [s.get("opposite_sum_mean") for s in summaries]
        ),
        "median_session_opposite_sum": _median(
            [s.get("opposite_sum_median") for s in summaries]
        ),
        "reference_fingerprints": {
            "first_fill_median_s": REF_FIRST_FILL_MEDIAN_S,
            "last_fill_median_s": REF_LAST_FILL_MEDIAN_S,
            "opposite_sum_median": REF_OPPOSITE_SUM_MEDIAN,
            "opposite_sum_mean": REF_OPPOSITE_SUM_MEAN,
            "underweight_fill_tendency": REF_UNDERWEIGHT_TENDENCY,
            "pair_basis_expected_band": [REF_PAIR_BASIS_LOW, REF_PAIR_BASIS_HIGH],
            "both_sides_expected": "near 100%",
            "note": (
                "References are chain-forensic comparison targets only. "
                "They are not used by the trading decision logic."
            ),
        },
    }


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    activity_path = run_dir / "activity.csv"
    summary_path = run_dir / "summary.json"

    print("READ ONLY   no wallet, no key, no orders, no real merge/redeem")
    print("OBJECTIVE   falsify/confirm Gabagool maker + inventory hypothesis")
    print(
        f"PLAN        {args.sessions} consecutive BTC 5m markets | clip={args.clip}sh "
        f"| poll={args.poll:.2f}s"
    )
    print(
        f"QUOTING     both sides | combined target={args.quote_pair_target} "
        f"| skew={args.skew_ticks} ticks"
    )
    print(
        f"INVENTORY   max gap={args.max_gap}sh | no mid-market merge | "
        f"stop new balanced seeds T-{args.stop_new_seed:.0f}s"
    )
    print("TAKER       disabled in forensic V1")
    print(f"OUTPUT      {run_dir}")

    client = AsyncPublicClient()
    summaries: list[dict[str, Any]] = []
    start = window_start_epoch(DURATION_S, time.time()) + DURATION_S

    try:
        with activity_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=ACTIVITY_FIELDS)
            writer.writeheader()

            for idx in range(args.sessions):
                target = start + idx * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(f"WAIT        session {idx + 1}/{args.sessions} starts in {wait:.1f}s")
                    await asyncio.sleep(wait)

                market = await _resolve_wait(client, target)
                engine = ForensicMarketEngine(
                    session_no=idx + 1,
                    market=market,
                    writer=writer,
                    clip=args.clip,
                    poll_s=args.poll,
                    quote_pair_target=args.quote_pair_target,
                    requote_s=args.requote,
                    max_gap=args.max_gap,
                    stop_new_seed_s=args.stop_new_seed,
                    skew_ticks=args.skew_ticks,
                )
                summaries.append(await engine.run(client))
                fh.flush()

        aggregate = _aggregate(summaries)
        result = {
            "created_utc": _iso(),
            "config": {
                "asset": ASSET,
                "duration_s": DURATION_S,
                "sessions": args.sessions,
                "poll_s": args.poll,
                "clip": str(args.clip),
                "quote_pair_target": str(args.quote_pair_target),
                "requote_s": args.requote,
                "max_gap": str(args.max_gap),
                "stop_new_seed_s": args.stop_new_seed,
                "skew_ticks": args.skew_ticks,
                "taker_repair": False,
                "mid_market_merge": False,
            },
            "aggregate": aggregate,
            "sessions": summaries,
            "files": {
                "activity_csv": str(activity_path),
                "summary_json": str(summary_path),
            },
            "interpretation": (
                "Read-only forward paper proof. Resting maker fills require a later sampled "
                "ask book to show the entire configured clip executable at or below the "
                "resting bid. Queue priority, maker rebates, taker fees, and real transaction "
                "latency are not modeled. Matched inventory is batch-harvested only after the "
                "market window for accounting; residual inventory is not fake-redeemed."
            ),
        }
        summary_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

        print("\n" + "=" * 96)
        print("GABAGOOL FORENSIC V1 SUMMARY")
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
        print(f"ACTIVITY    {activity_path}")
        print(f"SUMMARY     {summary_path}")
        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only Gabagool maker/inventory forensic validator"
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    parser.add_argument("--clip", type=Decimal, default=DEFAULT_CLIP)
    parser.add_argument(
        "--quote-pair-target",
        type=Decimal,
        default=DEFAULT_QUOTE_PAIR_TARGET,
    )
    parser.add_argument("--requote", type=float, default=DEFAULT_REQUOTE_S)
    parser.add_argument("--max-gap", type=Decimal, default=DEFAULT_MAX_GAP)
    parser.add_argument(
        "--stop-new-seed",
        type=float,
        default=DEFAULT_STOP_NEW_SEED_S,
    )
    parser.add_argument("--skew-ticks", type=int, default=DEFAULT_SKEW_TICKS)
    parser.add_argument(
        "--output",
        default="data/gabagool_forensic_v1",
    )
    args = parser.parse_args()

    if not (1 <= args.sessions <= 200):
        parser.error("--sessions must be 1..200")
    if not (0.1 <= args.poll <= 5.0):
        parser.error("--poll must be 0.1..5.0")
    if args.clip <= 0:
        parser.error("--clip must be positive")
    if not (Decimal("0.95") <= args.quote_pair_target < Decimal("1")):
        parser.error("--quote-pair-target must be in [0.95, 1)")
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
