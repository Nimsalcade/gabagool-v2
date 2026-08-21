"""Forward-running read-only Gabagool behavior reproducer for BTC 5-minute markets.

This is deliberately NOT another LEG1/LEG2 trigger bot. It implements the
observable economic engine supported by the Gabagool22 archive:

- continuously maintain small BUY quotes on both outcomes;
- allow temporary inventory imbalance;
- bias new quotes toward the underweight side;
- use an aggressive repair buy only when the pooled matched basis remains < $1;
- MERGE profitable matched inventory in the paper ledger and recycle virtual cash;
- never place a real order, never request a wallet, never sign, never transact.

The purpose is to prove/reject the behavior on live markets before reconnecting
this policy to a real executor.

Paper-fill model
----------------
A virtual maker order is considered filled only after it was already resting
and a later order-book snapshot shows the full configured size executable at or
below that bid. This is intentionally conservative. It does not model queue
priority and therefore is not a claim that a real maker order would fill.

A virtual taker repair uses the displayed 5-share ask VWAP in the current book.

Archive calibration used for the default behavior (not treated as secret source
code):
- full exact-chain reconstruction: maker-heavy but meaningful taker usage;
- pair-weighted combined BUY basis roughly high-.98s;
- BTC 5m overlap: first-fill median ~14s, last-fill median ~205s;
- BTC 5m overlap: both outcomes bought in 24/24 sampled markets;
- small dense clips; live venue minimum for this experiment is 5 shares.
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

# Evidence-calibrated defaults / small-wallet adaptation.
DEFAULT_SESSIONS = 3
DEFAULT_POLL_S = 0.50
DEFAULT_START_CASH = Decimal("15")
DEFAULT_CLIP = Decimal("5")
DEFAULT_QUOTE_PAIR_TARGET = Decimal("0.985")
DEFAULT_MERGE_MAX = Decimal("0.995")
DEFAULT_REPAIR_MAX = Decimal("0.999")
DEFAULT_REQUOTE_S = 3.0
DEFAULT_REPAIR_AFTER_S = 8.0
DEFAULT_MAX_GAP = Decimal("10")
DEFAULT_STOP_NEW_SEED_S = 45.0
DEFAULT_SKEW_TICKS = 2

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
    "cash",
    "up_shares",
    "up_avg",
    "down_shares",
    "down_avg",
    "gap",
    "matched",
    "pair_basis",
    "realized_merge_pnl",
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
        q = self.shares(side)
        return None if q <= 0 else self.cost(side) / q

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
        g = self.gap_signed()
        if g > 0:
            return "DOWN"
        if g < 0:
            return "UP"
        return None

    def pair_basis(self) -> Decimal | None:
        if self.matched() <= 0:
            return None
        ua = self.avg("UP")
        da = self.avg("DOWN")
        if ua is None or da is None:
            return None
        return ua + da

    def projected_pair_basis(self, side: str, qty: Decimal, cost: Decimal) -> Decimal | None:
        up_q, up_c = self.up_shares, self.up_cost
        dn_q, dn_c = self.down_shares, self.down_cost
        if side == "UP":
            up_q += qty
            up_c += cost
        else:
            dn_q += qty
            dn_c += cost
        if min(up_q, dn_q) <= 0:
            return None
        return (up_c / up_q) + (dn_c / dn_q)

    def remove_matched(self, qty: Decimal) -> tuple[Decimal, Decimal, Decimal]:
        """Remove qty from both sides at pooled average cost.

        Returns (up_cost_removed, down_cost_removed, pair_basis_before).
        """
        if qty <= 0 or qty > self.matched():
            raise ValueError("invalid matched removal")
        ua = self.avg("UP")
        da = self.avg("DOWN")
        if ua is None or da is None:
            raise ValueError("missing average cost")
        uc = ua * qty
        dc = da * qty
        self.up_shares -= qty
        self.up_cost -= uc
        self.down_shares -= qty
        self.down_cost -= dc
        if abs(self.up_shares) < Decimal("0.000001"):
            self.up_shares = Decimal(0)
            self.up_cost = Decimal(0)
        if abs(self.down_shares) < Decimal("0.000001"):
            self.down_shares = Decimal(0)
            self.down_cost = Decimal(0)
        return uc, dc, ua + da


class PaperAccount:
    def __init__(self, cash: Decimal) -> None:
        self.cash = cash
        self.realized_merge_pnl = Decimal(0)
        self.unresolved_markets: list[dict[str, Any]] = []


class MarketEngine:
    def __init__(
        self,
        *,
        session_no: int,
        market: Any,
        account: PaperAccount,
        writer: csv.DictWriter,
        clip: Decimal,
        poll_s: float,
        quote_pair_target: Decimal,
        merge_max: Decimal,
        repair_max: Decimal,
        requote_s: float,
        repair_after_s: float,
        max_gap: Decimal,
        stop_new_seed_s: float,
        skew_ticks: int,
    ) -> None:
        self.session_no = session_no
        self.market = market
        self.account = account
        self.writer = writer
        self.clip = clip
        self.poll_s = poll_s
        self.quote_pair_target = quote_pair_target
        self.merge_max = merge_max
        self.repair_max = repair_max
        self.requote_s = requote_s
        self.repair_after_s = repair_after_s
        self.max_gap = max_gap
        self.stop_new_seed_s = stop_new_seed_s
        self.skew_ticks = skew_ticks

        self.inv = Inventory()
        self.orders: dict[str, RestingOrder | None] = {"UP": None, "DOWN": None}
        self.gap_started_ts: float | None = None

        self.buy_count = 0
        self.maker_fill_count = 0
        self.taker_repair_count = 0
        self.merge_count = 0
        self.merged_shares = Decimal(0)
        self.merge_acquisition_cost = Decimal(0)
        self.first_fill_age: float | None = None
        self.last_fill_age: float | None = None
        self.max_gap_seen = Decimal(0)
        self.samples = 0
        self.read_errors = 0
        self.quote_posts = 0
        self.quote_cancels = 0
        self.pair_bases: list[float] = []
        self.start_cash = account.cash
        self.start_realized = account.realized_merge_pnl
        self.sides_bought: set[str] = set()

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
    ) -> dict[str, Any]:
        pb = self.inv.pair_basis()
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
            "cash": str(self.account.cash),
            "up_shares": str(self.inv.up_shares),
            "up_avg": "" if self.inv.avg("UP") is None else str(self.inv.avg("UP")),
            "down_shares": str(self.inv.down_shares),
            "down_avg": "" if self.inv.avg("DOWN") is None else str(self.inv.avg("DOWN")),
            "gap": str(self.inv.gap()),
            "matched": str(self.inv.matched()),
            "pair_basis": "" if pb is None else str(pb),
            "realized_merge_pnl": str(self.account.realized_merge_pnl),
        }

    def _emit(self, **kwargs: Any) -> None:
        self.writer.writerow(self._state_row(**kwargs))

    def _reserved_cash(self, exclude_side: str | None = None) -> Decimal:
        total = Decimal(0)
        for side, order in self.orders.items():
            if side == exclude_side or order is None:
                continue
            total += order.price * order.size
        return total

    def _free_after_reservations(self, exclude_side: str | None = None) -> Decimal:
        return self.account.cash - self._reserved_cash(exclude_side)

    def _cancel(self, side: str, now: float, reason: str, *, quiet: bool = False) -> None:
        order = self.orders.get(side)
        if order is None:
            return
        self.orders[side] = None
        self.quote_cancels += 1
        self._emit(now=now, event="CANCEL", side=side, qty=order.size, price=order.price, reason=reason)
        if not quiet:
            print(f"CANCEL     {side} {order.size}@{order.price} | {reason}")

    def _post(self, side: str, price: Decimal, now: float, reason: str) -> None:
        if price <= 0 or price >= 1:
            return
        need = price * self.clip
        if self._free_after_reservations() + Decimal("0.000001") < need:
            return
        self.orders[side] = RestingOrder(side, price, self.clip, now, reason)
        self.quote_posts += 1
        self._emit(now=now, event="QUOTE", side=side, qty=self.clip, price=price, cost=need, reason=reason)
        print(f"QUOTE      {side} {self.clip}@{price} | {reason}")

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
        dn_ask = _best(down_book, "asks")
        if up_ask is None or dn_ask is None:
            return {"UP": None, "DOWN": None}

        up_tick = _d(getattr(up_book, "tick_size", None)) or Decimal("0.01")
        dn_tick = _d(getattr(down_book, "tick_size", None)) or Decimal("0.01")
        up_mid = self._mid(up_book)
        dn_mid = self._mid(down_book)
        if up_mid is None or dn_mid is None or up_mid + dn_mid <= 0:
            return {"UP": None, "DOWN": None}

        raw_up = self.quote_pair_target * (up_mid / (up_mid + dn_mid))
        raw_dn = self.quote_pair_target - raw_up

        up_cap = max(Decimal(0), up_ask[0] - up_tick)
        dn_cap = max(Decimal(0), dn_ask[0] - dn_tick)
        up_q = min(_floor_tick(raw_up, up_tick), _floor_tick(up_cap, up_tick))
        dn_q = min(_floor_tick(raw_dn, dn_tick), _floor_tick(dn_cap, dn_tick))

        under = self.inv.underweight()
        if under == "DOWN" and up_q > up_tick:
            for _ in range(self.skew_ticks):
                if dn_q + dn_tick <= dn_cap and up_q - up_tick > 0:
                    dn_q += dn_tick
                    up_q -= up_tick
        elif under == "UP" and dn_q > dn_tick:
            for _ in range(self.skew_ticks):
                if up_q + up_tick <= up_cap and dn_q - dn_tick > 0:
                    up_q += up_tick
                    dn_q -= dn_tick

        gap = self.inv.gap()
        heavy = None
        if self.inv.gap_signed() > 0:
            heavy = "UP"
        elif self.inv.gap_signed() < 0:
            heavy = "DOWN"
        if gap >= self.max_gap and heavy:
            if heavy == "UP":
                up_q = Decimal(0)
            else:
                dn_q = Decimal(0)

        seconds_to_end = self.market.window_end - now
        if seconds_to_end <= self.stop_new_seed_s and self.inv.gap() == 0:
            up_q = Decimal(0)
            dn_q = Decimal(0)
        elif seconds_to_end <= self.stop_new_seed_s and heavy:
            if heavy == "UP":
                up_q = Decimal(0)
            else:
                dn_q = Decimal(0)

        while up_q > 0 and dn_q > 0 and up_q + dn_q > self.quote_pair_target:
            if under == "UP":
                dn_q = max(Decimal(0), dn_q - dn_tick)
            else:
                up_q = max(Decimal(0), up_q - up_tick)

        return {
            "UP": up_q if up_q > 0 else None,
            "DOWN": dn_q if dn_q > 0 else None,
        }

    def _refresh_gap_clock(self, now: float) -> None:
        gap = self.inv.gap()
        self.max_gap_seen = max(self.max_gap_seen, gap)
        if gap <= Decimal("0.000001"):
            self.gap_started_ts = None
        elif self.gap_started_ts is None:
            self.gap_started_ts = now

    def _record_fill(self, side: str, qty: Decimal, cost: Decimal, now: float, kind: str, price: Decimal, reason: str) -> None:
        if cost > self.account.cash + Decimal("0.000001"):
            raise RuntimeError("paper fill exceeded free cash")
        self.account.cash -= cost
        self.inv.add(side, qty, cost)
        self.sides_bought.add(side)
        self.buy_count += 1
        if kind == "MAKER_FILL":
            self.maker_fill_count += 1
        else:
            self.taker_repair_count += 1
        age = now - self.market.window_start
        self.first_fill_age = age if self.first_fill_age is None else self.first_fill_age
        self.last_fill_age = age
        self._refresh_gap_clock(now)
        self._emit(now=now, event=kind, side=side, qty=qty, price=price, cost=cost, reason=reason)
        print(
            f"{kind:<10} {side} {qty}@{price:.6f} cost=${cost:.6f} | "
            f"U={self.inv.up_shares}@{(self.inv.avg('UP') or Decimal(0)):.4f} "
            f"D={self.inv.down_shares}@{(self.inv.avg('DOWN') or Decimal(0)):.4f} "
            f"cash=${self.account.cash:.4f}"
        )

    def _detect_resting_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            order = self.orders.get(side)
            if order is None:
                continue
            ex = _buy_execution(book, order.size, order.price)
            if not ex.full:
                continue
            self.orders[side] = None
            cost = order.price * order.size
            self._record_fill(
                side,
                order.size,
                cost,
                now,
                "MAKER_FILL",
                order.price,
                f"resting bid crossed after {now - order.created_ts:.2f}s",
            )

    def _merge_if_profitable(self, now: float) -> None:
        while self.inv.matched() + Decimal("0.000001") >= self.clip:
            basis = self.inv.pair_basis()
            if basis is None or basis > self.merge_max:
                return
            qty = self.clip
            uc, dc, pair_basis = self.inv.remove_matched(qty)
            acquisition = uc + dc
            proceeds = qty
            pnl = proceeds - acquisition
            self.account.cash += proceeds
            self.account.realized_merge_pnl += pnl
            self.merge_count += 1
            self.merged_shares += qty
            self.merge_acquisition_cost += acquisition
            self.pair_bases.append(float(pair_basis))
            self._refresh_gap_clock(now)
            self._emit(
                now=now,
                event="MERGE",
                qty=qty,
                price=pair_basis,
                cost=acquisition,
                reason=f"matched basis <= {self.merge_max}",
            )
            print(
                f"MERGE      {qty} pairs | basis={pair_basis:.6f} "
                f"edge=${pnl:.6f} cash=${self.account.cash:.6f} "
                f"cumPnL=${self.account.realized_merge_pnl:.6f}"
            )
            self._cancel("UP", now, "post-merge requote", quiet=True)
            self._cancel("DOWN", now, "post-merge requote", quiet=True)

    def _maybe_taker_repair(self, now: float, up_book: Any, down_book: Any) -> None:
        gap = self.inv.gap()
        under = self.inv.underweight()
        if under is None or gap + Decimal("0.000001") < self.clip:
            return
        stale = 0.0 if self.gap_started_ts is None else now - self.gap_started_ts
        near_close = self.market.window_end - now <= self.stop_new_seed_s
        if gap < self.max_gap and stale < self.repair_after_s and not near_close:
            return

        book = up_book if under == "UP" else down_book
        ex = _buy_execution(book, self.clip)
        if not ex.full or ex.vwap is None:
            return
        projected = self.inv.projected_pair_basis(under, self.clip, ex.cost)
        if projected is None or projected > self.repair_max:
            return

        self._cancel(under, now, "paper taker repair", quiet=True)
        if self._free_after_reservations(exclude_side=under) + Decimal("0.000001") < ex.cost:
            return
        self._record_fill(
            under,
            self.clip,
            ex.cost,
            now,
            "TAKER_BUY",
            ex.vwap,
            f"repair gap={gap} stale={stale:.1f}s projected_pair={projected:.6f}",
        )
        self._merge_if_profitable(now)

    def _reconcile_quotes(self, now: float, up_book: Any, down_book: Any) -> None:
        desired = self._desired_quotes(up_book, down_book, now)
        ticks = {
            "UP": _d(getattr(up_book, "tick_size", None)) or Decimal("0.01"),
            "DOWN": _d(getattr(down_book, "tick_size", None)) or Decimal("0.01"),
        }

        under = self.inv.underweight()
        order_sides = ["UP", "DOWN"]
        if under:
            order_sides = [under, "DOWN" if under == "UP" else "UP"]
        else:
            candidates = [(s, desired[s]) for s in order_sides if desired[s] is not None]
            candidates.sort(key=lambda x: x[1])
            seen = {s for s, _ in candidates}
            order_sides = [s for s, _ in candidates] + [s for s in ("UP", "DOWN") if s not in seen]

        for side in ("UP", "DOWN"):
            want = desired[side]
            cur = self.orders.get(side)
            if want is None:
                self._cancel(side, now, "quote disabled", quiet=True)
                continue
            if cur is not None:
                stale = now - cur.created_ts >= self.requote_s
                moved = abs(cur.price - want) + Decimal("0.0000001") >= ticks[side]
                if stale or moved:
                    self._cancel(side, now, "stale/reprice", quiet=True)

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

    async def run(self, client: AsyncPublicClient) -> dict[str, Any]:
        print("\n" + "=" * 92)
        print(f"SESSION    {self.session_no}")
        print(f"MARKET     {self.market.slug}")
        print(f"WINDOW     {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")
        print(f"START CASH ${self.account.cash:.6f}")

        self._emit(now=time.time(), event="SESSION_START", reason="forward-running paper inventory engine")

        while time.time() < self.market.window_end:
            cycle = time.monotonic()
            now = time.time()
            try:
                up_book, down_book = await _books(client, self.market.up_token_id, self.market.down_token_id)
            except Exception as exc:  # noqa: BLE001
                self.read_errors += 1
                print(f"READ ERR   {type(exc).__name__}: {exc}")
                await asyncio.sleep(min(1.0, self.poll_s))
                continue

            self.samples += 1
            self._detect_resting_fills(now, up_book, down_book)
            self._merge_if_profitable(now)
            self._maybe_taker_repair(now, up_book, down_book)
            self._merge_if_profitable(now)
            self._reconcile_quotes(now, up_book, down_book)

            elapsed = time.monotonic() - cycle
            await asyncio.sleep(max(0.0, self.poll_s - elapsed))

        now = time.time()
        self._cancel("UP", now, "market close", quiet=True)
        self._cancel("DOWN", now, "market close", quiet=True)
        self._merge_if_profitable(now)

        if self.inv.up_shares > 0 or self.inv.down_shares > 0:
            self.account.unresolved_markets.append(
                {
                    "session": self.session_no,
                    "market": self.market.slug,
                    "condition_id": self.market.condition_id,
                    "up_shares": str(self.inv.up_shares),
                    "up_cost": str(self.inv.up_cost),
                    "down_shares": str(self.inv.down_shares),
                    "down_cost": str(self.inv.down_cost),
                    "pair_basis": None if self.inv.pair_basis() is None else str(self.inv.pair_basis()),
                }
            )

        self._emit(now=now, event="SESSION_END", reason="residuals left unresolved; no fake redemption")
        session_pnl = self.account.realized_merge_pnl - self.start_realized
        summary = {
            "session": self.session_no,
            "market": self.market.slug,
            "condition_id": self.market.condition_id,
            "start_cash": str(self.start_cash),
            "end_free_cash": str(self.account.cash),
            "buys": self.buy_count,
            "sides_bought": sorted(self.sides_bought),
            "maker_proxy_fills": self.maker_fill_count,
            "taker_repairs": self.taker_repair_count,
            "quote_posts": self.quote_posts,
            "quote_cancels": self.quote_cancels,
            "merges": self.merge_count,
            "merged_shares": str(self.merged_shares),
            "merge_acquisition_cost": str(self.merge_acquisition_cost),
            "session_merge_pnl": str(session_pnl),
            "first_fill_age_s": self.first_fill_age,
            "last_fill_age_s": self.last_fill_age,
            "max_gap_shares": str(self.max_gap_seen),
            "end_inventory": {
                "up_shares": str(self.inv.up_shares),
                "up_cost": str(self.inv.up_cost),
                "down_shares": str(self.inv.down_shares),
                "down_cost": str(self.inv.down_cost),
                "pair_basis": None if self.inv.pair_basis() is None else str(self.inv.pair_basis()),
            },
            "mean_merge_basis": None if not self.pair_bases else statistics.mean(self.pair_bases),
            "samples": self.samples,
            "read_errors": self.read_errors,
        }
        print(
            f"END        buys={self.buy_count} maker={self.maker_fill_count} taker={self.taker_repair_count} "
            f"merges={self.merge_count} merged={self.merged_shares}sh "
            f"free_cash=${self.account.cash:.6f} sessionPnL=${session_pnl:.6f}"
        )
        print(
            f"RESIDUAL   UP={self.inv.up_shares}@{(self.inv.avg('UP') or Decimal(0)):.4f} "
            f"DOWN={self.inv.down_shares}@{(self.inv.avg('DOWN') or Decimal(0)):.4f}"
        )
        return summary


async def _resolve_wait(client: AsyncPublicClient, target_start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, target_start)
        if market is not None:
            return market
        await asyncio.sleep(1.0)


def _aggregate(summaries: list[dict[str, Any]], account: PaperAccount, starting_cash: Decimal) -> dict[str, Any]:
    fills = [s for s in summaries if s["buys"] > 0]
    both_sides = [s for s in summaries if set(s.get("sides_bought", [])) == {"UP", "DOWN"}]
    firsts = [s["first_fill_age_s"] for s in fills if s["first_fill_age_s"] is not None]
    lasts = [s["last_fill_age_s"] for s in fills if s["last_fill_age_s"] is not None]
    merged = sum(Decimal(s["merged_shares"]) for s in summaries)
    buys = sum(int(s["buys"]) for s in summaries)
    maker = sum(int(s["maker_proxy_fills"]) for s in summaries)
    taker = sum(int(s["taker_repairs"]) for s in summaries)
    merges = sum(int(s["merges"]) for s in summaries)
    bases = [float(s["mean_merge_basis"]) for s in summaries if s["mean_merge_basis"] is not None]
    unresolved_cost = Decimal(0)
    for p in account.unresolved_markets:
        unresolved_cost += Decimal(p["up_cost"]) + Decimal(p["down_cost"])
    return {
        "sessions": len(summaries),
        "starting_cash": str(starting_cash),
        "ending_free_cash": str(account.cash),
        "realized_merge_pnl": str(account.realized_merge_pnl),
        "unresolved_inventory_cost": str(unresolved_cost),
        "buys": buys,
        "maker_proxy_fills": maker,
        "taker_repairs": taker,
        "maker_share_of_buys": None if buys == 0 else maker / buys,
        "merges": merges,
        "merged_shares": str(merged),
        "markets_buying_both_sides": len(both_sides),
        "median_first_fill_age_s": None if not firsts else statistics.median(firsts),
        "median_last_fill_age_s": None if not lasts else statistics.median(lasts),
        "mean_merge_basis": None if not bases else statistics.mean(bases),
        "archive_reference": {
            "btc5m_first_fill_median_s": 14.0,
            "btc5m_last_fill_median_s": 205.0,
            "btc5m_two_sided_sample": "24/24",
            "btc5m_pair_coverage_pct": 96.596,
            "btc5m_pair_weighted_combined": 0.994514,
            "note": "Archive targets are calibration references, not guarantees for a tiny live paper account.",
        },
    }


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    activity_path = run_dir / "activity.csv"
    summary_path = run_dir / "summary.json"

    print("READ ONLY   no wallet, no key, no orders, no real merges")
    print("ENGINE      continuous two-sided inventory accumulation, not LEG1/LEG2")
    print(
        f"BANKROLL    ${args.cash} virtual cash | clip={args.clip}sh | "
        f"fresh_quote_pair<={args.quote_pair_target}"
    )
    print(
        f"SETTLEMENT  paper MERGE when pooled pair<={args.merge_max}; "
        f"taker repair allowed only if projected pair<={args.repair_max}"
    )
    print(
        f"RISK        max gap={args.max_gap}sh | repair after {args.repair_after:.1f}s | "
        f"stop new balanced seeds T-{args.stop_new_seed:.0f}s"
    )
    print("PROOF       terminal prints every QUOTE, MAKER_FILL, TAKER_BUY and MERGE with inventory state")
    print(f"OUTPUT      {run_dir}")

    account = PaperAccount(args.cash)
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
                engine = MarketEngine(
                    session_no=idx + 1,
                    market=market,
                    account=account,
                    writer=writer,
                    clip=args.clip,
                    poll_s=args.poll,
                    quote_pair_target=args.quote_pair_target,
                    merge_max=args.merge_max,
                    repair_max=args.repair_max,
                    requote_s=args.requote,
                    repair_after_s=args.repair_after,
                    max_gap=args.max_gap,
                    stop_new_seed_s=args.stop_new_seed,
                    skew_ticks=args.skew_ticks,
                )
                summaries.append(await engine.run(client))
                fh.flush()

        aggregate = _aggregate(summaries, account, args.cash)
        result = {
            "created_utc": _iso(),
            "config": {
                "asset": ASSET,
                "duration_s": DURATION_S,
                "sessions": args.sessions,
                "poll_s": args.poll,
                "starting_cash": str(args.cash),
                "clip": str(args.clip),
                "quote_pair_target": str(args.quote_pair_target),
                "merge_max": str(args.merge_max),
                "repair_max": str(args.repair_max),
                "requote_s": args.requote,
                "repair_after_s": args.repair_after,
                "max_gap": str(args.max_gap),
                "stop_new_seed_s": args.stop_new_seed,
                "skew_ticks": args.skew_ticks,
            },
            "aggregate": aggregate,
            "sessions": summaries,
            "unresolved_markets": account.unresolved_markets,
            "files": {"activity_csv": str(activity_path), "summary_json": str(summary_path)},
            "interpretation": (
                "Forward-running read-only paper execution. Maker fills use a conservative resting-limit proxy; "
                "queue priority, fees, rebates and real settlement latency are not modeled. Residual positions are "
                "not fake-redeemed, so free cash is intentionally conservative when unresolved inventory remains."
            ),
        }
        summary_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

        print("\n" + "=" * 92)
        print("GABAGOOL BEHAVIOR REPRODUCTION SUMMARY")
        print(f"SESSIONS    {aggregate['sessions']}")
        print(f"BUYS        {aggregate['buys']} | maker-proxy={aggregate['maker_proxy_fills']} | taker={aggregate['taker_repairs']}")
        print(f"MERGES      {aggregate['merges']} | merged_shares={aggregate['merged_shares']}")
        print(f"TWO-SIDED   {aggregate['markets_buying_both_sides']}/{aggregate['sessions']}")
        print(f"FIRST FILL  median={aggregate['median_first_fill_age_s']}s | archive ref≈14s")
        print(f"LAST FILL   median={aggregate['median_last_fill_age_s']}s | archive ref≈205s")
        print(f"PAIR BASIS  mean merged={aggregate['mean_merge_basis']} | archive 5m ref≈0.994514")
        print(f"PAPER PNL   realized merge=${Decimal(aggregate['realized_merge_pnl']):.6f}")
        print(f"FREE CASH   ${Decimal(aggregate['ending_free_cash']):.6f}")
        print(f"UNRESOLVED  cost=${Decimal(aggregate['unresolved_inventory_cost']):.6f} (not fake-redeemed)")
        print(f"ACTIVITY    {activity_path}")
        print(f"SUMMARY     {summary_path}")
        return 0
    finally:
        await client.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Read-only forward-running Gabagool BTC 5m behavior reproducer")
    p.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    p.add_argument("--poll", type=float, default=DEFAULT_POLL_S)
    p.add_argument("--cash", type=Decimal, default=DEFAULT_START_CASH)
    p.add_argument("--clip", type=Decimal, default=DEFAULT_CLIP)
    p.add_argument("--quote-pair-target", type=Decimal, default=DEFAULT_QUOTE_PAIR_TARGET)
    p.add_argument("--merge-max", type=Decimal, default=DEFAULT_MERGE_MAX)
    p.add_argument("--repair-max", type=Decimal, default=DEFAULT_REPAIR_MAX)
    p.add_argument("--requote", type=float, default=DEFAULT_REQUOTE_S)
    p.add_argument("--repair-after", type=float, default=DEFAULT_REPAIR_AFTER_S)
    p.add_argument("--max-gap", type=Decimal, default=DEFAULT_MAX_GAP)
    p.add_argument("--stop-new-seed", type=float, default=DEFAULT_STOP_NEW_SEED_S)
    p.add_argument("--skew-ticks", type=int, default=DEFAULT_SKEW_TICKS)
    p.add_argument("--output", default="data/gabagool_live_paper_reproducer")
    args = p.parse_args()

    if not (1 <= args.sessions <= 50):
        p.error("--sessions must be 1..50")
    if not (0.1 <= args.poll <= 5.0):
        p.error("--poll must be 0.1..5.0")
    if args.cash <= 0 or args.clip <= 0:
        p.error("--cash and --clip must be positive")
    if not (Decimal("0.90") <= args.quote_pair_target < Decimal("1")):
        p.error("--quote-pair-target must be in [0.90, 1)")
    if not (args.quote_pair_target <= args.merge_max < Decimal("1")):
        p.error("--merge-max must be >= quote-pair-target and < 1")
    if not (args.merge_max <= args.repair_max < Decimal("1")):
        p.error("--repair-max must be >= merge-max and < 1")
    if args.max_gap < args.clip:
        p.error("--max-gap must be >= clip")
    if args.requote <= 0 or args.repair_after < 0:
        p.error("--requote must be positive and --repair-after nonnegative")
    if args.skew_ticks < 0 or args.skew_ticks > 10:
        p.error("--skew-ticks must be 0..10")

    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
