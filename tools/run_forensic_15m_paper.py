"""Run the reconstructed Gabagool 15-minute policy against REAL live books.

READ ONLY. This program uses ``AsyncPublicClient`` only. It never loads a wallet,
never signs an order, and never submits/cancels/merges/redeems anything on-chain.

The strategy rules come from :mod:`src.forensic_15m`. The two dimensions public
fills cannot identify exactly -- queue priority and private cancel/requote timing --
are intentionally kept outside the strategy and exposed as conservative paper-model
parameters.

Default maker fill proxy:
  A hypothetical BUY must already be resting. A later real order-book snapshot must
  show enough total ask size at/below the paper bid to fill the WHOLE paper order.
  The same displayed liquidity is consumed only once per snapshot across our orders.

Default quote TTL:
  10 seconds. This is NOT claimed to be Gabagool's private cancel timer. Existing
  heavy-side layers are allowed to survive until TTL instead of being instantly
  removed when inventory becomes imbalanced, matching the observed soft overshoot.

Optional aggressive repair:
  ``--taker-mode evidence`` uses the repository's full-history, evidence-calibrated
  deficient-leg repair gate. Its existence/monotonic fingerprints are observed; the
  exact historical trigger formula remains unidentifiable. Use ``--taker-mode off``
  to isolate the pure maker reconstruction.

Example:
  python -m tools.run_forensic_15m_paper --assets btc,eth --sessions 1
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from src.forensic_15m import (
    DURATION_S,
    QUOTE_END_AGE_S,
    QUOTE_START_AGE_S,
    Inventory as PolicyInventory,
    acquisition_spend,
    clip_for_age,
    complementary_base_bid,
    conservative_floor_pnl,
    desired_layer_count,
    hard_gap_allows,
    layer_prices,
    settlement_pnl,
    settlement_value,
)
from src.policy import (
    InventoryState as RepairState,
    projected_combined_vwap as repair_projected_vwap,
    taker_should_fire,
)
from tools.metamask_10session_strategy_observer import _best, _books, _levels


EVENT_FIELDS = [
    "utc", "session", "asset", "market", "age_s", "event", "side", "order_id",
    "qty", "price", "cost", "reason", "cash", "reserved", "up_shares", "up_vwap",
    "down_shares", "down_vwap", "combined_vwap", "gap_shares", "clip", "up_layers",
    "down_layers", "maker_fills", "taker_fills",
]


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc).isoformat()


def _best_ask(book: Any) -> tuple[float, float] | None:
    x = _best(book, "asks")
    return None if x is None else (float(x[0]), float(x[1]))


def _asks(book: Any) -> list[tuple[float, float]]:
    return [(float(p), float(q)) for p, q in _levels(book, "asks")]


def _tick(book: Any) -> float:
    try:
        x = float(getattr(book, "tick_size", None) or 0.01)
        return x if x > 0 else 0.01
    except (TypeError, ValueError):
        return 0.01


@dataclass
class Order:
    oid: str
    side: str
    price: float
    shares: float
    created: float
    expires: float


@dataclass
class Inventory:
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    last_up_fill: float | None = None
    last_down_fill: float | None = None

    def policy(self) -> PolicyInventory:
        return PolicyInventory(self.up_shares, self.down_shares, self.up_cost, self.down_cost)

    def add(self, side: str, shares: float, price: float, now: float) -> None:
        if side == "UP":
            self.up_shares += shares
            self.up_cost += shares * price
            self.last_up_fill = now
        else:
            self.down_shares += shares
            self.down_cost += shares * price
            self.last_down_fill = now

    @property
    def underweight(self) -> str | None:
        if abs(self.up_shares - self.down_shares) <= 1e-9:
            return None
        return "UP" if self.up_shares < self.down_shares else "DOWN"

    @property
    def deficit(self) -> float:
        return abs(self.up_shares - self.down_shares)

    @property
    def ratio(self) -> float:
        lo, hi = min(self.up_shares, self.down_shares), max(self.up_shares, self.down_shares)
        if hi <= 0:
            return 1.0
        return math.inf if lo <= 0 else hi / lo


@dataclass
class Result:
    session: int
    asset: str
    market: str
    condition_id: str
    first_fill_age_s: float | None = None
    last_fill_age_s: float | None = None
    maker_fills: int = 0
    taker_fills: int = 0
    quote_posts: int = 0
    quote_expiries: int = 0
    max_gap_shares: float = 0.0
    max_gap_clips: float = 0.0
    winner: str | None = None
    resolved: bool = False
    settlement_value: float | None = None
    settlement_pnl: float | None = None
    conservative_floor_pnl: float | None = None


class Engine:
    def __init__(self, *, session: int, market: Any, writer: csv.DictWriter, args: argparse.Namespace):
        self.session = session
        self.market = market
        self.writer = writer
        self.args = args
        self.cash = float(args.paper_cash)
        self.inv = Inventory()
        self.orders: dict[str, Order] = {}
        self.oid_seq = 1
        self.clip = 0.0
        self.result = Result(session, market.asset, market.slug, market.condition_id)

    def reserved(self, *, exclude_side: str | None = None) -> float:
        return sum(o.price * o.shares for o in self.orders.values() if o.side != exclude_side)

    def emit(self, now: float, event: str, *, side: str = "", order: Order | None = None,
             qty: float | None = None, price: float | None = None,
             cost: float | None = None, reason: str = "") -> None:
        p = self.inv.policy()
        c = self.clip
        up_layers = desired_layer_count(p, "UP", c) if c > 0 else 0
        dn_layers = desired_layer_count(p, "DOWN", c) if c > 0 else 0
        self.writer.writerow({
            "utc": _iso(now), "session": self.session, "asset": self.market.asset,
            "market": self.market.slug, "age_s": f"{now-self.market.window_start:.3f}",
            "event": event, "side": side, "order_id": "" if order is None else order.oid,
            "qty": "" if qty is None else f"{qty:.9f}",
            "price": "" if price is None else f"{price:.9f}",
            "cost": "" if cost is None else f"{cost:.9f}", "reason": reason,
            "cash": f"{self.cash:.9f}", "reserved": f"{self.reserved():.9f}",
            "up_shares": f"{p.up_shares:.9f}",
            "up_vwap": "" if p.up_vwap is None else f"{p.up_vwap:.9f}",
            "down_shares": f"{p.down_shares:.9f}",
            "down_vwap": "" if p.down_vwap is None else f"{p.down_vwap:.9f}",
            "combined_vwap": "" if p.combined_vwap is None else f"{p.combined_vwap:.9f}",
            "gap_shares": f"{p.abs_gap:.9f}", "clip": f"{c:.3f}",
            "up_layers": up_layers, "down_layers": dn_layers,
            "maker_fills": self.result.maker_fills, "taker_fills": self.result.taker_fills,
        })

    def post(self, side: str, price: float, now: float) -> None:
        notional = price * self.clip
        if notional > self.cash - self.reserved() + 1e-9:
            return
        o = Order(
            oid=f"P{self.session}-{self.market.asset.upper()}-{self.oid_seq}",
            side=side, price=price, shares=self.clip, created=now,
            expires=now + self.args.quote_ttl,
        )
        self.oid_seq += 1
        self.orders[o.oid] = o
        self.result.quote_posts += 1
        self.emit(now, "QUOTE", side=side, order=o, qty=o.shares, price=o.price,
                  cost=notional, reason="1-opposite-ask passive layer")

    def expire(self, now: float) -> None:
        for oid, o in list(self.orders.items()):
            if now < o.expires:
                continue
            del self.orders[oid]
            self.result.quote_expiries += 1
            self.emit(now, "EXPIRE", side=o.side, order=o, qty=o.shares, price=o.price,
                      reason=f"paper TTL={self.args.quote_ttl}s; cancellation timing unobservable")

    def fill(self, now: float, side: str, shares: float, price: float, kind: str,
             reason: str, order: Order | None = None) -> bool:
        cost = shares * price
        if cost > self.cash + 1e-9:
            return False
        self.cash -= cost
        self.inv.add(side, shares, price, now)
        age = now - self.market.window_start
        if self.result.first_fill_age_s is None:
            self.result.first_fill_age_s = age
        self.result.last_fill_age_s = age
        if kind == "MAKER_FILL":
            self.result.maker_fills += 1
        else:
            self.result.taker_fills += 1
        p = self.inv.policy()
        self.result.max_gap_shares = max(self.result.max_gap_shares, p.abs_gap)
        if self.clip > 0:
            self.result.max_gap_clips = max(self.result.max_gap_clips, p.abs_gap / self.clip)
        self.emit(now, kind, side=side, order=order, qty=shares, price=price,
                  cost=cost, reason=reason)
        print(
            f"{self.market.asset.upper():3} {kind:10} {side:4} {shares:5.1f}@{price:.3f} | "
            f"U={p.up_shares:.1f}@{(p.up_vwap or 0):.4f} "
            f"D={p.down_shares:.1f}@{(p.down_vwap or 0):.4f} "
            f"basis={(p.combined_vwap or 0):.4f} cash=${self.cash:.2f}"
        )
        return True

    def strict_maker_fills(self, now: float, up_book: Any, down_book: Any) -> None:
        for side, book in (("UP", up_book), ("DOWN", down_book)):
            levels = [[p, q] for p, q in _asks(book)]
            if not levels:
                continue
            orders = sorted(
                [o for o in self.orders.values() if o.side == side and now > o.created],
                key=lambda o: (-o.price, o.created),
            )
            for o in orders:
                if not hard_gap_allows(self.inv.policy(), side=side, shares=o.shares,
                                       parent_clip=max(self.clip, o.shares)):
                    self.orders.pop(o.oid, None)
                    self.emit(now, "CANCEL_SAFETY", side=side, order=o, qty=o.shares,
                              price=o.price, reason="8-clip emergency gap")
                    continue
                available = sum(q for p, q in levels if p <= o.price + 1e-12)
                if available + 1e-9 < o.shares:
                    continue
                need = o.shares
                for level in levels:
                    p, q = level
                    if p > o.price + 1e-12 or need <= 1e-12:
                        continue
                    take = min(q, need)
                    level[1] -= take
                    need -= take
                if need > 1e-8:
                    continue
                self.orders.pop(o.oid, None)
                self.fill(now, side, o.shares, o.price, "MAKER_FILL",
                          f"later real ask snapshot crossed paper bid after {now-o.created:.2f}s", o)

    def desired(self, up_book: Any, down_book: Any) -> dict[str, tuple[float, ...]]:
        ua, da = _best_ask(up_book), _best_ask(down_book)
        if ua is None or da is None or self.clip <= 0:
            return {"UP": (), "DOWN": ()}
        p = self.inv.policy()
        up_tick, dn_tick = _tick(up_book), _tick(down_book)
        ub = complementary_base_bid(
            own_best_ask=ua[0], opposite_best_ask=da[0], tick=up_tick
        )
        db = complementary_base_bid(
            own_best_ask=da[0], opposite_best_ask=ua[0], tick=dn_tick
        )
        return {
            "UP": layer_prices(ub, tick=up_tick,
                               layers=desired_layer_count(p, "UP", self.clip)),
            "DOWN": layer_prices(db, tick=dn_tick,
                                 layers=desired_layer_count(p, "DOWN", self.clip)),
        }

    def renew(self, now: float, up_book: Any, down_book: Any) -> None:
        want = self.desired(up_book, down_book)
        for side in ("UP", "DOWN"):
            target_n = len(want[side])
            active = [o for o in self.orders.values() if o.side == side]
            # Do not mass-cancel heavy-side stale layers; simply stop renewing them.
            if len(active) >= target_n:
                continue
            used = {round(o.price, 10) for o in active}
            for px in want[side]:
                if len([o for o in self.orders.values() if o.side == side]) >= target_n:
                    break
                if round(px, 10) in used:
                    continue
                if not hard_gap_allows(self.inv.policy(), side=side, shares=self.clip,
                                       parent_clip=self.clip):
                    break
                self.post(side, px, now)
                used.add(round(px, 10))

    def repair_state(self, now: float) -> RepairState:
        return RepairState(
            up_shares=self.inv.up_shares, down_shares=self.inv.down_shares,
            up_cost=self.inv.up_cost, down_cost=self.inv.down_cost,
            last_up_fill_ts=self.inv.last_up_fill, last_down_fill_ts=self.inv.last_down_fill,
            now_ts=now, window_start_ts=float(self.market.window_start),
            seconds_to_end=float(self.market.window_end-now),
        )

    def maybe_taker(self, now: float, up_book: Any, down_book: Any) -> None:
        if self.args.taker_mode != "evidence" or self.clip <= 0:
            return
        side = self.inv.underweight
        if side is None:
            return
        deficit = self.inv.deficit
        planned = min(deficit, self.clip * 2.0)
        if planned + 1e-9 < self.clip:
            return
        book = up_book if side == "UP" else down_book
        remaining, cost = planned, 0.0
        for px, qty in _asks(book):
            if remaining <= 1e-12:
                break
            take = min(qty, remaining)
            cost += take * px
            remaining -= take
        if remaining > 1e-8:
            return
        vwap = cost / planned
        state = self.repair_state(now)
        projected = repair_projected_vwap(state, side=side, price=vwap, shares=planned)
        if not taker_should_fire(
            state, candidate_side=side, projected_basis=projected,
            target_combined_vwap=.985, max_combined_vwap=self.args.max_combined_vwap,
            taker_stop_buffer_s=2.0,
        ):
            return
        # Orders on the deficient side are about to be replaced by the immediate
        # paper repair, so only reserve the opposite side when checking cash.
        if cost > self.cash - self.reserved(exclude_side=side) + 1e-9:
            return
        for oid, o in list(self.orders.items()):
            if o.side == side:
                del self.orders[oid]
                self.emit(now, "CANCEL_FOR_REPAIR", side=side, order=o, qty=o.shares,
                          price=o.price, reason="deficient-leg aggressive repair")
        self.fill(now, side, planned, vwap, "TAKER_FILL",
                  f"evidence repair ratio={self.inv.ratio:.4f} deficit={deficit:.2f} projected={projected}")

    async def run(self, client: AsyncPublicClient) -> Result:
        print("\n" + "=" * 96)
        print(f"PAPER {self.market.asset.upper()} | {self.market.slug}")
        print(f"WINDOW {_iso(self.market.window_start)} -> {_iso(self.market.window_end)}")
        self.emit(time.time(), "SESSION_START", reason="real-book read-only forensic 15m")
        while time.time() < self.market.window_end:
            started = time.monotonic()
            now = time.time()
            age = now - self.market.window_start
            self.clip = clip_for_age(age)
            try:
                up_book, down_book = await _books(
                    client, self.market.up_token_id, self.market.down_token_id
                )
            except Exception as exc:  # noqa: BLE001
                self.emit(now, "READ_ERROR", reason=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(min(1.0, self.args.poll))
                continue
            self.expire(now)
            self.strict_maker_fills(now, up_book, down_book)
            if QUOTE_START_AGE_S <= age < QUOTE_END_AGE_S and self.clip > 0:
                self.maybe_taker(now, up_book, down_book)
                self.renew(now, up_book, down_book)
            await asyncio.sleep(max(0.0, self.args.poll-(time.monotonic()-started)))

        now = time.time()
        for oid, o in list(self.orders.items()):
            del self.orders[oid]
            self.emit(now, "CANCEL_CLOSE", side=o.side, order=o, qty=o.shares,
                      price=o.price, reason="post-close settlement lifecycle")
        self.result.conservative_floor_pnl = conservative_floor_pnl(self.inv.policy())
        self.emit(now, "WINDOW_CLOSE",
                  reason=f"matched={self.inv.policy().matched:.6f} floorPnL={self.result.conservative_floor_pnl:.6f}")
        return self.result


async def gamma_winner(slug: str, timeout_s: float) -> str | None:
    deadline = time.time() + timeout_s
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    async with httpx.AsyncClient(timeout=10.0) as hc:
        while time.time() < deadline:
            try:
                r = await hc.get(url)
                if r.status_code == 200:
                    m = r.json()
                    outcomes, prices = m.get("outcomes", []), m.get("outcomePrices", [])
                    if isinstance(outcomes, str):
                        outcomes = json.loads(outcomes)
                    if isinstance(prices, str):
                        prices = json.loads(prices)
                    ranked = []
                    for i, raw in enumerate(prices):
                        try:
                            ranked.append((float(raw), i))
                        except (TypeError, ValueError):
                            pass
                    if ranked:
                        value, idx = max(ranked)
                        if value >= .99 and idx < len(outcomes):
                            label = str(outcomes[idx]).upper()
                            if label in ("UP", "YES"):
                                return "UP"
                            if label in ("DOWN", "NO"):
                                return "DOWN"
            except Exception:
                pass
            await asyncio.sleep(2.0)
    return None


async def choose_market(client: AsyncPublicClient, asset: str, *, clean_start: bool) -> Any:
    while True:
        now = time.time()
        start = window_start_epoch(DURATION_S, now)
        age = now - start
        if clean_start and age > QUOTE_START_AGE_S:
            start += DURATION_S
        if start > now:
            wait = start - now
            print(f"WAIT  {asset.upper()} clean 15m window in {wait:.1f}s")
            await asyncio.sleep(min(5.0, wait))
            continue
        m = await resolve_market(client, asset, DURATION_S, int(start))
        if m is not None and m.seconds_to_end > 0:
            return m
        await asyncio.sleep(1.0)


async def amain(args: argparse.Namespace) -> int:
    assets = tuple(a.strip().lower() for a in args.assets.split(",") if a.strip())
    if not assets or any(a not in ("btc", "eth") for a in assets):
        raise SystemExit("--assets must contain only btc and/or eth")

    prefix = Path(args.out)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    events_path = prefix.parent / f"{prefix.name}_{stamp}_events.csv"
    summary_path = prefix.parent / f"{prefix.name}_{stamp}_summary.json"

    client = AsyncPublicClient()
    final_rows: list[dict[str, Any]] = []
    try:
        with events_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=EVENT_FIELDS)
            writer.writeheader()
            for session in range(1, args.sessions+1):
                clean = (session == 1 and not args.join_current)
                markets = await asyncio.gather(
                    *(choose_market(client, asset, clean_start=clean) for asset in assets)
                )
                engines = [Engine(session=session, market=m, writer=writer, args=args) for m in markets]
                await asyncio.gather(*(e.run(client) for e in engines))
                fh.flush()
                winners = await asyncio.gather(
                    *(gamma_winner(e.market.slug, args.resolution_timeout) for e in engines)
                )
                for e, winner in zip(engines, winners, strict=True):
                    e.result.winner = winner
                    e.result.resolved = winner is not None
                    p = e.inv.policy()
                    if winner:
                        e.result.settlement_value = settlement_value(p, winner)
                        e.result.settlement_pnl = settlement_pnl(p, winner)
                    row = {
                        **asdict(e.result),
                        "up_shares": p.up_shares, "down_shares": p.down_shares,
                        "up_vwap": p.up_vwap, "down_vwap": p.down_vwap,
                        "combined_vwap": p.combined_vwap, "matched_shares": p.matched,
                        "gross_spend": acquisition_spend(p),
                        "paper_cash_after_buys": e.cash,
                        "maker_share": e.result.maker_fills / max(1, e.result.maker_fills+e.result.taker_fills),
                    }
                    final_rows.append(row)
                    print("-" * 96)
                    print(json.dumps(row, indent=2, sort_keys=True))

        summary = {
            "generated_utc": _iso(),
            "mode": "READ_ONLY_REAL_15M_BOOKS",
            "strategy": "maximum-identifiable Gabagool 15m reconstruction",
            "maker_fill_proxy": "later-snapshot full-depth cross-through",
            "quote_ttl_s": args.quote_ttl,
            "quote_ttl_is_historical_claim": False,
            "taker_mode": args.taker_mode,
            "markets": final_rows,
            "aggregate": {
                "markets": len(final_rows),
                "resolved": sum(bool(r["resolved"]) for r in final_rows),
                "maker_fills": sum(int(r["maker_fills"]) for r in final_rows),
                "taker_fills": sum(int(r["taker_fills"]) for r in final_rows),
                "gross_spend": sum(float(r["gross_spend"]) for r in final_rows),
                "settlement_pnl": sum(float(r["settlement_pnl"]) for r in final_rows if r["settlement_pnl"] is not None),
            },
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nEVENTS  {events_path}")
        print(f"SUMMARY {summary_path}")
        return 0
    finally:
        try:
            await client.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-only real-market Gabagool 15m paper harness")
    ap.add_argument("--assets", default="btc,eth")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--poll", type=float, default=.50)
    ap.add_argument("--quote-ttl", type=float, default=10.0)
    ap.add_argument("--paper-cash", type=float, default=500.0, help="independent cash per market")
    ap.add_argument("--taker-mode", choices=("evidence", "off"), default="evidence")
    ap.add_argument("--max-combined-vwap", type=float, default=1.01)
    ap.add_argument("--resolution-timeout", type=float, default=240.0)
    ap.add_argument("--join-current", action="store_true",
                    help="join current 15m window; default waits for a clean window if age>18s")
    ap.add_argument("--out", default="data/gabagool_15m_live_v5")
    args = ap.parse_args()
    if args.sessions < 1:
        ap.error("--sessions must be >= 1")
    if min(args.poll, args.quote_ttl, args.paper_cash, args.resolution_timeout) <= 0:
        ap.error("poll/quote-ttl/paper-cash/resolution-timeout must be positive")
    return args


def main() -> None:
    raise SystemExit(asyncio.run(amain(parse_args())))


if __name__ == "__main__":
    main()
