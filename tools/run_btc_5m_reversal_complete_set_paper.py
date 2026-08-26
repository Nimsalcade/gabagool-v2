"""Read-only paper test for the BTC 5m 65->40 reversal complete-set strategy.

No wallet, signing, orders, sells, merges, or redemptions are performed.
A paper trade requires equal UP/DOWN quantity to be simultaneously executable
from the sampled ask books with fee-adjusted pair basis <= 0.97 and total
acquisition spend <= $100. Matched shares are immediately valued at $1/pair.

Paper execution is an atomic same-snapshot depth walk. Live two-leg latency and
legging risk are deliberately outside this first profitability test.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from polymarket import AsyncPublicClient

from src.discovery import resolve_market, window_start_epoch
from src.reversal_complete_set import (
    FeeSchedule,
    ReversalGate,
    StrategyState,
    max_profitable_pair,
    normalize_asks,
)
from tools.metamask_10session_strategy_observer import _best, _books, _levels

D = Decimal
ASSET = "btc"
DURATION_S = 300

OBS_FIELDS = [
    "session", "market", "utc", "age_s", "seconds_to_end", "state",
    "high_side", "up_ask", "down_ask", "raw_best_pair", "event", "bankroll",
]
TRADE_FIELDS = [
    "session", "market", "utc", "age_s", "high_side",
    "high_seen_age_s", "high_seen_price", "reversal_seen_age_s",
    "reversal_seen_price", "shares", "up_vwap", "up_fee", "up_effective",
    "down_vwap", "down_fee", "down_effective", "raw_pair_basis",
    "fee_adjusted_pair_basis", "total_cost", "merge_value", "locked_profit",
    "roi_on_spend", "bankroll_before", "bankroll_after",
]


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(
        ts if ts is not None else time.time(), timezone.utc
    ).isoformat()


def _fmt(x: Decimal | None, n: int = 9) -> str:
    return "" if x is None else f"{x:.{n}f}"


def _ask(book: Any) -> Decimal | None:
    x = _best(book, "asks")
    return None if x is None else D(str(x[0]))


def _min_size(book: Any, fallback: Decimal) -> Decimal:
    try:
        x = D(str(getattr(book, "min_order_size", None)))
        return x if x > 0 else fallback
    except Exception:
        return fallback


async def _resolve_wait(client: AsyncPublicClient, start: int) -> Any:
    while True:
        market = await resolve_market(client, ASSET, DURATION_S, start)
        if market is not None:
            return market
        await asyncio.sleep(0.5)


@dataclass
class Result:
    session: int
    market: str
    condition_id: str
    start_bankroll: str
    end_bankroll: str = ""
    state: str = ""
    samples: int = 0
    read_errors: int = 0
    high_side: str | None = None
    high_seen_age_s: float | None = None
    high_seen_price: str | None = None
    reversal_seen_age_s: float | None = None
    reversal_seen_price: str | None = None
    traded: bool = False
    trade_age_s: float | None = None
    shares: str | None = None
    raw_pair_basis: str | None = None
    fee_adjusted_pair_basis: str | None = None
    total_cost: str | None = None
    merge_value: str | None = None
    locked_profit: str | None = None
    roi_on_spend: str | None = None
    skip_reason: str | None = None


async def run_market(
    *,
    client: AsyncPublicClient,
    market: Any,
    session: int,
    bankroll: Decimal,
    args: argparse.Namespace,
    obs: csv.DictWriter,
    trades: csv.DictWriter,
) -> tuple[Result, Decimal]:
    gate = ReversalGate(args.high_trigger, args.reversal_trigger)
    fee = FeeSchedule(args.fee_rate, args.fee_exponent, True)
    result = Result(session, market.slug, market.condition_id, str(bankroll))

    print("\n" + "=" * 88, flush=True)
    print(f"SESSION {session}/{args.sessions}  {market.slug}", flush=True)
    print(
        f"RULE    {args.high_trigger}->{args.reversal_trigger} same-side reversal; "
        f"all-in pair <= {args.pair_cap}",
        flush=True,
    )
    print(
        f"CAP     bankroll=${bankroll:.2f} max/market=${args.max_per_market:.2f}",
        flush=True,
    )

    while time.time() < market.window_end:
        started = time.monotonic()
        now = time.time()
        try:
            up_book, down_book = await _books(
                client, market.up_token_id, market.down_token_id
            )
        except Exception as exc:  # noqa: BLE001
            result.read_errors += 1
            print(f"READ ERR {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(min(1.0, args.poll))
            continue

        result.samples += 1
        up_ask, down_ask = _ask(up_book), _ask(down_book)
        events = gate.observe(
            now=now, up_best_ask=up_ask, down_best_ask=down_ask
        )

        if "HIGH_ARMED" in events:
            result.high_side = gate.high_side
            result.high_seen_age_s = gate.high_seen_at - market.window_start
            result.high_seen_price = str(gate.high_seen_price)
            print(
                f"HIGH    {gate.high_side} ask={gate.high_seen_price} "
                f"age={result.high_seen_age_s:.1f}s",
                flush=True,
            )

        if "REVERSAL_CONFIRMED" in events:
            result.reversal_seen_age_s = (
                gate.reversal_seen_at - market.window_start
            )
            result.reversal_seen_price = str(gate.reversal_seen_price)
            print(
                f"REVERSE {gate.high_side} ask={gate.reversal_seen_price} "
                f"age={result.reversal_seen_age_s:.1f}s -> PAIR_HUNT",
                flush=True,
            )

        event = "|".join(events)
        if gate.state == StrategyState.PAIR_HUNT:
            min_shares = max(
                args.min_shares,
                _min_size(up_book, args.min_shares),
                _min_size(down_book, args.min_shares),
            )
            quote = max_profitable_pair(
                normalize_asks(_levels(up_book, "asks")),
                normalize_asks(_levels(down_book, "asks")),
                budget=min(args.max_per_market, bankroll),
                pair_cap=args.pair_cap,
                fee_schedule=fee,
                min_shares=min_shares,
            )
            if quote is not None:
                before = bankroll
                bankroll += quote.locked_profit
                gate.mark_traded()
                result.traded = True
                result.trade_age_s = now - market.window_start
                result.shares = str(quote.shares)
                result.raw_pair_basis = str(quote.raw_pair_basis)
                result.fee_adjusted_pair_basis = str(
                    quote.fee_adjusted_pair_basis
                )
                result.total_cost = str(quote.total_cost)
                result.merge_value = str(quote.merge_value)
                result.locked_profit = str(quote.locked_profit)
                result.roi_on_spend = str(quote.roi_on_spend)

                trades.writerow({
                    "session": session,
                    "market": market.slug,
                    "utc": _iso(now),
                    "age_s": f"{result.trade_age_s:.3f}",
                    "high_side": gate.high_side,
                    "high_seen_age_s": f"{result.high_seen_age_s:.3f}",
                    "high_seen_price": result.high_seen_price,
                    "reversal_seen_age_s": f"{result.reversal_seen_age_s:.3f}",
                    "reversal_seen_price": result.reversal_seen_price,
                    "shares": _fmt(quote.shares),
                    "up_vwap": _fmt(quote.up.raw_vwap),
                    "up_fee": _fmt(quote.up.fee),
                    "up_effective": _fmt(quote.up.effective_vwap),
                    "down_vwap": _fmt(quote.down.raw_vwap),
                    "down_fee": _fmt(quote.down.fee),
                    "down_effective": _fmt(quote.down.effective_vwap),
                    "raw_pair_basis": _fmt(quote.raw_pair_basis),
                    "fee_adjusted_pair_basis": _fmt(
                        quote.fee_adjusted_pair_basis
                    ),
                    "total_cost": _fmt(quote.total_cost),
                    "merge_value": _fmt(quote.merge_value),
                    "locked_profit": _fmt(quote.locked_profit),
                    "roi_on_spend": _fmt(quote.roi_on_spend),
                    "bankroll_before": _fmt(before, 6),
                    "bankroll_after": _fmt(bankroll, 6),
                })
                event = (event + "|" if event else "") + "PAIR_EXECUTED"
                print(
                    f"TRADE   qty={quote.shares:.4f} "
                    f"raw={quote.raw_pair_basis:.5f} "
                    f"ALL_IN={quote.fee_adjusted_pair_basis:.5f} "
                    f"spend=${quote.total_cost:.4f} "
                    f"merge=${quote.merge_value:.4f} "
                    f"PNL=+${quote.locked_profit:.4f} "
                    f"bankroll=${bankroll:.4f}",
                    flush=True,
                )

        raw_pair = (
            up_ask + down_ask
            if up_ask is not None and down_ask is not None
            else None
        )
        obs.writerow({
            "session": session,
            "market": market.slug,
            "utc": _iso(now),
            "age_s": f"{now - market.window_start:.3f}",
            "seconds_to_end": f"{market.window_end - now:.3f}",
            "state": gate.state.value,
            "high_side": gate.high_side or "",
            "up_ask": _fmt(up_ask),
            "down_ask": _fmt(down_ask),
            "raw_best_pair": _fmt(raw_pair),
            "event": event,
            "bankroll": _fmt(bankroll, 6),
        })

        if result.traded:
            break
        await asyncio.sleep(max(0.0, args.poll - (time.monotonic() - started)))

    if not result.traded:
        gate.expire()
        if gate.high_seen_at is None:
            result.skip_reason = "NO_65_TRIGGER"
        elif gate.reversal_seen_at is None:
            result.skip_reason = "NO_SAME_SIDE_40_REVERSAL"
        else:
            result.skip_reason = "NO_FEE_ADJUSTED_PAIR_LE_097"
        print(f"SKIP    {result.skip_reason}", flush=True)

    result.end_bankroll = str(bankroll)
    result.state = gate.state.value

    remain = market.window_end - time.time()
    if remain > 0:
        await asyncio.sleep(remain)
    return result, bankroll


def aggregate(
    results: list[Result], initial: Decimal, bankroll: Decimal
) -> dict[str, Any]:
    traded = [r for r in results if r.traded]
    pnls = [D(r.locked_profit or "0") for r in traded]
    spend = sum((D(r.total_cost or "0") for r in traded), D("0"))
    pnl = bankroll - initial
    return {
        "sessions": len(results),
        "trades": len(traded),
        "skips": len(results) - len(traded),
        "trade_rate": None if not results else len(traded) / len(results),
        "initial_bankroll": str(initial),
        "final_bankroll": str(bankroll),
        "net_locked_pnl": str(pnl),
        "total_deployed": str(spend),
        "return_on_deployed_capital": None
        if spend <= 0 else str(pnl / spend),
        "winning_trades": sum(p > 0 for p in pnls),
        "losing_trades": sum(p < 0 for p in pnls),
        "min_trade_pnl": None if not pnls else str(min(pnls)),
        "max_trade_pnl": None if not pnls else str(max(pnls)),
    }


def config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "asset": ASSET,
        "duration_s": DURATION_S,
        "sessions": args.sessions,
        "poll_s": args.poll,
        "bankroll": str(args.bankroll),
        "max_per_market": str(args.max_per_market),
        "high_trigger": str(args.high_trigger),
        "reversal_trigger": str(args.reversal_trigger),
        "pair_cap_fee_adjusted": str(args.pair_cap),
        "fee_rate": str(args.fee_rate),
        "fee_exponent": str(args.fee_exponent),
        "min_shares": str(args.min_shares),
        "join_current": args.join_current,
        "paper_fill_model": (
            "atomic same-snapshot equal-share taker depth walk; "
            "fee-adjusted; immediate $1 complete-set merge"
        ),
    }


async def amain(args: argparse.Namespace) -> int:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    obs_path = run_dir / "observations.csv"
    trade_path = run_dir / "trades.csv"
    summary_path = run_dir / "summary.json"

    current = window_start_epoch(DURATION_S, time.time())
    first = current if args.join_current else current + DURATION_S
    initial = args.bankroll
    bankroll = initial
    results: list[Result] = []

    print("READ ONLY no wallet / no orders / no on-chain writes", flush=True)
    print(
        f"BTC 5M   {args.high_trigger}->{args.reversal_trigger}; "
        f"fee-adjusted pair <= {args.pair_cap}",
        flush=True,
    )
    print(
        f"CAPITAL  ${initial} bankroll; ${args.max_per_market}/market; "
        f"{args.sessions} sessions",
        flush=True,
    )
    print(
        f"FEE      shares*{args.fee_rate}*p*(1-p) "
        f"(exponent {args.fee_exponent})",
        flush=True,
    )
    print(f"OUTPUT   {run_dir}", flush=True)

    client = AsyncPublicClient()
    try:
        with (
            obs_path.open("w", newline="", encoding="utf-8") as ofh,
            trade_path.open("w", newline="", encoding="utf-8") as tfh,
        ):
            ow = csv.DictWriter(ofh, fieldnames=OBS_FIELDS)
            tw = csv.DictWriter(tfh, fieldnames=TRADE_FIELDS)
            ow.writeheader()
            tw.writeheader()

            for i in range(args.sessions):
                target = first + i * DURATION_S
                wait = target - time.time()
                if wait > 0:
                    print(
                        f"WAIT     session {i+1}/{args.sessions} in {wait:.1f}s",
                        flush=True,
                    )
                    await asyncio.sleep(wait)

                market = await _resolve_wait(client, target)
                result, bankroll = await run_market(
                    client=client,
                    market=market,
                    session=i + 1,
                    bankroll=bankroll,
                    args=args,
                    obs=ow,
                    trades=tw,
                )
                results.append(result)
                ofh.flush()
                tfh.flush()

                snapshot = {
                    "created_utc": _iso(),
                    "config": config(args),
                    "aggregate": aggregate(results, initial, bankroll),
                    "sessions": [asdict(r) for r in results],
                }
                summary_path.write_text(
                    json.dumps(snapshot, indent=2), encoding="utf-8"
                )
                a = snapshot["aggregate"]
                print(
                    f"RUN PNL  markets={a['sessions']} trades={a['trades']} "
                    f"net=${D(a['net_locked_pnl']):+.4f} "
                    f"bankroll=${bankroll:.4f}",
                    flush=True,
                )

        final = {
            "created_utc": _iso(),
            "config": config(args),
            "aggregate": aggregate(results, initial, bankroll),
            "sessions": [asdict(r) for r in results],
            "files": {
                "observations": str(obs_path),
                "trades": str(trade_path),
                "summary": str(summary_path),
            },
        }
        summary_path.write_text(json.dumps(final, indent=2), encoding="utf-8")
        print("\n" + "=" * 88, flush=True)
        print("FINAL PROFITABILITY", flush=True)
        for k in (
            "sessions", "trades", "skips", "total_deployed",
            "net_locked_pnl", "final_bankroll",
        ):
            print(f"{k.upper():18s} {final['aggregate'][k]}", flush=True)
        print(f"SUMMARY            {summary_path}", flush=True)
        return 0
    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BTC 5m reversal complete-set read-only paper test"
    )
    p.add_argument("--sessions", type=int, default=96)
    p.add_argument("--poll", type=float, default=0.50)
    p.add_argument("--bankroll", type=Decimal, default=D("1000"))
    p.add_argument("--max-per-market", type=Decimal, default=D("100"))
    p.add_argument("--high-trigger", type=Decimal, default=D("0.65"))
    p.add_argument("--reversal-trigger", type=Decimal, default=D("0.40"))
    p.add_argument("--pair-cap", type=Decimal, default=D("0.97"))
    p.add_argument("--fee-rate", type=Decimal, default=D("0.07"))
    p.add_argument("--fee-exponent", type=Decimal, default=D("1"))
    p.add_argument("--min-shares", type=Decimal, default=D("5"))
    p.add_argument(
        "--output", default="data/btc_5m_reversal_complete_set_paper"
    )
    p.add_argument("--join-current", action="store_true")
    args = p.parse_args()

    if not (1 <= args.sessions <= 10000):
        p.error("--sessions must be 1..10000")
    if not (0.10 <= args.poll <= 5.0):
        p.error("--poll must be 0.10..5.0")
    if args.bankroll <= 0:
        p.error("--bankroll must be positive")
    if args.max_per_market <= 0 or args.max_per_market > args.bankroll:
        p.error("--max-per-market must be >0 and <= bankroll")
    if not (D("0") < args.reversal_trigger < args.high_trigger < D("1")):
        p.error("require 0 < reversal-trigger < high-trigger < 1")
    if not (D("0") < args.pair_cap < D("1")):
        p.error("--pair-cap must be inside (0,1)")
    if args.fee_rate < 0 or args.fee_exponent <= 0:
        p.error("invalid fee schedule")
    if args.min_shares <= 0:
        p.error("--min-shares must be positive")
    return args


def main() -> None:
    raise SystemExit(asyncio.run(amain(parse_args())))


if __name__ == "__main__":
    main()
