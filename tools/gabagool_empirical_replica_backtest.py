"""Empirical Gabagool BTC 5m replica + historical backtest.

This is deliberately simple. It encodes the behavior visible in the wallet's
BTC 5m chain fills instead of trying to infer a hidden predictive signal:

* buy both outcomes, never predict direction;
* nominal 10-share clips;
* passive maker price one tick below the observed best ask;
* begin around the observed first-fill median (~14s);
* normally buy the underweight side;
* while the heavy side is getting cheaper, allow another clip every 3c and
  tolerate up to 40 shares of temporary imbalance;
* after ~210s stop expanding inventory and only repair the underweight side
  until residual imbalance is <=10 shares;
* do not merge intramarket;
* at settlement, matched shares are worth $1 per complete set and winner-side
  residual shares redeem at $1.

Important: there is NO per-market hard pair-basis ceiling. The observed wallet
has individual BTC 5m markets both below and above $1 pooled basis; what matters
is the aggregate behavior across markets.

Input format is the PredictionTicks BTC 5m CSV format:
    timestamp,datetime,up_price,down_price,remaining_minutes

The script accepts either a zip containing those CSVs or a directory of CSVs.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import statistics
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, TextIO


DEFAULT_CLIP = 10.0
DEFAULT_TICK = 0.01
DEFAULT_START_AGE = 14.0
DEFAULT_CADENCE = 3.0
DEFAULT_STACK_STEP = 0.03
DEFAULT_MAX_GAP = 40.0
DEFAULT_ACCUMULATION_STOP = 210.0
DEFAULT_END_AGE = 290.0
DEFAULT_FINAL_GAP = 10.0

# Direct BTC 5m chain reference from the Feb 20 2026 03:00-04:00 ET slice.
CHAIN_REFERENCE = {
    "markets": 12,
    "fill_rows_median": 59.5,
    "fill_rows_range": [34, 116],
    "first_fill_median_s": 14.0,
    "first_fill_range_s": [10.0, 18.0],
    "last_fill_median_s": 212.0,
    "last_fill_range_s": [186.0, 292.0],
    "matched_shares_median": 217.814911,
    "weighted_pair_basis": 0.9872373780999437,
    "median_pair_basis": 0.9879903949015434,
    "pair_basis_range": [0.9469535208820448, 1.0503609729054113],
    "residual_gap_median_shares": 12.40719700000001,
    "residual_gap_mean_shares": 14.5871835,
    "residual_gap_range_shares": [0.09782599999999775, 51.39041099999997],
}

MARKET_FIELDS = [
    "market", "start_epoch", "fills", "up_fills", "down_fills", "both_sides",
    "first_fill_age_s", "last_fill_age_s", "max_gap_shares", "up_shares",
    "down_shares", "up_avg", "down_avg", "matched_shares", "pair_basis",
    "residual_gap_shares", "winner", "gross_spend", "matched_gross_edge",
    "settlement_pnl",
]

FILL_FIELDS = [
    "market", "start_epoch", "age_s", "side", "qty", "observed_ask",
    "maker_price", "reason", "up_shares", "down_shares", "gap_shares",
    "up_avg", "down_avg", "pair_basis",
]


def _median(values: Iterable[float]) -> float | None:
    values = list(values)
    return None if not values else statistics.median(values)


def _mean(values: Iterable[float]) -> float | None:
    values = list(values)
    return None if not values else statistics.mean(values)


def _market_start_from_name(name: str) -> int:
    m = re.search(r"(?:btc-updown-5m-)?(\d{10})(?:\.csv)?$", Path(name).name)
    if not m:
        raise ValueError(f"cannot extract 10-digit market start from {name!r}")
    return int(m.group(1))


def _fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.12g}"


@dataclass
class Inventory:
    up_shares: float = 0.0
    up_cost: float = 0.0
    down_shares: float = 0.0
    down_cost: float = 0.0

    def shares(self, side: str) -> float:
        return self.up_shares if side == "UP" else self.down_shares

    def avg(self, side: str) -> float | None:
        qty = self.shares(side)
        if qty <= 0:
            return None
        return (self.up_cost if side == "UP" else self.down_cost) / qty

    def add(self, side: str, qty: float, price: float) -> None:
        if side == "UP":
            self.up_shares += qty
            self.up_cost += qty * price
        else:
            self.down_shares += qty
            self.down_cost += qty * price

    def gap_signed(self) -> float:
        return self.up_shares - self.down_shares

    def gap(self) -> float:
        return abs(self.gap_signed())

    def underweight(self) -> str | None:
        gap = self.gap_signed()
        if gap > 1e-9:
            return "DOWN"
        if gap < -1e-9:
            return "UP"
        return None

    def heavy(self) -> str | None:
        under = self.underweight()
        if under is None:
            return None
        return "DOWN" if under == "UP" else "UP"

    def matched(self) -> float:
        return min(self.up_shares, self.down_shares)

    def pair_basis(self) -> float | None:
        up = self.avg("UP")
        down = self.avg("DOWN")
        return None if up is None or down is None else up + down

    def spend(self) -> float:
        return self.up_cost + self.down_cost


@dataclass
class Config:
    clip: float = DEFAULT_CLIP
    tick: float = DEFAULT_TICK
    start_age: float = DEFAULT_START_AGE
    cadence: float = DEFAULT_CADENCE
    stack_step: float = DEFAULT_STACK_STEP
    max_gap: float = DEFAULT_MAX_GAP
    accumulation_stop: float = DEFAULT_ACCUMULATION_STOP
    end_age: float = DEFAULT_END_AGE
    final_gap: float = DEFAULT_FINAL_GAP


def _maker_price(observed_ask: float, tick: float) -> float:
    return max(tick, min(1.0 - tick, round(observed_ask - tick, 10)))


def backtest_market(name: str, fh: TextIO, config: Config) -> tuple[dict, list[dict]]:
    start = _market_start_from_name(name)
    rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"empty market CSV: {name}")

    inv = Inventory()
    fills: list[dict] = []
    last_fill_ts = -1e100
    last_side_price: dict[str, float | None] = {"UP": None, "DOWN": None}
    max_gap_seen = 0.0

    for row in rows:
        ts = float(row["timestamp"])
        age = ts - start
        if age < config.start_age or age > config.end_age:
            continue
        if ts - last_fill_ts < config.cadence:
            continue

        asks = {"UP": float(row["up_price"]), "DOWN": float(row["down_price"])}
        if any((not math.isfinite(p)) or p <= 0 or p >= 1.0000001 for p in asks.values()):
            continue
        prices = {side: _maker_price(ask, config.tick) for side, ask in asks.items()}

        under = inv.underweight()
        reason = ""

        if age >= config.accumulation_stop:
            if under is None or inv.gap() <= config.final_gap + 1e-9:
                continue
            side = under
            reason = "late underweight repair"
        elif under is None:
            side = "UP" if prices["UP"] <= prices["DOWN"] else "DOWN"
            reason = "balanced seed cheaper side"
        else:
            heavy = inv.heavy()
            assert heavy is not None
            heavy_last = last_side_price[heavy]
            can_stack_heavy = (
                inv.gap() < config.max_gap - 1e-9
                and heavy_last is not None
                and prices[heavy] <= heavy_last - config.stack_step + 1e-9
            )
            if can_stack_heavy:
                side = heavy
                reason = f"heavy side cheaper by >= {config.stack_step:.2f}"
            else:
                side = under
                reason = "underweight repair"

        projected_up = inv.up_shares + (config.clip if side == "UP" else 0.0)
        projected_down = inv.down_shares + (config.clip if side == "DOWN" else 0.0)
        if abs(projected_up - projected_down) > config.max_gap + 1e-9:
            under = inv.underweight()
            if under is None:
                continue
            side = under
            reason = "gap cap -> underweight repair"
            projected_up = inv.up_shares + (config.clip if side == "UP" else 0.0)
            projected_down = inv.down_shares + (config.clip if side == "DOWN" else 0.0)
            if abs(projected_up - projected_down) > config.max_gap + 1e-9:
                continue

        price = prices[side]
        inv.add(side, config.clip, price)
        last_side_price[side] = price
        last_fill_ts = ts
        max_gap_seen = max(max_gap_seen, inv.gap())
        fills.append({
            "market": Path(name).name,
            "start_epoch": start,
            "age_s": age,
            "side": side,
            "qty": config.clip,
            "observed_ask": asks[side],
            "maker_price": price,
            "reason": reason,
            "up_shares": inv.up_shares,
            "down_shares": inv.down_shares,
            "gap_shares": inv.gap(),
            "up_avg": inv.avg("UP"),
            "down_avg": inv.avg("DOWN"),
            "pair_basis": inv.pair_basis(),
        })

    final_up = float(rows[-1]["up_price"])
    final_down = float(rows[-1]["down_price"])
    winner = "UP" if final_up >= final_down else "DOWN"
    matched = inv.matched()
    pair_basis = inv.pair_basis()
    residual_up = inv.up_shares - matched
    residual_down = inv.down_shares - matched
    winner_residual = residual_up if winner == "UP" else residual_down
    settlement_proceeds = matched + winner_residual
    settlement_pnl = settlement_proceeds - inv.spend()
    matched_edge = 0.0 if pair_basis is None else matched * (1.0 - pair_basis)

    result = {
        "market": Path(name).name,
        "start_epoch": start,
        "fills": len(fills),
        "up_fills": sum(1 for f in fills if f["side"] == "UP"),
        "down_fills": sum(1 for f in fills if f["side"] == "DOWN"),
        "both_sides": inv.up_shares > 0 and inv.down_shares > 0,
        "first_fill_age_s": None if not fills else fills[0]["age_s"],
        "last_fill_age_s": None if not fills else fills[-1]["age_s"],
        "max_gap_shares": max_gap_seen,
        "up_shares": inv.up_shares,
        "down_shares": inv.down_shares,
        "up_avg": inv.avg("UP"),
        "down_avg": inv.avg("DOWN"),
        "matched_shares": matched,
        "pair_basis": pair_basis,
        "residual_gap_shares": abs(residual_up - residual_down),
        "winner": winner,
        "gross_spend": inv.spend(),
        "matched_gross_edge": matched_edge,
        "settlement_pnl": settlement_pnl,
    }
    return result, fills


def iter_market_files(path: Path):
    if path.is_dir():
        for csv_path in sorted(path.glob("*.csv")):
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                yield csv_path.name, fh
        return
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            for name in sorted(n for n in zf.namelist() if n.lower().endswith(".csv")):
                with zf.open(name) as raw:
                    yield name, io.TextIOWrapper(raw, encoding="utf-8", newline="")
        return
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            yield path.name, fh
        return
    raise ValueError(f"input must be a CSV, directory of CSVs, or zip of CSVs: {path}")


def aggregate(markets: list[dict]) -> dict:
    active = [m for m in markets if m["fills"] > 0]
    pairable = [m for m in markets if m["pair_basis"] is not None and m["matched_shares"] > 0]
    total_matched = sum(m["matched_shares"] for m in pairable)
    weighted_basis = None if total_matched <= 0 else (
        sum(m["matched_shares"] * m["pair_basis"] for m in pairable) / total_matched
    )
    total_spend = sum(m["gross_spend"] for m in markets)
    total_pnl = sum(m["settlement_pnl"] for m in markets)
    return {
        "markets": len(markets),
        "markets_with_fills": len(active),
        "both_sides_markets": sum(bool(m["both_sides"]) for m in markets),
        "both_sides_rate": None if not markets else sum(bool(m["both_sides"]) for m in markets) / len(markets),
        "fills_total": sum(m["fills"] for m in markets),
        "fills_mean": _mean(m["fills"] for m in markets),
        "fills_median": _median(m["fills"] for m in markets),
        "fills_min": None if not markets else min(m["fills"] for m in markets),
        "fills_max": None if not markets else max(m["fills"] for m in markets),
        "first_fill_median_s": _median(m["first_fill_age_s"] for m in active if m["first_fill_age_s"] is not None),
        "last_fill_median_s": _median(m["last_fill_age_s"] for m in active if m["last_fill_age_s"] is not None),
        "max_gap_median_shares": _median(m["max_gap_shares"] for m in active),
        "max_gap_max_shares": None if not active else max(m["max_gap_shares"] for m in active),
        "matched_shares_total": total_matched,
        "matched_shares_median": _median(m["matched_shares"] for m in pairable),
        "weighted_pair_basis": weighted_basis,
        "median_pair_basis": _median(m["pair_basis"] for m in pairable),
        "residual_gap_median_shares": _median(m["residual_gap_shares"] for m in markets),
        "residual_gap_mean_shares": _mean(m["residual_gap_shares"] for m in markets),
        "gross_spend": total_spend,
        "matched_gross_edge": sum(m["matched_gross_edge"] for m in markets),
        "settlement_pnl": total_pnl,
        "settlement_roi_on_spend": None if total_spend <= 0 else total_pnl / total_spend,
        "mean_market_pnl": _mean(m["settlement_pnl"] for m in markets),
        "median_market_pnl": _median(m["settlement_pnl"] for m in markets),
        "positive_market_rate": None if not markets else sum(m["settlement_pnl"] > 0 for m in markets) / len(markets),
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Backtest the empirical Gabagool BTC 5m replica")
    p.add_argument("input", type=Path, help="PredictionTicks CSV, directory, or zip")
    p.add_argument("--output", type=Path, default=Path("data/gabagool_empirical_backtest"))
    p.add_argument("--clip", type=float, default=DEFAULT_CLIP)
    p.add_argument("--tick", type=float, default=DEFAULT_TICK)
    p.add_argument("--start-age", type=float, default=DEFAULT_START_AGE)
    p.add_argument("--cadence", type=float, default=DEFAULT_CADENCE)
    p.add_argument("--stack-step", type=float, default=DEFAULT_STACK_STEP)
    p.add_argument("--max-gap", type=float, default=DEFAULT_MAX_GAP)
    p.add_argument("--accumulation-stop", type=float, default=DEFAULT_ACCUMULATION_STOP)
    p.add_argument("--end-age", type=float, default=DEFAULT_END_AGE)
    p.add_argument("--final-gap", type=float, default=DEFAULT_FINAL_GAP)
    args = p.parse_args()

    config = Config(
        clip=args.clip, tick=args.tick, start_age=args.start_age,
        cadence=args.cadence, stack_step=args.stack_step, max_gap=args.max_gap,
        accumulation_stop=args.accumulation_stop, end_age=args.end_age,
        final_gap=args.final_gap,
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    run_dir = args.output / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    market_results: list[dict] = []
    fill_results: list[dict] = []
    for name, fh in iter_market_files(args.input):
        result, fills = backtest_market(name, fh, config)
        market_results.append(result)
        fill_results.extend(fills)

    markets_csv = run_dir / "markets.csv"
    fills_csv = run_dir / "fills.csv"
    summary_json = run_dir / "summary.json"
    with markets_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=MARKET_FIELDS)
        w.writeheader()
        for row in market_results:
            w.writerow({k: _fmt(v) if isinstance(v, float) else v for k, v in row.items()})
    with fills_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FILL_FIELDS)
        w.writeheader()
        for row in fill_results:
            w.writerow({k: _fmt(v) if isinstance(v, float) else v for k, v in row.items()})

    agg = aggregate(market_results)
    result = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "strategy": "gabagool_empirical_replica_v1",
        "config": config.__dict__,
        "chain_reference_btc5m": CHAIN_REFERENCE,
        "aggregate": agg,
        "files": {"markets_csv": str(markets_csv), "fills_csv": str(fills_csv), "summary_json": str(summary_json)},
        "accounting": (
            "Gross historical paper accounting. Entry price assumes a passive maker fill one tick "
            "below the observed best ask. No fee/rebate is added. At settlement, matched UP+DOWN "
            "shares return $1 per pair and winner-side residual shares return $1 each."
        ),
    }
    summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print("GABAGOOL EMPIRICAL REPLICA BACKTEST")
    print(f"markets      {agg['markets']}")
    print(f"both sides   {agg['both_sides_markets']}/{agg['markets']} ({agg['both_sides_rate']:.1%})")
    print(f"fills        median={agg['fills_median']:.1f} mean={agg['fills_mean']:.2f} range={agg['fills_min']}-{agg['fills_max']}")
    print(f"timing       first median={agg['first_fill_median_s']:.2f}s | last median={agg['last_fill_median_s']:.2f}s")
    print(f"inventory    max-gap median={agg['max_gap_median_shares']:.1f}sh | residual median={agg['residual_gap_median_shares']:.1f}sh")
    print(f"matched      total={agg['matched_shares_total']:.0f}sh | median/market={agg['matched_shares_median']:.1f}sh")
    print(f"pair basis   weighted={agg['weighted_pair_basis']:.6f} | median={agg['median_pair_basis']:.6f}")
    print(f"matched edge ${agg['matched_gross_edge']:.2f}")
    print(f"settled pnl  ${agg['settlement_pnl']:.2f} | ROI/spend={agg['settlement_roi_on_spend']:.3%} | positive markets={agg['positive_market_rate']:.1%}")
    print("CHAIN BTC5m")
    print(
        f"fills median={CHAIN_REFERENCE['fill_rows_median']} | first={CHAIN_REFERENCE['first_fill_median_s']:.0f}s | "
        f"last={CHAIN_REFERENCE['last_fill_median_s']:.0f}s | weighted basis={CHAIN_REFERENCE['weighted_pair_basis']:.6f} | "
        f"residual median={CHAIN_REFERENCE['residual_gap_median_shares']:.1f}sh"
    )
    print(f"output       {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
