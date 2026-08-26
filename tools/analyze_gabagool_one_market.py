#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

getcontext().prec = 28

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

DEFAULT_WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"
DEFAULT_SLUG = "btc-updown-15m-1768666500"

D = Decimal
ZERO = D("0")
ONE = D("1")


@dataclass
class Lot:
    qty: Decimal
    price: Decimal
    ts: int
    seq: int
    tx_hash: str
    outcome: str


def dec(v: Any) -> Decimal:
    if v is None or v == "":
        return ZERO
    return D(str(v))


def f(v: Decimal | None) -> float | None:
    return None if v is None else float(v)


def iso(ts: int | float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def http_json(url: str, params: dict[str, Any] | None = None, retries: int = 6) -> Any:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    last: Exception | None = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "gabagool-market-forensic/1.0",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
            if isinstance(exc, urllib.error.HTTPError) and exc.code < 500 and exc.code != 429:
                body = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
            time.sleep(min(8.0, 0.8 * (2 ** attempt)))
    raise RuntimeError(f"request failed after {retries} attempts: {last}")


def gamma_market_by_slug(slug: str) -> dict[str, Any]:
    try:
        m = http_json(f"{GAMMA_API}/markets/slug/{urllib.parse.quote(slug)}")
        if isinstance(m, dict) and m:
            return m
    except Exception:
        pass

    rows = http_json(f"{GAMMA_API}/markets", {"slug": slug, "limit": 10})
    if isinstance(rows, list):
        for m in rows:
            if str(m.get("slug", "")) == slug:
                return m
    raise RuntimeError(f"could not resolve market slug via Polymarket Gamma: {slug}")


def condition_id_of(m: dict[str, Any]) -> str:
    for key in ("conditionId", "condition_id", "conditionID"):
        v = m.get(key)
        if v:
            return str(v)
    raise RuntimeError("Gamma market response did not contain conditionId")


def derive_window(slug: str, market: dict[str, Any], duration_s: int) -> tuple[int, int]:
    mt = re.search(r"-(\d{10})$", slug)
    if mt:
        start = int(mt.group(1))
        return start, start + duration_s

    for key in ("startDate", "start_date"):
        raw = market.get(key)
        if raw:
            try:
                start = int(datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp())
                return start, start + duration_s
            except Exception:
                pass
    raise RuntimeError("cannot derive market start; pass --start explicitly")


def _fetch_activity_window(
    *,
    wallet: str,
    condition_id: str,
    types: str,
    side: str | None,
    start: int,
    end: int,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Fetch one time window and split recursively if offset=5000 is exhausted."""
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        params: dict[str, Any] = {
            "user": wallet,
            "market": condition_id,
            "type": types,
            "start": start,
            "end": end,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "limit": limit,
            "offset": offset,
        }
        if side:
            params["side"] = side

        page = http_json(f"{DATA_API}/activity", params)
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected /activity response: {type(page).__name__}")

        rows.extend(page)
        if len(page) < limit:
            return rows

        if offset >= 5000:
            if end <= start:
                raise RuntimeError(
                    f"more than API offset capacity inside epoch second {start}; cannot split further"
                )
            mid = (start + end) // 2
            left = _fetch_activity_window(
                wallet=wallet,
                condition_id=condition_id,
                types=types,
                side=side,
                start=start,
                end=mid,
                limit=limit,
            )
            right = _fetch_activity_window(
                wallet=wallet,
                condition_id=condition_id,
                types=types,
                side=side,
                start=mid + 1,
                end=end,
                limit=limit,
            )
            return left + right

        offset += limit


def fetch_activity(
    *,
    wallet: str,
    condition_id: str,
    types: str,
    side: str | None,
    start: int,
    end: int,
) -> list[dict[str, Any]]:
    rows = _fetch_activity_window(
        wallet=wallet,
        condition_id=condition_id,
        types=types,
        side=side,
        start=start,
        end=end,
    )
    # IMPORTANT: never dedupe by transactionHash. One tx can contain multiple fills.
    indexed = list(enumerate(rows))
    indexed.sort(key=lambda x: (int(x[1].get("timestamp", 0)), x[0]))
    return [row for _, row in indexed]


def weighted_quantile(values_and_weights: list[tuple[Decimal, Decimal]], q: float) -> float | None:
    xs = [(v, w) for v, w in values_and_weights if w > ZERO]
    if not xs:
        return None
    xs.sort(key=lambda x: x[0])
    total = sum((w for _, w in xs), ZERO)
    target = D(str(q)) * total
    acc = ZERO
    for value, weight in xs:
        acc += weight
        if acc >= target:
            return float(value)
    return float(xs[-1][0])


def pct(n: Decimal, d: Decimal) -> float | None:
    return None if d <= ZERO else float(n / d * D("100"))


def percentile(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def analyze(
    *,
    trades: list[dict[str, Any]],
    market_start: int,
    market_end: int,
    pair_cap: Decimal,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    up_lots: deque[Lot] = deque()
    dn_lots: deque[Lot] = deque()

    up_qty = dn_qty = ZERO
    up_cost = dn_cost = ZERO
    matched_qty = matched_cost = ZERO
    pair_edge = ZERO

    fills_out: list[dict[str, Any]] = []
    pairs_out: list[dict[str, Any]] = []
    tx_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

    last_side: str | None = None
    run_len = 0
    runs: list[int] = []
    all_gaps: list[float] = []
    last_ts: int | None = None
    pair_seq = 0

    for seq, row in enumerate(trades, start=1):
        side = str(row.get("side") or "").upper()
        if side != "BUY":
            continue

        outcome = str(row.get("outcome") or "").strip().upper()
        if outcome == "YES":
            outcome = "UP"
        elif outcome == "NO":
            outcome = "DOWN"
        if outcome not in ("UP", "DOWN"):
            idx = row.get("outcomeIndex")
            if idx == 0:
                outcome = "UP"
            elif idx == 1:
                outcome = "DOWN"
            else:
                raise RuntimeError(f"cannot determine UP/DOWN for row: {row}")

        ts = int(row.get("timestamp") or 0)
        qty = dec(row.get("size"))
        price = dec(row.get("price"))
        cost = qty * price
        tx = str(row.get("transactionHash") or "")
        if qty <= ZERO:
            continue

        if last_ts is not None:
            all_gaps.append(float(ts - last_ts))
        if outcome != last_side:
            if last_side is not None:
                runs.append(run_len)
            run_len = 1
        else:
            run_len += 1
        last_side = outcome
        last_ts = ts

        opp_lots = dn_lots if outcome == "UP" else up_lots
        own_lots = up_lots if outcome == "UP" else dn_lots

        opposite_qty_before = sum((x.qty for x in opp_lots), ZERO)
        opposite_head_price = opp_lots[0].price if opp_lots else None
        safe_ceiling_before = pair_cap - opposite_head_price if opposite_head_price is not None else None

        remaining = qty
        fill_close_qty = ZERO
        fill_pair_cost = ZERO
        fill_locked_edge = ZERO

        while remaining > ZERO and opp_lots:
            lot = opp_lots[0]
            take = min(remaining, lot.qty)
            pair_cost = price + lot.price
            edge = ONE - pair_cost

            pair_seq += 1
            pairs_out.append(
                {
                    "pair_seq": pair_seq,
                    "fill_seq": seq,
                    "timestamp": ts,
                    "utc": iso(ts),
                    "market_age_s": ts - market_start,
                    "incoming_side": outcome,
                    "incoming_price": float(price),
                    "incoming_tx": tx,
                    "opposite_side": lot.outcome,
                    "opposite_price": float(lot.price),
                    "opposite_source_fill_seq": lot.seq,
                    "opposite_source_utc": iso(lot.ts),
                    "opposite_source_tx": lot.tx_hash,
                    "matched_qty": float(take),
                    "pair_cost": float(pair_cost),
                    "pair_edge_per_share": float(edge),
                    "pair_edge_total": float(edge * take),
                    "lt_1": pair_cost < ONE,
                    "le_099": pair_cost <= D("0.99"),
                    "le_098": pair_cost <= D("0.98"),
                    "le_097": pair_cost <= D("0.97"),
                    "le_096": pair_cost <= D("0.96"),
                }
            )

            matched_qty += take
            matched_cost += take * pair_cost
            pair_edge += take * edge
            fill_close_qty += take
            fill_pair_cost += take * pair_cost
            fill_locked_edge += take * edge

            remaining -= take
            lot.qty -= take
            if lot.qty <= D("0.000000000001"):
                opp_lots.popleft()

        if remaining > ZERO:
            own_lots.append(Lot(remaining, price, ts, seq, tx, outcome))

        if outcome == "UP":
            up_qty += qty
            up_cost += cost
        else:
            dn_qty += qty
            dn_cost += cost

        u_unmatched = sum((x.qty for x in up_lots), ZERO)
        d_unmatched = sum((x.qty for x in dn_lots), ZERO)
        u_unmatched_cost = sum((x.qty * x.price for x in up_lots), ZERO)
        d_unmatched_cost = sum((x.qty * x.price for x in dn_lots), ZERO)

        out = {
            "seq": seq,
            "timestamp": ts,
            "utc": iso(ts),
            "market_age_s": ts - market_start,
            "side": side,
            "outcome": outcome,
            "shares": float(qty),
            "price": float(price),
            "fill_cost": float(cost),
            "tx_hash": tx,
            "asset": str(row.get("asset") or ""),
            "condition_id": str(row.get("conditionId") or ""),
            "running_up_shares": float(up_qty),
            "running_up_cost": float(up_cost),
            "running_up_vwap": float(up_cost / up_qty) if up_qty > ZERO else None,
            "running_down_shares": float(dn_qty),
            "running_down_cost": float(dn_cost),
            "running_down_vwap": float(dn_cost / dn_qty) if dn_qty > ZERO else None,
            "running_total_cost": float(up_cost + dn_cost),
            "running_gap_shares": float(up_qty - dn_qty),
            "opposite_unmatched_before": float(opposite_qty_before),
            "fifo_head_opposite_price_before": f(opposite_head_price),
            "pair_cap_ceiling_before": f(safe_ceiling_before),
            "close_qty_this_fill": float(fill_close_qty),
            "overshoot_qty_this_fill": float(remaining),
            "matched_pair_cost_this_fill": float(fill_pair_cost),
            "locked_edge_this_fill": float(fill_locked_edge),
            "cumulative_matched_qty": float(matched_qty),
            "cumulative_matched_cost": float(matched_cost),
            "cumulative_pair_vwap": float(matched_cost / matched_qty) if matched_qty > ZERO else None,
            "cumulative_locked_pair_pnl": float(pair_edge),
            "unmatched_up_qty": float(u_unmatched),
            "unmatched_down_qty": float(d_unmatched),
            "unmatched_up_cost": float(u_unmatched_cost),
            "unmatched_down_cost": float(d_unmatched_cost),
        }
        fills_out.append(out)
        tx_groups[tx or f"NO_TX_{seq}"].append(out)

    if run_len:
        runs.append(run_len)

    total_cost = up_cost + dn_cost
    leftover_up_qty = sum((x.qty for x in up_lots), ZERO)
    leftover_dn_qty = sum((x.qty for x in dn_lots), ZERO)
    leftover_up_cost = sum((x.qty * x.price for x in up_lots), ZERO)
    leftover_dn_cost = sum((x.qty * x.price for x in dn_lots), ZERO)
    leftover_cost = leftover_up_cost + leftover_dn_cost
    merge_return = matched_qty
    strict_pnl = merge_return - total_cost
    strict_roi = strict_pnl / total_cost if total_cost > ZERO else None

    tx_rows: list[dict[str, Any]] = []
    for tx, xs in tx_groups.items():
        qty = sum((D(str(x["shares"])) for x in xs), ZERO)
        cost = sum((D(str(x["fill_cost"])) for x in xs), ZERO)
        prices = sorted({x["price"] for x in xs})
        outcomes = sorted({x["outcome"] for x in xs})
        tx_rows.append(
            {
                "tx_hash": tx,
                "timestamp": xs[0]["timestamp"],
                "utc": xs[0]["utc"],
                "market_age_s": xs[0]["market_age_s"],
                "fill_rows": len(xs),
                "outcomes": ",".join(outcomes),
                "distinct_prices": len(prices),
                "prices": ",".join(f"{p:.4f}" for p in prices),
                "shares": float(qty),
                "cost": float(cost),
            }
        )
    tx_rows.sort(key=lambda x: (x["timestamp"], x["tx_hash"]))

    pair_weights = [(D(str(r["pair_cost"])), D(str(r["matched_qty"]))) for r in pairs_out]
    pair_total_qty = sum((w for _, w in pair_weights), ZERO)

    def qty_under(threshold: str, inclusive: bool = True) -> Decimal:
        t = D(threshold)
        total = ZERO
        for value, weight in pair_weights:
            if (value <= t) if inclusive else (value < t):
                total += weight
        return total

    fill_prices = [x["price"] for x in fills_out]
    fill_sizes = [x["shares"] for x in fills_out]
    tx_fill_counts = [x["fill_rows"] for x in tx_rows]

    summary = {
        "market_window": {
            "start_epoch": market_start,
            "start_utc": iso(market_start),
            "end_epoch": market_end,
            "end_utc": iso(market_end),
        },
        "fills": {
            "buy_fill_rows": len(fills_out),
            "unique_transactions": len(tx_rows),
            "first_fill_utc": fills_out[0]["utc"] if fills_out else None,
            "first_fill_age_s": fills_out[0]["market_age_s"] if fills_out else None,
            "last_fill_utc": fills_out[-1]["utc"] if fills_out else None,
            "last_fill_age_s": fills_out[-1]["market_age_s"] if fills_out else None,
            "price_min": min(fill_prices) if fill_prices else None,
            "price_median": statistics.median(fill_prices) if fill_prices else None,
            "price_max": max(fill_prices) if fill_prices else None,
            "size_median": statistics.median(fill_sizes) if fill_sizes else None,
            "size_p90": percentile(fill_sizes, 0.90),
            "same_timestamp_gap_zero_pct": (
                100.0 * sum(1 for x in all_gaps if x == 0) / len(all_gaps)
                if all_gaps else None
            ),
            "interfill_gap_median_s": statistics.median(all_gaps) if all_gaps else None,
            "interfill_gap_p90_s": percentile(all_gaps, 0.90),
            "same_side_run_median": statistics.median(runs) if runs else None,
            "same_side_run_p90": percentile([float(x) for x in runs], 0.90),
            "same_side_run_max": max(runs) if runs else None,
            "tx_fill_rows_median": statistics.median(tx_fill_counts) if tx_fill_counts else None,
            "tx_fill_rows_p90": percentile([float(x) for x in tx_fill_counts], 0.90),
            "tx_fill_rows_max": max(tx_fill_counts) if tx_fill_counts else None,
        },
        "up": {
            "shares": float(up_qty),
            "cost": float(up_cost),
            "vwap": float(up_cost / up_qty) if up_qty > ZERO else None,
        },
        "down": {
            "shares": float(dn_qty),
            "cost": float(dn_cost),
            "vwap": float(dn_cost / dn_qty) if dn_qty > ZERO else None,
        },
        "session_accounting": {
            "total_filled_shares": float(up_qty + dn_qty),
            "total_fill_cost": float(total_cost),
            "matched_qty": float(matched_qty),
            "merge_return_at_1_per_set": float(merge_return),
            "matched_cost_basis_fifo": float(matched_cost),
            "completed_pair_vwap_fifo": float(matched_cost / matched_qty) if matched_qty > ZERO else None,
            "locked_pair_pnl_fifo": float(pair_edge),
            "leftover_up_qty_fifo": float(leftover_up_qty),
            "leftover_up_cost_fifo": float(leftover_up_cost),
            "leftover_down_qty_fifo": float(leftover_dn_qty),
            "leftover_down_cost_fifo": float(leftover_dn_cost),
            "leftover_total_cost_fifo": float(leftover_cost),
            "strict_merge_only_pnl": float(strict_pnl),
            "strict_merge_only_roi_pct": float(strict_roi * D("100")) if strict_roi is not None else None,
            "identity_error": float(total_cost - (matched_cost + leftover_cost)),
        },
        "pair_cost_distribution_fifo_share_weighted": {
            "pair_cap_for_test": float(pair_cap),
            "p10": weighted_quantile(pair_weights, 0.10),
            "p25": weighted_quantile(pair_weights, 0.25),
            "p50": weighted_quantile(pair_weights, 0.50),
            "p75": weighted_quantile(pair_weights, 0.75),
            "p90": weighted_quantile(pair_weights, 0.90),
            "p95": weighted_quantile(pair_weights, 0.95),
            "share_pct_lt_1": pct(qty_under("1.00", inclusive=False), pair_total_qty),
            "share_pct_le_099": pct(qty_under("0.99"), pair_total_qty),
            "share_pct_le_098": pct(qty_under("0.98"), pair_total_qty),
            "share_pct_le_097": pct(qty_under("0.97"), pair_total_qty),
            "share_pct_le_096": pct(qty_under("0.96"), pair_total_qty),
        },
        "notes": {
            "no_txhash_dedup": True,
            "timestamp_resolution": "Data API activity timestamp is integer epoch seconds",
            "fifo_pairing_is_analysis_convention": True,
            "strict_pnl_formula": "merge_return - total_cost_of_all_BUY_fills",
            "leftover_cost_is_already_inside_total_fill_cost": True,
        },
    }
    return fills_out, pairs_out, tx_rows, summary


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract and analyze one Gabagool Polymarket market using official Polymarket APIs only."
    )
    ap.add_argument("--wallet", default=DEFAULT_WALLET)
    ap.add_argument("--slug", default=DEFAULT_SLUG)
    ap.add_argument("--start", type=int, default=None, help="market start epoch; normally derived from slug")
    ap.add_argument("--duration", type=int, default=900)
    ap.add_argument("--pair-cap", type=Decimal, default=D("0.99"))
    ap.add_argument("--buffer", type=int, default=120, help="seconds around market window for TRADE retrieval")
    ap.add_argument("--lifecycle-days", type=int, default=14)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if not re.fullmatch(r"0x[a-fA-F0-9]{40}", args.wallet):
        ap.error("wallet must be a 0x-prefixed 40-hex address")

    market = gamma_market_by_slug(args.slug)
    condition_id = condition_id_of(market)

    if args.start is None:
        start, end = derive_window(args.slug, market, args.duration)
    else:
        start, end = args.start, args.start + args.duration

    outdir = Path(args.out or f"gabagool_market_{args.slug}")
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"MARKET      {args.slug}")
    print(f"CONDITION   {condition_id}")
    print(f"WALLET      {args.wallet}")
    print(f"WINDOW      {iso(start)} -> {iso(end)}")
    print("SOURCE      Polymarket Gamma + Data API only")
    print()

    trades = fetch_activity(
        wallet=args.wallet,
        condition_id=condition_id,
        types="TRADE",
        side="BUY",
        start=max(0, start - args.buffer),
        end=end + args.buffer,
    )
    trades = [
        r for r in trades
        if start <= int(r.get("timestamp") or 0) <= end
        and str(r.get("conditionId") or "").lower() == condition_id.lower()
        and str(r.get("side") or "").upper() == "BUY"
    ]

    lifecycle_end = end + args.lifecycle_days * 86400
    lifecycle = fetch_activity(
        wallet=args.wallet,
        condition_id=condition_id,
        types="MERGE,REDEEM,SPLIT",
        side=None,
        start=start,
        end=lifecycle_end,
    )

    fills, pairs, txs, summary = analyze(
        trades=trades,
        market_start=start,
        market_end=end,
        pair_cap=args.pair_cap,
    )

    observed = {
        "merge_rows": sum(1 for r in lifecycle if str(r.get("type")) == "MERGE"),
        "merge_size_sum": sum(float(r.get("size") or 0) for r in lifecycle if str(r.get("type")) == "MERGE"),
        "merge_usdc_sum": sum(float(r.get("usdcSize") or 0) for r in lifecycle if str(r.get("type")) == "MERGE"),
        "redeem_rows": sum(1 for r in lifecycle if str(r.get("type")) == "REDEEM"),
        "redeem_size_sum": sum(float(r.get("size") or 0) for r in lifecycle if str(r.get("type")) == "REDEEM"),
        "redeem_usdc_sum": sum(float(r.get("usdcSize") or 0) for r in lifecycle if str(r.get("type")) == "REDEEM"),
        "split_rows": sum(1 for r in lifecycle if str(r.get("type")) == "SPLIT"),
        "split_size_sum": sum(float(r.get("size") or 0) for r in lifecycle if str(r.get("type")) == "SPLIT"),
        "lifecycle_query_end_utc": iso(lifecycle_end),
    }

    summary["market"] = {
        "slug": args.slug,
        "condition_id": condition_id,
        "title": market.get("question") or market.get("title"),
        "outcomes": market.get("outcomes"),
    }
    summary["wallet"] = args.wallet
    summary["observed_lifecycle_activity"] = observed

    (outdir / "activity_raw.json").write_text(
        json.dumps({"trades": trades, "lifecycle": lifecycle}, indent=2),
        encoding="utf-8",
    )

    write_csv(
        outdir / "fills.csv",
        fills,
        [
            "seq", "timestamp", "utc", "market_age_s", "side", "outcome",
            "shares", "price", "fill_cost", "tx_hash", "asset", "condition_id",
            "running_up_shares", "running_up_cost", "running_up_vwap",
            "running_down_shares", "running_down_cost", "running_down_vwap",
            "running_total_cost", "running_gap_shares",
            "opposite_unmatched_before", "fifo_head_opposite_price_before",
            "pair_cap_ceiling_before", "close_qty_this_fill", "overshoot_qty_this_fill",
            "matched_pair_cost_this_fill", "locked_edge_this_fill",
            "cumulative_matched_qty", "cumulative_matched_cost",
            "cumulative_pair_vwap", "cumulative_locked_pair_pnl",
            "unmatched_up_qty", "unmatched_down_qty",
            "unmatched_up_cost", "unmatched_down_cost",
        ],
    )

    write_csv(
        outdir / "pairs_fifo.csv",
        pairs,
        [
            "pair_seq", "fill_seq", "timestamp", "utc", "market_age_s",
            "incoming_side", "incoming_price", "incoming_tx",
            "opposite_side", "opposite_price", "opposite_source_fill_seq",
            "opposite_source_utc", "opposite_source_tx", "matched_qty",
            "pair_cost", "pair_edge_per_share", "pair_edge_total",
            "lt_1", "le_099", "le_098", "le_097", "le_096",
        ],
    )

    write_csv(
        outdir / "transaction_groups.csv",
        txs,
        [
            "tx_hash", "timestamp", "utc", "market_age_s", "fill_rows",
            "outcomes", "distinct_prices", "prices", "shares", "cost",
        ],
    )

    (outdir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    sa = summary["session_accounting"]
    pc = summary["pair_cost_distribution_fifo_share_weighted"]
    print(f"BUY FILLS   {summary['fills']['buy_fill_rows']:,}")
    print(f"TXS         {summary['fills']['unique_transactions']:,}")
    print(
        f"UP          {summary['up']['shares']:.3f} sh | "
        f"cost ${summary['up']['cost']:.2f} | VWAP {summary['up']['vwap']:.4f}"
    )
    print(
        f"DOWN        {summary['down']['shares']:.3f} sh | "
        f"cost ${summary['down']['cost']:.2f} | VWAP {summary['down']['vwap']:.4f}"
    )
    print(f"TOTAL COST  ${sa['total_fill_cost']:.2f}")
    print(
        f"MERGE       {sa['matched_qty']:.3f} sets -> ${sa['merge_return_at_1_per_set']:.2f} | "
        f"FIFO pair VWAP {sa['completed_pair_vwap_fifo'] or 0:.4f}"
    )
    print(
        f"LEFTOVER    UP {sa['leftover_up_qty_fifo']:.3f} (${sa['leftover_up_cost_fifo']:.2f}) | "
        f"DOWN {sa['leftover_down_qty_fifo']:.3f} (${sa['leftover_down_cost_fifo']:.2f})"
    )
    print(
        f"STRICT PNL  ${sa['strict_merge_only_pnl']:+.2f} | "
        f"ROI {sa['strict_merge_only_roi_pct']:+.3f}%"
    )
    print(
        f"PAIR COST   p50={pc['p50']} p90={pc['p90']} | "
        f"<$1={pc['share_pct_lt_1']}% <=.99={pc['share_pct_le_099']}% "
        f"<=.98={pc['share_pct_le_098']}%"
    )
    print(
        f"OBS MERGE   rows={observed['merge_rows']} size={observed['merge_size_sum']:.3f} "
        f"usdc={observed['merge_usdc_sum']:.2f}"
    )
    print(
        f"OBS REDEEM  rows={observed['redeem_rows']} size={observed['redeem_size_sum']:.3f} "
        f"usdc={observed['redeem_usdc_sum']:.2f}"
    )
    print()
    print(f"FILES       {outdir.resolve()}")
    print("  activity_raw.json")
    print("  fills.csv")
    print("  pairs_fifo.csv")
    print("  transaction_groups.csv")
    print("  summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
