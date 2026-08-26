#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from tools.analyze_gabagool_one_market import (
    DATA_API,
    DEFAULT_SLUG,
    DEFAULT_WALLET,
    D,
    condition_id_of,
    derive_window,
    fetch_activity,
    gamma_market_by_slug,
    http_json,
    iso,
)

ZERO = D("0")

POSITIVE_CASH_TYPES = {
    "MERGE",
    "REDEEM",
    "REWARD",
    "MAKER_REBATE",
    "TAKER_REBATE",
    "REFERRAL_REWARD",
    "YIELD",
}
NEGATIVE_CASH_TYPES = {"SPLIT"}
REPORTED_ONLY_TYPES = {"CONVERSION", "DEPOSIT", "WITHDRAWAL"}
ALL_NONTRADE_TYPES = (
    "SPLIT,MERGE,REDEEM,REWARD,CONVERSION,YIELD,"
    "MAKER_REBATE,TAKER_REBATE,REFERRAL_REWARD"
)
REBATE_TYPES = "MAKER_REBATE,TAKER_REBATE,REFERRAL_REWARD,REWARD,YIELD"


def dec(v: Any) -> Decimal:
    if v is None or v == "":
        return ZERO
    return D(str(v))


def outcome(row: dict[str, Any]) -> str:
    x = str(row.get("outcome") or "").strip().upper()
    if x == "YES":
        return "UP"
    if x == "NO":
        return "DOWN"
    if x in ("UP", "DOWN"):
        return x
    idx = row.get("outcomeIndex")
    if idx == 0:
        return "UP"
    if idx == 1:
        return "DOWN"
    return ""


def scoped_to_market(row: dict[str, Any], condition_id: str, slug: str) -> bool:
    cid = str(row.get("conditionId") or "").lower()
    if cid and cid == condition_id.lower():
        return True
    if str(row.get("slug") or "") == slug:
        return True
    if str(row.get("eventSlug") or "") == slug:
        return True
    return False


def row_cash_direction(row: dict[str, Any]) -> tuple[str, Decimal]:
    typ = str(row.get("type") or "").upper()
    side = str(row.get("side") or "").upper()
    cash = dec(row.get("usdcSize"))
    if typ == "TRADE":
        if side == "BUY":
            return "OUT", cash
        if side == "SELL":
            return "IN", cash
        return "UNKNOWN", cash
    if typ in POSITIVE_CASH_TYPES:
        return "IN", cash
    if typ in NEGATIVE_CASH_TYPES:
        return "OUT", cash
    return "REPORTED_ONLY", cash


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "timestamp", "utc", "type", "side", "outcome", "size", "price",
        "price_x_size", "usdcSize", "cash_direction", "signed_cash",
        "conditionId", "asset", "transactionHash", "slug", "eventSlug",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, dict[str, Decimal | int]] = defaultdict(
        lambda: {"rows": 0, "size": ZERO, "usdc": ZERO, "signed_cash": ZERO}
    )
    inflow = ZERO
    outflow = ZERO
    reported_only = ZERO
    ledger_rows: list[dict[str, Any]] = []

    for row in rows:
        typ = str(row.get("type") or "").upper()
        side = str(row.get("side") or "").upper()
        size = dec(row.get("size"))
        price = dec(row.get("price"))
        api_cash = dec(row.get("usdcSize"))
        direction, cash = row_cash_direction(row)
        signed = ZERO
        if direction == "IN":
            signed = cash
            inflow += cash
        elif direction == "OUT":
            signed = -cash
            outflow += cash
        else:
            reported_only += cash

        b = by_type[typ]
        b["rows"] = int(b["rows"]) + 1
        b["size"] = dec(b["size"]) + size
        b["usdc"] = dec(b["usdc"]) + api_cash
        b["signed_cash"] = dec(b["signed_cash"]) + signed

        ts = int(row.get("timestamp") or 0)
        ledger_rows.append(
            {
                "timestamp": ts,
                "utc": iso(ts),
                "type": typ,
                "side": side,
                "outcome": outcome(row),
                "size": float(size),
                "price": float(price),
                "price_x_size": float(size * price),
                "usdcSize": float(api_cash),
                "cash_direction": direction,
                "signed_cash": float(signed),
                "conditionId": str(row.get("conditionId") or ""),
                "asset": str(row.get("asset") or ""),
                "transactionHash": str(row.get("transactionHash") or ""),
                "slug": str(row.get("slug") or ""),
                "eventSlug": str(row.get("eventSlug") or ""),
            }
        )

    ledger_rows.sort(key=lambda x: (x["timestamp"], x["transactionHash"], x["type"]))
    by_type_json = {
        k: {
            "rows": int(v["rows"]),
            "size_sum": float(dec(v["size"])),
            "usdc_sum": float(dec(v["usdc"])),
            "signed_cash_sum": float(dec(v["signed_cash"])),
        }
        for k, v in sorted(by_type.items())
    }
    return {
        "ledger_rows": ledger_rows,
        "by_type": by_type_json,
        "recognized_inflow": float(inflow),
        "recognized_outflow": float(outflow),
        "direct_net_cashflow": float(inflow - outflow),
        "reported_only_usdc": float(reported_only),
    }


def token_balance_from_activity(rows: list[dict[str, Any]]) -> dict[str, float]:
    bal = {"UP": ZERO, "DOWN": ZERO}
    merge_total = ZERO
    split_total = ZERO
    for row in rows:
        typ = str(row.get("type") or "").upper()
        side = str(row.get("side") or "").upper()
        out = outcome(row)
        size = dec(row.get("size"))
        if typ == "TRADE" and out in bal:
            if side == "BUY":
                bal[out] += size
            elif side == "SELL":
                bal[out] -= size
        elif typ == "MERGE":
            merge_total += size
        elif typ == "SPLIT":
            split_total += size
        elif typ == "REDEEM" and out in bal:
            bal[out] -= size
    # Binary SPLIT mints both sides; MERGE burns equal qty of both.
    for side in ("UP", "DOWN"):
        bal[side] += split_total - merge_total
    return {"UP": float(bal["UP"]), "DOWN": float(bal["DOWN"])}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build a full one-market Polymarket activity/cashflow ledger for Gabagool."
    )
    ap.add_argument("--wallet", default=DEFAULT_WALLET)
    ap.add_argument("--slug", default=DEFAULT_SLUG)
    ap.add_argument("--start", type=int, default=None)
    ap.add_argument("--duration", type=int, default=900)
    ap.add_argument("--lifecycle-days", type=int, default=30)
    ap.add_argument("--rebate-lookback-days", type=int, default=2)
    ap.add_argument("--rebate-lookforward-days", type=int, default=3)
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

    outdir = Path(args.out or f"gabagool_ledger_{args.slug}")
    outdir.mkdir(parents=True, exist_ok=True)

    # Fetch all trades in the market, not BUY-only.
    trades = fetch_activity(
        wallet=args.wallet,
        condition_id=condition_id,
        types="TRADE",
        side=None,
        start=start,
        end=end,
    )
    trades = [
        r for r in trades
        if start <= int(r.get("timestamp") or 0) <= end
        and scoped_to_market(r, condition_id, args.slug)
    ]

    lifecycle_end = end + args.lifecycle_days * 86400
    nontrade = fetch_activity(
        wallet=args.wallet,
        condition_id=condition_id,
        types=ALL_NONTRADE_TYPES,
        side=None,
        start=start,
        end=lifecycle_end,
    )
    nontrade = [r for r in nontrade if scoped_to_market(r, condition_id, args.slug)]

    # Some rebate/reward rows can be paid later or be returned without a useful
    # market filter. Pull a narrow wallet-wide window as an attribution cross-check.
    rebate_start = max(0, start - args.rebate_lookback_days * 86400)
    rebate_end = end + args.rebate_lookforward_days * 86400
    wallet_rebates = fetch_activity(
        wallet=args.wallet,
        condition_id=condition_id,
        types=REBATE_TYPES,
        side=None,
        start=rebate_start,
        end=rebate_end,
    )
    wallet_rebates_exact = [
        r for r in wallet_rebates if scoped_to_market(r, condition_id, args.slug)
    ]

    # Avoid duplicate exact-market rebate rows already present in nontrade.
    def key(r: dict[str, Any]) -> tuple[Any, ...]:
        return (
            r.get("timestamp"), r.get("type"), r.get("transactionHash"),
            r.get("size"), r.get("usdcSize"), r.get("asset"), r.get("outcomeIndex"),
        )

    seen = {key(r) for r in nontrade}
    for r in wallet_rebates_exact:
        if key(r) not in seen:
            nontrade.append(r)
            seen.add(key(r))

    all_rows = trades + nontrade
    all_rows.sort(key=lambda r: (int(r.get("timestamp") or 0), str(r.get("transactionHash") or "")))
    summary = summarize_rows(all_rows)

    buy_rows = [r for r in trades if str(r.get("side") or "").upper() == "BUY"]
    sell_rows = [r for r in trades if str(r.get("side") or "").upper() == "SELL"]

    def side_trade_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        qty = sum((dec(r.get("size")) for r in rows), ZERO)
        px_notional = sum((dec(r.get("size")) * dec(r.get("price")) for r in rows), ZERO)
        api_usdc = sum((dec(r.get("usdcSize")) for r in rows), ZERO)
        return {
            "rows": len(rows),
            "shares": float(qty),
            "price_x_size_sum": float(px_notional),
            "api_usdc_sum": float(api_usdc),
            "api_minus_price_x_size": float(api_usdc - px_notional),
        }

    summary.update(
        {
            "market": {
                "slug": args.slug,
                "condition_id": condition_id,
                "title": market.get("question") or market.get("title"),
                "start_utc": iso(start),
                "end_utc": iso(end),
            },
            "wallet": args.wallet,
            "trade_buy": side_trade_summary(buy_rows),
            "trade_sell": side_trade_summary(sell_rows),
            "token_balance_after_observed_activity": token_balance_from_activity(all_rows),
            "rebate_attribution_crosscheck": {
                "query_start_utc": iso(rebate_start),
                "query_end_utc": iso(rebate_end),
                "exact_market_rows": len(wallet_rebates_exact),
                "exact_market_usdc": float(sum((dec(r.get("usdcSize")) for r in wallet_rebates_exact), ZERO)),
                "note": "Only exact condition/slug matches are attributed to this market.",
            },
            "cashflow_semantics": {
                "included_as_inflow": sorted(POSITIVE_CASH_TYPES),
                "included_as_outflow": sorted(NEGATIVE_CASH_TYPES | {"TRADE BUY"}),
                "trade_sell_is_inflow": True,
                "reported_not_scored": sorted(REPORTED_ONLY_TYPES),
                "warning": "direct_net_cashflow is final PnL only if no economically valuable token balance remains and all market-attributable rebates/rewards are captured.",
            },
        }
    )

    write_csv(outdir / "cashflow_ledger.csv", summary.pop("ledger_rows"))
    (outdir / "all_activity_raw.json").write_text(
        json.dumps(
            {
                "market": market,
                "trades": trades,
                "nontrade_exact_market": nontrade,
                "rebate_crosscheck_exact_market": wallet_rebates_exact,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (outdir / "ledger_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"MARKET       {args.slug}")
    print(f"CONDITION    {condition_id}")
    print(f"WINDOW       {iso(start)} -> {iso(end)}")
    print("SOURCE       Polymarket Gamma + Data API only")
    print()
    print(f"BUY          rows={summary['trade_buy']['rows']:,} shares={summary['trade_buy']['shares']:.6f}")
    print(
        f"             price*size=${summary['trade_buy']['price_x_size_sum']:.6f} "
        f"API usdcSize=${summary['trade_buy']['api_usdc_sum']:.6f} "
        f"diff=${summary['trade_buy']['api_minus_price_x_size']:+.6f}"
    )
    print(f"SELL         rows={summary['trade_sell']['rows']:,} shares={summary['trade_sell']['shares']:.6f}")
    print(
        f"             API usdcSize=${summary['trade_sell']['api_usdc_sum']:.6f}"
    )
    print()
    for typ, x in summary["by_type"].items():
        print(
            f"{typ:16} rows={x['rows']:5d} size={x['size_sum']:12.6f} "
            f"usdc=${x['usdc_sum']:12.6f} signed=${x['signed_cash_sum']:+12.6f}"
        )
    print()
    print(f"INFLOW       ${summary['recognized_inflow']:.6f}")
    print(f"OUTFLOW      ${summary['recognized_outflow']:.6f}")
    print(f"DIRECT NET   ${summary['direct_net_cashflow']:+.6f}")
    tb = summary["token_balance_after_observed_activity"]
    print(f"TOKENS LEFT  UP={tb['UP']:.6f} DOWN={tb['DOWN']:.6f}")
    rb = summary["rebate_attribution_crosscheck"]
    print(f"REBATE MATCH rows={rb['exact_market_rows']} usdc=${rb['exact_market_usdc']:.6f}")
    print()
    print(f"FILES        {outdir.resolve()}")
    print("  cashflow_ledger.csv")
    print("  all_activity_raw.json")
    print("  ledger_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
