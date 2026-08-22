"""Pull exact public Polymarket activity for one Gabagool market.

Defaults target the first ETH 15m market visible in the DataDash sample:
    eth-updown-15m-1761728400
    2025-10-29 05:00-05:15 ET

The script deliberately uses Polymarket's public Data API, not the authenticated
CLOB /trades endpoint, because the task is to recover another public wallet's
historical onchain activity. It first discovers the market conditionId from the
wallet's trades in the exact 15-minute window, then pulls:

  * every activity row for that wallet + conditionId across full history
    (TRADE / SPLIT / MERGE / REDEEM / REWARD / rebates, etc.)
  * every public trade row for that wallet + conditionId with takerOnly=false

Identical rows are preserved. A single Polygon transaction can contain multiple
OrderFilled logs with identical price/size/timestamp, so this script never dedupes
rows merely because they look identical.

Usage:
    python -m tools.pull_gabagool_exact_market_activity

Optional:
    python -m tools.pull_gabagool_exact_market_activity \
      --wallet 0x... \
      --slug eth-updown-15m-1761728400 \
      --start 1761728400 \
      --end 1761729300
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

DEFAULT_WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"
DEFAULT_SLUG = "eth-updown-15m-1761728400"
DEFAULT_START = 1761728400
DEFAULT_END = 1761729300


def _get_json(base: str, path: str, params: dict[str, Any] | None = None, *, timeout: float = 30.0):
    qs = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    url = f"{base}{path}" + (f"?{qs}" if qs else "")
    req = Request(url, headers={"User-Agent": "gabagool-forensics/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed HTTPS API hosts
        body = resp.read().decode("utf-8")
    return json.loads(body), url


def _paged_activity(params: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    limit = 500
    offset = 0
    while True:
        page_params = dict(params)
        page_params.update({"limit": limit, "offset": offset})
        page, url = _get_json(DATA_API, "/activity", page_params)
        urls.append(url)
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected /activity response: {page!r}")
        rows.extend(x for x in page if isinstance(x, dict))
        if len(page) < limit:
            break
        offset += limit
        if offset > 5000:
            raise RuntimeError("activity pagination exceeded API offset cap; narrow the time window")
    return rows, urls


def _paged_trades(params: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    limit = 10000
    offset = 0
    while True:
        page_params = dict(params)
        page_params.update({"limit": limit, "offset": offset})
        page, url = _get_json(DATA_API, "/trades", page_params)
        urls.append(url)
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected /trades response: {page!r}")
        rows.extend(x for x in page if isinstance(x, dict))
        if len(page) < limit:
            break
        offset += limit
        if offset > 10000:
            raise RuntimeError("trade pagination exceeded API offset cap; narrow the time window")
    return rows, urls


def _iso(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def _num(x: Any) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _write_json(path: Path, obj: Any):
    path.write_text(json.dumps(obj, indent=2, sort_keys=False), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys: list[str] = []
    seen: set[str] = set()
    preferred = [
        "timestamp",
        "timestamp_iso_utc",
        "type",
        "side",
        "outcome",
        "outcomeIndex",
        "size",
        "price",
        "usdcSize",
        "conditionId",
        "asset",
        "slug",
        "title",
        "transactionHash",
        "proxyWallet",
    ]
    all_keys = set().union(*(r.keys() for r in rows))
    for k in preferred:
        if k in all_keys and k not in seen:
            keys.append(k)
            seen.add(k)
    for k in sorted(all_keys):
        if k not in seen:
            keys.append(k)
            seen.add(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _augment(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        x = dict(r)
        x["timestamp_iso_utc"] = _iso(x.get("timestamp"))
        out.append(x)
    return out


def _discover_condition(wallet: str, slug: str, start: int, end: int):
    seed_rows, seed_urls = _paged_activity(
        {
            "user": wallet,
            "start": start,
            "end": end,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "type": "TRADE",
        }
    )
    exact = [r for r in seed_rows if str(r.get("slug") or "") == slug]
    conds = {str(r.get("conditionId") or "") for r in exact if r.get("conditionId")}
    if len(conds) == 1:
        return conds.pop(), seed_rows, seed_urls, None
    if len(conds) > 1:
        raise RuntimeError(f"multiple conditionIds found for slug {slug}: {sorted(conds)}")

    # Fallback: Gamma market lookup by slug.
    market, gamma_url = _get_json(GAMMA_API, f"/markets/slug/{slug}")
    condition = str((market or {}).get("conditionId") or "") if isinstance(market, dict) else ""
    if not condition:
        raise RuntimeError(
            f"could not discover conditionId for {slug}; seed activity rows={len(seed_rows)}"
        )
    return condition, seed_rows, seed_urls, gamma_url


def _summary(wallet: str, slug: str, condition: str, activity: list[dict[str, Any]], trades: list[dict[str, Any]]):
    activity_types = Counter(str(r.get("type") or "") for r in activity)
    side_stats: dict[str, dict[str, Any]] = {}
    for outcome in ("Up", "Down"):
        rows = [
            r for r in trades
            if str(r.get("side") or "").upper() == "BUY"
            and str(r.get("outcome") or "").strip().lower() == outcome.lower()
        ]
        shares = sum(_num(r.get("size")) for r in rows)
        # /trades does not include usdcSize, so reconstruct notional from price*size.
        notional = sum(_num(r.get("size")) * _num(r.get("price")) for r in rows)
        side_stats[outcome.upper()] = {
            "trade_rows": len(rows),
            "unique_transaction_hashes": len({str(r.get("transactionHash") or "") for r in rows if r.get("transactionHash")}),
            "shares": shares,
            "reconstructed_cost": notional,
            "vwap": (notional / shares) if shares else None,
            "min_price": min((_num(r.get("price")) for r in rows), default=None),
            "max_price": max((_num(r.get("price")) for r in rows), default=None),
        }

    trade_ts = [_num(r.get("timestamp")) for r in trades if r.get("timestamp") is not None]
    return {
        "wallet": wallet,
        "slug": slug,
        "conditionId": condition,
        "activity_rows": len(activity),
        "activity_type_counts": dict(activity_types),
        "trade_rows_takerOnly_false": len(trades),
        "first_trade_timestamp": min(trade_ts) if trade_ts else None,
        "first_trade_iso_utc": _iso(min(trade_ts)) if trade_ts else None,
        "last_trade_timestamp": max(trade_ts) if trade_ts else None,
        "last_trade_iso_utc": _iso(max(trade_ts)) if trade_ts else None,
        "buy_side_stats": side_stats,
        "matched_gross_shares": min(side_stats["UP"]["shares"], side_stats["DOWN"]["shares"]),
        "gross_share_gap": abs(side_stats["UP"]["shares"] - side_stats["DOWN"]["shares"]),
        "important_note": (
            "Rows are preserved exactly as returned. Do not dedupe identical-looking same-hash rows; "
            "a single transaction may contain multiple OrderFilled logs. Data API trade rows establish "
            "wallet activity, but exact maker/taker classification and per-log fee reconciliation require "
            "exchange log decoding or authenticated CLOB data for the wallet itself."
        ),
    }


def main():
    p = argparse.ArgumentParser(description="Pull exact Gabagool wallet activity for one Polymarket market")
    p.add_argument("--wallet", default=DEFAULT_WALLET)
    p.add_argument("--slug", default=DEFAULT_SLUG)
    p.add_argument("--start", type=int, default=DEFAULT_START, help="seed market window start epoch")
    p.add_argument("--end", type=int, default=DEFAULT_END, help="seed market window end epoch")
    p.add_argument("--output", default="data/gabagool_exact_market")
    args = p.parse_args()

    out = Path(args.output) / f"{args.slug}_{int(time.time())}"
    out.mkdir(parents=True, exist_ok=True)

    print("PUBLIC DATA  Polymarket Data API; no wallet key or auth required")
    print(f"WALLET       {args.wallet}")
    print(f"TARGET       {args.slug}")
    print(f"SEED WINDOW  {args.start} -> {args.end}")
    print(f"OUTPUT       {out}")

    condition, seed_rows, seed_urls, gamma_url = _discover_condition(
        args.wallet, args.slug, args.start, args.end
    )
    print(f"CONDITION    {condition}")

    activity, activity_urls = _paged_activity(
        {
            "user": args.wallet,
            "market": condition,
            "start": 1,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
        }
    )
    activity = [r for r in activity if str(r.get("conditionId") or "") == condition]

    trades, trade_urls = _paged_trades(
        {
            "user": args.wallet,
            "market": condition,
            "start": 1,
            "takerOnly": "false",
        }
    )
    trades = [r for r in trades if str(r.get("conditionId") or "") == condition]
    trades.sort(key=lambda r: (_num(r.get("timestamp")), str(r.get("transactionHash") or "")))

    seed_aug = _augment(seed_rows)
    activity_aug = _augment(activity)
    trades_aug = _augment(trades)

    _write_json(out / "seed_window_activity.json", seed_aug)
    _write_json(out / "full_market_activity.json", activity_aug)
    _write_json(out / "all_market_trades_takerOnly_false.json", trades_aug)
    _write_csv(out / "full_market_activity.csv", activity_aug)
    _write_csv(out / "all_market_trades_takerOnly_false.csv", trades_aug)

    summary = _summary(args.wallet, args.slug, condition, activity, trades)
    summary["source_urls"] = {
        "seed_activity": seed_urls,
        "gamma_fallback": gamma_url,
        "full_activity": activity_urls,
        "all_trades": trade_urls,
    }
    _write_json(out / "summary.json", summary)

    print("\n=== EXACT MARKET SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print("\nFILES")
    for name in (
        "full_market_activity.csv",
        "all_market_trades_takerOnly_false.csv",
        "full_market_activity.json",
        "all_market_trades_takerOnly_false.json",
        "summary.json",
    ):
        print(f"  {out / name}")


if __name__ == "__main__":
    main()
