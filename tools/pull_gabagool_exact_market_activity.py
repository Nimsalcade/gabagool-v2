"""Pull exact public Polymarket activity for one Gabagool market.

Defaults target the first ETH 15m market visible in the DataDash sample:
    eth-updown-15m-1761728400
    2025-10-29 05:00-05:15 ET

The script uses Polymarket's public Data API. It discovers the conditionId from the
wallet's activity in the exact market window, then pulls the wallet + market activity
and public trade rows with takerOnly=false.

Important: identical rows are preserved. One Polygon transaction may contain more
than one identical-looking fill, so price/size/hash equality is not a safe dedupe key.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

DEFAULT_WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"
DEFAULT_SLUG = "eth-updown-15m-1761728400"
DEFAULT_START = 1761728400
DEFAULT_END = 1761729300


def _get_json(
    base: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
    attempts: int = 5,
):
    qs = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    url = f"{base}{path}" + (f"?{qs}" if qs else "")
    req = Request(
        url,
        headers={
            "User-Agent": "gabagool-forensics/1.1",
            "Accept": "application/json",
        },
    )

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed HTTPS hosts
                body = resp.read().decode("utf-8")
            return json.loads(body), url
        except HTTPError as exc:
            last_exc = exc
            # Data API occasionally returns transient 5xx responses on historical queries.
            if 500 <= exc.code < 600 and attempt < attempts:
                wait = min(8.0, 0.75 * (2 ** (attempt - 1)))
                print(f"RETRY       HTTP {exc.code} attempt {attempt}/{attempts} in {wait:.2f}s")
                time.sleep(wait)
                continue
            raise
        except URLError as exc:
            last_exc = exc
            if attempt < attempts:
                wait = min(8.0, 0.75 * (2 ** (attempt - 1)))
                print(f"RETRY       network error attempt {attempt}/{attempts} in {wait:.2f}s: {exc}")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"request failed: {url}: {last_exc}")


def _paged_activity(params: dict[str, Any], *, limit: int = 250) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
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
        if offset > 10000:
            raise RuntimeError("activity pagination exceeded API offset cap")
    return rows, urls


def _paged_trades(params: dict[str, Any], *, limit: int = 1000) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
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
            raise RuntimeError("trade pagination exceeded API offset cap")
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
    keys = [k for k in preferred if k in all_keys]
    keys.extend(sorted(k for k in all_keys if k not in keys))
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _augment(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
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
            "type": "TRADE",
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
        }
    )
    exact = [r for r in seed_rows if str(r.get("slug") or "") == slug]
    conds = {str(r.get("conditionId") or "") for r in exact if r.get("conditionId")}
    if len(conds) == 1:
        return conds.pop(), seed_rows, seed_urls, None
    if len(conds) > 1:
        raise RuntimeError(f"multiple conditionIds found for slug {slug}: {sorted(conds)}")

    market, gamma_url = _get_json(GAMMA_API, f"/markets/slug/{slug}")
    condition = str((market or {}).get("conditionId") or "") if isinstance(market, dict) else ""
    if not condition:
        raise RuntimeError(f"could not discover conditionId for {slug}; seed rows={len(seed_rows)}")
    return condition, seed_rows, seed_urls, gamma_url


def _pull_full_activity(wallet: str, condition: str, start: int, end: int):
    """Prefer an unbounded market-filtered query; fall back to a bounded lifecycle window.

    The previous build used start=1 + ascending sort on the full historical market query.
    That combination can trigger a Data API 500. It is unnecessary because the market
    conditionId already scopes the request. We request the market directly and sort locally.
    """
    params = {"user": wallet, "market": condition}
    try:
        return _paged_activity(params)
    except HTTPError as exc:
        if not (500 <= exc.code < 600):
            raise
        # A 24h post-close window safely captures trades plus normal merge/redeem lifecycle
        # for these 15m binary markets while avoiding a pathological whole-history query plan.
        bounded = {
            "user": wallet,
            "market": condition,
            "start": max(0, start - 3600),
            "end": end + 86400,
        }
        print("FALLBACK    /activity full-history query returned 5xx; using market lifecycle window")
        return _paged_activity(bounded)


def _summary(wallet: str, slug: str, condition: str, activity: list[dict[str, Any]], trades: list[dict[str, Any]]):
    activity_types = Counter(str(r.get("type") or "") for r in activity)
    side_stats: dict[str, dict[str, Any]] = {}
    for outcome in ("Up", "Down"):
        rows = [
            r
            for r in trades
            if str(r.get("side") or "").upper() == "BUY"
            and str(r.get("outcome") or "").strip().lower() == outcome.lower()
        ]
        shares = sum(_num(r.get("size")) for r in rows)
        notional = sum(_num(r.get("size")) * _num(r.get("price")) for r in rows)
        side_stats[outcome.upper()] = {
            "trade_rows": len(rows),
            "unique_transaction_hashes": len(
                {str(r.get("transactionHash") or "") for r in rows if r.get("transactionHash")}
            ),
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
            "Rows are preserved as returned; identical-looking same-hash rows are not deduped. "
            "Public Data API rows establish wallet activity. Exact exchange logIndex, maker/taker "
            "classification, and fee reconciliation require exchange receipt/log decoding."
        ),
    }


def main():
    p = argparse.ArgumentParser(description="Pull exact Gabagool wallet activity for one Polymarket market")
    p.add_argument("--wallet", default=DEFAULT_WALLET)
    p.add_argument("--slug", default=DEFAULT_SLUG)
    p.add_argument("--start", type=int, default=DEFAULT_START)
    p.add_argument("--end", type=int, default=DEFAULT_END)
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

    activity, activity_urls = _pull_full_activity(
        args.wallet, condition, args.start, args.end
    )
    activity = [r for r in activity if str(r.get("conditionId") or "") == condition]
    activity.sort(key=lambda r: (_num(r.get("timestamp")), str(r.get("transactionHash") or "")))

    # /trades does not document start/end filters. Do not send them.
    trades, trade_urls = _paged_trades(
        {
            "user": args.wallet,
            "market": condition,
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
