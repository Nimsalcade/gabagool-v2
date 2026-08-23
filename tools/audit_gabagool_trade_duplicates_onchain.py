#!/usr/bin/env python3
"""Audit exact-duplicate Polymarket Data API trade rows against Polygon OrderFilled logs.

Purpose
-------
The public /trades endpoint can return byte-for-byte identical rows sharing the same
transactionHash. Those rows MUST NOT be blindly deduplicated until we know whether
one API row corresponds to one distinct on-chain OrderFilled event.

This script:
  1) finds exact duplicate groups in trades_raw.json;
  2) samples/ranks duplicate-bearing transaction hashes;
  3) fetches Polygon receipts via JSON-RPC;
  4) decodes Polymarket V1 CTF Exchange OrderFilled logs;
  5) compares API multiplicity with matching on-chain event multiplicity.

It is read-only. No wallet keys, signatures, orders, approvals, merges, or redeems.

Default input/output target the Google Drive Desktop folder produced by
pull_gabagool_first_day_polymarket.py.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"
DAY = "2025-10-29"

# V1 contracts were the live contracts on 2025-10-29.
CTF_EXCHANGE_V1 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_CTF_EXCHANGE_V1 = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
EXCHANGES = {CTF_EXCHANGE_V1, NEG_RISK_CTF_EXCHANGE_V1}

# keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
ORDER_FILLED_TOPIC_V1 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

DEFAULT_RPCS = [
    "https://polygon-rpc.com",
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]


def canonical(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def addr_from_topic(topic: str) -> str:
    h = topic.lower().removeprefix("0x")
    return "0x" + h[-40:]


def words(data: str) -> list[int]:
    h = (data or "0x").removeprefix("0x")
    if len(h) % 64:
        raise ValueError(f"bad ABI data length {len(h)}")
    return [int(h[i:i+64], 16) for i in range(0, len(h), 64)]


def rpc_call(url: str, method: str, params: list, retries: int = 4):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "gabagool-forensics/1.0"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                obj = json.loads(r.read())
            if obj.get("error"):
                raise RuntimeError(obj["error"])
            return obj.get("result")
        except Exception as e:
            last = e
            if i + 1 < retries:
                time.sleep(min(5.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"RPC failed {url} {method}: {last}")


def get_receipt(txhash: str, rpc_urls: list[str]):
    errs = []
    for url in rpc_urls:
        try:
            r = rpc_call(url, "eth_getTransactionReceipt", [txhash])
            if r:
                return r, url
            errs.append(f"{url}: null receipt")
        except Exception as e:
            errs.append(f"{url}: {e}")
    raise RuntimeError("; ".join(errs))


def decode_order_filled(log: dict) -> dict | None:
    address = str(log.get("address") or "").lower()
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    if address not in EXCHANGES or len(topics) < 4 or topics[0] != ORDER_FILLED_TOPIC_V1:
        return None
    w = words(str(log.get("data") or "0x"))
    if len(w) < 5:
        return None
    maker_asset, taker_asset, maker_amt, taker_amt, fee = w[:5]
    maker = addr_from_topic(topics[2])
    taker = addr_from_topic(topics[3])
    # V1 asset convention: BUY => makerAssetId=0, takerAssetId=tokenId;
    # SELL => makerAssetId=tokenId, takerAssetId=0.
    if maker_asset == 0 and taker_asset != 0:
        side = "BUY"
        token_id = taker_asset
        cash = maker_amt / 1_000_000
        shares = taker_amt / 1_000_000
    elif taker_asset == 0 and maker_asset != 0:
        side = "SELL"
        token_id = maker_asset
        shares = maker_amt / 1_000_000
        cash = taker_amt / 1_000_000
    else:
        side = "OTHER"
        token_id = taker_asset or maker_asset
        shares = float("nan")
        cash = float("nan")
    price = cash / shares if shares and not math.isnan(shares) else float("nan")
    return {
        "log_index": int(str(log.get("logIndex") or "0x0"), 16),
        "exchange": address,
        "order_hash": topics[1],
        "maker": maker,
        "taker": taker,
        "side": side,
        "token_id": str(token_id),
        "shares": shares,
        "cash": cash,
        "price": price,
        "fee_raw": fee,
    }


def drive_folder(day: str) -> Path:
    root = Path.home() / "Library" / "CloudStorage"
    xs = [p / "My Drive" for p in sorted(root.glob("GoogleDrive-*")) if (p / "My Drive").is_dir()]
    if len(xs) != 1:
        raise RuntimeError("Expected exactly one Google Drive Desktop 'My Drive'; use --input/--output-dir")
    return xs[0] / f"gabagool-first-day-{day}"


def close(a: float, b: float, tol: float) -> bool:
    return abs(float(a) - float(b)) <= tol


def classify(api_n: int, chain_n: int) -> str:
    if chain_n == api_n:
        return "CHAIN_MATCHES_API_MULTIPLICITY"
    if api_n > 1 and chain_n == 1:
        return "LIKELY_API_DUPLICATION"
    if chain_n > api_n:
        return "CHAIN_HAS_MORE_MATCHING_EVENTS"
    if chain_n == 0:
        return "NO_MATCHING_CHAIN_EVENT"
    return "PARTIAL_MISMATCH"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, help="trades_raw.json")
    ap.add_argument("--output-dir", type=Path)
    ap.add_argument("--day", default=DAY)
    ap.add_argument("--wallet", default=WALLET)
    ap.add_argument("--rpc-url", action="append", dest="rpc_urls", help="repeatable; overrides built-in fallbacks")
    ap.add_argument("--max-txs", type=int, default=100, help="audit at most this many duplicate-bearing tx hashes; 0 = all")
    ap.add_argument("--size-tol", type=float, default=0.00001)
    ap.add_argument("--price-tol", type=float, default=0.00001)
    a = ap.parse_args()

    if not re.fullmatch(r"0x[a-fA-F0-9]{40}", a.wallet):
        ap.error("bad wallet")
    wallet = a.wallet.lower()

    try:
        folder = drive_folder(a.day)
    except Exception:
        folder = None
    inp = a.input.expanduser() if a.input else (folder / f"gabagool_{a.day}_trades_raw.json" if folder else None)
    if not inp or not inp.exists():
        print("ERROR: input file not found; pass --input", file=sys.stderr)
        return 2
    outdir = a.output_dir.expanduser() if a.output_dir else inp.parent
    outdir.mkdir(parents=True, exist_ok=True)
    rpc_urls = a.rpc_urls or DEFAULT_RPCS

    rows = json.loads(inp.read_text())
    if not isinstance(rows, list):
        raise RuntimeError("input must be JSON list")

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[canonical(r)].append(r)
    dup_groups = [g for g in groups.values() if len(g) > 1]
    tx_group_count = Counter(str(g[0].get("transactionHash") or "").lower() for g in dup_groups)
    txs = sorted(tx_group_count, key=lambda h: (-tx_group_count[h], h))
    if a.max_txs > 0:
        txs = txs[: a.max_txs]
    txset = set(txs)

    selected = [g for g in dup_groups if str(g[0].get("transactionHash") or "").lower() in txset]
    print(f"READ ONLY\nINPUT {inp}\nROWS {len(rows)}\nEXACT DISTINCT {len(groups)}\nEXTRA COPIES {len(rows)-len(groups)}")
    print(f"DUP GROUPS {len(dup_groups)}\nAUDIT TXS {len(txs)}")

    chain_by_tx: dict[str, list[dict]] = {}
    rpc_used: dict[str, str] = {}
    for i, tx in enumerate(txs, 1):
        try:
            receipt, used = get_receipt(tx, rpc_urls)
            ev = [x for x in (decode_order_filled(l) for l in receipt.get("logs", [])) if x]
            chain_by_tx[tx] = ev
            rpc_used[tx] = used
            print(f"RECEIPT {i}/{len(txs)} {tx} orderFilled={len(ev)}", flush=True)
        except Exception as e:
            chain_by_tx[tx] = []
            rpc_used[tx] = "ERROR: " + str(e)
            print(f"RECEIPT ERROR {tx}: {e}", flush=True)

    report = []
    for g in selected:
        r = g[0]
        tx = str(r.get("transactionHash") or "").lower()
        api_n = len(g)
        token = str(r.get("asset") or "")
        api_side = str(r.get("side") or "").upper()
        api_size = float(r.get("size") or 0)
        api_price = float(r.get("price") or 0)
        candidates = []
        for ev in chain_by_tx.get(tx, []):
            if ev["maker"] != wallet:
                continue
            if ev["side"] != api_side or ev["token_id"] != token:
                continue
            if not close(ev["shares"], api_size, a.size_tol):
                continue
            if not close(ev["price"], api_price, a.price_tol):
                continue
            candidates.append(ev)
        chain_n = len(candidates)
        report.append({
            "transactionHash": tx,
            "timestamp": r.get("timestamp"),
            "slug": r.get("slug"),
            "outcome": r.get("outcome"),
            "side": api_side,
            "asset": token,
            "size": api_size,
            "price": api_price,
            "api_exact_multiplicity": api_n,
            "matching_wallet_orderfilled_events": chain_n,
            "classification": classify(api_n, chain_n),
            "matching_log_indices": ";".join(str(x["log_index"]) for x in candidates),
            "matching_order_hashes": ";".join(x["order_hash"] for x in candidates),
            "rpc": rpc_used.get(tx, ""),
        })

    csv_path = outdir / f"gabagool_{a.day}_duplicate_chain_audit.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        keys = list(report[0]) if report else ["transactionHash"]
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader(); w.writerows(report)

    classes = Counter(r["classification"] for r in report)
    summary = {
        "wallet": wallet,
        "day": a.day,
        "input_rows": len(rows),
        "exact_distinct_rows": len(groups),
        "extra_exact_copies": len(rows) - len(groups),
        "duplicate_groups_total": len(dup_groups),
        "audited_transaction_hashes": len(txs),
        "audited_duplicate_groups": len(report),
        "classifications": dict(classes),
        "warning": "Do not globally deduplicate Data API rows unless the on-chain audit shows one OrderFilled event for repeated API copies. Distinct OrderFilled log indices/order hashes are distinct executions even when API fields are identical.",
        "output_csv": str(csv_path),
    }
    js_path = outdir / f"gabagool_{a.day}_duplicate_chain_audit_summary.json"
    js_path.write_text(json.dumps(summary, indent=2) + "\n")
    print("=== COMPLETE ===")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
