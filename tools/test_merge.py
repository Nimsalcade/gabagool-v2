"""
Merge proof: a ~$1 on-chain round trip that exercises the exact settlement pipeline.

    pUSD -> split_position -> 1 Up + 1 Down -> merge_positions -> pUSD

No order is placed. On success `.merge_proof` is written; live mode requires it.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import BotConfig, ConfigError  # noqa: E402
from src.discovery import discover  # noqa: E402
from src.inventory import fetch_holding, fetch_pusd_balance  # noqa: E402
from src.sdk import build_secure_client, ensure_trading_approvals  # noqa: E402

AMOUNT_PAIRS = 1
AMOUNT_MICRO = AMOUNT_PAIRS * 1_000_000


async def main() -> int:
    try:
        cfg = BotConfig.load("config/default.yaml")
        cfg.dry_run = False
        cfg.validate()
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}")
        return 2

    print("1/6  Binding wallet…")
    bc = await build_secure_client(cfg)
    client = bc.client
    try:
        print(f"     wallet={bc.wallet} type={bc.wallet_type}")

        print("2/6  Ensuring approvals (idempotent)…")
        await ensure_trading_approvals(bc)

        bal0 = await fetch_pusd_balance(client)
        print(f"3/6  pUSD before: ${bal0:.2f}")
        if not bal0 >= 1.5:
            print("     Need ≥ $1.50 pUSD to run the proof. Fund and retry.")
            return 2

        print("4/6  Picking a live 15m market for the round trip…")
        markets = await discover(client, ("btc", "eth"), (900,))
        live = [m for m in markets if m.accepting_orders and m.seconds_to_end > 180]
        if not live:
            print("     No live 15m window found right now — retry in a minute.")
            return 2
        m = live[0]
        print(f"     using {m.slug} | condition {m.condition_id[:16]}…")

        print(f"5/6  SPLIT ${AMOUNT_PAIRS} → {AMOUNT_PAIRS} Up + {AMOUNT_PAIRS} Down…")
        h = await client.split_position(condition_id=m.condition_id, amount=AMOUNT_MICRO)
        out = await h.wait()
        print(f"     split confirmed | tx={getattr(out, 'transaction_hash', '?')}")

        holding = await _settled_holding(client, m.condition_id, want_min_pairs=AMOUNT_PAIRS)
        print(f"     on-chain now: Up {holding.up_shares:g} / Down {holding.down_shares:g}")

        print("6/6  MERGE max pairs back to pUSD…")
        h = await client.merge_positions(condition_id=m.condition_id, amount="max")
        out = await h.wait()
        tx = getattr(out, "transaction_hash", "?")
        print(f"     merge confirmed | tx={tx}")

        after = await _settled_holding(client, m.condition_id, want_min_pairs=0)
        bal1 = await fetch_pusd_balance(client)
        ok = after.pairs < holding.pairs and (bal1 >= bal0 - 0.02)
        print(
            f"     pUSD after: ${bal1:.2f} (Δ {bal1 - bal0:+.4f}) | "
            f"pairs {holding.pairs:g}→{after.pairs:g}"
        )

        if ok:
            Path(".merge_proof").write_text(json.dumps({
                "ts": time.time(),
                "wallet": bc.wallet,
                "condition_id": m.condition_id,
                "merge_tx": str(tx),
            }, indent=2))
            print("\nMERGE PROOF PASSED — .merge_proof written.")
            return 0
        print("\nMerge confirmed but on-chain deltas do not verify. Do NOT trade live.")
        return 1
    finally:
        await client.close()


async def _settled_holding(client, condition_id: str, *, want_min_pairs: float):
    holding = await fetch_holding(client, condition_id)
    for _ in range(8):
        if want_min_pairs > 0:
            if holding.pairs >= want_min_pairs and (holding.up_shares or holding.down_shares):
                break
        else:
            if holding.pairs == 0 and not holding.up_shares and not holding.down_shares:
                break
        await asyncio.sleep(3)
        holding = await fetch_holding(client, condition_id)
    return holding


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
