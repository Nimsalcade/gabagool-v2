"""
Harvest: merge all available pairs across every active market, then report
how much profit is available to withdraw above the configured floor.

    python -m tools.harvest           # report only
    python -m tools.harvest --merge   # force-merge everything first, then report

The harvest floor is set by `capital.harvest_floor_usd` in config (defaults to
`min_starting_pusd`). Everything above the floor is harvestable profit.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import BotConfig, ConfigError          # noqa: E402
from src.discovery import discover                      # noqa: E402
from src.inventory import fetch_holding, fetch_pusd_balance  # noqa: E402
from src.sdk import build_secure_client                 # noqa: E402


async def main() -> int:
    try:
        cfg = BotConfig.load("config/default.yaml")
        cfg.dry_run = False
        cfg.validate()
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}")
        return 2

    do_merge = "--merge" in sys.argv

    bc = await build_secure_client(cfg)
    client = bc.client
    try:
        # 1. Snapshot current balance
        bal_before = await fetch_pusd_balance(client)
        print(f"pUSD balance:  ${bal_before:.2f}")

        # 2. Force-merge all available pairs across every live market
        if do_merge:
            print("discovering markets…")
            markets = await discover(client, cfg.strategy.assets)
            live = [m for m in markets if m.accepting_orders or m.seconds_to_end >= -300]
            print(f"checking {len(live)} markets for mergeable pairs…")

            total_merged = 0.0
            merges = 0
            for m in live:
                holding = await fetch_holding(client, m.condition_id)
                if holding.pairs < 1:
                    continue
                print(f"  {m.slug}: {holding.pairs:.1f} pairs → ", end="", flush=True)
                try:
                    handle = await client.merge_positions(
                        condition_id=m.condition_id, amount="max"
                    )
                    outcome = await handle.wait()
                    tx = str(getattr(outcome, "transaction_hash", "")
                            or getattr(handle, "transaction_hash", ""))
                    print(f"merged ${holding.pairs:.2f} | tx={tx[:18]}…")
                    total_merged += holding.pairs
                    merges += 1
                except Exception as exc:
                    print(f"FAILED: {exc}")

            if merges:
                print(f"\nforce-merged {merges} market(s) → ${total_merged:.2f} pUSD returned")
            else:
                print("  nothing mergeable right now")

        # 3. Balance after (optional) merge
        bal = await fetch_pusd_balance(client)

        # 4. Harvest floor (0 = fall back to min_starting_pusd)
        floor = cfg.capital.harvest_floor_usd or cfg.capital.min_starting_pusd
        harvestable = max(0.0, bal - floor)

        print(f"\n{'─' * 50}")
        print(f"pUSD now:      ${bal:.2f}")
        print(f"Harvest floor:  ${floor:.2f}")
        if harvestable > 0:
            print(f"\n  \033[1;32mHARVESTABLE: ${harvestable:.2f}\033[0m")
            print(f"\n  To withdraw: log into polymarket.com → Wallet → Withdraw")
            print(f"  Send ${harvestable:.2f} pUSD to your Polygon address.")
        else:
            print(f"\n  Nothing above floor. Keep farming.")
        print(f"{'─' * 50}")

        return 0
    finally:
        await client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
