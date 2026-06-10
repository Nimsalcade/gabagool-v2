"""Print current on-chain positions and pUSD balance for the wallet.

    python -m tools.show_positions
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import BotConfig   # noqa: E402
from src.inventory import fetch_pusd_balance  # noqa: E402
from src.sdk import build_secure_client  # noqa: E402


async def main() -> int:
    cfg = BotConfig.load("config/default.yaml")
    cfg.dry_run = False
    cfg.validate()
    bc = await build_secure_client(cfg)
    client = bc.client
    try:
        bal = await fetch_pusd_balance(client)
        print(f"wallet {bc.wallet} | pUSD ${bal:.2f}\n")
        rows = 0
        paginator = client.list_positions(size_threshold=0.0, page_size=50)
        async for page in paginator:
            for p in page.items:
                rows += 1
                flags = "".join([
                    "M" if getattr(p, "mergeable", False) else "·",
                    "R" if getattr(p, "redeemable", False) else "·",
                ])
                print(f"[{flags}] {float(p.size):>10.2f} {p.outcome:<5} "
                      f"@{float(p.avg_price or 0):.3f}  {p.title or p.condition_id[:16]}")
        if not rows:
            print("(no open positions)")
        return 0
    finally:
        await client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
