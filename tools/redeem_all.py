"""Sweep ALL redeemable positions for the wallet into pUSD (gasless).

    python -m tools.redeem_all
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import BotConfig   # noqa: E402
from src.sdk import build_secure_client  # noqa: E402


async def main() -> int:
    cfg = BotConfig.load("config/default.yaml")
    cfg.dry_run = False
    cfg.validate()
    bc = await build_secure_client(cfg)
    client = bc.client
    try:
        seen: set[str] = set()
        paginator = client.list_positions(redeemable=True, size_threshold=0.0, page_size=50)
        async for page in paginator:
            for pos in page.items:
                cid = str(pos.condition_id)
                if cid in seen:
                    continue
                seen.add(cid)
                try:
                    handle = await client.redeem_positions(condition_id=cid)
                    out = await handle.wait()
                    print(f"redeemed {pos.title or cid[:14]} | tx={getattr(out,'transaction_hash','?')}")
                except Exception as exc:  # noqa: BLE001
                    print(f"FAILED   {pos.title or cid[:14]} | {exc}")
        if not seen:
            print("nothing redeemable right now")
        return 0
    finally:
        await client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
