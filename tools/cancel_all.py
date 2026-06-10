"""Panic button: cancel every resting order for the wallet. Safe to run anytime.

    python -m tools.cancel_all
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
    try:
        resp = await bc.client.cancel_all()
        print(f"cancelled: {getattr(resp, 'canceled', resp)}")
        return 0
    finally:
        await bc.client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
