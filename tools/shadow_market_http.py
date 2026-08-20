"""HTTP-only zero-money shadow runner.

Use this when the public CLOB WebSocket handshake is blocked or timing out on the
local network. It uses the official Polymarket Data API trade feed plus CLOB REST
order books. Maker fills are conservative because indexed trades can arrive late.

    python -m tools.shadow_market_http --asset btc --duration 300
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymarket import AsyncPublicClient

from src.config import BotConfig
from src.discovery import discover
from tools.shadow_market_auto import ResilientShadowMarket


class HttpShadowMarket(ResilientShadowMarket):
    async def start_stream(self) -> None:
        self.trade_source = "DATA_API_HTTP_ONLY"
        self._http_activation_ts = time.time()
        print("trade tape: HTTP-only mode — official Data API; no WebSocket required")
        self._http_task = asyncio.create_task(
            self._http_poll_loop(), name="shadow-http-trade-tape"
        )

    async def stop_stream(self) -> None:
        # Reuse the fallback shutdown/final-poll path by presenting the source
        # identifier it expects during cleanup, then restore the report label.
        report_source = self.trade_source
        self.trade_source = "DATA_API_HTTP_FALLBACK"
        try:
            await super().stop_stream()
        finally:
            self.trade_source = report_source

    def report(self) -> None:
        # Base metrics first, then explicit HTTP diagnostics.
        from tools.shadow_market import ShadowMarket
        ShadowMarket.report(self)
        print(f"trade_source: {self.trade_source}")
        print(f"http_sell_rows_seen: {self.http_trade_rows}")
        print(f"http_poll_errors: {self.http_poll_errors}")
        print(
            "http_note: trades are official but can be indexed late; pre-order trades "
            "are rejected, so maker fill estimates are intentionally conservative."
        )


async def amain(args) -> int:
    cfg = BotConfig.load("config/default.yaml")
    client = AsyncPublicClient()
    try:
        markets = await discover(client, (args.asset,), (args.duration,))
        candidates = [m for m in markets if m.seconds_to_end > 20]
        if not candidates:
            print("No suitable current market found. Retry after the next window opens.")
            return 2
        market = candidates[0]
        runner = HttpShadowMarket(client, market, cfg.strategy, max_spend=args.max_spend)
        await runner.run()
        return 0
    finally:
        await client.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="HTTP-only zero-money live shadow runner")
    ap.add_argument("--asset", choices=("btc", "eth", "sol", "xrp"), default="btc")
    ap.add_argument("--duration", type=int, choices=(300, 900), default=300)
    ap.add_argument("--max-spend", type=float, default=2000.0)
    args = ap.parse_args()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
