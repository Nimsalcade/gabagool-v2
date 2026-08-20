"""
Pre-flight verification. Run before every live session:

    python -m tools.check_setup

Checks, in order: config shape -> wallet binding -> relayer auth -> approvals ->
pUSD balance -> live market data -> merge proof freshness.
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
from src.inventory import fetch_pusd_balance  # noqa: E402
from src.sdk import build_secure_client, ensure_trading_approvals  # noqa: E402

OK, BAD = "  PASS ", "  FAIL "


async def main() -> int:
    failures = 0

    def report(ok: bool, label: str, detail: str = "") -> None:
        nonlocal failures
        print((OK if ok else BAD) + label + (f" — {detail}" if detail else ""))
        failures += 0 if ok else 1

    try:
        cfg = BotConfig.load("config/default.yaml")
        cfg.dry_run = False
        cfg.validate()
        report(True, "config", "credentials present and well-formed")
    except ConfigError as exc:
        report(False, "config", str(exc))
        print(f"\n{failures} check(s) failed.")
        return 2

    try:
        bc = await build_secure_client(cfg)
        report(True, "wallet binding", f"{bc.wallet} ({bc.wallet_type})")
    except Exception as exc:  # noqa: BLE001
        report(False, "wallet binding / relayer auth", str(exc)[:200])
        print(f"\n{failures} check(s) failed.")
        return 2
    client = bc.client

    try:
        try:
            await ensure_trading_approvals(bc)
            report(True, "trading approvals")
        except Exception as exc:  # noqa: BLE001
            report(False, "trading approvals", str(exc)[:200])

        bal = await fetch_pusd_balance(client)
        report(
            bal >= cfg.capital.min_starting_pusd,
            "pUSD balance",
            f"${bal:.2f} (floor ${cfg.capital.min_starting_pusd:.2f})",
        )

        try:
            markets = await discover(
                client, cfg.strategy.assets, cfg.strategy.durations
            )
            live = [m for m in markets if m.accepting_orders]
            report(
                bool(markets),
                "market discovery",
                f"{len(markets)} current 5m/15m markets found, {len(live)} accepting orders",
            )
            if live:
                book = await client.get_order_book(token_id=live[0].up_token_id)
                report(
                    bool(book.asks and book.bids),
                    "order book read",
                    f"{live[0].slug} tick={book.tick_size} min={book.min_order_size}",
                )
        except Exception as exc:  # noqa: BLE001
            report(False, "market data", str(exc)[:200])

        proof = Path(".merge_proof")
        if proof.exists():
            try:
                data = json.loads(proof.read_text())
                age_h = (time.time() - float(data.get("ts", 0))) / 3600
                same_wallet = str(data.get("wallet", "")).lower() == bc.wallet.lower()
                report(
                    same_wallet,
                    "merge proof",
                    f"{age_h:.1f}h old, tx={str(data.get('merge_tx'))[:18]}…"
                    + ("" if same_wallet else " (DIFFERENT WALLET — rerun test_merge)"),
                )
                if age_h > 72:
                    print("         note: proof older than 72h — consider re-running test_merge")
            except Exception:  # noqa: BLE001
                report(False, "merge proof", "unreadable — rerun tools.test_merge")
        else:
            report(False, "merge proof", "missing — run `python -m tools.test_merge`")
    finally:
        await client.close()

    print(f"\n{failures} check(s) failed." if failures else "\nAll checks passed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
