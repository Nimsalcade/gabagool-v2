"""
Entry point.

    python -m src.main --dry-run          # full pipeline, no real orders/txs
    python -m src.main --live             # requires .merge_proof (see below)

THE MERGE-PROOF GATE
--------------------
The exit mechanism is the strategy. So this bot REFUSES to trade live until
`python -m tools.test_merge` has performed a real ~$1 split→merge round trip
on-chain and written `.merge_proof`. The old bot bought $3,500 of inventory
against an exit door that had never once opened; this gate makes that
sequence impossible to repeat.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from .capital import CapitalManager, KillSwitch
from .config import BotConfig, ConfigError
from .inventory import fetch_pusd_balance
from .ledger import Ledger
from .ops import Heartbeat
from .sdk import build_secure_client, ensure_trading_approvals
from .window_manager import WindowManager

MERGE_PROOF = Path(".merge_proof")


def _setup_logging(level: str) -> None:
    # Silence HTTP request spam
    for noisy in ("httpx", "httpcore", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    handler = logging.StreamHandler()
    handler.setFormatter(_ColoredFormatter())
    logging.root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.root.handlers = [handler]


class _ColoredFormatter(logging.Formatter):
    """Color-code important lines; dim the noise."""

    BOLD = "\033[1m"
    GREY = "\033[2m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    RESET = "\033[0m"

    COLOR_BY_NAME: dict[str, str] = {
        "merge_engine": GREEN,
        "fills": CYAN,
        "capital": YELLOW,
    }

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        color = self.RESET

        # Merge events
        if record.name == "merge_engine":
            if "MERGED" in msg:
                color = self.BOLD + self.GREEN
            elif "FAIL" in msg:
                color = self.BOLD + self.RED

        # Fills
        elif record.name == "fills":
            color = self.CYAN

        # Window lifecycle
        elif record.name.startswith("maker."):
            if "window start" in msg or "window done" in msg:
                color = self.BOLD + self.BLUE
            elif "imbalance brake" in msg:
                color = self.MAGENTA
            elif "order rejected" in msg or "backing off" in msg:
                color = self.GREY
            elif "FILL" in msg:
                color = self.CYAN
            elif "HOLD" in msg:
                color = self.BOLD + self.YELLOW

        # Capital / kill switch
        elif record.name == "capital":
            if "kill" in msg.lower():
                color = self.BOLD + self.RED
            else:
                color = self.YELLOW

        # Merge session total
        elif "MERGE SESSION" in msg:
            color = self.BOLD + self.GREEN

        # Reconciliation
        elif "RECONCILIATION" in msg:
            color = self.BOLD + self.BLUE

        # Window manager
        elif record.name == "window_manager":
            if "REDEEMED" in msg:
                color = self.BOLD + self.MAGENTA
            elif "launched" in msg:
                color = self.BLUE

        # SDK
        elif record.name == "sdk":
            if "Non-fatal" in msg:
                color = self.GREY

        # Heartbeat / ops
        elif record.name in ("ops", "heartbeat"):
            color = self.GREY

        time_str = self.formatTime(record, "%H:%M:%S")
        return f"{self.GREY}{time_str}{self.RESET} {color}{msg}{self.RESET}"


async def amain(cfg: BotConfig) -> int:
    ledger = Ledger(cfg.db_path)
    capital = CapitalManager(cfg.capital, ledger=ledger)

    bc = await build_secure_client(cfg) if not cfg.dry_run else None
    client = bc.client if bc else await _public_ish_client(cfg)
    heartbeat: Heartbeat | None = None

    try:
        if not cfg.dry_run:
            await ensure_trading_approvals(bc)
            bal = await fetch_pusd_balance(client)
            logging.info("wallet %s | pUSD $%.2f", bc.wallet, bal)
            if bal < cfg.capital.min_starting_pusd:
                logging.error(
                    "pUSD balance $%.2f below min_starting_pusd $%.2f — "
                    "fund the wallet (or lower the floor) and restart.",
                    bal, cfg.capital.min_starting_pusd,
                )
                return 2
            ledger.record_balance(bal)
            if cfg.heartbeat:
                heartbeat = Heartbeat(
                    cfg.private_key, client.credentials, bc.wallet, signature_type=3
                )
                await heartbeat.start()

        wm = WindowManager(client, cfg=cfg, capital=capital, ledger=ledger)
        await wm.run_forever()
        return 0
    except KillSwitch:
        return 3
    finally:
        if heartbeat is not None:
            await heartbeat.stop()
        print(ledger.report())
        ledger.close()
        try:
            await client.close()
        except Exception:  # noqa: BLE001
            pass


async def _public_ish_client(cfg: BotConfig):
    """Dry-run still needs real market data; a key is NOT required."""
    if cfg.private_key:
        from .sdk import build_secure_client as _b
        try:
            return (await _b(cfg)).client
        except Exception:  # noqa: BLE001 — fall through to public reads
            pass
    from polymarket import AsyncPublicClient
    return AsyncPublicClient()


def main() -> None:
    ap = argparse.ArgumentParser(description="Gabagool v2 — pure spread farmer")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--live", action="store_true")
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument(
        "--i-understand-no-merge-proof",
        action="store_true",
        help="bypass the merge-proof gate (NOT recommended)",
    )
    args = ap.parse_args()

    try:
        cfg = BotConfig.load(args.config)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    cfg.dry_run = bool(args.dry_run)
    _setup_logging(cfg.log_level)

    if args.live:
        try:
            cfg.dry_run = False
            cfg.validate()
        except ConfigError as exc:
            print(f"CONFIG ERROR: {exc}", file=sys.stderr)
            sys.exit(2)
        if not MERGE_PROOF.exists() and not args.i_understand_no_merge_proof:
            print(
                "REFUSING TO GO LIVE: no .merge_proof found.\n"
                "Run `python -m tools.test_merge` first — it performs a ~$1\n"
                "split→merge round trip on-chain and writes the proof file.\n"
                "The exit mechanism must be demonstrated before any entry is made.",
                file=sys.stderr,
            )
            sys.exit(2)

    loop = asyncio.new_event_loop()
    main_task = loop.create_task(amain(cfg))

    def _stop(*_):
        logging.getLogger("main").info("signal received — graceful shutdown")
        main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # windows
            signal.signal(sig, _stop)

    try:
        rc = loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        rc = 0
    finally:
        loop.close()
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
