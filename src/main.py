"""Entry point for the forensic-calibrated Gabagool replica.

    python -m src.main --dry-run
    python -m src.main --live

Live mode still requires a fresh, real split->merge proof. Strategy reconstruction never
weakens the operational safety gate.
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
    for noisy in ("httpx", "httpcore", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    handler = logging.StreamHandler()
    handler.setFormatter(_ColoredFormatter())
    logging.root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.root.handlers = [handler]


class _ColoredFormatter(logging.Formatter):
    BOLD = "\033[1m"
    GREY = "\033[2m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        color = self.RESET
        if record.name == "merge_engine":
            color = self.BOLD + (self.GREEN if "MERGED" in msg else self.RED if "FAIL" in msg else self.RESET)
        elif record.name == "fills":
            color = self.CYAN
        elif record.name.startswith("maker."):
            if "window start" in msg or "window done" in msg:
                color = self.BOLD + self.BLUE
            elif "TAKER" in msg:
                color = self.BOLD + self.MAGENTA
            elif "order rejected" in msg:
                color = self.GREY
            elif "HOLD" in msg:
                color = self.BOLD + self.YELLOW
        elif record.name == "capital":
            color = self.BOLD + self.RED if "kill" in msg.lower() else self.YELLOW
        elif record.name == "window_manager":
            if "REDEEM" in msg:
                color = self.BOLD + self.MAGENTA
            elif "launched" in msg:
                color = self.BLUE
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
                    "pUSD balance $%.2f below min_starting_pusd $%.2f — fund or adjust floor",
                    bal,
                    cfg.capital.min_starting_pusd,
                )
                return 2
            ledger.record_balance(bal)
            if cfg.heartbeat:
                heartbeat = Heartbeat(
                    cfg.private_key,
                    client.credentials,
                    bc.wallet,
                    signature_type=cfg.clob_signature_type,
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
    if cfg.private_key:
        from .sdk import build_secure_client as _b
        try:
            return (await _b(cfg)).client
        except Exception:  # noqa: BLE001
            pass
    from polymarket import AsyncPublicClient
    return AsyncPublicClient()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Gabagool V3 — forensic-calibrated complete-set accumulation engine"
    )
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--live", action="store_true")
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument(
        "--i-understand-no-merge-proof",
        action="store_true",
        help="bypass merge-proof gate (not recommended)",
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
                "REFUSING LIVE MODE: no .merge_proof found.\n"
                "Run `python -m tools.test_merge` first.",
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
        except NotImplementedError:
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
