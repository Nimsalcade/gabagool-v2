"""Production entry point for the forensic-calibrated Gabagool engine.

    python -m src.main --dry-run
    python -m src.main --live

Live mode is deliberately strict: credentials must bind the expected wallet, a recent
wallet-matched split/merge proof must exist, stale orders are cancelled, real positions
are recovered, and only then does the scheduler start.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

from .capital import CapitalManager, KillSwitch
from .config import BotConfig, ConfigError
from .inventory import fetch_pusd_balance
from .ledger import Ledger
from .ops import Heartbeat
from .recovery import recover_wallet_state
from .sdk import build_secure_client, ensure_trading_approvals
from .window_manager import WindowManager

MERGE_PROOF = Path(".merge_proof")
MERGE_PROOF_MAX_AGE_S = 7 * 24 * 3600


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
        elif record.name == "recovery":
            color = self.YELLOW
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


def _require_wallet_merge_proof(wallet: str) -> None:
    if not MERGE_PROOF.exists():
        raise ConfigError(
            "no .merge_proof found; run `python -m tools.test_merge` with this wallet first"
        )
    try:
        data = json.loads(MERGE_PROOF.read_text())
        proof_wallet = str(data.get("wallet", ""))
        ts = float(data.get("ts", 0))
        tx = str(data.get("merge_tx", ""))
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"invalid .merge_proof: {exc}") from exc
    if proof_wallet.lower() != wallet.lower():
        raise ConfigError(
            f".merge_proof belongs to {proof_wallet or 'unknown wallet'}, but SDK bound {wallet}"
        )
    age = time.time() - ts
    if ts <= 0 or age < 0 or age > MERGE_PROOF_MAX_AGE_S:
        raise ConfigError(
            f".merge_proof is stale ({age/86400:.1f} days); rerun `python -m tools.test_merge`"
        )
    if not tx:
        raise ConfigError(".merge_proof has no merge transaction hash")
    logging.info("merge proof accepted | wallet=%s age=%.1fh tx=%s", wallet, age / 3600, tx)


async def amain(cfg: BotConfig) -> int:
    ledger = Ledger(cfg.db_path)
    capital = CapitalManager(cfg.capital, ledger=ledger)

    bc = await build_secure_client(cfg) if not cfg.dry_run else None
    client = bc.client if bc else await _public_ish_client(cfg)
    heartbeat: Heartbeat | None = None

    try:
        wm = WindowManager(client, cfg=cfg, capital=capital, ledger=ledger)

        if not cfg.dry_run:
            _require_wallet_merge_proof(bc.wallet)
            await ensure_trading_approvals(bc)

            # Production restart reconciliation: cancel anything this process does
            # not own, then reconstruct wallet positions before making a new order.
            recovery = await recover_wallet_state(client)

            bal = await fetch_pusd_balance(client)
            logging.info(
                "wallet %s | pUSD $%.2f | recovered committed $%.2f",
                bc.wallet,
                bal,
                recovery.committed_cost,
            )
            if bal != bal:
                logging.error("cannot determine pUSD balance — refusing live startup")
                return 2

            # If cash is below the configured trading floor but inventory exists,
            # preserve the process as settlement-only rather than abandoning state.
            if bal < cfg.capital.min_starting_pusd:
                if not (recovery.active or recovery.settlement_due):
                    logging.error(
                        "pUSD balance $%.2f below min_starting_pusd $%.2f",
                        bal,
                        cfg.capital.min_starting_pusd,
                    )
                    return 2
                logging.warning(
                    "cash below trading floor; entering SETTLEMENT-ONLY mode for recovered inventory"
                )
                for cid, seed in list(recovery.active.items()):
                    recovery.settlement_due[cid] = seed
                    recovery.active.pop(cid, None)
                cfg.capital.max_concurrent_windows = 0

            wm.apply_recovery(recovery)
            ledger.record_balance(bal)
            if cfg.heartbeat:
                heartbeat = Heartbeat(
                    cfg.private_key,
                    client.credentials,
                    bc.wallet,
                    signature_type=cfg.clob_signature_type,
                )
                await heartbeat.start()

        await wm.run_forever()
        return 0
    except KillSwitch:
        return 3
    except RuntimeError as exc:
        logging.getLogger("main").critical("startup/runtime safety failure: %s", exc)
        return 4
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
        description="Gabagool V4 — production complete-set accumulation engine"
    )
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--live", action="store_true")
    ap.add_argument("--config", default="config/default.yaml")
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
