"""
Configuration: YAML defaults + environment overrides, validated at startup.

DESIGN RULES (learned from the post-mortem)
-------------------------------------------
1. There are NO directional parameters. No spike thresholds, no lean targets,
   no snipe sizing. The strategy cannot be configured into making a bet,
   because the knobs don't exist.
2. Live mode fails fast and loudly if any credential is missing or malformed.
   The old bot ran for hours throwing `AddressEncoder` errors because a whole
   ".env line" had been stuffed into an address field — we validate shapes here.
3. Everything money-related has a conservative default and an explicit cap.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_PRIVKEY_RE = re.compile(r"^(0x)?[a-fA-F0-9]{64}$")


class ConfigError(RuntimeError):
    """Raised when configuration is missing or malformed. Always fatal."""


@dataclass
class StrategyConfig:
    # Maximum sum of our two resting bids. A matched pair merges for $1.00,
    # so 1.00 - combined_budget is the locked gross margin per pair.
    combined_budget: float = 0.97
    # Re-quote a side when its resting price drifts this far from target.
    requote_drift: float = 0.02
    # Seconds between quote refresh cycles.
    requote_interval_s: float = 3.0
    # Stop posting this many seconds before window end; cancel + final merge.
    stop_posting_buffer_s: int = 75
    # Pause the heavy side when share imbalance exceeds this fraction...
    imbalance_pause_at: float = 0.15
    # ...and resume once it falls back under this fraction (hysteresis).
    imbalance_resume_at: float = 0.06
    # Also require this absolute share gap before pausing (ignore tiny books).
    imbalance_min_gap_shares: float = 10.0
    # Dollar target per resting rung, before min-size clamping.
    rung_dollar_target: float = 4.0
    # Max shares accumulated per side per window (runaway guard).
    max_shares_per_side: float = 500.0
    # Merge whenever at least this many matched pairs exist.
    min_pairs_to_merge: float = 5.0
    # Seconds between merge attempts per market (don't spam the relayer).
    merge_interval_s: float = 20.0
    # Assets to trade. Only 15-minute Up/Down series.
    assets: tuple[str, ...] = ("btc", "eth")

    def validate(self) -> None:
        if not (0.80 <= self.combined_budget <= 0.995):
            raise ConfigError(
                f"combined_budget={self.combined_budget} outside sane range "
                "[0.80, 0.995]. A budget >= 1.00 guarantees losses on every pair."
            )
        if self.min_pairs_to_merge < 1:
            raise ConfigError("min_pairs_to_merge must be >= 1")
        if self.stop_posting_buffer_s < 30:
            raise ConfigError(
                "stop_posting_buffer_s < 30 leaves no time for the final merge "
                "to confirm before the window resolves."
            )
        bad = [a for a in self.assets if a not in ("btc", "eth")]
        if bad:
            raise ConfigError(f"Unsupported assets {bad}; only btc/eth 15m series.")


@dataclass
class CapitalConfig:
    # Hard cap on cost basis + resting order value per market window.
    per_window_cap_usd: float = 50.0
    # Hard cap across all concurrently open windows.
    global_exposure_cap_usd: float = 150.0
    # Max concurrent market windows (BTC + ETH current window = 2).
    max_concurrent_windows: int = 2
    # Kill switch: stop the whole session if equity drops this fraction
    # below the session's starting equity.
    session_drawdown_kill: float = 0.10
    # Refuse to start live if pUSD balance is below this.
    min_starting_pusd: float = 20.0

    def validate(self) -> None:
        if self.per_window_cap_usd <= 0 or self.global_exposure_cap_usd <= 0:
            raise ConfigError("capital caps must be positive")
        if self.per_window_cap_usd > self.global_exposure_cap_usd:
            raise ConfigError("per_window_cap_usd cannot exceed global cap")
        if not (0.01 <= self.session_drawdown_kill <= 0.5):
            raise ConfigError("session_drawdown_kill must be in [0.01, 0.5]")


@dataclass
class BotConfig:
    private_key: str = ""
    # The Polymarket wallet that holds funds/positions (deposit wallet, Safe,
    # or proxy). If empty, the SDK binds the signer's deposit wallet after
    # setup_gasless_wallet().
    wallet: str = ""
    relayer_api_key: str = ""
    relayer_api_key_address: str = ""
    # CLOB signature type: 0=EOA, 1=PROXY, 2=SAFE, 3=POLY_1271 (deposit wallet).
    # New Polymarket accounts use deposit wallets which require type 3.
    # Using the wrong type causes the CLOB API to report $0 balance.
    clob_signature_type: int = 3
    dry_run: bool = True
    heartbeat: bool = True
    db_path: str = "data/gabagool_v2.sqlite"
    log_level: str = "INFO"
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    capital: CapitalConfig = field(default_factory=CapitalConfig)

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, yaml_path: str | Path | None = None) -> "BotConfig":
        data: dict = {}
        if yaml_path and Path(yaml_path).exists():
            with open(yaml_path) as fh:
                data = yaml.safe_load(fh) or {}

        clob = data.get("clob") or {}
        cfg = cls(
            private_key=os.getenv("POLY_PRIVATE_KEY", data.get("private_key", "")),
            wallet=os.getenv("POLY_WALLET", data.get("wallet", "")),
            relayer_api_key=os.getenv(
                "POLY_RELAYER_API_KEY", data.get("relayer_api_key", "")
            ),
            relayer_api_key_address=os.getenv(
                "POLY_RELAYER_API_KEY_ADDRESS",
                data.get("relayer_api_key_address", ""),
            ),
            clob_signature_type=int(
                os.getenv("POLY_CLOB_SIGNATURE_TYPE",
                          clob.get("signature_type", 3))
            ),
            dry_run=_env_bool("POLY_DRY_RUN", data.get("dry_run", True)),
            heartbeat=_env_bool("POLY_HEARTBEAT", data.get("heartbeat", True)),
            db_path=os.getenv("POLY_DB_PATH", data.get("db_path", "data/gabagool_v2.sqlite")),
            log_level=os.getenv("POLY_LOG_LEVEL", data.get("log_level", "INFO")),
            strategy=StrategyConfig(**(data.get("strategy") or {})),
            capital=CapitalConfig(**(data.get("capital") or {})),
        )
        cfg.validate()
        return cfg

    # -------------------------------------------------------------- validate
    def validate(self) -> None:
        self.strategy.validate()
        self.capital.validate()

        if self.dry_run:
            return  # dry-run needs no credentials

        if self.clob_signature_type not in (0, 1, 2, 3):
            raise ConfigError(
                f"clob_signature_type={self.clob_signature_type} must be 0, 1, 2, "
                "or 3 (3=POLY_1271 for deposit wallets)."
            )

        if not _PRIVKEY_RE.match(self.private_key or ""):
            raise ConfigError(
                "POLY_PRIVATE_KEY is missing or not a 64-hex-char key. "
                "Refusing to start live."
            )
        if self.wallet and not _ADDR_RE.match(self.wallet):
            raise ConfigError(
                f"POLY_WALLET={self.wallet!r} is not a 0x… address. "
                "(The old bot once shipped an entire .env line in this field — "
                "that is exactly the bug this check exists to catch.)"
            )
        if not self.relayer_api_key:
            raise ConfigError(
                "POLY_RELAYER_API_KEY missing. Merges/redeems are executed "
                "gaslessly through Polymarket's relayer and REQUIRE this key. "
                "Create one at polymarket.com -> Settings -> API Keys."
            )
        if not _ADDR_RE.match(self.relayer_api_key_address or ""):
            raise ConfigError(
                "POLY_RELAYER_API_KEY_ADDRESS must be the 0x… address shown "
                "next to your Relayer API key in Polymarket settings."
            )


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in ("1", "true", "yes", "on")
