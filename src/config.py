"""Typed configuration for the forensic-policy Gabagool replica."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_PRIVKEY_RE = re.compile(r"^(0x)?[a-fA-F0-9]{64}$")


class ConfigError(RuntimeError):
    pass


@dataclass
class StrategyConfig:
    # Forensic-calibrated aggregate economics. Median historical terminal
    # combined side VWAP was ~0.985; p95 was ~1.011.
    target_combined_vwap: float = 0.985
    max_combined_vwap: float = 1.01
    initial_pair_ceiling: float = 1.00

    # Quote cadence / queue behavior. Exact cancelled-quote policy is not observable.
    requote_drift: float = 0.01
    requote_interval_s: float = 1.0
    stop_posting_buffer_s: int = 2
    entry_delay_by_duration_s: dict[int, float] = field(
        default_factory=lambda: {300: 10.0, 900: 12.0}
    )

    # Observed clips are overwhelmingly 5-50 shares; 10-20 dominates.
    base_clip_shares: float = 10.0
    max_clip_shares: float = 40.0
    max_shares_per_side: float = 2500.0
    hard_pause_ratio: float = 2.0

    # Mixed execution: historical aggregate was ~85.45% maker / 14.55% taker.
    taker_enabled: bool = True
    taker_stop_buffer_s: float = 15.0

    # Settlement is post-close, not every few seconds while trading.
    merge_after_close_s: float = 5.0
    settlement_sweep_interval_s: float = 10.0
    min_pairs_to_merge: float = 1.0

    # Current candidate universe. Discovery safely ignores series that do not exist.
    assets: tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    durations: tuple[int, ...] = (300, 900)

    # Backward-compatible input only. It is deliberately ignored by the new policy.
    combined_budget: float | None = None

    def validate(self) -> None:
        if not (0.90 <= self.target_combined_vwap <= 1.02):
            raise ConfigError("target_combined_vwap outside [0.90, 1.02]")
        if not (self.target_combined_vwap <= self.max_combined_vwap <= 1.05):
            raise ConfigError("max_combined_vwap must be >= target and <= 1.05")
        if not (0.95 <= self.initial_pair_ceiling <= 1.02):
            raise ConfigError("initial_pair_ceiling outside [0.95, 1.02]")
        if self.requote_interval_s <= 0 or self.requote_drift <= 0:
            raise ConfigError("requote settings must be positive")
        if self.stop_posting_buffer_s < 1 or self.stop_posting_buffer_s > 15:
            raise ConfigError("stop_posting_buffer_s must be in [1, 15]")
        if self.base_clip_shares < 5 or self.max_clip_shares < self.base_clip_shares:
            raise ConfigError("invalid clip size bounds")
        if self.max_shares_per_side < self.max_clip_shares:
            raise ConfigError("max_shares_per_side below max_clip_shares")
        if self.hard_pause_ratio < 1.25:
            raise ConfigError("hard_pause_ratio must be >= 1.25")
        if self.taker_stop_buffer_s < self.stop_posting_buffer_s:
            raise ConfigError("taker_stop_buffer_s must be >= stop_posting_buffer_s")
        if self.min_pairs_to_merge < 0:
            raise ConfigError("min_pairs_to_merge must be >= 0")
        bad_assets = [a for a in self.assets if a not in ("btc", "eth", "sol", "xrp")]
        if bad_assets:
            raise ConfigError(f"unsupported assets: {bad_assets}")
        bad_durations = [d for d in self.durations if int(d) not in (300, 900)]
        if bad_durations:
            raise ConfigError(f"unsupported durations: {bad_durations}; use 300 and/or 900")


@dataclass
class CapitalConfig:
    # Safety limits, not forensic strategy parameters.
    per_window_cap_usd: float = 250.0
    global_exposure_cap_usd: float = 1000.0
    max_concurrent_windows: int = 4
    session_drawdown_kill: float = 0.10
    min_starting_pusd: float = 20.0
    harvest_floor_usd: float = 0.0

    def validate(self) -> None:
        if self.per_window_cap_usd <= 0 or self.global_exposure_cap_usd <= 0:
            raise ConfigError("capital caps must be positive")
        if self.per_window_cap_usd > self.global_exposure_cap_usd:
            raise ConfigError("per_window_cap_usd cannot exceed global cap")
        if self.max_concurrent_windows < 1:
            raise ConfigError("max_concurrent_windows must be >= 1")
        if not (0.01 <= self.session_drawdown_kill <= 0.5):
            raise ConfigError("session_drawdown_kill must be in [0.01, 0.5]")


@dataclass
class BotConfig:
    private_key: str = ""
    wallet: str = ""
    relayer_api_key: str = ""
    relayer_api_key_address: str = ""
    clob_signature_type: int = 3
    dry_run: bool = True
    heartbeat: bool = True
    db_path: str = "data/gabagool_v3.sqlite"
    log_level: str = "INFO"
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    capital: CapitalConfig = field(default_factory=CapitalConfig)

    @classmethod
    def load(cls, yaml_path: str | Path | None = None) -> "BotConfig":
        data: dict = {}
        if yaml_path and Path(yaml_path).exists():
            with open(yaml_path) as fh:
                data = yaml.safe_load(fh) or {}
        clob = data.get("clob") or {}
        sdata = dict(data.get("strategy") or {})
        if "assets" in sdata:
            sdata["assets"] = tuple(sdata["assets"])
        if "durations" in sdata:
            sdata["durations"] = tuple(int(x) for x in sdata["durations"])
        if "entry_delay_by_duration_s" in sdata:
            sdata["entry_delay_by_duration_s"] = {
                int(k): float(v) for k, v in sdata["entry_delay_by_duration_s"].items()
            }
        cfg = cls(
            private_key=os.getenv("POLY_PRIVATE_KEY", data.get("private_key", "")),
            wallet=os.getenv("POLY_WALLET", data.get("wallet", "")),
            relayer_api_key=os.getenv("POLY_RELAYER_API_KEY", data.get("relayer_api_key", "")),
            relayer_api_key_address=os.getenv(
                "POLY_RELAYER_API_KEY_ADDRESS", data.get("relayer_api_key_address", "")
            ),
            clob_signature_type=int(
                os.getenv("POLY_CLOB_SIGNATURE_TYPE", clob.get("signature_type", 3))
            ),
            dry_run=_env_bool("POLY_DRY_RUN", data.get("dry_run", True)),
            heartbeat=_env_bool("POLY_HEARTBEAT", data.get("heartbeat", True)),
            db_path=os.getenv("POLY_DB_PATH", data.get("db_path", "data/gabagool_v3.sqlite")),
            log_level=os.getenv("POLY_LOG_LEVEL", data.get("log_level", "INFO")),
            strategy=StrategyConfig(**sdata),
            capital=CapitalConfig(**(data.get("capital") or {})),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        self.strategy.validate()
        self.capital.validate()
        if self.dry_run:
            return
        if self.clob_signature_type not in (0, 1, 2, 3):
            raise ConfigError("clob_signature_type must be 0, 1, 2, or 3")
        if not _PRIVKEY_RE.match(self.private_key or ""):
            raise ConfigError("POLY_PRIVATE_KEY missing or malformed")
        if self.wallet and not _ADDR_RE.match(self.wallet):
            raise ConfigError("POLY_WALLET is not a valid 0x address")
        if not self.relayer_api_key:
            raise ConfigError("POLY_RELAYER_API_KEY missing")
        if not _ADDR_RE.match(self.relayer_api_key_address or ""):
            raise ConfigError("POLY_RELAYER_API_KEY_ADDRESS is not a valid 0x address")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in ("1", "true", "yes", "on")
