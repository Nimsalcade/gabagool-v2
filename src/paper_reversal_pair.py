"""Real-book paper runner for the BTC 5-minute reversal complete-set strategy.

Usage:
    python -m src.paper_reversal_pair --config config/reversal_pair_paper.yaml

This process never places, cancels, merges, or redeems an on-chain/CLOB order. It uses
public market data and records simulated fills only when both displayed best asks have
sufficient size in the same order-book snapshot.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
from dataclasses import dataclass, fields
from datetime import datetime, timezone
import logging
from pathlib import Path
import signal
import time
from typing import Any

import yaml

from .discovery import resolve_market, window_start_epoch
from .reversal_pair import Phase, ReversalState, TopOfBook, build_pair_fill, estimated_effective_pair_cost

log = logging.getLogger("paper_reversal_pair")


@dataclass
class PaperConfig:
    asset: str = "btc"
    duration_s: int = 300
    poll_interval_s: float = 1.0
    status_interval_s: float = 15.0
    leader_threshold: float = 0.65
    collapse_threshold: float = 0.40
    max_effective_pair_cost: float = 0.97
    taker_fee_rate: float = 0.07
    starting_balance_usd: float = 1000.0
    max_market_capital_usd: float = 100.0
    min_pair_shares: float = 5.0
    share_precision: int = 3
    require_non_neg_risk: bool = True
    results_csv: str = "data/reversal_pair_paper.csv"

    @classmethod
    def load(cls, path: str | Path) -> "PaperConfig":
        raw = yaml.safe_load(Path(path).read_text()) or {}
        raw = raw.get("paper_reversal_pair", raw)
        allowed = {f.name for f in fields(cls)}
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(f"unknown paper_reversal_pair settings: {unknown}")
        cfg = cls(**raw)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.asset.lower() != "btc":
            raise ValueError("paper strategy is locked to BTC")
        if int(self.duration_s) != 300:
            raise ValueError("paper strategy is locked to 300-second markets")
        if not (0.50 < self.leader_threshold < 1.0):
            raise ValueError("leader_threshold must be > 0.50 and < 1")
        if not (0.0 < self.collapse_threshold < 0.50):
            raise ValueError("collapse_threshold must be > 0 and < 0.50")
        if self.collapse_threshold >= self.leader_threshold:
            raise ValueError("collapse_threshold must be below leader_threshold")
        if not (0.0 < self.max_effective_pair_cost < 1.0):
            raise ValueError("max_effective_pair_cost must be below $1")
        if self.taker_fee_rate < 0:
            raise ValueError("taker_fee_rate must be non-negative")
        if self.starting_balance_usd <= 0 or self.max_market_capital_usd <= 0:
            raise ValueError("paper capital values must be positive")
        if self.min_pair_shares <= 0:
            raise ValueError("min_pair_shares must be positive")
        if self.poll_interval_s <= 0 or self.status_interval_s <= 0:
            raise ValueError("poll/status intervals must be positive")


@dataclass(frozen=True)
class Snapshot:
    up: TopOfBook
    down: TopOfBook


class PaperLedger:
    HEADERS = [
        "ts_utc",
        "condition_id",
        "slug",
        "leader_side",
        "leader_mid",
        "leader_peak_mid",
        "collapse_mid",
        "collapse_low_mid",
        "up_ask",
        "down_ask",
        "up_ask_size",
        "down_ask_size",
        "fee_rate",
        "shares",
        "gross_cost",
        "fee_cost",
        "total_cost",
        "effective_pair_cost",
        "paper_merge_value",
        "locked_profit",
        "roi_on_cost",
        "balance_after",
    ]

    def __init__(self, path: str | Path, starting_balance: float):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.traded_conditions: set[str] = set()
        self.balance = float(starting_balance)
        self.total_profit = 0.0
        self.trade_count = 0
        if self.path.exists():
            self._resume()
        else:
            with self.path.open("w", newline="") as fh:
                csv.DictWriter(fh, fieldnames=self.HEADERS).writeheader()

    def _resume(self) -> None:
        with self.path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                cid = str(row.get("condition_id", ""))
                if cid:
                    self.traded_conditions.add(cid)
                try:
                    self.balance = float(row["balance_after"])
                    self.total_profit += float(row["locked_profit"])
                    self.trade_count += 1
                except (KeyError, TypeError, ValueError):
                    continue

    def record(self, *, market, state: ReversalState, snapshot: Snapshot, fee_rate: float, fill) -> None:
        # A complete binary pair is immediately worth $1 per matched share. Paper
        # mode credits the merge value immediately so the bankroll mirrors a merge.
        self.balance = self.balance - fill.total_cost + fill.payout_value
        self.total_profit += fill.locked_profit
        self.trade_count += 1
        self.traded_conditions.add(market.condition_id)
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "condition_id": market.condition_id,
            "slug": market.slug,
            "leader_side": state.leader_side or "",
            "leader_mid": _fmt(state.leader_mid),
            "leader_peak_mid": _fmt(state.leader_peak_mid),
            "collapse_mid": _fmt(state.collapse_mid),
            "collapse_low_mid": _fmt(state.collapse_low_mid),
            "up_ask": f"{snapshot.up.best_ask:.6f}",
            "down_ask": f"{snapshot.down.best_ask:.6f}",
            "up_ask_size": f"{snapshot.up.ask_size:.6f}",
            "down_ask_size": f"{snapshot.down.ask_size:.6f}",
            "fee_rate": f"{fee_rate:.6f}",
            "shares": f"{fill.shares:.6f}",
            "gross_cost": f"{fill.gross_cost:.6f}",
            "fee_cost": f"{fill.fee_cost:.6f}",
            "total_cost": f"{fill.total_cost:.6f}",
            "effective_pair_cost": f"{fill.effective_pair_cost:.6f}",
            "paper_merge_value": f"{fill.payout_value:.6f}",
            "locked_profit": f"{fill.locked_profit:.6f}",
            "roi_on_cost": f"{fill.roi_on_cost:.8f}",
            "balance_after": f"{self.balance:.6f}",
        }
        with self.path.open("a", newline="") as fh:
            csv.DictWriter(fh, fieldnames=self.HEADERS).writerow(row)


async def fetch_snapshot(client, market) -> Snapshot | None:
    try:
        up_book, down_book = await asyncio.gather(
            client.get_order_book(token_id=market.up_token_id),
            client.get_order_book(token_id=market.down_token_id),
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("book fetch failed: %s", exc)
        return None
    up = _book_top(up_book)
    down = _book_top(down_book)
    if up is None or down is None:
        return None
    return Snapshot(up=up, down=down)


def _book_top(book: Any) -> TopOfBook | None:
    bids = list(getattr(book, "bids", None) or [])
    asks = list(getattr(book, "asks", None) or [])
    if not bids or not asks:
        return None
    try:
        bid = max(float(level.price) for level in bids)
        ask = min(float(level.price) for level in asks)
        ask_size = sum(
            float(getattr(level, "size", 0.0) or 0.0)
            for level in asks
            if abs(float(level.price) - ask) <= 1e-12
        )
        min_order = float(getattr(book, "min_order_size", 0.0) or 0.0)
    except (TypeError, ValueError, AttributeError):
        return None
    top = TopOfBook(bid, ask, ask_size, min_order)
    return top if top.valid() else None


async def _market_fee_rate(client, market, fallback: float) -> tuple[float, str]:
    """Read a fee rate when the SDK exposes one, otherwise use the configured crypto rate."""
    try:
        obj = await client.get_market(slug=market.slug)
    except Exception:  # noqa: BLE001
        return float(fallback), "config-fallback"
    paths = (
        "taker_fee_rate",
        "fee_rate",
        "takerFeeRate",
        "feeRate",
    )
    for name in paths:
        raw = getattr(obj, name, None)
        rate = _coerce_fee_rate(raw)
        if rate is not None:
            return rate, f"market.{name}"
    for container_name in ("fees", "fee_config", "feeConfig"):
        container = getattr(obj, container_name, None)
        if container is None:
            continue
        for name in paths:
            raw = getattr(container, name, None)
            rate = _coerce_fee_rate(raw)
            if rate is not None:
                return rate, f"market.{container_name}.{name}"
    return float(fallback), "config-fallback"


def _coerce_fee_rate(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    # Some APIs expose basis points. A raw 700 means 7%; 0.07 means 7%.
    if value > 1.0:
        value /= 10000.0
    return value if 0.0 <= value <= 1.0 else None


async def run_paper_live(cfg: PaperConfig) -> None:
    from polymarket import AsyncPublicClient

    client = AsyncPublicClient()
    ledger = PaperLedger(cfg.results_csv, cfg.starting_balance_usd)
    current_start: int | None = None
    market = None
    state: ReversalState | None = None
    fee_rate = cfg.taker_fee_rate
    fee_source = "config"
    last_status = 0.0

    log.info(
        "PAPER-LIVE start | BTC 5m | balance=$%.2f | leader>=%.2f collapse<=%.2f pair<=%.2f",
        ledger.balance,
        cfg.leader_threshold,
        cfg.collapse_threshold,
        cfg.max_effective_pair_cost,
    )
    log.info("results -> %s", cfg.results_csv)

    try:
        while True:
            now = time.time()
            start = window_start_epoch(cfg.duration_s, now)
            if start != current_start:
                current_start = start
                market = await resolve_market(client, cfg.asset, cfg.duration_s, start)
                state = ReversalState(cfg.leader_threshold, cfg.collapse_threshold)
                if market is None:
                    log.warning("current BTC 5m market not available yet")
                elif cfg.require_non_neg_risk and market.neg_risk:
                    log.warning("skip %s: neg-risk market", market.slug)
                    market = None
                else:
                    fee_rate, fee_source = await _market_fee_rate(client, market, cfg.taker_fee_rate)
                    log.info(
                        "MARKET %s | fee_rate=%.4f (%s)%s",
                        market.slug,
                        fee_rate,
                        fee_source,
                        " | already paper-traded" if market.condition_id in ledger.traded_conditions else "",
                    )

            if market is None or state is None:
                await asyncio.sleep(cfg.poll_interval_s)
                continue
            if market.condition_id in ledger.traded_conditions:
                await asyncio.sleep(cfg.poll_interval_s)
                continue

            snap = await fetch_snapshot(client, market)
            if snap is None:
                await asyncio.sleep(cfg.poll_interval_s)
                continue

            events = state.observe(up_mid=snap.up.midpoint, down_mid=snap.down.midpoint, ts=now)
            for event in events:
                if event.kind == "LEADER":
                    log.info("LEADER %s %.1fc | age=%.1fs", event.side, event.midpoint * 100, now - start)
                elif event.kind == "COLLAPSE":
                    log.info(
                        "COLLAPSE %s %.1fc | ARMED | age=%.1fs",
                        event.side,
                        event.midpoint * 100,
                        now - start,
                    )

            pair_est = estimated_effective_pair_cost(
                snap.up.best_ask, snap.down.best_ask, fee_rate
            )
            if state.phase is Phase.ARMED:
                capital_limit = min(cfg.max_market_capital_usd, ledger.balance)
                fill = build_pair_fill(
                    up=snap.up,
                    down=snap.down,
                    fee_rate=fee_rate,
                    max_effective_pair_cost=cfg.max_effective_pair_cost,
                    capital_limit_usd=capital_limit,
                    min_pair_shares=cfg.min_pair_shares,
                    share_precision=cfg.share_precision,
                )
                if fill is not None:
                    ledger.record(
                        market=market,
                        state=state,
                        snapshot=snap,
                        fee_rate=fee_rate,
                        fill=fill,
                    )
                    state.mark_filled()
                    log.info(
                        "PAPER FILL | UP %.1fc + DOWN %.1fc | %.3f sh | cost=$%.2f | pair=%.4f",
                        fill.up_price * 100,
                        fill.down_price * 100,
                        fill.shares,
                        fill.total_cost,
                        fill.effective_pair_cost,
                    )
                    log.info(
                        "PAPER MERGE | value=$%.2f profit=+$%.2f ROI=%.2f%% | balance=$%.2f",
                        fill.payout_value,
                        fill.locked_profit,
                        fill.roi_on_cost * 100,
                        ledger.balance,
                    )

            if now - last_status >= cfg.status_interval_s:
                last_status = now
                log.info(
                    "STATUS | phase=%s UP %.1f/%.1f DOWN %.1f/%.1f pair_est=%.4f T-%ds | trades=%d pnl=%+.2f bal=%.2f",
                    state.phase.value,
                    snap.up.best_bid * 100,
                    snap.up.best_ask * 100,
                    snap.down.best_bid * 100,
                    snap.down.best_ask * 100,
                    pair_est,
                    max(0, int(market.seconds_to_end)),
                    ledger.trade_count,
                    ledger.total_profit,
                    ledger.balance,
                )

            await asyncio.sleep(cfg.poll_interval_s)
    finally:
        try:
            await client.close()
        except Exception:  # noqa: BLE001
            pass


def _fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}"


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> None:
    ap = argparse.ArgumentParser(description="BTC 5m reversal complete-set paper-live runner")
    ap.add_argument("--config", default="config/reversal_pair_paper.yaml")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    _setup_logging(args.log_level)
    cfg = PaperConfig.load(args.config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run_paper_live(cfg))

    def stop() -> None:
        log.info("shutdown requested")
        task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
