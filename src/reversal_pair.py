"""Pure strategy logic for the BTC 5-minute reversal complete-set paper trader.

The module has no network or wallet dependencies. It turns real order-book snapshots
into a deterministic state transition and, when the full setup is present, a simulated
matched UP+DOWN purchase whose fee-adjusted cost is capped below $1.00.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math


class Phase(str, Enum):
    SEEK_LEADER = "SEEK_LEADER"
    SEEK_COLLAPSE = "SEEK_COLLAPSE"
    ARMED = "ARMED"
    FILLED = "FILLED"


@dataclass(frozen=True)
class TopOfBook:
    best_bid: float
    best_ask: float
    ask_size: float
    min_order_size: float = 0.0

    @property
    def midpoint(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    def valid(self) -> bool:
        return (
            math.isfinite(self.best_bid)
            and math.isfinite(self.best_ask)
            and math.isfinite(self.ask_size)
            and 0.0 < self.best_bid < self.best_ask < 1.0
            and self.ask_size > 0.0
        )


@dataclass(frozen=True)
class ReversalEvent:
    kind: str
    side: str
    ts: float
    midpoint: float


@dataclass
class ReversalState:
    leader_threshold: float = 0.65
    collapse_threshold: float = 0.40
    phase: Phase = Phase.SEEK_LEADER
    leader_side: str | None = None
    leader_ts: float | None = None
    leader_mid: float | None = None
    leader_peak_mid: float | None = None
    collapse_ts: float | None = None
    collapse_mid: float | None = None
    collapse_low_mid: float | None = None

    def observe(self, *, up_mid: float, down_mid: float, ts: float) -> list[ReversalEvent]:
        """Advance the 65c -> 40c reversal gate using midpoint prices.

        The first side to reach the leader threshold becomes the tracked side. The
        setup is armed only when that same side later reaches the collapse threshold
        and is also the cheaper of the two outcomes at that observation.
        """
        events: list[ReversalEvent] = []
        if self.phase is Phase.FILLED:
            return events
        if not _valid_probability(up_mid) or not _valid_probability(down_mid):
            return events

        if self.phase is Phase.SEEK_LEADER:
            side, mid = ("UP", up_mid) if up_mid >= down_mid else ("DOWN", down_mid)
            if mid >= self.leader_threshold:
                self.leader_side = side
                self.leader_ts = float(ts)
                self.leader_mid = float(mid)
                self.leader_peak_mid = float(mid)
                self.phase = Phase.SEEK_COLLAPSE
                events.append(ReversalEvent("LEADER", side, float(ts), float(mid)))
            return events

        if self.phase is Phase.SEEK_COLLAPSE and self.leader_side is not None:
            leader_mid = up_mid if self.leader_side == "UP" else down_mid
            other_mid = down_mid if self.leader_side == "UP" else up_mid
            if self.leader_peak_mid is None or leader_mid > self.leader_peak_mid:
                self.leader_peak_mid = float(leader_mid)
            if leader_mid <= self.collapse_threshold and leader_mid < other_mid:
                self.collapse_ts = float(ts)
                self.collapse_mid = float(leader_mid)
                self.collapse_low_mid = float(leader_mid)
                self.phase = Phase.ARMED
                events.append(
                    ReversalEvent("COLLAPSE", self.leader_side, float(ts), float(leader_mid))
                )
            return events

        if self.phase is Phase.ARMED and self.leader_side is not None:
            leader_mid = up_mid if self.leader_side == "UP" else down_mid
            if self.collapse_low_mid is None or leader_mid < self.collapse_low_mid:
                self.collapse_low_mid = float(leader_mid)
        return events

    def mark_filled(self) -> None:
        if self.phase is not Phase.ARMED:
            raise RuntimeError("cannot mark filled before the reversal gate is armed")
        self.phase = Phase.FILLED


@dataclass(frozen=True)
class PairFill:
    shares: float
    up_price: float
    down_price: float
    up_fee: float
    down_fee: float
    gross_cost: float
    fee_cost: float
    total_cost: float
    payout_value: float
    locked_profit: float
    effective_pair_cost: float
    roi_on_cost: float


def taker_fee_usdc(shares: float, price: float, fee_rate: float) -> float:
    """Polymarket taker fee in USDC, rounded to the documented 5 decimals."""
    if shares <= 0 or not (0 < price < 1) or fee_rate < 0:
        return 0.0
    return round(float(shares) * float(fee_rate) * float(price) * (1.0 - float(price)), 5)


def estimated_effective_pair_cost(up_ask: float, down_ask: float, fee_rate: float) -> float:
    """Linear per-pair cost used before exact fee rounding is known."""
    return (
        float(up_ask)
        + float(down_ask)
        + float(fee_rate) * float(up_ask) * (1.0 - float(up_ask))
        + float(fee_rate) * float(down_ask) * (1.0 - float(down_ask))
    )


def build_pair_fill(
    *,
    up: TopOfBook,
    down: TopOfBook,
    fee_rate: float,
    max_effective_pair_cost: float,
    capital_limit_usd: float,
    min_pair_shares: float,
    share_precision: int = 3,
) -> PairFill | None:
    """Return a conservative top-of-book paper fill or ``None``.

    Both legs must be executable from the same snapshot. Size is capped by displayed
    best-ask liquidity on both sides and by the configured dollar limit. The final
    threshold test uses the exact rounded fee at the chosen share count.
    """
    if not up.valid() or not down.valid():
        return None
    if fee_rate < 0 or capital_limit_usd <= 0 or min_pair_shares <= 0:
        return None
    if not (0.0 < max_effective_pair_cost < 1.0):
        return None

    estimated = estimated_effective_pair_cost(up.best_ask, down.best_ask, fee_rate)
    if estimated > max_effective_pair_cost + 1e-12:
        return None

    exchange_min = max(float(up.min_order_size or 0.0), float(down.min_order_size or 0.0))
    required_min = max(float(min_pair_shares), exchange_min)
    max_shares = min(
        float(up.ask_size),
        float(down.ask_size),
        float(capital_limit_usd) / estimated,
    )
    shares = _floor_decimals(max_shares, share_precision)
    if shares + 1e-12 < required_min:
        return None

    fill = _exact_fill(shares, up.best_ask, down.best_ask, fee_rate)
    if fill.total_cost > capital_limit_usd + 1e-9:
        adjusted = _floor_decimals(shares * capital_limit_usd / fill.total_cost, share_precision)
        if adjusted + 1e-12 < required_min:
            return None
        fill = _exact_fill(adjusted, up.best_ask, down.best_ask, fee_rate)

    if fill.effective_pair_cost > max_effective_pair_cost + 1e-12:
        return None
    if fill.locked_profit <= 0:
        return None
    return fill


def _exact_fill(shares: float, up_price: float, down_price: float, fee_rate: float) -> PairFill:
    up_fee = taker_fee_usdc(shares, up_price, fee_rate)
    down_fee = taker_fee_usdc(shares, down_price, fee_rate)
    gross = shares * (up_price + down_price)
    fee_cost = up_fee + down_fee
    total = gross + fee_cost
    payout = shares
    locked_profit = payout - total
    effective = total / shares if shares > 0 else float("inf")
    roi = locked_profit / total if total > 0 else 0.0
    return PairFill(
        shares=shares,
        up_price=up_price,
        down_price=down_price,
        up_fee=up_fee,
        down_fee=down_fee,
        gross_cost=gross,
        fee_cost=fee_cost,
        total_cost=total,
        payout_value=payout,
        locked_profit=locked_profit,
        effective_pair_cost=effective,
        roi_on_cost=roi,
    )


def _valid_probability(value: float) -> bool:
    return math.isfinite(value) and 0.0 < float(value) < 1.0


def _floor_decimals(value: float, places: int) -> float:
    factor = 10 ** max(0, int(places))
    return math.floor(max(0.0, float(value)) * factor + 1e-12) / factor
