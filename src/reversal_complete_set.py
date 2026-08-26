"""Pure logic for the BTC 5-minute reversal complete-set strategy.

Strategy:
1. Observe a clean BTC 5-minute Up/Down market.
2. Arm on the first outcome whose executable best ask is >= high_trigger.
3. Require that same outcome's best ask later falls <= reversal_trigger.
4. After reversal, scan both ask books.
5. Buy equal UP/DOWN shares only when the fee-adjusted executable pair basis
   is <= pair_cap.
6. Cap total acquisition spend per market.
7. Matched pairs are valued at exactly $1 each; no directional settlement
   assumption is needed.

This module contains no wallet, network, order, merge, or settlement code.
It is intentionally deterministic and Decimal-based so it can be unit tested
independently from the paper/live adapters.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from enum import Enum
from typing import Iterable, Sequence

D = Decimal
FEE_QUANTUM = D("0.00001")
SHARE_STEP = D("0.0001")
EPS = D("0.0000001")


def dec(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return D(str(value))


def floor_step(value: Decimal, step: Decimal = SHARE_STEP) -> Decimal:
    value = dec(value)
    step = dec(step)
    if step <= 0:
        raise ValueError("step must be > 0")
    if value <= 0:
        return D("0")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


@dataclass(frozen=True)
class FeeSchedule:
    """Taker fee curve used by the paper engine.

    Current Polymarket crypto documentation describes:
        fee_usdc = shares * fee_rate * p * (1-p)

    `exponent` is retained because CLOB market metadata exposes it. With the
    current crypto schedule exponent=1 reproduces the published fee table.
    """

    rate: Decimal = D("0.07")
    exponent: Decimal = D("1")
    taker_only: bool = True

    def fee(self, shares: Decimal, price: Decimal) -> Decimal:
        shares = dec(shares)
        price = dec(price)
        if shares <= 0 or self.rate <= 0:
            return D("0")
        if not (D("0") < price < D("1")):
            raise ValueError(f"price outside (0,1): {price}")
        curve = price * (D("1") - price)
        if self.exponent != D("1"):
            try:
                if self.exponent == self.exponent.to_integral_value():
                    curve = curve ** int(self.exponent)
                else:
                    curve = D(str(float(curve) ** float(self.exponent)))
            except Exception as exc:  # pragma: no cover
                raise ValueError(f"invalid fee exponent {self.exponent}") from exc
        raw = shares * self.rate * curve
        if raw < FEE_QUANTUM:
            return D("0")
        return raw.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class Level:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        if self.price <= 0 or self.price >= 1:
            raise ValueError("level price must be inside (0,1)")
        if self.size <= 0:
            raise ValueError("level size must be positive")


def normalize_asks(levels: Iterable[tuple[object, object] | Level]) -> tuple[Level, ...]:
    out: list[Level] = []
    for raw in levels:
        if isinstance(raw, Level):
            level = raw
        else:
            p, s = raw
            level = Level(dec(p), dec(s))
        out.append(level)
    out.sort(key=lambda x: x.price)
    return tuple(out)


@dataclass(frozen=True)
class BuyExecution:
    requested: Decimal
    filled: Decimal
    notional: Decimal
    fee: Decimal
    total_cost: Decimal
    raw_vwap: Decimal | None
    effective_vwap: Decimal | None
    worst_price: Decimal | None
    full: bool


def buy_execution(
    asks: Sequence[Level] | Iterable[tuple[object, object]],
    shares: Decimal,
    fee_schedule: FeeSchedule,
) -> BuyExecution:
    levels = tuple(asks) if isinstance(asks, tuple) and (not asks or isinstance(asks[0], Level)) else normalize_asks(asks)  # type: ignore[index]
    shares = dec(shares)
    if shares <= 0:
        return BuyExecution(shares, D("0"), D("0"), D("0"), D("0"), None, None, None, True)

    remaining = shares
    filled = D("0")
    notional = D("0")
    fee = D("0")
    worst: Decimal | None = None

    for level in levels:
        if remaining <= EPS:
            break
        take = min(level.size, remaining)
        if take <= 0:
            continue
        filled += take
        notional += take * level.price
        fee += fee_schedule.fee(take, level.price)
        remaining -= take
        worst = level.price

    full = filled + EPS >= shares
    raw_vwap = notional / filled if filled > 0 else None
    total_cost = notional + fee
    effective_vwap = total_cost / filled if filled > 0 else None
    return BuyExecution(shares, filled, notional, fee, total_cost, raw_vwap, effective_vwap, worst, full)


@dataclass(frozen=True)
class PairQuote:
    shares: Decimal
    up: BuyExecution
    down: BuyExecution
    total_cost: Decimal
    raw_pair_basis: Decimal
    fee_adjusted_pair_basis: Decimal
    merge_value: Decimal
    locked_profit: Decimal
    roi_on_spend: Decimal


def quote_pair(
    up_asks: Sequence[Level] | Iterable[tuple[object, object]],
    down_asks: Sequence[Level] | Iterable[tuple[object, object]],
    shares: Decimal,
    fee_schedule: FeeSchedule,
) -> PairQuote | None:
    shares = dec(shares)
    if shares <= 0:
        return None
    up = buy_execution(up_asks, shares, fee_schedule)
    down = buy_execution(down_asks, shares, fee_schedule)
    if not up.full or not down.full:
        return None
    assert up.raw_vwap is not None and down.raw_vwap is not None
    total_cost = up.total_cost + down.total_cost
    raw_basis = (up.notional + down.notional) / shares
    all_in = total_cost / shares
    merge_value = shares
    pnl = merge_value - total_cost
    roi = pnl / total_cost if total_cost > 0 else D("0")
    return PairQuote(shares, up, down, total_cost, raw_basis, all_in, merge_value, pnl, roi)


def _total_depth(levels: Sequence[Level]) -> Decimal:
    return sum((x.size for x in levels), D("0"))


def max_profitable_pair(
    up_asks: Iterable[tuple[object, object]] | Sequence[Level],
    down_asks: Iterable[tuple[object, object]] | Sequence[Level],
    *,
    budget: Decimal,
    pair_cap: Decimal,
    fee_schedule: FeeSchedule,
    min_shares: Decimal,
    share_step: Decimal = SHARE_STEP,
) -> PairQuote | None:
    """Return the largest equal-share executable pair under cap and budget."""
    ups = tuple(up_asks) if isinstance(up_asks, tuple) and (not up_asks or isinstance(up_asks[0], Level)) else normalize_asks(up_asks)  # type: ignore[index]
    dns = tuple(down_asks) if isinstance(down_asks, tuple) and (not down_asks or isinstance(down_asks[0], Level)) else normalize_asks(down_asks)  # type: ignore[index]
    budget = dec(budget)
    pair_cap = dec(pair_cap)
    min_shares = dec(min_shares)
    share_step = dec(share_step)

    if budget <= 0 or not ups or not dns:
        return None
    if pair_cap <= 0 or pair_cap >= 1:
        raise ValueError("pair_cap must be inside (0,1)")
    if min_shares <= 0:
        raise ValueError("min_shares must be positive")

    max_depth = min(_total_depth(ups), _total_depth(dns))
    if max_depth + EPS < min_shares:
        return None

    hi = max_depth
    lo = min_shares
    first = quote_pair(ups, dns, lo, fee_schedule)
    if first is None:
        return None
    if first.fee_adjusted_pair_basis > pair_cap + EPS or first.total_cost > budget + EPS:
        return None

    best = first
    for _ in range(80):
        if hi - lo <= share_step / D("2"):
            break
        mid = (lo + hi) / D("2")
        q = floor_step(mid, share_step)
        if q <= lo:
            q = lo + share_step
        if q > hi:
            q = hi
        quote = quote_pair(ups, dns, q, fee_schedule)
        ok = quote is not None and quote.fee_adjusted_pair_basis <= pair_cap + EPS and quote.total_cost <= budget + EPS
        if ok:
            best = quote
            lo = q
        else:
            hi = q - share_step
            if hi < lo:
                break

    candidates = {floor_step(best.shares, share_step)}
    for base in (lo, hi):
        q0 = floor_step(base, share_step)
        for delta in range(-2, 4):
            q = q0 + share_step * delta
            if q >= min_shares and q <= max_depth:
                candidates.add(q)

    for q in sorted(candidates):
        quote = quote_pair(ups, dns, q, fee_schedule)
        if quote is not None and quote.fee_adjusted_pair_basis <= pair_cap + EPS and quote.total_cost <= budget + EPS and quote.shares >= best.shares:
            best = quote

    return best if best.shares + EPS >= min_shares else None


class StrategyState(str, Enum):
    WAIT_HIGH = "WAIT_HIGH"
    WAIT_REVERSAL = "WAIT_REVERSAL"
    PAIR_HUNT = "PAIR_HUNT"
    TRADED = "TRADED"
    EXPIRED = "EXPIRED"


@dataclass
class ReversalGate:
    high_trigger: Decimal = D("0.65")
    reversal_trigger: Decimal = D("0.40")
    state: StrategyState = StrategyState.WAIT_HIGH
    high_side: str | None = None
    high_seen_at: float | None = None
    high_seen_price: Decimal | None = None
    reversal_seen_at: float | None = None
    reversal_seen_price: Decimal | None = None

    def observe(self, *, now: float, up_best_ask: Decimal | None, down_best_ask: Decimal | None) -> list[str]:
        events: list[str] = []
        if self.state in (StrategyState.TRADED, StrategyState.EXPIRED):
            return events
        asks = {"UP": up_best_ask, "DOWN": down_best_ask}

        if self.state == StrategyState.WAIT_HIGH:
            reached = [(side, price) for side, price in asks.items() if price is not None and price >= self.high_trigger]
            if reached:
                reached.sort(key=lambda x: (x[1], x[0]), reverse=True)
                side, price = reached[0]
                self.high_side = side
                self.high_seen_at = now
                self.high_seen_price = price
                self.state = StrategyState.WAIT_REVERSAL
                events.append("HIGH_ARMED")

        if self.state == StrategyState.WAIT_REVERSAL and self.high_side:
            price = asks[self.high_side]
            if price is not None and price <= self.reversal_trigger:
                self.reversal_seen_at = now
                self.reversal_seen_price = price
                self.state = StrategyState.PAIR_HUNT
                events.append("REVERSAL_CONFIRMED")
        return events

    def mark_traded(self) -> None:
        self.state = StrategyState.TRADED

    def expire(self) -> None:
        if self.state != StrategyState.TRADED:
            self.state = StrategyState.EXPIRED
