"""Economic repair guard for the V7.5 January paper controller.

V7.4 demonstrated that marginal repair at any price below $1 is too permissive
for a continuously rebalancing controller: although one deficient-side fill can
improve recovery of already-sunk inventory, repeated imbalance sign flips can
lock complete sets at gross cost far above $1.00.

V7.5 therefore gives repair its own *portfolio pair cap*.  Repair orders remain
maker-only and may be more permissive than ordinary accumulation, but they may
only be posted at prices that keep projected UP_VWAP + DOWN_VWAP within a small,
explicit loss budget.  The six supplied reference sessions finished with side-
VWAP sums ranging from about 0.9694 to 1.0059, so the default paper surface uses
1.005 early and ramps to 1.010 late rather than V7.3/V7.4's unconditional 0.92
single-side ceiling.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.v7_january_portfolio import clamp, max_buy_price_for_portfolio_cap


@dataclass(frozen=True)
class RepairEconomics:
    age_s: float
    pair_cap: float
    max_buy_price: float


def effective_repair_pair_cap(
    *,
    age_s: float,
    duration_s: float = 900.0,
    base_cap: float = 1.005,
    late_cap: float = 1.010,
    late_start_s: float = 600.0,
) -> float:
    """Return the gross combined-VWAP ceiling allowed for repair.

    The cap stays tight through the first two thirds of the market, then ramps
    quadratically toward ``late_cap`` so late balancing gets a small additional
    loss budget without ever approaching V7.4's observed 1.10-1.30 pair bases.
    """
    if duration_s <= 0:
        raise ValueError("duration_s must be positive")
    if late_start_s >= duration_s or age_s <= late_start_s:
        return float(base_cap)
    x = clamp((float(age_s) - float(late_start_s)) / (float(duration_s) - float(late_start_s)), 0.0, 1.0)
    return float(base_cap) + (float(late_cap) - float(base_cap)) * x * x


def economic_repair_price(
    *,
    side: str,
    add_qty: float,
    up_shares: float,
    up_cost: float,
    down_shares: float,
    down_cost: float,
    age_s: float,
    absolute_max: float = 0.92,
    duration_s: float = 900.0,
    base_cap: float = 1.005,
    late_cap: float = 1.010,
    late_start_s: float = 600.0,
) -> RepairEconomics:
    """Maximum deficient-side price consistent with the V7.5 pair-loss budget."""
    cap = effective_repair_pair_cap(
        age_s=age_s,
        duration_s=duration_s,
        base_cap=base_cap,
        late_cap=late_cap,
        late_start_s=late_start_s,
    )
    max_px = max_buy_price_for_portfolio_cap(
        side=side,
        add_qty=add_qty,
        up_shares=up_shares,
        up_cost=up_cost,
        down_shares=down_shares,
        down_cost=down_cost,
        combined_cap=cap,
        absolute_max=absolute_max,
    )
    return RepairEconomics(float(age_s), float(cap), float(max_px))
