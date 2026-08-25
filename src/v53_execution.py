"""V5.3 paper execution reconstruction from on-chain order forensics.

This module contains execution-adapter candidates only.  It does not claim to
recover Gabagool's private source code.  The observable facts motivating V5.3
are:

* multiple distinct passive orders can coexist at the same exact cent;
* parent size is age-, asset-, and regime-dependent;
* same-cent capacity is larger on the underweight side in the sampled markets;
* aggressive BUYs use the same parent-size family as passive orders;
* aggressive limits usually equal the execution price or sit within 1-2 cents;
* parent orders commonly spill through neutral instead of clipping to deficit.

V5.3 therefore separates two dimensions that V5.2a conflated:

1. logical price layers (the reconstructed 4 -> 0 inventory controller), and
2. independent parent slots at one exact price (same-cent stacking).

The exact private stack target and aggressive trigger remain unidentifiable, so
both are explicit evidence-calibrated paper-model surfaces.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from src.forensic_15m import Inventory, layer_prices
from src.shadow import ShadowOrder


# ---------------------------------------------------------------------------
# Parent-size profiles
# ---------------------------------------------------------------------------

# Intervals are [start_age, end_age).  October reproduces the independently
# recovered Oct-29 schedule.  November/December boundaries are consensus
# approximations from fresh per-market signed-order calldata samples and are
# intentionally labelled profiles rather than universal historical rules.
_PARENT_SCHEDULES: dict[str, dict[str, tuple[tuple[float, float, float], ...]]] = {
    "oct": {
        "BTC": (
            (18.0, 212.0, 10.0),
            (212.0, 382.0, 9.0),
            (382.0, 522.0, 8.0),
            (522.0, 664.0, 7.0),
            (664.0, 818.0, 6.0),
            (818.0, 899.0, 5.0),
        ),
        "ETH": (
            (18.0, 212.0, 10.0),
            (212.0, 382.0, 9.0),
            (382.0, 522.0, 8.0),
            (522.0, 664.0, 7.0),
            (664.0, 818.0, 6.0),
            (818.0, 899.0, 5.0),
        ),
    },
    "nov": {
        "BTC": (
            (18.0, 440.0, 10.0),
            (440.0, 590.0, 9.0),
            (590.0, 690.0, 8.0),
            (690.0, 780.0, 7.0),
            (780.0, 870.0, 6.0),
            (870.0, 899.0, 5.0),
        ),
        "ETH": (
            (18.0, 440.0, 10.0),
            (440.0, 590.0, 9.0),
            (590.0, 700.0, 8.0),
            (700.0, 780.0, 7.0),
            (780.0, 860.0, 6.0),
            (860.0, 899.0, 5.0),
        ),
    },
    "dec": {
        "BTC": (
            (18.0, 440.0, 20.0),
            (440.0, 550.0, 19.0),
            (550.0, 610.0, 18.0),
            (610.0, 670.0, 17.0),
            (670.0, 710.0, 16.0),
            (710.0, 750.0, 15.0),
            (750.0, 780.0, 14.0),
            (780.0, 830.0, 13.0),
            (830.0, 850.0, 12.0),
            (850.0, 880.0, 11.0),
            (880.0, 899.0, 10.0),
        ),
        "ETH": (
            (18.0, 470.0, 14.0),
            (470.0, 600.0, 13.0),
            (600.0, 680.0, 12.0),
            (680.0, 740.0, 11.0),
            (740.0, 780.0, 10.0),
            (780.0, 840.0, 9.0),
            (840.0, 899.0, 8.0),
        ),
    },
}


def parent_clip_for(age_s: float, *, asset: str, regime: str = "oct") -> float:
    """Return an evidence profile's original signed parent size.

    ``oct`` is the strict Oct-29 reconstruction.  ``nov`` and ``dec`` are
    out-of-sample execution profiles from fresh signed calldata and should be
    used as paper comparison regimes, not as claims about all dates.
    """
    reg = regime.lower()
    sym = asset.upper()
    if reg not in _PARENT_SCHEDULES:
        raise ValueError(f"unknown V5.3 regime {regime!r}")
    if sym not in _PARENT_SCHEDULES[reg]:
        raise ValueError("asset must be BTC or ETH")
    age = float(age_s)
    for start, end, shares in _PARENT_SCHEDULES[reg][sym]:
        if start <= age < end:
            return shares
    return 0.0


# ---------------------------------------------------------------------------
# Same-cent stack targets
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StackCalibration:
    underweight_base: int
    balanced_base: int
    overweight_base: int


_STACK_CALIBRATION: dict[str, StackCalibration] = {
    # Roughly follows observed p90 distinct-order depth by regime/role.
    "oct": StackCalibration(underweight_base=2, balanced_base=2, overweight_base=1),
    "nov": StackCalibration(underweight_base=4, balanced_base=4, overweight_base=3),
    "dec": StackCalibration(underweight_base=3, balanced_base=3, overweight_base=2),
}


def inventory_role(inv: Inventory, side: str) -> str:
    side = side.upper()
    if side not in ("UP", "DOWN"):
        raise ValueError("side must be UP or DOWN")
    if inv.abs_gap <= 1e-12:
        return "BALANCED"
    if side == inv.underweight:
        return "UNDERWEIGHT"
    return "OVERWEIGHT"


def stack_targets(
    inv: Inventory,
    *,
    side: str,
    parent_clip: float,
    logical_layers: int,
    regime: str = "oct",
) -> tuple[int, ...]:
    """Evidence-calibrated independent parent slots per logical price layer.

    The first one or two logical prices carry the extra same-cent capacity;
    deeper prices remain one-parent layers.  This is deliberately a paper
    adapter surface because public execution data identifies stacked orders but
    not the trader's exact hidden target-count formula.
    """
    n = max(0, int(logical_layers))
    if n == 0 or parent_clip <= 0:
        return ()
    reg = regime.lower()
    if reg not in _STACK_CALIBRATION:
        raise ValueError(f"unknown V5.3 regime {regime!r}")
    cal = _STACK_CALIBRATION[reg]
    role = inventory_role(inv, side)
    if role == "UNDERWEIGHT":
        base = cal.underweight_base
    elif role == "OVERWEIGHT":
        base = cal.overweight_base
    else:
        base = cal.balanced_base

    # Preserve the observed distinction between many independent parents at the
    # best logical cent and ordinary single-parent depth further from the anchor.
    out: list[int] = []
    for rank in range(n):
        if rank == 0:
            target = base
        elif rank == 1:
            target = max(1, base - 1)
        else:
            target = 1
        out.append(target)
    return tuple(out)


# ---------------------------------------------------------------------------
# Multi-parent sticky planner
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MultiParentPlan:
    backoff_2t_oids: tuple[str, ...]
    backoff_3plus_oids: tuple[str, ...]
    drop_oids: tuple[str, ...]
    keep_oids: tuple[str, ...]
    sticky_keep_oids: tuple[str, ...]
    hysteresis_1t_oids: tuple[str, ...]
    replenish_prices: tuple[float, ...]
    kept_levels: tuple[float, ...]


def adverse_ticks(price: float, current_base: float, tick: float) -> int:
    if tick <= 0:
        return 0
    return int(round((float(price) - float(current_base)) / float(tick)))


def plan_multi_parent_side(
    *,
    orders: Sequence[tuple[str, float, float]],
    current_base: float | None,
    desired_layers: int,
    stack_slots: Sequence[int],
    tick: float,
) -> MultiParentPlan:
    """Sticky 4->0 logical layers plus independent same-cent parent slots.

    ``orders`` are ``(oid, price, created_ts)``.  The oldest order at a price
    owns FIFO priority.  When a same-cent stack exceeds its target, newest
    copies are dropped first.  A one-tick adverse bid is preserved; 2+ ticks
    back off, matching V5.2a hysteresis.
    """
    n = max(0, int(desired_layers))
    slots = tuple(max(1, int(x)) for x in stack_slots[:n])
    if len(slots) < n:
        slots = slots + (1,) * (n - len(slots))

    theoretical = layer_prices(current_base, tick=tick, layers=n) if (
        current_base is not None and tick > 0 and n > 0
    ) else ()
    theo_set = {round(px, 10) for px in theoretical}

    by_price: dict[float, list[tuple[str, float, float]]] = {}
    back2: list[str] = []
    back3: list[str] = []
    hysteresis_candidates: set[str] = set()

    for oid, px, created in orders:
        key = round(float(px), 10)
        if current_base is not None:
            ticks = adverse_ticks(px, current_base, tick)
            if ticks >= 3:
                back3.append(oid)
                continue
            if ticks >= 2:
                back2.append(oid)
                continue
            if ticks == 1:
                hysteresis_candidates.add(oid)
        by_price.setdefault(key, []).append((oid, float(px), float(created)))

    # Distinct logical levels: highest price first, oldest representative first.
    levels: list[tuple[float, float, list[tuple[str, float, float]]]] = []
    for key, group in by_price.items():
        group.sort(key=lambda x: x[2])
        levels.append((key, group[0][2], group))
    levels.sort(key=lambda x: (-x[0], x[1]))

    kept_levels = levels[:n]
    dropped_levels = levels[n:]

    keep: list[str] = []
    drop: list[str] = []
    sticky: list[str] = []
    hyst_keep: list[str] = []
    replenish: list[float] = []
    occupied: set[float] = set()

    for rank, (key, _oldest, group) in enumerate(kept_levels):
        target = slots[rank] if rank < len(slots) else 1
        survivors = group[:target]
        extras = group[target:]
        drop.extend(o[0] for o in extras)
        occupied.add(key)
        for oid, px, _created in survivors:
            keep.append(oid)
            if round(px, 10) not in theo_set:
                sticky.append(oid)
            if oid in hysteresis_candidates:
                hyst_keep.append(oid)
        # Refill missing independent parents at a surviving sticky level before
        # creating a brand-new logical level.  This preserves FIFO survivors.
        missing = target - len(survivors)
        replenish.extend([group[0][1]] * max(0, missing))

    for _key, _oldest, group in dropped_levels:
        drop.extend(o[0] for o in group)

    # If fewer distinct logical levels survive than desired, add theoretical
    # prices not already occupied.  Each new level receives its rank's stack
    # target, so duplicate prices in replenish_prices are intentional.
    distinct_kept = len(kept_levels)
    if current_base is not None and tick > 0 and distinct_kept < n:
        rank = distinct_kept
        for px in theoretical:
            key = round(px, 10)
            if key in occupied:
                continue
            target = slots[rank] if rank < len(slots) else 1
            replenish.extend([float(px)] * target)
            occupied.add(key)
            rank += 1
            if rank >= n:
                break

    return MultiParentPlan(
        backoff_2t_oids=tuple(back2),
        backoff_3plus_oids=tuple(back3),
        drop_oids=tuple(drop),
        keep_oids=tuple(keep),
        sticky_keep_oids=tuple(sticky),
        hysteresis_1t_oids=tuple(hyst_keep),
        replenish_prices=tuple(replenish),
        kept_levels=tuple(x[0] for x in kept_levels),
    )


# ---------------------------------------------------------------------------
# Finite-volume tape allocator with same-cent stacks
# ---------------------------------------------------------------------------


def apply_sell_print_to_multi_orders(
    orders: Iterable[ShadowOrder],
    *,
    trade_price: float,
    trade_size: float,
) -> list[tuple[ShadowOrder, float]]:
    """Allocate one public SELL print once across stacked BUY parents.

    Unlike the V5 allocator, an exact-price print may continue through several
    of our independent orders at that *same* cent.  It still cannot leak into a
    lower bid level unless the observed trade price itself is below that level.
    """
    volume = max(0.0, float(trade_size))
    if volume <= 0:
        return []
    px = float(trade_price)
    eps = 1e-9
    fills: list[tuple[ShadowOrder, float]] = []
    live = sorted(
        (o for o in orders if not o.done),
        key=lambda o: (-o.price, o.posted_ts),
    )
    for o in live:
        if volume <= eps:
            break
        if px > o.price + eps:
            # The print is above this bid; it cannot reach this or lower prices.
            break
        if px < o.price - eps:
            o.queue_ahead = 0.0
        else:
            if math.isinf(o.queue_ahead):
                # Unknown queue at this exact level blocks this and later orders
                # at the same/lower price in the conservative paper model.
                break
            queue = max(0.0, float(o.queue_ahead))
            consumed = min(queue, volume)
            o.queue_ahead = queue - consumed
            volume -= consumed
            if volume <= eps:
                break
        delta = min(o.remaining, volume)
        if delta > eps:
            o.filled += delta
            volume -= delta
            fills.append((o, delta))
        # No explicit break on an exact-price fill: the next order may be another
        # independent parent at the same cent.  Sorting plus the px > lower-bid
        # check above stops allocation before any lower bid level.
    return fills


# ---------------------------------------------------------------------------
# Aggressive BUY candidate helpers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AggressiveCandidate:
    side: str
    shares: float
    execution_vwap: float
    limit_price: float
    repair_basis: float | None
    fresh_pair_basis: float | None
    closes_unmatched: float
    score: float
    reason: str


def depth_vwap(asks: Sequence[tuple[float, float]], shares: float) -> float | None:
    need = float(shares)
    if need <= 0:
        return None
    cost = 0.0
    for px, qty in asks:
        if need <= 1e-12:
            break
        take = min(max(0.0, float(qty)), need)
        cost += take * float(px)
        need -= take
    if need > 1e-8:
        return None
    return cost / float(shares)


def aggressive_candidate(
    *,
    side: str,
    shares: float,
    own_asks: Sequence[tuple[float, float]],
    opposite_asks: Sequence[tuple[float, float]],
    opposite_unmatched_qty: float,
    opposite_unmatched_vwap: float | None,
    repair_basis_cap: float = 1.05,
    fresh_pair_cap: float = 1.00,
    headroom: float = 0.0,
) -> AggressiveCandidate | None:
    """Construct an evidence-based marketable BUY candidate.

    This is intentionally not a claim about the hidden historical trigger.  A
    parent may route aggressively when either (a) it closes unmatched inventory
    at an acceptable pair basis or (b) both sides are immediately acquirable at
    an unusually cheap fresh-pair basis.  The parent is never deficit-clipped.
    """
    q = float(shares)
    if q <= 0:
        return None
    own_vwap = depth_vwap(own_asks, q)
    if own_vwap is None:
        return None

    closes = min(q, max(0.0, float(opposite_unmatched_qty)))
    repair_basis = None
    repair_ok = False
    if closes > 1e-12 and opposite_unmatched_vwap is not None:
        repair_basis = own_vwap + float(opposite_unmatched_vwap)
        repair_ok = repair_basis <= float(repair_basis_cap) + 1e-12

    opposite_vwap = depth_vwap(opposite_asks, q)
    fresh_pair_basis = None if opposite_vwap is None else own_vwap + opposite_vwap
    fresh_ok = (
        fresh_pair_basis is not None
        and fresh_pair_basis <= float(fresh_pair_cap) + 1e-12
    )

    if not repair_ok and not fresh_ok:
        return None

    if repair_ok and fresh_ok:
        reason = "repair+fresh-pair"
        score = min(float(repair_basis), float(fresh_pair_basis))
    elif repair_ok:
        reason = "unmatched-repair"
        score = float(repair_basis)
    else:
        reason = "fresh-pair"
        score = float(fresh_pair_basis)

    best_ask = float(own_asks[0][0]) if own_asks else own_vwap
    limit_price = min(0.99, max(best_ask, own_vwap) + max(0.0, float(headroom)))
    return AggressiveCandidate(
        side=side.upper(),
        shares=q,
        execution_vwap=own_vwap,
        limit_price=limit_price,
        repair_basis=repair_basis,
        fresh_pair_basis=fresh_pair_basis,
        closes_unmatched=closes,
        score=score,
        reason=reason,
    )
