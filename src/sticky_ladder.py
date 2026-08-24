"""Paper-only sticky complementary ladder.

Execution lifecycle, not Gabagool pricing.  Resting bids that are still at or
below the current allowed base keep FIFO priority when the complementary
anchor *rises*.  V5.2a hysteresis: a bid exactly one tick above the allowed
base is kept; 2+ ticks too aggressive are backed off.  Vacancies replenish at
the current anchor.  Inventory 4→0 still drops extra layers.

This is a paper candidate, not recovered source.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.forensic_15m import layer_prices


@dataclass(frozen=True)
class StickySidePlan:
    backoff_oids: tuple[str, ...]
    backoff_2t_oids: tuple[str, ...]
    backoff_3plus_oids: tuple[str, ...]
    drop_oids: tuple[str, ...]
    keep_oids: tuple[str, ...]
    sticky_keep_oids: tuple[str, ...]
    hysteresis_1t_oids: tuple[str, ...]
    replenish_prices: tuple[float, ...]
    skipped_higher: tuple[float, ...]


def adverse_ticks(price: float, current_base: float, tick: float) -> int:
    """Nearest tick count by which ``price`` exceeds ``current_base``."""
    if tick <= 0:
        return 0
    return int(round((float(price) - float(current_base)) / float(tick)))


def plan_sticky_side(
    *,
    orders: list[tuple[str, float, float]],
    current_base: float | None,
    desired_n: int,
    tick: float,
) -> StickySidePlan:
    """Decide keep / backoff / drop / replenish for one outcome.

    ``orders`` are ``(oid, price, created_ts)``.  Lowest-price (deepest) extra
    layers are dropped first when inventory shrinks the allowed count.

    Adverse drift:
      <= 0 ticks  keep (fully valid)
      == 1 tick   keep (V5.2a hysteresis)
      >= 2 ticks  back off
    """
    n = max(0, int(desired_n))
    theoretical = layer_prices(current_base, tick=tick, layers=n) if (
        current_base is not None and tick > 0 and n > 0
    ) else ()
    theo_set = {round(px, 10) for px in theoretical}

    backoff_2t: list[str] = []
    backoff_3p: list[str] = []
    hyst: list[str] = []
    remain: list[tuple[str, float, float]] = []
    for oid, px, created in orders:
        if current_base is None:
            remain.append((oid, px, created))
            continue
        ticks = adverse_ticks(px, current_base, tick)
        if ticks >= 3:
            backoff_3p.append(oid)
        elif ticks >= 2:
            backoff_2t.append(oid)
        else:
            if ticks == 1:
                hyst.append(oid)
            remain.append((oid, px, created))

    remain.sort(key=lambda x: (-x[1], x[2]))  # highest price, oldest first
    keep = remain[:n]
    drop = remain[n:]
    keep_ids = {o[0] for o in keep}
    keep_oids = tuple(o[0] for o in keep)
    sticky = tuple(o[0] for o in keep if round(o[1], 10) not in theo_set)
    hyst_keep = tuple(oid for oid in hyst if oid in keep_ids)

    occupied = {round(o[1], 10) for o in keep}
    replenish: list[float] = []
    skipped: list[float] = []
    if current_base is not None and tick > 0 and n > 0:
        for px in theoretical:
            if len(keep) + len(replenish) >= n:
                if round(px, 10) not in occupied:
                    skipped.append(px)
                continue
            if round(px, 10) in occupied:
                continue
            replenish.append(px)

    backoff = tuple(backoff_2t + backoff_3p)
    return StickySidePlan(
        backoff_oids=backoff,
        backoff_2t_oids=tuple(backoff_2t),
        backoff_3plus_oids=tuple(backoff_3p),
        drop_oids=tuple(o[0] for o in drop),
        keep_oids=keep_oids,
        sticky_keep_oids=sticky,
        hysteresis_1t_oids=hyst_keep,
        replenish_prices=tuple(replenish),
        skipped_higher=tuple(skipped),
    )
