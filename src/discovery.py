"""
Discovery of 15-minute BTC/ETH Up-or-Down markets.

DETERMINISTIC, NOT SCRAPED
--------------------------
Slugs for this series are fully deterministic:

    {asset}-updown-15m-{window_start_epoch}        e.g. eth-updown-15m-1761728400

where window_start_epoch is the unix time of the window's first second and is
always a multiple of 900. (Verified against 916 real markets in the
gabagool22 dataset.) We therefore *construct* the slug for the current and
next windows and confirm it via the Gamma API, instead of paging through
market lists.

WHY THE accepting_orders GATE MATTERS
-------------------------------------
The old bot fired 16,500 orders into markets that returned
`425 service not ready` / MARKET_NOT_READY, mostly while hammering a window
that hadn't opened its book yet. Here a window is handed to the maker loop
only after Gamma reports `accepting_orders=True`, and the maker loop's own
backoff covers the residual race.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from .constants import UPDOWN_SLUG_TEMPLATE, WINDOW_SECONDS

log = logging.getLogger("discovery")


@dataclass(frozen=True)
class UpDownMarket:
    asset: str                 # "btc" | "eth"
    slug: str
    market_id: str             # gamma market id (used by redeem_positions)
    condition_id: str
    up_token_id: str
    down_token_id: str
    neg_risk: bool
    window_start: int          # unix epoch, inclusive
    window_end: int            # unix epoch, exclusive
    accepting_orders: bool

    @property
    def seconds_to_end(self) -> float:
        return self.window_end - time.time()

    def __str__(self) -> str:  # compact log form
        end = datetime.fromtimestamp(self.window_end, tz=timezone.utc).strftime("%H:%M")
        return f"{self.slug} (ends {end}Z)"


def window_start_epoch(now: float | None = None) -> int:
    t = int(now if now is not None else time.time())
    return t - (t % WINDOW_SECONDS)


def slug_for(asset: str, start_epoch: int) -> str:
    return UPDOWN_SLUG_TEMPLATE.format(asset=asset.lower(), window_start=start_epoch)


def candidate_slugs(assets: tuple[str, ...], now: float | None = None) -> list[tuple[str, int, str]]:
    """(asset, window_start, slug) for the current and the next window."""
    cur = window_start_epoch(now)
    out: list[tuple[str, int, str]] = []
    for start in (cur, cur + WINDOW_SECONDS):
        for asset in assets:
            out.append((asset, start, slug_for(asset, start)))
    return out


def _outcome_token(market, label: str) -> str | None:
    """Map a gamma Market's outcomes to a token id by label.

    Up/Down series put 'Up' in the yes-slot and 'Down' in the no-slot, but we
    match by label first and only fall back to slot position, so a label swap
    upstream cannot silently flip our sides (that would be a directional bug)."""
    outcomes = getattr(market, "outcomes", None)
    if outcomes is None:
        return None
    slots = [getattr(outcomes, "yes", None), getattr(outcomes, "no", None)]
    for slot in slots:
        if slot is not None and (slot.label or "").strip().lower() == label:
            return str(slot.token_id) if slot.token_id else None
    # fallback by position: yes==up, no==down
    idx = 0 if label == "up" else 1
    slot = slots[idx]
    return str(slot.token_id) if (slot is not None and slot.token_id) else None


async def resolve_market(client, asset: str, start_epoch: int) -> UpDownMarket | None:
    """Fetch one constructed slug from Gamma and normalize it. Returns None if
    the market doesn't exist (series gap) or lacks tradable structure."""
    slug = slug_for(asset, start_epoch)
    try:
        m = await client.get_market(slug=slug)
    except Exception as exc:  # noqa: BLE001 — not-found and transport both land here
        log.debug("get_market(%s) -> %s", slug, exc)
        return None
    if m is None or not getattr(m, "condition_id", None):
        return None

    up_tok = _outcome_token(m, "up")
    down_tok = _outcome_token(m, "down")
    if not up_tok or not down_tok:
        log.warning("Market %s missing Up/Down token ids — skipping", slug)
        return None

    state = getattr(m, "state", None)
    accepting = bool(getattr(state, "accepting_orders", False))
    neg_risk = bool(getattr(state, "neg_risk", False))
    end_dt = getattr(state, "end_date", None)
    window_end = (
        int(end_dt.timestamp()) if end_dt is not None else start_epoch + WINDOW_SECONDS
    )

    return UpDownMarket(
        asset=asset,
        slug=slug,
        market_id=str(m.id),
        condition_id=str(m.condition_id),
        up_token_id=up_tok,
        down_token_id=down_tok,
        neg_risk=neg_risk,
        window_start=start_epoch,
        window_end=window_end,
        accepting_orders=accepting,
    )


async def discover(client, assets: tuple[str, ...]) -> list[UpDownMarket]:
    """All resolvable current+next windows for the configured assets,
    soonest-ending first."""
    found: list[UpDownMarket] = []
    for asset, start, _slug in candidate_slugs(assets):
        mkt = await resolve_market(client, asset, start)
        if mkt is not None and mkt.seconds_to_end > 0:
            found.append(mkt)
    found.sort(key=lambda m: m.window_end)
    return found
