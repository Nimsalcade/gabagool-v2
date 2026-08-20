"""Discovery for current 5-minute and 15-minute crypto Up/Down markets."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from .constants import SUPPORTED_DURATIONS, UPDOWN_SLUG_TEMPLATES

log = logging.getLogger("discovery")


@dataclass(frozen=True)
class UpDownMarket:
    asset: str
    duration_s: int
    slug: str
    market_id: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    neg_risk: bool
    window_start: int
    window_end: int
    accepting_orders: bool

    @property
    def seconds_to_end(self) -> float:
        return self.window_end - time.time()

    @property
    def age_seconds(self) -> float:
        return time.time() - self.window_start

    def __str__(self) -> str:
        end = datetime.fromtimestamp(self.window_end, tz=timezone.utc).strftime("%H:%M:%S")
        return f"{self.slug} (ends {end}Z)"


def window_start_epoch(duration_s: int, now: float | None = None) -> int:
    duration_s = int(duration_s)
    if duration_s not in SUPPORTED_DURATIONS:
        raise ValueError(f"unsupported duration {duration_s}")
    t = int(now if now is not None else time.time())
    return t - (t % duration_s)


def slug_for(asset: str, duration_s: int, start_epoch: int) -> str:
    template = UPDOWN_SLUG_TEMPLATES[int(duration_s)]
    return template.format(asset=asset.lower(), window_start=int(start_epoch))


def candidate_specs(
    assets: tuple[str, ...], durations: tuple[int, ...], now: float | None = None
) -> list[tuple[str, int, int, str]]:
    """Current market for every configured asset/duration combination."""
    out = []
    for duration in durations:
        start = window_start_epoch(int(duration), now)
        for asset in assets:
            out.append((asset, int(duration), start, slug_for(asset, int(duration), start)))
    return out


def _outcome_token(market, label: str) -> str | None:
    outcomes = getattr(market, "outcomes", None)
    if outcomes is None:
        return None
    slots = [getattr(outcomes, "yes", None), getattr(outcomes, "no", None)]
    for slot in slots:
        if slot is not None and (slot.label or "").strip().lower() == label:
            return str(slot.token_id) if slot.token_id else None
    idx = 0 if label == "up" else 1
    slot = slots[idx]
    return str(slot.token_id) if (slot is not None and slot.token_id) else None


async def resolve_market(client, asset: str, duration_s: int, start_epoch: int) -> UpDownMarket | None:
    slug = slug_for(asset, duration_s, start_epoch)
    try:
        m = await client.get_market(slug=slug)
    except Exception as exc:  # noqa: BLE001
        log.debug("get_market(%s) -> %s", slug, exc)
        return None
    if m is None or not getattr(m, "condition_id", None):
        return None

    up_tok = _outcome_token(m, "up")
    down_tok = _outcome_token(m, "down")
    if not up_tok or not down_tok:
        log.warning("market %s missing Up/Down token ids", slug)
        return None

    state = getattr(m, "state", None)
    accepting = bool(getattr(state, "accepting_orders", False))
    neg_risk = bool(getattr(state, "neg_risk", False))
    end_dt = getattr(state, "end_date", None)
    window_end = int(end_dt.timestamp()) if end_dt is not None else start_epoch + duration_s

    return UpDownMarket(
        asset=asset,
        duration_s=int(duration_s),
        slug=slug,
        market_id=str(m.id),
        condition_id=str(m.condition_id),
        up_token_id=up_tok,
        down_token_id=down_tok,
        neg_risk=neg_risk,
        window_start=int(start_epoch),
        window_end=window_end,
        accepting_orders=accepting,
    )


async def discover(
    client, assets: tuple[str, ...], durations: tuple[int, ...]
) -> list[UpDownMarket]:
    found: list[UpDownMarket] = []
    for asset, duration, start, _ in candidate_specs(assets, durations):
        mkt = await resolve_market(client, asset, duration, start)
        if mkt is not None and mkt.seconds_to_end > 0:
            found.append(mkt)
    found.sort(key=lambda m: (m.window_end, m.asset))
    return found
