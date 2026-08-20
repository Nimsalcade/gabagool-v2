"""Offline dry-run integration for the mixed execution loop."""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.capital import CapitalManager
from src.config import CapitalConfig, StrategyConfig
from src.discovery import UpDownMarket
from src.maker_loop import MakerLoop


class _Level:
    def __init__(self, price, size=100):
        self.price, self.size = price, size


class _Book:
    def __init__(self, bid, ask):
        self.bids = [_Level(bid)]
        self.asks = [_Level(ask)]
        self.tick_size = "0.01"
        self.min_order_size = "5"
        self.neg_risk = False


class FakeClient:
    def __init__(self):
        self.t = 0

    async def get_order_book(self, *, token_id):
        self.t += 1
        up = token_id == "tokU"
        shift = .02 if (self.t // 8) % 2 == 0 else -.02
        mid = .49 + (shift if up else -shift)
        return _Book(round(mid - .02, 2), round(mid + .02, 2))

    async def cancel_market_orders(self, **kwargs):
        return None

    async def cancel_order(self, **kwargs):
        return None


def test_dry_window_respects_policy_bounds():
    now = int(time.time())
    scfg = StrategyConfig(
        requote_interval_s=.01,
        stop_posting_buffer_s=1,
        taker_stop_buffer_s=1,
        entry_delay_by_duration_s={300: 0.0, 900: 0.0},
        max_combined_vwap=1.01,
    )
    cap = CapitalManager(
        CapitalConfig(per_window_cap_usd=100, global_exposure_cap_usd=100)
    )
    market = UpDownMarket(
        asset="btc", duration_s=300, slug="btc-updown-5m-test", market_id="1",
        condition_id="0xcond", up_token_id="tokU", down_token_id="tokD",
        neg_risk=False, window_start=now, window_end=now + 4, accepting_orders=True,
    )
    loop = MakerLoop(FakeClient(), market, cfg=scfg, capital=cap, dry_run=True)

    async def run():
        return await asyncio.wait_for(loop.run(), timeout=10)

    res = asyncio.run(run())
    assert res.total_cost <= 100 + 1e-6
    combined = loop.tracker.combined_avg()
    if combined is not None:
        assert combined <= scfg.max_combined_vwap + 1e-6
    assert all(o.mode in ("maker", "taker") for o in loop.tracker.orders.values())
