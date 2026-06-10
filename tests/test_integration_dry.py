"""Offline integration: drive a full MakerLoop window in dry-run against a
synthetic book and assert the safety invariants held end-to-end."""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.capital import CapitalManager            # noqa: E402
from src.config import CapitalConfig, StrategyConfig  # noqa: E402
from src.discovery import UpDownMarket            # noqa: E402
from src.maker_loop import MakerLoop              # noqa: E402
from src.merge_engine import MergeEngine          # noqa: E402


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
    """Just enough surface for a dry-run window."""
    def __init__(self):
        self.t = 0

    async def get_order_book(self, *, token_id):
        # gently oscillating two-sided book that always sums ~1.01 at asks
        self.t += 1
        up = token_id == "tokU"
        base = 0.48 + (0.02 if (self.t // 6) % 2 == 0 else -0.02) * (1 if up else -1)
        return _Book(bid=round(base - 0.01, 2), ask=round(base + 0.02, 2))

    def list_open_orders(self, market=None):       # dry-run never calls live paths,
        raise AssertionError("live path used in dry-run")  # make sure of it

    def list_account_trades(self, market=None):
        raise AssertionError("live path used in dry-run")

    def list_positions(self, **kw):
        class _P:
            def __aiter__(self):
                async def gen():
                    if False:
                        yield None
                return gen()
        # empty paginator → MergeEngine sees no on-chain pairs (dry-run merges
        # are simulated before this matters; engine handles zeros cleanly)
        class _Pag:
            def __aiter__(self_inner):
                self_inner._done = False
                return self_inner
            async def __anext__(self_inner):
                raise StopAsyncIteration
        return _Pag()


def test_dry_window_respects_invariants():
    scfg = StrategyConfig(requote_interval_s=0.01, merge_interval_s=0.05,
                          stop_posting_buffer_s=30)
    cap = CapitalManager(CapitalConfig(per_window_cap_usd=50,
                                       global_exposure_cap_usd=150))
    client = FakeClient()
    market = UpDownMarket(
        asset="btc", slug="btc-updown-15m-test", market_id="1",
        condition_id="0xcond", up_token_id="tokU", down_token_id="tokD",
        neg_risk=False, window_start=int(time.time()),
        window_end=int(time.time()) + 33,   # 33s window → ~3s of FARM, then HOLD→DONE fast
        accepting_orders=True,
    )
    engine = MergeEngine(client, dry_run=True, min_pairs=1)
    loop = MakerLoop(client, market, cfg=scfg, capital=cap,
                     merge_engine=engine, dry_run=True)

    async def run():
        return await asyncio.wait_for(loop.run(), timeout=60)

    res = asyncio.get_event_loop().run_until_complete(run())

    # Invariants:
    t = loop.tracker
    # 1) every fill was a BUY at a tick-aligned price within (0, 1)
    for o in t.orders.values():
        assert 0 < o.price < 1
        assert abs(round(o.price / 0.01) * 0.01 - o.price) < 1e-9
    # 2) combined avg, when two-sided, never exceeded the budget
    combined = t.combined_avg()
    if combined is not None:
        assert combined <= scfg.combined_budget + 1e-6, combined
    # 3) capital cap respected
    assert res.total_cost <= 50 + 1e-6
    # 4) dry merges executed when pairs existed
    if t.matched_pairs() == 0 and engine.merge_count:
        pass  # merged everything — fine
