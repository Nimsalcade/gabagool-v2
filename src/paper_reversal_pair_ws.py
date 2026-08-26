"""WebSocket paper-live runner for the BTC 5-minute reversal complete-set strategy."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from dataclasses import replace
from typing import Any

from polymarket import AsyncPublicClient
from polymarket.models.clob.market_events import (
    MarketBestBidAskEvent,
    MarketBookEvent,
    MarketLastTradePriceEvent,
    MarketPriceChangeEvent,
)
from polymarket.streams import MarketSpec

from .discovery import resolve_market, window_start_epoch
from .paper_reversal_pair import PaperConfig, PaperLedger, Snapshot, _book_top, _market_fee_rate
from .reversal_pair import Phase, ReversalState, TopOfBook, build_pair_fill, estimated_effective_pair_cost

log = logging.getLogger("paper_reversal_pair_ws")


def _replace_quote(top: TopOfBook | None, *, best_bid: float | None = None, best_ask: float | None = None) -> TopOfBook | None:
    if top is None:
        if best_bid is None or best_ask is None:
            return None
        return TopOfBook(float(best_bid), float(best_ask), 0.0, 0.0)
    return replace(
        top,
        best_bid=top.best_bid if best_bid is None else float(best_bid),
        best_ask=top.best_ask if best_ask is None else float(best_ask),
    )


def _event_token_id(event: Any) -> str | None:
    token = getattr(getattr(event, "payload", None), "token_id", None)
    return None if token is None else str(token)


def _apply_event(event: Any, *, up_token_id: str, down_token_id: str, up: TopOfBook | None, down: TopOfBook | None) -> tuple[TopOfBook | None, TopOfBook | None, float | None, str | None]:
    token_id = _event_token_id(event)
    payload = getattr(event, "payload", None)
    event_ts = None
    last_trade = None
    ts = getattr(payload, "timestamp", None)
    if ts is not None:
        try:
            event_ts = float(ts.timestamp())
        except Exception:
            pass

    if isinstance(event, MarketBookEvent):
        fresh = _book_top(event.payload)
        if fresh is not None:
            if token_id == up_token_id:
                up = fresh
            elif token_id == down_token_id:
                down = fresh
    elif isinstance(event, MarketBestBidAskEvent):
        bid = None if event.payload.best_bid is None else float(event.payload.best_bid)
        ask = None if event.payload.best_ask is None else float(event.payload.best_ask)
        if token_id == up_token_id:
            up = _replace_quote(up, best_bid=bid, best_ask=ask)
        elif token_id == down_token_id:
            down = _replace_quote(down, best_bid=bid, best_ask=ask)
    elif isinstance(event, MarketPriceChangeEvent):
        for change in event.payload.price_changes:
            tid = str(change.token_id)
            bid = None if change.best_bid is None else float(change.best_bid)
            ask = None if change.best_ask is None else float(change.best_ask)
            if tid == up_token_id:
                up = _replace_quote(up, best_bid=bid, best_ask=ask)
            elif tid == down_token_id:
                down = _replace_quote(down, best_bid=bid, best_ask=ask)
    elif isinstance(event, MarketLastTradePriceEvent) and token_id in (up_token_id, down_token_id):
        side = "UP" if token_id == up_token_id else "DOWN"
        last_trade = f"{side}@{float(event.payload.price)*100:.1f}c"
    return up, down, event_ts, last_trade


async def _initial_snapshot(client: AsyncPublicClient, market) -> Snapshot | None:
    books = await client.get_order_books(token_ids=[market.up_token_id, market.down_token_id])
    by_token = {str(book.token_id): book for book in books}
    ub = by_token.get(str(market.up_token_id))
    db = by_token.get(str(market.down_token_id))
    if ub is None or db is None:
        return None
    up, down = _book_top(ub), _book_top(db)
    return None if up is None or down is None else Snapshot(up=up, down=down)


async def _confirm_executable_snapshot(client: AsyncPublicClient, market, streamed: Snapshot) -> Snapshot | None:
    snap = await _initial_snapshot(client, market)
    if snap is None:
        return None
    if abs(snap.up.best_ask - streamed.up.best_ask) > 1e-9:
        return None
    if abs(snap.down.best_ask - streamed.down.best_ask) > 1e-9:
        return None
    return snap


async def _run_one_market(client: AsyncPublicClient, market, *, cfg: PaperConfig, ledger: PaperLedger) -> None:
    state = ReversalState(cfg.leader_threshold, cfg.collapse_threshold)
    fee_rate, fee_source = await _market_fee_rate(client, market, cfg.taker_fee_rate)
    initial = await _initial_snapshot(client, market)
    if initial is None:
        log.warning("skip %s: initial order books unavailable", market.slug)
        return

    up, down = initial.up, initial.down
    state.observe(up_mid=up.midpoint, down_mid=down.midpoint, ts=time.time())
    event_count = 0
    last_event_wall = time.time()
    last_exchange_ts: float | None = None
    last_trade = "-"
    last_status = 0.0
    traded = market.condition_id in ledger.traded_conditions

    log.info("MARKET %s | WS | fee_rate=%.4f (%s)%s", market.slug, fee_rate, fee_source, " | already paper-traded" if traded else "")

    spec = MarketSpec(token_ids=[market.up_token_id, market.down_token_id], custom_feature_enabled=True)
    async with await client.subscribe(spec) as stream:
        iterator = stream.__aiter__()
        while market.seconds_to_end > 0:
            event = None
            try:
                event = await asyncio.wait_for(iterator.__anext__(), timeout=min(1.0, max(0.05, market.seconds_to_end)))
            except asyncio.TimeoutError:
                pass
            except StopAsyncIteration as exc:
                raise RuntimeError("market WebSocket stream ended") from exc

            if event is not None:
                up, down, ets, trade = _apply_event(
                    event,
                    up_token_id=str(market.up_token_id),
                    down_token_id=str(market.down_token_id),
                    up=up,
                    down=down,
                )
                event_count += 1
                last_event_wall = time.time()
                if ets is not None:
                    last_exchange_ts = ets
                if trade is not None:
                    last_trade = trade

                if up is not None and down is not None:
                    signals = state.observe(up_mid=up.midpoint, down_mid=down.midpoint, ts=last_exchange_ts or time.time())
                    for s in signals:
                        if s.kind == "LEADER":
                            log.info("LEADER %s %.1fc | WS event=%d age=%.1fs", s.side, s.midpoint * 100, event_count, time.time() - market.window_start)
                        elif s.kind == "COLLAPSE":
                            log.info("COLLAPSE %s %.1fc | ARMED | WS event=%d age=%.1fs", s.side, s.midpoint * 100, event_count, time.time() - market.window_start)

            if up is None or down is None:
                continue

            streamed = Snapshot(up=up, down=down)
            pair_est = estimated_effective_pair_cost(up.best_ask, down.best_ask, fee_rate)
            if not traded and state.phase is Phase.ARMED and pair_est <= cfg.max_effective_pair_cost + 1e-12:
                confirmed = await _confirm_executable_snapshot(client, market, streamed)
                if confirmed is None:
                    log.info("PAIR SIGNAL %.4f rejected: REST depth/BBO did not match WS snapshot", pair_est)
                else:
                    fill = build_pair_fill(
                        up=confirmed.up,
                        down=confirmed.down,
                        fee_rate=fee_rate,
                        max_effective_pair_cost=cfg.max_effective_pair_cost,
                        capital_limit_usd=min(cfg.max_market_capital_usd, ledger.balance),
                        min_pair_shares=cfg.min_pair_shares,
                        share_precision=cfg.share_precision,
                    )
                    if fill is not None:
                        ledger.record(market=market, state=state, snapshot=confirmed, fee_rate=fee_rate, fill=fill)
                        state.mark_filled()
                        traded = True
                        log.info("PAPER FILL WS | UP %.1fc + DOWN %.1fc | %.3f sh | cost=$%.2f | pair=%.4f", fill.up_price * 100, fill.down_price * 100, fill.shares, fill.total_cost, fill.effective_pair_cost)
                        log.info("PAPER MERGE | value=$%.2f profit=+$%.2f ROI=%.2f%% | balance=$%.2f", fill.payout_value, fill.locked_profit, fill.roi_on_cost * 100, ledger.balance)

            now = time.time()
            if now - last_status >= cfg.status_interval_s:
                last_status = now
                wall_age = max(0.0, now - last_event_wall)
                exch_age = None if last_exchange_ts is None else max(0.0, now - last_exchange_ts)
                log.info(
                    "STATUS WS | phase=%s UP %.1f/%.1f sz=%.1f DOWN %.1f/%.1f sz=%.1f pair_est=%.4f T-%ds | events=%d event_age=%.1fs exch_age=%s last=%s | trades=%d pnl=%+.2f bal=%.2f",
                    state.phase.value,
                    up.best_bid * 100,
                    up.best_ask * 100,
                    up.ask_size,
                    down.best_bid * 100,
                    down.best_ask * 100,
                    down.ask_size,
                    pair_est,
                    max(0, int(market.seconds_to_end)),
                    event_count,
                    wall_age,
                    "n/a" if exch_age is None else f"{exch_age:.1f}s",
                    last_trade,
                    ledger.trade_count,
                    ledger.total_profit,
                    ledger.balance,
                )


async def run_paper_live_ws(cfg: PaperConfig) -> None:
    client = AsyncPublicClient()
    ledger = PaperLedger(cfg.results_csv, cfg.starting_balance_usd)
    log.info("PAPER-LIVE WS start | BTC 5m | balance=$%.2f | leader>=%.2f collapse<=%.2f pair<=%.2f", ledger.balance, cfg.leader_threshold, cfg.collapse_threshold, cfg.max_effective_pair_cost)
    log.info("results -> %s", cfg.results_csv)
    try:
        while True:
            start = window_start_epoch(cfg.duration_s)
            market = await resolve_market(client, cfg.asset, cfg.duration_s, start)
            if market is None:
                log.warning("current BTC 5m market not available yet")
                await asyncio.sleep(1.0)
                continue
            if cfg.require_non_neg_risk and market.neg_risk:
                log.warning("skip %s: neg-risk market", market.slug)
                await asyncio.sleep(max(1.0, market.seconds_to_end + 0.1))
                continue
            try:
                await _run_one_market(client, market, cfg=cfg, ledger=ledger)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if market.seconds_to_end > 0:
                    log.warning("WS stream failure for %s: %s; reconnecting", market.slug, exc)
                    await asyncio.sleep(1.0)
                    continue
                log.warning("market stream ended at close: %s", exc)
            await asyncio.sleep(0.25)
    finally:
        await client.close()


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    for noisy in ("httpx", "httpcore", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> None:
    ap = argparse.ArgumentParser(description="BTC 5m reversal complete-set realtime WebSocket paper runner")
    ap.add_argument("--config", default="config/reversal_pair_paper.yaml")
    args = ap.parse_args()
    cfg = PaperConfig.load(args.config)
    _setup_logging()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run_paper_live_ws(cfg))

    def _stop(*_args: object) -> None:
        if not task.done():
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _stop())
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
