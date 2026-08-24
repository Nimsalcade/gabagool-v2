"""Paper harness records unmatched-pool telemetry and never uses it as a gate."""
from __future__ import annotations

import argparse
import csv
import io
from types import SimpleNamespace

from tools.run_forensic_15m_paper import EVENT_FIELDS, Engine


def _engine() -> Engine:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EVENT_FIELDS)
    writer.writeheader()
    args = argparse.Namespace(
        paper_cash=2_000.0,
        quote_ttl=10.0,
        maker_fill_backend="public_tape",
        taker_mode="off",
        max_combined_vwap=1.01,
        poll=0.5,
        fresh_pair_cap=0.0,
    )
    market = SimpleNamespace(
        asset="btc",
        slug="btc-updown-15m-test",
        condition_id="0xcond",
        window_start=1_000_000.0,
        window_end=1_000_900.0,
        up_token_id="tokU",
        down_token_id="tokD",
    )
    eng = Engine(session=1, market=market, writer=writer, args=args)
    eng.clip = 10.0
    eng._events_buf = buf
    return eng


def _fills(eng: Engine) -> list[dict[str, str]]:
    eng._events_buf.seek(0)
    return [r for r in csv.DictReader(eng._events_buf) if r["event"] == "MAKER_FILL"]


def test_fill_emits_unmatched_columns_and_accepts_expensive_repair():
    e = _engine()
    now = e.market.window_start + 30.0
    assert e.fill(now, "UP", 10.0, 0.35, "MAKER_FILL", "test")
    assert e.fill(now + 1.0, "DOWN", 10.0, 0.90, "MAKER_FILL", "test")
    rows = _fills(e)
    assert len(rows) == 2
    close = rows[1]
    assert abs(float(close["repair_basis"]) - 1.25) < 1e-9
    assert abs(float(close["closing_qty"]) - 10) < 1e-9
    assert abs(float(close["overshoot_qty"])) < 1e-9
    assert abs(float(close["completed_set_cost_vwap_cumulative"]) - 1.25) < 1e-9
    # Not an admission filter: the expensive fill was booked.
    assert e.inv.down_shares == 10
    assert e.result.maker_fills == 2


def test_parent_residue_is_new_unmatched_not_a_resize():
    e = _engine()
    now = e.market.window_start + 40.0
    e.fill(now, "UP", 4.0, 0.07, "MAKER_FILL", "test")
    e.fill(now + 1.0, "DOWN", 10.0, 0.90, "MAKER_FILL", "test")
    row = _fills(e)[-1]
    assert abs(float(row["closing_qty"]) - 4) < 1e-9
    assert abs(float(row["overshoot_qty"]) - 6) < 1e-9
    assert abs(float(row["repair_basis"]) - 0.97) < 1e-9
    assert abs(float(row["unmatched_down_after"]) - 6) < 1e-9
    assert abs(float(row["unmatched_up_after"])) < 1e-9
