"""SQLite order/fill/settlement ledger with reconciliation."""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

log = logging.getLogger("ledger")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY, ts REAL, condition_id TEXT, side TEXT,
    price REAL, shares REAL, status TEXT DEFAULT 'open'
);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, condition_id TEXT,
    order_id TEXT, side TEXT, price REAL, shares REAL
);
CREATE TABLE IF NOT EXISTS merges (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, condition_id TEXT,
    pairs REAL, tx_hash TEXT, ok INTEGER, error TEXT
);
CREATE TABLE IF NOT EXISTS redeems (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, condition_id TEXT,
    tx_hash TEXT, ok INTEGER, error TEXT
);
CREATE TABLE IF NOT EXISTS balances (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, pusd REAL
);
CREATE TABLE IF NOT EXISTS windows (
    condition_id TEXT PRIMARY KEY, slug TEXT, ts_open REAL, ts_close REAL,
    up_shares REAL, down_shares REAL, up_cost REAL, down_cost REAL,
    merged_pairs REAL DEFAULT 0
);
"""


class Ledger:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def record_order(self, order_id, condition_id, side, price, shares) -> None:
        self._x(
            "INSERT OR REPLACE INTO orders(order_id,ts,condition_id,side,price,shares) VALUES (?,?,?,?,?,?)",
            (order_id, time.time(), condition_id, side, price, shares),
        )

    def record_fill(self, condition_id, order_id, side, price, shares) -> None:
        self._x(
            "INSERT INTO fills(ts,condition_id,order_id,side,price,shares) VALUES (?,?,?,?,?,?)",
            (time.time(), condition_id, order_id, side, price, shares),
        )

    def record_merge(self, condition_id, pairs, tx_hash) -> None:
        try:
            self._db.execute(
                "INSERT INTO merges(ts,condition_id,pairs,tx_hash,ok,error) VALUES (?,?,?,?,?,?)",
                (time.time(), condition_id, pairs, tx_hash, 1, None),
            )
            self._db.execute(
                "UPDATE windows SET merged_pairs=COALESCE(merged_pairs,0)+? WHERE condition_id=?",
                (pairs, condition_id),
            )
            self._db.commit()
        except Exception as exc:  # noqa: BLE001
            log.warning("ledger merge write failed: %s", exc)

    def record_merge_failure(self, condition_id, error) -> None:
        self._x(
            "INSERT INTO merges(ts,condition_id,pairs,tx_hash,ok,error) VALUES (?,?,?,?,?,?)",
            (time.time(), condition_id, 0.0, None, 0, str(error)[:500]),
        )

    def record_redeem(self, condition_id, tx_hash, ok, error=None) -> None:
        self._x(
            "INSERT INTO redeems(ts,condition_id,tx_hash,ok,error) VALUES (?,?,?,?,?)",
            (time.time(), condition_id, tx_hash, 1 if ok else 0, None if ok else str(error)[:500]),
        )

    def record_balance(self, pusd: float) -> None:
        self._x("INSERT INTO balances(ts,pusd) VALUES (?,?)", (time.time(), pusd))

    def record_window(
        self, condition_id, slug, ts_open, ts_close,
        up_sh, dn_sh, up_cost, dn_cost, merged_pairs,
    ) -> None:
        self._x(
            "INSERT OR REPLACE INTO windows VALUES (?,?,?,?,?,?,?,?,?)",
            (condition_id, slug, ts_open, ts_close, up_sh, dn_sh, up_cost, dn_cost, merged_pairs),
        )

    def _x(self, sql, params) -> None:
        try:
            self._db.execute(sql, params)
            self._db.commit()
        except Exception as exc:  # noqa: BLE001
            log.warning("ledger write failed: %s", exc)

    def report(self) -> str:
        c = self._db.cursor()
        fills_cost = c.execute("SELECT COALESCE(SUM(price*shares),0) FROM fills").fetchone()[0]
        merged = c.execute("SELECT COALESCE(SUM(pairs),0) FROM merges WHERE ok=1").fetchone()[0]
        mfail = c.execute("SELECT COUNT(*) FROM merges WHERE ok=0").fetchone()[0]
        fill_count = c.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        first_bal = c.execute("SELECT pusd FROM balances ORDER BY ts ASC LIMIT 1").fetchone()
        last_bal = c.execute("SELECT pusd FROM balances ORDER BY ts DESC LIMIT 1").fetchone()
        b0 = first_bal[0] if first_bal else float("nan")
        b1 = last_bal[0] if last_bal else float("nan")
        return (
            "── RECONCILIATION ───────────────────────────────\n"
            f" internal: fills={fill_count} bought=${fills_cost:.2f} merged=${merged:.2f} "
            f"(failed merges: {mfail})\n"
            f" wallet:   pUSD {b0:.2f} → {b1:.2f}  (Δ {b1 - b0:+.2f})\n"
            " note: wallet Δ also reflects open inventory, residual resolution, and fees.\n"
            "─────────────────────────────────────────────────"
        )

    def close(self) -> None:
        self._db.close()
