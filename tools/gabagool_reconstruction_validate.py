#!/usr/bin/env python3
"""Validate the locked Gabagool 15m reconstruction against historical fills.

Read-only. Input is the Polymarket-derived BTC/ETH 15m fills CSV produced by
pull_gabagool_first_day_polymarket.py. The script does not place orders.

It re-derives:
  * the 10->9->8->7->6->5 age schedule by dynamic programming;
  * exact-cent / full-parent-clip fingerprints;
  * normalized inventory-gap repair probabilities;
  * a one-feature logistic repair controller in clip units;
  * market-level gap and pair-basis distributions.
"""
from __future__ import annotations
import argparse, json, math
from pathlib import Path

import numpy as np
import pandas as pd

SIZES = [10, 9, 8, 7, 6, 5]


def fit_clip_boundaries(df: pd.DataFrame):
    x = df.copy()
    x["sizei"] = np.rint(x["size"]).astype(int)
    x = x[(np.abs(x["size"] - x["sizei"]) < 1e-6) & x["sizei"].between(5, 10)]
    max_age = int(x["market_age_s"].max())
    counts = np.zeros((max_age + 2, 11), dtype=np.int64)
    for (age, size), n in x.groupby(["market_age_s", "sizei"]).size().items():
        counts[int(age), int(size)] = int(n)
    cum = np.cumsum(counts, axis=0)

    def score(a, b, s):
        if b <= a:
            return -10**12
        return int(cum[b - 1, s] - (cum[a - 1, s] if a > 0 else 0))

    N = max_age + 2
    dp = np.full((7, N), -10**15, dtype=np.int64)
    prev = np.full((7, N), -1, dtype=np.int64)
    dp[0, 0] = 0
    for k, s in enumerate(SIZES, start=1):
        for b in range(1, N):
            best, best_a = -10**15, -1
            for a in range(b):
                if dp[k - 1, a] < -10**14:
                    continue
                v = int(dp[k - 1, a]) + score(a, b, s)
                if v > best:
                    best, best_a = v, a
            dp[k, b], prev[k, b] = best, best_a

    b = N - 1
    bounds = [b]
    for k in range(6, 0, -1):
        b = int(prev[k, b])
        bounds.append(b)
    bounds.reverse()
    return bounds


def nominal(age, bounds):
    for i, s in enumerate(SIZES):
        if bounds[i] <= age < bounds[i + 1]:
            return s
    return 5


def logistic_fit_one_feature(x, y, iters=50):
    # Newton-Raphson for y ~ logistic(a + b*x), avoiding sklearn dependency.
    X = np.column_stack([np.ones(len(x)), np.asarray(x, dtype=float)])
    y = np.asarray(y, dtype=float)
    beta = np.zeros(2)
    for _ in range(iters):
        z = X @ beta
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -40, 40)))
        w = np.maximum(p * (1 - p), 1e-9)
        grad = X.T @ (y - p)
        H = -(X.T * w) @ X
        step = np.linalg.solve(H, grad)
        beta -= step
        if np.max(np.abs(step)) < 1e-10:
            break
    return float(beta[0]), float(beta[1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fills_csv", type=Path)
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    df = pd.read_csv(args.fills_csv)
    bounds = fit_clip_boundaries(df)
    df["nominal_clip"] = df["market_age_s"].map(lambda a: nominal(float(a), bounds))

    # Timestamp-level state removes arbitrary API ordering among fills in the same second.
    recs = []
    for slug, g in df.sort_values(["slug", "timestamp", "row_index"]).groupby("slug"):
        up = down = 0.0
        for ts, h in g.groupby("timestamp", sort=True):
            age = float(h["market_age_s"].iloc[0])
            N = nominal(age, bounds)
            pre_gap = up - down
            up_flow = float(h.loc[h["outcome"] == "Up", "size"].sum())
            dn_flow = float(h.loc[h["outcome"] == "Down", "size"].sum())
            if pre_gap > 1e-9:
                under, over = dn_flow, up_flow
            elif pre_gap < -1e-9:
                under, over = up_flow, dn_flow
            else:
                under = over = math.nan
            if not math.isnan(under):
                recs.append({
                    "gap_clips": abs(pre_gap) / N,
                    "repair": int(under > over),
                    "under": under,
                    "total": up_flow + dn_flow,
                })
            up += up_flow
            down += dn_flow
    r = pd.DataFrame(recs)
    intercept, slope = logistic_fit_one_feature(r["gap_clips"], r["repair"])

    price_cents = df["price"].to_numpy() * 100.0
    exact_cent = float(np.mean(np.abs(price_cents - np.rint(price_cents)) < 1e-6))
    near_cent = float(np.mean(np.abs(price_cents - np.rint(price_cents)) < 1e-3))
    full_clip = float(np.mean(np.abs(df["size"] - df["nominal_clip"]) < 1e-6))

    # Market summaries directly from the fill tape.
    market_rows = []
    for slug, g in df.groupby("slug"):
        last = g.sort_values(["timestamp", "row_index"]).iloc[-1]
        market_rows.append({
            "slug": slug,
            "max_gap": float(g["running_abs_gap"].max()),
            "final_gap": float(abs(last["running_gap_up_minus_down"])),
            "pair_vwap": float(last["running_buy_pair_vwap"]) if pd.notna(last["running_buy_pair_vwap"]) else math.nan,
            "first_age": float(g["market_age_s"].min()),
            "last_age": float(g["market_age_s"].max()),
        })
    m = pd.DataFrame(market_rows)

    out = {
        "fills": int(len(df)),
        "markets": int(df["slug"].nunique()),
        "fitted_clip_boundaries": [int(v) for v in bounds],
        "clip_sizes": SIZES,
        "price_exact_cent_fraction": exact_cent,
        "price_within_0_001_cent_fraction": near_cent,
        "full_parent_clip_fraction": full_clip,
        "repair_logit": {"intercept": intercept, "slope_per_gap_clip": slope},
        "market": {
            "median_max_abs_gap": float(m["max_gap"].median()),
            "median_final_abs_gap": float(m["final_gap"].median()),
            "median_pair_vwap": float(m["pair_vwap"].median()),
            "median_first_fill_age": float(m["first_age"].median()),
            "median_last_fill_age": float(m["last_age"].median()),
        },
    }

    text = json.dumps(out, indent=2, sort_keys=True)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n")
        print(f"WROTE {args.output}")


if __name__ == "__main__":
    main()
