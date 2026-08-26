#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def f(x: Any) -> float:
    if x is None or x == "":
        return 0.0
    return float(x)


def pct(n: float, d: float) -> float | None:
    return None if d <= 0 else 100.0 * n / d


def median(xs: list[float]) -> float | None:
    return None if not xs else statistics.median(xs)


def weighted_vwap(qp: list[tuple[float, float]]) -> float | None:
    q = sum(x[0] for x in qp)
    return None if q <= 0 else sum(x[0] * x[1] for x in qp) / q


def side_sign(side: str) -> int:
    return 1 if side == "UP" else -1


def bin_label(v: float, edges: list[float], labels: list[str]) -> str:
    for i, e in enumerate(edges):
        if v < e:
            return labels[i]
    return labels[-1]


def load_fills(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    rows.sort(key=lambda r: (int(f(r.get("timestamp"))), int(f(r.get("seq")))))
    return rows


def analyze(rows: list[dict[str, Any]], duration_s: int = 900) -> dict[str, Any]:
    if not rows:
        raise RuntimeError("fills.csv is empty")

    sizes = [f(r.get("shares")) for r in rows if f(r.get("shares")) > 0]
    clip = statistics.median(sizes)

    q_up = 0.0
    q_dn = 0.0
    max_abs_gap = 0.0
    sign_flip_rows = 0
    sign_flip_shares = 0.0

    quad_rows = defaultdict(int)
    quad_shares = defaultdict(float)
    gap_reduce_rows = 0
    gap_reduce_shares = 0.0
    expensive_underweight_rows = 0
    expensive_underweight_shares = 0.0
    overweight_expensive_rows = 0
    overweight_expensive_shares = 0.0

    per_second: dict[int, dict[str, Any]] = {}
    timeline: list[dict[str, Any]] = []

    for r in rows:
        ts = int(f(r.get("timestamp")))
        age = int(f(r.get("market_age_s")))
        side = str(r.get("outcome") or "").upper()
        if side not in ("UP", "DOWN"):
            continue
        qty = f(r.get("shares"))
        price = f(r.get("price"))
        gap_before = q_up - q_dn

        if gap_before > 0:
            underweight = "DOWN"
        elif gap_before < 0:
            underweight = "UP"
        else:
            underweight = None

        price_class = "cheap" if price < 0.5 else ("expensive" if price > 0.5 else "mid")
        inventory_class = "underweight" if underweight == side else ("overweight" if underweight else "flat")
        qkey = f"{inventory_class}_{price_class}"
        quad_rows[qkey] += 1
        quad_shares[qkey] += qty

        if underweight == side:
            gap_reduce_rows += 1
            gap_reduce_shares += qty
            if price > 0.5:
                expensive_underweight_rows += 1
                expensive_underweight_shares += qty
        elif underweight is not None and price > 0.5:
            overweight_expensive_rows += 1
            overweight_expensive_shares += qty

        if side == "UP":
            q_up += qty
        else:
            q_dn += qty
        gap_after = q_up - q_dn
        max_abs_gap = max(max_abs_gap, abs(gap_after))
        if gap_before != 0 and gap_after != 0 and (gap_before > 0) != (gap_after > 0):
            sign_flip_rows += 1
            sign_flip_shares += qty

        sec = per_second.setdefault(ts, {
            "timestamp": ts,
            "age": age,
            "gap_before": gap_before,
            "up": [],
            "down": [],
        })
        sec["up" if side == "UP" else "down"].append((qty, price))

        timeline.append({
            "seq": int(f(r.get("seq"))),
            "age": age,
            "side": side,
            "qty": qty,
            "price": price,
            "gap_before": gap_before,
            "gap_after": gap_after,
            "underweight_before": underweight,
            "price_class": price_class,
            "inventory_class": inventory_class,
        })

    sec_rows: list[dict[str, Any]] = []
    for ts in sorted(per_second):
        s = per_second[ts]
        up_vol = sum(q for q, _ in s["up"])
        dn_vol = sum(q for q, _ in s["down"])
        total = up_vol + dn_vol
        up_px = weighted_vwap(s["up"])
        dn_px = weighted_vwap(s["down"])
        rec = {
            "timestamp": ts,
            "age": s["age"],
            "gap_before": s["gap_before"],
            "gap_clips": s["gap_before"] / clip if clip else 0.0,
            "up_vol": up_vol,
            "down_vol": dn_vol,
            "total_vol": total,
            "up_vwap": up_px,
            "down_vwap": dn_px,
            "flow_bias_up": (up_vol - dn_vol) / total if total else 0.0,
        }
        if up_px is not None and dn_px is not None:
            adv = dn_px - up_px  # positive => UP cheaper than DOWN
            rec["price_adv_up"] = adv
            rec["both_sides"] = True
        else:
            rec["price_adv_up"] = None
            rec["both_sides"] = False
        sec_rows.append(rec)

    both = [s for s in sec_rows if s["both_sides"] and abs(s["flow_bias_up"]) > 1e-12]

    def weighted_accuracy(k0: float, k1: float = 0.0) -> tuple[float, float]:
        correct = 0.0
        total = 0.0
        for s in both:
            age_n = max(0.0, min(1.0, s["age"] / duration_s))
            k = k0 + k1 * age_n * age_n
            score = s["price_adv_up"] - k * s["gap_clips"]
            pred = 1 if score > 0 else (-1 if score < 0 else 0)
            obs = 1 if s["flow_bias_up"] > 0 else -1
            w = s["total_vol"]
            if pred == obs:
                correct += w
            total += w
        return (correct / total if total else 0.0, total)

    price_only_acc, model_weight = weighted_accuracy(0.0, 0.0)

    inv_correct = 0.0
    inv_total = 0.0
    for s in both:
        if abs(s["gap_clips"]) < 1e-12:
            continue
        pred = -1 if s["gap_clips"] > 0 else 1
        obs = 1 if s["flow_bias_up"] > 0 else -1
        w = s["total_vol"]
        if pred == obs:
            inv_correct += w
        inv_total += w
    inventory_only_acc = inv_correct / inv_total if inv_total else 0.0

    best_static = {"accuracy": -1.0, "k": None}
    for i in range(0, 201):
        k = i / 1000.0  # 0 to 20 cents of price advantage per inventory clip
        acc, _ = weighted_accuracy(k, 0.0)
        if acc > best_static["accuracy"]:
            best_static = {"accuracy": acc, "k": k}

    best_dynamic = {"accuracy": -1.0, "k0": None, "k1": None}
    for i in range(0, 61):
        k0 = i / 1000.0  # 0..6c/clip early
        for j in range(0, 121, 2):
            k1 = j / 1000.0  # add 0..12c/clip near close
            acc, _ = weighted_accuracy(k0, k1)
            if acc > best_dynamic["accuracy"]:
                best_dynamic = {"accuracy": acc, "k0": k0, "k1": k1}

    gap_edges = [0.5, 1.0, 2.0, 4.0, 8.0]
    gap_labels = ["<0.5", "0.5-1", "1-2", "2-4", "4-8", ">=8"]
    adv_edges = [0.01, 0.03, 0.05, 0.10, 0.20]
    adv_labels = ["<1c", "1-3c", "3-5c", "5-10c", "10-20c", ">=20c"]
    conflict_cells: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    aligned_vol = 0.0
    conflict_vol = 0.0
    conflict_underweight_vol = 0.0
    conflict_cheap_vol = 0.0

    for s in both:
        gap = s["gap_before"]
        adv = s["price_adv_up"]
        if abs(gap) < 1e-12 or abs(adv) < 1e-12:
            continue
        under = "DOWN" if gap > 0 else "UP"
        cheap = "UP" if adv > 0 else "DOWN"
        up_vol, dn_vol = s["up_vol"], s["down_vol"]
        total = s["total_vol"]
        if under == cheap:
            aligned_vol += total
            continue
        conflict_vol += total
        uvol = up_vol if under == "UP" else dn_vol
        cvol = up_vol if cheap == "UP" else dn_vol
        conflict_underweight_vol += uvol
        conflict_cheap_vol += cvol
        gb = bin_label(abs(s["gap_clips"]), gap_edges, gap_labels)
        ab = bin_label(abs(adv), adv_edges, adv_labels)
        key = f"gap_{gb}|adv_{ab}"
        conflict_cells[key]["total"] += total
        conflict_cells[key]["underweight"] += uvol
        conflict_cells[key]["cheap"] += cvol
        conflict_cells[key]["seconds"] += 1

    conflict_matrix = {}
    for key, d in sorted(conflict_cells.items()):
        conflict_matrix[key] = {
            "seconds": int(d["seconds"]),
            "volume": d["total"],
            "pct_volume_to_underweight": pct(d["underweight"], d["total"]),
            "pct_volume_to_cheaper_side": pct(d["cheap"], d["total"]),
        }

    phase_defs = [(0, 300), (300, 600), (600, 750), (750, 900)]
    phases = {}
    for a, b in phase_defs:
        xs = [x for x in timeline if a <= x["age"] < b]
        shares = sum(x["qty"] for x in xs)
        reducing = sum(x["qty"] for x in xs if x["inventory_class"] == "underweight")
        expensive_repair = sum(
            x["qty"] for x in xs
            if x["inventory_class"] == "underweight" and x["price"] > 0.5
        )
        phases[f"{a}-{b}s"] = {
            "fill_rows": len(xs),
            "shares": shares,
            "pct_shares_gap_reducing": pct(reducing, shares),
            "pct_shares_expensive_underweight": pct(expensive_repair, shares),
        }

    checkpoints = {}
    for t in (120, 300, 480, 660, 780, 894, 900):
        xs = [x for x in timeline if x["age"] <= t]
        if not xs:
            continue
        last = xs[-1]
        u = sum(x["qty"] for x in xs if x["side"] == "UP")
        d = sum(x["qty"] for x in xs if x["side"] == "DOWN")
        mx = max(u, d)
        checkpoints[str(t)] = {
            "up": u,
            "down": d,
            "gap": u - d,
            "balance_min_over_max_pct": (100.0 * min(u, d) / mx) if mx else None,
            "last_age": last["age"],
        }

    total_shares = q_up + q_dn
    result = {
        "sample": {
            "fill_rows": len(timeline),
            "total_shares": total_shares,
            "median_fill_clip": clip,
            "up_shares": q_up,
            "down_shares": q_dn,
            "final_gap": q_up - q_dn,
            "final_abs_gap_pct_of_larger_side": pct(abs(q_up - q_dn), max(q_up, q_dn)),
            "max_abs_gap": max_abs_gap,
            "max_abs_gap_clips": max_abs_gap / clip if clip else None,
        },
        "fill_level_behavior": {
            "pct_rows_gap_reducing": pct(gap_reduce_rows, len(timeline)),
            "pct_shares_gap_reducing": pct(gap_reduce_shares, total_shares),
            "pct_shares_expensive_and_underweight": pct(expensive_underweight_shares, total_shares),
            "pct_shares_expensive_and_overweight": pct(overweight_expensive_shares, total_shares),
            "sign_flip_fill_rows": sign_flip_rows,
            "sign_flip_fill_shares": sign_flip_shares,
            "quadrants_rows": dict(sorted(quad_rows.items())),
            "quadrants_shares": dict(sorted(quad_shares.items())),
        },
        "same_second_model": {
            "seconds_total": len(sec_rows),
            "seconds_both_sides": len(both),
            "modeled_volume": model_weight,
            "price_only_weighted_direction_accuracy_pct": 100.0 * price_only_acc,
            "inventory_only_weighted_direction_accuracy_pct": 100.0 * inventory_only_acc,
            "best_static_rule": {
                "formula": "choose UP when (down_vwap-up_vwap) - k*(gap_before/median_clip) > 0; else DOWN",
                "k_dollars_per_inventory_clip": best_static["k"],
                "k_cents_per_inventory_clip": 100.0 * best_static["k"] if best_static["k"] is not None else None,
                "weighted_direction_accuracy_pct": 100.0 * best_static["accuracy"],
            },
            "best_late_pressure_rule": {
                "formula": "k(age)=k0+k1*(age/900)^2",
                "k0_cents_per_clip": 100.0 * best_dynamic["k0"] if best_dynamic["k0"] is not None else None,
                "k1_extra_cents_per_clip_at_close": 100.0 * best_dynamic["k1"] if best_dynamic["k1"] is not None else None,
                "weighted_direction_accuracy_pct": 100.0 * best_dynamic["accuracy"],
            },
        },
        "price_vs_inventory_conflict": {
            "aligned_volume": aligned_vol,
            "conflict_volume": conflict_vol,
            "pct_conflict_volume_to_underweight_side": pct(conflict_underweight_vol, conflict_vol),
            "pct_conflict_volume_to_cheaper_side": pct(conflict_cheap_vol, conflict_vol),
            "matrix": conflict_matrix,
        },
        "phases": phases,
        "balance_checkpoints": checkpoints,
        "interpretation_notes": [
            "Data API timestamps are integer seconds, so same-second ordering is not exact chronology.",
            "Filled prices are executions, not a complete historical order book. Same-second UP/DOWN VWAPs are used as an observed price proxy.",
            "The fitted k is a behavioral decision-boundary estimate, not recovered private source code.",
            "Positive price_adv_up means observed DOWN fills were more expensive than observed UP fills in that second, i.e. UP was the cheaper filled side.",
        ],
    }
    return result


def print_report(r: dict[str, Any]) -> None:
    s = r["sample"]
    b = r["fill_level_behavior"]
    m = r["same_second_model"]
    c = r["price_vs_inventory_conflict"]

    print("GABAGOOL PRICE × INVENTORY RULE CRACK")
    print("=" * 78)
    print(f"FILLS       {s['fill_rows']:,} rows | {s['total_shares']:.3f} shares | median clip {s['median_fill_clip']:.3f}")
    print(f"FINAL GAP   {s['final_gap']:+.3f} sh | {s['final_abs_gap_pct_of_larger_side']:.4f}% of larger side")
    print(f"MAX GAP     {s['max_abs_gap']:.3f} sh | {s['max_abs_gap_clips']:.2f} median clips")
    print()
    print(f"GAP REDUCE  {b['pct_shares_gap_reducing']:.2f}% of filled shares went to the underweight side")
    print(f"EXP REPAIR  {b['pct_shares_expensive_and_underweight']:.2f}% of all shares were >50c AND underweight-side buys")
    print(f"EXP OVERWT  {b['pct_shares_expensive_and_overweight']:.2f}% of all shares were >50c AND overweight-side buys")
    print(f"OVERSHOOT   {b['sign_flip_fill_rows']:,} fills crossed inventory through zero")
    print()
    print(f"BOTH-SIDE SECONDS {m['seconds_both_sides']:,}")
    print(f"PRICE ONLY         {m['price_only_weighted_direction_accuracy_pct']:.2f}% weighted direction accuracy")
    print(f"INVENTORY ONLY     {m['inventory_only_weighted_direction_accuracy_pct']:.2f}% weighted direction accuracy")
    bs = m['best_static_rule']
    print(f"PRICE+INVENTORY    {bs['weighted_direction_accuracy_pct']:.2f}% | inventory penalty ≈ {bs['k_cents_per_inventory_clip']:.2f}c per median-clip gap")
    bd = m['best_late_pressure_rule']
    print(f"LATE-PRESSURE FIT  {bd['weighted_direction_accuracy_pct']:.2f}% | k0={bd['k0_cents_per_clip']:.2f}c/clip + {bd['k1_extra_cents_per_clip_at_close']:.2f}c/clip near close")
    print()
    print(f"CONFLICT VOLUME    {c['conflict_volume']:.3f} shares where cheaper side and underweight side disagreed")
    uw = c['pct_conflict_volume_to_underweight_side']
    ch = c['pct_conflict_volume_to_cheaper_side']
    print(f"WHEN CONFLICTED    {uw:.2f}% volume → underweight side | {ch:.2f}% → cheaper side")
    print()
    print("PHASES")
    for name, x in r['phases'].items():
        print(f"  {name:9s} shares={x['shares']:9.3f} | gap-reducing={x['pct_shares_gap_reducing']:6.2f}% | expensive-repair={x['pct_shares_expensive_underweight']:6.2f}%")
    print()
    print("BALANCE")
    for t, x in r['balance_checkpoints'].items():
        print(f"  t<={int(t):3d}s U={x['up']:9.3f} D={x['down']:9.3f} gap={x['gap']:+8.3f} balance={x['balance_min_over_max_pct']:6.2f}%")
    print()
    print("CONFLICT MATRIX: % of volume routed to UNDERWEIGHT side")
    for key, x in r['price_vs_inventory_conflict']['matrix'].items():
        print(f"  {key:28s} n={x['seconds']:3d} vol={x['volume']:8.2f} -> {x['pct_volume_to_underweight']:6.2f}%")


def main() -> int:
    ap = argparse.ArgumentParser(description="Infer Gabagool's portfolio-level price-vs-inventory routing rule from one-market fills.csv")
    ap.add_argument("--fills", default="data/gabagool_jan17_1115_forensic/fills.csv")
    ap.add_argument("--out", default="data/gabagool_jan17_1115_forensic/inventory_price_rule.json")
    ap.add_argument("--duration", type=int, default=900)
    args = ap.parse_args()

    rows = load_fills(Path(args.fills))
    result = analyze(rows, duration_s=args.duration)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print_report(result)
    print()
    print(f"JSON        {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
