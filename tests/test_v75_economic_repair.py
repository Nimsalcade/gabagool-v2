import math

from src.v75_economic_repair import effective_repair_pair_cap, economic_repair_price


def test_repair_pair_cap_stays_tight_until_late_window():
    assert effective_repair_pair_cap(age_s=0) == 1.005
    assert effective_repair_pair_cap(age_s=600) == 1.005
    assert math.isclose(effective_repair_pair_cap(age_s=750), 1.00625, abs_tol=1e-12)
    assert effective_repair_pair_cap(age_s=900) == 1.010


def test_v74_bad_btc_repair_is_blocked_by_projected_pair_economics():
    # V7.4 paper snapshot: UP ~= 20.3 @ 0.5583, DOWN deficient.  It chased DOWN
    # at 0.59-0.67 and eventually locked a ~1.21 pair basis.  V7.5 should cap a
    # 14-share repair parent around 0.4467 instead.
    up_shares = 20.3
    up_cost = up_shares * 0.5583
    e = economic_repair_price(
        side="DOWN",
        add_qty=14,
        up_shares=up_shares,
        up_cost=up_cost,
        down_shares=0,
        down_cost=0,
        age_s=120,
    )
    assert e.pair_cap == 1.005
    assert 0.4466 < e.max_buy_price < 0.4468
    assert e.max_buy_price < 0.59


def test_profitable_099_pair_repair_remains_available():
    # If the overweight side costs 0.41, a deficient side near 0.58 is still
    # economically repairable under the 1.005 gross pair ceiling.
    e = economic_repair_price(
        side="UP",
        add_qty=14,
        up_shares=0,
        up_cost=0,
        down_shares=14,
        down_cost=14 * 0.41,
        age_s=120,
    )
    assert math.isclose(e.max_buy_price, 0.595, abs_tol=1e-12)
    assert e.max_buy_price > 0.58


def test_existing_expensive_deficient_inventory_requires_cheaper_repair():
    # Once the deficient side itself has been bought too expensively, the next
    # parent must be cheaper enough to pull its aggregate VWAP back under cap.
    e = economic_repair_price(
        side="DOWN",
        add_qty=14,
        up_shares=20.3,
        up_cost=20.3 * 0.5583,
        down_shares=3.0,
        down_cost=3.0 * 0.59,
        age_s=120,
    )
    assert e.max_buy_price < 0.42


def test_absolute_repair_ceiling_still_applies():
    e = economic_repair_price(
        side="DOWN",
        add_qty=14,
        up_shares=14,
        up_cost=14 * 0.05,
        down_shares=0,
        down_cost=0,
        age_s=900,
        absolute_max=0.92,
    )
    assert e.max_buy_price == 0.92
