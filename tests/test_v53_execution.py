from src.forensic_15m import Inventory
from src.shadow import ShadowOrder
from src.v53_execution import (
    aggressive_candidate,
    apply_sell_print_to_multi_orders,
    parent_clip_for,
    plan_multi_parent_side,
    stack_targets,
)


def test_parent_profiles():
    assert parent_clip_for(100, asset="BTC", regime="oct") == 10
    assert parent_clip_for(850, asset="ETH", regime="oct") == 5
    assert parent_clip_for(500, asset="BTC", regime="nov") == 9
    assert parent_clip_for(100, asset="BTC", regime="dec") == 20
    assert parent_clip_for(100, asset="ETH", regime="dec") == 14
    assert parent_clip_for(850, asset="ETH", regime="dec") == 8


def test_underweight_gets_more_stack_capacity_than_heavy():
    inv = Inventory(up_shares=10, down_shares=40)
    up = stack_targets(inv, side="UP", parent_clip=10, logical_layers=4, regime="nov")
    dn = stack_targets(inv, side="DOWN", parent_clip=10, logical_layers=4, regime="nov")
    assert up[0] == 4
    assert dn[0] == 3
    assert sum(up) > sum(dn)


def test_planner_keeps_duplicate_parents_and_replenishes_missing_copy():
    orders = [("A", .50, 1.0), ("B", .50, 2.0), ("C", .49, 3.0)]
    plan = plan_multi_parent_side(
        orders=orders,
        current_base=.50,
        desired_layers=2,
        stack_slots=(3, 1),
        tick=.01,
    )
    assert set(plan.keep_oids) == {"A", "B", "C"}
    assert plan.replenish_prices == (.50,)


def test_planner_drops_newest_excess_same_cent_copy():
    orders = [("A", .50, 1.0), ("B", .50, 2.0), ("C", .50, 3.0)]
    plan = plan_multi_parent_side(
        orders=orders,
        current_base=.50,
        desired_layers=1,
        stack_slots=(2,),
        tick=.01,
    )
    assert set(plan.keep_oids) == {"A", "B"}
    assert plan.drop_oids == ("C",)


def test_multi_allocator_consumes_two_same_cent_parents_once():
    a = ShadowOrder("UP", "t", .34, 7, 2, 1.0)
    b = ShadowOrder("UP", "t", .34, 7, 0, 1.0)
    fills = apply_sell_print_to_multi_orders([a, b], trade_price=.34, trade_size=16)
    assert [round(q, 6) for _, q in fills] == [7, 7]
    assert a.done and b.done


def test_exact_price_does_not_leak_to_lower_level():
    a = ShadowOrder("UP", "t", .34, 7, 0, 1.0)
    b = ShadowOrder("UP", "t", .34, 7, 0, 2.0)
    c = ShadowOrder("UP", "t", .33, 7, 0, 3.0)
    fills = apply_sell_print_to_multi_orders([a, b, c], trade_price=.34, trade_size=30)
    assert [o.price for o, _ in fills] == [.34, .34]
    assert c.filled == 0


def test_aggressive_parent_is_not_deficit_clipped_and_repair_can_qualify():
    c = aggressive_candidate(
        side="UP",
        shares=10,
        own_asks=[(.40, 20)],
        opposite_asks=[(.58, 20)],
        opposite_unmatched_qty=3,
        opposite_unmatched_vwap=.59,
        repair_basis_cap=1.05,
        fresh_pair_cap=1.00,
    )
    assert c is not None
    assert c.shares == 10
    assert c.closes_unmatched == 3
    assert c.execution_vwap == .40
    assert abs(c.repair_basis - .99) < 1e-12


def test_aggressive_rejects_bad_repair_and_bad_fresh_pair():
    c = aggressive_candidate(
        side="UP",
        shares=10,
        own_asks=[(.60, 20)],
        opposite_asks=[(.55, 20)],
        opposite_unmatched_qty=5,
        opposite_unmatched_vwap=.60,
        repair_basis_cap=1.05,
        fresh_pair_cap=1.00,
    )
    assert c is None
