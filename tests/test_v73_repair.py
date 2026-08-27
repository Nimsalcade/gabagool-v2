from src.v73_repair import in_bootstrap, repair_maker_top, repair_parent_count


def test_bootstrap_until_both_sides_have_half_clip():
    assert in_bootstrap(up_shares=0, down_shares=0, parent_clip=14, two_sided_threshold_clips=0.5)
    assert in_bootstrap(up_shares=14, down_shares=6.99, parent_clip=14, two_sided_threshold_clips=0.5)
    assert not in_bootstrap(up_shares=14, down_shares=7.0, parent_clip=14, two_sided_threshold_clips=0.5)


def test_repair_parent_count_caps_requested_exposure_to_gap_plus_one_parent():
    assert repair_parent_count(gap_shares=14, parent_clip=14, requested_parents=6, overshoot_parents=1) == 2
    assert repair_parent_count(gap_shares=56, parent_clip=14, requested_parents=12, overshoot_parents=1) == 5
    assert repair_parent_count(gap_shares=56, parent_clip=14, requested_parents=3, overshoot_parents=1) == 3


def test_repair_maker_top_quotes_one_tick_below_ask():
    assert repair_maker_top(best_ask=0.58, tick=0.01, repair_max_price=0.92) == 0.57


def test_repair_maker_top_respects_absolute_repair_cap():
    assert repair_maker_top(best_ask=0.97, tick=0.01, repair_max_price=0.92) == 0.92


def test_repair_maker_top_never_crosses_ask():
    assert repair_maker_top(best_ask=0.01, tick=0.01, repair_max_price=0.92) is None
