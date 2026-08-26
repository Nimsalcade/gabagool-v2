from src.v72_replenishment import replenishment_decision


def test_balanced_inventory_replenishes_both_sides():
    d = replenishment_decision(
        up_shares=100,
        down_shares=100,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    assert d.neutral is True
    assert d.allow_up is True
    assert d.allow_down is True


def test_small_gap_inside_neutral_band_replenishes_both_sides():
    d = replenishment_decision(
        up_shares=105,
        down_shares=100,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    assert d.gap_clips < .5
    assert d.allow_up is True
    assert d.allow_down is True


def test_down_heavy_inventory_only_replenishes_up():
    d = replenishment_decision(
        up_shares=5.02,
        down_shares=56.0,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    assert d.underweight == "UP"
    assert d.neutral is False
    assert d.allow_up is True
    assert d.allow_down is False


def test_up_heavy_inventory_only_replenishes_down():
    d = replenishment_decision(
        up_shares=70,
        down_shares=5,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    assert d.underweight == "DOWN"
    assert d.allow_up is False
    assert d.allow_down is True


def test_sign_flip_reverses_replenishment_permission():
    before = replenishment_decision(
        up_shares=20,
        down_shares=60,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    after = replenishment_decision(
        up_shares=80,
        down_shares=60,
        parent_clip=14,
        neutral_gap_clips=.5,
    )
    assert (before.allow_up, before.allow_down) == (True, False)
    assert (after.allow_up, after.allow_down) == (False, True)


def test_zero_parent_clip_disables_replenishment():
    d = replenishment_decision(
        up_shares=0,
        down_shares=0,
        parent_clip=0,
        neutral_gap_clips=.5,
    )
    assert d.allow_up is False
    assert d.allow_down is False
