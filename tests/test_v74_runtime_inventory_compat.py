import tools.run_forensic_15m_paper as base
from tools.run_forensic_15m_v73_january_portfolio_paper import _inventory_signed_gap


def test_v74_inherited_repair_uses_common_inventory_shape():
    inv = base.Inventory(up_shares=10.25, down_shares=3.75)
    assert _inventory_signed_gap(inv) == 6.5


def test_v74_inherited_repair_handles_down_heavy_inventory():
    inv = base.Inventory(up_shares=1.0, down_shares=15.0)
    assert _inventory_signed_gap(inv) == -14.0
