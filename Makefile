PYTHON ?= python3

.PHONY: check-python install test dry shadow check merge-proof live cancel positions redeem harvest

check-python:
	@$(PYTHON) -c 'import sys; req=(3,11); cur=sys.version_info[:2]; assert cur >= req, f"Python >=3.11 required, found {sys.version.split()[0]} at {sys.executable}"'

install: check-python
	$(PYTHON) -m pip install -r requirements.txt

test: check-python
	$(PYTHON) -m pytest tests/ -q

dry: check-python
	$(PYTHON) -m src.main --dry-run

shadow: check-python
	$(PYTHON) -m tools.shadow_market --asset btc --duration 300

check: check-python
	$(PYTHON) -m tools.check_setup

merge-proof: check-python
	$(PYTHON) -m tools.test_merge

live: check-python
	$(PYTHON) -m src.main --live

cancel: check-python
	$(PYTHON) -m tools.cancel_all

positions: check-python
	$(PYTHON) -m tools.show_positions

redeem: check-python
	$(PYTHON) -m tools.redeem_all

harvest: check-python
	$(PYTHON) -m tools.harvest --merge
