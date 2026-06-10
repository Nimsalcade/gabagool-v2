.PHONY: install test dry check merge-proof live cancel positions redeem

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -q

dry:
	python -m src.main --dry-run

check:
	python -m tools.check_setup

merge-proof:
	python -m tools.test_merge

live: 
	python -m src.main --live

cancel:
	python -m tools.cancel_all

positions:
	python -m tools.show_positions

redeem:
	python -m tools.redeem_all
