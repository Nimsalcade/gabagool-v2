from src.shadow_validation import evaluate_capture_quality


def test_full_window_capture_is_usable():
    times = [8.0 + i * 1.5 for i in range(194)]
    q = evaluate_capture_quality(
        snapshot_times=times,
        attempts=200,
        empty_snapshots=6,
        book_errors=0,
        window_start=0.0,
        window_end=300.0,
        snapshot_interval_s=1.0,
        tape_rows=1000,
        tape_sell_rows=100,
    )
    assert q.usable
    assert q.status == "USABLE"


def test_missing_last_third_is_not_usable():
    times = [8.0 + i * 1.5 for i in range(124)]  # ends near 192.5s
    q = evaluate_capture_quality(
        snapshot_times=times,
        attempts=200,
        empty_snapshots=76,
        book_errors=0,
        window_start=0.0,
        window_end=300.0,
        snapshot_interval_s=1.0,
        tape_rows=1139,
        tape_sell_rows=128,
    )
    assert not q.usable
    assert q.status == "PARTIAL_WINDOW_BOOK_DATA"
    assert "missing_terminal_book_coverage" in q.reasons


def test_good_books_but_missing_tape_is_not_usable():
    times = [8.0 + i * 1.5 for i in range(194)]
    q = evaluate_capture_quality(
        snapshot_times=times,
        attempts=200,
        empty_snapshots=6,
        book_errors=0,
        window_start=0.0,
        window_end=300.0,
        snapshot_interval_s=1.0,
        tape_rows=3,
        tape_sell_rows=1,
    )
    assert not q.usable
    assert q.status == "INSUFFICIENT_PUBLIC_TAPE"
