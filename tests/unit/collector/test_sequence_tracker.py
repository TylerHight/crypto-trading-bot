from crypto_exchange_adapters.sequence_tracker import SequenceTracker


def test_first_observation_initializes_the_stream() -> None:
    tracker = SequenceTracker()

    result = tracker.observe("coinbase:websocket", 100)

    assert result.status == "initialized"
    assert result.previous is None
    assert result.current == 100


def test_consecutive_observation_advances_the_stream() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 101)

    assert result.status == "ok"
    assert result.previous == 100
    assert result.current == 101


def test_forward_jump_reports_the_exact_missing_range() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 104)

    assert result.status == "gap"
    assert result.missing_from == 101
    assert result.missing_to == 103


def test_gap_updates_the_baseline() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 104)

    result = tracker.observe("coinbase:websocket", 105)

    assert result.status == "ok"


def test_duplicate_does_not_change_the_baseline() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 100)

    result = tracker.observe("coinbase:websocket", 101)

    assert result.status == "ok"


def test_out_of_order_message_does_not_move_the_baseline_backward() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:websocket", 101)
    tracker.observe("coinbase:websocket", 99)

    result = tracker.observe("coinbase:websocket", 102)

    assert result.status == "ok"


def test_independent_counter_streams_keep_independent_state() -> None:
    tracker = SequenceTracker()
    tracker.observe("coinbase:websocket", 100)
    tracker.observe("coinbase:heartbeat-counter", 500)

    envelope = tracker.observe("coinbase:websocket", 101)
    heartbeat = tracker.observe("coinbase:heartbeat-counter", 501)

    assert envelope.status == "ok"
    assert heartbeat.status == "ok"
