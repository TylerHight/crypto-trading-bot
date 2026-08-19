from dataclasses import dataclass
from typing import Literal

SequenceStatus = Literal[
    "initialized",
    "ok",
    "duplicate",
    "out_of_order",
    "gap",
]


@dataclass(frozen=True)
class SequenceResult:
    """Describe how one integer sequence compares with its previous value."""

    status: SequenceStatus
    previous: int | None
    current: int
    missing_from: int | None = None
    missing_to: int | None = None


class SequenceTracker:
    """Track independent integer sequences by an arbitrary stream name."""

    def __init__(self) -> None:
        # A key identifies the scope of a counter, not necessarily a market
        # symbol. Coinbase Advanced Trade uses one envelope sequence across all
        # channels on a WebSocket connection, while heartbeat_counter is a
        # separate sequence. The Coinbase adapter chooses those keys.
        self._last: dict[str, int] = {}

    def observe(self, key: str, current: int) -> SequenceResult:
        """Compare a newly received sequence number with the previous one."""

        previous = self._last.get(key)

        if previous is None:
            self._last[key] = current
            return SequenceResult(
                status="initialized",
                previous=None,
                current=current,
            )

        if current == previous + 1:
            self._last[key] = current
            return SequenceResult(
                status="ok",
                previous=previous,
                current=current,
            )

        if current == previous:
            return SequenceResult(
                status="duplicate",
                previous=previous,
                current=current,
            )

        if current < previous:
            return SequenceResult(
                status="out_of_order",
                previous=previous,
                current=current,
            )

        self._last[key] = current
        return SequenceResult(
            status="gap",
            previous=previous,
            current=current,
            missing_from=previous + 1,
            missing_to=current - 1,
        )
