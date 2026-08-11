# Replay Tests

Replay tests prove that live and bounded processing share the same data semantics.

Recorded raw fixtures are processed through streaming and batch entry points, then compared by primary key, row count, quarantine count, and deterministic OHLCV values or checksums. The suite also covers duplicates, late and out-of-order events, checkpoint restart, and idempotent reruns.

Fixtures must be small enough for CI, legally redistributable, timestamp-stable, and free of credentials or private account data.
