# One-minute market candles v1

Each row is one non-empty, half-open UTC minute for an exchange and symbol. The
source is pinned by `source_curated_snapshot_key`; later historical backfills
produce a new immutable candle snapshot rather than mutating this row.

Open and close use the total order `(event_time, kafka_topic, kafka_partition,
kafka_offset, event_id)`. High and low are extrema. Base volume is `sum(size)`,
quote volume is `sum(notional)`, and VWAP is quote volume divided by base volume.
All numeric outputs are `decimal(38,18)`; VWAP is rounded half-even. Empty minutes
are omitted.

`historical_backfill_trade_count` is bounded lineage metadata. It proves whether
a historical-backfill delivery contributed without embedding unbounded event
lists or source payloads.
