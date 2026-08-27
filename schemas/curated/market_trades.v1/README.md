# Curated market trades v1

This schema is the query-facing projection of `market.trade.raw.v1`. Each row
represents one non-conflicting logical exchange trade and retains the selected
raw Kafka position. Exact redeliveries are represented once; conflicting facts
for the same deterministic event ID are excluded and quarantined.

`price`, `size`, and `notional` are Parquet `decimal(38,18)`. Inputs must be
positive and exactly representable at scale 18. Notional is calculated with
decimal arithmetic and rounded to scale 18 using round-half-even. Curated data
is partitioned by the UTC date of `event_time`.

`source_side` preserves Coinbase's source field as normalized `BUY` or `SELL`;
it intentionally makes no buyer/seller or maker/taker interpretation.
