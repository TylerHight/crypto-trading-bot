### User story: Build the curated market-trades pipeline

**As a** data platform developer,  
**I want** Spark to transform immutable raw Kafka records into a typed, deduplicated curated dataset,  
**so that** market trades can be queried directly without manually decoding `kafka_value`.

#### Acceptance criteria

1. Valid `market.trade.raw` version `v1` events are parsed from `kafka_value`.
2. Curated Parquet contains typed columns including:
   - `event_id`
   - `exchange`
   - `symbol`
   - `event_time`
   - `ingested_at`
   - `trade_id`
   - `price`
   - `size`
   - `side`
   - Kafka topic, partition, and offset
3. `price` and `size` use precise decimal types—not floating point.
4. Duplicate events are removed using `event_id` and an event-time watermark.
5. Out-of-order events within the watermark are processed correctly.
6. Malformed or unsupported events are written to a quarantine location without stopping the stream.
7. Curated output is stored under:

```text
crypto-data/curated/market_trades/v1/
```

8. The curated job has a checkpoint separate from the raw sink.
9. Restarting the job resumes from its checkpoint without creating duplicate curated rows.
10. Automated tests cover:
    - A valid BTC trade
    - A valid ETH trade
    - A duplicate event
    - An out-of-order event
    - Malformed JSON
    - An unsupported schema version
11. The existing raw Parquet data remains unchanged.
12. The operational runbook explains how to start and verify the curated pipeline.

#### Definition of done

A developer can start the Podman stack, observe curated Parquet files in MinIO, and query readable rows resembling:

```text
event_time | symbol  | price    | size       | side
-----------|---------|----------|------------|-----
...        | BTC-USD | 63313.19 | 0.00000008 | BUY
```

One-minute candle aggregation should be the following user story after this curated trade dataset is reliable.