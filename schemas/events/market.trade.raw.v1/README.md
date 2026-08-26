# Market trade raw v1

The contract accepts the explicit producer provenances `apps.collector` and
`apps.historical_backfill`. Both producers use the same canonical envelope and
deterministic event ID. Producer provenance is operational metadata and is not
part of a trade's logical identity; identity is derived from exchange, symbol,
and source event ID.
