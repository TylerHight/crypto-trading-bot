# Strategy experiment v1

These contracts define the pinned experiment specification, train/validation
candidate results, exact-cost buy-and-hold baseline, sealed selection, and
out-of-sample comparison. Preparation may preserve test boundary strings but
must not read test candle rows. Evaluation accepts no parameter overrides and
can open the test range only through a pinned sealed-selection manifest.

Financial Parquet fields use `decimal(38,18)`. Negative returns are retained.
The result is simulation evidence for human review, not a profitability or
live-trading approval claim.
