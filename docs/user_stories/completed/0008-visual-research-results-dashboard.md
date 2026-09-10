# Visual research results dashboard

## Status

- Status: complete
- Priority: high
- Owner: strategy owner
- Dependencies: completed stories 0001 through 0006

## User story

As a strategy owner, I want clear charts for a sealed research run so that I
can understand candidate performance, losses, fees, and simulated trades
without decoding a long JSON report.

## Scope

- Show a candidate comparison chart for validation return and maximum drawdown
  against the fixed 20% drawdown limit.
- Let the operator choose a candidate and train or validation period.
- Show separate account-value lines for each contiguous source segment and
  buy/sell trade markers, plus fills, fees, return, and drawdown in plain
  language.
- Publish bounded chart evidence with the immutable selection manifest and
  hash-verify it before dashboard rendering.
- Keep the page read-only and state clearly that a reset segment is not one
  continuous, tradable account balance.

## Non-goals

- New strategy selection, test access, paper trading, order placement, or a
  chart that connects account value across source gaps.
- Hiding the failed result or presenting an attractive graph as a profit claim.

## Acceptance criteria

1. The dashboard renders return, drawdown, account-value, and trade-marker
   charts from SHA-verified sealed evidence only.
2. Each continuous source segment is drawn separately and explains its reset.
3. The chart evidence is bounded, retains exact aggregate counts, and has no
   access to test data before selection.
4. A missing, oversized, malformed, or hash-mismatched chart document fails
   closed rather than rendering unverified values.
5. Unit tests cover visual evidence production, tampering, and read-only
   dashboard rendering.

## Validation plan

- Run the approved BTC-USD research input again under a new immutable evidence
  version and verify dashboard read-back.
- Run focused trading-core and dashboard tests, lint, and type checks.

## Rollout constraints

Charts improve understanding; they do not change the failed selection result or
authorize a paper trial. The following story is the separate decision to retire
or replace the rejected hypothesis.

## Delivery and validation — September 9, 2026

Implemented a native-SVG Study charts card in the local read-only dashboard.
It has a strategy and period selector, a separate-segment account-value chart,
buy/sell markers, and a candidate comparison chart for return and drawdown
against the 20% limit. The screen also gives exact fills, fees, return, and
drawdown totals in plain language.

Chart evidence is bounded to 240 account-value points and 400 trade markers per
segment, while retaining exact counts. It is stored in the hash-pinned sealed
selection publication and the dashboard rejects an oversized, malformed, or
hash-mismatched selection document. It labels the reset segments explicitly;
the graphs never claim a continuous tradable balance.

The approved BTC-USD study was republished as visual evidence at
`s3a://crypto-data/analytics/strategy_experiments/v1/gap_aware_research/reports/0095f0a4d24695009a23afc501a583262b141f5dc53aa7791750527dbfb31fa2/manifest.json`
with SHA-256 `2db8c6042c487699eb86e99643d68a4ca6006a03fdb33bbbd615a0d7f5bb7228`.
The local dashboard at `http://127.0.0.1:8090` was restarted and verified to
serve its hash-verified visual evidence. The result remains no selected
candidate and no test-price access.

Validation passed: 6 focused trading-core tests, 11 focused dashboard tests,
Ruff, and mypy for both applications.

## Usability follow-up — September 9, 2026

Resolved operator feedback that trades were hard to see and dropdowns closed
after 30 seconds.

- Replaced the timed full-page reload with an explicit Refresh snapshot button.
  Same-tab session storage preserves navigation, strategy/period/segment,
  trade filters, and open chart details through manual refresh.
- Split the interface into Research (default), Trades, System, Evidence, and
  Paper trial tabs. Removed the empty final-test card when no result exists.
- Added a full-width trade-price chart with larger buy/sell triangles,
  accessible time/price/fee tooltips, side and search filters, and a paged,
  scrollable trade table. Account history has dated axes and value tooltips.
- Added a segment selector and separated period-total metrics from chart
  filtering. Comparison returns now put losses left of zero and gains right.
- Kept the published-sample limitation visible: up to 400 trade markers and
  240 account points per segment. This UI change does not recover omitted
  trades, rerun a study, publish new evidence, or modify database contents.

Validation: 28 dashboard unit tests and 4 optional Chrome browser tests passed.
Browser checks cover all saved candidates and train/validation periods, marker
counts, trade prices, filters, pagination, a real 32-second no-reload wait,
manual-refresh state, keyboard tabs, disabled storage, missing evidence, and
390-pixel layouts. Desktop and mobile screenshots were visually inspected.
Ruff, JavaScript syntax checking, and the dashboard web-module type check passed.
The running loopback server was restarted and its HTML and bundled script
verified at port 8090. See the dashboard README for repeatable checks.
