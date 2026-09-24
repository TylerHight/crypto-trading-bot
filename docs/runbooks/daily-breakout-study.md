# Collect the prospective daily breakout study

Status: retired September 24, 2026. The task
`CryptoResearch-BtcDailyBreakoutV1` is **disabled** after the
[historical screen](../research/btc-usd-breakout-historical-screen-2026-09-24.md).
The commands below document the original registered workflow for audit; do
not reactivate this study as a profitability test.

The [fixed protocol](../research/btc-usd-daily-breakout-protocol-2026-09-24.md)
defines one future test. Run commands from the repository root using Python
3.11. The Windows wrapper uses `.venv311` and pins the specification digest.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_daily_breakout_capture.ps1 -Action register
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_daily_breakout_capture.ps1 -Action collect
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_daily_breakout_capture.ps1 -Action evaluate
```

Register before September 25, 2026 00:00 UTC. Repeating the same registration
is safe; changing its configuration is rejected. Collection saves only closed,
settled days and catches up missing dates on the next invocation. Run it once
a day after 01:00 UTC. Repeating a completed capture is safe and does not need
another API call. If the API omits a day, preserve the failure and retry later;
do not loosen coverage checks or fill missing data manually.

Registration, capture, and evaluation share an operating-system lock. An
overlapping invocation exits 4 before reading or changing study evidence; retry
after the first run finishes. The OS releases the lock even if a process dies.
Leave `.study.lock` in place: its presence alone does not mean a run is active.

The process-only execution-policy option permits these reviewed local scripts
on Windows without changing the machine's execution policy. To install the
bounded daily capture task under your signed-in account:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install_daily_breakout_capture_task.ps1
Get-ScheduledTaskInfo -TaskName CryptoResearch-BtcDailyBreakoutV1
```

The task uses limited privileges and the interactive logon token; it requires
the machine on, network available, and the user logged in. Missed dates are
fetched on the next run. It expires after March 25, 2027, and runs collection
only; invoke evaluation separately after the test ends. Logs are saved for each
run. A nonzero task result requires inspecting the latest log. To stop future
captures, run `Disable-ScheduledTask -TaskName CryptoResearch-BtcDailyBreakoutV1`.

On September 24, the task was installed at 21:15 Central and its test run
completed with `LastTaskResult=0`. Registration and the first 19 warmup days
were sealed successfully. The full [delivery record](../user_stories/completed/0010-daily-research-gates-and-prospective-capture.md)
records artifact hashes and validation results. The observation period starts
September 25; no strategy performance result exists yet.

The portable command is `python -m crypto_trading_core.daily_breakout ACTION`
with these same arguments, or the installed `run-daily-breakout-study` entrypoint:

```text
--spec experiments/btc-usd-daily-breakout-v1.json
--spec-sha256 41551c83ec1e42a5dcb4be61ad6ffc889afb171a94300245c27daa55d4e18084
--output artifacts/strategy_experiments/daily_breakout_v1
```

The output holds a sealed registration, source responses, capture manifests,
and eventually an evaluation beneath `btc-usd-daily-breakout-v1/`. Windows
invocations also write individual logs under the output's `logs/` directory.
These files are ignored by Git and excluded from container build contexts.
Back them up separately if they must survive disk loss or a fresh checkout.
Checked-in specifications and protocols are retained in Git.

Evaluation reads sealed local evidence, without fetching prices, and remains
pending until March 24, 2027 01:00 UTC and complete coverage. An early evaluation
or a failed research gate exits 2; invalid input or changed evidence exits 4.
No command submits orders or registers a paper session. Capture status is data
availability, not a profitability result.
