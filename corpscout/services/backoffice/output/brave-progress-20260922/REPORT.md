# Brave progress logging — 2026-09-22

`company_brave_search_results` now emits `Brave progress` summaries at startup,
completion, and after either threshold is reached while processing advances:

- `progress_log_every`: 100 accounted entries by default.
- `progress_log_interval_seconds`: 30 seconds by default.

Each summary contains total selection size, processed results, remaining inputs,
successes, failures, skips, completion percentage, and new results in this run.
Processed totals include stored outcomes from previous runs of the same execution.
Skips are discovered while scanning the frozen selection. Therefore remaining
includes in-flight and unexamined inputs, including cache hits not yet discovered.
The percentage includes both processed and skipped entries.

Live result counters advance only after ClickHouse acknowledges the insert;
startup and final outcome counts come from ClickHouse. A lock protects the counters
and log cadence across the four route workers. Per-company logs are retained.
There are no new database tables or migrations.

Validation: 25 tests passed across the real HTTP/database Brave suites, including
row/time cadence, mixed success/error/skip totals, resume without duplicate requests,
and the existing lost-acknowledgement recovery test. Ruff passed. Ansible validates
local and staged Dagster definitions and reloads only the code location.

Operational receipts and final run checks are saved alongside this report.

## Deployment and recovery

Ansible light sync completed successfully (`ok=33 changed=11 failed=0`). Local
and staged server definitions passed validation. Only the Dagster code location
was reloaded; the supervisor retained PID 2232153 with zero restarts. The deployed
asset matches the local source (SHA256
`7bfb3070cb4bb861a86445fa8371e98111ca19716533e5ffc5099df2fd7fd458`).

The old run `19ac7955-07a7-46d9-beef-b92d45d06580` stopped gracefully and browser
leases drained. Its checkpoint contained 22,485 distinct results: 22,169 successes
and 316 failures. The fixed selection contains 736,554 inputs.

Run `ea691d13-1f11-4f33-b3ec-bbc0548b4783` resumes the same logical execution
`ce521036-669b-4b96-8d1d-a71147c3bfa4`, preserving the selection and search settings.
Its progress thresholds are explicitly set to 100 entries and 30 seconds.

Live verification confirmed the resumed run is STARTED with 22,488 distinct
results (22,172 successful, 316 failed), including three new successful results.
The result count equals the saved checkpoint plus the new run’s writes; no
duplicate inputs were found within this execution.

The live log confirmed startup with all 22,485 saved outcomes, row-based progress
as cached inputs were skipped, and time-based progress after a new result:

```text
execution=ce521036-669b-4b96-8d1d-a71147c3bfa4 phase=running total=736554 processed=22486 remaining=707598 succeeded=22170 failed=316 skipped=6470 progress=3.93% new_results=1
```

See `live-progress.log`, `verification.json`, and `resume-request.json` for receipts.
