# Brave request CAPTCHA statistics

PostgreSQL migration 130 adds `captcha_stats`, `brave_result_id`,
`captcha_stats_updated_at`, and `captcha_stats_complete` to
`processing.llm_external_requests`.

`brave_request_stats_sensor` reads up to 40 browser request statuses every 30
seconds, with four bounded HTTP readers. It saves cumulative reported input/output
tokens, CAPTCHA presentation and detection time, confirmed clearance time, assistant
attempts/models, and the configured proxy route name. No proxy credentials, LLM
keys, screenshots or prompts are copied into PostgreSQL or logs.

The sensor logs first detection, confirmed clearance, and terminal statistics both
in its evaluation log and in the owning Dagster run's event log. Repeated polling
overwrites the same request's measurements rather than adding token usage again.
Finished requests leave the polling index. Existing request records are collected
as a bounded backfill. Request failures and interrupted requests are included.
This sensor only reads the browser API; it does not cancel, retry, or disable work.
There are no health thresholds or alerts.

Backoffice adds CAPTCHA and proxy columns to the shared Brave results view and
shows each browser request's statistics when a result is selected. Multiple owners
of a resumed request are deduplicated by browser request ID; separate canceled or
retried requests remain visible.

The browser records `captcha.detected_at` before trying the assistant, even when
its budget is zero. `captcha.confirmed_at` is set only after checking that the page
no longer presents a challenge. An assistant's `appears_clear` response is not
confirmation. The live status exposes current runs and usage, and the terminal
result preserves them. Historical results can establish clearance from a successful
search, but have no exact confirmation time; this remains NULL in PostgreSQL and
"Not recorded" in the UI. Historical detection times from agent start are labeled.
Token counts are provider-reported; unreported usage is unknown, not an estimated
billable total. `direct` explicitly means no proxy.

Deploy PostgreSQL migration 130 before Backoffice/Dagster code. ClickHouse migration
457 removes the superseded health views; migration 456 is retained because it was
already applied. Dagster code-location reload enables collection for existing runs
without restarting them. Browser instrumentation takes effect after the browser
service's next deployment/restart; do not interrupt active browser requests just
to enable it.
