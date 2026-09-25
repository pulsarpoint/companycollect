# Saved models and execution control

Apply PostgreSQL migrations 000127 and 000128 before deploying Backoffice or Dagster. Set `LLM_CONTROL_PG_URL` on both to the application database (the same one used by `PROCESSING_PG_URL`), using the `processing_worker` role. This is separate from Dagster's internal database. Keep the existing shared `CRAWLER_LLM_ENCRYPTION_KEY` unchanged on Backoffice, Dagster, crawler, and browser service.

Run `pnpm exec tsx scripts/import-llm-catalog.ts` once in Backoffice. It copies and authenticates existing encrypted SQLite credentials, preserves profile identities/default selection, and records an idempotent import receipt. It leaves the source intact as an encrypted migration backup. SQLite still owns local Codex preferences and prompt settings; PostgreSQL is authoritative for remote model profiles.

Deploy crawler and browser verification API changes before using the updated Backoffice. Deploy Dagster using its normal hot-sync procedure. `llm_lifecycle_monitor` starts enabled and reconciles every 15 seconds when work is available. No Dagster supervisor restart is needed.

## Behavior

- Saving creates an immutable configuration revision. Enabled/disabled/archived state is separate from the default selection.
- A test with an explicit provider HTTP 401, 403, or 404 invalidates that revision, disables it if it is still current, and requests cancellation of its unfinished runs. Timeouts, rate limits, service connectivity, and capability failures remain visible without globally disabling the model.
- Test and enable is explicit. Stale tests cannot overwrite a newer test or re-enable a profile disabled/removed while that test was in flight. An obsolete revision's failure can stop its own runs without disabling its replacement.
- Remove archives the profile, excludes it from selectors, and stops its active work. Completed results and revision/run history remain. Archiving does not revoke a provider API key.
- Every Backoffice launch containing encrypted model configuration records exact dependencies before GraphQL submission. The `llm/request_id` tag recovers runs after lost acknowledgements. Definitive validation errors are recorded as launch failures; ambiguous launches stay visible for investigation rather than being submitted again automatically.
- Direct model clients check admission before each HTTP attempt. Crawler/Brave dispatch records exact external request IDs before POST. The monitor cancels only owned requests and confirms terminal service state; a 202 cancellation response is not considered completion. It also cleans up external work left by failed/canceled runs and avoids canceling a request owned by another active execution.
- Old environment-based/manual Dagster runs are not retroactively attributed by model name. Backoffice refuses resuming an encrypted legacy configuration without saved revision identity. Start a new execution with an enabled profile; completed results remain available. The rollout found no active encrypted LLM runs requiring adoption.

The settings page shows the latest 30 tracked launches, including pending external confirmations, and refreshes while work is pending. Already running work is not automatically resumed after re-enabling a model.

## Checks

Run `scripts/test-llm-postgres.sh` for isolated PostgreSQL lifecycle, admission, and monitor tests. The script creates a temporary local Docker PostgreSQL instance and removes it afterward. Run the focused Backoffice/service tests and `uv run dg check defs` for the remaining launch and transport contracts.
