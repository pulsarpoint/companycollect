# Saved-input manual crawl validation — 2026-09-20

Backoffice `/admin/crawls` now reads all three current input views independently
of crawler availability. It shows total/enabled/disabled counts, a domain filter,
25-row pages ordered by priority, and explicit single-domain/page-batch starts.
Opening or filtering the page does not launch work. Current inventory: full 0,
jobs 0, basic info 50 (all enabled).

Deployed only `src/dagster_v3/defs/website_crawl` to the running Dagster host and
added the existing crawler URL/token to its environment. Remote definitions
validated; all three input and dispatch jobs are visible in `dagster_v3`.
The location reload exceeded the client's 60-second response timeout but finished
successfully; reconciled the deployed files and verified `LOADED` with no load
error. Dagster supervisor remained PID 2232153, NRestarts 0. No active runs were
restarted and no unrelated source files were synchronized.

Started exactly one saved Basic info domain, `almi.se`, through the Backoffice
button. Dagster run `f30839a1-9bea-4aa8-820f-a563ca5f6e27` succeeded, dispatching
one REST request. The crawler completed and uploaded both its result and artifacts:

- Batch: `e7104c8d-e97b-416e-9052-2a334375ae01`
- Request: `dagster-crawl-73c6e35dd2bb3bb3d2abed262238d48661c753351f0c6b2a53ac757fdbbc4e3b`
- Result: `s3://crawls/company-crawls/dagster-crawl-73c6e35dd2bb3bb3d2abed262238d48661c753351f0c6b2a53ac757fdbbc4e3b/attempts/0001/result.json.gz`
- Outcome: `site_info_complete`, one collected page, no error.
- Crawl: 19:17:24–19:17:36 UTC (12 seconds); one model call, 28,966 input and
  1,338 output tokens. The description identifies Almi's financing and business
  advisory services.

Verified in the browser: inventory counts and filtering, the Dagster acknowledgement
and run link, the completed attempt, and the saved result page reading S3 through
ClickHouse. The other 49 saved inputs were not started.

Checks passed:

- Dagster: 51 tests in `test_website_crawl_input_assets.py`, including disposable
  ClickHouse + local HTTP execution for all three dispatch assets, priority,
  disabled current revisions, preview without HTTP, prevalidation before dispatch,
  and replay of stable request identities.
- Backoffice: 28 route/dispatch tests and 13 disposable ClickHouse tests.
- Local and deployed `dg check defs`, focused Ruff check, TypeScript typecheck,
  and production build.

These are manual crawl-now batches, not recurring monthly refresh processing.
Per-type result tables, freshness checks, and cross-batch execution claims remain
in the proposal. The bridge rejects unsupported custom page/instruction inputs
and proxy routes rather than silently discarding their settings. Successful
dispatch means durable crawler acceptance; crawl completion is shown separately
in the attempt history.
