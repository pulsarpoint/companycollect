# Crawl input/result asset validation — 2026-09-20

Implemented the two-stage flow. Backoffice launches input assets; it no longer
inserts crawl inputs or synthesizes materialization events. Separate downstream
result assets own completed output tables. Migration 430 is applied after 429.

The Backoffice processing form and Dagster results config require an explicit
CAPTCHA model/budget, model API/name, page and model-call limits, and page-selection
mode. Basic info is single-page; custom discovery requires instructions.

## Checks

- 43 input-selection integration tests passed against disposable ClickHouse.
- 150 result-processing/migration tests passed (real disposable PostgreSQL and
  ClickHouse, plus crawler HTTP fixture). Coverage includes timeout and pending-S3
  recovery, competing-run exclusion, required fields, priority, disabled revisions,
  freshness, partial failures and idempotent manual replay.
- 64 Backoffice tests, the ClickHouse inventory integration test, typecheck and
  production build passed.
- Ruff and local/server `dg check defs` passed.
- Only the `website_crawl` source package was synchronized. The running Dagster
  systemd service retained MainPID 2232153 and NRestarts 0. Code-location reload
  took longer than the first HTTP acknowledgement timeout; the new job catalog
  was checked before launching the result job.

## Live Backoffice runs

1. Selected the existing `almi.se` domain and chose **Add to crawl inputs → Basic info**.
   Job `website_site_info_input_job`, run `45a53318-0dec-44d1-95e4-27d27c7837a8`,
   succeeded and emitted a materialization for `website_site_info_requests`.
   The existing 50 inputs were preserved; this action started no crawler request.
2. From `/admin/crawls`, started only `almi.se` with DeepSeek Flash for both models,
   CAPTCHA budget 6, basic-info mode, page limit 1 and model-call limit 3.
   Job `website_site_info_results_job`, run `5baeabac-372f-47e7-b9b9-f358f63c6b57`,
   succeeded. The processing step took 17.88 seconds and materialized the result asset.
   Request: `dagster-crawl-fa27a99684bc6091dba5a9c7b167f1f3e97d7d8161dc13009fc76c6061831edc`.
   The `website_site_info_results_current` row is completed/successful and describes
   Almi's business loans, venture capital and business-development advisory services.
   It preserves `site_info`, page observations, page metadata, model usage and S3 path.
   The model made one call: 28,966 prompt tokens and 1,460 completion tokens.
   The S3 mapping returns the saved object with one collected HTML document.

S3 path:
`crawls/company-crawls/dagster-crawl-fa27a99684bc6091dba5a9c7b167f1f3e97d7d8161dc13009fc76c6061831edc/attempts/0001/result.json.gz`.

No full or jobs inputs were added and the other 49 basic-info inputs were not started.

3. Repeated the same request settings from Backoffice in run
   `80b46cb1-e4ce-49bc-a7d7-4ff4e7aec448`. It succeeded with `fresh_skipped=1`,
   `completed=0`, `unsuccessful=0` and no additional crawler submission. Both input
   and result assets have live materialization events. The service kept its original
   PID/restart count after deployment and validation.

Verified the complete stored `site_info` JSON equals the S3 object's
`crawl.site_info`, not merely a matching description string.
