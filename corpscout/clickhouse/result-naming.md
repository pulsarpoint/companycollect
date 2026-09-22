# Brave and crawler result names

The existing migration files were edited in place. Their numbers and filenames
are unchanged; no new migration version was added.

| Relation | Meaning |
| --- | --- |
| `company_brave_search_results` | Every completed Brave attempt, including errors. Also the Dagster asset name. |
| `company_brave_search_results_latest` | Full newest attempt per country/company/query type. |
| `se_company_brave_search_results_latest_success` | Stored newest successful Swedish answer per company/query type; read with `FINAL`. |
| `se_company_brave_search_results_s3_archive` | Older archived Brave responses in S3; new Brave attempts are stored directly in ClickHouse. |
| `website_{full_crawl,jobs_crawl,site_info}_results` | Every crawl attempt; use `FINAL` to remove duplicate writes of the same attempt. |
| `website_{full_crawl,jobs_crawl,site_info}_results_latest` | Newest attempt per domain/effective configuration, including failures. |
| `website_{full_crawl,jobs_crawl,site_info}_results_latest_success` | Newest successful attempt per domain/effective configuration. |
| `website_crawl_results_s3_archive` | Shared mapping of all crawler result JSON objects in S3. |

Input `*_requests_current` views remain: they select the newest request revision
per domain. Result `_current` views are removed because their deduplication kept
all attempts, not just the most recent scan.

The per-type result rows identify the complete S3 object with `s3_path = _path`.
`request_id` plus `attempt` identifies the crawl attempt. The archive view retains
support for older paths whose attempt is unknown. The standalone crawl/analysis
importer continues to use `website_crawl_results` and `website_crawl_results_latest`.

## Existing installations

Editing an applied migration does not change an existing database. Coordinate
the schema changes with deployment of the new consumers; do not rewind the
migration ledger or run destructive down migrations over stored results.

1. Drain the Brave worker and capture its original `execution_id` for resumption.
2. Rename the successful-answer table from `se_company_brave_domains` to
   `se_company_brave_search_results_latest_success`. Retain its rows. Recreate
   `se_company_brave_search_successes` with the target in migration 428.
3. Replace `company_brave_search_latest` with `company_brave_search_results_latest`
   using migration 428's full-row definition.
4. Rename/recreate `se_company_brave_domains_history` as
   `se_company_brave_search_results_s3_archive`, retaining migration 416's definer,
   and `website_crawl_results_s3` as `website_crawl_results_s3_archive`, using the
   final definition in migration 426. These changes do not move S3 objects.
5. Create the three crawl `_results_latest` views and replace `_latest_success`
   with migration 430's direct `FINAL` queries. After consumers are updated,
   remove the three old `_results_current` views. Preserve the physical tables.
6. Update the publisher's grants for the renamed relations, following
   `services/dagster_v3/scripts/provision-processing-storage.py`. Deploy Dagster,
   Backoffice and the crawler's ClickHouse importer together with these names.
7. Resume `company_brave_search_job` with `ops.company_brave_search_results.config.execution_id`
   set to the captured original execution. Existing `brave/execution` tags and
   result IDs remain valid, so completed attempts are skipped.

The source refactor does not itself deploy or perform this live cutover.
