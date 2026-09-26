# Workspace queues

`/admin/queues` opens Webtech; sibling pages expose Brave, crawler and IP enrichment inputs. Crawler has Full, Jobs and Site information selections. Input and task counts include retained inputs from earlier executions; they are not counts of pending work.

Each task row has **Configure processing**, which selects the task and opens its settings sheet directly. The selected-task page offers the same action. Disabled processing shows its reason (empty task, active run or unavailable Dagster status). Search narrows the preview only. The sheet provides processing settings and a JSON editor. Submission always processes the whole selected task and launches only its results asset:

| Page | Input membership | Results job |
| --- | --- | --- |
| Webtech | `webtech_scan_input` | `webtech_scan_results_job` |
| Brave | `company_brave_search_input` | `company_brave_search_job` |
| IP enrichment | `ip_enrichment_input` (draft) | `ip_enrichment_results_job` |
| Crawler | `website_crawl_task_domains`, by `crawl_type` | `website_full_crawl_results_job`, `website_jobs_crawl_results_job`, `website_site_info_results_job` |

Crawler request presets remain on `/admin/crawls`. They are distinct from a task's frozen domain membership. Brave's input table does not record submission time, so its tasks sort by ID and the time column says “Not recorded.” Legacy inputs without task IDs remain visible but cannot be processed here.

## Lifecycle and retries

Webtech draft submissions can be appended until its results asset begins. Freshness is checked during execution preparation, preserving skipped entries. The UI does not infer draft/frozen/completed PostgreSQL task state from input presence or run status: the asset validates task metadata and incomplete submissions when it begins. A queued Dagster run is not a confirmation that the task has frozen. Legacy Webtech tasks require default settings.

Brave retains its fixed input selection lifecycle. Existing source-page input-plus-processing actions are unchanged; this page provides separate processing of already prepared tasks. It does not yet provide input import, cancellation, cleanup or saved profile management.

The sheet accepts an execution ID for resuming with original settings. Webtech draft queues reuse their saved execution by default. Fully processed tasks clear their input rows after every outcome is published, retaining task history and results. Website errors produce a “Completed with errors” history entry with an error count. Pipeline failures that leave work incomplete keep their inputs for recovery. Add failed pages to a new queue for another attempt. To rescan a completed task, add the pages to a new queue. Brave uses the original results run ID for resumption. Progress and errors are available through the linked Dagster run; Refresh updates the page's status.

The action checks task membership in the chosen input table, blocks known active runs (including input runs), restricts parameter names and numeric ranges, and lets Dagster validate the asset config. It never accepts arbitrary jobs, assets, resources, tables or selection overrides. Request tags recover a retry after a lost launch acknowledgement. Check-plus-launch is serialized per task within a Backoffice instance; this is not a cross-instance distributed lock. Processor task locks and lifecycle validation remain authoritative.

## Verification

- Queue action tests cover results-only launch for every processor, all crawler modes, empty and active tasks, invalid parameters, preview isolation, request retries and concurrent clicks.
- All six default job configurations validated against live Dagster without launching work.
- Browser checked task/input navigation and Webtech, IP and crawler sheets.
- Live input queries verified against ClickHouse, including the 48.6 million-row IP queue. Input previews use primary-key order instead of sorting the whole table by task first.

## Add a domain from its detail page

The Swedish domain detail's Web technologies tab has **Add to Webtech queue**. It submits the route domain to `webtech_scan_input_job` with a stable submission UUID, `queue_scope=workspace` and source `backoffice:se-company-domain`. Task selection is owned by Dagster's draft resolver; the action never supplies a task ID or launches a results asset. Recent pages remain eligible for queue insertion.

The action reports an accepted import separately from a completed import, polls the input run, and links its saved task ID to the queue page. Failed imports retry with the same receipt. Existing successful/active requests are recovered from submission tags after an uncertain response. The homepage is the target; additional subdomains and paths are not implicitly queued.

PostgreSQL migration 125 repairs the existing worker's SELECT/INSERT/UPDATE privileges on `processing.input_submissions`. It was applied and verified using the deployed worker connection. No scan or input task was launched during this change's verification.

## Add Swedish domains from the list

`/admin/se/companies/domains` offers **Add to Webtech queue** for selected roots or **all matching domains** across all pages. The input asset reads `corpscout.se_company_domain FINAL`, applying the same list filters (including shared ownership), exclusions and distinct root selection. Backoffice submits the source and selection to Dagster; it does not fetch every selected row. Each root targets its HTTPS homepage once, including domains associated with multiple companies.

The list shares import progress and queue links with the domain detail action. It clears the submitted selection only after successful input materialization and preserves the receipt when retrying a failed import. Processing and freshness checks remain a separate step on the queue page.

Completed Webtech input cleanup is scoped to the task and runs under its selection lock. The `inputs_purged_at` marker is saved only after synchronous ClickHouse deletion is confirmed. Retrying a completed results asset retries pending cleanup without submitting scanner work. Retrying an old completed input submission does not recreate purged rows.

The Webtech page automatically opens the latest remaining input task and drops obsolete selections after cleanup. There is no Task ID search. If more than one unfinished queue exists (for example, a frozen task and a new draft), each remains selectable. Recent task history comes from Dagster results runs independently of input rows, showing the latest status per task within the most recent 50 runs and linking to run details.

## Crawler drafts

SE → Domains → Add to crawl queue imports into `website_crawl_task_domains` through
`website_crawl_input_job`. Each crawl type (full, jobs, basic site info) has its own open
workspace draft. Imports can combine source selections and manual targets (Dagster config).
The existing recurring request tables remain presets. The entry table is partitioned by task
and read without `FINAL`.

Queues → Crawler automatically selects the current draft. Configure processing opens the
sheet; Start launches only the chosen `website_*_results` asset with `task_id`. Freshness and
force settings are evaluated during execution against the frozen start time: any successful
crawl inside that window counts, and a later failure never hides it. Interrupted runs retain
inputs and resume the saved execution by re-sending requests; the crawler reattaches to a
request it already holds as a no-op, but a preset edited mid-execution stops the run instead
of silently crawling under the new settings. Completed tasks drop their partition after every
outcome is stored, including terminal website errors. History and links to Dagster remain
visible. `batch_size` is not a crawler queue parameter; the window size `max_in_flight` is.

The crawler page (`/admin/crawls`) shows saved-input batches only; draft tasks and their runs
are followed on the queue page. The old immediate “Send for crawl” action and the crawl-task
resume on the crawler page are removed.

Import status uses the same polling/status component as Webtech, with stable submission IDs
for retries. A successful Dagster import clears the selection; an accepted launch alone does not.

## IP enrichment drafts

Admin → IP addresses → **Add to enrichment queue** imports the selection into
`ip_enrichment_input` through `ip_enrichment_input_job` with a stable submission ID; the
status component polls `/admin/ip-enrichment/queue-submissions/<runId>` and links the saved
task. A successful import clears the selection; an accepted launch alone does not. The
entry table is partitioned by task and read without `FINAL`.

Queues → IP enrichment automatically selects the current draft; Start launches
`ip_enrichment_results` with `task_id`. `batch_size`, `max_requests` and
`request_delay_seconds` may change between resumes; the RDAP policy fields are frozen at
Start. Lookup errors complete the task “with errors”; add the failed addresses to a new
draft (`retry_failed_task_id`) to retry them. RIPE and APNIC are asked without personal data
(REST search, whois `-r`); the per-registry request budget (`registry_daily_budgets`) is a
Dagster launchpad setting, not a sheet field. The old one-shot `ip_enrichment_workflow` is
removed.
