# IP enrichment draft queues

Backoffice **Admin → IP addresses → Add to enrichment queue** launches only
`ip_enrichment_input_job`. There is one open draft per `queue_scope` (default `workspace`);
table selections, explicit IP lists and "failed addresses of task X" append to it. Adding
inputs never looks anything up. Since ClickHouse migration 456 the draft follows the shared
processing queue contract
([spec](../superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md)), like
Webtech and the crawler.

## Storage

- ClickHouse `corpscout.ip_enrichment_input`: the only stored copy of each entry
  (`task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id,
  observed_at, submitted_at`; `ip_version` and `bucket` materialized), `MergeTree`,
  `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`. Read it without `FINAL`.
  `input_id` is `bbb:` (the address's 256-way bucket, zero-padded) followed by the JSON
  tuple `[source_name, source_record_id, ip]`, computed in ClickHouse and enforced by a
  `CHECK`, so a task is walked bucket by bucket.
- PostgreSQL `processing.tasks`: lifecycle, the frozen execution (`config.execution`) and the
  counters written at completion. `processing.input_submissions`: idempotent import receipts
  (selection fingerprint and count; bulk IP lists are stored as count + sha256).
- `corpscout.ip_enrichment_results` (`ORDER BY (bucket, ip, result_id)`): one row per address
  and execution (`attempt` is always 1; a retry is a new draft). `ip_enrichment_current`
  serves the latest conclusive data per address. RDAP coverage stays in `rdap_networks`,
  `rdap_network_segments` (roles `lookup_result`, `parent`), `rdap_ip_lookup_results` and the
  class table `rdap_network_registry_class`
  ([ip-registry-reference-data.md](ip-registry-reference-data.md)).

## Import

Materialize `ip_enrichment_input` with a stable `submission_id` and exactly one of:

```yaml
ips: [116.203.39.236, 2001:4860:4860::8888]          # explicit list
source_relation: corpscout.commoncrawl_ip_addresses  # + filters / ip_search / max_rows / select_all
retry_failed_task_id: <task uuid>                    # addresses whose result in that task has an error
```

Selection, canonicalization and grouping run inside ClickHouse (`selected_ips_sql`); the
import is one `INSERT … SELECT` minus the identities the draft already holds, then two counts
for the receipt (`input_count` = rows this submission added, `total` = the draft). No size
cap. A failed import blocks Start; retry it with the same `submission_id` and selection: the
retry kills its ClickHouse query (`ip-queue-import:<submission_id>`), deletes only that
submission's rows (synchronous lightweight delete) and reselects from the source as it is now.
Imports and Start share the task's PostgreSQL advisory lock.

## Start and resume

**Queues → IP enrichment** selects the current draft; **Configure processing** launches
`ip_enrichment_results_job` with `task_id`. Under the task lock, the results asset:

1. freezes the draft (`status=selected`, `frozen_at`) and saves the execution in
   `processing.tasks.config.execution`: `execution_id` (the original Dagster run id),
   `profile` (`force_rdap`, `rdap_cache_days`, `parent_depth`, `rate_limit_retry_seconds`,
   `transient_retry_seconds`, `ripe_rest`, `apnic_whois`, `processor_version`), `started_at` and
   `freshness_cutoff = started_at − rdap_cache_days`. `batch_size`, `max_requests`,
   `request_delay_seconds` and `registry_daily_budgets` are transport settings and may
   change between resumes. The default-draft slot is released at once, so new additions form the
   next draft;
2. loops until nothing remains. *Remaining* is a live query per bucket: the task's entries
   in that bucket whose `input_id` has no row in `ip_enrichment_results` for this execution
   (`bucket = b AND task_id AND execution_id`, one primary-key range). Pages of `batch_size`
   follow an `input_id` cursor inside the bucket; each page costs one negative-cache read
   (`rdap_ip_lookup_results_current`), one trie `dictGet`, one read of the network rows the
   page needs (never `raw_response`), one insert of lookup markers and a share of one result
   insert. Freshness is judged against the frozen execution: a network or marker counts when
   its time is `>= freshness_cutoff`, a retryable error when `retry_after > started_at`;
   `force_rdap` skips both caches. Registry requests (RIPE through its REST search, APNIC
   through whois `-r`, the others through RDAP) happen only for misses, paced by `request_delay_seconds`; a miss also
   costs one registry-class context query, and networks fetched earlier in the run are reused
   before any request. GeoIP City/ASN are read locally
   per address;
3. stores outcomes through a `ResultBuffer` (500 rows or 5 seconds, acknowledged
   `async_insert`); the cursor never re-reads a page inside a pass, the buffer is flushed
   after every pass and a further pass confirms nothing remains. A reached `max_requests`
   flushes what was resolved and fails the run with "budget reached"; the task stays
   `selected` and re-running it resumes (this is the pause-and-resume procedure: terminate
   or let the run stop, change transport settings if needed, re-run the task). Nobody
   re-runs it automatically: `ip_enrichment_results_job` is tagged
   `dagster/max_retries: "0"`, because `dagster.yaml`'s run retries (enabled,
   `max_retries: 2`) relaunch any failed run — `dg.Failure(allow_retries=False)` only bypasses
   op retry policies — and each relaunch would resume the same execution and spend another
   `max_requests`. A crash or host restart likewise needs the operator's re-run;
4. finishes when a pass finds nothing: counts succeeded/failed per bucket from the results
   (failed = any of City/ASN/RDAP in `retryable_error`/`terminal_error`), `skipped = total −
   succeeded − failed` (expected 0), marks the task `completed` (`completed_with_errors` in
   the run tags when any address failed), drops the task's partition and records
   `inputs_purged_at`.

Re-running the same task with the same profile resumes the saved execution (`execution_id`
may be omitted; an explicit one must be the saved one). A completed task re-run only retries
cleanup. A run that loses its PostgreSQL connection over a multi-day execution fails at
completion, not mid-pass; re-running the task finishes it cheaply, since every earlier pass's
results are already stored. A changed profile is rejected; to re-look addresses up, add them
to a new draft (`force_rdap: true` there bypasses caches; `retry_failed_task_id` selects the
failures). A retry draft of **terminal** RDAP errors (`terminal_error`: `no_registry`,
`range_mismatch`, `registry_catch_all`, `invalid_response`, …) is a no-op for them within
`rdap_cache_days` of the failure: their markers are fresh and re-served without a request.
Process such a draft with `force_rdap: true` to ask the registry again (it also bypasses
the network cache for every other address of that draft, so keep the draft to the failures);
retryable errors are asked again once their `retry_after` has passed. History stays readable from run tags (`ip_enrichment/execution_id`,
`ip_enrichment/outcome`, `ip_enrichment/succeeded_pages`, `ip_enrichment/failed_pages`,
`ip_enrichment/skipped_pages`).

## RIPE and APNIC without personal data, and the per-registry budget

The RIPE Database acceptable use policy limits the **personal data sets** (person and role
objects) one source address may receive to **1,000 per 24 hours**; queries themselves are
unlimited within reasonable use (3 simultaneous connections at most), and pooling limits
across addresses is named as avoidance. RIPE's RDAP `ip` answers embed 1–5 person objects,
so every one counts. We do not need contacts, so RIPE misses go to the RIPE Database REST
search with `flags=no-referenced` and `flags=no-personal` (`commoncrawl_rdap/ripe_rest.py`):
the most specific `inetnum`/`inet6num` with `netname`, `country`, `status`, `org` handle,
`mnt-by` and dates, no person or role object, so nothing counts. Both flags together were
verified live on 2026-09-26: `193.0.6.139` returned only an `inetnum` object. The answer is
stored like an RDAP one (same `ripe:<range>` network key, `rdap_self_url` under
`rest.db.ripe.net`, empty registrant names; `descr` is dropped). Unallocated or
non-authoritative space answers with RIPE's root object; the resolver then asks RDAP once.
`ripe_rest: true` is part of the frozen profile.

APNIC misses go to APNIC's whois service on port 43 with the `-r` flag
(`commoncrawl_rdap/apnic_whois.py`; the HTTP gateway `wq.apnic.net` does not honour `-r`):
the most specific `inetnum`/`inet6num` with contact handles only, followed by route objects
that are ignored. The holder name is the **first `descr` line** (APNIC objects rarely carry
`org:`); further `descr` lines (addresses) and every contact handle are dropped; `status` is
upper-cased and whitespace-collapsed. An answer that is an NIR's own allocation object —
`netname` starting with `JPNIC`, `KRNIC`, `TWNIC`, `IDNIC`, `CNNIC`, `IRINN` or `VNNIC`, or a
first `descr` naming the NIR — falls back to RDAP, which the IANA bootstrap routes to the
NIR server; `mnt-by` alone decides nothing (FPT's `103.35.64.0/22` is maintained by
`MAINT-VN-VNNIC` and is the holder's allocation). `apnic_whois: true` is part of the frozen
profile. ARIN, LACNIC and AFRINIC stay on RDAP.

Cross-RIR redirects are refused the same way, before any personal data can be fetched: RIPE-
or APNIC-managed space registered inside another registry's IANA /8 (ERX legacy space,
transfers) answers on that registry's own RDAP server with a redirect to
`rdap.db.ripe.net`/`rdap.apnic.net`. That redirect — and a first URL that lands on either host
directly, when the bootstrap already resolves there — is refused before the target body is
fetched; the miss is re-sent to the REST search or whois `-r` instead and counted under the
target registry (`reroutes_by_registry`). An address with no exact bootstrap match
(`registry_for` returns `''`: 6to4 `2002::/16`, unmapped space) is never requested: it gets a
terminal `no_registry` marker, cached for `rdap_cache_days` like other terminal errors, so retry
drafts do not turn it into a request each time.

`registry_daily_budgets` (transport, default `{}`, keys are whoisit's names: `ripe`, `arin`,
`apnic`, `lacnic`, `afrinic`, `jpnic`, `idnic`, `krnic`, `twnic`, `registro.br`) is an
optional rolling 24-hour **request** budget per registry. The registry of a miss is resolved
from whoisit's bootstrap data before the request (`RdapClient.registry_for`); a miss of a
registry at its budget is deferred (no result row, no error), the run keeps processing
everything else, and when a whole pass resolved nothing it sleeps until an hour's share of
the budget frees (`wait_for_registry_budget`, logged every 10 minutes). The window is seeded
on start from `rdap_networks.fetched_at` of the last day (every writer); the seed
under-counts requests that stored no network — errors, not-founds and the redirected half of
a reroute — because the lookup-marker table (`rdap_ip_lookup_results`) has no registry
column. Proxy egress lanes were considered and dropped on 2026-09-26: they would not shorten a
run (pacing is global), the no-personal-data paths make them unnecessary, and pooling a
registry's allowance across addresses is the AUP's anti-avoidance case. The budget is not in
the backoffice sheet; set it in the Dagster launchpad.

## Counters and how to read them

`rdap_requests_by_registry` and `rdap_person_entities_by_registry` are keyed by the registry
that answered (`RdapLookupResponse.rir`). The person-entity counter counts person
(`kind: individual`) and role (`kind: group`) vCards alike, nested ones included — RIPE's AUP
counts both — so an answer that references only role objects is visible too. A plain `ripe` or `apnic` key in
`rdap_person_entities_by_registry` means personal data leaked: stop the run. RDAP traffic
sent because a REST/whois answer was a catch-all or an NIR's own object is counted
separately in `rdap_fallbacks_by_registry` (RIPE's catch-all root, an APNIC NIR allocation),
with its person entities under `"<rir>:fallback"` keys — that is expected, not a leak. A
fallback's optional parent lookup is counted under the NIR's own key (`jpnic`, `idnic`, …),
not `apnic`. `reroutes_by_registry` counts cross-RIR redirects re-sent to REST/whois, and
`pauses_by_registry` counts rate-limit/block pauses per registry, including the run-wide
`bootstrap` key described next.

## Pauses and a stalled bootstrap

A RIPE REST `403` or `429`, or an APNIC whois `%ERROR:2xx`, pauses that registry for
`max(retry, 15 min)`; its misses are deferred (no result row, no marker) until the pause
ends, the same as a budget deferral. A failed IANA bootstrap — which `registry_for` and every
lookup depend on — pauses the whole run instead, under the key `bootstrap`: every miss is
deferred with no per-address error, back-off doubles from 60 s to a 900 s cap, and it resets
on the first successful bootstrap. The run retries forever and never fails on its own; an
operator terminates it if IANA stays down. Both kinds of wait run inside the Dagster step and
hold the `commoncrawl_rdap` pool slot while sleeping.

## Registry-level registrations

Every miss is classified with the deployed rule (`classify_registration`, data from
`ip_registry_daily`): `reusable`, `registry_level`, `unallocated` or `unknown` while the
reference data is incomplete. A `registry_level`/`unallocated` registration is stored like
any other (role `lookup_result`) with its class row; `rdap_network_trie` excludes it
(migration 451) and the in-run cache never remembers it, so it answers only the queried
address — which is served next time by its own `found` marker in `rdap_ip_lookup_results`.
Other addresses of the block get their own lookup. Details and queries:
[ip-registry-reference-data.md](ip-registry-reference-data.md).

## GeoLite2 (upload page)

There is no MaxMind account, so the owner downloads the files by hand and installs them
from the backoffice: Admin → Settings → GeoLite2 (`/admin/settings/geolite2`).
`MAXMIND_DATABASE_DIRECTORY` on the Dagster host holds `GeoLite2-City.mmdb` and
`GeoLite2-ASN.mmdb` directly. Procedure:

1. Download `GeoLite2-City_YYYYMMDD.tar.gz` and `GeoLite2-ASN_YYYYMMDD.tar.gz` from MaxMind
   (owner's browser login).
2. Upload one or both on the page (MaxMind's `.tar.gz` or a bare `GeoLite2-<edition>.mmdb`,
   at most 200 MB each). The backoffice checks only the file name and size, stores each file
   unchanged in bucket `geolite2` under `uploads/<uuid>/<file name>` (the bucket is created
   on first use) and launches `geolite2_install_job` with the uploaded keys. The page
   refreshes until the run ends and links it in Dagster.
3. The asset `geolite2_databases` (group `commoncrawl_geoip`) is the authority. For each
   upload it extracts the single `GeoLite2-<edition>.mmdb` member (tar data filter; absolute
   paths, `..`, links and other non-regular members refused), requires `database_type == "GeoLite2-<edition>"` and
   refuses a build older than the installed one (an equal build with the same bytes is a
   no-op); a missing upload object fails the run naming the key. Every upload is validated
   before anything is replaced; then each file is copied to
   `.GeoLite2-<edition>.mmdb.<uuid>.tmp` in the same directory, fsynced, re-opened and moved
   over the old file with `os.replace` (a rename: a running enrichment keeps its mapped old
   inode). Right before each rename the installed build is read again and the run stops if
   it is now newer (a concurrent install). A refusal during validation replaces nothing; a
   failure between the two renames can leave one edition replaced — the metadata and the
   run log say which. Staged files not renamed are removed, and every run first sweeps
   `.GeoLite2-*.mmdb.*.tmp` files older than an hour (left by a run that died). The files
   are written by the user Dagster runs as (root on the host), mode 0644.
   After installing, the asset applies the lifecycle rule expiring `uploads/` after 90 days;
   a store that refuses it only logs a warning (`lifecycle_applied = false`).
4. No restart: `ip_enrichment_results` opens the files per run; the next run uses the new
   files. The materialization metadata (`city_build`, `asn_build`, `city_sha256`,
   `asn_sha256` — `missing` or `unreadable` when there is no readable file — `installed`,
   `source_keys`, `lifecycle_applied`, `fresh`, `city_age_days`, `asn_age_days`) is what the
   page shows as installed.
5. The check `geolite2_databases_fresh` (job `geolite2_freshness_job`, or launch
   `ip_enrichment_results` with its checks) fails when either build epoch is older than
   14 days. Every results run also reports `geolite2_city_build`/`geolite2_asn_build` and
   warns when stale.

Installs never overlap: the asset declares pool `geolite2_install`, and the instance limits
every pool to one slot (`dagster.yaml` `concurrency.pools.default_limit: 1`); a second
upload's run waits for the first. The page disables the upload button while the latest
install run is unfinished; a zombie run stuck in `STARTED` (host crash) keeps it disabled —
terminate it in Dagster (mark as canceled) to re-enable the button.

Files replaced on the host by any other route are not reflected on the page until the next
install run materializes the asset.

## The 2026-09 clean re-run

On 2026-09-25 the 48.6M-IP run (task `4802549d-…`) was terminated after 1.08M addresses:
~90% of its wall time were per-address ClickHouse round trips and 174k addresses had been
answered by registry-level blocks (`APNIC-AP` 103.0.0.0/8 and similar). The owner decided a
clean re-run instead of a remediation: at deploy (plan
`2026-09-25-ip-enrichment-queue-contract.md`, Task 8) the four legacy PostgreSQL tasks were
cancelled and `ip_enrichment_input`, `ip_enrichment_results` (including the 8.29M legacy
GeoIP import rows of 2026-07-10, task `cd603d91-…`), `rdap_networks`,
`rdap_network_segments`, `rdap_ip_lookup_results` and `rdap_network_registry_class` were
truncated after checking the latest ClickHouse B2 backup; the `ip_registry_*` reference
tables and `ip_registry_daily` were kept. Until the re-run reaches an address,
`ip_enrichment_current` has nothing for it (backoffice IP pages, `commoncrawl_ip_checks`).
Classes regenerate per miss and at the daily refresh.

## Known costs and follow-ups

- The remaining/completion query anti-joins by `bucket` (the results table's `ORDER BY
  (bucket, ip, result_id)`; `task_id` is not part of that key), so it scans a bucket's whole
  range across every task, not just the one being processed. Harmless right after the
  2026-09 wipe; a later small draft pays the size of the accumulated results table.
  Follow-up: a `task_id` skip index.
- A miss deferred by a paused target registry after a cross-RIR redirect (e.g. ARIN → RIPE
  while RIPE is paused) repeats the ARIN redirect on the next pass; nothing remembers the
  reroute across passes.
- `RdapClient._lookup` reaches into whoisit's private `_bootstrap` attribute, pinned in
  `uv.lock`; a whoisit upgrade needs re-verification.
- The NIR-object rule matches a netname prefix, so a holder's own netname such as
  `IDNIC-<HOLDER>-ID` could be misread as an NIR's own allocation and sent to RDAP
  unnecessarily; to be verified in the Task 8 smoke run.

## Validation

`tests/test_ip_enrichment_input.py` (disposable ClickHouse + PostgreSQL): entry-table contract,
canonical dedup and append, receipt replay, a retry replacing only its own rows, source
filters, inventory search, the failed-results mode, freeze → next draft.
`tests/test_ip_enrichment_results.py`: bounded round trips per page, frozen cache window,
negative markers, registry classes per miss (trie exclusion, per-address marker), in-run reuse
and the request budget, RIPE via REST and APNIC via whois `-r` without person objects (the
root-object and NIR fallbacks, the real FPT answer), budget deferral and wait (resolver and loop), completion
with partition purge, errors as published outcomes, budget resume, lost write and cleanup
acknowledgements, changed-profile refusal. `tests/test_geolite2_freshness.py`: build times,
14-day rule, check wiring. `tests/test_geolite2_install.py` (disposable RustFS, MaxMind's test
databases): archive and bare installs, atomic replace, older-build and edition refusals,
unsafe archive members (traversal, links, hardlinks, devices), missing uploads, staged-copy
cleanup and the stale-temp sweep, a concurrent newer install, validate-all-before-install,
metadata (including an unreadable installed file), the upload expiry rule and its
warning-only failure.
