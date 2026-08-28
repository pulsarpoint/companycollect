# Sweden Platsbanken jobs design doc

## 1. Source overview

- **Country / source**: Sweden — Arbetsförmedlingen JobTech Platsbanken
- **Module**: `defs/sweden_platsbanken/` · historical DuckDB files
  `data/sweden_platsbanken/duckdb/partition_key=<year>/data.duckdb` · live
  snapshot/event DuckDB file `data/sweden_platsbanken_source.duckdb` · pool
  `sweden_platsbanken_duckdb`
- **ClickHouse migrations**: `000302_corpscout_se_platsbanken_jobs` and
  `000303_corpscout_se_platsbanken_job_contacts`
- **License/authentication**: CC0, no subscription or API key

| dataset | URL | format | observed size/cadence | auth? |
|---|---|---|---|---|
| Complete historical ads | `https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/` | yearly/quarterly ZIP JSONL | 2016 onward, roughly 0.6–1.4 GB compressed per completed year | no |
| JobStream snapshot | `https://jobstream.api.jobtechdev.se/v2/snapshot` | JSONL | current active state, bootstrap only | no |
| JobStream stream | `https://jobstream.api.jobtechdev.se/v2/stream` | JSONL | full upserts and sparse removals by update window | no |

- **Job key**: `original_id` when present, otherwise `id`. Historical records use
  a content-derived `id` and carry the Platsbanken ad id in `original_id`; live
  JobStream records normally put that ad id in `id`.
- **Company key**: normalized ten-digit employer organization number, exact join
  to `corpscout.se_companies.company_id`. Employer-name matching is prohibited.

## 2. Ingest mode — and why

Two modes are required because the source exposes two different historical
guarantees:

1. The 2016+ archive is a finite collection of immutable bulk ZIP JSONL files.
   It is a one-time annual-partition backfill and object-storage checkpoint, not
   a scheduled full refresh. Completed annual files and any quarterly files for
   an in-progress year share the same `YYYY` partition.
2. JobStream is an append-only windowed API after a one-time current snapshot.
   Event manifests persist `updated_before`; the next run starts five minutes
   earlier to replay late/boundary events. Deterministic event/version UIDs make
   that overlap idempotent.

This deliberately deviates from the normal non-partitioned bulk full-refresh:
re-downloading and atomically republishing the complete multi-gigabyte 2016+
corpus on every stream window would get slower forever and would risk erasing
prospective history. Historical tables therefore append only previously unseen
stable UIDs.

## 3. Loading

- HTTP uses dlt's retrying request client. Large transfers also have a
  whole-download retry and `Content-Length` validation.
- Historical assets use annual Dagster partitions from 2016 through the current
  year, with one partition per run. The source catalog is filtered before
  download and each partition gets its own replay manifest.
- ZIP members are extracted one archive at a time. All annual/quarterly archives
  belonging to the selected year are loaded into that year's isolated DuckDB
  file with `read_json_objects(..., format='newline_delimited')`.
- DuckDB raw tables retain the JSON payload, payload hash, source object key,
  source URL, run id, line number, and retrieval timestamp.
- Official archive files and JobStream JSONL responses are stored unchanged in
  RustFS. This retains the source evidence needed to reproduce every normalized
  job version, including application and employer contact fields.
- The single-slot `sweden_platsbanken_duckdb` pool covers historical download,
  raw loading, normalization, and export. A multi-run backfill therefore keeps
  one I/O-heavy operation active at a time even when Dagster queues many annual
  partition runs.

## 4. Transform

Set-based DuckDB SQL produces:

- one complete row per content version;
- lifecycle events for publication, archive observation, JobStream upsert or
  snapshot observation, exact removal, and estimated historical scheduled end;
- one row per versioned must-have/nice-to-have taxonomy requirement;
- one row per application contact and job version, while scalar application
  details and employer email/phone remain on the complete version row.

Sparse JobStream removals produce an event but never a blank content version.
Timestamps without offsets are interpreted in `Europe/Stockholm`; JobStream
epoch milliseconds and stored timestamps are UTC.

## 5. ClickHouse schema — and DDL deviations

| table | grain |
|---|---|
| `se_platsbanken_job_ad_versions` | one complete source content version |
| `se_platsbanken_job_ad_events` | one lifecycle event |
| `se_platsbanken_job_ad_requirement_versions` | one taxonomy requirement per version |
| `se_platsbanken_job_ad_contact_versions` | one published application contact per version |
| `se_platsbanken_job_ad_active_intervals` | one contiguous active period |
| `company_job_history` | one exact-matched company/ad active period |
| `company_job_current` | active company/ad interval projection |
| `company_hiring_monthly` | one company/month advertised-hiring rollup |

Source history uses `ReplacingMergeTree(ingested_at)` plus stable SHA-256 UIDs.
Each append stages the batch and anti-joins the target `FINAL` by UID. Derived
interval/company tables are rebuilt into stage tables and atomically exchanged.

Historical end precision is explicit:

- `removed_event` and `removed_date` are observed source removals;
- `last_publication_date` and `application_deadline` are estimates;
- `is_end_estimated` prevents downstream readers from presenting an estimate as
  an exact closure.

`version_uid` is the intentional exception to the usual rule against payload
hashes in ClickHouse: exact SCD/version deduplication is the queried behavior of
this table. Raw JSON remains DuckDB/object-storage only.

## 6. Translation

Swedish text is retained in `*_original` columns with the source's
`detected_language`. Bulk translation is deliberately deferred: translating
millions of long historical descriptions would be a large unbounded LLM job and
is not required for Swedish-company hiring signals. A future bounded/on-demand
translation product must use `corpscout.text_translations`; `_en` columns must
not be added to the base history tables. Employer names and workplace addresses
are proper nouns and are never translated.

## 6b. Contacts

JobStream application contacts are retained as versioned job-ad evidence with
their published name, description, email, telephone, and contact type. Scalar
application instructions and employer email/phone are retained on each complete
job version. This makes both current and historical recruiting contacts
queryable without treating them as permanent company attributes.

These records prove only that a contact was published for a particular job ad
at a particular time. They do not prove employment, residence, property
ownership, or a current company role, and they do not automatically feed the
canonical company-person or company-contact tables.

## 7. Currency

Not applicable. `salary_description_original` is descriptive text and the API
does not publish a normalized monetary salary amount/currency pair.

## 8. Scheduling

Four manual jobs are registered:

- `sweden_platsbanken_historical_backfill_job` — one-time 2016+ annual-partition
  archive backfill, one partition per run;
- `sweden_platsbanken_company_jobs_job` — explicit global interval/company
  projection after the required history sources have landed;
- `sweden_platsbanken_jobstream_bootstrap_job` — one-time live snapshot;
- `sweden_platsbanken_jobstream_incremental_job` — subsequent stream windows.

The global company projection is deliberately outside the annual historical
backfill. Rebuilding it for every year would repeatedly scan incomplete global
history. Run it once after the historical partitions and live bootstrap have
landed; the bootstrap and incremental jobs also refresh it after their own
source updates.

No schedule is enabled until migrations are applied and all three jobs have
been manually materialized and reconciled. The incremental job intentionally
does not rerun its snapshot dependency; a fresh snapshot without absence
reconciliation could hide removal transitions.

## 9. Issues found during processing

- Historical `id` is not the live Platsbanken id; `original_id` is the bridge.
- Archive taxonomy fields such as occupation/employment type may be arrays,
  while JobStream v2 publishes objects. Normalization accepts both shapes.
- Removal events can omit employer and job content. They close lifecycle state
  but never overwrite the last complete version.
- Historical archives do not provide edit-by-edit history. Exact version and
  removal tracking begins with the JobStream bootstrap; older end dates may be
  estimated and are labelled accordingly.
- Current snapshots must not be scheduled as ordinary incremental refreshes
  until absence reconciliation exists.
- Whole-history DuckDB normalization repeatedly scanned and rewrote the complete
  multi-gigabyte corpus, saturating worker I/O and making retries all-or-nothing.
  Annual isolated databases bound scan, spill, and retry scope to one source
  period without changing the stable ClickHouse UID semantics.

## 10. Verification

- Unit/contract tests:
  `tests/test_sweden_platsbanken_assets.py`,
  `tests/test_sweden_platsbanken_source.py`,
  `tests/test_sweden_platsbanken_normalize.py`, and
  `tests/test_sweden_platsbanken_clickhouse.py`.
- Definition validation: `uv run dg check defs`.
- Live order: apply migration → all historical year partitions → JobStream
  bootstrap → optional incremental window → final company projection → compare
  source/version/event/interval/company counts.
