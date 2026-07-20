# Sweden Financial Raw Archive Pipeline Design

## Source

Sweden annual reports are published by Bolagsverket as bulk ZIP archives under the
public S3-style listing endpoint:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler?prefix=arsredovisningar/&delimiter=\
```

The listing returns object keys such as `arsredovisningar/2020/08_2.zip`. The
source denies year-specific listing prefixes, so the resource lists the allowed
root prefix `arsredovisningar/`, starts year scans with a marker such as
`arsredovisningar/2026/`, and filters returned keys client-side. The download URL
is the listing host plus the upstream key:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/2020/08_2.zip
```

One downloaded outer ZIP contains many nested ZIPs. Each nested ZIP name carries
the company id and report-period end date, while the XHTML filename inside the
nested ZIP is a UUID. Because the outer ZIP is the upstream audit artifact, the
pipeline stores the outer ZIP files first, then extracts each nested report XHTML
into deterministic object keys and catalogs them in DuckDB.

## Resource

`SwedenFinancialReportsResource` owns the source-specific behavior:

- list archives from the XML listing endpoint, including `NextMarker` pagination;
- scan by year using a marker and client-side key filtering;
- build download URLs from upstream keys;
- derive deterministic object keys from year, archive name, and upstream
  `LastModified`;
- skip existing archive objects before issuing the archive `GET`;
- stream missing ZIP downloads to a temporary file;
- upload missing ZIPs through the shared `ObjectStoreResource`;
- emit changed/unchanged counts and sample S3 keys in materialization metadata.

This is a concrete resource rather than a generic downloader because the source
uses a specific S3 XML listing endpoint, a specific download URL rewrite, and
source-specific object-key conventions.

## Asset

`sweden_financial_backfill_raw_archives_s3` materializes the raw backfill archive
layer. It uses static year partitions `2020` through `2026`.

`sweden_financial_current_raw_archives_s3` materializes the current refresh
archive layer. It uses 7-day date partitions from `2026-07-04` through the end of
2026 and scans upstream archive year `2026` on each run.

It writes to bucket:

```text
source-sweden-financial
```

Archive objects use deterministic keys:

```text
sweden_financial/raw_archives/
  year=2020/
  archive=08_2.zip/
  source_last_modified=2025-02-07T09-13-53.713Z/
  archive.zip
```

If the same upstream key and `LastModified` timestamp already exists in object
storage, materialization reuses it and does not download the ZIP again.

Each raw archive asset also writes an archive sync manifest to object storage:

```text
sweden_financial/raw_archive_sync_manifests/
  sync_kind=backfill/
  load_partition_key=2026/
  manifest.json
```

The manifest records every archive observed in that raw materialization,
including upstream key, `LastModified`, ETag, object-storage key, and whether the
ZIP was downloaded or reused. Raw archive assets do not use DuckDB and do not run
in the `sweden_financial_duckdb` pool.

`sweden_financial_backfill_report_xhtml_catalog_duckdb` materializes the
extracted XHTML catalog for a full backfill year partition. It reads raw archive
objects from:

```text
sweden_financial/raw_archives/year=<partition_year>/
```

For each nested report ZIP, the asset writes the report body to:

```text
sweden_financial/report_xhtml/
  year=2020/
  company_id=5561234567/
  report_period_end=2020-12-31/
  source_archive_hash=<sha256-prefix>/
  source_archive=08_2.zip/
  nested_zip=<nested-zip-name>/
  report.xhtml
```

`sweden_financial_current_report_xhtml_catalog_duckdb` materializes only changed
current ZIPs for the same Dagster run. It reads the raw asset's archive sync
manifest, writes `sweden_financial.archive_sync_catalog` in DuckDB, reads changed
archive keys where `downloaded = true`, parses those ZIPs, and replaces catalog
rows only for the affected archive names.

The DuckDB table `sweden_financial.report_xhtml_catalog` stores one row per
extracted report XHTML with the partition year, company id, report-period end,
source archive object key, nested ZIP name, report object key, content length,
content hash, and `source_run_id`.

The DuckDB table `sweden_financial.archive_sync_catalog` stores one row per
archive sync manifest consumed by the catalog assets, including the upstream
key, `LastModified`, ETag, source size, object-storage key, and whether the
archive was downloaded or reused.

Catalog DuckDB files are partitioned by archive year. The file path is:

```text
data/sweden_financial/sweden_financial_source_<year>.duckdb
```

Backfill partitions write their own year file. Current refresh partitions write
the active archive-year file, currently `sweden_financial_source_2026.duckdb`.

`sweden_financial_backfill_parsed_reports_duckdb` reads the XHTML catalog rows
for its backfill year after XHTML extraction, loads each XHTML body from object
storage, parses inline XBRL facts, and replaces parsed rows in the same year
DuckDB file.

`sweden_financial_current_parsed_reports_duckdb` reads only catalog rows written
by the current refresh run and replaces parsed rows for those changed archive
names in `sweden_financial_source_2026.duckdb`.

The parsed DuckDB tables are:

- `sweden_financial.reports` - one row per parsed XHTML report, aligned with the
  ClickHouse `corpscout.se_financial_reports` shape.
- `sweden_financial.facts` - one row per parsed inline XBRL fact, aligned with
  the ClickHouse `corpscout.se_financial_facts` shape.
- `sweden_financial.parse_errors` - one row per XHTML document that failed
  parsing, so a bad report does not block the rest of the partition.

The ClickHouse exports are **partition-scoped incremental upserts**, never
full-table replaces (architecture decision after the 2026-07-19 incident, where
a host holding only one year's DuckDB file full-replaced the seven-year
`se_financial_facts` table — see the incident entry in the repo SDD ledger).
Four partitioned assets mirror the parse assets' partition layout:
`sweden_financial_backfill_reports_clickhouse` /
`sweden_financial_current_reports_clickhouse` and their facts twins. Each
partition run resolves its own archive scope from the LOCAL DuckDB catalog
(fail-loud `ValueError` when the local file or scope is missing/empty), deletes
exactly its own scope in ClickHouse — reports by `source_archive_key` (small
Array param), facts by `statement_key` staged through a per-run Memory table so
hundreds of thousands of keys travel as data blocks, never query text — with
`mutations_sync = 1` (skipped entirely when the pre-count is 0, the steady-state
new-archive case), then inserts that partition's rows with explicit columns. A
run structurally cannot touch rows outside its own partition, so no host ever
needs the full history locally. The source is append-shaped (immutable weekly
archives), which is what makes delete-own-scope + insert exact.

Operational note: a backfill `2026` export and the weekly current writer share
the 2026 DuckDB file; running both concurrently fails loudly on the DuckDB
cross-process lock — sequence them.

`corpscout.se_financial_facts` is the lossless long-form layer: every parsed
inline-XBRL numeric, date, text, context, unit, currency, and dimensional value
is retained. `corpscout.se_financial_facts_with_source` joins each fact to its
filing provenance and exposes both the official Bolagsverket outer-archive URL
and the exact extracted XHTML URI under
`s3://source-sweden-financial/...`. This keeps every unmapped taxonomy concept
queryable and traceable without duplicating long document paths across hundreds
of millions of physical fact rows.

`sweden_financial_metrics_clickhouse` builds one canonical row per filing in
`corpscout.se_financial_metrics`. It selects undimensioned current-period facts,
prefers the highest declared XBRL precision when a document repeats rounded and
exact values, maps the standard Swedish concepts, derives total liabilities from
the balance-sheet equation, and converts every monetary metric from SEK to USD
using the shared `corpscout.exchange_rates` history. The metrics row includes the
official archive URL, exact XHTML S3 URI, taxonomy entrypoint, mapping version,
source/mapped/unmapped fact counts, and native and USD values. The full fact
table remains the comprehensive representation for concepts that do not belong
in the stable cross-country metric projection.

## Job And Schedule

`sweden_financial_backfill_job` selects both
`sweden_financial_backfill_raw_archives_s3` and
`sweden_financial_backfill_report_xhtml_catalog_duckdb`, then
`sweden_financial_backfill_parsed_reports_duckdb`. Backfill should
materialize the 2020-2026 partitions.

`sweden_financial_current_year_job` selects the full weekly chain as separate
non-partitioned assets in one run: `sweden_financial_current_raw_archives_s3`,
`sweden_financial_current_report_xhtml_catalog_duckdb`,
`sweden_financial_current_parsed_reports_duckdb`, then the
`sweden_financial_current_reports_clickhouse` /
`sweden_financial_current_facts_clickhouse` export pair.

The weekly chain is deliberately unpartitioned (2026-07-20 design): weekly
partition identities existed only to give each week's export a bookkeeping
scope (`archive_sync_catalog.load_partition_key`), and that bookkeeping was
exactly what a yearly re-parse destroyed (the 2026-07-18 incident -- the
backfill replaces the entire year DuckDB file). Instead, the weekly exports
are **reconcilers**: they diff the local year file against ClickHouse per
`source_archive_key` (row counts on both sides; facts counted through the
`statement_key` -> reports join) and upsert exactly the missing/mismatched
archives. No state can be lost because no state is kept.

**Order-independence invariant:** the weekly job and the yearly backfill
(parse + export) may be materialized in ANY order, any number of times, and
every run succeeds -- yearly-after-weekly is an idempotent superset upsert;
weekly-after-yearly reconciles to a no-op (metadata `skipped_reason`) or
fills exactly the remaining gap. The only remaining export error is the
corruption guard: a local year file with zero report rows refuses to export.

`sweden_financial_current_year_weekly` runs at `45 6 * * 6` in
`Europe/Belgrade` and is enabled by default. Each weekly run discovers
upstream `LastModified` changes, downloads only changed archives, parses
them, and reconciles ClickHouse.

**Deploy note (one-time, 2026-07-20):** before deploying the de-partitioned
current chain, cancel any in-flight/queued backfills or runs targeting the
old weekly partitions (`bulk_actions` / `run_tags key='dagster/backfill'`);
a queued partition run that starts after the partitions are gone fails with
`RUN_EXCEPTION` and can leak its `sweden_financial_current_2026_duckdb` pool
slot (see CLAUDE.md Troubleshooting). Historical weekly-partition
materializations remain in the event log as orphans; that is cosmetic.

The ClickHouse layer is three jobs:
`sweden_financial_backfill_clickhouse_job` (the backfill-partitioned
reports/facts export pair), `sweden_financial_current_clickhouse_job` (the
current-weekly export pair), and `sweden_financial_clickhouse_job` (the derived
wave: `sweden_financial_metrics_clickhouse`, `se_financial_history_clickhouse`,
`se_company_officers_clickhouse`, `se_company_audits_clickhouse` — full
rebuilds from ClickHouse facts, which stays correct for derivations and keeps
the shrink guard). Run exports after the matching parsed DuckDB partitions are
materialized, then the derived wave.
