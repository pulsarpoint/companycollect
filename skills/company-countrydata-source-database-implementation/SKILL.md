---
name: company-countrydata-source-database-implementation
description: Use when adding Corpscout Postgres storage, sqlc queries, DB store adapters, and DB-backed sync commands for an already implemented Go countrydata source.
---

# Company Countrydata Source Database Implementation

## Purpose

Use this skill to add Corpscout database storage for one existing Go
`corpscout/countrydata` source. The result should persist source metadata,
download/process audit metadata, and source-native raw records into Postgres
while keeping the source package itself independent from scheduler/sqlc.

Use the Finland PRH YTJ implementation as the worked reference:

```text
corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.*.sql
corpscout/database/queries/countrydata_finland_prh_ytj.sql
corpscout/scheduler/internal/countrydata/finland_prhytj_store.go
corpscout/scheduler/cmd/finland-prhytj-sync/
```

Do not use this skill to discover sources, analyze source fields, or implement
the source downloader/parser. Those must already be complete.

## Required Preflight Gate

The task must identify:

```text
country_slug: finland
source_slug: finland_prh_ytj_v3
source_package: prhytj
```

Before writing a plan or editing code, verify all upstream artifacts.

Discovery output from `company-open-data-discovery`:

```text
companies/analysis/{country_slug}/README.md
companies/analysis/{country_slug}/investigation.md
companies/analysis/{country_slug}/source_inventory.json
companies/analysis/{country_slug}/schema_notes.md
companies/analysis/{country_slug}/license_notes.md
```

Data-model output from `company-country-data-model-analysis`:

```text
companies/analysis/{country_slug}/data_model/company_data_analysis.md
companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.json
companies/analysis/{country_slug}/data_model/sources/{source_slug}/source_field_catalog.md
companies/analysis/{country_slug}/data_model/sources/{source_slug}/countrydata_implementation_handoff.json
companies/analysis/{country_slug}/data_model/country_company_profile.schema.json
companies/analysis/{country_slug}/data_model/country_company_profile_mapping.md
companies/analysis/{country_slug}/data_model/common_field_mapping_suggestions.md
```

Go source implementation output from
`company-countrydata-source-implementation`:

```text
corpscout/countrydata/{country_slug}/{source_package}/config.go
corpscout/countrydata/{country_slug}/{source_package}/source.go
corpscout/countrydata/{country_slug}/{source_package}/types.go
corpscout/countrydata/{country_slug}/{source_package}/download.go
corpscout/countrydata/{country_slug}/{source_package}/process.go
corpscout/countrydata/{country_slug}/{source_package}/store.go
corpscout/countrydata/{country_slug}/{source_package}/*_test.go
```

If discovery files are missing, stop and say:

```text
The discovery output is incomplete for {country_slug}. Run the
company-open-data-discovery skill first, then retry database implementation.
Missing files:
- ...
```

If data-model files are missing, stop and say:

```text
The data-model analysis output is incomplete for {country_slug}/{source_slug}.
Run the company-country-data-model-analysis skill first, then retry database
implementation. Missing files:
- ...
```

If the Go source package is missing, stop and say:

```text
The Go countrydata source implementation is missing for
{country_slug}/{source_package}. Run the
company-countrydata-source-implementation skill first, then retry database
implementation. Missing files:
- ...
```

Do not invent source fields, primary identifiers, licenses, source URLs, or
mapping rules to bypass this gate.

## Architecture Rules

- Keep `corpscout/countrydata` free of Corpscout DB, scheduler, and sqlc
  imports.
- Put database storage in `corpscout/database` and
  `corpscout/scheduler/internal/countrydata`.
- Use source-specific concrete code. Do not add generic registries, facades, or
  local interfaces unless there are multiple real implementations at that
  boundary.
- Use `log/slog` only at command/worker boundaries. Lower layers should wrap and
  return errors with `github.com/cockroachdb/errors`.
- Never log secrets, tokens, cookies, API keys, or sensitive raw request bodies.
- Use sqlc-generated params and row types directly where practical.
- Use real DB boundaries for tests when possible; optional DB round-trip tests
  should be gated by `CORPSCOUT_TEST_DATABASE_URL`.

## Naming

Use these names consistently:

```text
source_slug:      exact global source identity, e.g. finland_prh_ytj_v3
source_package:   Go package folder, e.g. prhytj
schema_name:      countrydata_{country_slug}_{source_package_or_short_source}
query_file:       corpscout/database/queries/{schema_name}.sql
sync_command:     corpscout/scheduler/cmd/{country_slug}-{source_package}-sync
```

Prefer a concise schema name without API version suffix unless the version is
needed to avoid incompatible storage. Keep the exact versioned source identity
inside `sources.source_slug` and `data_sources.name`.

Example:

```text
source_slug:    finland_prh_ytj_v3
source_package: prhytj
schema_name:    countrydata_finland_prh_ytj
sync_command:   finland-prhytj-sync
```

## Database Shape

Create one source-owned schema:

```text
countrydata_{country}_{source}/
  sources
  download_runs
  raw_records
```

`sources` stores stable source configuration and source-level state:

- `source_slug TEXT UNIQUE NOT NULL`
- `source_name TEXT NOT NULL`
- `source_type TEXT NOT NULL`
- `base_url` or source URL fields
- `country_iso2`
- `supports_incremental BOOLEAN NOT NULL DEFAULT false`
- `enabled`
- last started/success/failed timestamps
- last snapshot path/hash or last source marker
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`

`download_runs` stores one audit row per download/API pull:

- `source_id`
- `status`
- base/source URL
- snapshot path and full snapshot hash when applicable
- started/finished/duration
- byte, page/file, record, process, stored, and decode-error counts
- source-specific markers such as first page, last page, ETag, version, or file
  name
- `metadata JSONB`

For non-bulk APIs, keep the same audit concept but rename columns only when the
source makes the generic name misleading. Store whether incremental/diff pulls
are supported in `sources.supports_incremental` and source metadata.

`raw_records` stores versioned source-native company rows:

- `source_id`
- nullable `download_run_id`
- `source_native_id TEXT NOT NULL`
- one source-specific primary identifier column, e.g. `business_id`, `cvr_number`
- useful query columns from the source profile: legal name, status, legal form,
  website, source update date, country
- `raw_payload JSONB NOT NULL`
- `payload_hash TEXT NOT NULL`
- `is_current BOOLEAN NOT NULL DEFAULT true`
- first/last seen timestamps
- `metadata JSONB`

Required constraints:

```sql
UNIQUE ({primary_identifier}, payload_hash)
CREATE UNIQUE INDEX idx_{schema}_raw_records_current_{primary_identifier}
  ON {schema}.raw_records ({primary_identifier})
  WHERE is_current;
CHECK (jsonb_typeof(raw_payload) = 'object')
CHECK (jsonb_typeof(metadata) = 'object')
```

Also insert/update the global `data_sources` row with:

- `name = source_slug`
- `input_table_name = '{schema}.raw_records'`
- `country_id` from `countries.iso_alpha2`
- capabilities from the data-model analysis
- config JSON with source URLs, docs, protocol, source schema/table, fields,
  auth env names, and notes

Grant read access consistently:

```sql
GRANT USAGE ON SCHEMA {schema} TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO corpscout_anon;
```

## Implementation Workflow

### 1. Read Handoff And Source Code

Read `countrydata_implementation_handoff.json`,
`source_field_catalog.json`, and the implemented Go source package. Extract:

- source identity, package name, env prefix, source URLs, docs, license
- source access type and whether diff/incremental pulls exist
- primary source identifier and join keys
- `CompanyRecord` or equivalent source-native record type
- derived profile mapping such as `ToProfile()`
- raw payload/hash fields and whether `Process` already fills them

If `Process` does not preserve exact raw payload and per-row hash, add tests and
implement that first in the countrydata source package.

### 2. Write Tests First

Required tests:

- process test proving raw payload and per-row hash reach the store callback
- migration shape test for schema, tables, constraints, indexes, source slug,
  global `data_sources` input table, and down migration
- sqlc generation/compile check
- DB store conversion test from source record to sqlc params
- DB store nil/missing pool returns classified state error
- optional DB round-trip test gated by `CORPSCOUT_TEST_DATABASE_URL`; verify
  download metadata, raw insert, unchanged upsert, changed-row supersession, and
  process stats
- importer adapter test proving the typed store callback is called
- sync command flag/env parsing tests

Use real fixtures from legally usable source data where possible, including
messy records and changed versions of the same source identifier.

### 3. Add Migration

Use the next migration number under `corpscout/database/migrations`.

Create:

```text
NNNNNN_{source}_countrydata_storage.up.sql
NNNNNN_{source}_countrydata_storage.down.sql
```

Keep the down migration owned and simple:

```sql
DELETE FROM data_sources WHERE name = '{source_slug}';
DROP SCHEMA IF EXISTS {schema} CASCADE;
```

### 4. Add sqlc Queries

Create:

```text
corpscout/database/queries/{schema}.sql
```

Required query operations:

- upsert source row, returning source ID
- record download/API run, returning run ID
- update process stats on the latest run
- get current raw record by primary identifier
- supersede current raw record when hash changes
- upsert raw record by `{primary_identifier}, payload_hash`

Run:

```bash
cd corpscout/database
sqlc generate
```

Do not hand-edit generated sqlc files.

### 5. Add Scheduler DB Store

Create:

```text
corpscout/scheduler/internal/countrydata/{country_slug}_{source_package}_store.go
corpscout/scheduler/internal/countrydata/{country_slug}_{source_package}_store_test.go
```

The store should:

- implement `countryimport.MetadataStore`
- expose `StoreCompanies` or a source-specific typed store method matching the
  source record type
- upsert the `sources` row before first metadata or raw write
- record download audit metadata in `SaveDownload`
- update process stats in `SaveProcess` when a run is known; no-op if called
  without a DB-backed download run
- convert source records into sqlc params using the source `ToProfile()` mapping
- validate primary ID, raw payload object, and payload hash before DB writes
- supersede current rows when the current hash differs
- return `countryimport.StoreResult`
- wrap lower-layer errors with source context and classified source errors at
  the boundary

Keep helper names source-prefixed if they might collide in
`scheduler/internal/countrydata`.

### 6. Wire Scheduler Adapter

Modify:

```text
corpscout/scheduler/internal/countrydata/{country_slug}_{source_package}.go
```

Add a typed store function field to the import input and assign it to the
source before `Download`/`Process`. Also pass through request timeout and user
agent if the source supports them.

### 7. Add DB-Backed Sync Command

Create:

```text
corpscout/scheduler/cmd/{country_slug}-{source_package}-sync/
```

The command should:

- load optional `--env`/`--env-file`
- resolve `DATABASE_URL` or `CORPSCOUT_DATABASE_URL` after env loading
- resolve source env defaults from the source `ConfigFromEnv()`
- connect with `pgxpool`
- check `{schema}.raw_records` exists and tell the user to run migrations if not
- construct the DB store and scheduler importer
- run download/process/store
- print a JSON summary
- log boundary failures once with `slog`

Example:

```bash
cd corpscout/scheduler
DATABASE_URL="$DATABASE_URL" GOWORK=off go run ./cmd/{country}-{source}-sync \
  --env ../.env \
  --data-dir ../data/countrydata/{country}/{source_package} \
  --max-pages 2 \
  --chunk-size 100
```

Omit `--max-pages` only for a full sync.

### 8. Docker Build Check

If `scheduler/go.mod` uses:

```text
replace github.com/pulsarpoint/corpscout/countrydata => ../countrydata
```

ensure scheduler Docker builds from the `corpscout` context and copies both
`scheduler/` and `countrydata/`. Otherwise `go mod download` will fail because
`../countrydata` is missing inside the image.

## Verification

Run targeted checks:

```bash
cd corpscout/countrydata
GOWORK=off go test ./{country_slug}/{source_package} -count=1

cd ../scheduler
GOWORK=off go test ./cmd/{country}-{source}-sync ./internal/countrydata ./internal/db/gen -count=1
GOWORK=off go test ./internal/db -run Test{Source}CountrydataStorage -count=1 -v
```

If a broader package test fails in unrelated legacy code, report it separately
with the failing test name and file/query it references.

When DB env is available:

```bash
cd corpscout
make migrate-test-up
cd scheduler
CORPSCOUT_TEST_DATABASE_URL="$CORPSCOUT_TEST_DATABASE_URL" \
  GOWORK=off go test ./internal/countrydata -run Test{Source}DBStore -count=1 -v
```

For a real bounded sync:

```bash
cd corpscout
make migrate-up
cd scheduler
DATABASE_URL="$DATABASE_URL" GOWORK=off go run ./cmd/{country}-{source}-sync \
  --env ../.env \
  --data-dir ../data/countrydata/{country}/{source_package} \
  --max-pages 2 \
  --chunk-size 100
```

## Common Mistakes

- Running scheduler command tests from `corpscout/countrydata`; run scheduler
  paths from `corpscout/scheduler`.
- Passing `--env ../.env` from a worktree that does not have `corpscout/.env`;
  use the main checkout or an absolute env file path.
- Storing only derived fields and losing the full source JSONB payload.
- Making `raw_records` unique only by source ID; version by identifier plus
  payload hash.
- Treating all API sources as incremental; record incremental support explicitly.
- Logging source errors in both store and command. Store wraps; command logs once.
- Adding generic source database abstractions before two or more real sources
  prove the shape.
