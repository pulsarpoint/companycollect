# Corpscout-Owned ClickHouse Ingestion Design

## Purpose

Company source packages should stay focused on collecting source data and
exporting source-preserving Parquet files. Corpscout should own how those
Parquet files become ClickHouse tables, migrations, imports, and queryable
storage.

This replaces the earlier direction where `companies/companysource` embedded
ClickHouse table config and generated Corpscout migrations directly.

## Decision

Use this ownership boundary:

```text
companies/companysource
  download source data
  export source-preserving Parquet
  write run manifest

corpscout
  inspect Parquet schemas
  define ClickHouse table naming and ordering policy
  generate desired ClickHouse schema SQL
  use Atlas to diff desired schema into migrations
  apply migrations to remote ClickHouse
  import Parquet into ClickHouse
  own future search/detail projections and APIs
```

ClickHouse schema generation is Parquet-first. The real exported Parquet files
are the input used to infer columns and ClickHouse types. Source Go structs are
not the schema authority.

## Rationale

ClickHouse is part of Corpscout's storage and query layer, not part of source
collection. Corpscout owns the remote database, migration numbering, table
naming policy, Atlas integration, import policy, and future search/detail APIs.
Keeping those decisions in Corpscout avoids leaking application storage concerns
into every source package.

Parquet-first schema generation also catches drift. If a source export changes,
Corpscout sees the exact files it will import and can generate or reject a
schema migration before data is loaded.

## Source Run Contract

Each source run folder produced by `companysource` should be self-contained:

```text
runs/<run-id>/
  manifest.json
  downloaded source file(s)
  *.parquet
```

The manifest should describe the exported artifacts without making ClickHouse
decisions:

```json
{
  "country": "finland",
  "source": "prhytj",
  "run_id": "20260608T201348Z-prhytj",
  "exported_at": "2026-06-08T20:13:48Z",
  "files": [
    {
      "path": "companies.parquet",
      "kind": "parquet",
      "rows": 1000,
      "sha256": "..."
    }
  ]
}
```

`companysource` should not write ClickHouse migrations, ClickHouse table YAML,
or target database configuration.

## Corpscout Configuration

Corpscout keeps small source storage config files:

```text
corpscout/clickhouse/sources/
  finland/prhytj.yaml
  united_states/secedgar.yaml
```

Example:

```yaml
country: finland
source: prhytj
database: corpscout_sources
table_prefix: fi_prhytj
tables:
  companies:
    parquet: companies.parquet
    order_by: [business_id, source_run_id]
  company_names:
    parquet: company_names.parquet
    order_by: [business_id, source_position, source_item_hash]
```

This config only captures Corpscout storage policy:

- target database
- table prefix
- Parquet-to-table mapping when convention is not enough
- `ORDER BY`
- optional engine and partition overrides
- injected ingestion columns such as `source_export_id` and `ingested_at`

Columns and ClickHouse column types are derived from Parquet inspection.

## Schema Generation

Corpscout should build a canonical desired schema from:

- source config
- run manifest
- actual Parquet files
- default ClickHouse table policy

The default table name is:

```text
<table_prefix>_<parquet file base name>
```

For example:

```text
companies.parquet      -> fi_prhytj_companies
company_names.parquet  -> fi_prhytj_company_names
```

The generated desired schema should be deterministic:

- sort source configs by country/source
- sort tables by table name
- preserve Parquet column order inside each table
- sort injected columns by name
- render stable SQL formatting

Generation should fail when:

- a configured Parquet file is missing
- an exported Parquet file is not configured and convention mode is disabled
- `ORDER BY` references a missing column
- an injected column duplicates a Parquet column
- Parquet inspection returns no columns

## Atlas Migration Flow

Atlas should own schema diffing. Corpscout should not maintain a custom
ClickHouse diff engine.

Flow:

```text
run folder
  -> inspect Parquet
  -> generate desired_schema.sql
  -> atlas migrate diff
  -> write versioned migration under corpscout/clickhouse/migrations
  -> golang-migrate applies migrations to remote ClickHouse
```

Corpscout continues using `golang-migrate` to apply migrations because it is
already wired into the Makefile and remote ClickHouse workflow.

Migration versions are schema versions, not export-run versions. Re-exporting a
source with the same Parquet schema should not create a new migration.

## Initial Migration Policy

For now, automatic migrations should support:

- new source tables
- additive columns
- new injected columns

Corpscout should reject automatic migration generation for:

- removed columns
- renamed columns
- changed column types
- changed `ORDER BY`
- changed primary table identity policy

Those changes require a manual migration because they can affect already
imported ClickHouse data and query semantics.

## Import Flow

After migrations are applied, Corpscout imports each Parquet file into its
configured ClickHouse table.

Each import injects:

```text
source_export_id UUID
ingested_at DateTime64(3, 'UTC')
```

Import should be idempotent at the run level. The first implementation can
truncate and reload explicitly selected tables for test runs, but production
imports should record source export IDs and avoid silently duplicating the same
run.

## Removing Premature Projection Database

The active ClickHouse migrations should only create `corpscout_sources` for now.
`corpscout_projection` should be removed until concrete projection tables exist.

Future projection databases or tables should be introduced together with actual
query use cases, such as company search or company detail pages. They should not
be created as placeholders.

## CLI and Make Targets

Start with a small Corpscout-owned Go tool and Make targets:

```text
corpscout/clickhouse/cmd/corpscout-clickhouse/
```

Commands:

```bash
corpscout-clickhouse inspect-run --country finland --source prhytj --run-dir ...
corpscout-clickhouse generate-schema --country finland --source prhytj --run-dir ... --out ...
corpscout-clickhouse diff-migration --country finland --source prhytj --run-dir ...
corpscout-clickhouse import-run --country finland --source prhytj --run-dir ...
```

Make targets can wrap these commands:

```bash
make clickhouse-generate-schema COUNTRY=finland SOURCE=prhytj RUN_DIR=...
make clickhouse-diff-source COUNTRY=finland SOURCE=prhytj RUN_DIR=...
make clickhouse-migrate-up
make clickhouse-import-run COUNTRY=finland SOURCE=prhytj RUN_DIR=...
```

## Decommissioning Companysource ClickHouse Logic

After the Corpscout tool works for `finland/prhytj`, remove these responsibilities
from `companysource`:

- embedded `clickhouse.yaml` files
- source-specific `GenerateClickHouseMigration`
- source-specific `ImportClickHouse`
- shared ClickHouse migration/import packages under `companysource/internal`

The companysource CLI should retain:

```bash
companysource download
companysource export-parquet
companysource list-sources
companysource status
```

## First Implementation Slice

Use `finland/prhytj` as the first end-to-end source:

1. Remove `corpscout_projection` from ClickHouse migrations.
2. Add `corpscout/clickhouse/sources/finland/prhytj.yaml`.
3. Add Parquet inspection and desired schema generation in Corpscout.
4. Generate the same Finland PRH YTJ table definitions that exist today.
5. Wire Atlas migration diffing.
6. Move ClickHouse import-run into Corpscout.
7. Remove Finland ClickHouse config and migration/import methods from
   `companysource`.

After that, apply the same path to the existing United States sources.

## Testing

Unit tests should cover:

- source config parsing
- table naming conventions
- Parquet inspection result conversion into canonical table definitions
- stable desired SQL rendering
- rejection of missing `ORDER BY` columns
- rejection of duplicate injected columns
- import command construction for remote ClickHouse

Integration tests can use tiny Parquet fixtures and `clickhouse-local` for
schema inspection. Remote ClickHouse tests should stay opt-in because they
depend on deployment-specific credentials and network access.

## Open Direction

The system intentionally does not define unified company search/detail
projections yet. We should import and study more source schemas first. The first
goal is reliable source-specific ingestion into ClickHouse with no data loss
from the source exports.
