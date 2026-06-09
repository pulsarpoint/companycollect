# Corpscout-Owned ClickHouse Ingestion Design

## Purpose

Corpscout should own the full source-to-ClickHouse path: source modules,
source input parsing, manual ClickHouse migrations, direct imports, scheduling,
and APIs. Keeping these pieces in one application gives us one place to
understand the exact source format and one place to evolve storage when the
source changes.

This replaces the earlier direction where `companies/companysource` exported
Parquet and Corpscout generated ClickHouse schema from those files.

## Decision

Move active company source modules into Corpscout and remove the required
Parquet stage.

```text
corpscout source module
  download raw source snapshot
  parse raw input with source-specific Go structs
  map parsed records to ClickHouse insert rows
  stream/chunk mapped rows directly into ClickHouse

corpscout clickhouse migrations
  keep manual source-specific CREATE/ALTER SQL
  keep explicit down migrations where safe
  apply migrations with golang-migrate

corpscout scheduler/API
  trigger downloads/imports
  track runs and import state
  query ClickHouse for source facts
```

Raw source files and manifests remain required. Parquet becomes optional debug or
offline-export tooling, not the primary ingestion contract.

## Rationale

This is the simplest architecture that still preserves the controls we need:

- source code, parser structs, ClickHouse schema, and importer live together
- schema changes are explicit Go changes reviewed with the source code
- migrations are explicit SQL files, so there is no hidden schema generator
- imports do not write and reread large Parquet intermediates
- raw downloaded files remain replayable for debugging and reimport
- Corpscout owns storage and query behavior without a separate companysource
  lifecycle

Atlas, sqlc, and custom ClickHouse schema generation are not needed in the first
implementation.

## Source Module Contract

Each source module should live under Corpscout:

```text
corpscout/scheduler/internal/companysources/
  finland/prhytj/
  unitedstates/secedgar/
  unitedstates/irseobmf/
  unitedstates/coloradoentities/
```

Each source module owns:

- config and source URLs
- raw download logic
- raw input structs and parser
- source-specific row mapping
- table names and insert column lists matching manual migrations
- chunked import implementation
- focused parser/import tests

The source module should expose a concrete type, not an abstraction-heavy
service layer:

```go
type Source struct {
    cfg Config
    httpClient *http.Client
}

func (s *Source) Key() companysources.Key
func (s *Source) Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
func (s *Source) Import(ctx context.Context, opts ImportOptions, writer *clickhouse.Writer) (ImportResult, error)
```

## Raw Run Contract

Each run folder contains raw source artifacts and a manifest:

```text
corpscout/data/sources/<country>/<source>/runs/<run-id>/
  manifest.json
  raw/
    source-file-or-pages
```

The manifest records replay metadata:

```json
{
  "country": "finland",
  "source": "prhytj",
  "run_id": "20260608T201348Z-prhytj",
  "downloaded_at": "2026-06-08T20:13:48Z",
  "files": [
    {
      "path": "raw/companies.jsonl",
      "kind": "jsonl",
      "rows": 1000,
      "sha256": "..."
    }
  ]
}
```

The raw files are the durable replay boundary. Importing to ClickHouse should be
repeatable from the raw folder without calling the external source again.

## Input Parsing

Each source should define Go structs matching the source input format.

Example:

```go
type Company struct {
    BusinessID string    `json:"businessId"`
    Names      []Name    `json:"names"`
    Addresses  []Address `json:"addresses"`
}
```

Parsers should stream where possible:

```text
raw file/page -> decode record -> map to row batch -> insert batch -> continue
```

The importer should not load a full country source into memory. Batch size should
be configurable per source, with safe defaults.

## ClickHouse Migrations

ClickHouse schema is defined by manual migration SQL under:

```text
corpscout/clickhouse/migrations/
```

Each source adds explicit migrations:

```text
000002_create_finland_prhytj_tables.up.sql
000002_create_finland_prhytj_tables.down.sql
000003_create_us_secedgar_tables.up.sql
000003_create_us_secedgar_tables.down.sql
```

Migration SQL is the storage source of truth. The source package's importer must
match those table and column names.

For a new source, create the initial `CREATE TABLE` migration manually. For a
source schema change, create a manual `ALTER TABLE` migration. This keeps schema
review explicit and avoids premature generator complexity.

Migrations should be small and source-specific where possible. Shared database
creation remains in `000001_create_databases`.

## Import Flow

The import path writes direct batches into ClickHouse:

```text
raw snapshot
  -> source parser
  -> source mapper
  -> ClickHouse row batches
  -> remote ClickHouse
```

The shared ClickHouse writer should provide:

- native protocol connection configuration
- batch insert helpers
- configurable batch size
- source export/run ID injection
- import idempotency checks
- structured import metrics

Production import should record source export IDs and avoid silently duplicating
the same run. Development commands may allow explicit `--truncate` for selected
tables.

## Run Discovery And Incremental Processing

Corpscout should support targeted and bulk operation. With more than 200 sources,
commands must avoid doing heavy work for unchanged sources.

Targeted commands:

```bash
corpscout-source download --country finland --source prhytj
corpscout-source import-run --country finland --source prhytj --run-dir ...
```

Bulk commands:

```bash
corpscout-source import-runs --runs-root corpscout/data/sources --changed-only
```

Corpscout should maintain:

```text
corpscout/clickhouse/run-index.lock.yaml
```

The run index records:

- country/source
- latest selected run ID
- manifest hash
- raw file hashes
- latest imported source export ID
- import status

Processing rules:

- If a run manifest and raw file hashes are unchanged, skip reimport unless
  forced.
- If multiple runs exist for one source, choose the latest completed manifest by
  default, with `--run-id` available for pinning a specific run.
- `--force-rescan` recalculates hashes and import eligibility.

## Removing Premature Projection Database

The active ClickHouse migrations should only create `corpscout_sources` for now.
`corpscout_projection` should be removed until concrete projection tables exist.

Future projection databases or tables should be introduced together with actual
query use cases, such as company search or company detail pages. They should not
be created as placeholders.

## CLI And Make Targets

Start with a Corpscout-owned command:

```text
corpscout/scheduler/cmd/corpscout-source/
```

Commands:

```bash
corpscout-source list-sources
corpscout-source download --country finland --source prhytj
corpscout-source migrate-up
corpscout-source import-run --country finland --source prhytj --run-dir ...
corpscout-source import-runs --runs-root corpscout/data/sources --changed-only
```

Make targets can wrap these commands:

```bash
make source-list
make source-download COUNTRY=finland SOURCE=prhytj
make clickhouse-migrate-up
make source-import-run COUNTRY=finland SOURCE=prhytj RUN_DIR=...
make source-import-changed
```

## Decommissioning Companysource

After the Corpscout path works for `finland/prhytj`, migrate the remaining
active source modules into Corpscout and remove the separate companysource
module.

Remove from `companies/companysource`:

- standalone binary
- embedded ClickHouse YAML
- Parquet-first ClickHouse schema generation
- ClickHouse import commands
- source packages after they move into Corpscout

Keep or archive old Parquet exports as historical artifacts only. They should
not define the new ingestion contract.

## First Implementation Slice

Use `finland/prhytj` as the first end-to-end source:

1. Remove `corpscout_projection` from ClickHouse migrations.
2. Keep or rewrite the Finland PRH YTJ ClickHouse migration manually in
   `corpscout/clickhouse/migrations`.
3. Move Finland PRH YTJ source code into Corpscout.
4. Replace Parquet export/import with raw download plus direct chunked import.
5. Import one existing raw Finland run without downloading again.
6. Remove Finland ClickHouse generation/import code from `companysource`.

After that, migrate the existing United States sources.

## Testing

Unit tests should cover:

- source registry lookup
- source input parser behavior using real small fixtures
- importer column lists matching expected migration table columns
- ClickHouse writer command/connection construction
- run-root discovery and latest-run selection
- unchanged manifest skipping

Integration tests can use tiny raw fixtures and an opt-in local/remote
ClickHouse connection. Remote ClickHouse tests should stay opt-in because they
depend on deployment-specific credentials and network access.

## Open Direction

The system intentionally does not define unified company search/detail
projections yet. We should import and study more source schemas first. The first
goal is reliable source-specific ingestion into ClickHouse with no data loss
from the source snapshots.
