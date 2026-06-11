# Source Module Architecture Design

## Purpose

CorpScout needs to scale from a few company sources to hundreds of country and
source combinations. The current scheduler has useful pieces already, but source
behavior is split across global source catalog JSON, generic Temporal actions,
ClickHouse migrations in one folder, source-specific Go packages, and a mostly
generic UI.

This design proposes a source-module architecture where each source owns its
static metadata, download/import behavior, source-specific ClickHouse schema,
explorer behavior, and optional UI. Central Corpscout code should stay small and
boring: discover modules, sync metadata to Postgres, register generic Temporal
workflows, route API calls, run source migrations, and record execution state.

## Goals

- Make each company source understandable as an independent package.
- Support 400+ sources without one giant scheduler action file or UI component.
- Keep static source truth in versioned files, not editable database rows.
- Keep Postgres focused on metadata, source catalog projections, task logs, and
  Temporal workflow/run references.
- Keep ClickHouse as the primary storage for source company, financial, contact,
  and explorer data.
- Keep RustFS as the artifact store for downloaded files, with source bucket
  metadata synced from source definitions.
- Allow source-specific UI where sources differ naturally.
- Preserve generic routes and workflows where they reduce repeated boilerplate.

## Non-Goals

- This does not define a universal company schema across all sources.
- This does not move all existing sources in one large rewrite.
- This does not make source definitions editable from the UI.
- This does not require every source to expose an explorer immediately.
- This does not require every source to register unique Temporal workflow names.

## Current Pressure Points

The existing code already has source packages under:

```text
scheduler/internal/companysources/finland/prhytj
scheduler/internal/companysources/finland/prhxbrl
scheduler/internal/companysources/unitedstates/coloradoentities
scheduler/internal/companysources/unitedstates/irseobmf
scheduler/internal/companysources/unitedstates/secedgar
```

However, related responsibilities are still scattered:

- source catalog JSON lives in `companysources/sourcecatalog/sources`
- generic source actions contain source-specific exceptions, such as PRH XBRL
- ClickHouse migrations live in one global ordered folder
- explorer API code is hard-coded for `finland_prhytj`
- source-specific UI is embedded in the generic source detail area
- central Temporal registration imports all known source packages manually

This will become hard to maintain with hundreds of sources.

## Recommended Approach

Use source modules with mostly generic central workflows.

Most sources should use a small fixed set of generic Temporal workflow types:

```text
CompanySourceDownloadWorkflow
CompanySourceDownloadFileWorkflow
CompanySourceClickHouseImportWorkflow
CompanySourceExplorerCacheRefreshWorkflow
CompanySourceIndustryNACEMappingWorkflow
```

The generic workflows should call generic activities. Those activities should
look up the source module and delegate to source-owned behavior. The generic
activity layer should not contain source-specific branches such as:

```text
if source == finland_prh_xbrl { ... }
```

Exceptional sources may register source-specific workflows, but that should be a
rare escape hatch, not the default pattern.

## Backend Layout

Move toward this structure:

```text
scheduler/internal/companysources/
  module.go
  registry.go
  catalog/
  runtime/
  migrations/
  sources/
    finland/
      prhytj/
        source.json
        source.go
        download.go
        import.go
        explorer.go
        temporal.go
        clickhouse/
          migrations/
            000001_tables.up.sql
            000001_tables.down.sql
            000002_explorer_cache.up.sql
            000002_explorer_cache.down.sql
      prhxbrl/
        source.json
        source.go
        download.go
        import.go
        temporal.go
        clickhouse/
          migrations/
            000001_tables.up.sql
            000001_tables.down.sql
    unitedstates/
      secedgar/
      coloradoentities/
      irseobmf/
```

The existing source packages can be moved incrementally. Finland PRH YTJ should
be the pilot because it already exercises the most complete source shape:
multiple files, code lists, ClickHouse normalized tables, explorer cache,
filtering, NACE mapping, and source-specific UI.

## Source Module Shape

Avoid one huge interface. A giant source interface would force empty methods for
sources that do not support every capability.

Prefer a concrete module descriptor with optional capabilities:

```go
type Module struct {
    Key        Key
    Catalog    sourcecatalog.Spec
    Downloader Downloader
    Importer   Importer
    Explorer   Explorer
    Migrations fs.FS
}
```

The registry validates consistency at startup:

- if `source.json` declares `pull_source`, the module must provide a downloader
- if it declares `import_clickhouse`, the module must provide an importer
- if it declares `refresh_explorer_cache`, the module must provide an explorer
- if it declares source migrations, the migration runner must see them

This keeps source behavior explicit without inventing broad abstractions that
hide real differences between sources.

## Source Metadata

Each source owns a `source.json` file.

Example:

```json
{
  "name": "finland_prhytj",
  "country": "finland",
  "source": "prhytj",
  "registry_key": "finland/prhytj",
  "display_name": "Finland PRH YTJ",
  "description": "Finnish company registry data from PRH Open Data YTJ API v3",
  "source_group": "registry",
  "enabled": true,
  "auth_required": false,
  "storage_kind": "clickhouse",
  "bucket_name": "source-finland-prhytj",
  "clickhouse_database": "corpscout_sources",
  "clickhouse_table_prefix": "fi_prhytj",
  "source_url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
  "docs_url": "https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html",
  "capabilities": ["company_import", "clickhouse", "source_download", "explorer"],
  "files": [],
  "actions": []
}
```

The JSON file is the source of truth. On scheduler startup, Corpscout loads all
registered source modules and upserts their metadata into Postgres. The UI may
show this metadata read-only, but users should not edit it through the UI.

## Postgres Responsibility

Postgres should own operational metadata and durable execution history.

Keep in Postgres:

- `data_sources`
- `data_source_files`
- `data_source_actions`
- `data_source_action_runs`
- `data_source_file_runs`
- source bucket name copied from `source.json`
- source catalog sync hash or version
- Temporal workflow IDs and run IDs
- file download status, logs, paths, checksums, sizes, and record counts
- import status, logs, imported tables, and imported row counts

Do not make Postgres the editable owner of source definitions.

## RustFS Responsibility

RustFS is the artifact store. Each source should have either one bucket or one
deterministic bucket prefix. The current project direction says bucket per
source, so `bucket_name` should be explicit in `source.json` and synced to
Postgres.

Downloads should write artifacts to RustFS as the durable store. Local
filesystem paths can remain activity staging paths, but the run metadata should
record enough information to find the RustFS artifact later.

Expected source artifact shape:

```text
bucket: source-finland-prhytj
keys:
  runs/{run_id}/source.ndjson
  runs/{run_id}/codelists/REK.en.tsv
  runs/{run_id}/codelists/YRMU.en.tsv
```

For sources with many individual raw artifacts, such as XBRL statement XML
files, the module owns the exact key layout.

## ClickHouse Migrations

Move source-specific ClickHouse migrations under the source module instead of
keeping all source migrations in one global numbered folder.

Example:

```text
sources/finland/prhytj/clickhouse/migrations/
  000001_tables.up.sql
  000001_tables.down.sql
  000002_explorer_view.up.sql
  000002_explorer_view.down.sql
  000003_explorer_cache.up.sql
  000003_explorer_cache.down.sql
```

The migration runner should store applied migrations in ClickHouse using source
identity plus local migration version:

```text
source_key
migration_version
migration_name
checksum
applied_at
```

This avoids global `000123` numbering conflicts when many sources evolve
independently. Re-running migration discovery should be deterministic: unchanged
source migration files do not create new migration versions.

## Temporal Design

Central Temporal registration should stay direct in app wiring, but it should
register generic source workflows and activities once.

Generic workflows:

```text
DownloadSource
DownloadSourceFile
ImportSourceToClickHouse
RefreshExplorerCache
MapSourceIndustriesToNACE
```

Generic activities:

```text
PrepareSourceDownloadActivity
DownloadSourceFileActivity
FinishSourceDownloadActivity
ImportSourceToClickHouseActivity
RefreshSourceExplorerCacheActivity
MapSourceIndustriesToNACEActivity
```

The activity implementation should be generic orchestration only:

1. load run/action/file state from Postgres
2. validate workflow/run IDs
3. look up the source module
4. call the module capability
5. persist status, logs, checksums, and results

Source modules own source-specific execution details. For example,
`finland/prhxbrl` should own discovery windows, statement XML retries, rate
limit behavior, and manifest generation. Generic activity code should not know
that PRH XBRL exists.

## Explorer APIs

Keep generic source explorer routes:

```text
GET /api/v1/sources/{source}/explorer/companies
GET /api/v1/sources/{source}/explorer/filter-options
POST /api/v1/sources/{source}/actions/refresh_explorer_cache
```

But implementation should route to the source module:

```go
module.Explorer.ListCompanies(ctx, query)
module.Explorer.FilterOptions(ctx)
module.Explorer.RefreshCache(ctx)
```

The common explorer response should provide a baseline shape for list UIs:

```text
id
name
primary_identifier
country
status
registration_date
end_date
industry
website
updated_at
source_specific
```

`source_specific` can carry richer fields without forcing every source into the
same shape. Source-specific detail endpoints can be added where a source needs
more than the common explorer API.

## UI Layout

Keep generic routes:

```text
/sources
/sources/:source
/sources/:source/config
/sources/:source/actions
/sources/:source/explorer
```

Move source-specific components into source UI modules:

```text
ui/app/source-modules/
  registry.ts
  finland/
    prhytj/
      Explorer.tsx
      Actions.tsx
      Detail.tsx
      registry.ts
    prhxbrl/
      Actions.tsx
      Detail.tsx
```

Generic source routes should choose a module component when one exists:

```ts
const module = sourceUiRegistry[source.name]

return module?.Explorer
  ? <module.Explorer source={source} />
  : <GenericExplorer source={source} />
```

Generic UI should remain responsible for shared tabs:

- source header
- read-only source config
- file list and file run history
- action run history
- schedule view
- object storage links

Source-specific UI should own custom action forms, custom explorer columns,
source-specific data cards, and source-specific details.

## Relationship Between Related Sources

Some sources are logically connected. Finland PRH YTJ and Finland PRH XBRL are
separate source modules, but they should also be grouped.

The source metadata should keep:

```text
country: finland
source_group: registry | financial_statements | contact_data | ...
related_sources: ["finland/prh_xbrl"]
```

The UI can use this to show related source links on source detail pages without
merging independent source packages.

## Error Handling And Logging

Follow the project Go error-handling rules:

- lower-level source code wraps and returns errors with context
- activities and HTTP handlers log once at the boundary
- external client errors should include enough context for debugging without
  logging secrets or source credentials
- HTTP responses return safe messages

Source packages should not log every failure internally. They should return
structured results or wrapped errors to the activity boundary.

## Testing Strategy

Each source module should have focused tests:

- source metadata validates
- downloader handles expected source formats and errors
- parser preserves source data
- importer inserts expected ClickHouse rows, using focused writer tests or local
  ClickHouse integration where needed
- explorer query builder handles filters and sorting
- source migrations contain required tables/views/cache tables

Central tests should cover:

- module registry duplicate and missing capability validation
- startup catalog sync from modules into Postgres
- generic Temporal activity delegation into modules
- source migration discovery and checksum behavior
- generic HTTP routes returning 404 when a source lacks a capability
- UI registry fallback to generic components

## Migration Path

Implement this incrementally.

### Phase 1: Module Registry Skeleton

- Add `companysources.Module`.
- Add a registry that stores modules by `country/source` and `name`.
- Keep the current `Source` interface temporarily.
- Register existing sources through module descriptors.

### Phase 2: Move Source Catalog JSON Into Modules

- Move `sourcecatalog/sources/finland_prhytj.json` into the PRH YTJ module.
- Load source metadata through registered modules.
- Keep the Postgres upsert behavior.
- Add `bucket_name` to the source spec and database projection.

### Phase 3: Remove Source-Specific Branching From Generic Actions

- Move PRH XBRL special handling into the PRH XBRL module.
- Make generic actions call module capabilities only.
- Add tests proving generic actions do not import source-specific packages.

### Phase 4: Source-Owned ClickHouse Migrations

- Add source migration discovery.
- Move Finland PRH YTJ ClickHouse migrations under its source module.
- Add a ClickHouse migration tracking table keyed by source and local version.
- Keep global migrations only for shared infrastructure such as databases and
  reference tables.

### Phase 5: Module-Owned Explorer

- Move Finland PRH YTJ explorer query behavior into its module.
- Make generic HTTP explorer handlers delegate to the module explorer.
- Keep the current frontend behavior working.

### Phase 6: Source UI Modules

- Add `ui/app/source-modules/registry.ts`.
- Move Finland PRH YTJ explorer UI into `source-modules/finland/prhytj`.
- Update generic source routes to use module components when present.

### Phase 7: Repeat For PRH XBRL And US Sources

- Move PRH XBRL next because it validates special download behavior.
- Move the three US sources after that because they are simpler.

## Recommended First Pilot

Use Finland PRH YTJ as the first pilot source module.

It is the best test case because it already has:

- source snapshot downloads
- code list downloads
- normalized ClickHouse tables
- importer logic
- explorer cache
- filter options
- NACE mapping
- source-specific UI

After PRH YTJ works in the new shape, migrate Finland PRH XBRL to validate the
source-specific execution escape hatch. Then migrate the US sources to prove the
common path stays simple for smaller sources.

## Open Design Decision

The main remaining decision is how strict to be about bucket per source.

Current project direction says bucket per source. That is simple for the UI and
for source ownership. The alternative is one bucket with a source prefix, which
can be easier for object store administration. This spec assumes bucket per
source unless changed later.
