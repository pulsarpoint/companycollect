# NACE ClickHouse Sync Design

## Goal

Mirror the authoritative NACE taxonomy from Postgres into ClickHouse so company source explorers and source-specific mapping jobs can filter and join against NACE data locally.

## Scope

This phase covers only the NACE reference copy in ClickHouse. It does not map Finland PRH YTJ industries to NACE, and it does not create the Finland explorer cache table.

## Current State

Postgres owns NACE taxonomy data:

- `nace_classifications`
- `nace_codes`
- `nace_code_aliases`
- `v_nace_taxonomy_state`
- `v_nace_code_tree`

The existing NACE taxonomy workflow downloads RDF, parses it, and upserts taxonomy rows into Postgres. The settings page at `/settings/nace-taxonomy` can trigger this workflow and list recent Temporal runs.

ClickHouse currently has source-specific company tables under `corpscout_sources`, but no reference database for shared taxonomies.

## Design

Add a new ClickHouse database named `corpscout_reference`. Store the mirrored NACE taxonomy in three tables:

- `corpscout_reference.nace_classifications`
- `corpscout_reference.nace_codes`
- `corpscout_reference.nace_code_aliases`

Postgres remains authoritative. ClickHouse is a read-only analytics copy maintained by an explicit Temporal workflow.

The sync action reads all active and inactive NACE rows from Postgres, truncates the ClickHouse reference tables, and reinserts a complete snapshot. NACE is small enough that full refresh is simpler and safer than revision-specific mutations.

`nace_codes` includes derived hierarchy columns:

- `section_code`
- `division_code`
- `group_code`
- `class_code`

These columns are derived from the Postgres parent chain during sync and make future explorer filtering cheap.

## Workflows

Add a new Temporal workflow:

- `SyncNACEToClickHouse`

It runs on the existing `nace-taxonomy-sync` task queue and calls a new activity:

- `SyncNACEToClickHouseActivity`

The existing `SyncNACETaxonomy` workflow should trigger the new workflow after a successful or skipped Postgres taxonomy sync. A skipped import still represents a valid Postgres taxonomy state, and ClickHouse may be empty or stale.

Temporal code should live under the Temporal package tree:

- `scheduler/internal/temporal/workflow/nace` owns workflow names, task queue names, workflow input/result contracts, and workflow functions.
- `scheduler/internal/temporal/actions/nace` owns activity implementations and imports the workflow package for activity input/result contracts.

Use the existing singular `workflow` directory convention already used by `scheduler/internal/temporal/workflow/companysources`; do not create a parallel `workflows` tree.

The existing `scheduler/internal/nacetaxonomy` package should remain for non-Temporal domain code such as NACE code normalization, source download helpers, and RDF parsing.

Manual sync is also exposed from the settings UI through:

- `POST /api/v1/workflows/nace/clickhouse-sync`
- `GET /api/v1/workflows/nace/clickhouse-sync/runs`

## UI

Add a `Sync to CH` button on `/settings/nace-taxonomy`.

The button starts `SyncNACEToClickHouse` directly. It does not download NACE source files and does not mutate Postgres taxonomy rows.

The existing NACE sync button remains unchanged.

## Error Handling

Lower layers wrap and return errors using `github.com/cockroachdb/errors`.

The HTTP boundary logs workflow start failures once with `log/slog` and returns safe messages.

The activity logs no secrets and does not include source payloads in errors.

## Testing

Coverage should include:

- ClickHouse migration shape test for the new reference database and tables.
- SQL query shape test for the Postgres export queries.
- Unit test for NACE hierarchy derivation from parent chains.
- Temporal workflow test showing `SyncNACETaxonomy` triggers the ClickHouse sync child workflow after succeeded or skipped imports.
- HTTP handler test for manual `Sync to CH`.
- UI typecheck for the settings page API/types.

## Future Phases

The next phase creates `fi_prhytj_company_explorer_cache` and reads from that table instead of the live explorer view.

A later mapping phase can join Finland source industry values against `corpscout_reference.nace_codes` and store mapping results in the Finland explorer cache.
