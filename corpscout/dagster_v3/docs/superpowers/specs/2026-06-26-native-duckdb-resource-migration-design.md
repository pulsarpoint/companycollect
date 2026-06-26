# Native DuckDB Resource Migration Design

## Goal

Move Dagster v3 production DuckDB usage to Dagster's native `dagster_duckdb.DuckDBResource` and remove the custom `LocalDuckDBResource` wrapper. The migration should reduce source-specific DuckDB connection code, centralize runtime configuration, and keep dlt/dbt integrations path-based where their APIs require file paths.

## Current State

The project currently uses three DuckDB patterns:

- `LocalDuckDBResource` in `dagster_v3.defs.common.resources`, used mainly by Finland YTJ/XBRL/resolved assets.
- Direct `duckdb.connect(...)` calls throughout production modules for transforms, metrics, checks, exports, and helper reads.
- Path-based DuckDB destinations for dlt and dbt, where the third-party library owns the connection.

Brazil RFB also has a shared `duckdb_runtime.py` helper that applies runtime settings after opening a raw DuckDB connection. This helped with spill/memory tuning, but it is not a Dagster resource and does not apply consistently across the project.

## Target Architecture

Add `dagster-duckdb` and use `dagster_duckdb.DuckDBResource` for Dagster-managed DuckDB files. Assets and checks that need a DuckDB database should receive a native resource and open connections through:

```python
with source_duckdb.get_connection() as connection:
    ...
```

Shared transformation functions should stop accepting `database_path` when they need to query or mutate DuckDB. Instead, they should accept an existing `duckdb.DuckDBPyConnection`. This keeps connection ownership at the Dagster asset/resource boundary.

Path-based dlt and dbt APIs remain path-based. Those libraries construct their own DuckDB connections internally, so the migration should keep small path constants or path helper functions for:

- `dlt.destinations.duckdb(str(database_path))`
- dbt `profiles.yml` DuckDB `path`
- tests that create isolated DuckDB files

## Runtime Settings

Runtime settings should be configured through the native resource where DuckDB supports connection-level configuration. The common runtime policy should expose one function that builds a `connection_config` dictionary from generic environment variables:

- `DUCKDB_TEMP_DIRECTORY`
- `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`
- `DUCKDB_THREADS`
- `DUCKDB_MEMORY_LIMIT`
- `DUCKDB_PRESERVE_INSERTION_ORDER`

The default policy should preserve the Brazil fix:

- `preserve_insertion_order` defaults to `false`
- `threads` defaults to `4`
- `max_temp_directory_size` defaults to `100GiB`
- `temp_directory` defaults to a project data temp directory when not supplied
- `memory_limit` is optional

The runtime helper should create the configured temp directory before the native resource is instantiated or before the first connection is opened. It should be named around DuckDB resource configuration, not around Brazil.

If a DuckDB setting cannot be applied through `DuckDBResource(connection_config=...)`, the migration may keep a small post-connect initializer, but only as a native-resource companion. It must not be source-specific and must not reintroduce direct `duckdb.connect(...)` calls in production asset code.

## Resource Definitions

The root Dagster definitions should continue providing shared global resources such as ClickHouse and dlt. Source-specific DuckDB resources should be defined close to the source definitions when each source has its own database file.

Examples:

- Finland YTJ: `ytj_duckdb = DuckDBResource(database="data/finland_ytj.duckdb", connection_config=...)`
- Finland XBRL: `source_duckdb = DuckDBResource(database="data/finland_ytj.duckdb", connection_config=...)`
- Brazil RFB: `brazil_rfb_duckdb = DuckDBResource(database="data/brazil_rfb.duckdb", connection_config=...)`

Resource names should remain source-specific where the database file is source-specific. This keeps Dagster asset signatures readable and avoids a single ambiguous global DuckDB resource.

## Migration Scope

The strict migration covers production code under `src/dagster_v3`:

- Replace all `LocalDuckDBResource` imports and definitions with `dagster_duckdb.DuckDBResource`.
- Remove `LocalDuckDBResource` from `common/resources.py` once no production or test code imports it.
- Move production helper functions from path-owned connections to caller-owned connections where practical.
- Remove Brazil-specific DuckDB runtime wrapper once Brazil uses the native resource/runtime configuration.
- Keep `duckdb.connect(...)` in tests where tests create isolated databases directly.
- Keep path arguments for dlt and dbt integration points.

Some production direct connections may remain temporarily only when they are not inside Dagster-executed assets/checks and migrating them would require changing an external library boundary. Each exception should be explicit in the implementation plan.

## Data Flow

The new flow for resource-managed sources should be:

```text
Dagster asset/check
  -> source-specific DuckDBResource
  -> get_connection()
  -> transform/export/helper function receives DuckDBPyConnection
  -> dlt/dbt still receive database path where required
```

Brazil RFB should change from:

```text
asset -> transform(database_path) -> duckdb.connect(path) -> apply runtime settings
```

to:

```text
asset -> DuckDBResource.get_connection() -> transform(connection) -> SQL work
```

## Error Handling

Connection creation and temp-directory setup errors should fail at the asset boundary with Dagster's normal resource/asset error reporting. Helper functions should not catch broad DuckDB errors unless they can add source-specific context and re-raise. The migration should avoid logging the same error at multiple layers.

## Testing

Tests should cover:

- Native resource configuration builds the expected `connection_config` from environment variables.
- Temp directories are created before use.
- Former `LocalDuckDBResource` consumers work with `DuckDBResource`.
- Brazil RFB heavy transform functions work when passed an existing connection.
- `rg` checks show no `LocalDuckDBResource` imports remain.
- `rg` checks show production direct `duckdb.connect(...)` usage is either removed or limited to documented exceptions.
- Existing focused source tests still pass for Brazil RFB and Finland YTJ/XBRL.

## Rollout

The implementation should be split into small commits:

1. Add `dagster-duckdb` and common native DuckDB resource configuration helper.
2. Migrate Finland `LocalDuckDBResource` users.
3. Migrate Brazil RFB to native DuckDBResource connection ownership.
4. Convert high-value direct production connection helpers to connection-injected functions.
5. Remove `LocalDuckDBResource` and obsolete runtime wrappers.
6. Run `dg check` and focused pytest suites.

This order keeps the resource migration independently testable before touching larger country transforms.
