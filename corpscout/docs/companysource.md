# Companysource

`companies/companysource` is the active entry point for company source ingestion.
It owns the CLI, source registry, source-specific packages, Parquet export, and
ClickHouse migration/import actions.

## Run Layout

Each source run is one flat folder:

```text
companies/data/<country>/sources/<source>/runs/<run_id>/
  source.ndjson | source.json
  *.parquet
  manifest.json
```

Do not add `snapshots/`, `processed/`, nested `exports/`, or country-level final
exports to new source runs. Source packages should preserve as much source data
as practical in source-specific Parquet tables.

## Commands

Run from `companies/companysource`:

```bash
GOWORK=off go run ./cmd/companysource list-sources

GOWORK=off go run ./cmd/companysource download \
  --country finland \
  --source prhytj \
  --run-dir ../data/finland/sources/prhytj/runs/<run_id>

GOWORK=off go run ./cmd/companysource export-parquet \
  --country finland \
  --source prhytj \
  --run-dir ../data/finland/sources/prhytj/runs/<run_id>

GOWORK=off go run ./cmd/companysource generate-clickhouse-migration \
  --country finland \
  --source prhytj \
  --run-dir ../data/finland/sources/prhytj/runs/<run_id> \
  --database corpscout_sources \
  --out ../../corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql \
  --down-out ../../corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql

GOWORK=off go run ./cmd/companysource import-clickhouse \
  --country finland \
  --source prhytj \
  --run-dir ../data/finland/sources/prhytj/runs/<run_id> \
  --database corpscout_sources \
  --clickhouse-native-url 'clickhouse://host.docker.internal:9002?username=default&password=change-me&database=corpscout_sources' \
  --source-export-id 00000000-0000-0000-0000-000000000000
```

Current source keys:

- `finland/prhytj`
- `united_states/coloradoentities`
- `united_states/irseobmf`
- `united_states/secedgar`

## ClickHouse

Each source package embeds its own `clickhouse.yaml`. The YAML maps source
Parquet files to source-specific ClickHouse tables and deterministic ordering
keys. Migrations are generated from actual Parquet schemas with
`clickhouse-local`; imports stream Parquet through `clickhouse-local` into
`clickhouse-client` using Native format.

The Corpscout Makefile targets now call `companysource`:

```bash
make -C corpscout clickhouse-generate-finland-prhytj
make -C corpscout clickhouse-import-finland-prhytj
```
