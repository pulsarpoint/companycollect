# Companysource

`companysource` is the single active company-source ingestion module under
`companies`. It builds one binary from `cmd/companysource` and contains all
source-specific packages plus shared ingestion helpers.

## Layout

```text
cmd/companysource/        CLI entry point for the single binary
common/                   shared source-agnostic helpers
internal/                 CLI, registry, source contract, ClickHouse support
sources/                  country/source implementations
```

Runtime data lives outside the Go module under `../data`.

## Commands

```bash
make test
make build
make list-sources
```

Use the binary directly:

```bash
bin/companysource list-sources
bin/companysource export-parquet --country finland --source prhytj --run-dir ../data/finland/sources/prhytj/runs/<run-id>
```
