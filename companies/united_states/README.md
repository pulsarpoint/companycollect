# United States Country Data

Standalone Go module for United States company data collection and source
exports. It does not require corpscout. It has its own `go.mod` and should be
built, tested, and run from `companies/united_states` with `GOWORK=off`.

The current implementation supports source-level collection for three fully
open sources: SEC EDGAR, IRS EO BMF, and Colorado Business Entities. The final
United States export that combines multiple sources is intentionally not
implemented yet.

## Source status

| Source | Status | Notes |
| --- | --- | --- |
| `secedgar` | implemented | Downloads SEC `company_tickers.json`, processes it, and writes source Parquet exports. See [secedgar/README.md](secedgar/README.md). |
| `irseobmf` | implemented | Downloads the IRS EO BMF CSV extracts (`eo1`..`eo4`), converts to NDJSON, and writes source Parquet exports. See [irseobmf/README.md](irseobmf/README.md). |
| `coloradoentities` | implemented | Pages the Colorado SODA endpoint to NDJSON and writes source Parquet exports. See [coloradoentities/README.md](coloradoentities/README.md). |

The remaining analyzed sources (`sam_gov_entity`, `state_sos_registries`,
`opencorporates`) are planning-only or access-restricted and are intentionally
not implemented.

## Data layout

Run commands from this directory:

```bash
cd companies/united_states
```

When `--data-dir` is omitted, the CLI uses:

```text
../data/united_states/countrydata
```

For SEC EDGAR, source data is stored under:

```text
../data/united_states/countrydata/sources/secedgar
```

Generated files:

```text
sources/secedgar/snapshots/*.json
sources/secedgar/exports/<run-id>/companies.parquet
sources/secedgar/exports/<run-id>/company_names.parquet
sources/secedgar/exports/<run-id>/identifiers.parquet
sources/secedgar/exports/<run-id>/source_evidence.parquet
sources/secedgar/exports/<run-id>/manifest.json
```

## Commands

Build the countrydata CLI:

```bash
GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata
```

Sync SEC EDGAR. This downloads the current snapshot, processes it, and writes a
source export:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source secedgar --data-dir ../data/united_states/countrydata --chunk-size 500
```

`sync --source secedgar` is an alias for `sync-source`:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync --source secedgar --data-dir ../data/united_states/countrydata --chunk-size 500
```

The same commands work for `irseobmf` and `coloradoentities`. For the paginated
or multi-file sources, `--max-pages` bounds a smoke run (CSV files for
`irseobmf`, SODA pages for `coloradoentities`):

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source irseobmf --data-dir ../data/united_states/countrydata --max-pages 1
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source coloradoentities --data-dir ../data/united_states/countrydata --max-pages 2
```

See the per-source READMEs for source-specific mapping notes and environment
variables: [secedgar](secedgar/README.md), [irseobmf](irseobmf/README.md),
[coloradoentities](coloradoentities/README.md).

Export from an existing snapshot:

```bash
GOWORK=off go run ./cmd/united-states-countrydata export-source --source secedgar --data-dir ../data/united_states/countrydata --snapshot-path <path>
```

Show a single source status (`secedgar`, `irseobmf`, or `coloradoentities`):

```bash
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar --data-dir ../data/united_states/countrydata
```

Show country source status:

```bash
GOWORK=off go run ./cmd/united-states-countrydata status --data-dir ../data/united_states/countrydata
```

`build-export` is reserved for the future final USA multi-source export and
currently returns a not-implemented error.

## Configuration

Each source is configured with its own environment-variable prefix:

- SEC EDGAR: `USA_SEC_EDGAR_*` (see [secedgar/README.md](secedgar/README.md)).
- IRS EO BMF: `IRS_EO_BMF_*` (see [irseobmf/README.md](irseobmf/README.md)).
- Colorado: `COLORADO_BUSINESS_ENTITIES_*` (see
  [coloradoentities/README.md](coloradoentities/README.md)).

CLI users should prefer `--data-dir`; the CLI resolves it to the per-source
`sources/<slug>` directory. The optional Colorado Socrata app token
(`COLORADO_BUSINESS_ENTITIES_APP_TOKEN`) is the only credential any source
accepts, and it is never logged or written to manifests. No source requires a
secret to run.

## Testing

Unit tests use fixtures and `httptest`; they do not call live endpoints.

```bash
GOWORK=off go test ./... -count=1
```

Live tests are gated behind build tags and per-source env vars; see each source
README. Manual sync is the only default path that downloads live data.

## Limitations

- SEC EDGAR, IRS EO BMF, and Colorado Business Entities are implemented.
- `sam_gov_entity`, `state_sos_registries`, and `opencorporates` are
  planning-only or access-restricted and are not implemented.
- The final USA export across multiple sources (`build-export`) is not
  implemented yet.
