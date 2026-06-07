# United States Country Data

Standalone Go module for United States company data collection and source
exports. It does not require corpscout. It has its own `go.mod` and should be
built, tested, and run from `companies/united_states` with `GOWORK=off`.

The current implementation supports source-level SEC EDGAR collection only.
The final United States export that combines multiple sources is intentionally
not implemented yet.

## Source status

| Source | Status | Notes |
| --- | --- | --- |
| `secedgar` | implemented | Downloads SEC `company_tickers.json`, processes it, and writes source Parquet exports. |
| `irseobmf` | planned | Not implemented yet. |
| `coloradoentities` | planned | Not implemented yet. |

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

Export from an existing snapshot:

```bash
GOWORK=off go run ./cmd/united-states-countrydata export-source --source secedgar --data-dir ../data/united_states/countrydata --snapshot-path <path>
```

Show SEC EDGAR source status:

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

SEC EDGAR configuration can be set with environment variables:

| Variable | Purpose |
| --- | --- |
| `USA_SEC_EDGAR_DATA_DIR` | Direct source-package data directory override. CLI users should prefer `--data-dir`; the CLI resolves that country data directory to `sources/secedgar`. |
| `USA_SEC_EDGAR_DOWNLOAD_URL` | SEC company tickers JSON URL override. Defaults to `https://www.sec.gov/files/company_tickers.json`. |
| `USA_SEC_EDGAR_USER_AGENT` | HTTP User-Agent for SEC requests. Set a real production contact string before live syncs. |
| `USA_SEC_EDGAR_REQUEST_TIMEOUT` | Request timeout as a Go duration such as `30s`, or seconds as an integer. |

Do not put secrets, tokens, or cookies in these values. SEC EDGAR download does
not require a secret.

## Testing

Unit tests use fixtures and `httptest`; they do not call the live SEC endpoint.

```bash
GOWORK=off go test ./... -count=1
```

Manual sync is the only path that downloads live SEC EDGAR data.

## Limitations

- SEC EDGAR is the only implemented source.
- IRS EO BMF and Colorado entities are listed in status output as
  `not_implemented`.
- Final USA export across multiple sources is not implemented yet.
