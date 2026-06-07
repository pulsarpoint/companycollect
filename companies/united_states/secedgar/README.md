# SEC EDGAR Source

SEC EDGAR is the first implemented United States countrydata source. It
downloads `https://www.sec.gov/files/company_tickers.json`, validates and
processes the snapshot, and writes source-level exports that can later feed a
final United States multi-source export.

The United States package is standalone. Run these commands from
`companies/united_states` with `GOWORK=off`.

## Full source sync

Set a real production User-Agent before a live SEC request:

```bash
export USA_SEC_EDGAR_USER_AGENT="<organization name and contact email>"
```

Run the source sync:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source secedgar --data-dir ../data/united_states/countrydata --chunk-size 500
```

The `sync --source secedgar` command is an alias for `sync-source`.

## Export an existing snapshot

Use this when a snapshot already exists and only the source export should be
rebuilt:

```bash
GOWORK=off go run ./cmd/united-states-countrydata export-source --source secedgar --data-dir ../data/united_states/countrydata --snapshot-path <path>
```

If `--snapshot-path` is omitted, the exporter uses the latest JSON snapshot in
the SEC EDGAR snapshots directory.

## Status

```bash
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar --data-dir ../data/united_states/countrydata
GOWORK=off go run ./cmd/united-states-countrydata status --data-dir ../data/united_states/countrydata
```

## Data layout

With the default data directory, SEC EDGAR files are written under:

```text
../data/united_states/countrydata/sources/secedgar
```

Generated outputs:

```text
snapshots/*.json
exports/<run-id>/companies.parquet
exports/<run-id>/company_names.parquet
exports/<run-id>/identifiers.parquet
exports/<run-id>/source_evidence.parquet
exports/<run-id>/manifest.json
```

## Environment

| Variable | Purpose |
| --- | --- |
| `USA_SEC_EDGAR_DATA_DIR` | Source data directory override. |
| `USA_SEC_EDGAR_DOWNLOAD_URL` | Download URL override. Defaults to SEC `company_tickers.json`. |
| `USA_SEC_EDGAR_USER_AGENT` | User-Agent sent to SEC. Use a real organization/contact string in production. |
| `USA_SEC_EDGAR_REQUEST_TIMEOUT` | Request timeout, for example `30s` or `30`. |

No secret or token is required.

## Testing

```bash
GOWORK=off go test ./... -count=1
```

Tests use local fixtures and `httptest`; they do not make real network calls.

## Limitations

This source exports SEC EDGAR company ticker records only. The final USA export
that combines SEC EDGAR with future sources such as `irseobmf` and
`coloradoentities` is intentionally not implemented yet.
