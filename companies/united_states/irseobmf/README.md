# IRS EO BMF Source

IRS Exempt Organizations Business Master File (EO BMF) is a United States
countrydata source covering tax-exempt nonprofits, national and EIN-keyed. It
downloads the four regional CSV extracts (`eo1.csv`..`eo4.csv`), converts each
row to one NDJSON record keyed by EIN, processes the snapshot, and writes
source-level Parquet exports.

The United States package is standalone. Run these commands from
`companies/united_states` with `GOWORK=off`.

## Full source sync

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source irseobmf --data-dir ../data/united_states/countrydata --chunk-size 1000
```

`sync --source irseobmf` is an alias for `sync-source`.

For a quick smoke run that only fetches the first CSV file:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source irseobmf --data-dir ../data/united_states/countrydata --max-pages 1
```

## Export an existing snapshot

```bash
GOWORK=off go run ./cmd/united-states-countrydata export-source --source irseobmf --data-dir ../data/united_states/countrydata --snapshot-path <path>
```

If `--snapshot-path` is omitted, the exporter uses the latest NDJSON snapshot in
the IRS EO BMF snapshots directory.

## Status

```bash
GOWORK=off go run ./cmd/united-states-countrydata status-source --source irseobmf --data-dir ../data/united_states/countrydata
```

## Data layout

With the default data directory, IRS EO BMF files are written under:

```text
../data/united_states/countrydata/sources/irseobmf
```

Generated outputs:

```text
snapshots/*.ndjson
exports/<run-id>/companies.parquet
exports/<run-id>/company_names.parquet
exports/<run-id>/addresses.parquet
exports/<run-id>/classifications.parquet
exports/<run-id>/financials.parquet
exports/<run-id>/identifiers.parquet
exports/<run-id>/source_evidence.parquet
exports/<run-id>/manifest.json
```

## Mapping notes

- `EIN` is the primary key and the strongest cross-source join key. It is kept
  as a 9-character zero-padded string, never parsed to an integer.
- `NAME` is the legal name; `SORT_NAME` becomes a secondary `sort` name for
  chapter-style organizations.
- `STATUS` is tax-exempt status (`01` = active exemption), not corporate
  standing.
- `RULING` is a YYYYMM IRS recognition date and is **not** an incorporation
  date.
- `NTEE_CD` is the preferred activity classification over the legacy `ACTIVITY`
  field.
- Financial amounts (`ASSET_AMT`/`INCOME_AMT`/`REVENUE_AMT`) are sparse; a
  `financials` row is only emitted when a tax period or any amount is present.

See IRS Publication 5926 for the coded-field data dictionary:
<https://www.irs.gov/pub/irs-pdf/p5926.pdf>

## Environment

| Variable | Purpose |
| --- | --- |
| `IRS_EO_BMF_DATA_DIR` | Direct source-package data directory override. CLI users should prefer `--data-dir`. |
| `IRS_EO_BMF_BASE_URL` | Base URL for the EO BMF CSV files. Defaults to `https://www.irs.gov/pub/irs-soi/`. |
| `IRS_EO_BMF_FILES` | Comma-separated CSV file list. Defaults to `eo1.csv,eo2.csv,eo3.csv,eo4.csv`. |
| `IRS_EO_BMF_USER_AGENT` | HTTP User-Agent for IRS requests. |
| `IRS_EO_BMF_REQUEST_TIMEOUT` | Request timeout as a Go duration such as `60s`, or seconds as an integer. |

No secret or token is required; the EO BMF extracts are public U.S. Government
works.

## Testing

```bash
GOWORK=off go test ./irseobmf/... -count=1
```

Default tests use local CSV/NDJSON fixtures and `httptest`; they do not make
real network calls.

Live tests are gated behind build tags and env vars:

```bash
COUNTRYDATA_IRS_EO_BMF_LIVE=1 \
GOWORK=off go test -tags=integration ./irseobmf/... -run TestLive -count=1 -v

COUNTRYDATA_IRS_EO_BMF_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./irseobmf/... -run TestLive -count=1 -v
```
