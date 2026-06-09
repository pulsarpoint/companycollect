# Colorado Business Entities Source

Colorado Business Entities is a United States countrydata source and the state
exemplar for private companies. It pages the Socrata SODA endpoint for the
Colorado Information Marketplace dataset `4ykn-tg5h`, writing one NDJSON record
per entity, then processes the snapshot and writes source-level Parquet exports.

The United States package is standalone. Run these commands from
`companies/united_states` with `GOWORK=off`.

## Full source sync

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source coloradoentities --data-dir ../data/united_states/countrydata --chunk-size 1000
```

`sync --source coloradoentities` is an alias for `sync-source`.

For a quick smoke run that only fetches the first two pages:

```bash
GOWORK=off go run ./cmd/united-states-countrydata sync-source --source coloradoentities --data-dir ../data/united_states/countrydata --max-pages 2
```

## Export an existing snapshot

```bash
GOWORK=off go run ./cmd/united-states-countrydata export-source --source coloradoentities --data-dir ../data/united_states/countrydata --snapshot-path <path>
```

If `--snapshot-path` is omitted, the exporter uses the latest NDJSON snapshot in
the Colorado snapshots directory.

## Status

```bash
GOWORK=off go run ./cmd/united-states-countrydata status-source --source coloradoentities --data-dir ../data/united_states/countrydata
```

## Data layout

With the default data directory, Colorado files are written under:

```text
../data/united_states/countrydata/sources/coloradoentities
```

Generated outputs:

```text
snapshots/*.ndjson
exports/<run-id>/companies.parquet
exports/<run-id>/company_names.parquet
exports/<run-id>/legal_forms.parquet
exports/<run-id>/addresses.parquet
exports/<run-id>/registered_agents.parquet
exports/<run-id>/identifiers.parquet
exports/<run-id>/source_evidence.parquet
exports/<run-id>/manifest.json
```

## Mapping notes

- `entityid` is unique only within Colorado. The global company id is
  `CO:` + entityid.
- `entityname` sometimes carries an appended status annotation (e.g.
  `, Delinquent May 1, 2016`); the cleaned name is the legal name and the raw
  value is preserved as a `source_variant` name.
- `entitystatus` maps `Good Standing` to active; everything else is treated as
  inactive/at-risk.
- `entityformdate` is the most reliable incorporation/registration date across
  the US open sources; only its `YYYY-MM-DD` date portion is kept.
- `jurisdictonofformation` is **misspelled in the source** (missing an `i`); the
  exact key is preserved. A value other than `CO` marks a foreign entity merely
  registered to do business in Colorado.
- The registered agent (person or organization) is a legal contact for service
  of process, **not** an owner or officer.

## Environment

| Variable | Purpose |
| --- | --- |
| `COLORADO_BUSINESS_ENTITIES_DATA_DIR` | Direct source-package data directory override. CLI users should prefer `--data-dir`. |
| `COLORADO_BUSINESS_ENTITIES_BASE_URL` | SODA JSON endpoint. Defaults to `https://data.colorado.gov/resource/4ykn-tg5h.json`. |
| `COLORADO_BUSINESS_ENTITIES_PAGE_SIZE` | SODA `$limit` per request. Defaults to 1000. |
| `COLORADO_BUSINESS_ENTITIES_PAGE_DELAY_MS` | Delay between pages in milliseconds. |
| `COLORADO_BUSINESS_ENTITIES_REQUEST_TIMEOUT_SECONDS` | Per-request timeout in seconds. |
| `COLORADO_BUSINESS_ENTITIES_USER_AGENT` | HTTP User-Agent for SODA requests. |
| `COLORADO_BUSINESS_ENTITIES_APP_TOKEN` | Optional Socrata app token (`X-App-Token`) for higher rate limits. Not required. |

The app token is optional and is the only credential; it is never logged or
written to manifests.

## Testing

```bash
GOWORK=off go test ./coloradoentities/... -count=1
```

Default tests use a local NDJSON fixture and a paginated `httptest` server; they
do not make real network calls.

Live tests are gated behind build tags and env vars:

```bash
COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1 \
GOWORK=off go test -tags=integration ./coloradoentities/... -run TestLive -count=1 -v

COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./coloradoentities/... -run TestLive -count=1 -v
```
