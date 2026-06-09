# Colorado Business Entities Source

Colorado Business Entities is a United States companysource and the state
exemplar for private companies. It pages the Socrata SODA endpoint for the
Colorado Information Marketplace dataset `4ykn-tg5h`, writing one NDJSON record
per entity, then writes source-level Parquet exports into the same run folder.

Run these commands from `companies/companysource` with `GOWORK=off`.

## Full source sync

```bash
GOWORK=off go run ./cmd/companysource download --country united_states --source coloradoentities --run-dir ../data/united_states/sources/coloradoentities/runs/<run-id>
GOWORK=off go run ./cmd/companysource export-parquet --country united_states --source coloradoentities --run-dir ../data/united_states/sources/coloradoentities/runs/<run-id>
```

For a quick smoke run that only fetches the first two pages:

```bash
GOWORK=off go run ./cmd/companysource download --country united_states --source coloradoentities --run-dir ../data/united_states/sources/coloradoentities/runs/<run-id> --max-pages 2
```

## Export an existing snapshot

```bash
GOWORK=off go run ./cmd/companysource export-parquet --country united_states --source coloradoentities --run-dir ../data/united_states/sources/coloradoentities/runs/<run-id>
```

The exporter reads `source.ndjson` from the run folder.

## Status

```bash
GOWORK=off go run ./cmd/companysource status --country united_states --source coloradoentities --run-dir ../data/united_states/sources/coloradoentities/runs/<run-id>
```

## Data layout

Generated outputs:

```text
source.ndjson
companies.parquet
company_names.parquet
legal_forms.parquet
addresses.parquet
registered_agents.parquet
identifiers.parquet
source_evidence.parquet
manifest.json
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
GOWORK=off go test ./sources/unitedstates/coloradoentities -count=1
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
