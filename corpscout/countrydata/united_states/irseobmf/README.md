# United States IRS EO BMF

This package imports the IRS **Exempt Organizations Business Master File (EO
BMF)** — the national, EIN-keyed extract of tax-exempt organizations. The source
is four regional CSV files (`eo1.csv`…`eo4.csv`, 28 columns each, header row
present); concatenated they give national coverage. Download streams every CSV
row into one NDJSON record per line (keys = column names), so a multi-hundred-MB
extract is never loaded fully into memory.

Coverage is **tax-exempt organizations only** (nonprofits with an EIN and an IRS
determination). EIN is the strongest cross-source join key in the US model. EO
BMF describes *tax-exempt status* and *nonprofit financials* — not corporate
standing or incorporation date — so the derived profile keeps those distinct
(e.g. `RULING` maps to an approximate `IRSRulingDate`, never a formation date).

Field semantics follow IRS Publication 5926.

Run commands from `corpscout/countrydata`.

## Configuration (env)

| Variable | Default | Meaning |
|---|---|---|
| `IRS_EO_BMF_DOWNLOAD_URLS` | `eo1..eo4.csv` (comma-separated) | Regional CSV file URLs |
| `IRS_EO_BMF_DATA_DIR` | `./data/countrydata/united_states/irseobmf` | Snapshot directory |
| `IRS_EO_BMF_PAGE_DELAY_MS` | `500` | Delay between files |
| `IRS_EO_BMF_REQUEST_TIMEOUT_SECONDS` | `60` | Per-file request timeout |
| `IRS_EO_BMF_USER_AGENT` | `corpscout-countrydata/1.0` | Request User-Agent |

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live IRS site:

```sh
GOWORK=off go test ./united_states/irseobmf/... -count=1
```

## CLI

```sh
GOWORK=off go run ./cmd/irseobmf-import download --env .env            # all files
GOWORK=off go run ./cmd/irseobmf-import download --env .env --max-files 1
GOWORK=off go run ./cmd/irseobmf-import process  --env .env
GOWORK=off go run ./cmd/irseobmf-import run      --env .env
```

## Live Tests

Gated behind the `integration` build tag and an env switch:

```sh
# Smoke: download only eo1.csv, process a bounded subset (Limit=500)
COUNTRYDATA_IRS_EO_BMF_LIVE=1 \
GOWORK=off go test -tags=integration ./united_states/irseobmf/... -run TestLiveIRSEoBmfSmoke -count=1 -v

# Full: download and stream-parse all four regional files
COUNTRYDATA_IRS_EO_BMF_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./united_states/irseobmf/... -run TestLiveIRSEoBmfFullDataset -count=1 -v
```
