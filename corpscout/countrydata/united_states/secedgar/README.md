# United States SEC EDGAR

This package imports the SEC EDGAR `company_tickers.json` ticker map — the bulk
list of SEC-registered filers that have a public stock ticker. The endpoint is a
single, unpaginated JSON object keyed by sequential integer index; each value is
one company `{cik_str, ticker, title}`. Download flattens that object into a
one-record-per-line NDJSON snapshot.

Coverage is **public / SEC-reporting companies only** (~10k with tickers). It is
not a general US company register. Richer attributes (address, SIC, state of
incorporation, EIN, former names, XBRL financials) live behind the
`data.sec.gov` submissions / companyfacts APIs and are planning-only per the
data-model analysis — they are not fetched here.

Run commands from `corpscout/countrydata`.

## User-Agent requirement

SEC rejects requests without a descriptive `User-Agent` containing a contact
email (HTTP 403) and enforces 10 requests/second/IP. Set `SEC_EDGAR_USER_AGENT`
before any live download, e.g. `corpscout/1.0 (contact@example.com)`.

## Configuration (env)

| Variable | Default | Meaning |
|---|---|---|
| `SEC_EDGAR_DOWNLOAD_URL` | `https://www.sec.gov/files/company_tickers.json` | Ticker map URL |
| `SEC_EDGAR_DATA_DIR` | `./data/countrydata/united_states/secedgar` | Snapshot directory |
| `SEC_EDGAR_REQUEST_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `SEC_EDGAR_USER_AGENT` | `corpscout-countrydata/1.0` | **Override with a contact email for live use** |

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live SEC API:

```sh
GOWORK=off go test ./united_states/secedgar/... -count=1
```

## CLI

```sh
GOWORK=off go run ./cmd/secedgar-import download --env .env
GOWORK=off go run ./cmd/secedgar-import process --env .env
GOWORK=off go run ./cmd/secedgar-import run --env .env
```

## Live Tests

Live tests are gated behind the `integration` build tag, an env switch, and a
real `SEC_EDGAR_USER_AGENT`:

```sh
# Smoke: download the full ticker map, process a bounded subset (Limit=200)
COUNTRYDATA_SEC_EDGAR_LIVE=1 \
SEC_EDGAR_USER_AGENT='corpscout/1.0 (you@example.com)' \
GOWORK=off go test -tags=integration ./united_states/secedgar/... -run TestLiveSECEDGARSmoke -count=1 -v

# Full: download and process the entire ticker map
COUNTRYDATA_SEC_EDGAR_LIVE_FULL=1 \
SEC_EDGAR_USER_AGENT='corpscout/1.0 (you@example.com)' \
GOWORK=off go test -tags=integration ./united_states/secedgar/... -run TestLiveSECEDGARFullDataset -count=1 -v
```
