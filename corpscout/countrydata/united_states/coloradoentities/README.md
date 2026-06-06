# United States Colorado Business Entities

This package imports the **Colorado Business Entities** dataset (Colorado
Secretary of State, published on the Colorado Information Marketplace / Socrata
SODA API, dataset `4ykn-tg5h`). It is the first US source covering *private*
companies — in the US, company registration happens at the state level, and
Colorado is the exemplar open state register (1M+ entities, free, no auth).

Download pages the SODA API with `$limit`/`$offset` (ordered by `entityid` for
stable paging), writing one compact NDJSON record per entity. Records are sparse
(most optional fields absent). Note the source field `jurisdictonofformation` is
**misspelled** upstream (missing an "i") — the exact key is preserved.

The derived profile carries authoritative private-company fields: cleaned legal
name (status annotations stripped, raw kept as an alternate name), corporate
standing (`Good Standing` → active), decoded legal form, formation date, and a
registered agent (a service-of-process contact — **not** an owner/officer).

Run commands from `corpscout/countrydata`.

## Configuration (env)

| Variable | Default | Meaning |
|---|---|---|
| `COLORADO_BUSINESS_ENTITIES_BASE_URL` | `https://data.colorado.gov/resource/4ykn-tg5h.json` | SODA endpoint |
| `COLORADO_BUSINESS_ENTITIES_APP_TOKEN` | _(unset)_ | Optional Socrata app token (higher rate limits) |
| `COLORADO_BUSINESS_ENTITIES_DATA_DIR` | `./data/countrydata/united_states/coloradoentities` | Snapshot directory |
| `COLORADO_BUSINESS_ENTITIES_PAGE_SIZE` | `1000` | Socrata `$limit` per page |
| `COLORADO_BUSINESS_ENTITIES_PAGE_DELAY_MS` | `500` | Delay between pages |
| `COLORADO_BUSINESS_ENTITIES_REQUEST_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `COLORADO_BUSINESS_ENTITIES_USER_AGENT` | `corpscout-countrydata/1.0` | Request User-Agent |

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live Socrata API:

```sh
GOWORK=off go test ./united_states/coloradoentities/... -count=1
```

## CLI

```sh
GOWORK=off go run ./cmd/coloradoentities-import download --env .env
GOWORK=off go run ./cmd/coloradoentities-import download --env .env --max-pages 1
GOWORK=off go run ./cmd/coloradoentities-import process  --env .env
GOWORK=off go run ./cmd/coloradoentities-import run      --env .env
```

## Live Tests

Gated behind the `integration` build tag and an env switch (an app token is
optional but recommended to avoid throttling):

```sh
# Smoke: one small page ($limit=50)
COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1 \
GOWORK=off go test -tags=integration ./united_states/coloradoentities/... -run TestLiveColoradoEntitiesSmoke -count=1 -v

# Full: page the entire register with $limit=1000
COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL=1 \
GOWORK=off go test -tags=integration ./united_states/coloradoentities/... -run TestLiveColoradoEntitiesFullDataset -count=1 -v
```
