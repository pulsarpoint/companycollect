# United States SAM.gov Entity Management

This package imports the **SAM.gov Entity Management** dataset (System for Award
Management, U.S. GSA) via the Entity Management v3 API. It covers entities
registered to do business with the US federal government (contractors, grantees)
— **not** a general company register. Its registration status reflects
*federal-award eligibility*, which is distinct from state corporate standing.

The schema in this package was **verified against a live Public-extract
response** (the data-model analysis was planning-only). Download pages the API
with `page`/`size` (the API caps `size` at 10), writing one compact NDJSON
record per entity.

## Authentication & sensitivity

- Requires a free SAM.gov **System Account API key with "Read Public"**
  permission, supplied via `SAM_GOV_ENTITY_API_KEY`.
- The key is sent in the **`X-Api-Key` header**, never in a URL — so it never
  appears in request URLs, error messages, metadata, or logs.
- Consume **only** the FOIA-releasable Public extract. Never request or store
  FOUO/Sensitive sensitivity levels. EIN/TIN is sensitive and redacted in the
  Public extract, so it is intentionally not modeled. `pointsOfContact` (personal
  contact data) is not modeled in the typed record.

The API key lives in `corpscout/countrydata/.env` (gitignored). Never commit it.

## Configuration (env)

| Variable | Default | Meaning |
|---|---|---|
| `SAM_GOV_ENTITY_API_KEY` | _(required)_ | Read Public System Account key (sent as X-Api-Key) |
| `SAM_GOV_ENTITY_BASE_URL` | `https://api.sam.gov/entity-information/v3/entities` | API endpoint |
| `SAM_GOV_ENTITY_SAM_REGISTERED` | `Yes` | `samRegistered` filter |
| `SAM_GOV_ENTITY_DATA_DIR` | `./data/countrydata/united_states/samgoventity` | Snapshot directory |
| `SAM_GOV_ENTITY_PAGE_SIZE` | `10` | `size` per page (API max is 10) |
| `SAM_GOV_ENTITY_PAGE_DELAY_MS` | `500` | Delay between pages |
| `SAM_GOV_ENTITY_REQUEST_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `SAM_GOV_ENTITY_USER_AGENT` | `corpscout-countrydata/1.0` | Request User-Agent |

## Local Fixture Tests

Default tests use checked-in fixtures and do not call the live API:

```sh
GOWORK=off go test ./united_states/samgoventity/... -count=1
```

## CLI

```sh
GOWORK=off go run ./cmd/samgoventity-import download --env .env --max-pages 1
GOWORK=off go run ./cmd/samgoventity-import process  --env .env
GOWORK=off go run ./cmd/samgoventity-import run      --env .env
```

## Live Tests

Gated behind the `integration` build tag, an env switch, and a real key
(`SAM_GOV_ENTITY_API_KEY`). The smoke test is verified to pass end-to-end:

```sh
COUNTRYDATA_SAM_GOV_ENTITY_LIVE=1 \
SAM_GOV_ENTITY_API_KEY=... \
GOWORK=off go test -tags=integration ./united_states/samgoventity/... -run TestLiveSamGovEntitySmoke -count=1 -v

COUNTRYDATA_SAM_GOV_ENTITY_LIVE_FULL=1 \
SAM_GOV_ENTITY_API_KEY=... \
GOWORK=off go test -tags=integration ./united_states/samgoventity/... -run TestLiveSamGovEntityFullDataset -count=1 -v
```
