# Adapter DB Config + Tests — Design Spec

**Date:** 2026-05-18
**Status:** Approved

## Problem

The four existing country adapters (`companies_house`, `brreg`, `cvr`, `ariregister`) hardcode their API URLs, page sizes, and auth env var names as Python class variables. The `data_sources` table already stores this same information in its `config` JSONB column (populated by migration `000003_sources_seed.up.sql`). The values are duplicated and the adapters ignore the database entirely.

## Goal

1. The scheduler passes `data_sources.config` to the crawler with every crawl request. Adapters use those values instead of their hardcoded defaults, falling back to hardcoded defaults only when config is absent (backward compatibility).
2. Python crawler tests verify each adapter produces valid `CompanyRecord` results when given config that mirrors the database seed data. Tests also confirm the config override is actually used (not silently ignored).

## Non-Goals

- Adding a database connection to the Python crawler service — it stays stateless.
- Changing the scheduler Go code in this task — the scheduler already sends `since`/`cursor`/`page`; adding `config` to the request is a separate scheduler task.
- Modifying adapters beyond the four listed.

## Architecture

The crawler is stateless. Config flows: **DB → scheduler (Go) → crawl request body → adapter**. The scheduler reads `data_sources.config` and includes it in the `POST /crawl/{source_name}` request. The crawler never connects to the database.

```
Scheduler:  SELECT config FROM data_sources WHERE name = 'brreg'
            POST /crawl/brreg  {"page": 1, "since": "...", "config": {"api_url": "...", "page_size": 200}}
                                                                       ↑ from DB
Crawler:    adapter.crawl(since, cursor, page, config={"api_url": ...})
            api_url = config.get("api_url") or self.endpoint   ← uses DB value
```

## Changes

### 1. `CrawlRequest` model

Add one optional field. Location: `crawler/main.py` (where `CrawlRequest` is currently defined).

```python
class CrawlRequest(BaseModel):
    since: datetime | None = None
    cursor: str | None = None
    page: int = 1
    config: dict[str, Any] | None = None   # new — from data_sources.config
```

### 2. `SourceAdapter.crawl()` signature

Add one optional parameter to the abstract method in `crawler/adapters/base.py`:

```python
@abstractmethod
async def crawl(
    self,
    since: datetime | None,
    cursor: str | None,
    page: int,
    config: dict[str, Any] | None = None,
) -> CrawlResponse:
```

Default `None` preserves backward compatibility — existing tests that call `crawl()` without config continue to work.

### 3. Route handler

`main.py` route passes `req.config` through to the adapter:

```python
@app.post("/crawl/{source_name}", response_model=CrawlResponse)
async def crawl(source_name: str, req: CrawlRequest) -> CrawlResponse:
    adapter = registry.get_adapter(source_name)
    ...
    return await adapter.crawl(req.since, req.cursor, req.page, req.config)
```

### 4. Adapter changes (all four)

Each adapter reads config values with fallback to its existing ClassVar default. ClassVars are kept — they serve as documented defaults and fallbacks.

**Pattern used in all adapters:**
```python
_cfg      = config or {}
api_url   = _cfg.get("api_url")   or self.endpoint    # or self.data_url for ariregister
page_size = int(_cfg.get("page_size") or self.page_size)
auth_env  = _cfg.get("auth_env")  or "DEFAULT_ENV_VAR_NAME"
```

**`crawler/adapters/api/countries/uk.py` (CompaniesHouseAdapter):**
- `api_url` from `config["api_url"]` or `self.endpoint`
- `page_size` from `config["page_size"]` or `self.page_size`
- `auth_env` from `config["auth_env"]` or `"COMPANIES_HOUSE_API_KEY"`

**`crawler/adapters/api/countries/no.py` (BrregAdapter):**
- `api_url` from `config["api_url"]` or `self.endpoint`
- `page_size` from `config["page_size"]` or `self.page_size`
- No auth

**`crawler/adapters/api/countries/dk.py` (CVRAdapter):**
- `api_url` from `config["api_url"]` or `self.endpoint`
- `page_size` from `config["page_size"]` or `self.page_size`
- `auth_env` from `config["auth_env"]` or `"CVR_API_TOKEN"`

**`crawler/adapters/api/countries/ee.py` (EstoniaAdapter):**
- `data_url` from `config["api_url"]` or `self.data_url`
- No page_size (bulk download, always full dataset)
- No auth

### 5. All other adapters

`GLEIF`, `Wikidata`, `OpenCorporates`, `Crawl4AIGeneric` must add `config: dict[str, Any] | None = None` to their `crawl()` signatures to satisfy the updated abstract method, but do not need to use the value. A single-line addition each.

## Tests

### Fixtures — `crawler/tests/conftest.py`

Add four fixtures. Values match `database/migrations/000003_sources_seed.up.sql` exactly:

```python
@pytest.fixture
def companies_house_config():
    return {
        "api_url": "https://api.company-information.service.gov.uk/advanced-search/companies",
        "page_size": 100,
        "auth_env": "COMPANIES_HOUSE_API_KEY",
    }

@pytest.fixture
def brreg_config():
    return {
        "api_url": "https://data.brreg.no/enhetsregisteret/api/enheter",
        "page_size": 200,
        "auth_env": None,
    }

@pytest.fixture
def cvr_config():
    return {
        "api_url": "https://cvrapi.dk/api",
        "page_size": 100,
        "auth_env": "CVR_API_TOKEN",
    }

@pytest.fixture
def ariregister_config():
    return {
        "api_url": "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
        "page_size": None,
        "auth_env": None,
    }
```

### New test files

Four new files, one per adapter:

- `crawler/tests/test_companies_house_config.py`
- `crawler/tests/test_brreg_config.py`
- `crawler/tests/test_cvr_config.py`
- `crawler/tests/test_ariregister_config.py`

Each file contains exactly two tests:

1. **`test_crawl_with_db_config_returns_records`** — passes the config fixture in the request body, mocks the external API with `respx`, asserts at least one `CompanyRecord` comes back with the correct `country_iso2` and non-empty `name`.

2. **`test_crawl_config_overrides_hardcoded_url`** — replaces `api_url` in the config fixture with a custom URL, asserts the custom URL is hit (via `respx` call count and URL matching), not the hardcoded ClassVar default.

### Fixture company data per adapter

**companies_house:**
```python
FIXTURE_COMPANY = {
    "company_name": "Test Company Ltd",
    "company_number": "12345678",
    "company_status": "active",
    "company_type": "ltd",
    "date_of_creation": "2020-01-15",
    "registered_office_address": {
        "address_line_1": "1 Test Street",
        "locality": "London",
        "postal_code": "EC1A 1BB",
        "country": "England",
    },
    "sic_codes": ["62012"],
}
API_RESPONSE = {"items": [FIXTURE_COMPANY], "total_results": 1}
```

**brreg:**
```python
FIXTURE_COMPANY = {
    "organisasjonsnummer": "987654321",
    "navn": "Test Norsk AS",
    "registreringsdatoEnhetsregisteret": "2019-03-10",
    "organisasjonsform": {"kode": "AS"},
    "naeringskode1": {"beskrivelse": "Software"},
    "forretningsadresse": {
        "adresse": ["Testveien 1"],
        "postnummer": "0150",
        "poststed": "Oslo",
        "landkode": "NO",
    },
    "antallAnsatte": 5,
    "hjemmeside": "https://testnorsk.no",
    "konkurs": False,
    "underAvvikling": False,
}
API_RESPONSE = {"_embedded": {"enheter": [FIXTURE_COMPANY]}, "page": {"totalElements": 1, "size": 200, "number": 0}}
```

**cvr:**
```python
FIXTURE_COMPANY = {
    "vat": "12345678",
    "name": "Test Dansk ApS",
    "address": "Testvej 1",
    "zipcode": "1000",
    "city": "Copenhagen",
    "phone": "12345678",
    "email": "test@testdansk.dk",
    "industrydesc": "IT services",
    "startdate": "2018-06-01",
    "enddate": None,
}
API_RESPONSE = {"results": [FIXTURE_COMPANY]}
```

**ariregister:** Use the existing `_make_zip()` helper pattern from `test_ariregister.py`:
```python
CSV_CONTENT = (
    "ariregistri_kood;nimi;asukoha_ettevotja_aadress;tegevusala_emtak_tekst;staatus\n"
    "12345678;Test OÜ;Testmnt 1 Tallinn;Software;R\n"
)
```

## Existing tests

No existing tests are modified. New tests are additive.

## File Summary

| File | Change |
|------|--------|
| `crawler/main.py` | Add `config` field to `CrawlRequest`; pass to `adapter.crawl()` |
| `crawler/adapters/base.py` | Add `config` param to abstract `crawl()` |
| `crawler/adapters/api/countries/uk.py` | Read api_url, page_size, auth_env from config |
| `crawler/adapters/api/countries/no.py` | Read api_url, page_size from config |
| `crawler/adapters/api/countries/dk.py` | Read api_url, page_size, auth_env from config |
| `crawler/adapters/api/countries/ee.py` | Read data_url (via api_url key) from config |
| `crawler/adapters/api/gleif.py` | Add `config=None` to crawl signature |
| `crawler/adapters/api/wikidata.py` | Add `config=None` to crawl signature |
| `crawler/adapters/api/opencorporates.py` | Add `config=None` to crawl signature |
| `crawler/adapters/crawl4ai/generic.py` | Add `config=None` to crawl signature |
| `crawler/tests/conftest.py` | Add 4 config fixtures |
| `crawler/tests/test_companies_house_config.py` | New — 2 tests |
| `crawler/tests/test_brreg_config.py` | New — 2 tests |
| `crawler/tests/test_cvr_config.py` | New — 2 tests |
| `crawler/tests/test_ariregister_config.py` | New — 2 tests |
