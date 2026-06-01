# Adapter DB Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread a `config` dict from `CrawlRequest` through to each country adapter, so API URL, page size, and auth env-var name are read from the database-seeded `data_sources.config` column instead of hardcoded ClassVars.

**Architecture:** The crawler stays stateless. Config flows DB → scheduler → `POST /crawl/{source_name}` body → adapter. The abstract `SourceAdapter.crawl()` gains a `config: dict[str, Any] | None = None` parameter; each country adapter reads from it with `or self.ClassVar` fallback. Four other adapters (GLEIF, Wikidata, OpenCorporates, Crawl4AI) add the parameter but ignore it.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, httpx, pytest, respx, pytest-asyncio (auto mode)

---

## File Summary

| File | Change |
|------|--------|
| `crawler/main.py` | Add `config` field to `CrawlRequest`; pass to `adapter.crawl()` |
| `crawler/adapters/base.py` | Add `config` param to abstract `crawl()` |
| `crawler/adapters/api/gleif.py` | Add `config=None` to crawl signature |
| `crawler/adapters/api/wikidata.py` | Add `config=None` to crawl signature |
| `crawler/adapters/api/opencorporates.py` | Add `config=None` to crawl signature |
| `crawler/adapters/crawl4ai/generic.py` | Add `config=None` to crawl signature |
| `crawler/adapters/api/countries/uk.py` | Read api_url, page_size, auth_env from config |
| `crawler/adapters/api/countries/no.py` | Read api_url, page_size from config |
| `crawler/adapters/api/countries/dk.py` | Read api_url, page_size, auth_env from config |
| `crawler/adapters/api/countries/ee.py` | Read data_url (via api_url key) from config |
| `crawler/tests/conftest.py` | Add 4 config fixtures |
| `crawler/tests/test_companies_house_config.py` | New — 2 tests |
| `crawler/tests/test_brreg_config.py` | New — 2 tests |
| `crawler/tests/test_cvr_config.py` | New — 2 tests |
| `crawler/tests/test_ariregister_config.py` | New — 2 tests |

---

### Task 1: Update abstract interface and non-country adapters

Update `CrawlRequest`, the abstract `SourceAdapter.crawl()`, the route handler, and the four adapters that accept but ignore config. This must land first — the country adapter tests call `crawl(..., config=...)` and need the abstract signature in place.

**Files:**
- Modify: `crawler/main.py`
- Modify: `crawler/adapters/base.py`
- Modify: `crawler/adapters/api/gleif.py`
- Modify: `crawler/adapters/api/wikidata.py`
- Modify: `crawler/adapters/api/opencorporates.py`
- Modify: `crawler/adapters/crawl4ai/generic.py`

- [ ] **Step 1: Update `CrawlRequest` and route handler in `crawler/main.py`**

Replace the `CrawlRequest` class and the `crawl` route:

```python
# Old CrawlRequest (line 44-47):
class CrawlRequest(BaseModel):
    since: datetime | None = None
    cursor: str | None = None
    page: int = 1

# New:
class CrawlRequest(BaseModel):
    since: datetime | None = None
    cursor: str | None = None
    page: int = 1
    config: dict[str, Any] | None = None
```

Add `Any` to the existing import at the top of `main.py`:
```python
from __future__ import annotations

from datetime import datetime
from typing import Any
import logging
```

Update the route handler (line 93-102) to pass config:
```python
@app.post("/crawl/{source_name}", response_model=CrawlResponse)
async def crawl(source_name: str, req: CrawlRequest) -> CrawlResponse:
    adapter = registry.get_adapter(source_name)
    if adapter is None:
        raise HTTPException(status_code=404, detail=f"source not found: {source_name}")
    try:
        return await adapter.crawl(req.since, req.cursor, req.page, req.config)
    except Crawl4AIUnconfiguredError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
```

- [ ] **Step 2: Update abstract `crawl()` in `crawler/adapters/base.py`**

Add `from typing import Any` to the existing imports (it's already present via `from typing import Any, ClassVar`).

Replace the abstract method (lines 99-107):
```python
@abstractmethod
async def crawl(
    self,
    since: datetime | None,
    cursor: str | None,
    page: int,
    config: dict[str, Any] | None = None,
) -> CrawlResponse:
    """Fetch one page of records. Must be deterministic for a given input."""
    raise NotImplementedError
```

- [ ] **Step 3: Add `config=None` to `GLEIFAdapter.crawl()` in `crawler/adapters/api/gleif.py`**

Find the `crawl` method signature and add the parameter. Locate the line that reads:
```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
    ) -> CrawlResponse:
```

Replace with:
```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
```

Ensure `Any` is imported. The file already has `from typing import Any, ClassVar` — no change needed there.

- [ ] **Step 4: Add `config=None` to `WikidataAdapter.crawl()` in `crawler/adapters/api/wikidata.py`**

Locate the crawl signature and add the parameter:
```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
```

Add `Any` to the typing import if not already present:
```python
from typing import Any, ClassVar
```

- [ ] **Step 5: Add `config=None` to `OpenCorporatesAdapter.crawl()` in `crawler/adapters/api/opencorporates.py`**

The file already imports `Any`. Update crawl signature:
```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
```

- [ ] **Step 6: Add `config=None` to `Crawl4AIGenericAdapter.crawl()` in `crawler/adapters/crawl4ai/generic.py`**

Add `Any` import and update signature:
```python
from typing import Any, ClassVar
```

```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
```

- [ ] **Step 7: Run all existing tests to verify nothing broke**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest -x -q
```

Expected: all previously passing tests still pass. The signature change is backward-compatible — all callers pass positional args and the new `config` kwarg defaults to `None`.

- [ ] **Step 8: Commit**

```bash
git add crawler/main.py crawler/adapters/base.py crawler/adapters/api/gleif.py crawler/adapters/api/wikidata.py crawler/adapters/api/opencorporates.py crawler/adapters/crawl4ai/generic.py
git commit -m "feat(crawler): add config param to CrawlRequest and SourceAdapter.crawl"
```

---

### Task 2: Add config fixtures to conftest.py

Values match `database/migrations/000003_sources_seed.up.sql` exactly.

**Files:**
- Modify: `crawler/tests/conftest.py`

- [ ] **Step 1: Add the four fixtures**

The current `conftest.py` is three lines (sys.path setup). Append:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


@pytest.fixture
def companies_house_config() -> dict:
    return {
        "api_url": "https://api.company-information.service.gov.uk/advanced-search/companies",
        "page_size": 100,
        "auth_env": "COMPANIES_HOUSE_API_KEY",
    }


@pytest.fixture
def brreg_config() -> dict:
    return {
        "api_url": "https://data.brreg.no/enhetsregisteret/api/enheter",
        "page_size": 200,
        "auth_env": None,
    }


@pytest.fixture
def cvr_config() -> dict:
    return {
        "api_url": "https://cvrapi.dk/api",
        "page_size": 100,
        "auth_env": "CVR_API_TOKEN",
    }


@pytest.fixture
def ariregister_config() -> dict:
    return {
        "api_url": (
            "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
            "ettevotja_rekvisiidid__lihtandmed.csv.zip"
        ),
        "page_size": None,
        "auth_env": None,
    }
```

- [ ] **Step 2: Verify fixtures are discovered**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest --fixtures -q 2>&1 | grep "_config"
```

Expected output includes `companies_house_config`, `brreg_config`, `cvr_config`, `ariregister_config`.

- [ ] **Step 3: Commit**

```bash
git add crawler/tests/conftest.py
git commit -m "test(crawler): add DB config fixtures for country adapters"
```

---

### Task 3: Companies House — write tests, verify failing test, update adapter

**Files:**
- Create: `crawler/tests/test_companies_house_config.py`
- Modify: `crawler/adapters/api/countries/uk.py`

- [ ] **Step 1: Write the test file**

Create `crawler/tests/test_companies_house_config.py`:

```python
from __future__ import annotations

import httpx
import pytest
import respx

from adapters.api.countries.uk import CompaniesHouseAdapter

_CH_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
_CUSTOM_URL = "https://custom.example.com/ch-api"

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
API_RESPONSE = {"items": [FIXTURE_COMPANY], "hits": 1}


@respx.mock
async def test_crawl_with_db_config_returns_records(
    companies_house_config: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "testkey")
    respx.get(_CH_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = CompaniesHouseAdapter()
    resp = await adapter.crawl(since=None, cursor=None, page=1, config=companies_house_config)

    assert len(resp.records) >= 1
    rec = resp.records[0]
    assert rec.country_iso2 == "GB"
    assert rec.name != ""


@respx.mock
async def test_crawl_config_overrides_hardcoded_url(
    companies_house_config: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "testkey")
    custom_config = {**companies_house_config, "api_url": _CUSTOM_URL}

    custom_route = respx.get(_CUSTOM_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))
    default_route = respx.get(_CH_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = CompaniesHouseAdapter()
    await adapter.crawl(since=None, cursor=None, page=1, config=custom_config)

    assert custom_route.called
    assert not default_route.called
```

- [ ] **Step 2: Run and verify `test_crawl_config_overrides_hardcoded_url` fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_companies_house_config.py -v
```

Expected:
- `test_crawl_with_db_config_returns_records` — PASSED (config URL equals ClassVar fallback URL)
- `test_crawl_config_overrides_hardcoded_url` — FAILED (respx raises `httpx.ConnectError` or assert fails because custom URL not called)

- [ ] **Step 3: Update `CompaniesHouseAdapter.crawl()` in `crawler/adapters/api/countries/uk.py`**

Add `config` parameter and read values with fallback. Replace the current `crawl` signature and the first few lines of the method body:

```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        api_url = _cfg.get("api_url") or self.endpoint
        page_size = int(_cfg.get("page_size") or self.page_size)
        auth_env = _cfg.get("auth_env") or "COMPANIES_HOUSE_API_KEY"

        api_key = os.getenv(auth_env)
        if not api_key:
            raise RuntimeError(f"{auth_env} is not set — API requires HTTP Basic auth")

        # cursor = "YYYY-MM-DD,N" (incorporated_from date + 0-indexed page within bucket)
        # or None / legacy integer → start from page 0 with no date filter.
        date_cursor: str | None = None
        page_offset: int = 0

        if cursor and "," in cursor:
            parts = cursor.split(",", 1)
            date_cursor = parts[0] or None
            try:
                page_offset = int(parts[1])
            except ValueError:
                page_offset = 0

        start_index = page_offset * page_size
        params: dict[str, Any] = {
            "size": str(page_size),
            "start_index": str(start_index),
            "company_status": "active",
        }
        if date_cursor:
            params["incorporated_from"] = date_cursor

        async with httpx.AsyncClient(timeout=30.0, auth=(api_key, "")) as client:
            resp = await client.get(api_url, params=params, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
            resp.raise_for_status()
            data = resp.json()

        items = data.get("items") or []
        records: list[CompanyRecord] = []
        for item in items:
            locations = []
            addr = item.get("registered_office_address") or {}
            if addr:
                locations.append(CompanyLocation(
                    location_type="registered_address",
                    address_line1=addr.get("address_line_1"),
                    address_line2=addr.get("address_line_2"),
                    city=addr.get("locality"),
                    region=addr.get("region"),
                    postal_code=addr.get("postal_code"),
                    country=addr.get("country"),
                    country_code="GB",
                ))

            founded_year: int | None = None
            date_of_creation = item.get("date_of_creation")
            if date_of_creation:
                try:
                    founded_year = int(str(date_of_creation)[:4])
                except (ValueError, TypeError):
                    pass

            industries = []
            for sic in (item.get("sic_codes") or []):
                if sic:
                    industries.append(str(sic))

            records.append(
                CompanyRecord(
                    name=str(item.get("company_name") or ""),
                    country_iso2="GB",
                    registration_number=str(item.get("company_number") or ""),
                    status=_map_status(item.get("company_status")),
                    raw_data=item,
                    snapshot_hash=compute_hash(item),
                    locations=locations,
                    founded_year=founded_year,
                    industries=industries,
                )
            )

        total = int(data.get("hits") or 0)
        has_more = len(items) == page_size

        next_cursor: str | None = None
        if has_more:
            if page_offset < _CH_MAX_PAGE:
                next_cursor = f"{date_cursor or ''},{page_offset + 1}"
            else:
                last_date = (items[-1].get("date_of_creation") or "") if items else ""
                next_cursor = f"{last_date},0"

        return CrawlResponse(
            records=records,
            has_more=has_more,
            total=total,
            next_cursor=next_cursor,
        )
```

- [ ] **Step 4: Run tests and verify both pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_companies_house_config.py tests/test_companies_house.py -v
```

Expected: all tests PASS (new tests plus existing tests unaffected).

- [ ] **Step 5: Commit**

```bash
git add crawler/tests/test_companies_house_config.py crawler/adapters/api/countries/uk.py
git commit -m "feat(crawler): CompaniesHouseAdapter reads api_url/page_size/auth_env from config"
```

---

### Task 4: Brreg — write tests, verify failing test, update adapter

**Files:**
- Create: `crawler/tests/test_brreg_config.py`
- Modify: `crawler/adapters/api/countries/no.py`

- [ ] **Step 1: Write the test file**

Create `crawler/tests/test_brreg_config.py`:

```python
from __future__ import annotations

import httpx
import respx

from adapters.api.countries.no import BrregAdapter

_BRREG_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"
_CUSTOM_URL = "https://custom.example.com/brreg-api"

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
API_RESPONSE = {
    "_embedded": {"enheter": [FIXTURE_COMPANY]},
    "page": {"totalElements": 1, "totalPages": 1, "size": 200, "number": 0},
}


@respx.mock
async def test_crawl_with_db_config_returns_records(brreg_config: dict) -> None:
    respx.get(_BRREG_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = BrregAdapter()
    resp = await adapter.crawl(since=None, cursor=None, page=1, config=brreg_config)

    assert len(resp.records) >= 1
    rec = resp.records[0]
    assert rec.country_iso2 == "NO"
    assert rec.name != ""


@respx.mock
async def test_crawl_config_overrides_hardcoded_url(brreg_config: dict) -> None:
    custom_config = {**brreg_config, "api_url": _CUSTOM_URL}

    custom_route = respx.get(_CUSTOM_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))
    default_route = respx.get(_BRREG_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = BrregAdapter()
    await adapter.crawl(since=None, cursor=None, page=1, config=custom_config)

    assert custom_route.called
    assert not default_route.called
```

- [ ] **Step 2: Run and verify `test_crawl_config_overrides_hardcoded_url` fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_brreg_config.py -v
```

Expected:
- `test_crawl_with_db_config_returns_records` — PASSED (config URL equals ClassVar fallback)
- `test_crawl_config_overrides_hardcoded_url` — FAILED

- [ ] **Step 3: Update `BrregAdapter.crawl()` in `crawler/adapters/api/countries/no.py`**

Replace the crawl signature and add config reads at the top of the method body:

```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        api_url = _cfg.get("api_url") or self.endpoint
        page_size = int(_cfg.get("page_size") or self.page_size)

        # cursor = "YYYY-MM-DD,N" (date bucket + 0-indexed page within bucket)
        # or None (start from beginning)
        # Legacy integer cursors (page numbers) are reset to the beginning.
        date_cursor: str | None = None
        page_offset: int = 0

        if cursor and "," in cursor:
            parts = cursor.split(",", 1)
            date_cursor = parts[0] or None
            try:
                page_offset = int(parts[1])
            except ValueError:
                page_offset = 0
        # else: legacy int cursor or None → start from page 0 with no date filter

        params: dict[str, Any] = {
            "page": str(page_offset),
            "size": str(page_size),
            "sort": "registreringsdatoEnhetsregisteret,asc",
        }
        if date_cursor:
            params["fraRegistreringsdatoEnhetsregisteret"] = date_cursor

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(api_url, params=params, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
            resp.raise_for_status()
            data = resp.json()
```

Keep the rest of the method (record parsing and pagination) unchanged except for the `page_size` usage. There are no other references to `self.page_size` in the body — only in `params`, which is now updated.

Also add `Any` to the imports (currently missing from `no.py` — the file has `from typing import Any, ClassVar` check first):

Current imports in `no.py`:
```python
from typing import Any, ClassVar
```
Already present — no change needed.

- [ ] **Step 4: Run tests and verify both pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_brreg_config.py tests/test_brreg.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crawler/tests/test_brreg_config.py crawler/adapters/api/countries/no.py
git commit -m "feat(crawler): BrregAdapter reads api_url/page_size from config"
```

---

### Task 5: CVR — write tests, verify failing test, update adapter

**Files:**
- Create: `crawler/tests/test_cvr_config.py`
- Modify: `crawler/adapters/api/countries/dk.py`

- [ ] **Step 1: Write the test file**

Create `crawler/tests/test_cvr_config.py`:

```python
from __future__ import annotations

import httpx
import pytest
import respx

from adapters.api.countries.dk import CVRAdapter

_CVR_URL = "https://cvrapi.dk/api"
_CUSTOM_URL = "https://custom.example.com/cvr-api"

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
API_RESPONSE = [FIXTURE_COMPANY]


@respx.mock
async def test_crawl_with_db_config_returns_records(
    cvr_config: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CVR_API_TOKEN", "testtoken")
    respx.get(_CVR_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = CVRAdapter()
    resp = await adapter.crawl(since=None, cursor=None, page=1, config=cvr_config)

    assert len(resp.records) >= 1
    rec = resp.records[0]
    assert rec.country_iso2 == "DK"
    assert rec.name != ""


@respx.mock
async def test_crawl_config_overrides_hardcoded_url(
    cvr_config: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CVR_API_TOKEN", "testtoken")
    custom_config = {**cvr_config, "api_url": _CUSTOM_URL}

    custom_route = respx.get(_CUSTOM_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))
    default_route = respx.get(_CVR_URL).mock(return_value=httpx.Response(200, json=API_RESPONSE))

    adapter = CVRAdapter()
    await adapter.crawl(since=None, cursor=None, page=1, config=custom_config)

    assert custom_route.called
    assert not default_route.called
```

- [ ] **Step 2: Run and verify `test_crawl_config_overrides_hardcoded_url` fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_cvr_config.py -v
```

Expected:
- `test_crawl_with_db_config_returns_records` — PASSED
- `test_crawl_config_overrides_hardcoded_url` — FAILED

- [ ] **Step 3: Update `CVRAdapter.crawl()` in `crawler/adapters/api/countries/dk.py`**

Replace the entire `crawl` method:

```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        api_url = _cfg.get("api_url") or self.endpoint
        page_size = int(_cfg.get("page_size") or self.page_size)
        auth_env = _cfg.get("auth_env") or "CVR_API_TOKEN"

        token = (os.getenv(auth_env) or "").strip()
        if not token or token.startswith("#"):
            return CrawlResponse(records=[], has_more=False, total=0, next_cursor=None)

        offset = int(cursor) if cursor else 0
        params: dict[str, Any] = {
            "search": "",
            "country": "dk",
            "start": str(offset),
            "token": token,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(api_url, params=params, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
            resp.raise_for_status()
            data = resp.json()

        results = data if isinstance(data, list) else []
        records: list[CompanyRecord] = []
        for item in results:
            enddate = item.get("enddate")
            status = "dissolved" if enddate else "active"

            locations = []
            street = item.get("address")
            city = item.get("city")
            zipcode = item.get("zipcode")
            if street or city:
                locations.append(CompanyLocation(
                    location_type="registered_address",
                    address_line1=street,
                    city=city,
                    postal_code=str(zipcode) if zipcode else None,
                    country="Denmark",
                    country_code="DK",
                ))

            phones = []
            phone = item.get("phone")
            if phone:
                phones.append(CompanyPhone(phone=str(phone), purpose="main"))

            emails = []
            email = item.get("email")
            if email:
                emails.append(CompanyEmail(email=str(email), purpose="general"))

            industries = []
            industry_desc = item.get("industrydesc")
            if industry_desc:
                industries.append(str(industry_desc))

            records.append(
                CompanyRecord(
                    name=str(item.get("name") or ""),
                    country_iso2="DK",
                    registration_number=str(item.get("vat") or ""),
                    status=status,
                    website=item.get("website"),
                    raw_data=item,
                    snapshot_hash=compute_hash(item),
                    locations=locations,
                    phones=phones,
                    emails=emails,
                    industries=industries,
                )
            )

        has_more = len(results) >= page_size
        next_cursor = str(offset + page_size) if has_more else None

        return CrawlResponse(
            records=records,
            has_more=has_more,
            total=-1,
            next_cursor=next_cursor,
        )
```

- [ ] **Step 4: Run tests and verify both pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_cvr_config.py tests/test_cvr.py -v
```

Expected: all tests PASS (existing tests pass because they still use `self.endpoint` via the fallback).

- [ ] **Step 5: Commit**

```bash
git add crawler/tests/test_cvr_config.py crawler/adapters/api/countries/dk.py
git commit -m "feat(crawler): CVRAdapter reads api_url/page_size/auth_env from config"
```

---

### Task 6: Ariregister — write tests, verify failing test, update adapter

**Files:**
- Create: `crawler/tests/test_ariregister_config.py`
- Modify: `crawler/adapters/api/countries/ee.py`

- [ ] **Step 1: Write the test file**

Create `crawler/tests/test_ariregister_config.py`:

```python
from __future__ import annotations

import io
import zipfile

import httpx
import respx

from adapters.api.countries.ee import EstoniaAdapter

_DATA_URL = (
    "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
    "ettevotja_rekvisiidid__lihtandmed.csv.zip"
)
_CUSTOM_URL = "https://custom.example.com/ariregister.zip"

CSV_CONTENT = (
    "ariregistri_kood;nimi;asukoha_ettevotja_aadress;tegevusala_emtak_tekst;staatus\n"
    "12345678;Test OÜ;Testmnt 1 Tallinn;Software;R\n"
)


def _make_zip(csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("companies.csv", csv_content)
    return buf.getvalue()


@respx.mock
async def test_crawl_with_db_config_returns_records(ariregister_config: dict) -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(200, content=_make_zip(CSV_CONTENT)))

    adapter = EstoniaAdapter()
    resp = await adapter.crawl(since=None, cursor=None, page=1, config=ariregister_config)

    assert len(resp.records) >= 1
    rec = resp.records[0]
    assert rec.country_iso2 == "EE"
    assert rec.name != ""


@respx.mock
async def test_crawl_config_overrides_hardcoded_url(ariregister_config: dict) -> None:
    custom_config = {**ariregister_config, "api_url": _CUSTOM_URL}

    custom_route = respx.get(_CUSTOM_URL).mock(
        return_value=httpx.Response(200, content=_make_zip(CSV_CONTENT))
    )
    default_route = respx.get(_DATA_URL).mock(
        return_value=httpx.Response(200, content=_make_zip(CSV_CONTENT))
    )

    adapter = EstoniaAdapter()
    await adapter.crawl(since=None, cursor=None, page=1, config=custom_config)

    assert custom_route.called
    assert not default_route.called
```

- [ ] **Step 2: Run and verify `test_crawl_config_overrides_hardcoded_url` fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_ariregister_config.py -v
```

Expected:
- `test_crawl_with_db_config_returns_records` — PASSED
- `test_crawl_config_overrides_hardcoded_url` — FAILED

- [ ] **Step 3: Update `EstoniaAdapter.crawl()` in `crawler/adapters/api/countries/ee.py`**

Add `Any` to the imports at the top:
```python
from typing import Any, ClassVar
```

Replace the `crawl` method:
```python
    async def crawl(
        self,
        since: datetime | None,
        cursor: str | None,
        page: int,
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        data_url = _cfg.get("api_url") or self.data_url

        async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
            resp = await client.get(
                data_url,
                headers={"User-Agent": _USER_AGENT},
            )
            resp.raise_for_status()

        return await asyncio.to_thread(_parse_csv_zip, resp.content)
```

- [ ] **Step 4: Run tests and verify both pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest tests/test_ariregister_config.py tests/test_ariregister.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Run the full test suite**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/crawler
pytest -v
```

Expected: all tests PASS. Count should be all prior tests plus 8 new ones.

- [ ] **Step 6: Commit**

```bash
git add crawler/tests/test_ariregister_config.py crawler/adapters/api/countries/ee.py
git commit -m "feat(crawler): EstoniaAdapter reads data_url from config api_url key"
```
