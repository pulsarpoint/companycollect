# Generic Data Task System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ad-hoc financial enrichment button with a generic task system where any enabled data source can be triggered for individual companies (or bulk), with all results flowing through the existing raw_input → processor → suggestion pipeline.

**Architecture:** A new `source_task_types` table describes what tasks each source supports and which company identifier they need. A new `DataTaskWorker` River job calls a new `/task/{source_name}` crawler endpoint, stores results in the existing `{source}_company_raw_inputs` tables (reusing existing upsert queries), then queues the existing `SourceProcessWorker`. From the company detail page a "Create Task" sheet lists applicable task types filtered by country match and whether the company has the required identifier. Company officers are a first-class data type with their own table and suggestion table.

**Tech Stack:** PostgreSQL + sqlc (Go), River (job queue), Python FastAPI + httpx (crawler), Chi router + pgx/v5 (scheduler API), React Router v7 + shadcn/ui (frontend)

---

## File Map

**Create:**
- `database/migrations/000031_data_task_system.up.sql` / `.down.sql`
- `database/queries/task_types.sql`
- `database/queries/company_officers.sql`
- `crawler/adapters/api/countries/no_fetch.py` — BrregAdapter.fetch_by_ids
- `crawler/adapters/api/countries/uk_fetch.py` — CompaniesHouseAdapter.fetch_by_ids
- `crawler/adapters/api/gleif_fetch.py` — GLEIFAdapter.fetch_by_ids
- `scheduler/internal/workers/upsert.go` — extracted upsertCompanyRecord
- `scheduler/internal/workers/data_task.go` — DataTaskWorker
- `scheduler/internal/httpapi/tasks.go` — new task endpoints
- `ui/app/components/app/company/CreateTaskSheet.tsx`

**Modify:**
- `crawler/adapters/base.py` — add CompanyOfficer, officers field, fetch_by_ids
- `crawler/main.py` — add POST /task/{source_name}
- `scheduler/internal/crawlerclient/client.go` — add CompanyOfficer, RunTask
- `scheduler/internal/workers/source_pull.go` — delegate to upsertCompanyRecord
- `scheduler/internal/workers/workers.go` — add DataTaskArgs, remove EnrichCompanyFinancialsArgs
- `scheduler/internal/workers/brreg_processor.go` — financial data + status suggestions for existing companies
- `scheduler/internal/workers/companies_house_processor.go` — officer suggestions
- `scheduler/internal/app/river.go` — add data_task queue, remove enrich_financials
- `scheduler/internal/httpapi/handlers.go` — swap routes
- `scheduler/internal/httpapi/testhelpers_test.go` — new stub methods
- `ui/app/types/api.ts` — SourceTaskType, CompanyOfficer, OfficerSuggestion
- `ui/app/lib/api.ts` — getCompanyTaskTypes, createTask, getCompanyOfficers
- `ui/app/routes/companies_.$id.tsx` — Create Task button, Officers tab, source chips
- `ui/app/routes/sources_.$name.tsx` — capabilities section
- `ui/app/routes/jobs.tsx` — data_task kind
- `ui/app/routes/review.tsx` — Officer Suggestions tab

**Delete:**
- `scheduler/internal/workers/financial_enrich.go`

---

### Task 1: DB migration 000031 — source_task_types, company_officers, company_officer_suggestions

**Files:**
- Create: `database/migrations/000031_data_task_system.up.sql`
- Create: `database/migrations/000031_data_task_system.down.sql`

- [ ] **Step 1: Write the up migration**

```sql
-- database/migrations/000031_data_task_system.up.sql

-- ── source_task_types ─────────────────────────────────────────────────────────
CREATE TABLE source_task_types (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id        UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    task_type        TEXT NOT NULL,
    display_name     TEXT NOT NULL,
    supports_bulk    BOOLEAN NOT NULL DEFAULT true,
    supports_individual BOOLEAN NOT NULL DEFAULT false,
    required_id_field TEXT,   -- 'registration_number', 'lei', or NULL
    capabilities     TEXT[] NOT NULL DEFAULT '{}',
    enabled          BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_source_task_types UNIQUE (source_id, task_type),
    CONSTRAINT chk_source_task_types_required_id CHECK (
        required_id_field IS NULL
        OR required_id_field IN ('registration_number', 'lei')
    )
);

CREATE INDEX idx_source_task_types_source ON source_task_types(source_id);

-- Seed task types for active sources
INSERT INTO source_task_types (source_id, task_type, display_name, supports_bulk, supports_individual, required_id_field, capabilities)
SELECT id, 'pull_brreg_company', 'Brreg — Company & Financials', true, true, 'registration_number',
       ARRAY['employee_count','revenue','profit','company_name','status']
FROM data_sources WHERE name = 'brreg';

INSERT INTO source_task_types (source_id, task_type, display_name, supports_bulk, supports_individual, required_id_field, capabilities)
SELECT id, 'pull_companies_house_company', 'Companies House — Company & Officers', true, true, 'registration_number',
       ARRAY['employee_count','company_name','status','directors']
FROM data_sources WHERE name = 'companies_house';

INSERT INTO source_task_types (source_id, task_type, display_name, supports_bulk, supports_individual, required_id_field, capabilities)
SELECT id, 'pull_gleif_company', 'GLEIF — Legal Entity', true, true, 'lei',
       ARRAY['company_name','lei','legal_form','status','locations']
FROM data_sources WHERE name = 'gleif';

-- ── company_officers ──────────────────────────────────────────────────────────
CREATE TABLE company_officers (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id   UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    full_name    TEXT NOT NULL,
    role         TEXT NOT NULL DEFAULT 'director',
    appointed_on DATE,
    resigned_on  DATE,
    nationality  TEXT,
    occupation   TEXT,
    source_name  TEXT NOT NULL,
    source_id    UUID REFERENCES data_sources(id) ON DELETE SET NULL,
    evidence     JSONB NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_officers_role CHECK (
        role IN ('director','secretary','cfo','ceo','cto','chair','other')
    ),
    CONSTRAINT chk_company_officers_evidence_object CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX idx_company_officers_company ON company_officers(company_id);
CREATE INDEX idx_company_officers_active  ON company_officers(company_id) WHERE resigned_on IS NULL;

-- ── company_officer_suggestions ───────────────────────────────────────────────
CREATE TABLE company_officer_suggestions (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id       UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    operation        TEXT NOT NULL,
    full_name        TEXT NOT NULL,
    role             TEXT NOT NULL DEFAULT 'director',
    appointed_on     DATE,
    resigned_on      DATE,
    nationality      TEXT,
    occupation       TEXT,
    current_payload  JSONB NOT NULL DEFAULT '{}',
    proposed_payload JSONB NOT NULL,
    confidence       REAL,
    status           TEXT NOT NULL DEFAULT 'pending',
    reviewed_by      TEXT,
    reviewed_at      TIMESTAMPTZ,
    review_note      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_officer_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove')
    ),
    CONSTRAINT chk_company_officer_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_officer_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_officer_suggestions_current_object CHECK (jsonb_typeof(current_payload) = 'object'),
    CONSTRAINT chk_company_officer_suggestions_proposed_object CHECK (jsonb_typeof(proposed_payload) = 'object')
);

CREATE INDEX idx_company_officer_suggestions_review
    ON company_officer_suggestions(company_id, status);
```

- [ ] **Step 2: Write the down migration**

```sql
-- database/migrations/000031_data_task_system.down.sql
DROP TABLE IF EXISTS company_officer_suggestions;
DROP TABLE IF EXISTS company_officers;
DROP TABLE IF EXISTS source_task_types;
```

- [ ] **Step 3: Apply migration**

Run from `scheduler/`:
```bash
make migrate-up
```

Expected: `OK  000031_data_task_system.up.sql`

- [ ] **Step 4: Verify tables exist**

```bash
psql "$DATABASE_URL" -c "\dt source_task_types" -c "\dt company_officers" -c "\dt company_officer_suggestions"
psql "$DATABASE_URL" -c "SELECT task_type, display_name, supports_individual FROM source_task_types ORDER BY task_type;"
```

Expected: 3 rows (brreg, companies_house, gleif).

- [ ] **Step 5: Commit**

```bash
git add database/migrations/000031_data_task_system.up.sql database/migrations/000031_data_task_system.down.sql
git commit -m "feat(db): migration 000031 — source_task_types, company_officers, company_officer_suggestions"
```

---

### Task 2: SQL queries — task types + officers

**Files:**
- Create: `database/queries/task_types.sql`
- Create: `database/queries/company_officers.sql`

- [ ] **Step 1: Write task_types queries**

```sql
-- database/queries/task_types.sql

-- name: ListSourceTaskTypes :many
SELECT stt.*, ds.name AS source_name, ds.display_name AS source_display_name
FROM source_task_types stt
JOIN data_sources ds ON ds.id = stt.source_id
WHERE stt.enabled = true
ORDER BY ds.display_name, stt.display_name;

-- name: GetAvailableTaskTypesForCompany :many
-- Returns individual-capable task types whose source country matches the company
-- and the company has the required identifier.
SELECT stt.*
FROM source_task_types stt
JOIN data_sources ds ON ds.id = stt.source_id
JOIN companies c ON c.id = $1
WHERE stt.supports_individual = true
  AND stt.enabled = true
  AND ds.enabled = true
  AND (ds.country_id IS NULL OR ds.country_id = c.country_id)
  AND (
    (stt.required_id_field = 'registration_number' AND c.registration_number IS NOT NULL)
    OR (stt.required_id_field = 'lei' AND c.lei IS NOT NULL)
    OR stt.required_id_field IS NULL
  )
ORDER BY stt.display_name;

-- name: GetSourceTaskType :one
SELECT stt.*
FROM source_task_types stt
JOIN data_sources ds ON ds.id = stt.source_id
WHERE ds.name = $1 AND stt.task_type = $2 AND stt.enabled = true;
```

- [ ] **Step 2: Write company_officers queries**

```sql
-- database/queries/company_officers.sql

-- name: InsertCompanyOfficer :one
INSERT INTO company_officers (company_id, full_name, role, appointed_on, resigned_on, nationality, occupation, source_name, source_id, evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING *;

-- name: ListCompanyOfficers :many
SELECT co.*, ds.display_name AS source_display_name
FROM company_officers co
LEFT JOIN data_sources ds ON ds.id = co.source_id
WHERE co.company_id = $1
ORDER BY co.resigned_on NULLS LAST, co.full_name;

-- name: InsertCompanyOfficerSuggestion :one
INSERT INTO company_officer_suggestions (
    company_id, operation, full_name, role,
    appointed_on, resigned_on, nationality, occupation,
    current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING *;

-- name: ListPendingCompanyOfficerSuggestions :many
SELECT cos.*, c.name AS company_name
FROM company_officer_suggestions cos
JOIN companies c ON c.id = cos.company_id
WHERE cos.status = 'pending'
ORDER BY cos.created_at DESC
LIMIT $2 OFFSET $1;

-- name: CountPendingCompanyOfficerSuggestions :one
SELECT COUNT(*) FROM company_officer_suggestions WHERE status = 'pending';

-- name: ApproveCompanyOfficerSuggestion :one
-- Inserts into company_officers and marks suggestion approved in a single call.
WITH approved AS (
    UPDATE company_officer_suggestions
    SET status = 'approved', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
    WHERE id = $1 AND status = 'pending'
    RETURNING *
)
INSERT INTO company_officers (company_id, full_name, role, appointed_on, resigned_on, nationality, occupation, source_name, evidence)
SELECT company_id, full_name, role, appointed_on, resigned_on, nationality, occupation,
       'manual', proposed_payload
FROM approved
RETURNING *;

-- name: RejectCompanyOfficerSuggestion :exec
UPDATE company_officer_suggestions
SET status = 'rejected', reviewed_by = $2, reviewed_at = now(), review_note = $3, updated_at = now()
WHERE id = $1 AND status = 'pending';
```

- [ ] **Step 3: Commit**

```bash
git add database/queries/task_types.sql database/queries/company_officers.sql
git commit -m "feat(db): queries for source_task_types and company_officers"
```

---

### Task 3: sqlc generate

**Files:**
- Modify: `scheduler/internal/db/gen/` (generated, do not edit manually)

- [ ] **Step 1: Run sqlc generate**

From `scheduler/`:
```bash
make sqlc-generate
```

Expected: no errors. New files appear in `scheduler/internal/db/gen/`:
- `task_types.sql.go`
- `company_officers.sql.go`
- `querier.go` updated with new methods

- [ ] **Step 2: Verify build still passes**

```bash
GOWORK=off go build ./...
```

Expected: no output (clean build).

- [ ] **Step 3: Commit generated code**

```bash
git add scheduler/internal/db/gen/
git commit -m "chore: sqlc generate — task types and officers queries"
```

---

### Task 4: Crawler base — CompanyOfficer model + fetch_by_ids interface

**Files:**
- Modify: `crawler/adapters/base.py`

- [ ] **Step 1: Write a failing test**

```python
# crawler/tests/test_base_models.py
from datetime import date
from adapters.base import CompanyOfficer, CompanyRecord

def test_company_officer_model():
    o = CompanyOfficer(full_name="Jane Smith", role="director")
    assert o.full_name == "Jane Smith"
    assert o.role == "director"
    assert o.appointed_on is None
    assert o.resigned_on is None

def test_company_record_has_officers_field():
    rec = CompanyRecord(name="Acme", country_iso2="GB", raw_data={}, snapshot_hash="abc")
    assert rec.officers == []

def test_company_record_officers_populated():
    rec = CompanyRecord(
        name="Acme", country_iso2="GB", raw_data={}, snapshot_hash="abc",
        officers=[CompanyOfficer(full_name="Bob Jones", role="secretary")]
    )
    assert len(rec.officers) == 1
    assert rec.officers[0].full_name == "Bob Jones"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd crawler && python -m pytest tests/test_base_models.py -v
```

Expected: `ImportError` or `AttributeError` on `CompanyOfficer`.

- [ ] **Step 3: Add CompanyOfficer and update CompanyRecord in base.py**

In `crawler/adapters/base.py`, add after the `CompanyEmail` class and before `CompanyRecord`:

```python
class CompanyOfficer(BaseModel):
    full_name: str
    role: str = "director"  # director, secretary, cfo, ceo, cto, chair, other
    appointed_on: date | None = None
    resigned_on: date | None = None   # None = currently active
    nationality: str | None = None
    occupation: str | None = None
```

In `CompanyRecord`, add `officers` field after `employee_estimate`:
```python
    employee_estimate: dict = {}
    officers: list[CompanyOfficer] = []
```

Add `date` to the imports at the top of `base.py`:
```python
from datetime import date, datetime
```

Also add an optional `fetch_by_ids` method to `SourceAdapter` (non-abstract, default raises):

```python
    async def fetch_by_ids(
        self,
        ids: list[str],
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        """Fetch specific records by their native IDs (registration number or LEI).

        Override in adapters that support individual record lookup.
        Default raises NotImplementedError — callers must check supports_individual.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support fetch_by_ids")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd crawler && python -m pytest tests/test_base_models.py -v
```

Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add crawler/adapters/base.py crawler/tests/test_base_models.py
git commit -m "feat(crawler): CompanyOfficer model + fetch_by_ids interface on SourceAdapter"
```

---

### Task 5: Crawler — BrregAdapter.fetch_by_ids

**Files:**
- Modify: `crawler/adapters/api/countries/no.py`

The Brreg individual company endpoint: `GET https://data.brreg.no/enhetsregisteret/api/enheter/{org_number}`
Financial data endpoint: `GET https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}`
Financial response is an array; take `[0]` (most recent).

- [ ] **Step 1: Write a failing test**

```python
# crawler/tests/test_brreg_fetch.py
import json
import pytest
import respx
import httpx
from adapters.api.countries.no import BrregAdapter

ENHETER_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/123456789"
REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap/123456789"

ENHETER_RESPONSE = {
    "organisasjonsnummer": "123456789",
    "navn": "Test AS",
    "organisasjonsform": {"kode": "AS"},
    "registreringsdatoEnhetsregisteret": "2010-01-15",
    "antallAnsatte": 42,
    "hjemmeside": "https://test.no",
}

REGNSKAP_RESPONSE = [
    {
        "regnskapsperiode": {"tilDato": "2023-12-31"},
        "resultatregnskapResultat": {
            "driftsresultat": {
                "driftsinntekter": {"sumDriftsinntekter": 5000000.0}
            },
            "ordinaertResultatFoerSkattekostnad": 800000.0
        }
    }
]


@pytest.mark.anyio
@respx.mock
async def test_brreg_fetch_by_ids_returns_record():
    respx.get(ENHETER_URL).mock(return_value=httpx.Response(200, json=ENHETER_RESPONSE))
    respx.get(REGNSKAP_URL).mock(return_value=httpx.Response(200, json=REGNSKAP_RESPONSE))

    adapter = BrregAdapter()
    resp = await adapter.fetch_by_ids(["123456789"])

    assert len(resp.records) == 1
    assert resp.has_more is False
    rec = resp.records[0]
    assert rec.name == "Test AS"
    assert rec.registration_number == "123456789"
    assert rec.country_iso2 == "NO"
    assert rec.raw_data["antallAnsatte"] == 42
    # Financial data embedded in raw_data
    assert rec.raw_data["financials"]["year"] == 2023
    assert rec.raw_data["financials"]["revenue"] == 5000000.0


@pytest.mark.anyio
@respx.mock
async def test_brreg_fetch_by_ids_not_found():
    respx.get(ENHETER_URL).mock(return_value=httpx.Response(404))
    respx.get(REGNSKAP_URL).mock(return_value=httpx.Response(404))

    adapter = BrregAdapter()
    resp = await adapter.fetch_by_ids(["123456789"])

    assert resp.records == []
    assert resp.has_more is False
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd crawler && python -m pytest tests/test_brreg_fetch.py -v
```

Expected: `NotImplementedError`.

- [ ] **Step 3: Implement fetch_by_ids in BrregAdapter**

Add at the end of `BrregAdapter` class in `crawler/adapters/api/countries/no.py`:

```python
    async def fetch_by_ids(
        self,
        ids: list[str],
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        base_url = _cfg.get("api_url") or self.endpoint
        records: list[CompanyRecord] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for org_number in ids:
                rec = await self._fetch_one(client, base_url, org_number)
                if rec is not None:
                    records.append(rec)

        return CrawlResponse(records=records, has_more=False, total=len(records))

    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        org_number: str,
    ) -> "CompanyRecord | None":
        url = f"{base_url}/{org_number}"
        resp = await client.get(url, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()

        # Fetch financial data (best-effort — 404 is fine)
        financials: dict = {}
        fin_url = f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}"
        fin_resp = await client.get(fin_url, headers={"Accept": "application/json"})
        if fin_resp.status_code == 200:
            fin_items = fin_resp.json()
            if fin_items:
                f = fin_items[0]
                year_str = (f.get("regnskapsperiode") or {}).get("tilDato", "")
                year = int(year_str[:4]) if len(year_str) >= 4 else None
                res = f.get("resultatregnskapResultat") or {}
                revenue = ((res.get("driftsresultat") or {}).get("driftsinntekter") or {}).get("sumDriftsinntekter")
                profit = res.get("ordinaertResultatFoerSkattekostnad")
                financials = {"year": year, "revenue": revenue, "profit": profit}

        raw = dict(data)
        if financials:
            raw["financials"] = financials

        return self._map_record(org_number, data, raw)

    def _map_record(self, org_number: str, data: dict, raw: dict) -> "CompanyRecord":
        from adapters.base import CompanyLocation, compute_hash
        import json

        name = data.get("navn") or org_number
        status_raw = (data.get("organisasjonsform") or {}).get("kode", "")
        status = "dissolved" if status_raw in {"SLETTET", "KONKURS"} else "active"

        locations: list[CompanyLocation] = []
        for addr_key in ("forretningsadresse", "postadresse"):
            addr = data.get(addr_key) or {}
            if addr.get("poststed"):
                locations.append(CompanyLocation(
                    location_type="registered_address" if addr_key == "postadresse" else "headquarters",
                    city=addr.get("poststed"),
                    postal_code=addr.get("postnummer"),
                    country="Norway",
                    country_code="NO",
                    source="brreg",
                ))

        industries: list[str] = []
        for key in ("naeringskode1", "naeringskode2", "naeringskode3"):
            n = data.get(key) or {}
            if n.get("beskrivelse"):
                industries.append(n["beskrivelse"])

        employee_estimate: dict = {}
        if data.get("antallAnsatte") is not None:
            employee_estimate = {"count": data["antallAnsatte"], "source": "brreg"}

        payload_hash = compute_hash(raw)
        return CompanyRecord(
            name=name,
            country_iso2="NO",
            registration_number=org_number,
            status=status,
            website=data.get("hjemmeside"),
            raw_data=raw,
            snapshot_hash=payload_hash,
            locations=locations,
            industries=industries,
            employee_estimate=employee_estimate,
        )
```

Also add the import at the top of the file if not already present:
```python
from adapters.base import CompanyLocation, CompanyRecord, CrawlResponse, SourceAdapter, compute_hash
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd crawler && python -m pytest tests/test_brreg_fetch.py -v
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add crawler/adapters/api/countries/no.py crawler/tests/test_brreg_fetch.py
git commit -m "feat(crawler): BrregAdapter.fetch_by_ids — single company + financials"
```

---

### Task 6: Crawler — CompaniesHouseAdapter.fetch_by_ids

**Files:**
- Modify: `crawler/adapters/api/countries/uk.py`

CH endpoints:
- `GET https://api.company-information.service.gov.uk/company/{company_number}` — company profile
- `GET https://api.company-information.service.gov.uk/company/{company_number}/officers` — officers list

Auth: HTTP Basic with API key as username, empty password.

- [ ] **Step 1: Write a failing test**

```python
# crawler/tests/test_ch_fetch.py
import os
import pytest
import respx
import httpx
from adapters.api.countries.uk import CompaniesHouseAdapter

BASE = "https://api.company-information.service.gov.uk"
COMPANY_URL = f"{BASE}/company/12345678"
OFFICERS_URL = f"{BASE}/company/12345678/officers"

COMPANY_RESPONSE = {
    "company_number": "12345678",
    "company_name": "Test Ltd",
    "company_status": "active",
    "type": "ltd",
    "date_of_creation": "2015-03-10",
}

OFFICERS_RESPONSE = {
    "items": [
        {
            "name": "SMITH, John",
            "officer_role": "director",
            "appointed_on": "2015-03-10",
            "nationality": "British",
            "occupation": "Manager",
        },
        {
            "name": "DOE, Jane",
            "officer_role": "secretary",
            "appointed_on": "2015-03-10",
            "resigned_on": "2020-01-01",
        },
    ],
    "total_results": 2,
}


@pytest.mark.anyio
@respx.mock
async def test_ch_fetch_by_ids_returns_record(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    respx.get(COMPANY_URL).mock(return_value=httpx.Response(200, json=COMPANY_RESPONSE))
    respx.get(OFFICERS_URL).mock(return_value=httpx.Response(200, json=OFFICERS_RESPONSE))

    adapter = CompaniesHouseAdapter()
    resp = await adapter.fetch_by_ids(["12345678"])

    assert len(resp.records) == 1
    rec = resp.records[0]
    assert rec.name == "Test Ltd"
    assert rec.registration_number == "12345678"
    assert rec.country_iso2 == "GB"
    assert len(rec.officers) == 2
    assert rec.officers[0].full_name == "SMITH, John"
    assert rec.officers[0].role == "director"
    assert rec.officers[1].resigned_on is not None


@pytest.mark.anyio
@respx.mock
async def test_ch_fetch_by_ids_not_found(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    respx.get(COMPANY_URL).mock(return_value=httpx.Response(404))

    adapter = CompaniesHouseAdapter()
    resp = await adapter.fetch_by_ids(["12345678"])

    assert resp.records == []
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd crawler && python -m pytest tests/test_ch_fetch.py -v
```

Expected: `NotImplementedError`.

- [ ] **Step 3: Implement fetch_by_ids on CompaniesHouseAdapter**

Add at the end of `CompaniesHouseAdapter` class in `crawler/adapters/api/countries/uk.py`:

```python
    async def fetch_by_ids(
        self,
        ids: list[str],
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        _cfg = config or {}
        auth_env = _cfg.get("auth_env") or "COMPANIES_HOUSE_API_KEY"
        api_key = os.getenv(auth_env)
        if not api_key:
            raise RuntimeError(f"{auth_env} is not set")

        records: list[CompanyRecord] = []
        async with httpx.AsyncClient(timeout=30.0, auth=(api_key, "")) as client:
            for number in ids:
                rec = await self._fetch_one_company(client, number)
                if rec is not None:
                    records.append(rec)

        return CrawlResponse(records=records, has_more=False, total=len(records))

    async def _fetch_one_company(
        self,
        client: httpx.AsyncClient,
        company_number: str,
    ) -> "CompanyRecord | None":
        from adapters.base import CompanyOfficer, compute_hash
        from datetime import date

        base = "https://api.company-information.service.gov.uk"
        resp = await client.get(
            f"{base}/company/{company_number}",
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()

        # Fetch officers (best-effort)
        officers: list[CompanyOfficer] = []
        off_resp = await client.get(
            f"{base}/company/{company_number}/officers",
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        )
        if off_resp.status_code == 200:
            for item in (off_resp.json().get("items") or []):
                def _parse_date(s: str | None) -> date | None:
                    if not s:
                        return None
                    try:
                        return date.fromisoformat(s)
                    except ValueError:
                        return None

                role_raw = (item.get("officer_role") or "other").lower()
                role = role_raw if role_raw in {"director", "secretary", "cfo", "ceo", "cto", "chair"} else "other"
                officers.append(CompanyOfficer(
                    full_name=item.get("name") or "",
                    role=role,
                    appointed_on=_parse_date(item.get("appointed_on")),
                    resigned_on=_parse_date(item.get("resigned_on")),
                    nationality=item.get("nationality"),
                    occupation=item.get("occupation"),
                ))

        raw = dict(data)
        raw["officers"] = [o.model_dump(mode="json") for o in officers]

        return CompanyRecord(
            name=data.get("company_name") or company_number,
            country_iso2="GB",
            registration_number=company_number,
            status=_map_status(data.get("company_status")),
            raw_data=raw,
            snapshot_hash=compute_hash(raw),
            officers=officers,
        )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd crawler && python -m pytest tests/test_ch_fetch.py -v
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add crawler/adapters/api/countries/uk.py crawler/tests/test_ch_fetch.py
git commit -m "feat(crawler): CompaniesHouseAdapter.fetch_by_ids — company + officers"
```

---

### Task 7: Crawler — GLEIFAdapter.fetch_by_ids

**Files:**
- Modify: `crawler/adapters/api/gleif.py`

GLEIF endpoint: `GET https://api.gleif.org/api/v1/lei-records/{lei}`

- [ ] **Step 1: Write a failing test**

```python
# crawler/tests/test_gleif_fetch.py
import pytest
import respx
import httpx
from adapters.api.gleif import GLEIFAdapter

LEI = "529900T8BM49AURSDO55"
GLEIF_URL = f"https://api.gleif.org/api/v1/lei-records/{LEI}"

GLEIF_RESPONSE = {
    "data": {
        "type": "lei-records",
        "id": LEI,
        "attributes": {
            "lei": LEI,
            "entity": {
                "legalName": {"name": "ACME Corp"},
                "status": "ACTIVE",
                "legalForm": {"id": "8888"},
                "headquartersAddress": {
                    "addressLines": ["123 Main St"],
                    "city": "London",
                    "postalCode": "EC1A 1BB",
                    "country": "GB",
                }
            },
            "registration": {"status": "ISSUED"}
        }
    }
}


@pytest.mark.anyio
@respx.mock
async def test_gleif_fetch_by_ids_returns_record():
    respx.get(GLEIF_URL).mock(return_value=httpx.Response(200, json=GLEIF_RESPONSE))

    adapter = GLEIFAdapter()
    resp = await adapter.fetch_by_ids([LEI])

    assert len(resp.records) == 1
    rec = resp.records[0]
    assert rec.name == "ACME Corp"
    assert rec.lei == LEI
    assert rec.country_iso2 == "GB"


@pytest.mark.anyio
@respx.mock
async def test_gleif_fetch_by_ids_not_found():
    respx.get(GLEIF_URL).mock(return_value=httpx.Response(404))

    adapter = GLEIFAdapter()
    resp = await adapter.fetch_by_ids([LEI])

    assert resp.records == []
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd crawler && python -m pytest tests/test_gleif_fetch.py -v
```

Expected: `NotImplementedError`.

- [ ] **Step 3: Implement fetch_by_ids on GLEIFAdapter**

In `crawler/adapters/api/gleif.py`, add inside the `GLEIFAdapter` class:

```python
    async def fetch_by_ids(
        self,
        ids: list[str],
        config: dict[str, Any] | None = None,
    ) -> CrawlResponse:
        records: list[CompanyRecord] = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for lei in ids:
                rec = await self._fetch_one_lei(client, lei)
                if rec is not None:
                    records.append(rec)
        return CrawlResponse(records=records, has_more=False, total=len(records))

    async def _fetch_one_lei(
        self,
        client: httpx.AsyncClient,
        lei: str,
    ) -> "CompanyRecord | None":
        url = f"https://api.gleif.org/api/v1/lei-records/{lei}"
        resp = await client.get(url, headers={"Accept": "application/json"})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        attrs = data.get("attributes") or {}
        entity = attrs.get("entity") or {}

        name = (entity.get("legalName") or {}).get("name") or lei
        status_raw = (entity.get("status") or "").upper()
        status = "active" if status_raw == "ACTIVE" else "inactive"
        hq = entity.get("headquartersAddress") or {}
        country_code = (hq.get("country") or "").upper() or "XX"

        locations: list = []
        if hq.get("city"):
            from adapters.base import CompanyLocation
            locations.append(CompanyLocation(
                location_type="headquarters",
                address_line1=", ".join(hq.get("addressLines") or []) or None,
                city=hq.get("city"),
                postal_code=hq.get("postalCode"),
                country_code=country_code,
                source="gleif",
            ))

        raw = attrs
        return CompanyRecord(
            name=name,
            country_iso2=country_code,
            lei=lei,
            status=status,
            raw_data=raw,
            snapshot_hash=compute_hash(raw),
            locations=locations,
        )
```

Make sure `compute_hash` is imported at the top: `from adapters.base import ..., compute_hash`

- [ ] **Step 4: Run test to verify it passes**

```bash
cd crawler && python -m pytest tests/test_gleif_fetch.py -v
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add crawler/adapters/api/gleif.py crawler/tests/test_gleif_fetch.py
git commit -m "feat(crawler): GLEIFAdapter.fetch_by_ids"
```

---

### Task 8: Crawler — POST /task/{source_name} endpoint

**Files:**
- Modify: `crawler/main.py`

- [ ] **Step 1: Add TaskRequest model and endpoint**

In `crawler/main.py`, after the existing imports and models, add:

```python
class TaskRequest(BaseModel):
    task_type: str
    ids: list[str] | None = None   # None = bulk pull
    page: int = 1
    cursor: str | None = None
    config: dict[str, Any] | None = None
```

Then add the endpoint (after the existing `/crawl/{source_name}` route):

```python
@app.post("/task/{source_name}")
async def run_task(source_name: str, req: TaskRequest) -> CrawlResponse:
    """Generic task endpoint.

    If req.ids is None: delegates to adapter.crawl() (bulk paginated pull).
    If req.ids is set: calls adapter.fetch_by_ids(ids) for individual lookup.
    """
    adapter = registry.get(source_name)
    if adapter is None:
        try:
            adapter = Crawl4AIGenericAdapter(source_name=source_name)
        except Crawl4AIUnconfiguredError as exc:
            raise HTTPException(status_code=404, detail=f"unknown source: {source_name}") from exc

    if req.ids is None:
        return await adapter.crawl(
            since=None,
            cursor=req.cursor,
            page=req.page,
            config=req.config,
        )
    else:
        return await adapter.fetch_by_ids(ids=req.ids, config=req.config)
```

- [ ] **Step 2: Write a test for the endpoint**

```python
# crawler/tests/test_task_endpoint.py
import pytest
import respx
import httpx
from httpx import AsyncClient
from main import app

@pytest.mark.anyio
@respx.mock
async def test_task_endpoint_bulk_delegates_to_crawl(monkeypatch):
    """When ids=None, /task/{source} behaves like /crawl/{source}."""
    import os
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test")
    # Mock the CH advanced search endpoint
    respx.get("https://api.company-information.service.gov.uk/advanced-search/companies").mock(
        return_value=httpx.Response(200, json={"items": [], "total_results": 0})
    )
    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/task/companies_house", json={"task_type": "pull_companies_house_company"})
    assert resp.status_code == 200
    data = resp.json()
    assert "records" in data
    assert data["has_more"] is False


@pytest.mark.anyio
@respx.mock
async def test_task_endpoint_with_ids_calls_fetch_by_ids(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test")
    respx.get("https://api.company-information.service.gov.uk/company/12345678").mock(
        return_value=httpx.Response(404)
    )
    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/task/companies_house", json={
            "task_type": "pull_companies_house_company",
            "ids": ["12345678"]
        })
    assert resp.status_code == 200
    data = resp.json()
    assert data["records"] == []
    assert data["has_more"] is False


@pytest.mark.anyio
async def test_task_endpoint_unknown_source():
    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/task/unknown_xyz", json={"task_type": "pull_xyz"})
    assert resp.status_code == 404
```

- [ ] **Step 3: Run tests**

```bash
cd crawler && python -m pytest tests/test_task_endpoint.py -v
```

Expected: `3 passed`.

- [ ] **Step 4: Commit**

```bash
git add crawler/main.py crawler/tests/test_task_endpoint.py
git commit -m "feat(crawler): POST /task/{source_name} — generic task endpoint"
```

---

### Task 9: Go crawlerclient — CompanyOfficer type + RunTask method

**Files:**
- Modify: `scheduler/internal/crawlerclient/client.go`

- [ ] **Step 1: Add CompanyOfficer and RunTask**

In `crawlerclient/client.go`, add after the `CompanyRecord` struct:

```go
// CompanyOfficer mirrors the Python CompanyOfficer model.
type CompanyOfficer struct {
	FullName    string  `json:"full_name"`
	Role        string  `json:"role"`
	AppointedOn *string `json:"appointed_on,omitempty"` // "YYYY-MM-DD"
	ResignedOn  *string `json:"resigned_on,omitempty"`  // nil = still active
	Nationality *string `json:"nationality,omitempty"`
	Occupation  *string `json:"occupation,omitempty"`
}
```

Add `Officers []CompanyOfficer` to `CompanyRecord` after `EmployeeEstimate`:

```go
	EmployeeEstimate   map[string]any    `json:"employee_estimate,omitempty"`
	Officers           []CompanyOfficer  `json:"officers,omitempty"`
```

Add the `RunTask` method after the existing `Crawl` method:

```go
// TaskRequest is the request body for POST /task/{source_name}.
type TaskRequest struct {
	TaskType string   `json:"task_type"`
	IDs      []string `json:"ids,omitempty"` // nil/empty = bulk pull
	Page     int      `json:"page,omitempty"`
}

// RunTask calls POST /task/{source} with optional IDs.
// If ids is empty it triggers a bulk pull (page 1, no cursor).
// If ids is non-empty it fetches specific records by their native IDs.
func (c *Client) RunTask(ctx context.Context, source, taskType string, ids []string) (*CrawlResponse, error) {
	body := TaskRequest{
		TaskType: taskType,
		Page:     1,
	}
	if len(ids) > 0 {
		body.IDs = ids
	}

	path := fmt.Sprintf("/task/%s", source)
	var result CrawlResponse
	if err := c.postJSON(ctx, path, body, &result); err != nil {
		return nil, errors.Wrap(err, "crawler POST /task/"+source)
	}
	return &result, nil
}
```

- [ ] **Step 2: Build to verify**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output.

- [ ] **Step 3: Write a test**

```go
// scheduler/internal/crawlerclient/client_test.go — add this test:

func TestRunTaskWithIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/task/brreg" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		ids, _ := body["ids"].([]any)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, "123456789", ids[0])

		json.NewEncoder(w).Encode(map[string]any{
			"records":  []any{},
			"has_more": false,
			"total":    0,
		})
	}))
	defer srv.Close()

	c := New(srv.URL)
	resp, err := c.RunTask(context.Background(), "brreg", "pull_brreg_company", []string{"123456789"})
	require.NoError(t, err)
	assert.False(t, resp.HasMore)
}
```

- [ ] **Step 4: Run the test**

```bash
cd scheduler && GOWORK=off go test ./internal/crawlerclient/... -v -run TestRunTask
```

Expected: `PASS`.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/crawlerclient/client.go
git commit -m "feat(crawlerclient): CompanyOfficer type + RunTask method"
```

---

### Task 10: Go workers — Extract upsertCompanyRecord to workers/upsert.go

The `upsertRecord` method is currently private on `*SourcePullWorker`. Both `SourcePullWorker` and the new `DataTaskWorker` need it. Extract it to a package-level function.

**Files:**
- Create: `scheduler/internal/workers/upsert.go`
- Modify: `scheduler/internal/workers/source_pull.go`

- [ ] **Step 1: Create workers/upsert.go**

```go
package workers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// upsertCompanyRecord stores a single CompanyRecord in the appropriate raw_inputs table.
// Returns (inserted, updated, unchanged, err). Shared by SourcePullWorker and DataTaskWorker.
func upsertCompanyRecord(ctx context.Context, q db.Querier, sourceName string, runID uuid.UUID, rec crawlerclient.CompanyRecord) (inserted, updated, unchanged int, err error) {
	raw, marshalErr := json.Marshal(rec.RawData)
	if marshalErr != nil {
		return 0, 0, 0, errors.Wrap(marshalErr, "marshal raw_data")
	}
	hash := rec.SnapshotHash

	switch sourceName {
	case "gleif":
		if rec.LEI == nil || *rec.LEI == "" {
			return 0, 0, 0, errors.New("gleif record missing lei")
		}
		lei := *rec.LEI
		regStatus, _ := rec.RawData["registration_status"].(string)
		hqCountry, _ := rec.RawData["headquarters_country_code"].(string)
		parentLEI, _ := rec.RawData["direct_parent_lei"].(string)
		ultimateLEI, _ := rec.RawData["ultimate_parent_lei"].(string)
		row, e := q.UpsertGLEIFCompanyRawInput(ctx, db.UpsertGLEIFCompanyRawInputParams{
			SourcePullRunID:         runID,
			SourceNativeID:          lei,
			Lei:                     lei,
			LegalName:               ptrStr(rec.Name),
			RegistrationStatus:      ptrStr(regStatus),
			HeadquartersCountryCode: ptrStr(hqCountry),
			ParentLei:               ptrStr(parentLEI),
			UltimateParentLei:       ptrStr(ultimateLEI),
			SourceUpdatedAt:         pgtype.Timestamptz{},
			RawPayload:              raw,
			PayloadHash:             hash,
		})
		if e != nil {
			return 0, 0, 0, errors.Wrap(e, "upsert gleif")
		}
		if row.LastSeenAt.Equal(row.FirstSeenAt) {
			return 1, 0, 0, nil
		}
		if row.ProcessingStatus == "pending" {
			return 0, 1, 0, nil
		}
		return 0, 0, 1, nil

	case "companies_house":
		if rec.RegistrationNumber == nil || *rec.RegistrationNumber == "" {
			return 0, 0, 0, errors.New("companies_house record missing registration_number")
		}
		num := *rec.RegistrationNumber
		companyType, _ := rec.RawData["type"].(string)
		_, e := q.UpsertCompaniesHouseRawInput(ctx, db.UpsertCompaniesHouseRawInputParams{
			SourcePullRunID: runID,
			SourceNativeID:  num,
			CompanyName:     ptrStr(rec.Name),
			CompanyStatus:   ptrStr(rec.Status),
			CompanyType:     ptrStr(companyType),
			SourceUpdatedAt: pgtype.Timestamptz{},
			RawPayload:      raw,
			PayloadHash:     hash,
		})
		if e != nil {
			return 0, 0, 0, errors.Wrap(e, "upsert companies_house")
		}
		return 1, 0, 0, nil

	case "brreg":
		if rec.RegistrationNumber == nil || *rec.RegistrationNumber == "" {
			return 0, 0, 0, errors.New("brreg record missing registration_number")
		}
		num := *rec.RegistrationNumber
		website := ""
		if rec.Website != nil {
			website = *rec.Website
		}
		_, e := q.UpsertBrregRawInput(ctx, db.UpsertBrregRawInputParams{
			SourcePullRunID:  runID,
			SourceNativeID:   num,
			OrganizationName: ptrStr(rec.Name),
			Website:          ptrStr(website),
			SourceUpdatedAt:  pgtype.Timestamptz{},
			RawPayload:       raw,
			PayloadHash:      hash,
		})
		if e != nil {
			return 0, 0, 0, errors.Wrap(e, "upsert brreg")
		}
		return 1, 0, 0, nil

	default:
		return 0, 0, 0, fmt.Errorf("unknown source: %s", sourceName)
	}
}
```

- [ ] **Step 2: Update source_pull.go to delegate to upsertCompanyRecord**

In `scheduler/internal/workers/source_pull.go`, find the existing `upsertRecord` method on `*SourcePullWorker` and replace its entire body with a call to the new function:

```go
func (w *SourcePullWorker) upsertRecord(ctx context.Context, sourceName string, runID uuid.UUID, rec crawlerclient.CompanyRecord) (inserted, updated, unchanged int, err error) {
	return upsertCompanyRecord(ctx, w.db, sourceName, runID, rec)
}
```

Remove the old duplicated switch statement from `source_pull.go` (the entire body that was there before).

- [ ] **Step 3: Build and test**

```bash
cd scheduler && GOWORK=off go build ./... && GOWORK=off go test ./internal/workers/... -v -count=1
```

Expected: all existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/workers/upsert.go scheduler/internal/workers/source_pull.go
git commit -m "refactor(workers): extract upsertCompanyRecord to package-level function"
```

---

### Task 11: Go workers — DataTaskArgs + DataTaskWorker

**Files:**
- Modify: `scheduler/internal/workers/workers.go`
- Create: `scheduler/internal/workers/data_task.go`

- [ ] **Step 1: Add DataTaskArgs to workers.go**

In `scheduler/internal/workers/workers.go`, add:

```go
// DataTaskArgs is the job argument for a generic data task.
// If IDs is empty, the worker performs a bulk pull (page 1).
// If IDs is non-empty, the worker fetches those specific records.
type DataTaskArgs struct {
	SourceName string   `json:"source_name"`
	TaskType   string   `json:"task_type"`
	IDs        []string `json:"ids"` // empty = bulk
}

func (DataTaskArgs) Kind() string { return "data_task" }
```

Also, `EnrichCompanyFinancialsArgs` can stay for now (removal is Task 15).

- [ ] **Step 2: Write a failing test for DataTaskWorker**

```go
// scheduler/internal/workers/data_task_test.go
package workers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func TestDataTaskWorker_IndividualFetch(t *testing.T) {
	// Crawler returns one brreg record for org 123456789
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/task/brreg" {
			http.Error(w, "not found", 404)
			return
		}
		regNum := "123456789"
		json.NewEncoder(w).Encode(map[string]any{
			"records": []map[string]any{{
				"name":                "Test AS",
				"country_iso2":        "NO",
				"registration_number": regNum,
				"status":              "active",
				"raw_data":            map[string]any{"organisasjonsnummer": regNum},
				"snapshot_hash":       "abc123",
			}},
			"has_more": false,
			"total":    1,
		})
	}))
	defer srv.Close()

	stub := newStubQuerier()
	crawler := crawlerclient.New(srv.URL)
	w := workers.NewDataTaskWorker(stub, crawler, nil)

	job := &river.Job[workers.DataTaskArgs]{
		Args: workers.DataTaskArgs{
			SourceName: "brreg",
			TaskType:   "pull_brreg_company",
			IDs:        []string{"123456789"},
		},
	}
	err := w.Work(context.Background(), job)
	require.NoError(t, err)
	assert.Equal(t, 1, stub.brregUpsertCount)
}
```

(You will need to add `brregUpsertCount` to the `stubQuerier` in Task 17. For now just confirm it compiles with a placeholder.)

- [ ] **Step 3: Implement DataTaskWorker**

Create `scheduler/internal/workers/data_task.go`:

```go
package workers

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// DataTaskWorker handles generic data tasks for any source.
// It calls the crawler's /task endpoint, stores raw inputs, and
// queues the source processor.
type DataTaskWorker struct {
	river.WorkerDefaults[DataTaskArgs]
	db      db.Querier
	crawler *crawlerclient.Client
	pool    *pgxpool.Pool
	rv      *river.Client[pgx.Tx]
}

func NewDataTaskWorker(q db.Querier, crawler *crawlerclient.Client, pool *pgxpool.Pool) *DataTaskWorker {
	return &DataTaskWorker{db: q, crawler: crawler, pool: pool}
}

func (w *DataTaskWorker) SetRiverClient(rc *river.Client[pgx.Tx]) {
	w.rv = rc
}

func (w *DataTaskWorker) Work(ctx context.Context, job *river.Job[DataTaskArgs]) error {
	args := job.Args

	src, err := w.db.GetSourceByName(ctx, args.SourceName)
	if err != nil {
		return errors.Wrap(err, "get source")
	}

	triggerType := "scheduled"
	if len(args.IDs) > 0 {
		triggerType = "manual"
	}
	riverJobID := job.ID
	run, err := w.db.CreatePullRun(ctx, db.CreatePullRunParams{
		Name:        src.Name,
		RiverJobID:  &riverJobID,
		TaskType:    args.Kind(),
		TriggerType: triggerType,
	})
	if err != nil {
		return errors.Wrap(err, "create pull run")
	}

	resp, err := w.crawler.RunTask(ctx, args.SourceName, args.TaskType, args.IDs)
	if err != nil {
		errMsg := err.Error()
		_ = w.db.FailPullRun(ctx, db.FailPullRunParams{ID: run.ID, ErrorMessage: &errMsg})
		slog.Error("data task: crawler call failed", "source", args.SourceName, "task_type", args.TaskType, "job_id", job.ID, "error", err)
		return err
	}

	var inserted, updated, unchanged int
	for _, rec := range resp.Records {
		ins, upd, unch, upsertErr := upsertCompanyRecord(ctx, w.db, args.SourceName, run.ID, rec)
		if upsertErr != nil {
			slog.Error("data task: upsert failed", "source", args.SourceName, "record", rec.Name, "error", upsertErr)
			continue
		}
		inserted += ins
		updated += upd
		unchanged += unch
	}

	_ = w.db.SucceedPullRun(ctx, db.SucceedPullRunParams{
		ID:               run.ID,
		RowsSeen:         int32(len(resp.Records)),
		RawRowsInserted:  int32(inserted),
		RawRowsUpdated:   int32(updated),
		RawRowsUnchanged: int32(unchanged),
	})

	// Queue the processor to handle newly inserted raw inputs.
	if w.rv != nil && (inserted+updated) > 0 {
		_, qErr := w.rv.Insert(ctx, SourceProcessArgs{
			SourceName: args.SourceName,
			PullRunID:  run.ID.String(),
		}, nil)
		if qErr != nil {
			slog.Warn("data task: failed to queue source process", "source", args.SourceName, "error", qErr)
		}
	}

	slog.Info("data task complete",
		"source", args.SourceName, "task_type", args.TaskType,
		"ids_count", len(args.IDs),
		"inserted", inserted, "updated", updated, "unchanged", unchanged,
	)
	return nil
}
```

- [ ] **Step 4: Build to verify**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/workers/workers.go scheduler/internal/workers/data_task.go
git commit -m "feat(workers): DataTaskArgs + DataTaskWorker — generic source task"
```

---

### Task 12: Go app — River config update

**Files:**
- Modify: `scheduler/internal/app/river.go`

- [ ] **Step 1: Register DataTaskWorker, remove enrich_financials queue**

In `scheduler/internal/app/river.go`:

1. Add `dataTaskWorker := workers.NewDataTaskWorker(q, crawler, pool)` after the existing worker creations.
2. Add `river.AddWorker(w, dataTaskWorker)` after the existing AddWorker calls.
3. In `riverCfg.Queues`, add `"data_task": {MaxWorkers: 5}` and remove `"enrich_financials": {MaxWorkers: 2}`.
4. Add `dataTaskWorker.SetRiverClient(rc)` after `sourcePullWorker.SetRiverClient(rc)`.

The final `riverCfg.Queues` should be:
```go
Queues: map[string]river.QueueConfig{
    "source_pull":    {MaxWorkers: cfg.CrawlConcurrency},
    "source_process": {MaxWorkers: cfg.DomainConcurrency},
    "domain_crawl":   {MaxWorkers: 3},
    "domain_import":  {MaxWorkers: 2},
    "data_task":      {MaxWorkers: 5},
},
```

- [ ] **Step 2: Build and verify**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add scheduler/internal/app/river.go
git commit -m "feat(app): register DataTaskWorker, replace enrich_financials queue with data_task"
```

---

### Task 13: Go workers — Update BrregProcessor for financial data + status suggestions

When a Brreg record already has an associated company, the processor should:
1. Create a `company_status_suggestion` if the name or status differs.
2. Create a `company_financial` record if financial data is present in `raw_payload`.

**Files:**
- Modify: `scheduler/internal/workers/brreg_processor.go`

- [ ] **Step 1: Write a failing test for the financial data path**

In `scheduler/internal/workers/brreg_processor_test.go`, add:

```go
func TestBrregProcessorCreatesFinancialForExistingCompany(t *testing.T) {
	// Build raw_payload that includes financials
	raw, _ := json.Marshal(map[string]any{
		"organisasjonsnummer": "111222333",
		"navn":                "Existing AS",
		"financials": map[string]any{
			"year":    2023,
			"revenue": 5000000.0,
			"profit":  800000.0,
		},
	})
	// stub should return an existing company for "111222333"
	// and expect CreateCompanyFinancial to be called
	// (adjust stub as needed after Task 17)
	_ = raw
	t.Skip("wire up after stub supports CreateCompanyFinancial")
}
```

(This test is intentionally skipped until the stub is wired up in Task 17. The important part is that the implementation below is correct.)

- [ ] **Step 2: Update BrregProcessor.processOne**

In `brreg_processor.go`, find the `else if err != nil` / `else` block in `processOne` and extend it. After the existing `else if row.CompanyStatus != nil` block for the existing company case, add financial handling:

```go
} else {
	// Existing company: create status suggestion if name changed
	if row.OrganizationName != nil && *row.OrganizationName != existing.Name {
		current, _ := json.Marshal(map[string]any{"legal_name": existing.Name})
		proposed, _ := json.Marshal(map[string]any{"legal_name": *row.OrganizationName})
		sug, err := p.db.InsertCompanyStatusSuggestion(ctx, db.InsertCompanyStatusSuggestionParams{
			CompanyID:       pgtype.UUID{Bytes: existing.ID, Valid: true},
			Operation:       "update",
			StatusField:     "legal_name",
			CurrentValue:    &existing.Name,
			ProposedValue:   row.OrganizationName,
			CurrentPayload:  current,
			ProposedPayload: proposed,
			Confidence:      ptrFloat32(0.85),
		})
		if err != nil {
			return errors.Wrap(err, "insert name suggestion")
		}
		if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
			SuggestionTable:  "company_status_suggestions",
			SuggestionID:     sug.ID,
			SourceID:         src.ID,
			SourceInputTable: "brreg_company_raw_inputs",
			SourceInputKey:   row.ID.String(),
			SourcePullRunID:  pgtype.UUID{Bytes: row.SourcePullRunID, Valid: true},
		}); err != nil {
			return errors.Wrap(err, "insert source link for name suggestion")
		}
	}

	// Financial data — present when row came from a DataTask fetch
	var rawMap map[string]any
	if err := json.Unmarshal(row.RawPayload, &rawMap); err == nil {
		if fin, ok := rawMap["financials"].(map[string]any); ok {
			year := int32(0)
			if y, ok := fin["year"].(float64); ok {
				year = int32(y)
			}
			if year > 0 {
				currency := "NOK"
				var revPtr, profPtr *int64
				if rev, ok := fin["revenue"].(float64); ok {
					v := int64(rev * 100)
					revPtr = &v
				}
				if prof, ok := fin["profit"].(float64); ok {
					v := int64(prof * 100)
					profPtr = &v
				}
				if _, err := p.db.CreateCompanyFinancial(ctx, db.CreateCompanyFinancialParams{
					CompanyID:       existing.ID,
					Year:            year,
					SourceName:      src.Name,
					RevenueAmount:   revPtr,
					RevenueCurrency: &currency,
					ProfitAmount:    profPtr,
				}); err != nil {
					slog.Warn("brreg processor: create financial failed", "company_id", existing.ID, "year", year, "error", err)
				}
			}
		}
	}
}
```

Replace the old `else if row.CompanyStatus != nil { ... }` block (which was the only existing-company handler) with this full `else` block that handles both status AND financial data.

- [ ] **Step 3: Build**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output.

- [ ] **Step 4: Run existing brreg processor tests**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -run TestBrreg -v
```

Expected: all pass (the new test is skipped).

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/workers/brreg_processor.go scheduler/internal/workers/brreg_processor_test.go
git commit -m "feat(workers): BrregProcessor — financial data + name change suggestions for existing companies"
```

---

### Task 14: Go workers — Update CompaniesHouseProcessor for officer suggestions

When a CH record has officers in `raw_payload.officers`, create `company_officer_suggestions` for each.

**Files:**
- Modify: `scheduler/internal/workers/companies_house_processor.go`

- [ ] **Step 1: Update processOne in CompaniesHouseProcessor**

In `companies_house_processor.go`, after the existing `else if err != nil` / status suggestion block for existing companies, add:

```go
	// Officers — present when row came from a DataTask fetch
	var rawMap map[string]any
	if jsonErr := json.Unmarshal(row.RawPayload, &rawMap); jsonErr == nil {
		if officers, ok := rawMap["officers"].([]any); ok {
			for _, o := range officers {
				om, ok := o.(map[string]any)
				if !ok {
					continue
				}
				fullName, _ := om["full_name"].(string)
				role, _ := om["role"].(string)
				if fullName == "" {
					continue
				}
				if role == "" {
					role = "director"
				}
				proposed, _ := json.Marshal(om)
				sug, err := p.db.InsertCompanyOfficerSuggestion(ctx, db.InsertCompanyOfficerSuggestionParams{
					CompanyID:       pgtype.UUID{Bytes: existing.ID, Valid: true},
					Operation:       "add",
					FullName:        fullName,
					Role:            role,
					CurrentPayload:  []byte("{}"),
					ProposedPayload: proposed,
					Confidence:      ptrFloat32(0.8),
				})
				if err != nil {
					slog.Warn("ch processor: insert officer suggestion failed", "company_id", existing.ID, "name", fullName, "error", err)
					continue
				}
				if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
					SuggestionTable:  "company_officer_suggestions",
					SuggestionID:     sug.ID,
					SourceID:         src.ID,
					SourceInputTable: "companies_house_company_raw_inputs",
					SourceInputKey:   row.ID.String(),
					SourcePullRunID:  pgtype.UUID{Bytes: row.SourcePullRunID, Valid: true},
				}); err != nil {
					slog.Warn("ch processor: insert officer source link failed", "error", err)
				}
			}
		}
	}
```

This goes inside the `else` (existing company) branch, after the status suggestion code.

- [ ] **Step 2: Build and test**

```bash
cd scheduler && GOWORK=off go build ./... && GOWORK=off go test ./internal/workers/... -run TestCompaniesHouse -v
```

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add scheduler/internal/workers/companies_house_processor.go
git commit -m "feat(workers): CompaniesHouseProcessor — officer suggestions from raw_payload.officers"
```

---

### Task 15: Go workers — Remove FinancialEnrichWorker

**Files:**
- Delete: `scheduler/internal/workers/financial_enrich.go`
- Modify: `scheduler/internal/workers/workers.go` (remove EnrichCompanyFinancialsArgs)
- Modify: `scheduler/internal/app/river.go` (already done in Task 12 — verify)

- [ ] **Step 1: Delete the file**

```bash
rm scheduler/internal/workers/financial_enrich.go
```

- [ ] **Step 2: Remove EnrichCompanyFinancialsArgs from workers.go**

Delete these lines from `scheduler/internal/workers/workers.go`:

```go
// EnrichCompanyFinancialsArgs is the job argument for fetching financial data for a company.
type EnrichCompanyFinancialsArgs struct {
	CompanyID  string `json:"company_id"`
	OrgNumber  string `json:"org_number"`
	SourceName string `json:"source_name"`
}

func (EnrichCompanyFinancialsArgs) Kind() string { return "enrich_company_financials" }
```

- [ ] **Step 3: Build to confirm no references remain**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output (clean build, no references to deleted type).

- [ ] **Step 4: Run all tests**

```bash
cd scheduler && GOWORK=off go test ./... 2>&1 | tail -20
```

Expected: all pass except the intentionally-skipped financial test.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(workers): remove FinancialEnrichWorker — financial data now via Brreg DataTask"
```

---

### Task 16: Go API — task-types + create-task + officers endpoints

**Files:**
- Create: `scheduler/internal/httpapi/tasks.go`
- Modify: `scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Create tasks.go**

```go
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

// handleGetCompanyTaskTypes returns task types applicable for a specific company.
// Filters by: supports_individual=true, source country matches company country
// or source is global, and the company has the required identifier.
func (h *Handlers) handleGetCompanyTaskTypes(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	if _, err := h.db.GetCompany(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, "company not found")
		return
	}
	taskTypes, err := h.db.GetAvailableTaskTypesForCompany(r.Context(), id)
	if err != nil {
		slog.Error("get available task types", "company_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if taskTypes == nil {
		taskTypes = []db.SourceTaskType{}
	}
	writeJSON(w, http.StatusOK, taskTypes)
}

// handleCreateTask queues a DataTaskArgs River job for a company + task type.
func (h *Handlers) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CompanyID string `json:"company_id"`
		SourceName string `json:"source_name"`
		TaskType   string `json:"task_type"`
	}
	if err := decodeJSON(r, &body); err != nil || body.CompanyID == "" || body.SourceName == "" || body.TaskType == "" {
		writeError(w, http.StatusBadRequest, "company_id, source_name and task_type are required")
		return
	}
	companyID, err := uuid.Parse(body.CompanyID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company_id")
		return
	}

	company, err := h.db.GetCompany(r.Context(), companyID)
	if err != nil {
		writeError(w, http.StatusNotFound, "company not found")
		return
	}

	// Resolve the ID the task needs based on the task type's required_id_field.
	taskTypeDef, err := h.db.GetSourceTaskType(r.Context(), db.GetSourceTaskTypeParams{
		Name:     body.SourceName,
		TaskType: body.TaskType,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "task type not found or not enabled")
		return
	}

	var ids []string
	if taskTypeDef.RequiredIdField != nil {
		switch *taskTypeDef.RequiredIdField {
		case "registration_number":
			if company.RegistrationNumber == nil {
				writeError(w, http.StatusUnprocessableEntity, "company has no registration number")
				return
			}
			ids = []string{*company.RegistrationNumber}
		case "lei":
			if company.Lei == nil {
				writeError(w, http.StatusUnprocessableEntity, "company has no LEI")
				return
			}
			ids = []string{*company.Lei}
		}
	}

	if h.rv == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler not available")
		return
	}
	job, err := h.rv.Insert(r.Context(), workers.DataTaskArgs{
		SourceName: body.SourceName,
		TaskType:   body.TaskType,
		IDs:        ids,
	}, &river.InsertOpts{Queue: "data_task"})
	if err != nil {
		slog.Error("insert data task", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue task")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": job.Job.ID})
}

// handleGetCompanyOfficers returns approved officers for a company.
func (h *Handlers) handleGetCompanyOfficers(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	officers, err := h.db.ListCompanyOfficers(r.Context(), id)
	if err != nil {
		slog.Error("list company officers", "company_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if officers == nil {
		officers = []db.ListCompanyOfficersRow{}
	}
	writeJSON(w, http.StatusOK, officers)
}

// handleListPendingOfficerSuggestions lists pending officer suggestions for review.
func (h *Handlers) handleListPendingOfficerSuggestions(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := int32((page - 1) * limit)

	suggestions, err := h.db.ListPendingCompanyOfficerSuggestions(r.Context(), db.ListPendingCompanyOfficerSuggestionsParams{
		Offset: offset,
		Limit:  int32(limit),
	})
	if err != nil {
		slog.Error("list officer suggestions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, err := h.db.CountPendingCompanyOfficerSuggestions(r.Context())
	if err != nil {
		slog.Error("count officer suggestions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if suggestions == nil {
		suggestions = []db.ListPendingCompanyOfficerSuggestionsRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": suggestions, "total": total, "page": page, "limit": limit,
	})
}

// handleApproveOfficerSuggestion approves a pending officer suggestion.
func (h *Handlers) handleApproveOfficerSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid suggestion id")
		return
	}
	var body struct {
		Note string `json:"note"`
	}
	_ = decodeJSON(r, &body)

	reviewer := "system"
	officer, err := h.db.ApproveCompanyOfficerSuggestion(r.Context(), db.ApproveCompanyOfficerSuggestionParams{
		ID:         id,
		ReviewedBy: &reviewer,
		ReviewNote: &body.Note,
	})
	if err != nil {
		slog.Error("approve officer suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, officer)
}

// handleRejectOfficerSuggestion rejects a pending officer suggestion.
func (h *Handlers) handleRejectOfficerSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid suggestion id")
		return
	}
	var body struct {
		Note string `json:"note"`
	}
	_ = decodeJSON(r, &body)

	reviewer := "system"
	if err := h.db.RejectCompanyOfficerSuggestion(r.Context(), db.RejectCompanyOfficerSuggestionParams{
		ID:         id,
		ReviewedBy: &reviewer,
		ReviewNote: &body.Note,
	}); err != nil {
		slog.Error("reject officer suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}
```

- [ ] **Step 2: Register new routes in handlers.go**

In `scheduler/internal/httpapi/handlers.go`, inside `RegisterRoutes`, add after the existing company routes:

```go
r.Get("/companies/{id}/task-types", h.handleGetCompanyTaskTypes)
r.Post("/tasks", h.handleCreateTask)
r.Get("/companies/{id}/officers", h.handleGetCompanyOfficers)
r.Get("/review/officers", h.handleListPendingOfficerSuggestions)
r.Post("/review/officers/{id}/approve", h.handleApproveOfficerSuggestion)
r.Post("/review/officers/{id}/reject", h.handleRejectOfficerSuggestion)
```

Also note: the `GetSourceTaskType` sqlc query takes `db.GetSourceTaskTypeParams{Name, TaskType}` — the generated struct field for `name` from the source join is `Name`. Verify the generated struct after `sqlc generate` in Task 3.

- [ ] **Step 3: Build**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add scheduler/internal/httpapi/tasks.go scheduler/internal/httpapi/handlers.go
git commit -m "feat(api): task-types, create-task, officers, and officer suggestion review endpoints"
```

---

### Task 17: Go API — Remove old enrich routes + update testhelpers

**Files:**
- Modify: `scheduler/internal/httpapi/companies.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Remove handleEnrichCompanyFromSource and handleGetCompanyEnrichmentSources from companies.go**

Delete the two functions `handleGetCompanyEnrichmentSources` and `handleEnrichCompanyFromSource` from `scheduler/internal/httpapi/companies.go`. Also remove the `workers` import if it is now unused.

- [ ] **Step 2: Remove those routes from handlers.go**

Delete these two lines from `RegisterRoutes`:
```go
r.Get("/companies/{id}/enrichment-sources", h.handleGetCompanyEnrichmentSources)
r.Post("/companies/{id}/enrich-from-source", h.handleEnrichCompanyFromSource)
```

- [ ] **Step 3: Add new stub methods to testhelpers_test.go**

The `stubQuerier` in `testhelpers_test.go` must implement every method in the `db.Querier` interface. Add stubs for all new methods generated in Task 3. Add the following to the `stubQuerier` struct and its method list:

```go
// In the struct, add counter for testing:
brregUpsertCount int

// New stub methods:
func (s *stubQuerier) ListSourceTaskTypes(ctx context.Context) ([]db.ListSourceTaskTypesRow, error) {
    return nil, nil
}
func (s *stubQuerier) GetAvailableTaskTypesForCompany(ctx context.Context, companyID uuid.UUID) ([]db.SourceTaskType, error) {
    return nil, nil
}
func (s *stubQuerier) GetSourceTaskType(ctx context.Context, arg db.GetSourceTaskTypeParams) (db.SourceTaskType, error) {
    return db.SourceTaskType{}, nil
}
func (s *stubQuerier) InsertCompanyOfficer(ctx context.Context, arg db.InsertCompanyOfficerParams) (db.CompanyOfficer, error) {
    return db.CompanyOfficer{}, nil
}
func (s *stubQuerier) ListCompanyOfficers(ctx context.Context, companyID uuid.UUID) ([]db.ListCompanyOfficersRow, error) {
    return nil, nil
}
func (s *stubQuerier) InsertCompanyOfficerSuggestion(ctx context.Context, arg db.InsertCompanyOfficerSuggestionParams) (db.CompanyOfficerSuggestion, error) {
    return db.CompanyOfficerSuggestion{}, nil
}
func (s *stubQuerier) ListPendingCompanyOfficerSuggestions(ctx context.Context, arg db.ListPendingCompanyOfficerSuggestionsParams) ([]db.ListPendingCompanyOfficerSuggestionsRow, error) {
    return nil, nil
}
func (s *stubQuerier) CountPendingCompanyOfficerSuggestions(ctx context.Context) (int64, error) {
    return 0, nil
}
func (s *stubQuerier) ApproveCompanyOfficerSuggestion(ctx context.Context, arg db.ApproveCompanyOfficerSuggestionParams) (db.CompanyOfficer, error) {
    return db.CompanyOfficer{}, nil
}
func (s *stubQuerier) RejectCompanyOfficerSuggestion(ctx context.Context, arg db.RejectCompanyOfficerSuggestionParams) error {
    return nil
}
```

- [ ] **Step 4: Build and run all tests**

```bash
cd scheduler && GOWORK=off go build ./... && GOWORK=off go test ./... 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/httpapi/companies.go scheduler/internal/httpapi/handlers.go scheduler/internal/httpapi/testhelpers_test.go
git commit -m "feat(api): remove old enrich endpoints, update testhelpers for new querier methods"
```

---

### Task 18: Frontend — New types + API methods

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`

- [ ] **Step 1: Add types to api.ts**

Add these interfaces to `ui/app/types/api.ts`:

```typescript
export interface SourceTaskType {
  id: string;
  source_id: string;
  task_type: string;
  display_name: string;
  supports_bulk: boolean;
  supports_individual: boolean;
  required_id_field: string | null;
  capabilities: string[];
  enabled: boolean;
}

export interface CompanyOfficer {
  id: string;
  company_id: string;
  full_name: string;
  role: string;
  appointed_on: string | null;   // "YYYY-MM-DD"
  resigned_on: string | null;    // null = currently active
  nationality: string | null;
  occupation: string | null;
  source_name: string;
  source_display_name: string | null;
  created_at: string;
}

export interface OfficerSuggestion {
  id: string;
  company_id: string;
  company_name: string;
  operation: string;
  full_name: string;
  role: string;
  appointed_on: string | null;
  resigned_on: string | null;
  nationality: string | null;
  occupation: string | null;
  confidence: number | null;
  status: string;
  created_at: string;
}
```

- [ ] **Step 2: Add API methods to api.ts**

Add to the `api` object in `ui/app/lib/api.ts`:

```typescript
  async getCompanyTaskTypes(companyId: string): Promise<SourceTaskType[]> {
    const res = await fetch(`/api/v1/companies/${companyId}/task-types`);
    if (!res.ok) throw new Error("Failed to fetch task types");
    return res.json();
  },

  async createTask(body: { company_id: string; source_name: string; task_type: string }): Promise<{ job_id: number }> {
    const res = await fetch("/api/v1/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error("Failed to create task");
    return res.json();
  },

  async getCompanyOfficers(companyId: string): Promise<CompanyOfficer[]> {
    const res = await fetch(`/api/v1/companies/${companyId}/officers`);
    if (!res.ok) throw new Error("Failed to fetch officers");
    return res.json();
  },

  async getPendingOfficerSuggestions(page = 1): Promise<{ items: OfficerSuggestion[]; total: number; page: number; limit: number }> {
    const res = await fetch(`/api/v1/review/officers?page=${page}`);
    if (!res.ok) throw new Error("Failed to fetch officer suggestions");
    return res.json();
  },

  async approveOfficerSuggestion(id: string): Promise<CompanyOfficer> {
    const res = await fetch(`/api/v1/review/officers/${id}/approve`, { method: "POST" });
    if (!res.ok) throw new Error("Failed to approve");
    return res.json();
  },

  async rejectOfficerSuggestion(id: string): Promise<void> {
    const res = await fetch(`/api/v1/review/officers/${id}/reject`, { method: "POST" });
    if (!res.ok) throw new Error("Failed to reject");
  },
```

- [ ] **Step 3: Type-check**

```bash
cd ui && pnpm typecheck
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add ui/app/types/api.ts ui/app/lib/api.ts
git commit -m "feat(frontend): SourceTaskType, CompanyOfficer, OfficerSuggestion types + API methods"
```

---

### Task 19: Frontend — CreateTaskSheet component

**Files:**
- Create: `ui/app/components/app/company/CreateTaskSheet.tsx`

- [ ] **Step 1: Create the component**

```tsx
// ui/app/components/app/company/CreateTaskSheet.tsx
import { useState } from "react";
import { Loader2, Zap } from "lucide-react";
import { api } from "~/lib/api";
import type { SourceTaskType } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import {
  Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle,
} from "~/components/ui/sheet";

interface CreateTaskSheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  companyId: string;
  taskTypes: SourceTaskType[];
  onSuccess?: (jobIds: number[]) => void;
}

export function CreateTaskSheet({
  open,
  onOpenChange,
  companyId,
  taskTypes,
  onSuccess,
}: CreateTaskSheetProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();

  function toggle(taskType: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(taskType)) next.delete(taskType); else next.add(taskType);
      return next;
    });
  }

  async function handleSubmit() {
    if (selected.size === 0) return;
    setSubmitting(true);
    setError(undefined);
    const jobIds: number[] = [];
    try {
      for (const tt of taskTypes) {
        if (!selected.has(tt.task_type)) continue;
        const res = await api.createTask({
          company_id: companyId,
          source_name: tt.source_id,   // NOTE: see below — pass source name not id
          task_type: tt.task_type,
        });
        jobIds.push(res.job_id);
      }
      setSelected(new Set());
      onOpenChange(false);
      onSuccess?.(jobIds);
    } catch {
      setError("Failed to create one or more tasks.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-[440px]">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            <Zap className="size-4" /> Create Data Task
          </SheetTitle>
          <SheetDescription>
            Select one or more sources to pull fresh data for this company.
            Results will appear in the review queue.
          </SheetDescription>
        </SheetHeader>

        <div className="mt-6 space-y-3">
          {taskTypes.length === 0 && (
            <p className="text-sm text-muted-foreground">
              No task types available for this company.
            </p>
          )}
          {taskTypes.map((tt) => (
            <label
              key={tt.task_type}
              className="flex items-start gap-3 rounded-lg border p-3 cursor-pointer hover:bg-muted/50 transition-colors"
            >
              <Checkbox
                checked={selected.has(tt.task_type)}
                onCheckedChange={() => toggle(tt.task_type)}
                className="mt-0.5"
              />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium">{tt.display_name}</p>
                <div className="flex flex-wrap gap-1 mt-1">
                  {tt.capabilities.map((cap) => (
                    <Badge key={cap} variant="secondary" className="text-xs">
                      {cap}
                    </Badge>
                  ))}
                </div>
              </div>
            </label>
          ))}
        </div>

        {error && <p className="mt-3 text-sm text-red-600">{error}</p>}

        <div className="mt-6 flex justify-end gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={selected.size === 0 || submitting}
          >
            {submitting ? <Loader2 className="size-4 animate-spin mr-2" /> : null}
            Queue {selected.size > 0 ? `${selected.size} ` : ""}Task{selected.size !== 1 ? "s" : ""}
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
```

**Important note about source_name:** The `SourceTaskType` returned by the API includes `source_id` (a UUID). But `handleCreateTask` on the backend expects `source_name` (a string like `"brreg"`). The API query `GetAvailableTaskTypesForCompany` returns `SourceTaskType` rows which don't include `source_name` directly. You have two options:

**Option A (recommended):** Update the `GetAvailableTaskTypesForCompany` SQL query to also return `ds.name AS source_name`:
```sql
SELECT stt.*, ds.name AS source_name
FROM source_task_types stt
JOIN data_sources ds ON ds.id = stt.source_id
...
```
This makes sqlc generate a result row with `SourceName` included. Update the frontend type to `source_name: string` and use it in `createTask`.

**Option B:** Keep it as-is and make the Go handler resolve source_name from source_id internally.

Use Option A: update the SQL query to include `ds.name AS source_name`, re-run `make sqlc-generate`, update the Go handler to use the source_name from the request body directly (it already does), and update the TS type.

- [ ] **Step 2: Type-check**

```bash
cd ui && pnpm typecheck
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add ui/app/components/app/company/CreateTaskSheet.tsx
git commit -m "feat(frontend): CreateTaskSheet — source task selection with capabilities"
```

---

### Task 20: Frontend — Company detail: Create Task button + Officers tab + source provenance chips

**Files:**
- Modify: `ui/app/routes/companies_.$id.tsx`

The current company detail page has tabs (Overview, Financial, Locations, Contacts) and an "Enrich" button. Changes:
1. Replace "Enrich" button with "Create Task" button that opens `CreateTaskSheet`.
2. Add an "Officers" tab showing `company_officers`.
3. On Locations tab rows: show a source chip linking to `/sources/{source_name}`.

- [ ] **Step 1: Read the current file to understand its structure**

```bash
head -80 ui/app/routes/companies_.$id.tsx
```

- [ ] **Step 2: Replace the Enrich button with Create Task**

Find the Enrich button section (it will be something like a button calling `handleEnrichCompanyFromSource`). Replace it with:

```tsx
// Add state near the top of the component:
const [taskSheetOpen, setTaskSheetOpen] = useState(false);
const [taskTypes, setTaskTypes] = useState<SourceTaskType[]>([]);

// Add loader call alongside the existing data fetch:
useEffect(() => {
  api.getCompanyTaskTypes(company.id).then(setTaskTypes).catch(() => setTaskTypes([]));
}, [company.id]);

// Replace the Enrich button:
<Button variant="outline" size="sm" onClick={() => setTaskSheetOpen(true)}>
  <Zap className="size-4 mr-1" /> Create Task
</Button>

// Add the Sheet after the header:
<CreateTaskSheet
  open={taskSheetOpen}
  onOpenChange={setTaskSheetOpen}
  companyId={company.id}
  taskTypes={taskTypes}
  onSuccess={(jobIds) => {
    // Navigate to jobs page to see the queued tasks
    navigate(`/jobs?kind=data_task`);
  }}
/>
```

Add imports: `import { CreateTaskSheet } from "~/components/app/company/CreateTaskSheet"`, `import type { SourceTaskType, CompanyOfficer } from "~/types/api"`, `import { Zap } from "lucide-react"`.

- [ ] **Step 3: Add Officers tab**

In the Tabs component, add a new tab after the existing ones:

```tsx
<TabsTrigger value="officers">Officers</TabsTrigger>
```

And the content panel:

```tsx
<TabsContent value="officers" className="mt-4">
  <OfficersPanel companyId={company.id} />
</TabsContent>
```

Create the `OfficersPanel` component inline or as a small function above the main component:

```tsx
function OfficersPanel({ companyId }: { companyId: string }) {
  const [officers, setOfficers] = useState<CompanyOfficer[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getCompanyOfficers(companyId)
      .then(setOfficers)
      .catch(() => setOfficers([]))
      .finally(() => setLoading(false));
  }, [companyId]);

  if (loading) return <div className="py-8 text-center text-sm text-muted-foreground">Loading...</div>;
  if (officers.length === 0) return <div className="py-8 text-center text-sm text-muted-foreground">No officers on record.</div>;

  return (
    <div className="rounded-md border overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-2 font-medium">Name</th>
            <th className="text-left px-4 py-2 font-medium">Role</th>
            <th className="text-left px-4 py-2 font-medium">Appointed</th>
            <th className="text-left px-4 py-2 font-medium">Resigned</th>
            <th className="text-left px-4 py-2 font-medium">Source</th>
          </tr>
        </thead>
        <tbody>
          {officers.map((o) => (
            <tr key={o.id} className={`border-t ${o.resigned_on ? "opacity-60" : ""}`}>
              <td className="px-4 py-2 font-medium">{o.full_name}</td>
              <td className="px-4 py-2 capitalize">{o.role}</td>
              <td className="px-4 py-2 text-muted-foreground">{o.appointed_on ?? "—"}</td>
              <td className="px-4 py-2 text-muted-foreground">{o.resigned_on ?? "Active"}</td>
              <td className="px-4 py-2">
                <SourceChip sourceName={o.source_name} displayName={o.source_display_name} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 4: Add SourceChip component and use it on location rows**

Add a small `SourceChip` component (can be in this file or a shared file):

```tsx
function SourceChip({ sourceName, displayName }: { sourceName: string; displayName: string | null }) {
  return (
    <Link
      to={`/sources/${sourceName}`}
      className="inline-flex items-center gap-1 rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground hover:text-foreground hover:bg-muted/80 transition-colors"
      onClick={(e) => e.stopPropagation()}
    >
      {displayName ?? sourceName}
    </Link>
  );
}
```

In the Locations tab, for each location row that has a `source` field, add `<SourceChip sourceName={loc.source} displayName={null} />`. Do the same for phones and emails rows if they appear in tabs.

- [ ] **Step 5: Type-check**

```bash
cd ui && pnpm typecheck
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add ui/app/routes/companies_.$id.tsx ui/app/components/app/company/CreateTaskSheet.tsx
git commit -m "feat(frontend): company detail — Create Task button, Officers tab, source provenance chips"
```

---

### Task 21: Frontend — Source detail capabilities section + Jobs page data_task kind

**Files:**
- Modify: `ui/app/routes/sources_.$name.tsx`
- Modify: `ui/app/routes/jobs.tsx`

- [ ] **Step 1: Read sources_.$name.tsx to understand its current structure**

```bash
cat ui/app/routes/sources_.$name.tsx | head -60
```

- [ ] **Step 2: Add capabilities section to source detail page**

The source detail page already displays source metadata. Find the section where source fields are shown and add after it:

```tsx
{/* Capabilities */}
{source.capabilities && source.capabilities.length > 0 && (
  <div className="rounded-lg border p-4">
    <h3 className="text-sm font-semibold mb-3">Data Capabilities</h3>
    <div className="flex flex-wrap gap-2">
      {source.capabilities.map((cap: string) => (
        <Badge key={cap} variant="secondary" className="capitalize">
          {cap.replace(/_/g, " ")}
        </Badge>
      ))}
    </div>
  </div>
)}
```

Make sure `Badge` is imported from `~/components/ui/badge`.

Also add a "Task Types" section that shows the task types for this source. Fetch from `GET /api/v1/sources/{name}` (the source already includes `capabilities`; task types require a separate call or can be shown from the existing `capabilities` array).

- [ ] **Step 3: Update jobs.tsx — add data_task to KIND_LABELS, KindBadge, and filter**

In `ui/app/routes/jobs.tsx`:

Add to `KIND_LABELS`:
```typescript
data_task: "Data Task",
```

Add to `KindBadge`:
```tsx
if (kind === "data_task")
  return <Badge className="bg-teal-100 text-teal-800 border-teal-200 text-xs" variant="outline">Data Task</Badge>;
```

Add to the kind filter `<select>`:
```tsx
<option value="data_task">Data Task</option>
```

- [ ] **Step 4: Type-check**

```bash
cd ui && pnpm typecheck
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add ui/app/routes/sources_.$name.tsx ui/app/routes/jobs.tsx
git commit -m "feat(frontend): source detail capabilities section + data_task kind in jobs page"
```

---

### Task 22: Frontend — Review page Officer Suggestions tab

**Files:**
- Modify: `ui/app/routes/review.tsx`
- Create: `ui/app/components/app/review/OfficerSuggestionsTab.tsx`

- [ ] **Step 1: Create OfficerSuggestionsTab component**

```tsx
// ui/app/components/app/review/OfficerSuggestionsTab.tsx
import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router";
import { CheckCircle2, XCircle } from "lucide-react";
import { api } from "~/lib/api";
import type { OfficerSuggestion } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "~/components/ui/table";
import { timeAgo } from "~/lib/utils";

export function OfficerSuggestionsTab() {
  const [items, setItems] = useState<OfficerSuggestion[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState<string>();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getPendingOfficerSuggestions(page);
      setItems(res.items);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [page]);

  useEffect(() => { load(); }, [load]);

  async function approve(id: string) {
    setWorking(id);
    try { await api.approveOfficerSuggestion(id); await load(); }
    finally { setWorking(undefined); }
  }

  async function reject(id: string) {
    setWorking(id);
    try { await api.rejectOfficerSuggestion(id); await load(); }
    finally { setWorking(undefined); }
  }

  if (loading) return <div className="py-10 text-center text-sm text-muted-foreground">Loading…</div>;
  if (items.length === 0) return <div className="py-10 text-center text-sm text-muted-foreground">No pending officer suggestions.</div>;

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">{total} pending</p>
      <div className="rounded-md border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Company</TableHead>
              <TableHead>Name</TableHead>
              <TableHead>Role</TableHead>
              <TableHead>Appointed</TableHead>
              <TableHead>Operation</TableHead>
              <TableHead>Added</TableHead>
              <TableHead></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((s) => (
              <TableRow key={s.id}>
                <TableCell>
                  <Link to={`/companies/${s.company_id}`} className="hover:underline font-medium">
                    {s.company_name}
                  </Link>
                </TableCell>
                <TableCell className="font-medium">{s.full_name}</TableCell>
                <TableCell className="capitalize">{s.role}</TableCell>
                <TableCell className="text-muted-foreground">{s.appointed_on ?? "—"}</TableCell>
                <TableCell>
                  <Badge variant={s.operation === "add" ? "default" : "secondary"} className="capitalize">
                    {s.operation}
                  </Badge>
                </TableCell>
                <TableCell className="text-sm text-muted-foreground">{timeAgo(s.created_at)}</TableCell>
                <TableCell>
                  <div className="flex items-center gap-1">
                    <Button size="sm" variant="ghost" className="text-green-600 hover:text-green-700"
                      disabled={working === s.id} onClick={() => approve(s.id)}>
                      <CheckCircle2 className="size-4" />
                    </Button>
                    <Button size="sm" variant="ghost" className="text-red-600 hover:text-red-700"
                      disabled={working === s.id} onClick={() => reject(s.id)}>
                      <XCircle className="size-4" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="flex items-center justify-between">
        <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>Previous</Button>
        <span className="text-sm text-muted-foreground">Page {page}</span>
        <Button variant="outline" size="sm" disabled={items.length < 50} onClick={() => setPage((p) => p + 1)}>Next</Button>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Add the tab to review.tsx**

In `ui/app/routes/review.tsx`, import `OfficerSuggestionsTab` and add a fourth tab:

```tsx
import { OfficerSuggestionsTab } from "~/components/app/review/OfficerSuggestionsTab";

// In the Tabs component, add:
<TabsTrigger value="officers">Officers</TabsTrigger>

// In the TabsContent sections:
<TabsContent value="officers" className="mt-4">
  <OfficerSuggestionsTab />
</TabsContent>
```

- [ ] **Step 3: Type-check and run final build check**

```bash
cd ui && pnpm typecheck
cd ../scheduler && GOWORK=off go test ./...
```

Expected: no type errors, all Go tests pass.

- [ ] **Step 4: Commit**

```bash
git add ui/app/routes/review.tsx ui/app/components/app/review/OfficerSuggestionsTab.tsx
git commit -m "feat(frontend): review page — Officer Suggestions tab"
```

---

## Self-Review

**Spec coverage check:**

| Requirement | Task |
|---|---|
| source_task_types table + seed | Task 1 |
| company_officers table | Task 1 |
| company_officer_suggestions table | Task 1 |
| CompanyOfficer model in crawler | Task 4 |
| fetch_by_ids interface on SourceAdapter | Task 4 |
| BrregAdapter.fetch_by_ids + financials | Task 5 |
| CompaniesHouseAdapter.fetch_by_ids + officers | Task 6 |
| GLEIFAdapter.fetch_by_ids | Task 7 |
| POST /task/{source_name} crawler endpoint | Task 8 |
| crawlerclient.RunTask | Task 9 |
| upsertCompanyRecord extracted | Task 10 |
| DataTaskArgs + DataTaskWorker | Task 11 |
| River config: data_task queue, remove enrich_financials | Task 12 |
| BrregProcessor: financial data for existing companies | Task 13 |
| CompaniesHouseProcessor: officer suggestions | Task 14 |
| Remove FinancialEnrichWorker | Task 15 |
| GET /companies/:id/task-types | Task 16 |
| POST /tasks | Task 16 |
| GET /companies/:id/officers | Task 16 |
| Officer suggestion review endpoints | Task 16 |
| Remove old enrich endpoints | Task 17 |
| testhelpers updated | Task 17 |
| Frontend types: SourceTaskType, CompanyOfficer, OfficerSuggestion | Task 18 |
| Frontend API methods | Task 18 |
| CreateTaskSheet component | Task 19 |
| Company detail: Create Task button | Task 20 |
| Company detail: Officers tab | Task 20 |
| Source provenance chips | Task 20 |
| Source detail: capabilities section | Task 21 |
| Jobs page: data_task kind | Task 21 |
| Review page: Officer Suggestions tab | Task 22 |

**Note on Task 19 / source_name vs source_id:** The `GetAvailableTaskTypesForCompany` query must return `ds.name AS source_name` (not just the UUID) so the frontend can pass it to `POST /tasks`. Update the SQL query in `database/queries/task_types.sql` accordingly and re-run `make sqlc-generate` before starting Task 16.

**Note on `ApproveCompanyOfficerSuggestion` SQL:** The CTE-based query inserts an officer AND marks the suggestion approved atomically. The sqlc `RETURNING *` from the INSERT gives us back the new officer row. Verify that sqlc handles this correctly after generate (it should produce a method returning `CompanyOfficer`).
