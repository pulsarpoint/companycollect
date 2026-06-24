# Company data sources for Thailand

## Status

- Official bulk data: **not open** — no open bulk register; the DBD OpenAPI is
  per-company (by 13-digit ID)
- Official API: **YES, OPEN** — DBD OpenAPI returns real juristic-person JSON with
  **no token** (verified live)
- Open data portal: `data.go.th` exists but was WAF-blocked ("Access Denied") for
  automation from this environment
- License: DBD OpenAPI is the official open service; reuse terms per DBD/PDPA
- Recommended ingestion path: **per-company API lookup** via the DBD OpenAPI +
  **DBD DataWarehouse** (login) for full financial statements

## Best source

**DBD OpenAPI** — `https://openapi.dbd.go.th/api/v1/juristic_person/{13-digit-id}`
(Department of Business Development, Ministry of Commerce). It is **fully open (no
API key)** and returns rich real JSON per company: **juristic ID** (13-digit),
**name TH/EN**, **type** (บริษัทจำกัด / บริษัทมหาชนจำกัด / ห้างหุ้นส่วน), **register
date**, **status**, **objective** (TSIC activity code + TH/EN text), **registered &
paid-up capital (THB)**, branch, and a **fully structured address** (with
province/district/sub-district codes).

Verified live: **PTT PCL** (0107544000108, capital ฿28.56bn), **Bangkok Bank PCL**
(0107536000374, ฿40bn), **CP All PCL** (0107542000011), **Internet Thailand PCL**
(0107544000094). This is the strongest **open** official company API found in
Southeast Asia so far.

## Financial data

The OpenAPI returns **registered & paid-up capital**. Full **financial statements**
(balance sheet / income statement, filed annually) are in the **DBD DataWarehouse**
(`datawarehouse.dbd.go.th`), per company, but **login/session-gated** (returned
302/403 for automation). **SET** (`set.or.th`) publishes listed-company financials.

## Identifiers & tax

- **Juristic Person ID (เลขทะเบียนนิติบุคคล)** — **13-digit** company registration
  number. It is **also the Tax ID** (Thailand uses one 13-digit number for company
  registration and tax).
- **VAT** — VAT-registered businesses (ภ.พ.20) use the **same 13-digit Tax ID**;
  there is **no separate VAT number**.
- **TSIC** — Thailand Standard Industrial Classification (activity code).
- Currency **THB**. Names in Thai + English.

## Next action

Ingest via the **DBD OpenAPI** per 13-digit juristic ID (open, no key) for identity
+ capital + activity + address; use **DBD DataWarehouse** (login) for full financial
statements and **SET** for listed financials. Directors/shareholders are personal
data (PDPA) — not in the open API; redact if obtained elsewhere.
