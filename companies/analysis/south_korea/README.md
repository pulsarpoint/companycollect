# Company data sources for South Korea

## Status

- Official bulk data: **partial** (OpenDART corpCode.xml = full DART-entity list; full unlisted-company register is court-fee-based)
- Official API: **found** (OpenDART API — identity + financials; NTS business-status API — all free key)
- Open data portal: **found** (data.go.kr / odcloud — free service key)
- License: **known-ish** — OpenDART data is public disclosure, reusable; data.go.kr datasets carry their own terms (often KOGL)
- Recommended ingestion path: **API** (OpenDART, free key) for identity + financials; NTS status API for tax-registration status

## Best source

**OpenDART API** (`https://opendart.fss.or.kr/api/…`) run by the **Financial
Supervisory Service (FSS)** — the open API of the **DART** electronic disclosure
system. It returns, with a **free API key (`crtfc_key`)**:

- **`corpCode.xml`** — the bulk list of every DART-registered company (corp_code,
  corp_name, English name, stock_code, modify_date).
- **`company.json`** — company identity: name (KO/EN), **법인등록번호 (corporate
  registration number, `jurir_no`, 13-digit)**, **사업자등록번호 (business
  registration number, `bizr_no`, 10-digit = tax id)**, listing class, CEO,
  industry (KSIC), establishment date, address, homepage.
- **`fnlttSinglAcnt(All).json`** — **financial statements** (balance sheet + income
  statement, XBRL-derived) in **KRW** per fiscal year/report.

Coverage: all **listed** companies plus **external-audit** companies (대규모 /
외부감사 대상) — i.e. the disclosure-obligated universe, not every micro-company.

Verified live: every OpenDART endpoint returns `{"status":"900"}` / a 302 without a
key (free key required); the company/corpCode/financial field schemas were
confirmed from the official API guide.

## Financial data

**OpenDART** is the financial source — XBRL balance sheet + income statement (and
full statements via `fnlttSinglAcntAll`) for DART-registered companies, KRW, free
key. This is one of the better open financial sources of any country covered.

## Other sources

- **NTS business-status API** (data.go.kr `15081808` / `api.odcloud.kr`) — business
  registration **status** (active / closed / suspended, tax type) by 사업자등록번호.
  Free service key (verified 401 without one).
- **IROS** (Supreme Court Internet Registry, iros.go.kr) — the full commercial
  register incl. **unlisted** companies; documents are **fee-based** per issue.
- **KED / NICE** — paid commercial aggregators for full company master data.

## Identifiers & tax

- **법인등록번호** (corporate registration number, 13-digit) — court-issued company id.
- **사업자등록번호** (business registration number, 10-digit) — NTS tax id. Korea has
  **VAT (부가가치세)** but the VAT number **is** the business registration number —
  there is no separate VAT id.

## Next action

Register a free OpenDART key; pull `corpCode.xml` then `company.json` +
`fnlttSinglAcntAll` per corp_code; add NTS status via the data.go.kr API. The
committed sample is **schematic** (APIs are key-gated).
