# Company data sources for Norway

## Status

- Official bulk data: **found** (full entity + sub-entity register as gzipped JSON/CSV/XLSX, no auth)
- Official API: **found** (Brreg Enhetsregisteret REST API — open, no auth, no registration)
- Financial data: **found** (Brreg Regnskapsregisteret API — annual accounts figures as open JSON)
- Open data portal: **found** (data.norge.no catalog; Brreg is the publisher)
- License: **known** — Norwegian Licence for Open Government Data (NLOD 2.0)
- Recommended ingestion path: **bulk file (daily) + Enhetsregisteret update feed (incremental) + per-orgnr Regnskapsregisteret enrichment**

## Best source

**Brønnøysund Register Centre (Brønnøysundregistrene / Brreg)** — the official Norwegian
business register operator. Two complementary open services cover everything needed:

### 1. Enhetsregisteret — Central Coordinating Register for Legal Entities (base company data)

`https://data.brreg.no/enhetsregisteret/api`

- Verified live: **1,164,396** currently-registered legal entities (`/api/enheter`)
  and **842,538** sub-entities/establishments (`/api/underenheter`).
- No authentication, no API key, no registration. License **NLOD 2.0**.
- Bulk downloads (verified live, gzip):
  - Entities JSON `…/api/enheter/lastned` (~197 MB)
  - Entities CSV `…/api/enheter/lastned/csv` (~154 MB, **1,458,299 rows** incl. dissolved)
  - Sub-entities CSV `…/api/underenheter/lastned/csv` (~60 MB)
- Rich record: org number, name, legal form, NACE/industry codes (×3), addresses
  (postal + business), employees, VAT/Foretaksregister/foundation flags, share capital,
  bankruptcy/liquidation status, parent entity, group membership, website, phone.
- Incremental update feeds: `…/api/oppdateringer/{enheter|underenheter|roller}`.

### 2. Regnskapsregisteret — Register of Company Accounts (financial data)

`https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}`

- Verified live for Equinor (923609016): returns full annual-accounts figures as JSON —
  income statement (driftsinntekter, driftsresultat, årsresultat, finans), balance sheet
  (sum eiendeler, egenkapital, sum gjeld, kort-/langsiktig), currency, accounting rules,
  audit flags, accounting period.
- No authentication, open license (NLOD 2.0). One call per organisation number.
- Coverage ~80% of accounting-liable companies (AS, ASA, NUF, etc.); excludes most
  sole proprietorships. Returns the most recent filed year(s); historical depth varies.

## Excluded / caveats

- Personal sole proprietors (ENK) appear in Enhetsregisteret but rarely file accounts.
- Regnskapsregisteret API is labelled by Brreg as a "temporary/research" open distribution —
  stable for years in practice but worth monitoring; full historical figures + image copies
  are behind the paid Subscription Service.
- Beneficial ownership register (Register over reelle rettighetshavere) exists but is
  **not** fully open as bulk (see investigation.md).

## Next action

Implement a bulk loader for `enheter` + `underenheter` (daily gzip pull), then a delta job
against the `oppdateringer` feeds, then enrich each AS/ASA with one Regnskapsregisteret call.
Map fields to the internal company model (see `schema_notes.md`). Add "Kilde: Brønnøysundregistrene,
NLOD 2.0" attribution. Suggested registry keys: `norway/brregenhet` (base) and `norway/brregregnskap` (financials).
