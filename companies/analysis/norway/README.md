# Company data sources for Norway

## Status

- Official bulk data: **found** (full entity + sub-entity register as gzipped JSON/CSV/XLSX, no auth)
- Official API: **found** (Brreg Enhetsregisteret REST API — open, no auth, no registration)
- Financial data: **partially found** (latest filing as open JSON; public report copies for
  the latest 15 years as PDF; complete structured feed requires a paid subscription)
- Open data portal: **found** (data.norge.no catalog; Brreg is the publisher)
- License: **known** — Norwegian Licence for Open Government Data (NLOD 2.0)
- Recommended ingestion path: **Enhetsregisteret bulk/deltas + latest financial JSON + paid
  XML financial feed with negotiated historical backfill**

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

Open key figures:

`https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}`

Public report-copy archive:

`https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar`

- Verified live for Equinor (923609016): returns full annual-accounts figures as JSON —
  income statement (driftsinntekter, driftsresultat, årsresultat, finans), balance sheet
  (sum eiendeler, egenkapital, sum gjeld, kort-/langsiktig), currency, accounting rules,
  audit flags, accounting period.
- No authentication, open license (NLOD 2.0). One call per organisation number.
- Coverage excludes entities that do not file accounts and excludes non-standard layouts such
  as banks and insurers from the open key-figures response.
- The unauthenticated structured API returns **only the latest approved company filing**. Its
  documented `år` and `regnskapstype` parameters are applied only in the restricted partner path;
  public calls ignore them.
- The separate public report-copy API lists and downloads annual-report PDFs for the latest
  **15 years**. Verified reports are image-only PDFs, so extracting figures requires OCR and a
  document parser rather than the current JSON mapper.
- Brreg's paid annual-accounts subscription delivers all newly registered filings as XML over
  SFTP (about 300,000/year), with optional TIFF copies. The public product page does not promise
  a retroactive structured archive, so an initial historical delivery must be confirmed with
  Brreg before purchase.

## Excluded / caveats

- Personal sole proprietors (ENK) appear in Enhetsregisteret but rarely file accounts.
- Regnskapsregisteret's open key-figures API is a preview and cannot bootstrap history.
- Public PDFs cover at most 15 years and are image-only in verified samples; large-scale OCR
  would be expensive and should be agreed with Brreg before crawling millions of documents.
- The paid subscription is an XML feed of registered annual accounts. Historical backfill and
  redistribution rights require explicit confirmation under the subscription agreement.
- Beneficial ownership register (Register over reelle rettighetshavere) exists but is
  **not** fully open as bulk (see investigation.md).

## Next action

For a genuinely historical financial dataset, contact Brreg about the annual-accounts XML
subscription and ask specifically for a one-time historical backfill. If no structured backfill
is available, decide between a bounded 15-year PDF/OCR bootstrap and a separately licensed
historical provider. Keep the open JSON endpoint only for latest-filing refresh and validation;
do not treat it as the archive source.
