# United Kingdom Company Data — Investigation

## Conclusion

The UK is **best-in-class fully-open** — the register, bulk data, API, **and**
financial accounts are all free, from **Companies House**, joined on the
**company number** (8 characters):

- **Free Company Data Product** — the full register of live companies (~5.9M) as
  CSV ZIPs (7 split parts, ~70 MB each, or one 470 MB file), **55 columns**.
  **Open Government Licence**, monthly snapshot.
- **Accounts Bulk Data** — **iXBRL/XBRL** financial statements of electronically-
  filed accounts (~60–75% of filings), daily (last 60 days) + monthly ZIPs. Free.
- **REST API** (free key, 600 req/5 min) — company profile, officers, PSC, filing
  history, charges, document API. Plus a Streaming API.
- **PSC snapshot** — persons with significant control (beneficial owners), bulk +
  API. Free.

## Identifiers

- **Company number** — 8 characters (digits, or 2-letter prefix + 6 digits, e.g.
  `SC`/`NI`/`OC`); the company id and universal join key.
- **No tax id** in Companies House; VAT numbers are held separately by HMRC.
- **SIC code** — UK SIC 2007 activity classification (in the basic data, with text).

## Sources found

### 1. Free Company Data Product (basic data CSV) — RECOMMENDED
- Index `http://download.companieshouse.gov.uk/en_output.html`. Files dated
  `YYYY-MM-01`: `BasicCompanyDataAsOneFile-2026-06-01.zip` (470 MB) or
  `BasicCompanyData-2026-06-01-part{1..7}_7.zip` (~70 MB each).
- Verified: part1 = **849,999** rows (× 7 ≈ 5.9M). **55 columns**: CompanyName,
  CompanyNumber, RegAddress.* (full address), CompanyCategory (legal form),
  CompanyStatus, CountryOfOrigin, DissolutionDate, IncorporationDate, Accounts.*
  (AccountRefDay/Month, NextDueDate, LastMadeUpDate, AccountCategory), Returns.*,
  Mortgages.Num* (charges), SICCode.SicText_1..4, LimitedPartnerships.*, URI,
  PreviousName_1..10, ConfStmt* dates. **OGL**, monthly.

### 2. Accounts Bulk Data (iXBRL) — RECOMMENDED for financials
- Index `http://download.companieshouse.gov.uk/en_accountsdata.html` (daily, last
  60 days) and `en_monthlyaccountsdata.html` (monthly). Files
  `Accounts_Bulk_Data-YYYY-MM-DD.zip` (~70–90 MB) → many `.html` (iXBRL) + some
  `.xml` (XBRL). Verified: one daily zip = **9,717** filings; filename
  `Prod223_<run>_<companynumber>_<madeupto>.html`.
- iXBRL facts tagged to the **FRC/UK GAAP taxonomy** (`core:`, `bus:`
  namespaces). Verified real facts for company **00009604** (Hull & Humber Chamber
  of Commerce): TurnoverRevenue 1,615,243; ProfitLoss 221,523; FixedAssets
  1,619,290; NetAssetsLiabilities 5,782,684; Equity 402,324 (GBP). Join on the
  company number (filename + `bus:UKCompaniesHouseRegisteredNumber`).
- Coverage: **electronically-filed accounts only** (~60–75%); paper/scanned PDFs
  excluded.

### 3. Companies House REST API — free key
- `https://api.company-information.service.gov.uk/` — company profile, **officers**,
  **PSC**, filing history, charges, registered-office, document API. Free API key
  (HTTP Basic with key as username); 600 requests / 5 min. → the route to officers
  and per-company detail; key required (free).

### 4. PSC snapshot (beneficial owners) — RECOMMENDED for ownership
- `http://download.companieshouse.gov.uk/en_pscdata.html` — persons-with-
  significant-control bulk snapshot (JSON), free; also via the REST API. Beneficial
  ownership. **Personal data**.

### Other
- **Streaming API** (real-time changes), **CH Guide** (community docs). HMRC VAT is
  a separate dataset (not in CH). Aggregators (e.g. OpenCorporates) mirror CH.

## What was NOT bypassed

- Only the open bulk products were downloaded (OGL). The REST API key gate was not
  bypassed (it is free on registration). Person data (officers/PSC) flagged for
  redaction.

## Recommended ingestion

Bulk-load the basic-data CSV and the accounts iXBRL, join on **company number**;
add the PSC snapshot for beneficial owners and the REST API (free key) for
officers/filing history. Handle person data per UK GDPR.
