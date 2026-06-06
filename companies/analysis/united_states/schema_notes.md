# Schema notes — United States

## Source field observations

### SEC EDGAR — company_tickers.json
Object keyed by integer index; each value:
```json
{ "cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP" }
```
- `cik_str` → Central Index Key (zero-pad to 10 digits for API calls). Primary federal id for public companies.
- `ticker` → stock ticker. `title` → company name (uppercase, inconsistent casing).
- Richer data (addresses, SIC, fiscal year, former names, filings) via `https://data.sec.gov/submissions/CIK##########.json`.

### IRS EO BMF — eo{1..4}.csv
Header: `EIN,NAME,ICO,STREET,CITY,STATE,ZIP,GROUP,SUBSECTION,AFFILIATION,CLASSIFICATION,RULING,DEDUCTIBILITY,FOUNDATION,ACTIVITY,ORGANIZATION,STATUS,TAX_PERIOD,ASSET_CD,INCOME_CD,FILING_REQ_CD,PF_FILING_REQ_CD,ACCT_PD,ASSET_AMT,INCOME_AMT,REVENUE_AMT,NTEE_CD,SORT_NAME`
- `EIN` → Employer Identification Number (national tax id, primary key). Stored zero-padded 9 digits.
- `NAME`, `STREET/CITY/STATE/ZIP` → name + registered address.
- `SUBSECTION`/`CLASSIFICATION`/`RULING`/`NTEE_CD` → coded; see Pub 5926 dictionary.
- `RULING` is YYYYMM of IRS determination.

### Colorado Business Entities — Socrata JSON
```json
{ "entityid", "entityname", "principaladdress1", "principalcity", "principalstate",
  "principalzipcode", "principalcountry", "entitystatus", "jurisdictonofformation",
  "entitytype", "agentfirstname", "agentlastname", "agentprincipaladdress1...",
  "entityformdate" }
```
- `entityid` → unique within Colorado. Combine with state code for a global key.
- `entitystatus` e.g. "Good Standing"; `entitytype` e.g. "DLLC".
- `entityformdate` ISO8601 (`2025-06-16T00:00:00.000`).

### SAM.gov (Entity) — not downloaded (key required)
Key fields: `ueiSAM` (Unique Entity ID), `cageCode`, `legalBusinessName`, `physicalAddress`, `registrationStatus`, `naicsList`.

## Date formats observed
- SEC: integers / ISO in submissions API.
- IRS: `RULING`/`TAX_PERIOD` as YYYYMM integers.
- Colorado: ISO8601 with `.000` millis.

## Encodings
- All observed samples ASCII/UTF-8, CSV comma-delimited (IRS), JSON (SEC, Colorado).

## Mapping to internal company model

| internal field        | SEC EDGAR            | IRS EO BMF        | Colorado          | SAM.gov            |
|-----------------------|----------------------|-------------------|-------------------|--------------------|
| company_id            | CIK (padded)         | EIN               | entityid          | ueiSAM             |
| registration_number   | CIK                  | —                 | entityid          | cageCode           |
| tax_id / ein          | —                    | EIN               | —                 | EIN (if present)   |
| legal_name            | title                | NAME              | entityname        | legalBusinessName  |
| company_type          | (SIC via submissions)| ORGANIZATION code | entitytype        | (entity type)      |
| status                | (from submissions)   | STATUS code       | entitystatus      | registrationStatus |
| incorporation_date    | (from submissions)   | RULING (approx)   | entityformdate    | registrationDate   |
| registered_address    | (from submissions)   | STREET/CITY/...   | principaladdress* | physicalAddress    |
| region/state          | (from submissions)   | STATE             | principalstate    | stateOrProvince    |
| country               | US                   | US                | principalcountry  | countryCode        |
| source_url/source_name| per source           | per source        | per source        | per source         |

### Suggested global primary key
Federal-id when present, else `state_code + ':' + state_entity_id`. Cross-link on EIN where available (IRS↔SAM) to dedupe. A company may legitimately appear in multiple sources.
