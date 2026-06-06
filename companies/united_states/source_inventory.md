# Source inventory — United States

| # | Source | Type | Coverage | Access | Formats | License | Status |
|---|--------|------|----------|--------|---------|---------|--------|
| 1 | **SEC EDGAR** | Federal regulator | Public / SEC-reporting companies | Open, no key (User-Agent required), 10 req/s | JSON, ZIP, XBRL | US Gov / public domain | ✅ recommended |
| 2 | **IRS EO BMF** | Federal tax authority | Tax-exempt nonprofits (national, EIN) | Open, no key | CSV | US Gov / public domain | ✅ recommended |
| 3 | **SAM.gov Entity** | Federal registry (GSA) | Federal contractors / grantees | Public extract, **free API key required** | JSON, CSV | FOIA public | ✅ recommended |
| 4 | **Colorado Business Entities** | State registry (open data) | All CO entities (1M+) | Open, no key (Socrata) | JSON, CSV, XML | Open data (verify) | ✅ recommended |
| 5 | **State SoS registries (50+DC)** | State registries | All US private companies (authoritative) | Free search; **bulk often paid** | varies | varies | ◐ useful secondary |
| 6 | **Data.gov catalog** | Open data portal | Discovery of fed/state datasets | Open | varies | varies | ◐ useful secondary |
| 7 | **OpenCorporates** | Aggregator | All 50 states normalized | **Paid/licensed bulk** | JSON (API) | restricted | ⚠ license uncertainty |

## Direct bulk / API endpoints

- SEC tickers: `https://www.sec.gov/files/company_tickers.json`
- SEC submissions: `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`
- SEC company facts: `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip`
- SEC REST API: `https://data.sec.gov/`
- IRS EO BMF: `https://www.irs.gov/pub/irs-soi/eo1.csv` … `eo4.csv` (dictionary: Pub 5926)
- SAM.gov extracts: `https://open.gsa.gov/api/sam-entity-extracts-api/` ; API: `https://api.sam.gov/entity-information/v3/entities`
- Colorado dataset: `https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h`
  - CSV: `https://data.colorado.gov/api/views/4ykn-tg5h/rows.csv?accessType=DOWNLOAD`
  - API: `https://data.colorado.gov/resource/4ykn-tg5h.json?$limit=1000&$offset=0`
- NASS state registry directory: `https://www.nass.org/`

## Free / open state registries (for expansion)
Colorado (confirmed open data). Reportedly also offering free bulk or open APIs: Oregon, Connecticut, Iowa, Minnesota (non-commercial). Most other states charge for bulk (e.g. AZ $2,000+/yr, SC $12,000/yr UCC, NC $750 setup + $250/yr).
