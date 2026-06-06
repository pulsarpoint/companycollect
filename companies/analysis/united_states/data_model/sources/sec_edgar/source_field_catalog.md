# SEC EDGAR Field Catalog

## Source Summary

- Country: United States
- Source type: official_regulator (federal)
- Organization: U.S. Securities and Exchange Commission
- URL: https://www.sec.gov/files/company_tickers.json (bulk); https://data.sec.gov/ (REST APIs)
- License: U.S. Government work / public domain
- Access: public (no key; descriptive User-Agent with contact email mandatory, else HTTP 403)
- Freshness: near real-time (submissions <1s, XBRL <1 min); ticker map refreshed regularly
- Record shape: JSON object keyed by sequential integer index; each value is one company `{cik_str, ticker, title}`
- Primary keys: `cik_str` (zero-pad to 10 digits → CIK0000000000)
- Join keys: `cik_str`, `ticker`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `<index>.cik_str` | cik_str | SEC Central Index Key, federal filer id | integer | identifier | 1045810, 320193 | Pad to 10 digits for API joins |
| `<index>.ticker` | ticker | Primary stock ticker | string | identifier | NVDA, AAPL | Not unique across share classes |
| `<index>.title` | title | Filer / company name | string | legal_name | NVIDIA CORP, Apple Inc. | Inconsistent casing; no name history |
| submissions API | addresses/sic/formerNames/… | Richer profile (address, industry, incorp. state, EIN, former names, filings) | object | metadata | — | **Planning-only**, documented not downloaded |
| companyfacts API | facts.us-gaap.* | XBRL financial statement values | object | financial | — | **Planning-only**, on-demand only |

## Interpretation Notes

- **Coverage:** SEC EDGAR covers only public / SEC-reporting companies (~10,405 with tickers; more filers without). It is NOT a general company register and misses the vast majority of US private companies.
- **CIK is the federal key for public companies.** It is the most reliable identifier here. The downloaded `company_tickers.json` only carries CIK, ticker, and name — three observed fields.
- **Richer data is one hop away but was not downloaded.** The `submissions/CIK##########.json` and `companyfacts/CIK##########.json` endpoints add addresses, SIC industry, state of incorporation, fiscal year end, EIN, former names, full filing history, and XBRL financials. Those fields are cataloged here as **planning-only** from public SEC API documentation — they were not captured to a raw sample in this investigation, so their exact shapes are documented-but-unverified.
- **Operational constraint:** every request must send a descriptive `User-Agent` header containing a contact email; missing header returns HTTP 403. Honor the 10 requests/second/IP limit.
- **Name casing** is inconsistent in the ticker file (mix of all-caps and mixed case) — normalize for display.
