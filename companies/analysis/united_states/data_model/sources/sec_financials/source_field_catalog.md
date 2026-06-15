# SEC EDGAR — XBRL Financial Data Field Catalog

## Source Summary

- Country: United States
- Source type: official_financial
- Organization: U.S. Securities and Exchange Commission (SEC)
- URL: `https://data.sec.gov/api/xbrl/companyfacts/CIK{10-digit}.json` (per company);
  Financial Statement Data Sets (bulk) at sec.gov/dera/data/financial-statement-data-sets
- License: U.S. Government work / public domain
- Access: public (descriptive **User-Agent header required**; no key)
- Freshness: near real-time (as filed); bulk data sets **quarterly**
- Record shape: JSON XBRL facts per CIK; quarterly bulk TSVs for all filers
- Primary keys: `cik` + `concept` + `period`
- Join keys: `cik`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cik | cik | Central Index Key | integer | identifier | 320193 | join to sec_edgar |
| entityName | entityName | Registrant name | string | legal_name | Apple Inc. | |
| facts.us-gaap.<C>.units.<u>[].val | val | Fact value | decimal | financial | 416161000000 | USD; ~503 concepts |
| …[].end | end | Period end | date | date | 2025-09-27 | |
| …[].fy / fp | fy/fp | Fiscal year/period | string | date | 2025 / FY | |
| …[].form | form | Source form | string | filing | 10-K | annual = 10-K |
| facts.dei.<C> | dei facts | Doc/entity info | object | metadata | | shares, FY end |
| Financial Statement Data Sets | num/sub/pre/tag.tsv | Bulk all-filer facts | array | financial | 2025q1.zip (128 MB) | bulk route |

## Interpretation Notes

- **The open route to U.S. public-company financials.** Two access modes:
  - **`companyfacts` API** (per CIK) — JSON of every XBRL fact the company has
    filed (us-gaap + dei), each as a list of `{val, end, fy, fp, form, accn}`
    datapoints. Verified live for Apple (CIK 320193): **503 us-gaap concepts**;
    FY2025 Revenue $416,161,000,000; NetIncomeLoss $112,010,000,000; Assets
    $359,241,000,000.
  - **Financial Statement Data Sets** — quarterly bulk ZIPs (`num.tsv`/`sub.tsv`/
    `pre.tsv`/`tag.tsv`) covering **all** filers; verified `2025q1.zip` (128 MB,
    200 OK). Use for full-population financial harvesting.
  - Also `xbrl/companyconcept/CIK…/us-gaap/<Concept>.json` (one concept) and
    `xbrl/frames/us-gaap/<Concept>/USD/CY2024.json` (one concept across all filers
    for a period).
- **Parsing**: pick the right datapoint per concept by `form` (10-K = annual) and
  the latest `end`; dedupe restatements by `accn`/`frame`. Values are unit-tagged
  (USD, shares, USD/share).
- **Coverage = SEC-reporting companies only** (public + some large private debt
  issuers) — not the private-company universe. **Requires a descriptive
  User-Agent** per SEC fair-access policy; ≤10 req/s.
- `sample_record.json` holds **real** trimmed Apple facts (latest 10-K per concept).
