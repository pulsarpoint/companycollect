# IRS Exempt Organizations Business Master File (EO BMF) Field Catalog

## Source Summary

- Country: United States
- Source type: official_tax_authority (federal)
- Organization: U.S. Internal Revenue Service
- URL: https://www.irs.gov/pub/irs-soi/eo1.csv (regions eo1–eo4)
- License: U.S. Government work / public domain
- Access: public (no key)
- Freshness: monthly (2nd Tuesday)
- Record shape: CSV, one row per tax-exempt organization
- Primary keys: `EIN` (zero-padded 9 digits, keep as string)
- Join keys: `EIN`
- Code list reference: **IRS Publication 5926** — https://www.irs.gov/pub/irs-pdf/p5926.pdf

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| EIN | EIN | Federal tax id (primary key) | string | identifier | 000019818 | Keep leading zeros |
| NAME | NAME | Org legal name | string | legal_name | AMERICAN LEGION | Uppercase |
| ICO | ICO | In-care-of contact | string | person | % JOANNE SOUSA | Often role, not person |
| STREET | STREET | Mailing street | string | address | PO BOX 471009 | Often PO box |
| CITY | CITY | City | string | address | BROOKLINE | |
| STATE | STATE | State postal code | string | geography | MA, NY | Mailing state |
| ZIP | ZIP | ZIP / ZIP+4 | string | address | 01069-1507 | Keep as string |
| GROUP | GROUP | Group exemption number | string | metadata | 0000 = none | Parent/central affiliation |
| SUBSECTION | SUBSECTION | IRC 501(c) subsection | string | legal_form | 03 = 501(c)(3) | Pub 5926 coded |
| AFFILIATION | AFFILIATION | Affiliation code | string | relationship | 3, 9 | Pub 5926 coded |
| CLASSIFICATION | CLASSIFICATION | Exempt purpose subclass | string | activity | 7000 | Pub 5926 coded |
| RULING | RULING | IRS ruling date YYYYMM | string | date | 195504 | Recognition date, NOT formation |
| DEDUCTIBILITY | DEDUCTIBILITY | Contribution deductibility | string | metadata | 1, 2 | Pub 5926 coded |
| FOUNDATION | FOUNDATION | Foundation status | string | legal_form | 10, 15, 16 | Public charity vs private foundation |
| ACTIVITY | ACTIVITY | Legacy 3×3-digit activity | string | activity | 001000000 | Deprecated; prefer NTEE_CD |
| ORGANIZATION | ORGANIZATION | Structure code | string | legal_form | 1=Corp,2=Trust,5=Assoc | Pub 5926 coded |
| STATUS | STATUS | Exempt status | string | status | 01 = active | Revoked orgs in separate list |
| TAX_PERIOD | TAX_PERIOD | Latest filing period YYYYMM | string | date | 202412 | Freshness of filing |
| ASSET_CD | ASSET_CD | Asset range code | string | financial | 0–9 | Band, not value |
| INCOME_CD | INCOME_CD | Income range code | string | financial | 0–9 | Band, not value |
| FILING_REQ_CD | FILING_REQ_CD | 990 filing requirement | string | filing | 01,02,06 | Pub 5926 coded |
| PF_FILING_REQ_CD | PF_FILING_REQ_CD | 990-PF requirement | string | filing | 0 | Pub 5926 coded |
| ACCT_PD | ACCT_PD | Fiscal year-end month | string | metadata | 12 | MM |
| ASSET_AMT | ASSET_AMT | Total assets (USD) | integer | financial | 2967709 | Blank for many |
| INCOME_AMT | INCOME_AMT | Income (USD) | integer | financial | 713976 | Blank for many |
| REVENUE_AMT | REVENUE_AMT | Revenue (USD) | integer | financial | 596954 | Blank for many |
| NTEE_CD | NTEE_CD | NTEE purpose taxonomy | string | activity | X20, N64, A80 | Preferred activity code |
| SORT_NAME | SORT_NAME | Secondary/chapter name | string | legal_name | 22 DEPT OF MAINE | Often blank |

## Interpretation Notes

- **Coverage:** tax-exempt organizations (nonprofits) only — churches, charities, veterans posts, labor unions, clubs, foundations. This is a true *national, EIN-keyed* dataset, which makes it valuable despite covering only the nonprofit slice. It does **not** cover for-profit companies.
- **EIN is the strongest cross-source join key** available in US open data: it can link an IRS record to a SAM.gov entity (when that entity is a nonprofit federal contractor/grantee) and is the federal tax id. Always store as a 9-char zero-padded string.
- **Heavy use of coded fields.** SUBSECTION, CLASSIFICATION, AFFILIATION, FOUNDATION, ORGANIZATION, STATUS, DEDUCTIBILITY, FILING_REQ_CD, ASSET_CD, INCOME_CD are all numeric codes whose meanings come from **Publication 5926**. Examples in this catalog (e.g. 03 = 501(c)(3), 19 = 501(c)(19) veterans, 05 = 501(c)(5) labor) are the well-known mappings; resolve the full set against Pub 5926 at ingestion rather than hard-coding.
- **`RULING` is a recognition date, not a formation date.** It is YYYYMM of the IRS exemption ruling — the closest available proxy for "established", but it can post-date actual formation by years. Mark it approximate.
- **Financials are sparse and point-in-time.** ASSET_AMT/INCOME_AMT/REVENUE_AMT are whole-dollar figures from the latest return (period in TAX_PERIOD); blank for orgs that file the 990-N postcard or are not required to file. The *_CD band codes exist even when the *_AMT value is blank.
- **Revocations live elsewhere.** Auto-revoked orgs are published in a separate IRS Auto-Revocation list, not the EO BMF — presence here implies currently-recognized exempt status.
- **Sample is a partial range download** (first ~4KB of eo1.csv) captured for header/field inspection; the full eo1–eo4 set is the complete file.
