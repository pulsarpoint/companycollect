# MCA Annual Financial Statements (AOC-4 / XBRL) Field Catalog

> **PLANNING-ONLY / PAID.** Full company financial statements are filed with MCA
> (Form AOC-4; XBRL for larger companies) and are **pay-per-document** on the
> MCA21 "View Public Documents" service. No open bulk or API. Cataloged from
> public documentation only — no records fetched, no values copied.

## Source Summary

- Country: India
- Source type: financial_disclosure
- Organization: Ministry of Corporate Affairs
- URL: https://www.mca.gov.in/
- License: restricted (paid)
- Access: paid per-document (MCA21)
- Freshness: annual filings (fiscal year Apr–Mar)
- Record shape: per-company, per-year XBRL/PDF document
- Primary keys: `cin` + `financial_year`
- Join keys: `cin`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| filing.cin | CIN | Filer CIN | string | identifier | join key |
| filing.financial_year | Financial Year | Fiscal year | string | date | Apr–Mar |
| filing.turnover | Turnover | Revenue (INR) | decimal | financial | paid |
| filing.net_profit | Profit/Loss | Net profit (INR) | decimal | financial | paid |
| filing.net_worth | Net worth | Net worth (INR) | decimal | financial | paid |
| filing.total_assets | Total assets | Total assets (INR) | decimal | financial | paid |
| filing.borrowings | Borrowings | Borrowings (INR) | decimal | financial | paid |

## Interpretation Notes

- This is the **authoritative all-company financial source**, but it is
  **pay-per-document** with no open bulk — so it cannot be ingested openly.
- Larger companies file in **XBRL** (Ind-AS / Schedule III taxonomy); smaller ones
  file PDF AOC-4. Currency INR; fiscal year ends 31 March.
- The open master data only signals the **latest filing year** (`latest_year_bs`),
  not the figures — those live here.
- Join on **CIN**. No raw sample record (paid source).
