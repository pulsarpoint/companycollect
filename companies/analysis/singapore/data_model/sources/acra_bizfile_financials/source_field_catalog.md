# ACRA BizFile+ — Business Profiles & Financial Statements Field Catalog

> **PLANNING-ONLY / PAID.** Full business profiles (officers, shareholders, share
> capital) and financial statements (filed in **XBRL** via BizFinx) are sold
> **per-document** on BizFile+. No open bulk/API. Cataloged from public
> documentation only — no records fetched. Officer/shareholder data is personal
> data (PDPA).

## Source Summary

- Country: Singapore
- Source type: financial_disclosure
- Organization: ACRA
- URL: https://www.bizfile.gov.sg/
- License: restricted (paid)
- Access: paid per-document
- Freshness: filing-driven
- Record shape: per-entity business profile + XBRL financials
- Primary keys: `uen`
- Join keys: `uen`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| profile.uen | UEN | Entity id | string | identifier | join to ACRA entities |
| profile.officers | Officers/Directors | Officers | array | person | **PERSONAL DATA (PDPA)** |
| profile.shareholders | Shareholders | Ownership | array | ownership | **PERSONAL DATA where individuals** |
| profile.share_capital | Issued/Paid-up capital | Capital (SGD) | decimal | financial | paid |
| financials.revenue | Revenue | Revenue (SGD) | decimal | financial | XBRL |
| financials.profit | Profit/(Loss) | Net profit (SGD) | decimal | financial | XBRL |
| financials.total_assets | Total assets | Total assets (SGD) | decimal | financial | XBRL |
| financials.financial_year_end | Financial year end | Period | date | date | |

## Interpretation Notes

- This is the **authoritative source of officers, shareholders, share capital, and
  private-company financial statements** — none of which are in the open ACRA
  dataset (which carries only the officer **count**).
- Financial statements are filed in **XBRL** (BizFinx) for companies required to
  file; profiles + statements are sold **per-document** on BizFile+. Currency SGD.
- **Join**: on the **UEN** to the open ACRA entities dataset.
- **Personal data**: officers/shareholders are personal data under **PDPA** —
  redact. No raw sample record (paid source).
