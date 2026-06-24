# Ársreikningaskrá (Annual Accounts Register) Field Catalog

> **PLANNING-ONLY / PAID.** Companies file annual accounts electronically
> (Hnappurinn) with the Annual Accounts Register for **public disclosure**, but
> retrieval of the filed statements is **paid per-document** — no open bulk/XBRL.
> Cataloged from public documentation only; no records fetched.

## Source Summary

- Country: Iceland
- Source type: financial_statements
- Organization: Skatturinn (Iceland Revenue and Customs)
- URL: https://www.skatturinn.is/fyrirtaekjaskra/arsreikningaskra/
- License: restricted (paid retrieval)
- Access: paid per-document
- Freshness: annual filings
- Record shape: per-company, per-year annual account
- Primary keys: `kennitala` + `fiscal_year`
- Join keys: `kennitala`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| account.kennitala | Kennitala | Filer id | string | identifier | join key |
| account.fiscal_year | Reikningsár | Fiscal year | string | date | |
| account.revenue | Rekstrartekjur | Operating revenue (ISK) | decimal | financial | paid |
| account.profit | Hagnaður/tap | Profit/loss (ISK) | decimal | financial | paid |
| account.total_assets | Eignir samtals | Total assets (ISK) | decimal | financial | paid |
| account.equity | Eigið fé | Equity (ISK) | decimal | financial | paid |

## Interpretation Notes

- Companies submit annual accounts electronically (the "Hnappurinn" e-filing) for
  **public disclosure** ("til opinberrar birtingar"), but **retrieval is paid
  per-document**; there is no open bulk/XBRL download.
- Keyed on the **kennitala** (joins to the Fyrirtækjaskrá identity). Currency
  **ISK**.
- This is the authoritative source of Icelandic company financials, but **not
  openly available**. No raw sample record (paid source).
