# BSE / NSE / SEBI Listed-Company Disclosures Field Catalog

> **PLANNING-ONLY / EXCHANGE TERMS.** Open financials for **listed** companies
> only (CIN starts `L`). Published by BSE/NSE and under SEBI LODR disclosure
> rules; reuse governed by each exchange's website terms of use. Cataloged from
> public documentation only — not fetched here.

## Source Summary

- Country: India
- Source type: financial_disclosure
- Organization: BSE Ltd / NSE / SEBI
- URL: https://www.bseindia.com/ , https://www.nseindia.com/
- License: exchange terms of use (verify before redistribution)
- Access: public via exchange websites (anti-bot likely)
- Freshness: quarterly results + event-driven disclosures
- Record shape: per-listed-company results + shareholding
- Primary keys: `scrip_code` / `isin`
- Join keys: `cin`, `isin`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| listing.scrip_code | Scrip Code / Symbol | Exchange code | string | identifier | listed only |
| listing.isin | ISIN | Security id | string | identifier | join to CIN |
| listing.company_name | Company Name | Listed name | string | legal_name | |
| results.period | Quarter/Year ended | Period | string | date | |
| results.revenue | Revenue from operations | Revenue (INR) | decimal | financial | SEBI LODR |
| results.net_profit | Net Profit/Loss | Net profit (INR) | decimal | financial | SEBI LODR |
| shareholding.pattern | Shareholding Pattern | Promoter/public split | array | ownership | quarterly |

## Interpretation Notes

- Covers only **listed** companies (a small fraction of the MCA universe; CIN
  begins with `L`). For the vast majority of (unlisted) Indian companies there is
  **no open financial statement** source — only paid MCA documents.
- **Join**: ISIN ↔ CIN via the exchange/SEBI master; or by name. Then to MCA
  master data on CIN.
- **Shareholding pattern** is aggregate (promoter/public/institutional), not
  individual personal data.
- Currency INR. Respect exchange terms; anti-bot protections likely. No raw sample.
