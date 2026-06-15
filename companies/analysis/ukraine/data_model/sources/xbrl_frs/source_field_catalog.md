# Financial Reporting System — IFRS Financial Statements (XBRL) Field Catalog

> **DOCUMENTED.** Ukraine mandates XBRL for IFRS reporters (full rollout by Nov
> 2025) via a single Financial Reporting Collection Centre; data is open and
> integrated to XBRL International (filings.xbrl.org). Cataloged from official
> NSSMC/XBRL sources; the exact open-bulk endpoint is to be confirmed at
> implementation (no records pulled here).

## Source Summary

- Country: Ukraine
- Source type: official_financial
- Organization: NSSMC / Financial Reporting Collection Centre; XBRL International
- URL: https://www.nssmc.gov.ua/ ; https://filings.xbrl.org/
- License: open (XBRL International / NSSMC disclosure)
- Access: public
- Freshness: annual
- Record shape: XBRL facts per filing (UA MSFS taxonomy)
- Primary keys: `edrpou` + `period`
- Join keys: `edrpou`

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| edrpou | entity id | Filer EDRPOU | string | identifier | join to EDR |
| period | reporting period | Fiscal year | string | date | XBRL context |
| assets_total | Assets | Total assets | decimal | financial | UAH |
| equity_total | Equity | Total equity | decimal | financial | UAH |
| revenue | Revenue | Revenue | decimal | financial | UAH |
| net_profit | ProfitLoss | Net profit/loss | decimal | financial | UAH |

## Interpretation Notes

- The **structured route to Ukrainian financials** — balance sheet + income
  statement facts tagged to the **UA MSFS XBRL** taxonomy, keyed on **EDRPOU**.
- Coverage = **IFRS reporters** (banks, PIEs, larger companies and voluntary
  adopters), not every SME. Confirm the precise open endpoint (NSSMC portal /
  filings.xbrl.org) and the entity-id scheme before hardcoding tags.
