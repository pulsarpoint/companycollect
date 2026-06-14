# KvK Handelsregister Open Data Set — jaarrekeningen Field Catalog

## Source Summary

- Country: Netherlands
- Source type: official_financial_disclosure
- Organization: Kamer van Koophandel (KvK)
- URL: https://www.kvk.nl/producten-bestellen/kvk-jaarrekeningen-open-data-set/ (bulk: kvk-open-data-set-jaarrekeningen{0..5}.zip; HVDS API: opendata.kvk.nl/api/v1/hvds/jaarrekeningen/kvknummer/{nr})
- License: CC-BY 4.0
- Access: public (free; bulk + HVDS API with free key)
- Freshness: monthly (latest deposited accounts)
- Record shape: one XML file per deposited report (XBRL-derived `opendataField` key/value tree); **anonymised** (no KvK number)
- Primary keys: none (anonymised)
- Join keys: none in bulk (KvK-nummer via the HVDS/paid API)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| FinancialYear | FinancialYear | Financial year | integer | date | 2025 | |
| DocumentAdoptionDate | DocumentAdoptionDate | Adoption/deposit date | date | date | 2026-06-03 | |
| SbiBusinessCode | SbiBusinessCode | SBI activity | string | activity | 66120 | only entity descriptor |
| Assets | Assets | Total assets | decimal | financial | 428763 | EUR |
| AssetsCurrent | AssetsCurrent | Current assets | decimal | financial | 36869 | + Other |
| AssetsNoncurrent | AssetsNoncurrent | Fixed assets | decimal | financial | 391894 | + Other |
| Equity | Equity | Equity | decimal | financial | 275698 | can be negative |
| Liabilities | Liabilities | Total liabilities | decimal | financial | 153065 | |
| LiabilitiesMaturityLessThanOneYear | … | Short-term liabilities | decimal | financial | 153065 | + ExceedingOneYear |
| Provisions | Provisions | Provisions | decimal | financial | — | optional |
| CalledUpShareCapital | CalledUpShareCapital | Share capital | decimal | financial | — | equity component |
| BalanceSheetBeforeAfterAppropriationResults | … | Before/after appropriation | string | metadata | Na | Voor/Na |

## Interpretation Notes

- **Structured open financial data — but anonymised.** Verified: each ZIP (0..5+, ~200 MB) holds many individual
  `OpendataJaarrekening_{year}_{hash}.xml` files, one deposited annual report each, under **CC-BY 4.0**. The data
  is **XBRL-derived** balance-sheet figures: assets (current/non-current), equity, liabilities (by maturity),
  provisions, called-up share capital, plus financial year, adoption date, and SBI code. Real example:
  FinancialYear 2025, Assets 428763, Equity 275698, Liabilities 153065. Currency **EUR**.
- **Anonymised in bulk** — **no KvK number / name** (only the SBI code identifies the kind of entity). A
  jaarrekening cannot be linked to a named company from the bulk.
- **Identified access.** The **HVDS jaarrekeningen API** returns a company's accounts **by KvK number** (free with
  an API key) — the identified financial route.
- **Coverage.** Most NL companies (BV) file **micro/small abridged** accounts (balance sheet only; no
  income-statement detail). No `sample_record.json` here in addition — the field examples are verbatim from the
  downloaded sample XML.
