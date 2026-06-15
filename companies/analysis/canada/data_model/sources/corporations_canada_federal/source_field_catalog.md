# Corporations Canada — Federal Corporations Field Catalog

## Source Summary

- Country: Canada
- Source type: official_registry
- Organization: ISED — Corporations Canada
- URL: https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2
- License: Open Government Licence – Canada (OGL)
- Access: public
- Freshness: periodic
- Record shape: CSV, one row per federal corporation, **17 columns**
- Primary keys: `Corporation number`
- Join keys: `Corporation number`, `Business number (BN)`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Corporation number | (same) | Federal corp id | string | identifier | 8660115 | primary key |
| Business number (BN) | (same) | CRA tax id | string | identifier | 835752437 | tax/join key; GST/HST=BN+RT |
| Corporate name - form 1 | (same) | Name (EN) | string | legal_name | MINDANGLER CAPITAL INC. | |
| Corporate name - form 2 | (same) | Name (FR) | string | legal_name | | bilingual; often blank |
| Governing legislation | (same) | Act | string | legal_form | Canada Business Corporations Act | CBCA / non-CBCA |
| Status | (same) | Status | string | status | Active | active vs inactive files |
| Anniversary date | (same) | Anniversary date | date | date | 2013-10-10 | ≈ incorporation |
| Year of last annual filing | (same) | Last annual return | integer | filing | 2025 | compliance |
| Date of last annual meeting | (same) | Last AGM | date | date | 2023-10-10 | |
| Street/City/Province/Country/Postal code | (address) | Registered office | string | address | 515 Legget Drive, Ottawa, ON, K2K 3G4 | full address |
| Min/Max number of directors | (same) | Director counts | integer | metadata | 1 / 12 | not names |

## Interpretation Notes

- The **open federal register**: 8 CSVs (active/inactive × CBCA/non-CBCA × EN/FR).
  The active CBCA business-corporations file = **642,720** rows. OGL.
- **Covers federally-incorporated corporations ONLY** (CBCA business corps +
  non-CBCA NFP/cooperatives/boards of trade). **Provincially-incorporated companies
  are NOT here** — a major coverage caveat (use provincial registries).
- **Identifiers**: `Corporation number` (federal id) + **BN** (CRA tax id). Canada
  has **no separate VAT** — GST/HST registration is the BN + RT program account.
- **Bilingual** names (form 1 / form 2). Full **registered address** included
  (unlike Australia's state+postcode). **No NAICS / activity code** and **no
  financials** in this dataset; **director names** are only in the API (counts
  only here).
- `sample_record.json` is a real record (MINDANGLER CAPITAL INC., corp # 8660115).
