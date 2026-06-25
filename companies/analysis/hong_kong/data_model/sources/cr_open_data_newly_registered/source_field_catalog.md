# Companies Registry — Newly Incorporated / Registered Companies (Open Data) Field Catalog

## Source Summary

- Country: Hong Kong
- Source type: official_registry
- Organization: Companies Registry (CR), HKSAR Government
- URL: https://data.gov.hk/en-data/dataset/hk-cr-crdata-list-newly-registered-companies-2526
- License: data.gov.hk Terms and Conditions of Use (license id not exposed — confirm)
- Access: **public open CSV/XLS** (no auth/payment)
- Freshness: weekly
- Record shape: weekly CSV, two streams — RNC063L (local) and RNC063F (non-HK)
- Primary keys: BR Number
- Join keys: BR Number

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Seq | Seq | Row sequence | integer | metadata | 1, 7 | per-file ordinal |
| Current Company Name in English | (RNC063L) | English name | string | legal_name | 197 VENTURES HK LIMITED | local stream |
| Current Company Name in Chinese | (RNC063L) | Chinese name | string | legal_name | 眾加國際有限公司 | often empty |
| BR Number | BR Number | IRD Business Registration no. | string | identifier | 77541433 | **primary key (≠ CR No.)** |
| Date of Incorporation | (RNC063L) | Incorporation date | date | date | 02-01-2025 | DD-MM-YYYY |
| Date of Registration | (RNC063F) | HK registration date | date | date | 02-01-2025 | non-HK stream |
| Date of Change of name | Date of Change of name | Name-change date | date | date | 03-01-2025 | name-change rows |
| Current Approved Name for Carrying on Business in H.K. | (RNC063F) | Approved HK business name | string | legal_name |  | non-HK stream |

## Interpretation Notes

- Two weekly streams: **RNC063L** = newly **incorporated local** companies (columns: English
  name, Chinese name, BR Number, Date of Incorporation, Date of Change of name); **RNC063F**
  = newly **registered non-Hong-Kong** companies (columns: Corporate Name / Other Corporate
  Name, Current Approved Name for Carrying on Business in H.K., BR Number, Date of
  Registration, Date of Change of name). Verified live: `RNC063L_20241230.csv` = **3,286
  rows**.
- **Identifier**: the feed exposes the **BR Number** (Inland Revenue Department Business
  Registration number, 8-digit) — **not** the CR Company Number used by ICRIS. There is no
  registered address, status detail, or officer data in this feed.
- **No personal data** — company-level only. Safe to store real values.
- **Incremental**: each weekly file lists new/changed entries; accumulate over time to build
  a company list. It is **not** a full-register snapshot.
- **Dates** are `DD-MM-YYYY` (Gregorian) — convert to ISO 8601. Encoding UTF-8 with BOM.
- A real sample is saved at `raw/bulk/RNC063L_20241230.csv`; a redaction-free
  `sample_record.json` is included (no PII in this source).
