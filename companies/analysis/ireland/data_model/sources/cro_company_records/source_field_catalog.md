# CRO Open Data Portal — Company Records Field Catalog

## Source Summary

- Country: Ireland
- Source type: official_registry
- Organization: Companies Registration Office (CRO)
- URL: https://opendata.cro.ie/dataset/companies (download: companies.csv.zip)
- License: Creative Commons Attribution 4.0 (CC-BY-4.0)
- Access: public (free)
- Freshness: daily snapshot
- Record shape: one row per company (companies.csv, UTF-8)
- Primary keys: `company_num`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_num | company_num | CRO number (id) | string | identifier | 784992 | join key |
| company_name | company_name | Legal name | string | legal_name | SILVACRAFT FURNITURE LIMITED | trim double spaces |
| company_status | company_status | Status text | string | status | Normal | trailing spaces |
| company_status_code | company_status_code | Status code | string | status | 1151 | |
| company_type | company_type | Legal form | string | legal_form | LTD - Private Company Limited by Shares | |
| company_type_code | company_type_code | Type code | string | legal_form | 1153 | |
| company_reg_date | company_reg_date | Incorporation date | date | date | 2025-03-31 | ISO |
| comp_dissolved_date | comp_dissolved_date | Dissolution date | date | date | (empty if active) | |
| company_address_1..4 | company_address_1..4 | Registered address | string | address | Unit 13 …, Longford | concatenate |
| eircode | eircode | Eircode (postcode) | string | geography | N39 D880 | |
| nace_v2_code | nace_v2_code | NACE Rev.2 activity | string | activity | 3101.0 | strip trailing .0 |
| last_ar_date | last_ar_date | Last annual return | date | filing | 2025-09-30 | |
| nard | nard | Next annual return date | date | filing | 2026-09-30 | |
| last_accounts_date | last_accounts_date | Accounts last filed to | date | filing | (empty if none) | links to financials |
| company_status_date | company_status_date | Status effective date | date | date | | |
| princ_object_code | princ_object_code | Principal objects code | string | activity | | often empty |

## Interpretation Notes

- **The open spine.** Verified: **817,068 companies** (current + dissolved) keyed on the **CRO number**
  (`company_num`). Rich identity: name, status (code + text + date), legal form (code + text), incorporation and
  dissolution dates, registered address + **eircode**, **NACE Rev.2** activity, and filing signals
  (`last_ar_date`, `nard`, `last_accounts_date`). Daily snapshot; bulk CSV + CKAN API; **CC-BY 4.0**.
- **Normalization.** `nace_v2_code` carries a trailing `.0` (e.g. `3101.0`) — strip it. Status/name values may
  have trailing/double spaces — trim. CSV is ~193 MB — stream/chunk.
- **No officers / no VAT here.** Directors/officers are **not** in Company Records (they're in filed documents);
  VAT (`IE…`) is not in the CRO data (source via VIES/Revenue).
- A real `sample_record.json` (CRO 784992) is included from the downloaded CSV.
