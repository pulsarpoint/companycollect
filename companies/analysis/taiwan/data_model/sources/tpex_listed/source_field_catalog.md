# Taipei Exchange (TPEx) — OTC Listed Company Basic Info Field Catalog

## Source Summary

- Country: Taiwan
- Source type: financial_disclosure
- Organization: Taipei Exchange (TPEx / 證券櫃檯買賣中心)
- URL: https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O
- License: Open Government Data License, Taiwan
- Access: **public open JSON API** (no auth/payment)
- Freshness: daily
- Record shape: JSON array of OTC company objects (~890)
- Primary keys: SecuritiesCompanyCode
- Join keys: UnifiedBusinessNo. (= GCIS 統一編號), SecuritiesCompanyCode

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| SecuritiesCompanyCode | SecuritiesCompanyCode | OTC securities code | string | identifier |  | listed key |
| UnifiedBusinessNo. | UnifiedBusinessNo. | 統一編號 | string | identifier |  | **JOIN KEY to GCIS** |
| CompanyName | CompanyName | Company name | string | legal_name |  | |
| CompanyAbbreviation | CompanyAbbreviation | Short name | string | legal_name |  | |
| Registration | Registration | Foreign reg. country | string | geography |  | F-shares |
| SecuritiesIndustryCode | SecuritiesIndustryCode | Industry code | string | activity |  | TPEx codes |
| Address | Address | Registered address | string | address |  | |
| Chairman | Chairman | Chairman | string | person |  | **PERSONAL DATA — redact** |
| GeneralManager | GeneralManager | General manager | string | person |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- **Fully open** TPEx OpenAPI; one JSON array of all OTC (櫃買) listed companies (~890),
  with **English field names**. Complements TWSE for the over-the-counter market.
- **Join**: `UnifiedBusinessNo.` = **GCIS 統一編號** (= TWSE 營利事業統一編號). The
  `SecuritiesCompanyCode` is the OTC listed code.
- **Currency** TWD. **Personal data**: Chairman / GeneralManager are natural persons —
  redact (PDPA).
- A real full array is saved at `raw/api/tpex_listed.json`. No `sample_record.json` with
  cleartext personal names is reproduced here.
