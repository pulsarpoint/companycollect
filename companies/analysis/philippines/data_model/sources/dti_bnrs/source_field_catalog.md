# DTI BNRS — Business Name Registration Field Catalog

## Source Summary

- Country: Philippines
- Source type: official_registry (sole proprietorships)
- Organization: Department of Trade and Industry (DTI)
- URL: https://bnrs.dti.gov.ph/search
- License: not stated (verification use)
- Access: public free search
- Freshness: live
- Record shape: per-business-name search result
- Primary keys: bn_number
- Join keys: bn_number, business_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| business_name | Business Name | Trade name | string | legal_name |  | sole props only |
| bn_number | BN Number | DTI registration number | string | identifier |  | sole-prop id |
| status | Status | Status | string | status |  | Active/Expired/Cancelled |
| scope | Scope | Territorial coverage | string | metadata |  | Barangay/City/Regional/National |
| region | Region | Region | string | geography |  | |

## Interpretation Notes

- **DTI BNRS** registers **business names for sole proprietorships** — distinct from
  the SEC corporate register. Its free **search/verification** confirms a business
  name's existence, **BN number**, status, scope, and region.
- It does **not** cover corporations/partnerships (those are SEC) and carries no
  financials. Use for **sole-proprietor** existence checks only.
- The sole proprietor's name behind a business name is personal data (Data Privacy
  Act 2012) — handle with care.
