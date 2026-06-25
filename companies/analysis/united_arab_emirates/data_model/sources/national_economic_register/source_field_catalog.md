# National Economic Register (NER) Field Catalog

## Source Summary

- Country: United Arab Emirates
- Source type: official_registry
- Organization: Ministry of Economy (UAE)
- URL: https://economy.gov.ae/
- License: restricted (login)
- Access: **login-gated** (no open bulk/API)
- Freshness: live
- Record shape: per-company unified record (login-gated)
- Primary keys: economic_register_number
- Join keys: economic_register_number, trade_license_number, trn, legal_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| economic_register_number | Economic Register Number | National unified number | string | identifier |  | primary key |
| trade_license_number | License Number | Trade/commercial license | string | identifier |  | per emirate/free zone |
| trn | TRN | Tax Registration Number (15-digit) | string | identifier |  | FTA |
| legal_name | Legal Name (AR/EN) | Legal/trade name | string | legal_name |  | |
| license_authority | Licensing Authority | Issuing authority | string | metadata | Dubai DET, ADGM | DED / free zone |
| company_type | Legal Form | Legal form | string | legal_form | LLC, PJSC | |
| status | Status | Status | string | status |  | Active/Expired/Cancelled |
| activity | Economic Activities | Activities | array | activity |  | DED/ISIC |
| emirate | Emirate | Emirate | string | geography | Dubai | |

## Interpretation Notes

- The **NER** is the federal **unified company-search** layer (Ministry of Economy)
  across all emirates and free zones — the closest the UAE has to a single register.
  It is **login-gated**; the dedicated host `ner.economy.gov.ae` **does not resolve
  (NXDOMAIN)** and the service sits under `economy.gov.ae`. **No open bulk/API.**
- The field model is documented from **public knowledge**; **no real values copied**.
- **Identifiers**: the **economic register number** is the unified primary key; the
  **trade license number** comes from the issuing authority (emirate DED or free
  zone); the **TRN** (15-digit) links tax.
- The **licensing authority** field is critical — it routes to the underlying
  registry (emirate DED vs free zone). **Owners/managers** (when present) are
  personal data (PDPL) — redact.
- Implementation is **blocked on authentication** (login); planning-only.
