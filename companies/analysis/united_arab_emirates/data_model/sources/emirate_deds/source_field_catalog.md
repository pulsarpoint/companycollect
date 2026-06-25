# Emirate DEDs — trade-license registries Field Catalog

## Source Summary

- Country: United Arab Emirates
- Source type: official_registry
- Organization: Emirate Departments of Economic Development (Dubai DET, Abu Dhabi ADDED, Sharjah SEDD, etc.)
- URL: https://invest.dubai.ae/
- License: restricted
- Access: **per-emirate, WAF/login-gated**
- Freshness: live
- Record shape: per-company trade-license record (per emirate; gated)
- Primary keys: trade_license_number
- Join keys: trade_license_number, trn, trade_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| trade_name | Trade Name | Trade/company name | string | legal_name |  | AR/EN |
| trade_license_number | Trade License Number | License number | string | identifier |  | primary key (per emirate) |
| license_type | License Type | License type | string | metadata | Commercial | |
| legal_form | Legal Form | Legal form | string | legal_form | LLC | |
| status | Status | Status | string | status |  | Active/Expired/Cancelled |
| activities | Activities | Activities | array | activity |  | DED codes |
| issue_date | Issue Date | Issue date | date | date |  | |
| expiry_date | Expiry Date | Expiry date | date | date |  | annual renewal |
| owners | Owners / Partners | Owners | array | ownership |  | PERSONAL DATA — redact |

## Interpretation Notes

- Mainland companies are licensed by the **emirate-level DEDs** — Dubai (Department
  of Economy & Tourism / **Invest in Dubai**), Abu Dhabi (**ADDED** via TAMM), Sharjah
  (**SEDD**), and the other emirates. Each maintains its **own trade-license registry**
  and verification tool — **WAF/login-gated** (Invest in Dubai returned **403**).
- The field model is documented from **public knowledge**; **no real values copied**.
- **Identifiers**: the **trade license number** is the company id **within that
  emirate** (formats differ per emirate); the **TRN** (FTA) is the cross-emirate tax
  key. There is **no single national number** at this layer — use the **NER** to
  unify across emirates.
- **Trade licenses expire annually** (the `expiry_date` field matters for "active"
  status). **Owners/partners** are personal data (PDPL) — redact.
- Implementation is **blocked on authentication** (per-emirate WAF/login); planning-
  only. Each emirate would be a separate sub-source.
