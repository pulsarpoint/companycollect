# Common field mapping suggestions — United Arab Emirates

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific UAE profile, which is keyed on the trade-license / free-zone
> registration number (per authority) with the NER economic register number as the
> unified id.

| Common field | UAE source path | Notes |
|---|---|---|
| company_id | `registration.trade_license_number` (or free-zone reg no) | per issuing authority |
| registration_number | `registration.trade_license_number` / `economic_register_number` | per-authority / unified (NER) |
| tax_id | `tax_identifiers.trn` | 15-digit, FTA |
| vat_id | `tax_identifiers.trn` | = TRN (no separate VAT number) |
| legal_name | `legal_identity.legal_name` | DED (gated) / DFM-ADX (listed) |
| status | `status.status_text` | Active/Expired/Cancelled/Dissolved |
| legal_form | `legal_identity.company_type` | LLC/PJSC/FZE/branch |
| incorporation_date | not_available_in_open_sources | gated registry (DED issue date / free-zone inc. date) |
| dissolution_date | not_available_in_open_sources | status implies it |
| registered_address | `registered_location.registered_address` | free-zone register (gated) |
| activity_code | `activity.activities` / `activity.exchange_sector` | DED/ISIC / exchange sector |
| financials | `financial_statements[]` | DFM/ADX listed (AED, WAF-gated); private not open |
| officers | not_available_in_open_sources | managers in gated DED records |
| owners | `owners[]` | PERSONAL DATA — redact; gated |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- The UAE has **no single national company id** — `company_id` depends on the issuing
  authority (emirate DED trade license, or free-zone registration number); the **NER
  economic register number** is the closest unified id. `tax_id == vat_id == TRN`.
- The defining constraint is **end-to-end gating**: every registry layer (NER, emirate
  DEDs, DIFC/ADGM, DFM/ADX) is login/WAF/rate-limited, and the open-data portals were
  unreachable. There is **no open company register and no open programmatic
  financials**. Currency **AED**; Arabic + English.
- Treat owners/managers/directors as personal data (PDPL; DIFC DP Law 2020; ADGM DP
  Regs 2021) — redact.
