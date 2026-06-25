# Common field mapping suggestions — Morocco

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Morocco profile, which stays keyed on the ICE.

| Common field | Morocco source path | Notes |
|---|---|---|
| company_id | `registration.ice` (OMPIC ICE) | 15-digit unified id |
| registration_number | `registration.rc_number` (Numéro RC) | per court |
| tax_id | `tax_identifiers.if_tax_id` (IF) | DGI |
| vat_id | not_available_in_open_sources | TVA tied to the IF (no separate number) |
| legal_name | `legal_identity.legal_name` | OMPIC (gated) / Bourse (listed) |
| status | `status.status_text` | en activité/liquidation/radiée |
| legal_form | `legal_identity.legal_form` | SA/SARL/SARL-AU/SNC/SCS/succursale |
| incorporation_date | not_available_in_open_sources | in gated OMPIC detail |
| dissolution_date | not_available_in_open_sources | status implies it |
| registered_address | `registered_location.registered_address` | OMPIC (gated/paid) |
| activity_code | `activity.activity_object` / `activity.bourse_sector` | OMPIC NMA / Bourse sector |
| financials | `financial_statements[]` | Casablanca Bourse (listed, open) / OMPIC Bilans (paid) — MAD |
| officers | `officers[]` (Dirigeants) | PERSONAL DATA — redact; paid |
| owners | `officers[]` (Associés) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == ICE` (15-digit unified); `registration_number == Numéro RC`;
  `tax_id == IF`; no separate `vat_id` (TVA tied to the IF).
- The defining constraint is **OMPIC reCAPTCHA + paid** access (with a paid
  subscription API); the only **open** source is the **Casablanca Stock Exchange**
  (listed). data.gov.ma has no company dataset. Currency **MAD**; French + Arabic.
- Treat dirigeants/associés as personal data (Law 09-08) — redact.
