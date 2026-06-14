# Greece — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Greece profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Greece source | Greece path | Notes |
|---|---|---|---|
| company_id | gemi_portal | gemi_number | register-side id |
| registration_number | gemi_portal | gemi_number | same |
| tax_id | gemi_portal / aade_rgwspublic | afm | 9-digit ΑΦΜ; cross-source key |
| vat_id | vies_vat | EL + afm | validate via VIES |
| legal_name | gemi_portal | επωνυμία | Greek + Latin |
| status | gemi_portal | κατάσταση | ΕΝΕΡΓΗ/ΛΥΘΕΙΣΑ/… |
| legal_form | gemi_portal | νομική μορφή | ΑΕ/ΕΠΕ/ΙΚΕ/ΟΕ/ΕΕ |
| incorporation_date | gemi_portal | ημερομηνία σύστασης | |
| dissolution_date | not_available_in_open_sources | — | derive from status (ΛΥΘΕΙΣΑ) |
| registered_address | gemi_portal | έδρα | parse δήμος/περιφέρεια |
| activity_code | gemi_portal / aade_rgwspublic | ΚΑΔ (primary from AADE) | NACE-aligned |
| financials | gemi_financial_statements (PDF) / commercial_aggregators (paid) | ισολογισμοί / vendor | not structured open; OCR or vendor; EUR |
| officers | gemi_portal | representatives | directors; PII |
| owners | not_available_in_open_sources | — | UBO register access-controlled |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Greece is a **partial-open / automation-blocked** country: GEMI identity is free to view **manually**, but
  the API is **reCAPTCHA-protected + rate-limited** with **no open bulk**, and financials are **PDF**. For a
  cross-country pipeline, Greece needs either manual lookups, **AADE credentials** (per-ΑΦΜ), or a **commercial
  provider** — there is no lawful open bulk/automation path.
- `financials` maps to GEMI PDFs (OCR) or a vendor — not a structured feed.
- `owners` (beneficial ownership) is `not_available_in_open_sources` (UBO register access-controlled); officers
  are available from GEMI.
- `dissolution_date` is `not_available_in_open_sources` as a field — derive from status.
- The **ΑΦΜ** is the clean universal key; pair every source on it.
