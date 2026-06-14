# Luxembourg — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Luxembourg profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Luxembourg source | Luxembourg path | Notes |
|---|---|---|---|
| company_id | rcs_register | rcs_number | prefix = entity class (B/A/F) |
| registration_number | rcs_register | rcs_number | same |
| tax_id | rcs_register | matricule | 13-digit national id |
| vat_id | vies_vat | LU + 8 digits | separate; not in RCS data; validate via VIES |
| legal_name | rcs_register | denomination | |
| status | rcs_register | statut | inscrite/en liquidation/radiée |
| legal_form | rcs_register | forme_juridique | S.A./S.à r.l./SCSp |
| incorporation_date | rcs_register | date_constitution | |
| dissolution_date | not_available_in_open_sources | — | derive from status (radiée/en liquidation) |
| registered_address | rcs_register | siege_social | parse commune |
| activity_code | not_available_in_open_sources | — | no public NACE in the free RCS data |
| financials | rcs_annual_accounts (free PDF/eCDF) / commercial_aggregators (paid) | comptes annuels / vendor | free per company but document-based; EUR |
| officers | rcs_register (documents) / commercial_aggregators | gérants/administrateurs | in free RCS documents; PII |
| owners | not_available_in_open_sources | — | RBE beneficial ownership restricted |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Luxembourg is **partial-open / automation-blocked but document-rich**: the RCS gives **free** identity + **free
  document downloads** (incl. annual accounts), but the search is **captcha-gated** with **no open bulk/API**, so
  a cross-country pipeline needs manual lookups or a **commercial provider** (Kyckr, Creditreform).
- `financials` maps to the free RCS comptes annuels (PDF/eCDF, OCR/parse) or a vendor — not a structured open
  feed. eCDF filings parse more reliably than scanned PDFs.
- `officers` are in the free RCS documents; `owners` (beneficial ownership, RBE) is restricted →
  `not_available_in_open_sources` for open use.
- `tax_id` ← matricule (national id); `vat_id` via VIES (no open crosswalk). `activity_code` and
  `dissolution_date` are `not_available_in_open_sources` (derive dissolution from status).
- Two identifiers: **RCS number** (prefix = entity class) and the **matricule** (13-digit national id).
