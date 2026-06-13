# France — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how France's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | France source path | Notes |
|---|---|---|
| company_id | registration.siren | Single clean national id (9 digits) — always present. |
| registration_number | registration.siren | SIREN is both the id and the registration number. |
| tax_id | registration.siren | SIREN underlies the SIRET/VAT; no separate company tax id. |
| vat_id | registration.vat_id | Computable: `FR` + key(2) + SIREN. |
| legal_name | legal_identity.nom_raison_sociale (else nom_complet) | |
| status | status.etat_administratif (A/C) + derived | Clean code; BODACC adds insolvency. |
| legal_form | legal_identity.nature_juridique | INSEE catégorie juridique code (needs label table). |
| incorporation_date | (Sirene dateCreationUniteLegale) / recherche date_creation | |
| dissolution_date | status.date_fermeture | |
| registered_address | registered_location.* | Head-office (siège) + geo coords. |
| activity_code | activity.naf_rev2 / activity.naf2025 | **Available and clean** (France, unlike DE/ES). |
| financials | financial_statements[] | **Open**: headline ca+résultat net (no auth) + full INPI statements (free account). Nullable under confidentiality. |
| officers | officers[] | dirigeants (PII; GDPR). |
| owners | beneficial_owners[] = restricted (planning-only) | RBE not open since 2022. |
| source_provenance | source_provenance[] | Per-source provenance retained. |

## Cross-country notes for a future mapper

- **France is the clean-key case**: one `siren` joins every source. A cross-country mapper gets France
  "for free" on identity — no fuzzy matching, unlike Germany (no key) or Spain (sparse CIF).
- **Activity code is present and clean** (NAF Rev2 + NAF2025) — `activity_code` is NOT
  `not_available_in_open_sources` for France (contrast DE/ES).
- **Financials are open** (headline pair no-auth; full statements free account) — a cross-country
  `financials` mapper should still tolerate **nulls** (confidentiality option) and a per-record `source`
  discriminator (headline vs full).
- **Ownership**: beneficial owners restricted (planning-only); only dirigeants are open.
- **Currency** effectively always EUR; store per figure rather than hardcoding.
- **No separate company tax id** — SIREN is the id, registration number, and VAT root.
