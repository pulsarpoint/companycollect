# Spain — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Spain's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Spain source path | Notes |
|---|---|---|
| company_id | registration.cif (else registration.hoja_registral else openmercantil_slug) | No single always-present id; CIF sparse (~18%). |
| registration_number | registration.hoja_registral (+ tomo/folio) | Province-scoped registry sheet; the stable open key. |
| tax_id | registration.cif | Spanish CIF/NIF; **sparse in open data**. |
| vat_id | "ES" + registration.cif | Derived. |
| legal_name | legal_identity.name | Uppercase, form suffix embedded. |
| status | status.derived | From BORME Disolución/Extinción + name suffix; no clean code list. |
| legal_form | legal_identity.company_type | Derived from name suffix / CIF leading letter (SL/SA/SLU…). |
| incorporation_date | acts[] Constitución date (else openmercantil.first_seen) | first_seen is a proxy only. |
| dissolution_date | status.dissolution_date | From BORME Disolución/Extinción act. |
| registered_address | registered_location.registered_address | Free text; sparse in open data. |
| activity_code | not_available_in_open_sources | CNAE only inside Constitución objeto social text (derive) or DIRCE aggregate. |
| financials | financial_statements[] | CNMV (open, listed) + Registro Mercantil (paid, general). Revenue null for micro models. |
| officers | officers[] | From BORME acts (PII; GDPR). |
| owners | ownership.sole_shareholder only | General cap table / beneficial ownership NOT open. |
| source_provenance | source_provenance[] | Per-source provenance retained. |

## Cross-country notes for a future mapper

- **No single national company number always present.** Use CIF when available, else the **Hoja
  registral** (province + number — keep the province, it is not globally unique alone). A cross-country
  `registration_number` mapping must retain the province/registry scope.
- **CIF is sparse in open data (~18%)** — a cross-country `tax_id` mapper must tolerate nulls and expect
  an enrichment/matching step before financials attach.
- **Financials are split**: open only for **listed** issuers (CNMV); the general population is **XBRL but
  paid** (Registro Mercantil). A cross-country `financials` mapper must tolerate Spain's
  `financial_statements[]` being empty (no listing + no purchase) and `revenue` null for micro filers.
- **Activity code** is `not_available_in_open_sources` cleanly — derive from the Constitución objeto
  social or enrich.
- **Ownership** beyond the sole-shareholder case is not open.
- **Currency** is effectively always EUR for Spain, but store it per figure rather than hardcoding.
