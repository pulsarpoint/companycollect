# Common field mapping suggestions — Bosnia and Herzegovina

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific BiH profile, which stays keyed on the JIB across the entity
> court registers.

| Common field | BiH source path | Notes |
|---|---|---|
| company_id | `registration.jib` (rs_business_register Records[].JIB) | 13-digit; country-wide id |
| registration_number | `registration.mbs` / `registration.mb` | court reg number / statistical number |
| tax_id | `registration.jib` | JIB is also the tax id |
| vat_id | `tax_identifiers.pdv_broj` (UINO) | 12-digit PDV; separate, only if VAT-registered |
| legal_name | `legal_identity.business_name` | PoslovnoIme / Naziv |
| status | `status.status_text` | registrovan / likvidacija / stečaj / brisan |
| legal_form | `legal_identity.legal_form` | d.o.o. / a.d. / d.d. / s.p. |
| incorporation_date | not_available_in_open_sources | only on per-company extract/PDF |
| dissolution_date | not_available_in_open_sources | only on per-company extract/PDF |
| registered_address | `registered_location.registered_address` | Sjedište (free-text) |
| activity_code | `activity.activity_code` | KD BiH (~NACE Rev.2) |
| financials | `financial_statements[]` | APIF RFI / FIA, paid, BAM — planning-only |
| officers | `officers[]` | OdgovornoLice / representatives — PERSONAL DATA, redact |
| owners | `owners[]` | Osnivači — REDACT natural persons |
| source_provenance | `source_provenance[]` | per-section provenance |

## Cross-country notes

- BiH is **federal**: no single national register. Route by entity (RS vs FBiH vs
  Brčko) but join everything on the **JIB**.
- The RS register is the **only open structured (JSON) source**; FBiH/Brčko is a
  per-company APEX portal; financials are **paid**.
- `company_id == tax_id == JIB`; `vat_id` (PDV broj) is separate.
- Treat founders/officers as personal data (BiH Law on Protection of Personal
  Data) — redact in shared outputs.
