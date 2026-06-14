# Romania — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Romania profile, which is authoritative.

| Common field | Romania source | Romania path | Notes |
|---|---|---|---|
| company_id | onrc_od_firme | registration.cui | fall back to cod_inmatriculare when CUI=0 |
| registration_number | onrc_od_firme | registration.cod_inmatriculare | J40/.../yyyy or numeric |
| tax_id | onrc_od_firme | registration.cui | CUI = tax id |
| vat_id | anaf_ws_tva | tax_identifiers.vat_id | RO + CUI when VAT-registered |
| legal_name | onrc_od_firme | legal_identity.legal_name | |
| status | onrc_od_stare_firma | status.status_code | 1048 active / 1084 struck off / 2069 dissolution |
| legal_form | onrc_od_firme | legal_identity.legal_form | SRL/SA/PF/PFA/... |
| incorporation_date | onrc_od_firme | incorporation.registration_date | DD/MM/YYYY |
| dissolution_date | not_available_in_open_sources | — | only implied by status code |
| registered_address | onrc_od_firme | registered_location.full_address | reassemble ADR_* |
| activity_code | anaf_bilant / onrc_od_caen_autorizat | activity.caen_main / caen_authorized[] | CAEN ≈ NACE |
| financials | anaf_bilant | financial_statements[] | structured, RON, 2014-2024 |
| officers | onrc_od_reprezentanti_legali | officers[] | OPEN but PII — redact |
| owners | onrc_portal_recom / onrc_rbr | ownership.* | paid / restricted — planning-only |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Two-identifier country.** A cross-country mapper must handle Romania's split:
  `company_id`/`tax_id` = **CUI** (ANAF/VAT space), while the register's internal
  joins use **COD_INMATRICULARE**. `vat_id = "RO" + CUI`.
- **Financials are a first-class open source.** Unlike most EU registers, Romania
  offers structured per-company financials for free via a single API — map
  directly into `financials[]` (RON, plain units), keyed on CUI/year. Map by
  indicator **code** (I13, I18, …), not array order.
- **Officers open, owners closed.** `officers` is populated from open data (with
  GDPR redaction); `owners` (shareholders/beneficial owners) is **not** open —
  mark planning-only, do not synthesise.
