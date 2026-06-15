# Sweden — Common Field Mapping Suggestions (cross-country mapper)

> **Suggestion only.** This file does **not** constrain the Sweden country-specific profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper that wants
> a thin common layer over many countries. Always ingest into the country-specific model first.

| Common field | Sweden source field | Source slug | Notes |
|---|---|---|---|
| company_id | organisationsnummer | bolagsverket_vdm / scb_fdb | Digits-only; personnummer for sole traders |
| registration_number | organisationsnummer | bolagsverket_vdm / scb_fdb | Same as company_id |
| tax_id | organisationsnummer | scb_fdb | Sweden uses orgnr as the tax identity base |
| vat_id | momsregistreringsnummer (SE+orgnr+01) | bolagsverket_vdm / derived | Active only if SCB moms flag set; mark derived |
| legal_name | organisationsnamn | bolagsverket_vdm | Bolagsverket authoritative; SCB företagsnamn fallback |
| status | status | bolagsverket_vdm | registrerad / avregistrerad / konkurs / likvidation |
| legal_form | juridisk_form | bolagsverket_vdm | AB, HB, KB, Enskild firma, Ekonomisk förening |
| incorporation_date | registreringsdatum | bolagsverket_vdm | ISO date |
| dissolution_date | avregistreringsdatum | bolagsverket_vdm | Only if deregistered |
| registered_address | postadress_organisation (+ kommun/län) | bolagsverket_vdm + scb_fdb | Address from Bolagsverket; kommun/län from SCB |
| activity_code | SNI 2025 (sni_kod / naringsgrenskod) | scb_fdb / bolagsverket_vdm | Union both; SCB fuller |
| financials | iXBRL K2/K3 annual-report concepts | bolagsverket_annual_reports | Parse iXBRL; per fiscal year |
| officers | `not_available_in_open_sources` | — | Board/officers not in the free open API set |
| owners | `not_available_in_open_sources` | — | Beneficial ownership (verklig huvudman) is restricted |
| source_provenance | source_provenance[] | all | One entry per contributing source |

## Concepts not available in Sweden open sources

- **officers** — `not_available_in_open_sources` (not part of the free VDM/SCB datasets).
- **owners / beneficial owners** — `not_available_in_open_sources` (verklig huvudman register is
  restricted, out of scope for open ingestion).
- **exact employee count** — `not_available_in_open_sources` (only SCB size-class bands).
- **pre-computed financial ratios** — `not_available_in_open_sources` (derive from iXBRL).

## Sweden-specific concepts a thin common layer would drop

These are worth keeping in the country-specific model even if a common mapper ignores them:

- **CFAR workplaces (arbetsställen)** — local-unit granularity unique to SCB.
- **F-skatt / employer / VAT register flags** — strong Swedish activity signals.
- **K2 vs K3 accounting framework** — determines financial-statement structure.
- **kommun / län** — Swedish administrative geography.
