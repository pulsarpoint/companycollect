# Netherlands — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Netherlands profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Netherlands source | Netherlands path | Notes |
|---|---|---|---|
| company_id | kvk_handelsregister_api | kvkNummer | 8-digit; NOT in open bulk (anonymised) |
| registration_number | kvk_handelsregister_api | kvkNummer | same |
| tax_id | kvk_handelsregister_api | rsin | 9-digit RSIN |
| vat_id | vies_vat | NL + rsin + B + 2 | derivable; validate via VIES |
| legal_name | kvk_handelsregister_api | naam | PAID; stripped from open data |
| status | kvk_open_basis | Actief (+ Insolventie) | open (anonymised) |
| legal_form | kvk_open_basis | Rechtsvorm | BV/NV/EZ/VOF/Stichting (open) |
| incorporation_date | kvk_open_basis | Datum aanvang | open; YYYYMMDD |
| dissolution_date | not_available_in_open_sources | — | derive from Actief / API |
| registered_address | kvk_handelsregister_api | adressen | PAID; open has only postcode region |
| activity_code | kvk_open_basis | Hoofdactiviteiten / SBI activiteiten | open; SBI (NACE-aligned) |
| financials | kvk_open_jaarrekeningen (open, anonymised) / commercial_aggregators (identified, paid) | XBRL balance sheet / vendor | open structured but anonymised; EUR |
| officers | kvk_handelsregister_api (paid) | functionarissen | PII |
| owners | ubo_register (restricted) | ubo[] | beneficial owners restricted (AML) |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- The Netherlands is a **split-access** country: genuinely **open (CC-BY 4.0)** basic data + **structured
  financials**, but **anonymised in bulk** (no KvK number). For a cross-country pipeline, the open bulk is for
  **statistics/benchmarks**; **identified** company data (name, address, officers, and financials linked to a
  named company) needs the **free HVDS API by KvK number**, the **paid KvK API**, or a **commercial provider**.
- `financials` maps to the open jaarrekeningen (XBRL balance-sheet figures, EUR) — structured but anonymised in
  bulk; identified via the HVDS API by KvK number.
- `legal_name`/`officers` are paid; `owners` (UBO) is restricted (AML); `dissolution_date` and income-statement
  detail are largely `not_available_in_open_sources`.
- Identifiers: **KvK-nummer** (8, join key), **RSIN** (9, VAT base), vestigingsnummer (12). VAT = NL+RSIN+B+2.
