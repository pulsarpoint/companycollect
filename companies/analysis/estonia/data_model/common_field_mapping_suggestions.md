# Estonia — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Estonia profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Estonia source | Estonia path | Notes |
|---|---|---|---|
| company_id | ariregister_company_data | ariregistri_kood | registrikood (8-digit) |
| registration_number | ariregister_company_data | ariregistri_kood | same |
| tax_id | ariregister_company_data | kmkr_nr | KMKR doubles as tax id (no separate open tax id) |
| vat_id | ariregister_company_data | kmkr_nr | EE + 9 digits |
| legal_name | ariregister_company_data | nimi | |
| status | ariregister_company_data | ettevotja_staatus_tekstina | code in ettevotja_staatus |
| legal_form | ariregister_company_data | ettevotja_oiguslik_vorm | OÜ/AS/… |
| incorporation_date | ariregister_company_data | ettevotja_esmakande_kpv | dd.mm.yyyy |
| dissolution_date | not_available_in_open_sources | — | derive from status (liquidation/bankruptcy) |
| registered_address | ariregister_company_data | ads_normaliseeritud_taisaadress | EHAK code available |
| activity_code | ariregister_annual_reports | EMTAK_myygitulu.emtak | EMTAK; from financial reports |
| financials | ariregister_annual_reports | aruannete_yldandmed + {year}_aruannete_elemendid | **structured** line items, EUR |
| officers | ariregister_persons_other | kaardile_kantud_isikud[] | board members; PII |
| owners | ariregister_shareholders / ariregister_beneficial_owners | osanikud[] / kasusaajad[] | registered shareholders AND beneficial owners (both open) |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Estonia is **best-in-class fully-open**: a single source (RIK, CC-BY 4.0) supplies identity, **structured
  financial statements**, **registered shareholders**, **beneficial owners** and officers — all open. Few
  countries offer structured financial line items + open beneficial ownership together.
- For a cross-country `financials` field, Estonia is a model of **structured** open financials (XBRL-like element
  names + EUR values) — no PDF/OCR step. Map directly; pivot elements per report.
- For `owners`, Estonia uniquely lets you populate **both** registered shareholders (osanikud) and beneficial
  owners (kasusaajad) — keep them as distinct sub-concepts rather than a single `owners` blob.
- `dissolution_date` and exact employee count are `not_available_in_open_sources` (derive status; revenue is in
  the reports).
