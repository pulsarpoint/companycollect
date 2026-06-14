# Czech Republic — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Czech profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Czech source | Czech path | Notes |
|---|---|---|---|
| company_id | ares_api | ico | IČO (8-digit; normalize padding) |
| registration_number | ares_api | ico | same as company_id |
| tax_id | ares_api | dic | DIČ = CZ + IČO |
| vat_id | ares_api | dic | confirm via stavZdrojeDph / VIES |
| legal_name | ares_api | obchodniJmeno | |
| status | ares_api | seznamRegistraci.stavZdrojeVr + datumZaniku | + LIKVIDATOR/INSOLVENCE (Justice) |
| legal_form | ares_api | pravniForma (code) | label via codebook / Justice PRAVNI_FORMA |
| incorporation_date | ares_api | datumVzniku | Justice zapisDatum agrees |
| dissolution_date | ares_api | datumZaniku | absent if active |
| registered_address | ares_api | sidlo.textovaAdresa | RUIAN codes available |
| activity_code | ares_api / csu_res | czNace2008 / cz_nace (primary) | CZ-NACE |
| financials | justice_sbirka_listin | účetní závěrka (PDF) | not structured; OCR/parse; CZK |
| officers | justice_vr_bulk | STATUTARNI_ORGAN_CLEN / DOZORCI_RADA_CLEN | DOB → PII (GDPR) |
| owners | justice_vr_bulk | AKCIONAR (a.s.) / SPOLECNIK (s.r.o.) | registered shareholders/members |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- The Czech Republic is a **fully-open** country for company **identity and structure**: one open API (ARES) +
  one open deep bulk (Justice VR), both on IČO. Officers **and** registered shareholders/members are open — a
  richer ownership signal than many EU registers.
- **Financials** are the one gap for a cross-country mapper: public and free but **document-based PDF**
  (Sbírka listin), so map `financials` to that source and expect an OCR/parsing step (or a commercial provider),
  not a structured feed.
- `owners` should map to **registered shareholders/members** (AKCIONAR/SPOLECNIK), distinct from **beneficial
  owners** (the separate, access-controlled Evidence skutečných majitelů — `not_available_in_open_sources`).
- Exact employee headcount is `not_available_in_open_sources` (ČSÚ size band only).
