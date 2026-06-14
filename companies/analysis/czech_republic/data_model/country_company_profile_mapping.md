# Czech Republic Company Profile — Mapping Report

Join everything on **IČO** (normalize zero-padding: ARES padded, Justice unpadded). ARES API is the clean
aggregated spine; the Justice VR bulk adds the deepest structure; ČSÚ RES, the VAT register and RŽP enrich;
financial statements come from the Sbírka listin (PDF).

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.ico | ares_api | ico | self | near real-time | public / open | authoritative | normalize to 8 digits |
| registration.court_file_mark | justice_vr_bulk | Udaj[SPIS_ZN].spisZn | ico | monthly | public / open | authoritative | also Sbírka listin link |
| tax_identifiers.dic | ares_api | dic | ico | near real-time | public / open | authoritative | CZ + IČO |
| tax_identifiers.vat_registered | ares_api | seznamRegistraci.stavZdrojeDph | ico | near real-time | public | authoritative | confirm via VIES |
| tax_identifiers.unreliable_vat_payer | vat_register | nespolehlivy_platce | dic | real-time | public / validation | enrichment | CZ risk flag |
| legal_identity.legal_name | ares_api | obchodniJmeno | ico | near real-time | public / open | authoritative | history in dalsiUdaje |
| legal_identity.legal_form_code | ares_api | pravniForma | ico | near real-time | public | authoritative | codebook |
| legal_identity.legal_form_name | justice_vr_bulk | Udaj[PRAVNI_FORMA].pravniForma.nazev | ico | monthly | public | secondary label | |
| status.value | ares_api | seznamRegistraci.stavZdrojeVr + datumZaniku | ico | near real-time | public | authoritative | + LIKVIDATOR/INSOLVENCE from Justice |
| activity.cz_nace | ares_api | czNace2008 | ico | near real-time | public | authoritative | unordered list |
| activity.primary_cz_nace | csu_res | cz_nace | ico | regular | public / open | enrichment | primary flagged |
| activity.scope_of_business | justice_vr_bulk | Udaj[PREDMET_PODNIKANI] | ico | monthly | public | supplementary | free text |
| incorporation.incorporation_date | ares_api | datumVzniku | ico | near real-time | public | authoritative | (Justice zapisDatum agrees) |
| incorporation.dissolution_date | ares_api | datumZaniku | ico | near real-time | public | authoritative | absent if active |
| registered_location.* | ares_api | sidlo.* | ico | near real-time | public | authoritative | RUIAN codes |
| capital.share_capital_czk | justice_vr_bulk | Udaj[ZAKLADNI_KAPITAL] | ico | monthly | public / open | authoritative | CZK; + % paid |
| officers[] | justice_vr_bulk | Udaj[STATUTARNI_ORGAN_CLEN]/[DOZORCI_RADA_CLEN] | ico | monthly | public / open | authoritative | **DOB → PII (GDPR)** |
| owners[] | justice_vr_bulk | Udaj[AKCIONAR] (a.s.) / [SPOLECNIK] (s.r.o.) | ico | monthly | public / open | authoritative (registered) | may be PII |
| financial_statements[] | justice_sbirka_listin | ucetni_zaverka.* | ico | per filing | public / PDF | document-based | OCR/parse; CZK |
| documents[] | justice_sbirka_listin | vyrocni_zprava / zprava_auditora | ico | per filing | public / PDF | document-based | |
| (full population load) | ares_opendata_bulk | bulk export | ico | periodic | public / open | alt to API | resolve URL via portal |
| (discovery) | nkod_portal | DCAT distribution/license | — | varies | per dataset | metadata | resolve URLs + licences |

## Precedence Rules

1. **ARES API is authoritative** for clean identity, DIČ, legal form, structured RUIAN address, CZ-NACE, dates
   and per-register status — it aggregates and standardizes the underlying registers.
2. **Justice VR bulk is authoritative** for the deep structure ARES does not expose: share capital, officers
   (with DOB), shareholders/members, boards, liquidation and insolvency, court file mark.
3. **ČSÚ RES** supplies the **primary** CZ-NACE + sector/size band; **VAT register** supplies the unreliable-payer
   risk flag; **RŽP** supplies trade-licence detail and OSVČ coverage. All are enrichment.
4. **Financials** come only from the **Sbírka listin** (PDF) — document-based, structured after OCR/parsing.
5. For **full-population** loads, `ares_opendata_bulk` is the alternative to per-IČO API calls; use `nkod_portal`
   to resolve exact resource URLs and licences.

## Missing-Data Notes

- **Financials are not structured open data** — PDF only (Sbírka listin).
- **Exact employee headcount** is not open (ČSÚ size band only).
- **Beneficial ownership** (Evidence skutečných majitelů) is a separate, access-controlled register — not
  included here; registered shareholders/members (AKCIONAR/SPOLECNIK) are the open ownership signal.
- **IČO zero-padding** and **licence exactness** (empty CKAN licence) are the two main normalization/compliance
  to-dos.
