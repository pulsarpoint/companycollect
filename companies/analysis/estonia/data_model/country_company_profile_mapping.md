# Estonia Company Profile — Mapping Report

One authoritative open source (e-Business Register / RIK, CC-BY 4.0), keyed on **registrikood** (8-digit).
Financials join via **report_id** (in the report metadata, which carries the registrikood).

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.registrikood | ariregister_company_data | ariregistri_kood | self | daily | public / CC-BY 4.0 | authoritative | 8-digit |
| registration.register_url | ariregister_company_data | teabesysteemi_link | registrikood | daily | public | authoritative | back-link |
| tax_identifiers.kmkr | ariregister_company_data | kmkr_nr | registrikood | daily | public | authoritative | EE + 9 digits; = tax id |
| tax_identifiers.vat_valid | emta_vat | vat_valid | kmkr_nr | real-time | public / validation | enrichment | VIES |
| legal_identity.legal_name | ariregister_company_data | nimi | registrikood | daily | public | authoritative | |
| legal_identity.legal_form | ariregister_company_data | ettevotja_oiguslik_vorm | registrikood | daily | public | authoritative | OÜ/AS |
| status.code / text | ariregister_company_data | ettevotja_staatus(_tekstina) | registrikood | daily | public | authoritative | R = registered |
| activity.emtak_primary | ariregister_annual_reports | EMTAK_myygitulu.emtak (põhitegevusala) | report_id→registrikood | monthly | public / CC-BY 4.0 | authoritative | EMTAK |
| incorporation.first_registration_date | ariregister_company_data | ettevotja_esmakande_kpv | registrikood | daily | public | authoritative | dd.mm.yyyy |
| registered_location.* | ariregister_company_data | ads_normaliseeritud_taisaadress / asukoha_ehak_* | registrikood | daily | public | authoritative | EHAK |
| officers[] | ariregister_persons_other | kaardile_kantud_isikud[] | registrikood | daily | public / CC-BY 4.0 | authoritative | **PII (GDPR)** |
| shareholders[] | ariregister_shareholders | osanikud[] | registrikood | daily | public / CC-BY 4.0 | authoritative | registered owners; **PII** |
| beneficial_owners[] | ariregister_beneficial_owners | kasusaajad[] | registrikood | daily | public / CC-BY 4.0 | authoritative | open BO; **PII** |
| financial_statements[] | ariregister_annual_reports | aruannete_yldandmed + {year}_aruannete_elemendid | report_id → registrikood | monthly | public / CC-BY 4.0 | authoritative | **structured**, EUR |

## Precedence Rules

1. **Single authoritative source.** Everything comes from the e-Business Register open data (RIK). There is no
   conflicting aggregator to reconcile; the only precedence is between the **daily** company/owner datasets and
   the **monthly** financial datasets.
2. **Financial join is two-step.** Pivot `{year}_aruannete_elemendid` rows by `report_id` into a statement,
   attach `aruannete_yldandmed` metadata (year/audited/consolidated/auditor), then join `registrikood` → company.
3. **Three person/ownership layers, distinct:** officers (`kaardile_kantud_isikud`), registered shareholders
   (`osanikud`), beneficial owners (`kasusaajad`). Never conflate.
4. **EMTA/VIES** only validate/enrich (VAT validity, tax debt); the register is the master.
5. **avaandmed.eesti.ee** is discovery only.

## Missing-Data Notes

- **Dissolution date** is not a basic-data column — derive end-of-life from status (liquidation/bankruptcy).
- **Exact employee count** is not in the open company data (revenue/financials are, via the reports).
- The richer `yldandmed` JSON has deeper fields (capital, contacts) not fully cataloged here — `raw_extension`
  until parsed.
- **GDPR**: officers, shareholders and beneficial owners are personal data — lawful basis + retention; no direct
  marketing. CC-BY governs IP reuse only.
