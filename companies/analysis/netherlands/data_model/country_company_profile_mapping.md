# Netherlands Company Profile — Mapping Report

The KvK open data (basis + jaarrekeningen) is CC-BY 4.0 but **anonymised** (no KvK number). Identified data keys
on the **KvK-nummer** (8 digits) via the free HVDS API (by number) or the paid KvK API. RSIN (9) = VAT base.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.kvk_nummer | kvk_handelsregister_api | kvkNummer | self | real-time | paid / HVDS API | authoritative | NOT in open bulk |
| registration.rsin | kvk_handelsregister_api | rsin | kvkNummer | real-time | paid | authoritative | VAT base |
| tax_identifiers.vat_id | vies_vat | NL + rsin + B + 2 | rsin | real-time | public / validation | derived | not in open data |
| legal_identity.legal_name | kvk_handelsregister_api | naam | kvkNummer | real-time | **paid** | authoritative (identified) | stripped from open data |
| legal_identity.legal_form | kvk_open_basis | Rechtsvorm | (anon) | regular | public / CC-BY 4.0 | open | BV/NV/EZ/… |
| status.active / insolvency | kvk_open_basis | Actief / Insolventie | (anon) | regular | public / CC-BY 4.0 | open | |
| activity.sbi_main / sbi_all | kvk_open_basis | Hoofdactiviteiten / SBI activiteiten | (anon) | regular | public / CC-BY 4.0 | open | SBI |
| incorporation.registration_date | kvk_open_basis | Datum aanvang | (anon) | regular | public / CC-BY 4.0 | open | YYYYMMDD |
| registered_location.postcode_region | kvk_open_basis | Postcode regio | (anon) | regular | public / CC-BY 4.0 | open | 2-digit |
| registered_location.registered_address | kvk_handelsregister_api | adressen | kvkNummer | real-time | **paid** | authoritative (identified) | full address |
| officers[] | kvk_handelsregister_api / commercial_aggregators | functionarissen | kvkNummer | real-time | **paid** | planning-only | **PII (GDPR)** |
| beneficial_owners[] | ubo_register | ubo[] | kvkNummer | continuous | **restricted** | planning-only | **PII (GDPR)** |
| financial_statements[] | kvk_open_jaarrekeningen | Assets/Equity/Liabilities/… (XBRL) | (anon in bulk; kvkNummer via HVDS API) | monthly | public / CC-BY 4.0 | open | EUR; anonymised in bulk |
| financial_statements[] (identified) | commercial_aggregators / kvk_handelsregister_api | company.financials[] | kvkNummer | vendor | paid | identified | links figures to named company |

## Precedence Rules

1. **Open KvK datasets (CC-BY 4.0)** are authoritative for legal form, status, activity (SBI), registration
   date, postcode region (basis) and **structured financials** (jaarrekeningen) — but **anonymised** (no join
   key in bulk).
2. **Identity** (KvK-nummer, RSIN, name, full address, officers) is **paid** (KvK Handelsregister API) or
   obtained per-company via the **free HVDS API by KvK number**.
3. **Financial join**: the open jaarrekeningen bulk cannot be linked to a named company; use the **HVDS
   jaarrekeningen API by KvK number** (free) or a **commercial provider** for identified financials.
4. **VAT** = `NL` + RSIN + `B` + 2 (derivable once RSIN known; VIES validates).
5. **UBO** restricted (AML-obliged entities); **officers** distinct from beneficial owners.

## Missing-Data Notes

- **No join key in the open bulk** (anonymised) — name/KvK number/address need the paid/HVDS API.
- **Officers/identity are paid**; **beneficial ownership restricted**.
- **Income-statement detail** limited in the open jaarrekeningen (most BV file micro/small abridged = balance
  sheet only).
- **GDPR**: officers and beneficial owners are personal data. CC-BY governs the open datasets only.
- **Volume**: basis CSV 95 MB; jaarrekeningen ~200 MB/zip (×6) — stream/chunk.
