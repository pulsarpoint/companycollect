# Hungary Company Profile — Mapping Report

Join on **adószám** (8-digit base) across sources and the **cégjegyzékszám** on the register side. Free basic
identity + free-to-view financials, but full register data is paid and e-beszámoló automation is reCAPTCHA-gated.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.cegjegyzekszam | ecegjegyzek | cegjegyzekszam | self | real-time | free basic | authoritative | NN-NN-NNNNNN |
| tax_identifiers.adoszam | ecegjegyzek | adoszam | self | real-time | free basic | authoritative | 8-digit base = stem |
| tax_identifiers.vat_id | vies_vat | vatNumber | adoszam base | real-time | public / validation | authoritative | HU + base |
| tax_identifiers.vat_valid | vies_vat | valid | adoszam base | real-time | public / validation | enrichment | point-in-time |
| tax_identifiers.vat_status | nav_afaalany | afa_status | adoszam | daily | public | authoritative (tax) | VAT-subject status |
| tax_identifiers.tax_number_cancelled | nav_afaalany | tax_number_cancelled | adoszam | daily | public | risk flag | distress signal |
| tax_identifiers.statistical_code | ksh_register | statisztikai_szamjel | adoszam base | periodic | open | authoritative | 17-digit |
| legal_identity.legal_name | ecegjegyzek | name | cegjegyzekszam | real-time | free basic | authoritative | |
| legal_identity.legal_form | ecegjegyzek | legal_form | cegjegyzekszam | real-time | free basic | authoritative | Kft/Zrt/Nyrt |
| status.value | ecegjegyzek | status | cegjegyzekszam | real-time | free basic | authoritative | bejegyezve/törölve |
| activity.teaor | ecegjegyzek / ksh_register | main_activity / TEÁOR | cegjegyzekszam | real-time | free / open | authoritative | KSH canonical |
| registered_location.* | ecegjegyzek | registered_seat | cegjegyzekszam | real-time | free basic | authoritative | parse település/megye |
| officers[] | ecegjegyzek / commercial_aggregators | képviselők | cegjegyzekszam/adoszam | real-time | **paid** | planning-only | **PII (GDPR)** |
| owners[] | commercial_aggregators / ecegjegyzek | tulajdonosok | adoszam | vendor | **paid** | planning-only | **PII** |
| financial_statements[] | ebeszamolo | key figures + PDF/XML | cegjegyzekszam/adoszam | annual | public / **reCAPTCHA-gated** | planning-only (automation) | HUF/EUR |
| financial_statements[] (alt) | commercial_aggregators | company.financials[] | adoszam | vendor | **paid** | planning-only | scalable structured |
| public_sector_links[] | procurement_ekr | supplier adószám | adoszam | continuous | open | cross-reference | adószám↔name |

## Precedence Rules

1. **Register identity** (cégjegyzékszám, name, legal form, status, seat, TEÁOR) is authoritative from
   **e-cégjegyzék free basic info**; **officers/owners/history** are **paid** (full extract or vendor).
2. **adószám (8-digit base) is the cross-source key**; EU VAT = HU + base (VIES validates); NAV adds VAT status
   + the **cancelled-tax-number** risk flag (daily).
3. **KSH** owns the canonical **TEÁOR** + the statistical code.
4. **Financials**: free to view on **e-beszámoló** (structured key figures + PDF/XML) but **reCAPTCHA-gated** for
   automation → planning-only; a **commercial provider** is the scalable structured route.
5. **Procurement (EKR)** is an open adószám↔name cross-reference, not a company master.

## Missing-Data Notes

- **No open bulk** company/financials export; **e-beszámoló automation blocked** (reCAPTCHA).
- **Officers/owners are paid** (full cégjegyzék / vendor).
- **Beneficial ownership** (Hungary's UBO register, tényleges tulajdonosi nyilvántartás / BO register) is
  access-restricted — `not_available_in_open_sources` for open use.
- **GDPR**: officers/owners are personal data.
- **License**: register/financial reuse terms unclear — confirm before redistribution.
