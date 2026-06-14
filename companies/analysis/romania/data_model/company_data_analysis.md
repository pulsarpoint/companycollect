# Company Data Analysis For Romania

## Summary

Romania is a **best-in-class fully-open** country for company data — comparable
to Estonia/Latvia, and stronger than most because **both** halves of a rich
profile are official and free:

- **Complete identified register**: ONRC (*Oficiul Național al Registrului
  Comerțului*) publishes the **entire** trade register as open bulk CSV on
  **data.gov.ro** — `OD_FIRME` holds **4,116,356** companies, with five companion
  CSVs for status, authorized CAEN activities, legal representatives (PII), and
  foreign branches. Downloaded in full this run.
- **Free structured financials**: ANAF's `/bilant` web service returns
  per-company per-year **financial statements** as JSON (turnover, revenue,
  expenses, gross/net profit, employees, fixed/current assets, liabilities,
  equity, paid-up capital), verified live for **2014–2024**.

The two join cleanly: **CUI** (fiscal code) links to ANAF/VAT, **COD_INMATRICULARE**
(register number) links the ONRC companion files, and OD_FIRME carries both. The
only genuinely closed concept is **ownership** (shareholders / beneficial
owners), available via the paid ONRC portal or the restricted RBR.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| onrc_od_firme | ONRC register OD_FIRME | ready | public | open | Company master (identity, form, address) |
| onrc_od_stare_firma | ONRC firm status | ready | public | open | Status code per company |
| onrc_od_caen_autorizat | ONRC authorized CAEN | ready | public | open | Authorized activities |
| onrc_od_reprezentanti_legali | ONRC legal representatives | ready | public (PII) | open | Officers (GDPR — redact) |
| onrc_od_sucursale_alte_state_membre | ONRC foreign branches | ready | public | open | EU branches |
| anaf_bilant | ANAF financial statements | ready | public | public info | Structured financials 2014-2024 |
| anaf_ws_tva | ANAF VAT/fiscal-info | insufficient_transport_info | public | public info | VAT/inactive status (endpoint version TBC) |
| onrc_rbr | Beneficial Ownership Register | blocked_authentication | restricted | restricted | Beneficial owners (planning-only) |
| onrc_portal_recom | ONRC portal / RECOM | blocked_payment | paid | paid | Shareholders, share capital, history (planning-only) |

## What Each Source Contributes

- **onrc_od_firme** — the company master: DENUMIRE, **CUI**,
  **COD_INMATRICULARE**, EUID, FORMA_JURIDICA, registration date, full address.
  The bridge between the CUI and COD_INMATRICULARE identifier spaces.
- **onrc_od_stare_firma** — per-company status code (1048 active / 1084 struck off
  / 2069 dissolution); join on COD_INMATRICULARE.
- **onrc_od_caen_autorizat** — all authorized CAEN activity codes per company.
- **onrc_od_reprezentanti_legali** — officers/representatives (administrators,
  liquidators). **OPEN but personal data** — redact names/birth fields.
- **onrc_od_sucursale_alte_state_membre** — branches in other EU member states.
- **anaf_bilant** — the standout: free structured financials per CUI/year. Map by
  indicator code; values plain RON. Needs a browser User-Agent (F5 WAF) and ≤1
  req/s.
- **anaf_ws_tva** — current VAT-registration / fiscal-inactivity enrichment;
  documented but its version path 404'd this run (reconfirm before use).
- **onrc_rbr / onrc_portal_recom** — the only routes to **ownership**
  (beneficial owners / shareholders + detailed share capital); restricted/paid →
  planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.cod_inmatriculare`**
(plus `cui`) and groups fields by real Romanian concepts: registration,
tax_identifiers, legal_identity, status, activity (caen_main + caen_authorized[]),
incorporation, registered_location, officers[] (PII-flagged), foreign_branches[],
financial_statements[] (RON, by indicator code), and a planning-only `ownership`
block. The `example.json` is a **real** record for Dante International SA (eMAG
operator): real ONRC identity/address/status + real ANAF financials for
2019/2021/2023/2024 (net turnover 4.56B → 8.99B RON), with **officers and owners
redacted** per GDPR / access rules.

## Join And Precedence Rules

- **CUI** ↔ ANAF financials/VAT; **COD_INMATRICULARE** ↔ ONRC companion CSVs;
  OD_FIRME bridges. `vat_id = "RO" + CUI`.
- Precedence: ONRC register (identity/status/activity/officers/branches) > ANAF
  bilant (financials, filing-time CAEN) > ANAF ws/tva (current VAT status) >
  ONRC portal / RBR (ownership; planning-only). Prefer the ONRC name over ANAF
  `deni` on conflict.

## Missing Or Restricted Data

- **Shareholders / beneficial owners** — not open (paid portal / restricted RBR).
- **Detailed share capital** — paid portal; free proxy = ANAF `I11`.
- **Dissolution date** — not a field; only implied by status.
- **Status nomenclator** (full code→label) — obtain from ONRC.
- **Officers** — open but GDPR personal data; redact.

## Common Mapper Notes

Romania is a **two-identifier** country (CUI vs COD_INMATRICULARE) and a rare EU
case where **structured financials are openly available via API**. A cross-country
mapper should map `company_id/tax_id`←CUI, derive `vat_id`=RO+CUI, map
`financials`←ANAF bilant by indicator code, populate `officers` from open data
(redacted), and mark `owners` planning-only. See
`common_field_mapping_suggestions.md`.
