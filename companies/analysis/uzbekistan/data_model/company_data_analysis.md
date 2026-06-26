# Company Data Analysis For Uzbekistan

## Summary

Uzbekistan keys company data on the **9-digit STIR/INN** (taxpayer id), with the **EGRPO**
statistical code as an alternative id. The authoritative register is the **EGRPO** (Unified
State Register of Enterprises and Organizations), maintained by the **Statistics Agency**
(stat.uz) and published via the open-data portal **data.egov.uz** — name, legal form, status,
registration date, address, and **OKED** activity. However, **from this environment
`data.egov.uz`/`data.gov.uz` and the State Tax Committee `soliq.uz` are firewalled**
(timeout/refused), so those sources are **planning-only** (documented from public knowledge,
nothing captured). **stat.uz** is reachable as the EGRPO custodian/entry point, and the
**Republican Stock Exchange 'Toshkent' (UZSE)** is a reachable browser-public **JS SPA** for
listed companies. A profile can be **modelled** around the STIR/INN; the open register should
be pulled from an **unblocked network** (the firewall is environmental, not a real-world
block). Nothing was bypassed and no identifiers were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| egrpo_register | EGRPO (data.egov.uz / stat.uz) | planning_only (firewalled) | firewalled from environment | unknown | Authoritative register: STIR/INN, name, legal form, status, OKED, address |
| soliq_taxpayer | State Tax Committee (soliq.uz) | planning_only (firewalled) | firewalled from environment | restricted | Tax/VAT status by STIR/INN |
| uzse_listed | Republican Stock Exchange 'Toshkent' | insufficient_transport_info | browser-public SPA | public disclosure | Listed companies (ISIN) |

(`stat_uz` is the EGRPO custodian/entry point — reachable but serves the register via the firewalled portal; not modeled as a separate data source.)

## What Each Source Contributes

- **EGRPO** — the authoritative register: STIR/INN, EGRPO code, name (UZ/RU), legal form
  (MCHJ/AJ/YaTT), status, registration date, registered address, OKED activity (a director/
  head field may be present — uncertain; redact if so). Firewalled here; planning-only.
- **soliq** — tax/VAT status: STIR/INN, taxpayer name, VAT (QQS) registration, taxpayer
  status. Firewalled here; planning-only; covers individuals (personal data).
- **UZSE** — listed-company **ISINs**, tickers, issuer names; browser-public JS SPA, API
  route not located; listed only.

## Proposed Country Company Profile

A STIR/INN-keyed object with sections: `registration` (stir_inn, egrpo_code), `legal_identity`
(name, legal form), `status` (EGRPO registration_status + soliq taxpayer_status + vat_status +
registration_date), `activity` (OKED), `registered_location`, and `listing` (UZSE ISIN), each
with `source_provenance`. The example is the STIR/INN-keyed model with all gated fields null
(firewalled / not captured).

## Join And Precedence Rules

- **Primary key**: STIR/INN, shared by EGRPO and soliq. **Join** EGRPO ↔ soliq on the
  STIR/INN; **UZSE** joins by **name** (no STIR on the page).
- **Precedence**: EGRPO authoritative for registration identity/activity/address (firewalled,
  planning-only); soliq for tax/VAT status; UZSE for listing.
- **Keep two statuses distinct**: EGRPO **registration_status** vs soliq **taxpayer_status**.
- **Language** Uzbek (Latin/Cyrillic) + Russian; **currency** UZS; **activity** OKED classifier.

## Missing Or Restricted Data

- **EGRPO and soliq are firewalled from this environment** → planning-only; nothing captured.
  Re-run from an **unblocked network**.
- **UZSE** populated listings are not cleanly available (SPA).
- **financials**, **owners**, and a confident **officers** field are not available openly
  (UZSE listed-only/SPA; EGRPO director/head uncertain).
- **Director/head** (EGRPO, if present) and **individual taxpayers** (soliq) are personal
  data — redact.

## Common Mapper Notes

`company_id` / `registration_number` / `tax_id` → the **STIR/INN** (EGRPO, firewalled here);
`vat_id` is a **status** via soliq; `activity_code` → OKED. `financials`, `owners`, `officers`
are `not_available_in_open_sources`. EGRPO and soliq are `planning_only` (firewalled); UZSE is
`insufficient_transport_info`. The firewall is environmental — the EGRPO open dataset is the
recommended source from an unblocked network.
