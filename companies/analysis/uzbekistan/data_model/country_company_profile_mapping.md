# Uzbekistan Company Profile — Mapping

Uzbekistan keys everything on the **9-digit STIR/INN** (taxpayer id), with the **EGRPO**
statistical code as an alternative id. The authoritative **EGRPO** register (Statistics
Agency / data.egov.uz) and the **State Tax Committee** (soliq.uz) were **firewalled from the
investigation environment** (planning-only). The **UZSE** stock exchange lists companies by
**ISIN** (browser-public JS SPA). No per-company values were captured (firewalled); the
firewall is environmental, not a real-world block.

## Identifiers

- **STIR/INN** — 9-digit; registration number, tax id, and universal join key (EGRPO ↔ soliq).
- **EGRPO code** — statistical register code (alt id).
- **ISIN** — UZSE listed securities (listed only).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.stir_inn | egrpo_register | stir_inn | yes | EGRPO | firewalled (planning-only) |
| registration.egrpo_code | egrpo_register | egrpo_code | yes | EGRPO | alt id |
| legal_identity.legal_name | egrpo_register | legal_name | no | EGRPO > soliq | UZ/RU; soliq taxpayer_name alt |
| legal_identity.legal_form | egrpo_register | legal_form | no | EGRPO | MCHJ/AJ/YaTT |
| status.registration_status | egrpo_register | status | no | EGRPO | active/liquidated |
| status.taxpayer_status | soliq_taxpayer | taxpayer_status | no | soliq | TAX status, not registration |
| status.vat_status | soliq_taxpayer | vat_status | no | soliq | VAT (QQS) status |
| status.registration_date | egrpo_register | registration_date | no | EGRPO | Gregorian |
| activity.oked_activity | egrpo_register | oked_activity | no | EGRPO | OKED classifier |
| registered_location.registered_address | egrpo_register | registered_address | no | EGRPO | |
| listing.ticker / listing.isin | uzse_listed | ticker / isin | no | UZSE | listed only; SPA |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Core identity / registration / activity / address**: from **EGRPO** (authoritative;
  firewalled here, planning-only). **Tax/VAT status**: from **soliq** (firewalled here,
  planning-only). **Listing**: from **UZSE** (by ISIN).
- **Join**: EGRPO ↔ soliq on the **STIR/INN** (both use it); **UZSE** joins by **name** (no
  STIR on the page). The **STIR/INN** is canonical.
- **Keep two statuses distinct**: EGRPO **registration_status** vs soliq **taxpayer_status**
  (tax).
- **Language** Uzbek (Latin/Cyrillic) + Russian; **currency** UZS; **activity** OKED classifier.

## Missing / restricted

- **EGRPO and soliq are firewalled from this environment** → all their fields are
  **planning-only**; nothing captured. Re-run from an **unblocked network** (the firewall is
  environmental, not a real-world block).
- **UZSE** populated listings are not cleanly available (SPA; API route not located).
- **Director/head** (EGRPO, if present) and **individual taxpayers** (soliq) are personal
  data — redact.
- The **STIR/INN** and **EGRPO code** are public company identifiers.
