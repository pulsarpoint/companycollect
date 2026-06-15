# Ukraine — Schema Notes

## Identifiers

- **EDRPOU** (ЄДРПОУ) — 8-digit code for legal entities; company id + universal
  join key (EDR ↔ financials).
- **РНОКПП / ІПН** — individual tax number (FOP / persons).
- **VAT**: Ukraine has **no separate VAT number** — VAT payers are identified by
  EDRPOU; EDR `EXCHANGE_DATA` lists tax-register memberships (`TAX_PAYER_TYPE`).
- **KVED** (activity classifier ≈ NACE) — **NOT in the current open EDR export**.

## EDR UO — `<SUBJECT>` record (UO.xml, windows-1251)

| Path | Meaning |
|---|---|
| RECORD | Internal record id |
| NAME / SHORT_NAME | Full / short name |
| OPF | Organizational-legal form (ТОВ = LLC, ПП = private enterprise, …) |
| EDRPOU | 8-digit company code (join key) |
| STAN | Status (зареєстровано=registered, в стані припинення=in termination, припинено=terminated) |
| FOUNDERS/FOUNDER | Founders (name + share "розмір частки - … грн.") — PII |
| BENEFICIARIES/BENEFICIARY | Beneficial owners (UBO) — PII |
| SUPERIOR_MANAGEMENT | Governing body (ЗАГАЛЬНІ ЗБОРИ, ЗАСНОВНИК, ДИРЕКТОР, …) |
| SIGNERS/SIGNER | Officers/signatories (name + role, e.g. "… - керівник") — PII |
| AUTHORIZED_CAPITAL | Authorized/share capital (UAH; decimal comma) |
| REGISTRATION | "DD.MM.YYYY; DD.MM.YYYY; <number>" (registration dates + record no.) |
| BRANCHES | Branches |
| TERMINATION_STARTED_INFO / BANKRUPTCY_READJUSTMENT_INFO | Termination / bankruptcy detail (OP_DATE, REASON, SBJ_STATE, SIGNER_NAME) |
| PREDECESSORS / ASSIGNEES | Predecessor / successor entities (NAME + CODE) |
| TERMINATED_INFO | "DD.MM.YYYY; <number>; <reason>" |
| EXCHANGE_DATA/EXCHANGE_ANSWER | Tax registrations: TAX_PAYER_TYPE (Реєстр платників податків / єдиного внеску), START_DATE, START_NUM, END_DATE, END_NUM |

**Not present (wartime reduction):** registered **address**, **KVED** activity.

## Financials (XBRL FRS / NSSMC-SMIDA)

- IFRS reporters file **XBRL** (UA MSFS taxonomy) via the Financial Reporting
  Collection Centre — balance sheet + income statement facts, keyed on EDRPOU,
  open via XBRL International (filings.xbrl.org).
- NSSMC/SMIDA: securities-issuer financial statements + disclosures.

## Mapping to internal model

| Internal | Ukraine source |
|---|---|
| company_id | EDR EDRPOU |
| registration_number | EDR EDRPOU |
| tax_id | EDR EDRPOU (legal entity tax code) |
| vat_id | not_available (no separate VAT number; see TAX_PAYER_TYPE) |
| legal_name | EDR NAME |
| company_type / legal_form | EDR OPF |
| status | EDR STAN (map registered/in-termination/terminated) |
| incorporation_date | EDR REGISTRATION (first date) |
| dissolution_date | EDR TERMINATED_INFO (date) |
| registered_address | not_available in open export (wartime) |
| activity_code | not_available in open export (no KVED) |
| financials | XBRL FRS / NSSMC-SMIDA (IFRS reporters/issuers), join on EDRPOU |
| officers | EDR SIGNERS (PII; redact) |
| owners | EDR FOUNDERS + BENEFICIARIES (PII; redact) |
| share_capital | EDR AUTHORIZED_CAPITAL (UAH) |

## Gotchas

- **windows-1251** encoding; decimal comma in capital.
- `UO.xml` is **3.1 GB** — stream `<SUBJECT>` records; do not load fully.
- **No address / KVED** in the open export (wartime). **Person data** in
  founders/officers/beneficiaries — redact.
