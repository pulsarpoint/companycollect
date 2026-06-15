# Company Data Analysis For Ukraine

## Summary

Ukraine offers a **genuinely open company register** — one of the most open in the
world — plus **open financials** for IFRS reporters/issuers. The **EDR** (Unified
State Register, Ministry of Justice) is published as open bulk on **data.gov.ua**
under **CC-BY 4.0**, refreshed weekly. The legal-entities file (`UO.zip`, 325 MB →
3.1 GB XML, **2,008,750** entities) carries — all openly, keyed on the **EDRPOU**
(8-digit) — name, legal form (OPF), status, founders, **beneficial owners (UBO)**,
officers, authorized capital, and registration/termination history. Financials
come from the **XBRL Financial Reporting System** (IFRS reporters, UA MSFS
taxonomy, integrated to XBRL International) and **NSSMC/SMIDA** (securities
issuers), joined on EDRPOU.

Two important caveats: (1) the **wartime data reduction** means the open export has
**no registered address and no KVED activity code** (removed for security since
2022; the full register is access-restricted); (2) founder/officer/beneficial-owner
records are **personal data** — redact. Ukraine has **no separate VAT number**
(EDRPOU is the tax code; `TAX_PAYER_TYPE` flags registrations).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| edr_uo | EDR legal entities (UO) | ready | public | CC-BY 4.0 | Identity, status, owners, UBO, officers, capital |
| edr_fop | EDR entrepreneurs (FOP) | ready | public | CC-BY 4.0 | Sole traders (separate personal-data stream) |
| xbrl_frs | IFRS financial statements (XBRL) | insufficient_transport_info | public | open | Structured financials (IFRS reporters) |
| nssmc_smida | NSSMC / SMIDA issuer disclosure | insufficient_transport_info | public | open | Issuer financials/disclosures |
| edr_full_restricted | EDR full (address/KVED) | blocked_authentication | restricted | restricted | Address + KVED (planning-only, wartime) |

## What Each Source Contributes

- **edr_uo** — the open register: EDRPOU, NAME/SHORT_NAME, OPF, STAN, FOUNDERS,
  **BENEFICIARIES (UBO)**, SIGNERS (officers), AUTHORIZED_CAPITAL,
  REGISTRATION/TERMINATED_INFO, PREDECESSORS/ASSIGNEES, TAX_PAYER_TYPE.
- **edr_fop** — sole traders (ФОП); personal data; kept separate.
- **xbrl_frs** — balance sheet + income statement facts (UA MSFS XBRL) for IFRS
  reporters, keyed on EDRPOU.
- **nssmc_smida** — securities-issuer financial statements + disclosures.
- **edr_full_restricted** — documents the address/KVED gap; restricted (wartime).

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.edrpou`** and groups
fields by real concepts: registration, legal_identity, status (raw + mapped),
incorporation (registration/termination dates), share_capital (UAH),
tax_registrations[], officers[] (PII-flagged), owners (founders + beneficial
owners, PII-flagged), related_entities, financial_statements[] (XBRL/NSSMC),
plus planning-only registered_location and activity (restricted). The
`example.json` is a **real** record — credit union EDRPOU 26535980 — with real
identity/status/dates/capital/tax registrations, **owners and officers redacted**
per data-protection, and address/KVED/financials null (not in the open set).

## Join And Precedence Rules

- **EDRPOU** joins EDR ↔ financials. Precedence: EDR UO (identity/owners/officers)
  > XBRL FRS (financials) > NSSMC/SMIDA (issuer financials) > EDR full (address/
  KVED; planning-only). No VAT number to derive.

## Missing Or Restricted Data

- **Registered address** and **KVED** — not in the open export (wartime); restricted.
- **Financials** — open but **partial coverage** (IFRS reporters/issuers).
- **Personal data** — founders/officers/beneficial owners — redact.
- Encoding **windows-1251**; `UO.xml` 3.1 GB — stream.

## Common Mapper Notes

Ukraine is a **single-key (EDRPOU)** country with **open beneficial ownership** (a
standout) but **no VAT number** and **no open address/KVED** (wartime). Map
`company_id`/`tax_id`←EDRPOU, owners/officers from EDR (redacted), financials from
XBRL/NSSMC (partial), and mark address/activity/vat `not_available`. See
`common_field_mapping_suggestions.md`.
