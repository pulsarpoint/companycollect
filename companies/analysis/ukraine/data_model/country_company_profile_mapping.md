# Ukraine Company Profile — Mapping Report

Ukraine has a **genuinely open register** (EDR, CC-BY 4.0, weekly) including
beneficial owners and officers, plus **open financials** for IFRS reporters/issuers
(XBRL FRS / NSSMC-SMIDA). Everything keys on the **EDRPOU** (8-digit). Wartime
caveat: the open export has **no address and no KVED**. Founder/officer/UBO names
are personal data.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.edrpou | edr_uo | SUBJECT/EDRPOU | EDRPOU | EDR | company id + join key |
| legal_identity.legal_name | edr_uo | SUBJECT/NAME | EDRPOU | EDR | |
| legal_identity.short_name | edr_uo | SUBJECT/SHORT_NAME | EDRPOU | EDR | |
| legal_identity.legal_form | edr_uo | SUBJECT/OPF | EDRPOU | EDR | ТОВ=LLC |
| status.status_raw/status | edr_uo | SUBJECT/STAN | EDRPOU | EDR | registered/in-termination/terminated |
| incorporation.registration_date | edr_uo | SUBJECT/REGISTRATION[0] | EDRPOU | EDR | split ';' |
| incorporation.termination_date | edr_uo | SUBJECT/TERMINATED_INFO[0] | EDRPOU | EDR | |
| share_capital.authorized_capital | edr_uo | SUBJECT/AUTHORIZED_CAPITAL | EDRPOU | EDR | UAH |
| tax_registrations[] | edr_uo | …/TAX_PAYER_TYPE | EDRPOU | EDR | no separate VAT no. |
| officers[] | edr_uo | SUBJECT/SIGNERS/SIGNER | EDRPOU | EDR | OPEN but PII — redact |
| owners.founders[] | edr_uo | SUBJECT/FOUNDERS/FOUNDER | EDRPOU | EDR | OPEN but PII — redact |
| owners.beneficial_owners[] | edr_uo | SUBJECT/BENEFICIARIES/BENEFICIARY | EDRPOU | EDR | OPEN UBO; PII — redact |
| related_entities.* | edr_uo | PREDECESSORS/ASSIGNEES | EDRPOU | EDR | reorg links |
| financial_statements[] | xbrl_frs / nssmc_smida | assets/equity/revenue/profit | EDRPOU | XBRL > NSSMC | IFRS reporters/issuers; UAH |
| registered_location.registered_address | edr_full_restricted | ADDRESS | EDRPOU | PLANNING-ONLY | not in open export (wartime) |
| activity.kved_codes[] | edr_full_restricted | KVED | EDRPOU | PLANNING-ONLY | not in open export (wartime) |

## Source Precedence

1. **EDR UO** — authoritative for identity, status, dates, capital, officers,
   founders, **beneficial owners**, tax registrations. CC-BY 4.0.
2. **XBRL FRS** — authoritative for **financials** (IFRS reporters). Open.
3. **NSSMC/SMIDA** — issuer financials/disclosures. Open.
4. **EDR full (restricted)** — address + KVED → planning-only (wartime).
5. **EDR FOP** — sole traders, separate personal-data stream (not merged).

## Join Keys

- **EDRPOU** (8-digit) joins EDR ↔ all financial sources. There is **no separate
  VAT number** — `TAX_PAYER_TYPE` flags tax/social-contribution registration.

## Missing / Restricted

- **Registered address** and **KVED activity code** — not in the open export
  (wartime reduction); restricted full register.
- **Financials** — open only for **IFRS reporters/issuers**, not every SME.
- **Personal data** — founders/officers/beneficial owners (redact).
- Encoding **windows-1251**; `UO.xml` is 3.1 GB (stream).
