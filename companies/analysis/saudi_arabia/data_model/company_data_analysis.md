# Company Data Analysis For Saudi Arabia

## Summary

A usable Saudi company profile is **keyed on the CR number** (10-digit, region-prefixed
Commercial Registration), with the **Unified National Number (`700…`)** as the
cross-agency company id and the **VAT number (15-digit, ZATCA)** as the tax/VAT key. The
authoritative source is the **Ministry of Commerce Commercial Register (السجل التجاري)**,
which is **Nafath login-gated** — and its inquiry/verification hosts were
**firewalled/NXDOMAIN** from the investigation environment, with no open bulk file or API.
For **listed** companies, the **Saudi Exchange (Tadawul)** publishes profiles, sector,
disclosures, and **financial statements (SAR)**; the issuer directory is **public via the
browser** but returned **HTTP 403 "Access Denied" (WAF)** for automation. The national
open-data portal `open.data.gov.sa` was **firewalled**. Net result: the model is solid,
but **both per-company sources are gated** — no open per-company values were captured, and
none were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| moc_commercial_register | Ministry of Commerce — Commercial Register | blocked_authentication | Nafath login-gated; hosts firewalled | restricted_login | Primary identity/registration/tax/capital/officers |
| tadawul_listed | Saudi Exchange (Tadawul) | blocked_authentication | public via browser; WAF-gated | public disclosure | Listed financials, sector, ISIN/symbol |

(`open_data_gov_sa` was firewalled/unavailable and is not modeled as a data source.)

## What Each Source Contributes

- **MoC Commercial Register** — the authoritative registry: CR number, Unified Number
  (`700…`), VAT number, legal name, entity type (JSC/LLC/SJSC/Sole Proprietorship/Branch),
  status, issue/expiry dates (Hijri), capital (SAR), licensed activities (ISIC), head
  office, and managers/owners (personal data — redact). Nafath login-gated; inquiry hosts
  firewalled; no open bulk/API.
- **Saudi Exchange (Tadawul)** — for the listed subset: 4-digit symbol, ISIN (`SA…`),
  TASI sector, disclosures, and financial statements in SAR. Public via the browser;
  WAF-gated for automation.

## Proposed Country Company Profile

A CR-number-keyed object with sections: `registration` (cr_number, unified_number_700),
`tax_identifiers` (vat_number), `legal_identity` (legal_name, company_type), `status`,
`activity` (ISIC activities + Tadawul sector), `registered_location`, `capital` (SAR,
gated), `officers` (redacted, gated), `listing` (Tadawul symbol/ISIN/sector), and
`financial_statements` (Tadawul, SAR, listed only), each with `source_provenance`. The
example is anchored on **Saudi Aramco (Tadawul 2222)** with registry identifiers `null`
and officers `[REDACTED-PII]`.

## Join And Precedence Rules

- **Primary key**: CR number; **Unified Number** and **VAT number** are alternative joins.
- **Listed entities**: join MoC ↔ Tadawul by **company name / Unified Number** (no shared
  numeric key); Tadawul provides symbol/ISIN/sector/financials.
- **Legal name**: CR preferred; Tadawul name for listed companies when CR is inaccessible.
- **Currency** SAR; **dates** primarily **Hijri** → convert to Gregorian.

## Missing Or Restricted Data

- **Everything per-company is gated**: CR is Nafath login-gated with firewalled inquiry
  hosts; Tadawul is WAF-gated for automation. No open values captured.
- **Owners / beneficial ownership / managers** — personal data under the **PDPL (Royal
  Decree M/19 of 1443H)**; redact.
- **Private-company financials** — not public; only **Tadawul-listed** financials are.
- `open.data.gov.sa` — firewalled/unavailable.

## Common Mapper Notes

`company_id` → CR number; `tax_id`/`vat_id` → ZATCA VAT number; `legal_form` → entity
type; `financials` → Tadawul (listed, SAR). `owners` and private financials are
`not_available_in_open_sources`. All mappings are **planning-only** until a Nafath-cleared
CR access path or an official Tadawul/MoC data channel is established. Do not bypass the
Nafath login or the Tadawul WAF.
