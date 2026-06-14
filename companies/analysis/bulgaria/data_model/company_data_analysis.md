# Company Data Analysis For Bulgaria

## Summary

Bulgaria is a **partial-open** case — between the fully-open group (Belgium/Poland/Norway/France) and the
paid-register group (Germany/Austria/Italy). The **Commercial Register** (Агенция по вписванията) is the
authoritative spine and is **open-ish**: a free **public search**, **CC-BY daily publications** on
data.egov.bg, and a registered **web service**; a **full anonymous bulk** needs a **data-sharing agreement**.
Uniquely, the register openly carries **managers/board and capital partners** (PII). The catch is
**financials (ГФО)**: they are **public** in the register (by 30 June) but as **filed PDF documents**, **not**
structured open data (no XBRL) — extracting figures needs **OCR/parsing** or a **commercial provider**
(CompanyBook/APIS). Everything joins cleanly on the **ЕИК** (= VAT root).

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| commercial_register | Търговски регистър | blocked_authentication | free search / registered WS / agreement bulk | free; CC-BY publications | **Authoritative spine** |
| data_egov_bg | data.egov.bg publications | insufficient_transport_info | public (WAF/api_key) | **CC-BY** | **Open bulk path** (change stream) |
| gfo_financial_statements | ГФО (annual accounts) | insufficient_transport_info | public per-company | public PDF | **Financials (document-based)** |
| companybook_bg | CompanyBook.BG | blocked_payment | free non-financial / paid financials | per terms | Structured financials (convenience) |
| beneficial_ownership | Регистър на действителните собственици | blocked_authentication | restricted | restricted | Beneficial ownership (planning-only) |

Also in `source_inventory.json`: Регистър БУЛСТАТ (non-traders), commercial aggregators (APIS), NSI (aggregate).

## What Each Source Contributes

- **commercial_register (spine).** EIK, name (Cyrillic), legal form, status, seat/address, capital,
  **managers/board**, **capital partners/sole owner**, registration date, registered acts. Free public
  search; registered web service; full bulk by agreement. **No coded activity** (предмет на дейност free text).
- **data_egov_bg (open bulk path).** The Registry Agency's **CC-BY daily publications** — a change/event
  stream keyed on EIK; **accumulate** to build/maintain the master. The open way to get register data without
  an agreement.
- **gfo_financial_statements (financials).** Annual accounts public in the register (by 30 June) as **PDFs**
  (баланс + ОПР inside): total assets, equity, revenue, net result. **Document-based** → OCR/parse; no XBRL.
- **companybook_bg.** Third-party REST API; non-financial free, **structured financials paid** (parsed ГФО,
  2022+) — the convenient route to structured financials without Cyrillic OCR.
- **beneficial_ownership.** Restricted (legitimate interest); planning-only, sensitive PII. (The register's
  **share owners** are the open partial ownership signal.)

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ schematic `.example.json`) models a Bulgaria-specific object:
`registration` (EIK + derived VAT), `legal_identity` (Cyrillic + Latin name + form), `status`, `activity`
(free-text object), `registered_location`, `capital`, `officers[]` (open/PII), `owners[]` (open share owners/
PII), `beneficial_owners[]` (restricted/planning-only), `acts[]` (CC-BY publications), `financial_statements[]`
(document-based or paid-structured, size-category nullability), and `source_provenance[]`.

## Join And Precedence Rules

- **Single clean key**: the **EIK** (= VAT root) joins the register, the CC-BY publications, the ГФО, and the
  restricted BO register — **no fuzzy matching**.
- **Authority**: Commercial Register authoritative; ГФО for financials (PDF); providers for structured financials.
- **Build order**: accumulate the **CC-BY publications** into an EIK-keyed master (or registered web service
  per EIK) → attach ГФО (parse PDFs, triggered by 'обявяване на ГФО' acts) or a provider → (BO only with lawful access).
- **Normalization**: Cyrillic (+Latin); free-text activity; BGN→EUR (2026); PDF OCR for financials.

## Missing Or Restricted Data

- **Open structured financials**: none — ГФО PDFs (parse) or paid provider; no XBRL.
- **Coded activity (КИД/NACE)**: not in the register (free text) — derive.
- **Full anonymous registry bulk**: needs a data-sharing agreement; open path = CC-BY change stream (accumulate).
- **Beneficial ownership**: restricted (planning-only) — but **directors + share owners are OPEN**.
- **PII**: managers, share owners, beneficial owners — GDPR.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Bulgaria is **clean-key, partial-open**: one EIK (= VAT root) joins
everything; registry is open-ish (CC-BY) and carries officers + share owners openly; **financials are
document-based** (parse/OCR) or paid-structured; activity is free text (no КИД); Cyrillic with Latin
transliteration; currency BGN → EUR (2026).
