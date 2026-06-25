# Company Data Analysis For Egypt

## Summary

Egypt has **no open company register and no open programmatic financials**. The
official company authority (**GAFI**) runs **login-gated** investor eServices (company
establishment), the **Commercial Registry** (السجل التجاري, GOEIC / Ministry of
Supply) is **not openly searchable** online, and the **EGX (Egyptian Exchange)** —
the source of listed-company profiles and financial statements — is **public via the
browser but WAF-gated** for automation. The national open-data portal
(`data.gov.eg` / `egypt.gov.eg`) was **unreachable**.

A company profile is **designable** keyed on the **Commercial Registry number**
(رقم السجل التجاري), with the **Tax ID** (الرقم الضريبي, 9-digit) as the tax key, but
ingestion is **gated** end-to-end. The only browser-public source is **EGX** (listed
companies, WAF-gated). Currency **EGP**; board/shareholders are personal data (PDP Law
151/2020). No registry per-company values were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| egx_listed | EGX — listed companies + financials | blocked_authentication | browser; WAF-gated | public disclosure | Listed identity + financials |
| gafi_registry | GAFI — company establishment / eServices | blocked_authentication | login-gated | restricted | Corporate identity |
| commercial_registry | Commercial Registry (السجل التجاري) | blocked_authentication | not openly searchable | restricted | Commercial registration |

(data.gov.eg / egypt.gov.eg is recorded in discovery as unavailable.)

## What Each Source Contributes

- **egx_listed** — listed-company profiles, disclosures, and financial statements
  (EGP), keyed on the EGX symbol / ISIN. Browser-public; WAF-gated for automation.
- **gafi_registry** — the corporate record (Commercial Registry number, Tax ID, type,
  status, capital, activity, board, shareholders) via login-gated eServices. Field
  model from public knowledge.
- **commercial_registry** — the underlying commercial registration (number, trade
  name, activity, capital, owner); not openly searchable. Same key as GAFI.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **commercial_registry_number** with
sections: `tax_identifiers` (tax_id; VAT under it), `legal_identity`, `status`,
`activity` (registry / EGX sector), `registered_location`, `capital` (EGP, gated),
`owners`/`officers` (redacted, gated), `listing` (EGX), `financial_statements[]`
(EGX listed), and `source_provenance[]`. The example uses the public-knowledge
EGX-listed **Commercial International Bank (Egypt) S.A.E. (COMI)** with registry
identifiers null.

## Join And Precedence Rules

- **Commercial Registry number** is the corporate key (shared by GAFI + Commercial
  Registry); **Tax ID** links tax; **EGX symbol** keys the listed entity.
- **GAFI / Commercial Registry** authoritative for corporate identity (gated); **EGX**
  for listed (browser-public, WAF-gated).

## Missing Or Restricted Data

- **No open company register; no open programmatic financials** — every source gated.
- **No company dataset on data.gov.eg** (unreachable).
- **No separate VAT number** (VAT under the Tax ID).
- **Incorporation/dissolution dates** only in the gated registry.
- **Board/shareholders** redacted as personal data (PDP Law 151/2020).

## Common Mapper Notes

`company_id == registration_number == Commercial Registry number`; `tax_id == الرقم
الضريبي`; no separate `vat_id`. The blocker is **end-to-end gating** (GAFI login,
Commercial Registry closed, EGX WAF). Currency **EGP**. See
`common_field_mapping_suggestions.md`.
