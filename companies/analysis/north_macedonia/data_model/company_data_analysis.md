# Company Data Analysis For North Macedonia

## Summary

North Macedonia's authoritative source is the **Central Registry (Централен
регистар на РСМ, CRM)**, which runs both the **Trade Registry** and the **Registry
of Annual Accounts**. A rich profile is **designable**, keyed on the **ЕМБС**
(7-digit unique entity registration number = company id), with **ЕДБ** (13-digit)
as the tax number and **ДДВ** as the VAT registration (UJP). But the CRM
**commercially distributes** its data: only a **free basic per-company search** is
open; **bulk extracts, detailed data, and annual financial statements (Биланс на
состојба / Биланс на успех, MKD) are PAID** (`blocked_payment`). There is **no open
bulk register and no open financials**.

The investigation was also constrained by environment: the `.mk` government hosts
(`crm.com.mk`, `data.gov.mk`) **resolved via DNS but were firewalled (TCP/HTTP
timeout)**, and `ujp.gov.mk` returned 502 — so the model is documented from public
sources and **no per-company values were captured** (the sample uses public-knowledge
legal names with null identifiers).

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| crm_trade_registry | CRM — Trade Registry | blocked_payment | free basic search / paid bulk | commercial distribution | Primary identity |
| crm_annual_accounts | CRM — Registry of Annual Accounts | blocked_payment | paid | commercial distribution | Financials (planning-only) |

(UJP tax/VAT and data.gov.mk are recorded in discovery as secondary / unavailable.)

## What Each Source Contributes

- **crm_trade_registry** — the full identity record: ЕМБС, ЕДБ, name, legal form
  (ДОО/ДООЕЛ/АД/ТП), status, address, activity (НКД), managers/founders, capital.
  Free basic search is open; detail/bulk is paid. Documented from public docs.
- **crm_annual_accounts** — annual balance sheet + income statement (MKD) for all
  companies, joined on ЕМБС. Paid; planning-only (no raw values).

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **ЕМБС** with sections:
`registration` (embs), `tax_identifiers` (edb/vat), `legal_identity`, `status`,
`activity`, `registered_location`, `capital` (MKD, paid), `owners` (redacted, paid),
`financial_statements[]` (paid, planning-only, MKD), and `source_provenance[]`. The
example uses a real public-knowledge company (Макпетрол АД Скопје) with CRM
identifiers null.

## Join And Precedence Rules

- **ЕМБС** is the universal key; **ЕДБ** links to UJP/VAT.
- **CRM** authoritative for identity and financials; both **paid** for detail/bulk.

## Missing Or Restricted Data

- **No open bulk register; no open financials** — CRM commercial distribution
  (`blocked_payment`); only a free basic search is open.
- **Environment block** prevented live access (hosts firewalled).
- **Incorporation/dissolution dates, capital, owners** are paid/detailed fields.
- **Managers/founders** redacted as personal data.

## Common Mapper Notes

`company_id == registration_number == ЕМБС`; `tax_id == ЕДБ`; `vat_id` (ДДВ)
separate. Financial statements exist for all companies (a strength) but are **paid**.
Currency **MKD**. See `common_field_mapping_suggestions.md`.
