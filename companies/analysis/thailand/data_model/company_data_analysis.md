# Company Data Analysis For Thailand

## Summary

Thailand has an **excellent open official company API** — the **DBD OpenAPI**
(`openapi.dbd.go.th/api/v1/juristic_person/{id}`), which returns real JSON with **no
key**, keyed on the **13-digit juristic person ID** that is **both the company
registration number and the Tax ID** (VAT uses the same number — no separate VAT
id). Per company it gives **name TH/EN, legal form, register date, status, TSIC
activity, registered & paid-up capital (THB), and a structured address**. This is
the strongest open company source in the project's SE Asia set so far.

Full **annual financial statements** (balance sheet + income statement, THB) live
in the **DBD DataWarehouse** (login-gated) and, for listed firms, **SET** (public).
The profile is **ready** to build from the open API for identity + capital +
activity; financials are a gated/listed enrichment. Currency **THB**;
directors/shareholders are personal data (PDPA) and are **not** in the open API.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| dbd_openapi_juristic | DBD OpenAPI — Juristic Person | ready | **open, no key** | official open API | Core identity + capital |
| dbd_datawarehouse | DBD DataWarehouse — financials | blocked_authentication | login-gated | not stated | Full financial statements |
| set_listed | SET — listed financials | sample_only | public (browser) | public disclosure | Listed financials |

(data.go.th was WAF-blocked for automation and is recorded in discovery as
unavailable here.)

## What Each Source Contributes

- **dbd_openapi_juristic** — verified open data: juristic ID (= Tax ID), name TH/EN,
  type, register date, status, **TSIC** activity, **registered & paid-up capital
  (THB)**, structured address. Per-company by 13-digit ID. Real records: PTT,
  Bangkok Bank, CP All, Internet Thailand.
- **dbd_datawarehouse** — full annual statements + ratios (THB), login-gated;
  planning-only.
- **set_listed** — listed-company financials/disclosures (THB), public; joins on
  the juristic ID.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **juristic_id** with sections:
`registration`, `tax_identifiers` (tax_id = vat_id = juristic_id), `legal_identity`
(EN/TH), `status`, `activity` (TSIC), `registered_location`, `capital` (THB, open),
`listing` (SET), `financial_statements[]` (gated), and `source_provenance[]`. The
example is the real **PTT PCL** (0107544000108).

## Join And Precedence Rules

- The **13-digit juristic ID** is the single universal key (`company_id ==
  registration_number == tax_id == vat_id`). SET symbol keys the listed entity.
- **DBD OpenAPI** authoritative for identity/capital; **DataWarehouse** for full
  financials; **SET** for listed.

## Missing Or Restricted Data

- **No open bulk enumeration** — per-company by ID (drive by a worklist).
- **Full financial statements** login-gated (DataWarehouse) or listed-only (SET);
  only capital is open.
- **Officers/owners** not in the open API (PDPA).
- **No separate VAT number** (same as Tax ID).

## Common Mapper Notes

Thailand collapses company_id/registration/tax/VAT into **one 13-digit number** —
clean single-key joins. The DBD OpenAPI is a rare fully-open official company API.
Currency **THB**. See `common_field_mapping_suggestions.md`.
