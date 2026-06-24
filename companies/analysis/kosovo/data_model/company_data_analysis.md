# Company Data Analysis For Kosovo

## Summary

Kosovo has a comprehensive official company register — **ARBK** (Kosovo Business
Registration Agency) — but it is **not openly accessible programmatically**. The
ARBK SPA API (`/api/api/Services/*`) returns **HTTP 401** without the app's bearer
token, and the search is behind a **Cloudflare Turnstile CAPTCHA**; the export
endpoint is also gated. The tax administration's per-company lookup (**ATK
VatRegist**) is likewise **CAPTCHA-gated**. So a Kosovo company profile is fully
**designable** (the field model is well documented), but ingestion is
**`blocked_authentication`** — it requires an official ARBK/ATK arrangement, not
scraping. No access controls were bypassed and no per-company values were captured.

The profile is keyed on the **NUI** (Numri Unik Identifikues = Numri Fiskal,
9-digit) = company id = tax id; the VAT number (Numri i TVSH) is separate. The only
open financial datapoint is ARBK registered **capital** (EUR); there is **no public
financial-statements register**.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| arbk_business_register | ARBK — Kosovo Business Registration Agency | blocked_authentication | bearer 401 + Turnstile CAPTCHA | not stated | Primary identity |
| atk_vatregist | ATK — VatRegist / SearchTaxPayer | blocked_authentication | CAPTCHA | not stated | Tax/VAT cross-check |

(ATK Open Data XLSX are aggregate statistics, recorded in discovery as a secondary
non-company-level source, not modelled here.)

## What Each Source Contributes

- **arbk_business_register** — the full company record model: NUI / NRB / fiscal /
  VAT, name, status (Aktiv/Pasiv/Shuar), registration date, legal form, activity,
  address/municipality, registered capital (EUR), owners (+%), foreign-ownership %,
  employees. Documented from the SPA's own JS field model (gated; no live values).
- **atk_vatregist** — a tax-side cross-check keyed on the same fiscal number:
  taxpayer status, VAT number/type, address, responsible tax centre. Confirms the
  FiscalNo ↔ NRB ↔ VatNo mapping. CAPTCHA-gated.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **NUI** with sections:
`registration` (nui/NRB), `tax_identifiers` (fiscal/tax/VAT), `legal_identity`,
`status`, `activity`, `registered_location`, `capital` (EUR), `owners` (redacted),
`employment`, `tax_status` (ATK cross-check), and `source_provenance[]`. The
example record is **structural only** (placeholders), reflecting that no
per-company values were extractable without bypassing controls.

## Join And Precedence Rules

- **NUI** (= Numri Fiskal) is the universal key; ATK's `FiscalNo` equals it.
- **ARBK** authoritative for identity/status/activity/capital/ownership; **ATK**
  cross-checks tax status / VAT. Both **live** but **gated**.

## Missing Or Restricted Data

- **No open bulk / API** — both sources CAPTCHA/bearer gated (`blocked_authentication`).
- **No financial statements** — only registered capital.
- **No working national open-data portal**; ATK Open Data is aggregate.
- **Dissolution date / separate directors** not in the open model.
- **Owners** are personal data — redacted.

## Common Mapper Notes

`company_id == tax_id == NUI`; `vat_id` separate. The defining issue is **access**,
not schema — a future implementation needs an official ARBK/ATK data-sharing
channel. See `common_field_mapping_suggestions.md`.
