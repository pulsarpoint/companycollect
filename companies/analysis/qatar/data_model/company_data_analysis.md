# Company Data Analysis For Qatar

## Summary

Qatar has a **dual-registry** company structure plus a separate exchange, and **none** of
the company sources is openly downloadable. The authoritative **onshore** registry is the
**Ministry of Commerce and Industry (MoCI) Commercial Register**, keyed on the
**CR number** — but it is **lookup-only / auth-gated** (e-service paths 404, Single Window
host unresolved, no open bulk/API). The **Qatar Financial Centre (QFC) Public Register**
covers **financial-centre firms**, keyed on the **QFC Number**; it is **browser-public**
but **ASP.NET postback-driven** (the result grid is empty on a plain GET). The **Qatar
Stock Exchange (QSE)** covers **listed** companies (browser-public Liferay portal; portlet
AJAX; no clean open JSON API). The national open-data portal (**data.gov.qa**) runs a
working Opendatasoft API but carries **statistics only**, not a company register. The model
below is sound, but every per-company source is gated — **no per-company registry values
were captured, and none were fabricated**.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| moci_commercial_registration | MoCI Commercial Registration | blocked_authentication | lookup-only / auth-gated | restricted | Primary onshore identity (CR number), legal form, status, capital, activities, owners |
| qfc_public_register | QFC Public Register | blocked_authentication | browser-public; postback | unknown public register | Financial-centre firms (QFC Number), approved individuals, registration date |
| qse_listed | Qatar Stock Exchange (listed) | insufficient_transport_info | browser-public; AJAX | public disclosure | Listed companies: symbol/ISIN, sector, financials, disclosures |

(`data_gov_qa` is statistical only and is not modeled as a company source.)

## What Each Source Contributes

- **MoCI Commercial Register** — the authoritative onshore registry: CR number,
  trade/establishment name (Arabic/English), legal form (W.L.L./Q.P.S.C./sole
  proprietorship/branch), status, registered capital (QAR), licensed activities, and
  owners/partners/manager (personal data — redact). Lookup-only; no open bulk/API.
- **QFC Public Register** — for the financial centre: firm name, QFC Number, approved
  individuals + senior executive functions (personal data — redact), addresses, date of
  registration, and QFCA licensing status. Browser-public but postback-gated.
- **Qatar Stock Exchange** — for the listed subset: ticker symbol, ISIN (`QA…`), sector,
  financial statements (QAR), and disclosures. Browser-public; AJAX; no identified data
  endpoint.

## Proposed Country Company Profile

A registration-keyed object (CR number for onshore, QFC Number for financial-centre firms)
with sections: `registration`, `legal_identity` (name, legal form), `status` (+ date of
registration), `activity` (MoCI activities + QSE sector), `registered_location`, `capital`
(QAR, gated), `officers` (redacted, gated), `listing` (QSE symbol/ISIN/sector), and
`financial_statements` (QSE, QAR, listed only), each with `source_provenance`. The example
is anchored on **Qatar National Bank (QSE: QNBK)** with registry identifiers `null` and
officers `[REDACTED-PII]`.

## Join And Precedence Rules

- **Two distinct registry keys**: CR number (MoCI, onshore) and QFC Number (QFC, financial
  centre); a company is in one or the other. **QSE symbol/ISIN** keys the listed subset.
- **Joins** across MoCI/QFC/QSE are by **company name** (no shared numeric key).
- **Legal name**: MoCI preferred; QFC firm name or QSE listed name as fallback.
- **Currency** QAR; **language** Arabic primary / English secondary; **dates** Gregorian.

## Missing Or Restricted Data

- **Everything per-company is gated**: MoCI is auth-gated (no open bulk/API), QFC is
  postback-gated, QSE is AJAX with no identified data endpoint. No open values captured.
- **tax_id / vat_id** — establishment/tax card via the General Tax Authority (Dhareeba); no
  open register; no general VAT register as of investigation.
- **Owners / beneficial ownership / approved individuals / managers** — personal data under
  **Law No. 13 of 2016**; redact.
- **Private-company financials** — not public; only **QSE-listed** financials are.

## Common Mapper Notes

`company_id` → CR number (onshore) / QFC Number (financial centre); `legal_form` → MoCI
legal form; `financials` → QSE (listed, QAR). `tax_id`/`vat_id`, `owners`, and private
financials are `not_available_in_open_sources`. All mappings are **planning-only** until a
MoCI data-sharing channel, the QFC register postback, or the QSE AJAX endpoint is
established. Do not bypass authentication, the postback, or the portal.
