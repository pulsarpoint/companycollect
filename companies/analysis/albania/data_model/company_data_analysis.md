# Company Data Analysis For Albania

## Summary

Albania's commercial register (QKB) is **openly mirrored** by Open Data Albania,
keyed on the **NIPT/NUIS** (letter+8digits+letter), which is the company id, tax id,
AND VAT id. A solid open identity profile can be built (name, legal form, owners,
capital, activity, status, former names); financial statements (bilanci, ALL) are
filed with QKB per-company. The example uses real data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| open_data_albania_qkb | Open Corporates Albania (QKB open data) | recommended | public, no key | open data | Open identity register |
| qkb_registry | QKB (official register) | insufficient_transport_info | free per-company extract | official | Authoritative extract + financials |

## What Each Source Contributes

- **open_data_albania_qkb** — the QKB register as open data: NIPT, name, legal form,
  administrator/owners (personal data), capital, activity, status, former names.
  Verified live (4,459 NIPTs; NEXUS GROUP L67508702G, etc.).
- **qkb_registry** — the official per-company extract (ekstrakt) + financial
  statements (bilanci, ALL). Authoritative; per-company, no open bulk.

## Proposed Country Company Profile

Keyed on `registration.nipt`: registration, tax_identifiers (tax_id = vat_id =
NIPT), legal_identity (name, legal form, former names), status, incorporation,
activity, capital, officers (personal data), financial_statements (QKB, ALL),
source_provenance.

## Join And Precedence Rules

- **Join key**: NIPT/NUIS across both sources.
- **Precedence**: Open Data Albania (open identity) > QKB (official extract +
  financials).
- The NIPT is the company id, tax id, and VAT id.

## Missing Or Restricted Data

- **Clean open bulk financials** — none (QKB per-company).
- **Administrator/owners** — open but personal data (Law 9887 / GDPR), redact.
- No separate VAT number — the NIPT is the VAT id.

## Common Mapper Notes

- Map `company_id`/`registration_number`/`tax_id`/`vat_id` all to the NIPT.
- Map `financials` from QKB (bilanci, ALL, per-company); redact owner/admin data.
