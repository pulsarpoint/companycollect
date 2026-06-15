# Company Data Analysis For India

## Summary

India supports a **solid open identity profile** with **gated financials**. The
MCA **Company Master Data** is open via the **data.gov.in OGD REST API** under
**GODL-India** (free key), keyed on the **21-char CIN**, giving identity, status,
class/category, **authorized & paid-up capital**, business activity, registrar,
and registered address. **Full financial statements are not open** — they are
**paid** (MCA AOC-4/XBRL) for all companies or **open only for listed** companies
(BSE/NSE). India has **GST, not VAT**; PAN/GSTIN tax ids are not in the open data.

The profile is **open for identity + capital**, and **planning-only** for
financials and officers. The example uses real MCA data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| mca_company_master_data | MCA Company Master Data (data.gov.in / OGD) | recommended | public, free key | GODL-India | Authoritative open identity + capital |
| mca_portal_master_data | MCA21 portal (live master data + documents) | blocked_authentication | free lookup / paid docs (WAF) | restricted | Live data; directors/charges (planning-only) |
| mca_xbrl_financials | MCA annual financials (AOC-4/XBRL) | blocked_payment | paid per-document | restricted | All-company financials (paid) |
| bse_nse_listed_financials | BSE/NSE/SEBI listed disclosures | planning_only | exchange terms | exchange terms | Listed-company financials (open) |

## What Each Source Contributes

- **mca_company_master_data** — the authoritative open layer: CIN, name, status,
  class/category/sub-category, **authorized & paid-up capital**, incorporation
  date, principal business activity (+ 4-digit industrial class in 2021),
  registrar, registered address, and latest filing-year markers. Verified live via
  the OGD API (128 state×year resources, 2015–2021). No actual financials.
- **mca_portal_master_data** — the **live** register and the only source of
  **directors/DIN** (personal data) and **charges**; WAF-gated, documents paid.
- **mca_xbrl_financials** — authoritative all-company financial statements (AOC-4;
  XBRL for larger filers), **pay-per-document**; not openly ingestible.
- **bse_nse_listed_financials** — open financial results + shareholding for
  **listed** companies (CIN starts `L`), under exchange terms; join via ISIN↔CIN.

## Proposed Country Company Profile

A single object keyed on `registration.cin` (plus a derived `cin_decoded`):

- `registration` — CIN + decoded segments (listing/industry/state/year/type/RoC).
- `tax_identifiers` — pan/gstin/vat_id all null (not in open data; no VAT).
- `legal_identity` — name, class, category, sub-category.
- `status` — raw + normalized (active/struck_off/…).
- `incorporation` — date of registration.
- `capital` — authorized & paid-up (INR).
- `activity` — principal business activity + industrial class.
- `registered_location` — address, state, RoC.
- `compliance_markers` — latest annual-return / balance-sheet filing years.
- `financial_statements[]` — planning-only (paid or listed-only).
- `officers[]` — planning-only (DPDP personal data).
- `source_provenance[]`.

## Join And Precedence Rules

- **Join key:** CIN (21-char) everywhere; listed financials via ISIN↔CIN.
- **Precedence:** open master data (newest snapshot) for identity/capital; the
  live portal supersedes for freshness/officers when lawful; MCA AOC-4/XBRL
  (paid) then BSE/NSE (listed) for financials.
- **No VAT/PAN/GSTIN** in the open layer — do not synthesize.

## Missing Or Restricted Data

- **Financial statements** — paid (MCA) or listed-only (BSE/NSE).
- **Directors/officers (DIN)** — MCA portal; personal data (DPDP).
- **PAN/GSTIN** — not open; **no VAT**.
- **Beneficial ownership (SBO)** — filed but not openly published.
- **Live data** — open data is point-in-time (latest 2021).
- **Contact email** in the dataset — personal data; redact.

## Common Mapper Notes

- Map `company_id`/`registration_number` to CIN; mark `tax_id`, `vat_id` as
  `not_available_in_open_sources`.
- Capital is open but is not `financials`; map `financials` only from paid/listed
  sources.
- Redact email + director data (DPDP) in any committed output.
