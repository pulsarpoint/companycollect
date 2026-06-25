# Company Data Analysis For Ghana

## Summary

Ghana's official register is the **ORC** (Office of the Registrar of Companies,
formerly the **Registrar General's Department, RGD**), keyed on the **company
registration number**, with the **TIN** (GRA) as the tax key. Company search and
documents (incorporation, status report, annual returns) are delivered via the
**eServices** portal — **paid per transaction** — and the ORC/RGD/GRA hosts were
**firewalled** from this environment (DNS resolves; TCP/HTTP blocked).

The one genuinely **open** source is the **Ghana Stock Exchange (GSE)** —
listed-company directory + financial statements — **verified live** (Ecobank Ghana
PLC, GCB Bank, AngloGold Ashanti, CalBank, Standard Chartered Ghana…). **data.gov.gh**
was firewalled. So there is **no open bulk corporate register and no open private
financials** — ingestion is `blocked_payment` (ORC) + open-for-listed (GSE). Currency
**GHS**; directors/shareholders are personal data (Act 843). No ORC per-company
values were captured.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| gse_listed | GSE — listed companies + financials | ready | **open** | public disclosure | Listed identity + financials |
| orc_eservices | ORC / RGD — register + documents (eServices) | blocked_payment | login + paid; firewalled here | paid | Corporate identity + financials |

(data.gov.gh is recorded in discovery as unavailable — firewalled.)

## What Each Source Contributes

- **gse_listed** — open listed-company directory (name, ticker, sector) + profiles +
  financial statements (GHS). Verified live (Ecobank Ghana PLC, etc.).
- **orc_eservices** — the canonical corporate record (registration number, type,
  status, incorporation date, address, stated capital, directors, shareholders, TIN,
  annual returns), via eServices, paid. Field model from public knowledge.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **registration_number** with sections:
`tax_identifiers` (tin; VAT under it), `legal_identity`, `status`, `activity` (ORC /
GSE sector), `registered_location`, `capital` (GHS, paid), `owners`/`officers`
(redacted, paid), `listing` (GSE, open), `financial_statements[]` (GSE listed / ORC
paid), and `source_provenance[]`. The example uses the GSE-verified **Ecobank Ghana
PLC** with ORC identifiers null.

## Join And Precedence Rules

- **Registration number** is the corporate key; **TIN** links tax; **GSE ticker** keys
  the listed entity (join to ORC by name).
- **ORC** authoritative for corporate identity + financials (paid); **GSE** for listed
  (open).

## Missing Or Restricted Data

- **No open bulk corporate register; no open private financials** — ORC paid/gated;
  only the GSE (listed) is open.
- **No company dataset on data.gov.gh** (firewalled).
- **No separate VAT number** (VAT tied to the TIN).
- **Directors/shareholders** redacted as personal data (Act 843).

## Common Mapper Notes

`company_id == registration_number`; `tax_id == TIN`; no separate `vat_id`. The blocker
is **eServices-gated, paid ORC** (and the firewall); the open path is the **GSE**
(listed). Currency **GHS**. See `common_field_mapping_suggestions.md`.
