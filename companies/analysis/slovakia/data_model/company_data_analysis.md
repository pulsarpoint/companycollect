# Company Data Analysis For Slovakia

## Summary

Slovakia is **best-in-class fully-open** — among the strongest in Europe. Two
official, free, machine-readable registers cover identity and financials, both
with permissive licences, joined on the **IČO** (8-digit):

- **RPO — Register právnických osôb** (Štatistický úrad SR — Statistics Office),
  `https://api.statistics.sk/rpo/v1/`, **CC-BY 4.0**. The single public register
  consolidating the commercial register etc. — identity, legal form, activities,
  **officers (statutory bodies)**, **shareholders (stakeholders)**, **share
  capital (equities/deposits)**, predecessors, and **name/address history**.
- **RÚZ — Register účtovných závierok** (Register of Financial Statements,
  Ministry of Finance), `https://www.registeruz.sk/cruz-public/api/`, **CC0**.
  Accounting-unit master (IČO, DIČ, address, SK NACE, dates) **plus full
  structured financial statements** — balance sheet (Súvaha) and income statement
  (Výkaz ziskov a strát) as positional data tables decoded via templates
  (`sablona`).

Both were verified live this run (ESET, IČO 31333532). The combination yields a
very rich profile: identity + officers + owners + share capital + multi-year
structured financials — all from free official sources. The only notable caveats
are GDPR (officers/owners are personal data) and the template-based decoding of
the RÚZ financial tables (with some large filers exposing only PDF).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| rpo | RPO — Register of Legal Entities | ready | public | CC-BY 4.0 | Identity, officers, owners, share capital, history |
| ruz_accounting_units | RÚZ — accounting units | ready | public | CC0 | IČO↔DIČ master, address, SK NACE, dates, statement links |
| ruz_financial_reports | RÚZ — statements & reports | ready | public | CC0 | Structured financial statements (EUR) |
| finstat | FinStat / aggregators | blocked_payment | paid | commercial | Credit rating only (planning-only) |

(ORSR — the commercial-register web portal — is superseded by the RPO API and is
not modeled separately.)

## What Each Source Contributes

- **rpo** — full commercial-register content via API: identifiers/names/addresses
  (history), legal form, free-text activities, **statutoryBodies** (directors),
  **stakeholders** (shareholders), **equities/deposits** (share capital),
  predecessors, statisticalCodes (main SK NACE). CC-BY 4.0. Personal data present.
- **ruz_accounting_units** — IČO, DIČ, name, address, SK NACE, founding/dissolution
  dates, region/district, consolidated flag, and the lists of statement/report
  ids. CC0. The bridge to financials.
- **ruz_financial_reports** — per statement (year), the structured tables (assets,
  liabilities/equity, income statement) decoded against the template; EUR;
  multi-year. CC0.
- **finstat** — paid aggregator; only a proprietary credit rating adds value →
  planning-only.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.ico`** and groups
fields by real concepts: registration, tax_identifiers, legal_identity (+ name
history), status (derived from dissolution date), activity (SK NACE + main + scope),
incorporation, registered_location, share_capital, officers[] (PII-flagged),
owners[] (PII-flagged), financial_statements[] (decoded line items, EUR), and
predecessors[]. The `example.json` is a **real** record for ESET, spol. s r.o.
(IČO 31333532): real identity, address, legal form, share capital (140,000 EUR),
SK NACE 62090, a merger predecessor (COMDOM Software), with officers/owners
**redacted** per GDPR. (ESET is a large filer whose RÚZ statement is a PDF, so the
structured line-items are noted as empty for that unit; most micro/small entities
expose populated tables — verified separately with template 687 "Úč MUJ".)

## Join And Precedence Rules

- **IČO** joins RPO ↔ RÚZ; `vat_id = "SK" + DIČ` (DIČ from RÚZ). Within RÚZ:
  unit `id` → `idUctovnychZavierok[]` → `idUctovnychVykazov[]`; a report's `idUJ`
  resolves back to the IČO.
- Precedence: RPO (identity/officers/owners/capital) > RÚZ units (IČO↔DIČ,
  address, NACE, dates) > RÚZ reports (financials) > FinStat (rating;
  planning-only). Prefer RPO names (history) over RÚZ `nazovUJ`.

## Missing Or Restricted Data

- **No status enum** — derive active/inactive from `datumZrusenia` / legal facts.
- **Officers / shareholders / deposits** — OPEN (RPO) but **personal data**;
  redact.
- **Financials decoding** — requires the template (`sablona`); cache templates;
  some large filers expose only PDF (empty `obsah`).
- **Credit rating** — only via paid FinStat.

## Common Mapper Notes

Slovakia is a **two-open-source, single-key** country (IČO) where **officers,
owners, and structured multi-year financials are all openly available**. Derive
`vat_id` = SK+DIČ; map `financials` from RÚZ (template-decoded, EUR);
populate `officers`/`owners` from RPO with GDPR redaction; derive `status` from the
dissolution date. See `common_field_mapping_suggestions.md`.
