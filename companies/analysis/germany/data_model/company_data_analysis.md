# Company Data Analysis For Germany

## Summary

Germany supports a **moderate company profile but a weak open financial profile**. Unlike Norway (one
official publisher, one key, fully open financials), Germany has **no official open bulk dataset and no
free official API** for either the commercial register or financial statements. The practical profile is
built from **one stale-but-open spine** plus **planning-only financial sources**:

- **Registry spine** — `offeneregister_companies` (OffeneRegister.de open bulk, OpenCorporates schema):
  ~5.1M companies + officers, **CC-BY (license ambiguous vs CC-BY-NC)**, but coverage is **stale
  (2017-2019)**. Gives legal name, register identity (court-scoped), status (text), seat/region,
  officers, and document-availability flags. **No NACE, no tax_id/VAT, no incorporation date.**
- **Financials** — **none are open**. Since DiRUG (2022-08-01) statements are **free to view** at the
  Unternehmensregister (XBRL/HGB), but with **no bulk and no free retrieval API**. Real ingestion needs a
  **commercial API** (`openregister_financials_api`, paid) or the free **per-company** `bundesanzeiger`
  tool (captcha-limited, returns HTML to parse).

The hardest modeling fact: **there is no shared numeric key** between the registry spine and the
financial sources — financials must be matched by register number or name+seat.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| offeneregister_companies | OffeneRegister.de company bulk (JSONL) | recommended (stale) | public, no auth | CC-BY 4.0 (confirm vs NC) | **Registry spine** |
| unternehmensregister_financials | Unternehmensregister/Bundesanzeiger statements | planning_only | free view, no bulk/API | free view; reuse not granted | Financial **field definition** + lawful free-view source |
| openregister_financials_api | OpenRegister.de Bundesanzeiger financial API | blocked_payment | paid API | commercial | **Structured financials at scale** |
| bundesanzeiger_reports | bundesanzeiger (bundesAPI `deutschland`) | sample_only | free, captcha-limited | Apache-2.0 tool; portal terms | Free **per-company** financial retrieval |

Excluded / not given their own catalog (documented in `source_inventory.json`): official
handelsregister.de (no bulk/API, terms restrict scraping), OpenSanctions FTM mirror (same data, CC-BY-NC,
graph reshape), BRIS/e-Justice (single-company lookup), bundesAPI/handelsregister scraper (≤60/hr,
registry not financials), GovData.de (no master file), other commercial registry APIs (alternative
transports for the same registry data).

## What Each Source Contributes

- **offeneregister_companies (spine).** The only open bulk. `company_number` (synthetic PK), legal name +
  previous names, court-scoped register identity (`_registerArt`/`_registerNummer`/`registrar`, plus the
  human `native_company_number`), textual status, free-text address + seat + federal state, **officers**
  (Geschäftsführer/Vorstand/Prokurist — PII), and `additional_data` booleans showing which documents
  exist at handelsregister.de. ~260 MB bz2 / ~4 GB NDJSON. **Stale 2017-2019**; a 2022 SQLite snapshot
  exists for fresher coverage.
- **unternehmensregister_financials (the financial definition).** Establishes *what financial fields
  exist* under §325 HGB — balance sheet (Bilanzsumme, Anlage-/Umlaufvermögen, Eigenkapital,
  Verbindlichkeiten), P&L (Umsatzerlöse, Jahresüberschuss — medium/large only), employees, fiscal period,
  taxonomy (HGB/IFRS/US-GAAP), Einzel-/Konzernabschluss, size class. Free to view, **no bulk/API**, so
  it's a reference + manual-lookup source, not a pipeline input.
- **openregister_financials_api (scale path).** Commercial vendor that pre-parses Bundesanzeiger filings
  into **structured JSON** (total assets, asset/liability breakdown, revenue, profitability, net income,
  equity, cash, employees), daily, multi-year, hundreds of thousands of companies. The realistic way to
  get financials at scale without building an XBRL pipeline. Paid; planning-only here.
- **bundesanzeiger_reports (free, targeted).** `get_reports("<name>")` returns `{report_title: content}`
  including `Jahresabschluss …`, solving the search captcha with an ML model. Free but per-company,
  captcha/rate-limited, and returns **HTML/text not XBRL** — needs a figure extractor. Good for enriching
  a bounded target set, not the population.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`) models a Germany-specific object with sections:
`registration` (court-scoped identity + derived `natural_key`), `legal_identity`, `status` (text +
derived), `registered_location` (no activity code), `officers[]` (PII-flagged), `related_registrations`,
`available_documents` (handelsregister.de availability booleans), `financial_statements[]`
(**planning-only**, multi-source, size-class-aware nullability), and `source_provenance[]`. Repeatable
concepts (previous names, officers, yearly financials) are arrays. Every section carries `x-source`
provenance; financial entries carry a `source` discriminator.

## Join And Precedence Rules

- **Registry internal join**: single key `company_number` (officers/documents/related hang off it).
- **Registry ↔ financials**: **no shared numeric key** — compose `registration.natural_key`
  (`registrar_registerType_registerNumber`) or fall back to normalized `name` + seat. Build this as an
  explicit, auditable matcher; it is the main engineering risk.
- **Financial source precedence**: paid API (`openregister_financials_api`) → free per-company tool
  (`bundesanzeiger_reports`) → official free-view (`unternehmensregister_financials`, reference only).
  Dedupe on `key + period_end + consolidated`; prefer Einzelabschluss for own figures, keep Konzern as
  the group view; always store `currency`.
- **Freshness**: spine is **stale (2017-2019)**; financials are **current** → mismatch is expected. Mark
  spine staleness; consider the 2022 SQLite or a commercial registry API to refresh the spine.

## Missing Or Restricted Data

- **Open financials**: none — paid API or captcha-limited per-company scraping only.
- **tax_id / vat_id**: not in open data (VIES validates, does not list).
- **activity / NACE (WZ) code**: absent from OffeneRegister.
- **incorporation / dissolution date**: no clean fields; only textual status.
- **Beneficial ownership** (Transparenzregister): access-restricted; not modeled.
- **License**: OffeneRegister CC-BY vs CC-BY-NC unresolved — treat as NonCommercial until confirmed.
- **PII**: officers carry personal data (names, city, role dates) — GDPR lawful basis + retention needed.
- **revenue/net_income/employees**: NULL for micro/small filers (no P&L required) — most German GmbHs.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Key cross-country points: Germany has **no single national
company number** (register numbers are court-scoped — keep court + type), **no open `tax_id`/`vat_id`**,
**no open financials** (a cross-country `financials` mapper must tolerate empty/paid-sourced data and
NULL revenue for most companies), **no activity code**, and a **stale** open spine. Currency must be
stored per figure, never hardcoded to EUR.
