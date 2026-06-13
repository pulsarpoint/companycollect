# Unternehmensregister / Bundesanzeiger — Annual Financial Statements — Field Catalog

> **PLANNING-ONLY.** Statements are **free to view** (since DiRUG, 2022-08-01) but free viewing does
> **not** grant reuse/redistribution, and there is **no open bulk and no free retrieval API**. No raw
> records or observed values are copied here; fields are derived from the HGB/XBRL taxonomy structure
> documented in `schema_notes.md` and the investigation, not from extracted filings.

## Source Summary

- Country: Germany
- Source type: official_financial_disclosure (§325 HGB)
- Organization: Bundesanzeiger Verlag GmbH (for the Federal Ministry of Justice)
- URL: https://www.unternehmensregister.de/ (FY≥2022) · https://www.bundesanzeiger.de/ (FY<2022)
- License: free view only; reuse not granted → **planning-only**
- Access: public viewing, no registration; **no bulk, no free API**
- Freshness: authoritative / current
- Record shape: per-company **XBRL / iXBRL (ESEF)** (HGB / IFRS / US-GAAP taxonomy) or rendered HTML/PDF
- Primary keys: company (name + seat) + fiscal_year
- Join keys: company name + registered_office (seat); register number where present

## Fields

| Path | Source field (DE) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_identity | Firma / Sitz | Filer name + seat | string | identifier | Fuzzy match to registry spine |
| fiscal_year | Geschäftsjahr | Financial year | integer | date | One per year → array |
| period_start | Geschäftsjahr von | Period start | date | date | |
| period_end | Geschäftsjahr bis / Stichtag | Balance-sheet date | date | date | Per-statement key |
| accounting_standard | Rechnungslegungsstandard | HGB/IFRS/US-GAAP taxonomy | string | financial | Drives XBRL parsing |
| consolidated | Einzel-/Konzernabschluss | Single vs group | boolean | financial | Avoid double counting |
| size_class | Größenklasse (§§267/267a) | micro/small/medium/large | string | financial | Determines disclosure scope |
| currency | Währung | Reporting currency | string | financial | Usually EUR; store per figure |
| total_assets | Bilanzsumme | Balance sheet total | decimal | financial | ~always present |
| fixed_assets | Anlagevermögen | Non-current assets | decimal | financial | |
| current_assets | Umlaufvermögen | Current assets | decimal | financial | |
| equity | Eigenkapital | Total equity | decimal | financial | |
| liabilities | Verbindlichkeiten (+ Rückstellungen) | Liabilities/provisions | decimal | financial | May be split concepts |
| revenue | Umsatzerlöse | Turnover | decimal | financial | **medium/large only** |
| net_income | Jahresüberschuss/-fehlbetrag | Net profit/loss | decimal | financial | P&L-dependent |
| employees | durchschnittliche Zahl der Beschäftigten | Avg employees | integer | employment | Often in notes; may be absent |
| documents | Bilanz/GuV/Anhang/Lagebericht/Bestätigungsvermerk | Component docs | array | document | GuV/Lagebericht/audit = medium/large |

## Interpretation Notes

- **Size class drives everything.** Under §§267/267a HGB, *micro* and *small* companies disclose a
  (possibly abridged) **balance sheet only** — **no P&L, so no `revenue`/`net_income`**. Expect those
  fields NULL for the large majority of German GmbHs. *Medium/large* add P&L, notes, management report
  and audit opinion. The model must tolerate nulls and not treat "missing revenue" as zero.
- **Format reality.** Submissions are XBRL (HGB taxonomy; IFRS/US-GAAP supported; ESEF iXBRL for listed
  issuers). On the public view, many statements render as **HTML/PDF**, so clean machine-readable XBRL
  is not always retrievable — budget an extraction/parsing layer (Arelle / Brel / tidyxbrl).
- **Matching is the hard problem.** Statements are attributed by **name + seat** (sometimes register
  number). There is no shared numeric key with the OffeneRegister spine — joining requires a matcher on
  `registrar`+register number or normalized name+seat. This is the central modeling risk for Germany.
- **Number formats.** HTML figures use German locale (`1.234.567,89`); XBRL gives clean numerics.
  Negative values may appear in parentheses / with taxonomy-specific sign conventions.
- **Access route is not this source's UI.** This catalog documents *what financial fields exist*; the
  realistic *retrieval* paths are the commercial API (`openregister_financials_api`) for scale or the
  free per-company tool (`bundesanzeiger_reports`) — both modeled as separate sources.
- **No sample record** is provided: copying filings from the portal is not permitted under free-view
  terms, and none were retrieved.
