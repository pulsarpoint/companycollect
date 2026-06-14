# Sbírka listin — financial statements (účetní závěrka) Field Catalog

> **Document-based.** Free to view at `or.justice.cz`, but the statements are **PDF** (native or scanned).
> No official structured/XBRL bulk. No raw `sample_record.json` (figures live in PDFs; structured extraction
> requires OCR/parsing). Currency CZK.

## Source Summary

- Country: Czech Republic
- Source type: official_financial_disclosure
- Organization: Ministerstvo spravedlnosti ČR (Ministry of Justice)
- URL: https://or.justice.cz/ias/ui/rejstrik → company → Sbírka listin (vypis-sl-firma?subjektId=…)
- License: public register documents (free to view; confirm reuse terms)
- Access: public
- Freshness: per filing (annual obligation)
- Record shape: PDF documents per company per fiscal year
- Primary keys: `ico`, `fiscal_year`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ucetni_zaverka.rozvaha | rozvaha | Balance sheet | object | financial | (PDF) | OCR; CZK |
| ucetni_zaverka.vykaz_zisku_a_ztraty | výkaz zisku a ztráty | Income statement | object | financial | (PDF) | OCR; CZK |
| ucetni_zaverka.priloha | příloha | Notes | string | financial | (PDF) | free text |
| ucetni_zaverka.fiscal_year | účetní období | Fiscal year | string | date | (PDF) | per-year key |
| vyrocni_zprava | výroční zpráva | Annual report | document | document | (PDF) | where required |
| zprava_auditora | zpráva auditora | Auditor report | document | document | (PDF) | if audited |

## Interpretation Notes

- **Financials are public and free but not structured.** Czech companies must file the **účetní závěrka**
  (rozvaha + výkaz zisku a ztráty + příloha) into the **Sbírka listin** of the public register. Anyone can view
  them free at `or.justice.cz` — but they are **PDF documents**, often scanned. There is **no official
  XBRL/CSV** of the figures.
- **Structured financials at scale** therefore need OCR/parsing of the PDFs, or a commercial provider that has
  already parsed them.
- **Join** via the company's `subjektId` (from SPIS_ZN) ↔ IČO.
- Compliance varies: not every company files on time; coverage/recency is uneven.
