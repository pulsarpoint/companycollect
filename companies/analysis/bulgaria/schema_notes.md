# Bulgaria — Schema Notes

No per-company open record was downloadable here (data.egov.bg WAF-blocked; register web service needs
registration); fields below are from the documented Commercial Register public search / daily publications
and the ГФО document structure. Join on the ЕИК.

## Identifiers
- **ЕИК (EIK)** — Единен идентификационен код / Unified Identification Code: **9 digits** for companies
  (**13 digits** for branches / secondary entities). The company id. Cyrillic abbreviation: ЕИК.
- **VAT (ДДС номер)** = **`BG` + EIK** (e.g. `BG123456789`).
- **Булстат (BULSTAT)** — same numbering, used for non-traders / other entities (BULSTAT register).
- Names are **Bulgarian (Cyrillic)**; many entities also have a **Latin transliteration**.

## Commercial Register (public search / daily publications) — documented fields
```
eik                  - ЕИК (9 or 13 digits) — primary key
naименование         - наименование / firma (company name; Cyrillic, + Latin transliteration)
pravna_forma         - правна форма (legal form: ЕООД, ООД, АД, ЕАД, ЕТ, СД, КД, ...)
sedalishte_adres     - седалище и адрес на управление (registered seat + address)
status               - статус (вписано/active, заличено/struck-off, в ликвидация/liquidation, ...)
predmet_na_deynost   - предмет на дейност (object of activity; free text — NACE/КИД not always coded)
kapital              - капитал (registered capital, BGN/EUR)
upraviteli           - управители / съвет на директорите (managers / board) [PII]
sobstvenitsi         - съдружници / едноличен собственик (partners / sole owner) [PII]
data_na_vpisvane     - дата на вписване (registration date)
```
- Legal forms: **ЕООД** (single-member LLC), **ООД** (LLC), **АД** (JSC), **ЕАД** (single-member JSC),
  **ЕТ** (sole trader), **СД/КД** (partnerships), **ЮЛНЦ** (non-profit legal entities).
- The open data.egov.bg feed is a **daily publication/change stream** — accumulate to build a master keyed
  on EIK; latest-wins per field.

## ГФО — Annual Financial Statements — DOCUMENT-based (PDF)
Each filed ГФО (PDF/scanned) contains, per отчетен период (fiscal year):
```
баланс (balance sheet):
  обща сума на активите (total assets), дълготрайни активи (fixed assets),
  краткотрайни активи (current assets), собствен капитал (equity), задължения (liabilities)
отчет за приходите и разходите / ОПР (income statement):
  нетни приходи от продажби (net sales revenue), печалба/загуба от дейността (operating result),
  печалба/загуба за периода (net profit/loss)
приложение (notes): среден брой персонал (average employees)
категория предприятие: микро / малко / средно / голямо (size category)
```
- **NOT structured open data** — figures are inside PDFs → require **OCR/parsing** (or a commercial
  provider's structured feed). Size category (micro/small) limits disclosure; currency BGN (EUR from 2026).

## Mapping to internal company model
```
company_id          <- eik
registration_number <- eik
tax_id / vat_id     <- "BG" + eik
legal_name          <- наименование (prefer one script; keep Latin transliteration)
company_type        <- правна форма (ЕООД/ООД/АД/...)
status              <- статус (вписано/заличено/в ликвидация)
incorporation_date  <- дата на вписване
registered_address  <- седалище и адрес
municipality        <- from address (settlement/oblast)
region              <- област (oblast)
activity_code       <- (предмет на дейност free text; КИД/NACE not always coded -> derive)
financials[]        <- ГФО (PDF; parse) | commercial provider, keyed by fiscal year
country             <- "Bulgaria"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — no per-company open record was downloadable here).
