# e-Business Register open data — annual report financial data Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_financial_disclosure
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://avaandmed.ariregister.rik.ee/en/downloading-open-data
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Access: public (free)
- Freshness: monthly
- Record shape: three joined CSV layers (report metadata; per-year line items; revenue breakdowns) joined by `report_id`
- Primary keys: `report_id`
- Join keys: `report_id`, `registrikood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| aruannete_yldandmed.report_id | report_id | Report id | string | identifier | 1703729 | bridge key |
| aruannete_yldandmed.registrikood | registrikood | Filing company id | string | identifier | 10000018 | → company |
| aruannete_yldandmed.aruandeaasta | aruandeaasta | Report year | integer | date | 2024 | per-year files |
| aruannete_yldandmed.period_start/end | period_start/end | Accounting period | date | date | 01.01.2019 / 31.12.2019 | dd.mm.yyyy |
| aruannete_yldandmed.kas konsolideeritud? | kas konsolideeritud? | Consolidated? | boolean | financial | Ei | Jah/Ei |
| aruannete_yldandmed.kas auditeeritud? | kas auditeeritud? | Audited? | boolean | financial | Jah | Jah/Ei |
| aruannete_yldandmed.audiitori otsuse tüüp | audiitori otsuse tüüp | Auditor opinion | string | financial | Märkuseta | |
| aruannete_yldandmed.Audiitorettevõtja (AEV) | Audiitorettevõtja (AEV) | Audit firm | string | identifier | 10384467 | a registrikood |
| {year}_aruannete_elemendid.tabel | tabel | Statement table | string | financial | Lõppbilanss | balance sheet / P&L |
| {year}_aruannete_elemendid.elemendi_nimetus | elemendi_nimetus | XBRL-like element name | string | financial | CurrentAssets, Equity | + elemendi_label (ET) |
| {year}_aruannete_elemendid.vaartus | vaartus | Value (EUR) | decimal | financial | 12000.0 | dot-decimal string |
| EMTAK_myygitulu.emtak / Jaotatud müügitulu | emtak | Revenue by activity | string/decimal | activity/financial | 96099 | revenue split by EMTAK |

## Interpretation Notes

- **Structured open financial data — rare and excellent.** Unlike most countries (PDF statements), Estonia
  publishes the **financial statement line items** themselves as open CSV under CC-BY 4.0. Three layers:
  1. **Report metadata** (`1.aruannete_yldandmed`): one row per report — year, period, consolidated?, audited?,
     auditor opinion, audit firm, plus the **registrikood** linking to the company.
  2. **Line items** (`4.{year}_aruannete_elemendid`): one row per financial fact — `(report_id, tabel,
     elemendi_nimetus, vaartus)`. `tabel` groups facts into balance sheet (`Lõppbilanss`) vs income statement
     (`Kasumiaruanne`); `elemendi_nimetus` is an **XBRL-style English element name** (CurrentAssets, Equity,
     Assets, IssuedCapital, CashAndCashEquivalents, TotalAnnualPeriodProfitLoss, CurrentLiabilities, …);
     `vaartus` is the EUR value.
  3. **Revenue breakdowns** (`2.EMTAK_myygitulu`, `3.myygitulu_geograafiline`): revenue split by activity / by
     geography.
- **Join model.** `aruannete_elemendid.report_id` = `aruannete_yldandmed.report_id`; then
  `aruannete_yldandmed.registrikood` → company. So: pivot the line items per report into a financial statement,
  attach the report metadata, then attach to the company by registrikood.
- **Coverage.** Per-year element files for **2019–2025**. Not every company reports every element; the element
  set depends on statement type and company size category.
- **Currency EUR**; values are dot-decimal strings; dates dd.mm.yyyy. Files are large (300+ MB CSV per year) —
  stream/chunk.
- No `sample_record.json` here (the data is row-per-element across three files, not one nested record); the
  field examples above are taken verbatim from the downloaded 2024 element file and report metadata.
