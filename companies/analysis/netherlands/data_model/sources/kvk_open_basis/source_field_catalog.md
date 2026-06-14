# KvK Handelsregister Open Data Set — basis bedrijfsgegevens Field Catalog

## Source Summary

- Country: Netherlands
- Source type: official_registry
- Organization: Kamer van Koophandel (KvK)
- URL: https://www.kvk.nl/producten-bestellen/kvk-handelsregister-open-data-set/ (bulk: kvk-open-dataset-basis-bedrijfsgegevens.zip; HVDS API: opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer/{nr})
- License: CC-BY 4.0
- Access: public (free; bulk + HVDS API with free key)
- Freshness: regular (EU High-Value Dataset)
- Record shape: one row per entity (CSV, `;`-delimited, UTF-8); **anonymised** (no KvK number)
- Primary keys: none (anonymised)
- Join keys: none in bulk (KvK-nummer via the HVDS/paid API)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Datum aanvang | Datum aanvang | Registration date | date | date | 19720516 | YYYYMMDD |
| Actief | Actief | Active | boolean | status | J | J/N |
| Insolventie | Insolventie | Insolvency | string | status | (blank) | distress flag |
| Rechtsvorm | Rechtsvorm | Legal form | string | legal_form | BV | BV/NV/EZ/VOF/… |
| Postcode regio | Postcode regio | 2-digit postcode region | string | geography | 89 | no full address |
| SBI activiteiten | SBI activiteiten | SBI activity codes | string | activity | 64210,68203 | comma list |
| Hoofdactiviteiten | Hoofdactiviteiten | Main SBI | string | activity | 64210 | primary |
| Lidstaat | Lidstaat | Member state | string | geography | NL | |

## Interpretation Notes

- **Open but anonymised.** Verified: **1,891,639 records** under **CC-BY 4.0** (EU High-Value Dataset). The bulk
  carries **no KvK number, name, address or directors** — only registration date, active/insolvency, legal form,
  **2-digit postcode region**, and SBI activity codes. It is therefore **statistical** (no join key); useful for
  legal-form/activity/age distributions.
- **Identified access.** The **HVDS open-data API** returns these same fields **by a supplied KvK number** (free
  with an API key; rate-limited 429 without). Names/addresses/officers require the **paid KvK Handelsregister
  API**.
- **SBI** = Standaard Bedrijfsindeling (NACE-aligned). A real `sample_record.json` (one anonymised row) is
  included.
