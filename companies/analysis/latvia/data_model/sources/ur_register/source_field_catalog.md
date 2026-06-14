# Uzņēmumu reģistrs — Register of Enterprises Field Catalog

## Source Summary

- Country: Latvia
- Source type: official_registry
- Organization: Latvijas Republikas Uzņēmumu reģistrs (UR)
- URL: https://data.gov.lv/dati/lv/dataset/uz (download: register.csv)
- License: CC0-1.0 (public domain)
- Access: public (free)
- Freshness: daily/regular
- Record shape: one row per entity (register.csv, `;`-delimited, UTF-8)
- Primary keys: `regcode`
- Join keys: `regcode`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| regcode | regcode | Registration number (id) | string | identifier | 40103550818 | join key |
| name | name | Full legal name | string | legal_name | SIA Psihologs Tavā kabatā | with form prefix |
| name_in_quotes | name_in_quotes | Bare firm name | string | legal_name | KRASTNIEKI A I | matching |
| type | type | Legal-form code | string | legal_form | SIA / AS / IK | |
| type_text | type_text | Legal-form label | string | legal_form | Sabiedrība ar ierobežotu atbildību | |
| regtype_text | regtype_text | Sub-register | string | metadata | Komercreģistrs | |
| registered | registered | Registration date | date | date | 2012-05-31 | incorporation |
| terminated | terminated | Termination date | date | date | 2014-04-10 | dissolution |
| closed | closed | Closed/liquidation flag | string | status | L | |
| address | address | Registered address | string | address | Valmiera, … | |
| index | index | Postcode | string | geography | 4201 | |
| atvk | atvk | Territory code | string | geography | 0885162 | ATVK |
| sepa | sepa | SEPA id | string | identifier | LV95ZZZ40103550818 | contains regcode |

## Interpretation Notes

- **The open spine.** Verified: **485,134 entities** keyed on the **regcode** (11-digit). Identity: name (full +
  parsed parts), legal form (code `type` + label `type_text`: SIA = Ltd, AS = plc, IK = sole trader), sub-register
  (`regtype_text`, e.g. Komercreģistrs), registration/termination dates, address + postcode + **ATVK** territory
  code, and a **SEPA** id. **CC0-1.0** (public domain — no attribution, commercial OK). Also XLSX/JSON/Parquet/
  SQLite/PostgreSQL dump.
- **Status** is derived: registered unless `terminated` is set / `closed` flagged.
- **No VAT, no NACE** in this CSV: VAT = `LV` + regcode (VIES/VID); activity codes come from other UR/CSP datasets
  if needed.
- A real `sample_record.json` (regcode 40103550818) is included from the downloaded CSV.
