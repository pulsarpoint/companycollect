# ONRC firm status — OD_STARE_FIRMA Field Catalog

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: ONRC via data.gov.ro
- URL: resource od_stare_firma.csv (same dataset as OD_FIRME)
- License: open (Romanian open-data; exact license not stated)
- Access: public
- Freshness: with the dataset snapshot
- Record shape: `^`-delimited CSV (89 MB), one row per company
- Primary keys: `COD_INMATRICULARE`
- Join keys: `COD_INMATRICULARE`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| COD_INMATRICULARE | COD_INMATRICULARE | Registration number | string | identifier | J2002000372404 | join key |
| COD | COD | Status code | integer | status | 1048, 1084, 2069 | decode via nomenclator |

## Interpretation Notes

- Per-company **status** is stored separately from OD_FIRME; join on
  `COD_INMATRICULARE`.
- Observed codes: **1048** = *funcţiune* (active/in operation), **1084** =
  *radiată* (struck off / deregistered), **2069** = *dizolvare* (dissolution).
  Other codes exist; obtain the ONRC status nomenclator to decode fully.
- `dissolution_date` is not provided — a struck-off/dissolved status only signals
  the state, not the date.
- No `sample_record.json`: the file is a trivial two-column code mapping (PII-free
  but uninformative as a single row); the codes are documented above.
