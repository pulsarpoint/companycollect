# ONRC authorized CAEN — OD_CAEN_AUTORIZAT Field Catalog

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: ONRC via data.gov.ro
- URL: resource od_caen_autorizat.csv (same dataset as OD_FIRME)
- License: open (Romanian open-data; exact license not stated)
- Access: public
- Freshness: with the dataset snapshot
- Record shape: `^`-delimited CSV, **multiple rows per company**
- Primary keys: `COD_INMATRICULARE` + `COD_CAEN_AUTORIZAT`
- Join keys: `COD_INMATRICULARE`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| COD_INMATRICULARE | COD_INMATRICULARE | Registration number | string | identifier | C01/1/2009 | join key; repeats |
| COD_CAEN_AUTORIZAT | COD_CAEN_AUTORIZAT | Authorized CAEN code | string | activity | 0142, 4754 | 4-digit; many per company |
| VER_CAEN_AUTORIZAT | VER_CAEN_AUTORIZAT | CAEN revision | integer | metadata | 2, 3 | Rev.2/Rev.3 |

## Interpretation Notes

- Lists **all authorized activities** (CAEN codes) per company — many rows each.
  Aggregate into an `activities[]` array keyed on `COD_INMATRICULARE`.
- These are the **authorized** activities, not necessarily the **main** one. For
  the main activity, prefer ANAF `caen` (from the bilant service) or the
  company's declared principal CAEN.
- `VER_CAEN_AUTORIZAT` distinguishes CAEN revisions (Rev.2 vs the newer Rev.3);
  reconcile codes across revisions when comparing companies of different ages.
- No `sample_record.json`: trivial three-column mapping; example rows shown above.
