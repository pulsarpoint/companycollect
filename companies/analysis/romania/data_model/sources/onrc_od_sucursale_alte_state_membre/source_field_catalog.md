# ONRC foreign branches — OD_SUCURSALE_ALTE_STATE_MEMBRE Field Catalog

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: ONRC via data.gov.ro
- URL: resource od_sucursale_alte_state_membre.csv (same dataset as OD_FIRME)
- License: open (Romanian open-data; exact license not stated)
- Access: public
- Freshness: with the dataset snapshot
- Record shape: `^`-delimited CSV, one row per branch
- Primary keys: `COD_INMATRICULARE` + `DENUMIRE_SUCURSALA`
- Join keys: `COD_INMATRICULARE`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| COD_INMATRICULARE | COD_INMATRICULARE | Parent reg. number | string | identifier | J06/363/2001 | join key |
| TIP_UNITATE | TIP_UNITATE | Unit type | string | legal_form | Sucursală | branch |
| DENUMIRE_SUCURSALA | DENUMIRE_SUCURSALA | Branch name | string | legal_name | …SUCURSALA PIREU | |
| EUID | EUID | Branch EUID | string | identifier | | often blank |
| COD_FISCAL | COD_FISCAL | Branch fiscal code | string | identifier | 13376636 | 0/blank when none |
| TARA | TARA | Branch country | string | geography | Italia, Germania | EU member state |

## Interpretation Notes

- Lists **branches/units of Romanian companies located in other EU member
  states** (e.g. a Romanian SRL with a branch in Greece). Join on
  `COD_INMATRICULARE` and aggregate into a `foreign_branches[]` array.
- Naming is free-text and often mixes the parent name with the branch suffix
  (Sucursală/Filiala).
- No `sample_record.json`: trivial flat row; examples shown above.
