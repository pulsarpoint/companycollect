# ONRC company register — OD_FIRME (data.gov.ro) Field Catalog

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: Oficiul National al Registrului Comertului (ONRC) via data.gov.ro
- URL: https://data.gov.ro/dataset/firme-08-12-2025 (resource od_firme.csv)
- License: open (Romanian open-data; exact license text not stated on page) — attribute ONRC/data.gov.ro
- Access: public
- Freshness: regular dated snapshots (monthly+); last_modified 2025-12-09
- Record shape: `^`-delimited CSV, UTF-8 (BOM), one row per company
- Primary keys: `COD_INMATRICULARE`
- Join keys: `COD_INMATRICULARE`, `CUI`, `EUID`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| DENUMIRE | DENUMIRE | Legal name | string | legal_name | DANTE INTERNATIONAL SA | current only |
| CUI | CUI | Fiscal/unique code (company id) | integer | identifier | 14399840 | 0 = none; VAT=RO+CUI |
| COD_INMATRICULARE | COD_INMATRICULARE | Registration number | string | identifier | J2002000372404; J40/630/1992 | primary join key |
| DATA_INMATRICULARE | DATA_INMATRICULARE | Registration date | date | date | 23/01/2002 | DD/MM/YYYY |
| EUID | EUID | European Unique Identifier | string | identifier | ROONRC.J2002000372404 | BRIS |
| FORMA_JURIDICA | FORMA_JURIDICA | Legal form | string | legal_form | SA, SRL, PF | code |
| ADR_TARA | ADR_TARA | Country | string | geography | România | |
| ADR_JUDET | ADR_JUDET | County | string | geography | Bucureşti | |
| ADR_LOCALITATE | ADR_LOCALITATE | Locality/sector | string | address | Bucureşti Sectorul 6 | |
| ADR_DEN_STRADA | ADR_DEN_STRADA | Street | string | address | Şos. VIRTUŢII | |
| ADR_NR_STRADA | ADR_NR_STRADA | Number | string | address | 148 | |
| ADR_BLOC … ADR_APARTAMENT | (same) | block/staircase/floor/apartment | string | address | | often blank |
| ADR_COD_POSTAL | ADR_COD_POSTAL | Postal code | string | address | 060787 | leading spaces possible |
| ADR_SECTOR | ADR_SECTOR | Bucharest sector | string | geography | 6 | Bucharest only |
| ADR_COMPLETARE | ADR_COMPLETARE | Address extra | string | address | spatiul E47 | |
| WEB | WEB | Website | string | metadata | | usually blank |
| TARA_FIRMA_MAMA | TARA_FIRMA_MAMA | Parent-company country | string | geography | | branches only |

## Interpretation Notes

- **The complete trade register**: 4,116,356 companies in one file (643 MB).
- Two identifiers matter: **CUI** (fiscal code → ANAF financials/VAT) and
  **COD_INMATRICULARE** (register number → ONRC companion CSVs). This file is the
  **bridge** between the two identifier spaces.
- CUI `0` marks entities without a fiscal code (some sole traders / PF).
- Address is split across many ADR_* fields; reassemble for a display address.
- Status is NOT in this file — join `OD_STARE_FIRMA` on COD_INMATRICULARE.
- `sample_record.json` is a real company-level row (Dante International SA); no
  personal data.
