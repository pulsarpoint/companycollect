# ONRC legal representatives — OD_REPREZENTANTI_LEGALI Field Catalog

> **PERSONAL DATA (GDPR).** This file lists named representatives with birth
> dates/places. Redact person-level fields in any published profile. No
> `sample_record.json` is provided (would expose personal data).

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: ONRC via data.gov.ro
- URL: resource od_reprezentanti_legali.csv (same dataset as OD_FIRME)
- License: open (Romanian open-data) — **PERSONAL DATA**
- Access: public
- Freshness: with the dataset snapshot
- Record shape: `^`-delimited CSV, **multiple rows per company**
- Primary keys: `COD_INMATRICULARE` + `PERSOANA_IMPUTERNICITA`
- Join keys: `COD_INMATRICULARE`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| COD_INMATRICULARE | COD_INMATRICULARE | Company reg. number | string | identifier | J40/20659/2004 | join key; repeats |
| PERSOANA_IMPUTERNICITA | PERSOANA_IMPUTERNICITA | Representative name | string | person | (redacted) | person or legal-entity (IPURL) |
| CALITATE | CALITATE | Capacity/role | string | relationship | lichidator, administrator | |
| DATA_NASTERE | DATA_NASTERE | Birth date | date | person | (redacted) | SENSITIVE; redact |
| LOCALITATE_NASTERE | LOCALITATE_NASTERE | Birth locality | string | person | (redacted) | redact |
| JUDET_NASTERE | JUDET_NASTERE | Birth county | string | person | (redacted) | redact |
| TARA_NASTERE | TARA_NASTERE | Birth country | string | person | (redacted) | redact |
| LOCALITATE | LOCALITATE | Current locality | string | address | Huşi | minimise |
| JUDET | JUDET | Current county | string | geography | Vaslui | |
| TARA | TARA | Current country | string | geography | România | |

## Interpretation Notes

- Provides **officers / legal representatives** keyed on `COD_INMATRICULARE`
  (administrators, liquidators). Many rows per company.
- The `PERSOANA_IMPUTERNICITA` value is **sometimes a legal entity** (e.g.
  insolvency practitioner `... IPURL`) and **sometimes a natural person** — only
  the latter is personal data, but treat all conservatively.
- **GDPR**: do not publish names, birth date, or birth place. For a profile,
  store role + (optionally) coarse current county; redact the rest. Have a lawful
  basis before persisting officer identities.
- Ownership (shareholders) is **NOT** here — that requires the paid ONRC portal
  or the restricted RBR.
