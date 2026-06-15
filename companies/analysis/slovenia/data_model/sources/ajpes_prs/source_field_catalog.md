# AJPES PRS (OPSI open data) Field Catalog

## Source Summary

- Country: Slovenia
- Source type: official_registry
- Organization: AJPES via OPSI (podatki.gov.si)
- URL: https://podatki.gov.si/dataset/poslovni-register-slovenije (resource opsiprs.csv)
- License: CC-BY 4.0
- Access: public
- Freshness: twice monthly (dvotedensko)
- Record shape: **UTF-16** CSV, comma-delimited, quoted; one row per entity
- Primary keys: `Matična številka`
- Join keys: `Matična številka`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Matična številka | Matična številka | Registration number | string | identifier | 3282490000 | join key |
| Popolno ime | Popolno ime | Full name | string | legal_name | ISTRA XLL …, d.o.o. | |
| HSEID | HSEID | Address identifier | string | identifier | 100400000140334594 | HSMID |
| Pravnoorganizacijska oblika | (same) | Legal form (text) | string | legal_form | Družba z omejeno odgovornostjo d.o.o. | no code |
| Registrski organ | (same) | Registering authority | string | metadata | Okrožno sodišče Koper | court/AJPES |
| Ulica/Hišna št/… | (address) | Street/number/settlement/postal/post | string | address | Fazanska ulica 004, Lucija 6320 | split fields |
| Država | Država | Country | string | geography | SLOVENIJA | |

## Interpretation Notes

- The national business register, **293,222 entities** of all types (d.o.o., s.p.,
  društvo, poslovna enota, javni zavod, …). **CC-BY 4.0**, refreshed twice monthly.
- **Encoding is UTF-16** — convert on ingest (BOM + 2-byte chars).
- Identity + address only: **no tax number, no status, no SKD activity, no
  incorporation date, no officers, no financials** in this open feed. Enrich tax/
  activity from FURS (join on Matična številka); status/SKD/history need the
  credentialed restPrsInfo.
- `Registrski organ` hints at the form: a district court (Okrožno sodišče) for
  d.o.o./d.d.; an AJPES branch for s.p./associations.
- `sample_record.json` is a real entity (ISTRA XLL d.o.o., MB 3282490000).
