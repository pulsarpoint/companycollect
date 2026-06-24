# RS Business Register (bizreg.esrpska.com) Field Catalog

## Source Summary

- Country: Bosnia and Herzegovina (Republika Srpska entity)
- Source type: official_registry
- Organization: APIF / Republika Srpska courts
- URL: http://bizreg.esrpska.com/Home/SearchPoslovniSubjekt
- License: public per-company; no open bulk
- Access: public per-company JSON search (no key)
- Freshness: live register
- Record shape: JSON envelope `{Result, Records[], TotalRecordCount}` per query
- Primary keys: JIB
- Join keys: JIB

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Records[].JIB | JIB | 13-digit unique id = company id = tax id | string | identifier | 4400374890002 | join key; RS starts 44 |
| Records[].MBS | MBS | Court registration number | string | identifier |  | nullable in envelope |
| Records[].MB | MB | Statistical number (7-digit) | string | identifier |  |  |
| Records[].PrivrednoDrustvoId | PrivrednoDrustvoId | Internal subject id | integer | identifier | 3021 | for detail/PDF |
| Records[].PoslovnoIme | PoslovnoIme | Full business name | string | legal_name | "Nova banka" a.d. Banja Luka | display name |
| Records[].SkracenoPoslovnoIme | SkracenoPoslovnoIme | Short name | string | legal_name |  |  |
| Records[].Sjediste | Sjediste | Registered seat / address | string | address | Ulica kralja Alfonsa XIII 37a, Banja Luka | free-text |
| Records[].PreteznaDjelatnost | PreteznaDjelatnost | Primary activity (code+text) | string | activity | 64.19 Ostalo novčano poslovanje | KD BiH ~NACE |
| Records[].StatusPoslovniSubjekatOpis | StatusPoslovniSubjekatOpis | Status | string | status | registrovan | may contain HTML |
| Records[].Osnivaci | Osnivaci | Founders/owners | string | ownership |  | PERSONAL DATA if individual — redact |
| Records[].OdgovornoLice | OdgovornoLice | Responsible person | string | person |  | PERSONAL DATA — redact |
| Records[].Telefon | Telefon/Email/Fax | Contacts | string | raw_extension |  | treat cautiously |
| Records[].PoslovneJedinice | PoslovneJedinice | Branch count | integer | metadata | 68 | branch list endpoint |

## Interpretation Notes

- **Query**: `POST /Home/SearchPoslovniSubjekt`, body
  `term=<Naziv|JIB|MBS>&opstinaId=&osnivac=&djelatnostId=&jtStartIndex=0&jtPageSize=10&jtSorting=PoslovnoIme ASC`,
  header `X-Requested-With: XMLHttpRequest`. Returns JSON. Verified live (Nova
  banka, RiTE Gacko, B2 LINK).
- **Detail**: `/Home/PregledPoslovnogSubjekta/{id}` (HTML, shows JIB) and
  **`/Home/DetaljiPoslovnogSubjekta/{id}`** (official **PDF** extract). Sub-lists:
  `ListLicaOvlastenaZaZastupanje…` (representatives), `ListPoslovneJedinice`
  (branches), `ListProkura` (procura).
- **Scope**: Republika Srpska entities only. FBiH/Brčko entities are in
  `bizreg.pravosudje.ba`.
- **Scripts**: bi-scriptal (Latin + Cyrillic). UTF-8. `StatusPoslovniSubjekatOpis`
  may include `<br>` — strip HTML.
- **Personal data**: `Osnivaci`, `OdgovornoLice`, and representative sub-lists are
  personal data when natural persons — redact in committed samples.
- **No bulk**: per-company search only; there is no documented bulk export or
  enumeration endpoint. Iterate by JIB/name lists obtained elsewhere.
