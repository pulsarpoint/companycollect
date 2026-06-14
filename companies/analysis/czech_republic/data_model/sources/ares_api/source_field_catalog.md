# ARES — Administrativní registr ekonomických subjektů (REST API) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: official_registry (aggregator)
- Organization: Ministerstvo financí ČR (Ministry of Finance)
- URL: https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty
- License: Open data (otevřená data; attribute ARES/MF ČR — confirm exact terms)
- Access: public (rate-limited ~tens of thousands/day)
- Freshness: near real-time (aggregates ROS, RES/ČSÚ, VR/Justice, RŽP, DPH)
- Record shape: `GET /{ico}` → single JSON object; `POST /vyhledat` → `{pocetCelkem, ekonomickeSubjekty[]}`
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ico | ico | IČO company id (8-digit, padded) | string | identifier | 27082440 | join key |
| obchodniJmeno | obchodniJmeno | Business name | string | legal_name | Alza.cz a.s. | history in dalsiUdaje |
| dic | dic | DIČ (VAT/tax id) | string | identifier | CZ27082440 | CZ + IČO |
| pravniForma | pravniForma | Legal-form code | string | legal_form | 121, 325 | codebook |
| sidlo.textovaAdresa | textovaAdresa | Registered address (text) | string | address | Jankovcova 1522/53, … Praha 7 | structured sub-fields too |
| sidlo.nazevObce | nazevObce | Municipality | string | geography | Praha | RUIAN kodObce |
| sidlo.nazevKraje | nazevKraje | Region (kraj/NUTS3) | string | geography | Hlavní město Praha | kodKraje |
| czNace2008 | czNace2008 | CZ-NACE activity codes | array | activity | 46900, 47911 | NACE Rev.2 |
| datumVzniku | datumVzniku | Incorporation date | date | date | 2003-08-26 | |
| datumZaniku | datumZaniku | Dissolution date | date | date | (none — active) | absent if active |
| seznamRegistraci.stavZdrojeVr | stavZdrojeVr | Status in public register | string | status | AKTIVNI | per-source flags |
| primarniZdroj | primarniZdroj | Primary source register | string | metadata | ros | provenance |
| datumAktualizace | datumAktualizace | Last updated | date | metadata | 2026-06-10 | freshness |

## Interpretation Notes

- **Aggregator.** ARES merges the underlying registers and exposes a clean per-entity JSON. The
  `seznamRegistraci` block carries a status flag for **each** source register (`stavZdrojeVr` = Veřejný
  rejstřík/Justice, `stavZdrojeRes` = ČSÚ, `stavZdrojeRos`, `stavZdrojeRzp`, `stavZdrojeDph` = VAT). Derive a
  single company status from the relevant register plus `datumZaniku`.
- **IČO padding.** ARES returns the IČO **zero-padded to 8** (`00006947`); the Justice bulk may store it
  unpadded (`3431509`). Normalize both to 8 digits before joining.
- **Address is fully structured** with RUIAN codes (`kodObce`, `kodKraje`, `kodAdresnihoMista`) enabling
  precise geocoding; `textovaAdresa` is the convenience single line.
- **CZ-NACE** is a list of mixed-granularity codes; the primary activity is not explicitly flagged here —
  cross-reference ČSÚ RES.
- **History** (previous names/addresses) is under `dalsiUdaje[]` (not fully cataloged; `raw_extension`).
- **Search**: `POST /vyhledat` with `{start, pocet, obchodniJmeno|ico|sidlo...}` returns `pocetCelkem` for
  paging. Verified live for "Alza" (pocetCelkem=2).
