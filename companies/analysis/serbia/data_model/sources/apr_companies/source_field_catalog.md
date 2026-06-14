# APR Companies (Open Data API) Field Catalog

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR) / Serbian Business Registers Agency
- URL: https://openapi.apr.gov.rs/api/opendata/companies
- License: public_domain ("Јавни подаци", declared on data.gov.rs)
- Access: public (plain GET, no auth)
- Freshness: monthly (DatumPreseka 2026-05-31 at retrieval)
- Record shape: JSON `{DatumPreseka, Podaci:{<maticni_broj>:{...}}}` — map keyed by matični broj
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Podaci.<mb> (key) | maticni_broj | Registration number | string | identifier | 21141666 | company id; join key |
| …PoslovnoIme | PoslovnoIme | Business name | string | legal_name | ENEKS MONT PLUS DOO KRUŠEVAC | Latin |
| …SifraOpstine | SifraOpstine | Municipality code | string | geography | 70670 | RZS/APR code |
| …NazivOpstine | NazivOpstine | Municipality name | string | geography | КРУШЕВАЦ | Cyrillic |
| …NazivStatus | NazivStatus | Status | string | status | Активан | Cyrillic; map to enum |
| …DatumOsnivanja | DatumOsnivanja | Incorporation date | date | date | 2015-10-09 | ISO |
| …NazivPravneForme | NazivPravneForme | Legal form | string | legal_form | Друштво са ограниченом одговорношћу | Cyrillic |
| …SifraDelatnosti | SifraDelatnosti | Activity code | string | activity | 4322 | KD2010 ≈ NACE Rev.2 |

## Interpretation Notes

- **133,357 companies** (2026-05-31). The full national company register
  (privredna društva), free and public-domain, in a single GET (~57 MB).
- **Script mix**: `PoslovnoIme` is Latin; `NazivOpstine`, `NazivStatus`,
  `NazivPravneForme` are **Cyrillic** — normalise/transliterate for display and
  status mapping.
- **What's missing** vs a full register: **no PIB/VAT**, **no street address**
  (only municipality), **no directors/shareholders**, **no beneficial owners**,
  and **no sole traders (preduzetnici)**. Those require the paid APR web service.
- Join to `apr_financial_statements` on the matični broj key for financials.
- `sample_record.json` is a real record (ENEKS MONT PLUS DOO, MB 21141666).
