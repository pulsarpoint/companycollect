# Enhetsregisteret (sub-entities / underenheter) Field Catalog

## Source Summary

- Country: Norway
- Source type: official_registry_api (+ gzip bulk)
- Organization: Brønnøysund Register Centre (Brreg)
- URL: https://data.brreg.no/enhetsregisteret/api/underenheter
- License: NLOD 2.0
- Access: public (no auth)
- Freshness: daily (delta feed /api/oppdateringer/underenheter)
- Record shape: HAL JSON per sub-entity; list wraps `_embedded.underenheter[]`; bulk gzip JSON/CSV
- Primary keys: `organisasjonsnummer`
- Join keys: `organisasjonsnummer`, `overordnetEnhet` (→ parent enhet)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| organisasjonsnummer | organisasjonsnummer | Sub-entity org number | string | identifier | 933489272 | distinct from parent |
| navn | navn | Establishment name | string | legal_name | - ZOTKO .NO | often trade/site name |
| organisasjonsform.kode | organisasjonsform.kode | Form code (usually BEDR) | string | legal_form | BEDR | |
| organisasjonsform.beskrivelse | ... | Form description | string | legal_form | Underenhet til næringsdrivende... | |
| naeringskode1.kode | naeringskode1.kode | Industry (SN2007) | string | activity | 47.250 | may differ from parent |
| naeringskode1.beskrivelse | ... | Industry description | string | activity | Detaljhandel med drikkevarer | |
| overordnetEnhet | overordnetEnhet | Parent entity org number | string | relationship | 933365573 | mandatory join key |
| oppstartsdato | oppstartsdato | Start/opening date | date | date | 2024-06-01 | |
| registreringsdatoEnhetsregisteret | ... | CCR registration date | date | date | 2024-05-23 | |
| registrertIMvaregisteret | registrertIMvaregisteret | VAT-registered | boolean | status | false | |
| harRegistrertAntallAnsatte | harRegistrertAntallAnsatte | Has employee count | boolean | employment | false | antallAnsatte when true |
| beliggenhetsadresse.* | beliggenhetsadresse | Physical location address | object | address | Follerøvegen 20, 6652 SURNADAL | site-level geo |

## Interpretation Notes

- **Sub-entities (underenheter)** are establishments/branches/operating sites belonging to a
  parent legal entity (`enhet`). They carry their own org number and their own activity code,
  which makes them valuable for **site-level coverage** (each physical location of a company).
- **Address field name differs from enheter**: sub-entities use `beliggenhetsadresse`
  (physical location), whereas legal entities use `forretningsadresse` + `postadresse`.
- **`overordnetEnhet` is mandatory** and is the join key back to the parent in `brregenhet`.
- Total: 842,538 sub-entities (verified `page.totalElements`).
- A sub-entity is **not** a separate legal person; financials and roles attach to the parent
  `enhet`, not the sub-entity.
- All descriptive text is Norwegian; English names are helper metadata only.
