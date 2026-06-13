# Enhetsregisteret (entities) Field Catalog

## Source Summary

- Country: Norway
- Source type: official_registry_api (+ gzip bulk file)
- Organization: Brønnøysund Register Centre (Brønnøysundregistrene / Brreg)
- URL: https://data.brreg.no/enhetsregisteret/api/enheter
- License: NLOD 2.0
- Access: public (no auth, no key)
- Freshness: daily (delta feed at /api/oppdateringer/enheter)
- Record shape: HAL JSON object per entity; list endpoint wraps `_embedded.enheter[]`; bulk is gzip JSON array or flattened CSV with dot-notation columns
- Primary keys: `organisasjonsnummer`
- Join keys: `organisasjonsnummer` (→ regnskap, roller), `overordnetEnhet` (→ parent enhet)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| organisasjonsnummer | organisasjonsnummer | 9-digit national org number | string | identifier | 923609016 | Keep as string (leading zeros); join key |
| navn | navn | Legal name | string | legal_name | EQUINOR ASA | Uppercase; no history here |
| organisasjonsform.kode | organisasjonsform.kode | Legal form code | string | legal_form | ASA, AS, ENK, NUF | code list /organisasjonsformer |
| organisasjonsform.beskrivelse | organisasjonsform.beskrivelse | Legal form name | string | legal_form | Allmennaksjeselskap | |
| naeringskode1.kode | naeringskode1.kode | Primary industry (SN2007/NACE) | string | activity | 06.100 | naeringskode2/3 same shape |
| naeringskode1.beskrivelse | naeringskode1.beskrivelse | Industry description | string | activity | Utvinning av råolje | |
| antallAnsatte | antallAnsatte | Employee count | integer | employment | 21467 | only if harRegistrertAntallAnsatte |
| harRegistrertAntallAnsatte | harRegistrertAntallAnsatte | Has employee count | boolean | employment | true | |
| hjemmeside | hjemmeside | Website | string | metadata | www.equinor.com | unvalidated, can be stale |
| telefon | telefon | Phone | string | metadata | 51 99 00 00 | mobil/epostadresse siblings |
| epostadresse | epostadresse | Email (bulk CSV only) | string | metadata | — | possible personal data (GDPR) |
| forretningsadresse.* | forretningsadresse | Business address (lines, postnummer, poststed, kommune, kommunenummer, landkode) | object | address | Forusbeen 50, 4035 STAVANGER | principal location |
| postadresse.* | postadresse | Mailing address (same shape) | object | address | Postboks 8500 | often a PO box |
| institusjonellSektorkode.kode | institusjonellSektorkode.kode | Institutional sector (SSB) | string | activity | 1120 | 1120=state-owned ltd |
| stiftelsesdato | stiftelsesdato | Foundation date | date | date | 1972-09-18 | best incorporation date |
| registreringsdatoEnhetsregisteret | registreringsdatoEnhetsregisteret | CCR registration date | date | date | 1995-03-12 | CSV casing differs |
| registrertIMvaregisteret | registrertIMvaregisteret | In VAT register | boolean | status | true | VAT = NO{orgnr}MVA |
| registrertIForetaksregisteret | registrertIForetaksregisteret | In Business Enterprise reg. | boolean | status | true | constitutive register |
| registrertIStiftelsesregisteret | ... | In foundations register | boolean | status | false | |
| registrertIFrivillighetsregisteret | ... | In voluntary-org register | boolean | status | false | |
| registrertIPartiregisteret | ... | In party register | boolean | status | false | |
| sisteInnsendteAarsregnskap | sisteInnsendteAarsregnskap | Year of last filed accounts | string | filing | 2024 | triggers financial fetch |
| konkurs | konkurs | Bankrupt | boolean | status | false | konkursdato in CSV |
| underAvvikling | underAvvikling | Under voluntary liquidation | boolean | status | false | underAvviklingDato in CSV |
| underTvangsavviklingEllerTvangsopplosning | ... | Under compulsory liquidation | boolean | status | false | tvangs* date columns explain reason |
| overordnetEnhet | overordnetEnhet | Parent org number | string | relationship | — | join back to enheter |
| erIKonsern | erIKonsern | Part of a group | boolean | relationship | true | members not enumerated |
| kapital.belop | kapital.belop | Share capital amount | decimal | financial | 6392018780.0 | AS/ASA; entity API only |
| kapital.valuta | kapital.valuta | Share capital currency | string | financial | NOK | ISO 4217 |
| maalform | maalform | Language form | string | metadata | Bokmål | |
| vedtektsfestetFormaal | vedtektsfestetFormaal | Articles purpose | array | activity | ["Å utvikle, produsere..."] | line-wrapped array |
| aktivitet | aktivitet | Activity free text | array | activity | ["Selv, eller gjennom..."] | line-wrapped array |

## Interpretation Notes

- **Two address blocks**: `forretningsadresse` (business/visiting) and `postadresse` (mailing).
  Prefer `forretningsadresse` for the registered location; addresses are arrays of street lines.
- **Industry codes** use SN2007 (Norwegian NACE Rev.2); up to three (`naeringskode1..3`).
- **Status is a set of booleans**, not a single enum. Derive a single status:
  `konkurs` → bankrupt; `underTvangsavviklingEllerTvangsopplosning` → compulsory_liquidation;
  `underAvvikling` → liquidation; else active. Dates for each live in the bulk CSV columns.
- **Register flags** (`registrertI*`) show which sub-registers the entity belongs to —
  Foretaksregisteret (commercial), MVA (VAT), Frivillighetsregisteret (non-profit), etc.
- **API vs bulk skew**: the entity API record (used for this catalog, Equinor) carries
  `kapital`, `vedtektsfestetFormaal`, `aktivitet` and richer detail; the bulk CSV adds flat
  columns like `epostadresse`, all the `*Dato` status dates, and foreign-entity columns
  (`registreringsnummerIHjemlandet`, `utenlandskRegister*`, `foretaksformIHjemlandet.*`) for NUFs.
  Field casing differs slightly in CSV (e.g. `registreringsdatoenhetsregisteret`,
  `registrertIMvaRegisteret`).
- **Bulk count vs active count**: bulk CSV `enheter_alle` has 1,458,299 rows (incl. dissolved);
  the API search reports 1,164,396 currently registered. Filter on status if you want live only.
- **Language**: all descriptive text is Norwegian (Bokmål/Nynorsk). English translations here are
  helper metadata only.
