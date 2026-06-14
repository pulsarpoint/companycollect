# RPO — Register of Legal Entities (Statistics Office) Field Catalog

## Source Summary

- Country: Slovakia
- Source type: official_registry
- Organization: Štatistický úrad Slovenskej republiky (Statistics Office SR)
- URL: https://api.statistics.sk/rpo/v1/ (`search?identifier={ICO}` → `entity/{id}`)
- License: CC-BY 4.0 (returned inline in `license`)
- Access: public (no auth)
- Freshness: continuous (per-entity `dbModificationDate`)
- Record shape: JSON entity with history arrays (validFrom/validTo on most fields)
- Primary keys: `ico` (`identifiers[].value`)
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| id | id | RPO entity id | integer | identifier | 937053 | for entity/{id} |
| identifiers[] | identifiers | IČO | array | identifier | 31333532 | join key |
| fullNames[] | fullNames | Name history | array | legal_name | ESET, spol. s r.o. | validFrom/validTo |
| addresses[] | addresses | Address history | array | address | Einsteinova 24, 85101 Bratislava | |
| legalForms[] | legalForms | Legal form | array | legal_form | 112 = s.r.o. | CL000056 |
| establishment | establishment | Incorporation date | date | date | 1992-09-17 | |
| activities[] | activities | Business activities | array | activity | free-text scope | not coded |
| statutoryBodies[] | statutoryBodies | Officers (Konateľ) | array | person | — | **PII; redact** |
| stakeholders[] | stakeholders | Shareholders (Spoločník) | array | ownership | — | **PII; redact** |
| equities[] | equities | Share capital | array | financial | 140000 EUR | registered |
| deposits[] | deposits | Capital contributions | array | ownership | — | **PII; redact** |
| otherLegalFacts[] | otherLegalFacts | Legal facts/history | array | filing | — | free-text |
| authorizations[] | authorizations | Signing authority | array | relationship | — | |
| predecessors[] | predecessors | Predecessor entities | array | relationship | 35889691 | mergers |
| sourceRegister | sourceRegister | Origin register | object | metadata | Obchodný register | CL010112 |
| statisticalCodes.mainActivity | statisticalCodes | Main SK NACE | object | activity | — | coded main activity |

## Interpretation Notes

- **The richest open source for Slovakia**: full commercial-register content via
  API — identity, **officers**, **shareholders**, **share capital**, activities,
  predecessors, and **name/address history**. CC-BY 4.0 (attribute SUSR/RPO).
- **History model**: most fields are arrays with `validFrom`/`validTo`; the
  "current" value is the entry with no `validTo` (or latest `validFrom`).
- **No single status flag**: liquidation/dissolution is inferred from
  `otherLegalFacts` text or RÚZ `datumZrusenia`.
- **PERSONAL DATA (GDPR)**: `statutoryBodies`, `stakeholders`, `deposits` carry
  person names and addresses — **redact/minimise** in published outputs (the
  committed `sample_record.json` redacts them).
- Access pattern: `search?identifier={ICO}` returns `results[]` with the entity
  `id`; then `entity/{id}` returns the full record above. A V2 API exists; the v1
  endpoint is marked deprecated but still serving.
