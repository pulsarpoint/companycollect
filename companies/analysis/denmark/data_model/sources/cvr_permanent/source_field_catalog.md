# CVR-permanent (Det Centrale Virksomhedsregister) Field Catalog

## Source Summary

- Country: Denmark
- Source type: official_registry_api (Elasticsearch 1.7.x)
- Organization: Erhvervsstyrelsen (Danish Business Authority)
- URL: http://distribution.virk.dk/cvr-permanent
- License: Free reuse incl. commercial under CVR-loven; honour `reklamebeskyttelse` (advertising-protection) flag
- Access: public_with_free_credentials (HTTP Basic; request free at cvrselvbetjening@erst.dk, sign protected-data declaration)
- Freshness: near real-time
- Record shape: Elasticsearch `hits.hits[]._source` wrapped as `Vrvirksomhed`
- Primary keys: `cvrNummer`
- Join keys: `cvrNummer`

> **Planning-only basis.** No raw records were downloaded for this source. A live
> `match_all` against `/cvr-permanent/virksomhed/_search` without credentials returned
> **HTTP 401**, confirming the auth gate. The fields below are documented from
> `schema_notes.md`, `source_inventory.json`, and public CVR distribution documentation.
> They describe well-known public CVR concepts, but `example_values` are documented
> code/format illustrations (or CVR numbers seen in the open offentliggoerelser source),
> **not observed records from this index**. Confirm exact shapes after obtaining credentials.

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `_source.Vrvirksomhed.cvrNummer` | cvrNummer | 8-digit CVR number; primary national id; VAT = `DK`+cvrNummer | integer | identifier | 22756214 | Primary join key across all layers |
| `…navne[]` | navne | Name history; current = entry with `periode.gyldigTil == null` | array | legal_name | — | Pick current for display |
| `…virksomhedsform[].virksomhedsformkode` | virksomhedsformkode | Legal form code (period-bearing) | array | legal_form | 80=ApS, 60=A/S, 10=Enkeltmandsvirksomhed | Carry code + text |
| `…virksomhedsstatus[].status` | status | Status (period-bearing) | array | status | NORMAL, UNDER KONKURS, OPLØST | Pick current |
| `…beliggenhedsadresse[]` | beliggenhedsadresse | Registered/physical address; incl. kommune code | array | address | — | Pick current; expose municipality |
| `…postadresse[]` | postadresse | Postal address if different | array | address | — | Optional |
| `…hovedbranche[].branchekode` | branchekode | Primary industry (DB07/NACE) + branchetekst | array | activity | — | Map to NACE |
| `…bibranche1/2/3[]` | bibranche1/2/3 | Up to 3 secondary industries (DB07) | array | activity | — | Often empty |
| `…attributter[]` | attributter | Typed attributes incl. KAPITAL (capital), FORMÅL (purpose) | array | raw_extension | — | Extract capital + purpose |
| `…aarsbeskaeftigelse[]` | aarsbeskaeftigelse | Yearly employment bands | array | employment | — | Latest year band |
| `…erstMaanedsbeskaeftigelse[]` | erstMaanedsbeskaeftigelse | Monthly employment bands | array | employment | — | Latest month band |
| `…livsforloeb[]` | livsforloeb | Lifecycle/existence intervals | array | date | — | Derive dissolution |
| `…stiftelsesDato` | stiftelsesDato | Incorporation date | date | date | — | Primary incorporation source |
| `…deltagerRelation[]` | deltagerRelation | Owner/management/beneficial-owner relations | array | relationship | — | Join to `deltager`; GDPR |
| `…elektroniskPost / telefonNummer / hjemmeside` | contact | Email / phone / website | array | metadata | — | Honour advertising protection |
| `…virksomhedMetadata` | virksomhedMetadata | Convenience rollup of newest name/form/status/branche | object | metadata | — | Shortcut for current values |

## Interpretation Notes

- **Period-bearing arrays.** Most attributes are arrays of period-stamped values
  (`periode.gyldigFra` / `periode.gyldigTil`). The "current" value is the entry whose
  `gyldigTil` is null. The `virksomhedMetadata` rollup pre-computes the newest values.
- **Identifiers.** `cvrNummer` is the universal join key. The Danish VAT number is the
  literal string `"DK" + cvrNummer` (8 digits). CVR also serves as the SE/tax base number.
- **Related indexes (same distribution, same credentials).**
  - `produktionsenhed` — 2,787,126 production units (P-numbers); each links to a parent via
    `virksomhedsrelation[].cvrNummer`. Treat as establishments/sites under a company.
  - `deltager` — 1,772,344 participants (persons and legal entities) holding roles/ownership,
    including beneficial owners (*reelle ejere*). Personal data — GDPR + address protection.
- **Code lists.** Legal form (`virksomhedsform`) and industry (`branchekode`, DB07/NACE) are
  coded; resolve to text. Municipality (`kommune`) codes geo-locate the address.
- **Advertising protection.** Entities may set `reklamebeskyttelse`; flag protected entities
  and gate any marketing/outreach use on that flag (license obligation under CVR-loven).
- **Uncertainty.** Exact JSON nesting (e.g. whether values sit directly under
  `Vrvirksomhed` or under an additional wrapper) must be confirmed against a real authenticated
  response. Confidence is **medium** for field meanings, **low** for exact paths.
