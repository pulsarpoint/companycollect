# Denmark — schema notes

Two official layers, both JSON over Elasticsearch at `distribution.virk.dk`. Field names are Danish.

## Layer 1 — CVR base register (`cvr-permanent`, auth required)

### Index `virksomhed` (companies) — 2,194,982 records

Source records are wrapped as `Vrvirksomhed`. Key fields:

| CVR field (Danish)                         | Meaning / notes                                            |
|--------------------------------------------|------------------------------------------------------------|
| `cvrNummer`                                | 8-digit CVR number — primary company identifier            |
| `navne[]`                                  | Name history; each has `navn`, `periode.gyldigFra/gyldigTil` |
| `virksomhedsform[].virksomhedsformkode`    | Legal form code (e.g. 80=ApS, 60=A/S, 10=Enkeltmandsvirksomhed) |
| `virksomhedsstatus[].status`               | Status (NORMAL, UNDER KONKURS, OPLØST, …)                  |
| `beliggenhedsadresse[]`                    | Registered/physical address (municipality `kommune` codes) |
| `postadresse[]`                            | Postal address (if different)                              |
| `hovedbranche[].branchekode` / `branchetekst` | Primary industry (DB07 / NACE-based)                    |
| `bibranche1/2/3[]`                         | Secondary industries                                       |
| `attributter[]`                            | Typed attributes incl. registered capital (KAPITAL), purpose (FORMÅL) |
| `aarsbeskaeftigelse[]` / `erstMaanedsbeskaeftigelse[]` | Employee counts (yearly/monthly bands)         |
| `livsforloeb[]`                            | Lifecycle periods (existence intervals)                    |
| `stiftelsesDato`                           | Incorporation date                                         |
| `deltagerRelation[]`                       | Links to participants (owners/management/reelle ejere)     |
| `elektroniskPost`, `telefonNummer`, `hjemmeside` | Contact info                                         |

### Index `produktionsenhed` (production units) — 2,787,126 records

| Field                              | Meaning                                              |
|------------------------------------|------------------------------------------------------|
| `pNummer`                          | 10-digit P-number — production-unit identifier       |
| `navne[]`                          | Unit name history                                    |
| `virksomhedsrelation[].cvrNummer`  | Parent company CVR number                            |
| `beliggenhedsadresse[]`            | Unit location                                        |
| `hovedbranche` / bibrancher        | Unit industry codes                                  |
| `erstMaanedsbeskaeftigelse[]`      | Unit-level employment                                |

### Index `deltager` (participants) — 1,772,344 records

Persons and legal entities holding roles/ownership. Links companies to owners, management, and
beneficial owners (*reelle ejere*). Personal data — handle under GDPR / address protection.

## Layer 2 — Financial statements (`offentliggoerelser`, OPEN)

### Filing metadata record (`_source`)

```json
{
  "cvrNummer": 25313763,
  "sagsNummer": "09-63.378",
  "regnskab": { "regnskabsperiode": { "startDato": "2008-01-01", "slutDato": "2008-12-31" } },
  "offentliggoerelsesTidspunkt": "2009-03-09T23:00:00.000Z",
  "sidstOpdateret": "2009-03-09T23:00:00.000Z",
  "indlaesningsTidspunkt": "2018-04-04T13:40:50.047Z",
  "offentliggoerelsestype": "regnskab",
  "omgoerelse": false,
  "dokumenter": [
    { "dokumentType": "AARSRAPPORT", "dokumentMimeType": "image/tiff", "dokumentUrl": "http://regnskaber.virk.dk/.../...tif" }
  ]
}
```

| Field                              | Meaning                                                       |
|------------------------------------|---------------------------------------------------------------|
| `cvrNummer`                        | Company the filing belongs to (join key to `virksomhed`)      |
| `regnskab.regnskabsperiode.*`      | Accounting period start/end                                   |
| `offentliggoerelsesTidspunkt`      | Publication timestamp                                         |
| `sidstOpdateret`                   | Last-updated — use for incremental sync                       |
| `offentliggoerelsestype`           | e.g. `regnskab`                                               |
| `dokumenter[].dokumentType`        | AARSRAPPORT, DELAARSRAPPORT, DELAARSRAPPORT_ESEF, ESEF_EXTENSION |
| `dokumenter[].dokumentMimeType`    | application/xml (XBRL), application/xhtml+xml (iXBRL), application/zip (ESEF), image/tiff, application/pdf |
| `dokumenter[].dokumentUrl`         | Direct download (served **gzip-compressed** — decompress)     |

### XBRL document content (the actual figures)

Inline XBRL / XBRL using the Danish **DCCA taxonomy**:
- `fsa:` (`http://xbrl.dcca.dk/fsa`) — financial-statement facts (income statement, balance sheet)
- `gsd:` (`http://xbrl.dcca.dk/gsd`) — general/company-identifying data
- `cmn:` (`http://xbrl.dcca.dk/cmn`) — common elements
- Listed groups also use `ifrs-full:` + ESEF taxonomy.

Parse the XBRL instance to extract line items (revenue, profit/loss, assets, equity, liabilities,
etc.) with their context periods and `iso4217` currency.

## Mapping to internal company model

| Internal field          | Denmark source                                                        |
|-------------------------|-----------------------------------------------------------------------|
| `company_id`            | `cvrNummer`                                                           |
| `registration_number`   | `cvrNummer`                                                           |
| `tax_id` / `vat_id`     | `cvrNummer` (CVR doubles as the VAT/SE base; "DK" + CVR = VAT number) |
| `legal_name`            | current `navne[]` (where `periode.gyldigTil` is null)                |
| `company_type`          | `virksomhedsform.virksomhedsformkode` (+ text)                       |
| `status`                | `virksomhedsstatus.status`                                            |
| `incorporation_date`    | `stiftelsesDato`                                                      |
| `dissolution_date`      | from `livsforloeb` / `virksomhedsstatus`                             |
| `registered_address`    | `beliggenhedsadresse`                                                 |
| `municipality`          | `beliggenhedsadresse.kommune`                                         |
| `industry_code`         | `hovedbranche.branchekode` (DB07)                                    |
| `country`               | "Denmark"                                                            |
| `source_name`           | "CVR / Erhvervsstyrelsen"                                            |
| `financials`            | from `offentliggoerelser` filings + parsed XBRL (DCCA fsa/gsd)       |
| `raw_record`            | full `_source`                                                       |

## Gotchas

- CVR/VAT number: the Danish VAT number is `"DK" + cvrNummer` (8 digits).
- Documents from `regnskaber.virk.dk` are gzip-compressed even when `Content-Type: text/xml`.
- Elasticsearch query cap is 3,000 docs — use the scroll API for full extraction.
- Many array fields carry validity periods (`periode.gyldigFra/gyldigTil`) — pick the current one.
