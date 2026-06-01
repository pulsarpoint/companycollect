# Bolivia (BO) — GDP Rank #86

> **Summary:** SEPREC (Servicio Plurinacional de Registro de Comercio) at seprec.gob.bo manages commercial registrations with Matrícula de Comercio as the identifier; no public API or bulk download is available.

## Official Registry

### SEPREC (Servicio Plurinacional de Registro de Comercio)
| Field      | Detail |
|------------|--------|
| URL        | https://www.seprec.gob.bo |
| Access     | Portal search |
| Cost       | Free |
| Fields     | Matrícula de Comercio number, company name, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | National commercial registry; no bulk export; no API; company documents require paid extraction; SEPREC was created to centralize what were previously regional registers |

### SIN (Servicio de Impuestos Nacionales) — NIT Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.impuestos.gob.bo |
| Access     | Portal search |
| Cost       | Free |
| Fields     | NIT (Número de Identificación Tributaria), taxpayer name, type, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority NIT verification; limited business data |

## Commercial Providers

### Dun & Bradstreet (Andean coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Bolivia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Bolivia coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Bolivia LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SEPREC portal scrape for basic lookups
- Priority: Low
