# Dominican Republic (DO) — GDP Rank #62

> **Summary:** The Registro Mercantil at registromercantil.gob.do manages company registrations with RNC (Registro Nacional de Contribuyentes) as the identifier; no public bulk API is available. Portal search is the main free access path.

## Official Registry

### Registro Mercantil (Cámara de Comercio)
| Field      | Detail |
|------------|--------|
| URL        | https://www.registromercantil.gob.do |
| Access     | Portal search |
| Cost       | Free |
| Fields     | RNC (Registro Nacional de Contribuyentes), company name, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Mercantile register portal; no bulk export; no API; company certificates require paid extraction |

### DGII (Dirección General de Impuestos Internos) — RNC Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://dgii.gov.do/app/WebApps/ConsultasWeb/consultas/rnc.aspx |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | RNC, company name, type, tax category, status |
| Auth       | None |
| Rate limit | Moderate |
| Notes      | Tax authority RNC lookup; useful for status verification; RNC doubles as tax ID and business identifier |

## Commercial Providers

### Dun & Bradstreet (Caribbean coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Dominican Republic coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Dominican Republic coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited DR coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: DGII RNC portal scrape for basic lookups
- Priority: Low
