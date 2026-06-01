# Guatemala (GT) — GDP Rank #65

> **Summary:** The Registro Mercantil at registromercantil.gob.gt manages company registrations; no public bulk API or free data download is available. Portal search is the only free access path.

## Official Registry

### Registro Mercantil de Guatemala
| Field      | Detail |
|------------|--------|
| URL        | https://www.registromercantil.gob.gt |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; full company documents require in-person or paid extraction; NIT (Número de Identificación Tributaria) used for tax purposes |

### SAT (Superintendencia de Administración Tributaria) — NIT Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://portal.sat.gob.gt |
| Access     | Portal search |
| Cost       | Free |
| Fields     | NIT, taxpayer name, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority; NIT verification available; limited business data |

## Commercial Providers

### Dun & Bradstreet (Central America coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Guatemala coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Guatemala coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Guatemala LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Registro Mercantil portal scrape for basic lookups
- Priority: Low
