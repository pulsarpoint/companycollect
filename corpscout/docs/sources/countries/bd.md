# Bangladesh (BD) — GDP Rank #36

> **Summary:** RJSC (Registrar of Joint Stock Companies and Firms) provides the official registry with a basic online portal; there is no public API or bulk download. Digital infrastructure is limited; manual portal search is the only free access path.

## Official Registry

### RJSC (Registrar of Joint Stock Companies and Firms)
| Field      | Detail |
|------------|--------|
| URL        | https://www.rjsc.gov.bd |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, type, status, registration date |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online portal at rjscforms.gov.bd; no bulk export; no API; full company documents require paid extraction; portal reliability varies |

## Commercial Providers

### Dun & Bradstreet Bangladesh
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Bangladesh coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Very limited Bangladesh coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; minimal Bangladesh coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: RJSC portal scrape (reliability concerns); no strong programmatic option available
- Priority: Low
