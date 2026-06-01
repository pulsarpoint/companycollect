# Pakistan (PK) — GDP Rank #39

> **Summary:** SECP (Securities and Exchange Commission of Pakistan) maintains the national company registry with a CUIN identifier system and an e-Services portal for search; no public bulk API exists. Commercial data providers are limited for Pakistan.

## Official Registry

### SECP e-Services (Securities and Exchange Commission of Pakistan)
| Field      | Detail |
|------------|--------|
| URL        | https://eservices.secp.gov.pk |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CUIN (Corporate Universal Identification Number), company name, status, incorporation date, registered office, company type |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Portal at eservices.secp.gov.pk/SECP/; no bulk export; no API; full company documents require paid extraction; SECP also publishes some statistical data on secp.gov.pk |

## Commercial Providers

### Dun & Bradstreet Pakistan
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Pakistan coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Very limited Pakistan coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; minimal Pakistan coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SECP e-Services portal scrape for basic lookups; no strong bulk option available
- Priority: Low
