# Sri Lanka (LK) — GDP Rank #79

> **Summary:** The Registrar of Companies (ROC) at drc.gov.lk manages company registrations; no public bulk API or download is available. Portal search provides basic lookup. Limited digital infrastructure.

## Official Registry

### ROC (Registrar of Companies) — Department of the Registrar of Companies
| Field      | Detail |
|------------|--------|
| URL        | https://www.drc.gov.lk |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number (PV/PB/BN prefix), company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal at drc.gov.lk; no bulk export; no API; full company documents require paid extraction; PV = private limited, PB = public limited, BN = business name |

## Commercial Providers

### Dun & Bradstreet (South Asia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, company number, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Sri Lanka coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Sri Lanka coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; minimal Sri Lanka coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: ROC portal scrape for basic lookups
- Priority: Low
