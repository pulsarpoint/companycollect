# Kazakhstan (KZ) — GDP Rank #50

> **Summary:** The Business Information System (bis.gov.kz) and eGov portal (egov.kz) provide some company lookup capability using BIN (Business Identification Number); limited open data access with no bulk API. Digital infrastructure is developing.

## Official Registry

### BIS (Business Information System) — bis.gov.kz
| Field      | Detail |
|------------|--------|
| URL        | https://bis.gov.kz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | BIN (Business Identification Number, 12-digit), company name, legal form, status, registration date, address, activity (OKED code) |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Main business registry portal; no bulk export; no API; BIN is the unified identifier for both legal entities and individual entrepreneurs |

### eGov.kz — Business Registry Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://egov.kz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | BIN, company name, status, registration date |
| Auth       | EDS (electronic digital signature) for some services |
| Rate limit | Low |
| Notes      | Government e-services portal; company lookup available without auth for basic info; limited fields |

## Commercial Providers

### Dun & Bradstreet Kazakhstan
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, BIN, address, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Kazakhstan coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Very limited Kazakhstan coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; limited Kazakhstan coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: bis.gov.kz portal scrape for basic BIN/name lookups
- Priority: Low
