# Kenya (KE) — GDP Rank #59

> **Summary:** BRS (Business Registration Service) at brs.go.ke and the eCitizen platform handle company registrations; no public bulk API exists. The company number is the identifier. eCitizen provides basic lookup functionality.

## Official Registry

### BRS (Business Registration Service)
| Field      | Detail |
|------------|--------|
| URL        | https://www.brs.go.ke |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Main company register portal; also accessible via eCitizen; no bulk export or API; full documents require paid extraction via eCitizen |

### eCitizen — Company Search
| Field      | Detail |
|------------|--------|
| URL        | https://ecitizen.go.ke |
| Access     | Portal search |
| Cost       | Free (search); paid for certificates |
| Fields     | company number, name, status, type |
| Auth       | eCitizen account (free registration) |
| Rate limit | Low |
| Notes      | Government services portal; company certificates cost ~KES 650; no API |

## Commercial Providers

### Dun & Bradstreet Kenya
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, company number, address, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Kenya coverage |

### Metropol Corporation
| Field      | Detail |
|------------|--------|
| URL        | https://www.metropol.co.ke |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, registration number, credit score, directors, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local Kenyan credit bureau; good SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Kenya coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: BRS/eCitizen portal scrape for basic lookups
- Priority: Low
