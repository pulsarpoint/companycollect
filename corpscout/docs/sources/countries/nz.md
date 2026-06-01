# New Zealand (NZ) — GDP Rank #46

> **Summary:** The Companies Office provides a free API (free API key required) at api.business.govt.nz returning company details, directors, shareholders, and filing history. Excellent free programmatic access — among the best globally.

## Official Registry

### New Zealand Companies Office API
| Field      | Detail |
|------------|--------|
| URL        | https://api.business.govt.nz/api/v1/companies |
| Access     | API (free) |
| Cost       | Free |
| Fields     | company number, company name, status, entity type, incorporation date, registered address, directors, shareholders, annual return date |
| Auth       | API key (free registration at api.business.govt.nz) |
| Rate limit | 100 requests/minute per key |
| Notes      | REST API with JSON; covers ~900K companies; real-time data; also provides directors search, shareholders, filing documents; API key registration is free and instant at developer.business.govt.nz |

### New Zealand Companies Register (Search Portal)
| Field      | Detail |
|------------|--------|
| URL        | https://app.companiesoffice.govt.nz/companies/app/search |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, name, status, type, incorporation date, address, directors |
| Auth       | None |
| Rate limit | None |
| Notes      | Public search portal; same data as API; free document downloads for most filings |

## Commercial Providers

### Dun & Bradstreet New Zealand
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, company number, address, financials, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good NZ coverage; useful for credit scoring beyond the official API |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good New Zealand coverage; data sourced from Companies Office |

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
- Recommended source: Companies Office free API at api.business.govt.nz — free API key, excellent data
- Priority: High
