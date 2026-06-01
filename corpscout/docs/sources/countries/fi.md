# Finland (FI) — GDP Rank #47

> **Summary:** PRH (Patent and Registration Office) provides a completely free REST API at avoindata.prh.fi with no authentication required, returning company data by Y-tunnus (business ID). Excellent free coverage — one of the best globally.

## Official Registry

### PRH Open Data API (Patent and Registration Office)
| Field      | Detail |
|------------|--------|
| URL        | https://avoindata.prh.fi/tr/v1/companies |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Y-tunnus (business ID), company name, legal form, registration date, status, address, activity code (TOL), liquidation/bankruptcy flags |
| Auth       | None |
| Rate limit | None documented |
| Notes      | REST API with JSON responses; search by Y-tunnus or company name; full company register coverage; documentation at avoindata.prh.fi/tietopalvelu/tr |

### YTJ (Yritys- ja yhteisötietojärjestelmä) — Business Information System
| Field      | Detail |
|------------|--------|
| URL        | https://www.ytj.fi |
| Access     | API (free) / Portal search |
| Cost       | Free |
| Fields     | Y-tunnus, company name, legal form, status, address, names history |
| Auth       | None |
| Rate limit | None |
| Notes      | Joint PRH and Tax Administration register; also accessible via avoindata.prh.fi; YTJ API at avoindata.prh.fi/bis/v1 |

## Commercial Providers

### Asiakastieto (Suomen Asiakastieto Oy)
| Field      | Detail |
|------------|--------|
| URL        | https://www.asiakastieto.fi |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, Y-tunnus, address, credit score, payment history, directors, financial statements |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Major Finnish credit bureau; strong local coverage for financials and credit data |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Finland coverage; data sourced from PRH |

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
- Recommended source: PRH Open Data API at avoindata.prh.fi — completely free, no auth, full coverage
- Priority: High
