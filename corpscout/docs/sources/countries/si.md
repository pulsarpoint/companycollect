# Slovenia (SI) — GDP Rank #80

> **Summary:** AJPES (Agency of the Republic of Slovenia for Public Legal Records) provides a free API and bulk downloads at ajpes.si, with registration number as the identifier. Excellent free programmatic access — among the best in Central/Southeast Europe.

## Official Registry

### AJPES (Agencija Republike Slovenije za javnopravne evidence in storitve)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ajpes.si/prs/ |
| Access     | API (free) / Bulk download |
| Cost       | Free |
| Fields     | registration number (matična številka), name, legal form, status, registration date, address, activity (SKD code), capital, tax number (davčna številka) |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Free REST API and downloadable datasets; covers all registered entities; PRS (Poslovni register Slovenije — Business Register of Slovenia) API at ajpes.si; comprehensive coverage including nonprofits and sole traders |

### ePRS (Electronic Business Register)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ajpes.si/prs/default.asp?L=2 |
| Access     | Portal search |
| Cost       | Free |
| Fields     | registration number, name, legal form, status, address, directors |
| Auth       | None |
| Rate limit | None |
| Notes      | Public search interface for the Business Register |

## Commercial Providers

### Bisnode Slovenia (now Dun & Bradstreet)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.si |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, registration number, address, financial data, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Slovenia coverage for financial enrichment |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Slovenia coverage; data from AJPES |

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
- Recommended source: AJPES free API at ajpes.si/prs/ — free, no auth, bulk download available
- Priority: High
