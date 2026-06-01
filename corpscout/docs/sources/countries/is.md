# Iceland (IS) — GDP Rank #99

> **Summary:** Iceland uses the kennitala (10-digit national/company ID) as the universal identifier. RSK (Skatturinn / Directorate of Internal Revenue) and Fyrirtækjaskrá (Company Registry) provide free lookup. Good free programmatic access for a small Nordic country.

## Official Registry

### Fyrirtækjaskrá / RSK (Skatturinn — Iceland Revenue and Customs)
| Field      | Detail |
|------------|--------|
| URL        | https://www.skatturinn.is/fyrirtaekjaskra/ |
| Access     | API (free) / Portal search |
| Cost       | Free |
| Fields     | kennitala (10-digit company/national ID), company name, legal form, status, registration date, address, activity |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Kennitala is the universal identifier used for both individuals and companies; free search at skatturinn.is; API available at skra.is; also accessible at fjarmal.is |

### Skrá (Registers Iceland)
| Field      | Detail |
|------------|--------|
| URL        | https://www.skra.is |
| Access     | API (free) |
| Cost       | Free |
| Fields     | kennitala, name, address, type |
| Auth       | None |
| Rate limit | None |
| Notes      | National registry including companies, individuals, addresses; REST API available; kennitala lookup |

## Commercial Providers

### Creditinfo Iceland
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditinfo.is |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | kennitala, name, address, financial data, credit score, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local Icelandic credit bureau; good coverage for financial enrichment |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Iceland coverage; data from RSK/Fyrirtækjaskrá |

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
- Recommended source: Skatturinn/Skrá free API — kennitala-based, free, no auth
- Priority: High
