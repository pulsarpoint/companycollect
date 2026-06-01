# Portugal (PT) — GDP Rank #48

> **Summary:** IRN (Instituto dos Registos e do Notariado) manages the commercial register with NIPC as the business identifier; no free public API exists. Racius.com is a useful commercial aggregator. ePortugal has limited business data.

## Official Registry

### IRN (Instituto dos Registos e do Notariado)
| Field      | Detail |
|------------|--------|
| URL        | https://www.irn.mj.pt |
| Access     | Portal search |
| Cost       | Free (basic); paid for full certificates |
| Fields     | NIPC (Número de Identificação de Pessoa Coletiva), company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Commercial register portal; full company certificates require paid extraction (~€10–20); no bulk API; also accessible at publicacoes.mj.pt |

### Registo Comercial Online
| Field      | Detail |
|------------|--------|
| URL        | https://publicacoes.mj.pt |
| Access     | Portal search |
| Cost       | Free (basic search) |
| Fields     | NIPC, company name, legal form, status |
| Auth       | None |
| Rate limit | Low |
| Notes      | Public filings and announcements portal; useful for status checking |

## Commercial Providers

### Racius.com
| Field      | Detail |
|------------|--------|
| URL        | https://www.racius.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | NIPC, name, address, activity (CAE code), turnover, employees, directors, financial data |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Leading Portuguese commercial data aggregator; good coverage of ~1.3M companies |

### Informa D&B Portugal
| Field      | Detail |
|------------|--------|
| URL        | https://www.informa.pt |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, NIPC, address, financial statements, credit score, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of D&B network; strong local coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Portugal coverage |

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
- Recommended source: Racius.com API for enrichment; IRN portal scrape for basic lookups
- Priority: Medium
