# Bulgaria (BG) — GDP Rank #68

> **Summary:** BRRA (Bulgarian Registry Agency) offers a free REST API and bulk download at portal.registryagency.bg with UIC (Unified Identification Code) as the identifier. Excellent free programmatic access — one of the best in Southeast Europe.

## Official Registry

### BRRA (Bulgarian Registry Agency) — Trade Register
| Field      | Detail |
|------------|--------|
| URL        | https://portal.registryagency.bg |
| Access     | API (free) / Bulk download |
| Cost       | Free |
| Fields     | UIC (ЕИК — Единен идентификационен код), company name, legal form, status, registration date, seat and management address, activity, directors |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Free REST API and downloadable XML bulk data at portal.registryagency.bg; covers all registered companies; real-time search; also accessible at brra.bg; comprehensive coverage |

### Търговски регистър (Trade Register) — brra.bg
| Field      | Detail |
|------------|--------|
| URL        | https://www.brra.bg |
| Access     | Portal search |
| Cost       | Free |
| Fields     | UIC, company name, legal form, status, documents |
| Auth       | None |
| Rate limit | None |
| Notes      | Public search interface; document downloads free for published filings |

## Commercial Providers

### Creditreform Bulgaria
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.bg |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, UIC, address, financial data, credit score, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of Creditreform network; good for financial enrichment |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Bulgaria coverage; data sourced from BRRA |

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
- Recommended source: BRRA REST API at portal.registryagency.bg — free, no auth, bulk download available
- Priority: High
