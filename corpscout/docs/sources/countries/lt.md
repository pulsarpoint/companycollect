# Lithuania (LT) — GDP Rank #70

> **Summary:** Registrų centras (Register of Legal Entities) provides a free API and open data at registrucentras.lt with company code (juridinio asmens kodas) as the identifier. Excellent free programmatic access — among the best in the Baltic states.

## Official Registry

### Registrų centras — Juridinių asmenų registras
| Field      | Detail |
|------------|--------|
| URL        | https://www.registrucentras.lt/jar/ |
| Access     | API (free) / Bulk download |
| Cost       | Free |
| Fields     | company code (juridinio asmens kodas), company name, legal form, status, registration date, address, activity (EVRK code), authorized capital |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Free open data API and downloadable datasets; covers all registered legal entities; REST API documentation at registrucentras.lt; data also on open data portal |

### Rekvizitai.lt
| Field      | Detail |
|------------|--------|
| URL        | https://www.rekvizitai.lt |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | company code, name, status, address, directors, VAT number, financial data |
| Auth       | None |
| Rate limit | Moderate |
| Notes      | Third-party aggregator built on Registrų centras data; good coverage including financial data |

## Commercial Providers

### Creditreform Lithuania
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.lt |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, company code, address, financial data, credit score, payment history |
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
| Notes  | Good Lithuania coverage; data from Registrų centras |

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
- Recommended source: Registrų centras free API at registrucentras.lt — free, open data, full coverage
- Priority: High
