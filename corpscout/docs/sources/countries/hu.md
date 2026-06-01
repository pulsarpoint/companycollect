# Hungary (HU) — GDP Rank #54

> **Summary:** e-Cégjegyzék provides free access to the Hungarian Company Register via a web portal; open data downloads are available via opten.hu. The cégjegyzékszám (court company number) is the primary identifier. Good free access.

## Official Registry

### e-Cégjegyzék (Electronic Company Register)
| Field      | Detail |
|------------|--------|
| URL        | https://e-cegjegyzek.hu |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | cégjegyzékszám (company register number), company name, legal form, status, registration date, registered address, tax number (adószám), directors |
| Auth       | None |
| Rate limit | Moderate |
| Notes      | Free company register portal; search by name or number; no formal API; scraping feasible; full court documents require paid extraction |

### Court Company Register (Cégbíróság)
| Field      | Detail |
|------------|--------|
| URL        | https://birosag.hu/ceg-es-ngo-informaciok |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, company register number, status, court, address |
| Auth       | None |
| Rate limit | Low |
| Notes      | Court registry portal; complementary to e-Cégjegyzék |

## Commercial Providers

### OPTEN Kft.
| Field      | Detail |
|------------|--------|
| URL        | https://www.opten.hu |
| Access     | API (paid) / Bulk download |
| Cost       | Paid — contact for pricing |
| Fields     | company number, name, tax number, address, financial data, directors, ownership structure, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Leading Hungarian commercial data aggregator; also publishes some open data; comprehensive coverage |

### Creditreform Hungary
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.hu |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, number, tax ID, address, financial data, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of Creditreform network; good coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Hungary coverage; data from e-Cégjegyzék |

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
- Recommended source: e-Cégjegyzék scrape for basic data; OPTEN API for enrichment
- Priority: High
