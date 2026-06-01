# Slovakia (SK) — GDP Rank #57

> **Summary:** The Slovak Business Register (Obchodný register) at orsr.sk is free and searchable; open data downloads are available at justice.gov.sk. IČO is the primary company identifier. Good free access for a Central European country.

## Official Registry

### Obchodný register SR (Slovak Business Register)
| Field      | Detail |
|------------|--------|
| URL        | https://www.orsr.sk |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | IČO (Identifikačné číslo organizácie), company name, legal form, status, registration date, registered address, court, directors |
| Auth       | None |
| Rate limit | Low to moderate |
| Notes      | Full company details available free online; no formal API but scraping is feasible; register maintained by Ministry of Justice |

### Justice.gov.sk — Open Data Download
| Field      | Detail |
|------------|--------|
| URL        | https://www.justice.gov.sk/sluzby/obchodny-register/ |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | IČO, company name, legal form, status, registration date, address, directors |
| Auth       | None |
| Rate limit | None |
| Notes      | Downloadable XML/CSV datasets of the business register; updated periodically; good for bulk load |

## Commercial Providers

### Creditreform Slovakia
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.sk |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, IČO, address, financial data, credit score, payment history |
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
| Notes  | Good Slovakia coverage; data from orsr.sk |

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
- Recommended source: justice.gov.sk bulk download for initial load; orsr.sk scrape for lookups
- Priority: High
