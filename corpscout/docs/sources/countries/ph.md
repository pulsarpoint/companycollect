# Philippines (PH) — GDP Rank #37

> **Summary:** The SEC Philippines maintains the official company registry at sec.gov.ph with an online eFiling search portal; no bulk API is available. Some datasets are published on data.gov.ph. Commercial providers are limited for Philippines-specific data.

## Official Registry

### SEC Philippines (Securities and Exchange Commission)
| Field      | Detail |
|------------|--------|
| URL        | https://efiling.sec.gov.ph |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | company registration number, company name, type, status, registration date, principal address |
| Auth       | None for basic search |
| Rate limit | Moderate |
| Notes      | Search at efiling.sec.gov.ph/eFiling/home.html; no bulk export or API; full documents require paid extraction (~PHP 100–500 per report); registration number format varies by entity type |

### DTI (Department of Trade and Industry) — Business Name Registry
| Field      | Detail |
|------------|--------|
| URL        | https://bnrs.dti.gov.ph |
| Access     | Portal search |
| Cost       | Free |
| Fields     | business name, registration number, status, owner name |
| Auth       | None |
| Rate limit | Low |
| Notes      | For sole proprietorships and trade names; separate from SEC corporate registry |

### data.gov.ph
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.ph |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | varies by dataset; may include company name, registration number, sector |
| Auth       | None |
| Rate limit | None |
| Notes      | Limited company datasets; not comprehensive; check for SEC-published open datasets |

## Commercial Providers

### Dun & Bradstreet Philippines
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Philippines coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Philippines coverage |

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
- Recommended source: SEC eFiling portal scrape for corporate entities; DTI BNRS for sole proprietorships
- Priority: Low
