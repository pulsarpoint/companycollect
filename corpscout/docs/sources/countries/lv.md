# Latvia (LV) — GDP Rank #88

> **Summary:** UR (Uzņēmumu reģistrs / Enterprise Register) at ur.gov.lv provides free search and downloadable open data with registration number as the identifier. Lursoft is the leading commercial provider with paid API. Good free access overall.

## Official Registry

### UR (Uzņēmumu reģistrs — Enterprise Register of Latvia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ur.gov.lv |
| Access     | Portal search / Bulk download |
| Cost       | Free |
| Fields     | registration number, company name, legal form, status, registration date, registered address, shareholders, directors |
| Auth       | None |
| Rate limit | None |
| Notes      | Free search at ur.gov.lv; open data downloadable at data.gov.lv; covers all registered companies; PDF company extracts free for basic info; API not formally documented but data portal provides bulk access |

### data.gov.lv — UR Open Data
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.lv |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | registration number, name, legal form, status, registration date, address |
| Auth       | None |
| Rate limit | None |
| Notes      | Open government data including UR datasets; periodic updates |

## Commercial Providers

### Lursoft
| Field      | Detail |
|------------|--------|
| URL        | https://www.lursoft.lv |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | registration number, name, legal form, address, directors, shareholders, financial data, credit score, history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Leading Latvian business information provider; best coverage for financial enrichment; also sells bulk data |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Latvia coverage; data from UR |

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
- Recommended source: UR open data download for bulk; Lursoft API for enrichment
- Priority: High
