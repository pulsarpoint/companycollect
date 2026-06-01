# Israel (IL) — GDP Rank #28

> **Summary:** The Companies Registrar under the Ministry of Justice offers a web search portal; there is no public bulk API, so commercial providers (Creditsafe, D&B Israel) are the best path for programmatic access. OpenCorporates has partial coverage.

## Official Registry

### Companies Registrar (Rasham HaHavarot)
| Field      | Detail |
|------------|--------|
| URL        | https://ica.justice.gov.il/GenericCorporarionInfo/SearchCorporation |
| Access     | Scrape |
| Cost       | Free |
| Fields     | company name, registration number, status, registered address, incorporation date |
| Auth       | None |
| Rate limit | Moderate (anti-scrape measures) |
| Notes      | Hebrew-language portal; search by company name or number; no bulk export; results per query only |

## Commercial Providers

### Creditsafe Israel
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/gb/en/solutions/our-data/global-data/israel.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | company name, number, status, address, directors, financials, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good coverage; integrates well with Creditsafe global API |

### Dun & Bradstreet Israel
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S number, address, sector, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | D&B number assigned; D-U-N-S linkage available |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Partial coverage; data sourced from Rasham HaHavarot scrapes |

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
- Recommended source: Official registry scrape for basic lookup; Creditsafe API for enrichment
- Priority: Medium
