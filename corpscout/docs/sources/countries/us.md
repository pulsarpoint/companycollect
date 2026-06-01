# United States (US) — GDP Rank #1

> **Summary:** No single national business registry exists; federal SEC EDGAR covers public companies via free API, while each state maintains its own registry. For broad coverage, commercial providers like D&B or OpenCorporates are the most practical aggregation layer.

## Official Registry

### SEC EDGAR (Public Companies)
| Field      | Detail |
|------------|--------|
| URL        | https://efts.sec.gov/LATEST/search-index?q=%22company%22&dateRange=custom&startdt=2020-01-01&enddt=2020-12-31 / https://data.sec.gov/submissions/ |
| Access     | API (free) |
| Cost       | Free |
| Fields     | CIK, company name, SIC code, state of incorporation, EIN, filing type, fiscal year end |
| Auth       | None required; User-Agent header strongly recommended (SEC policy) |
| Rate limit | 10 requests/second per IP; requests must include User-Agent with contact email |
| Notes      | Public/listed companies only (~12,000 active filers). Full submissions JSON at data.sec.gov/submissions/{CIK}.json. EDGAR full-text search also available. Not useful for private companies. |

### Delaware Division of Corporations
| Field      | Detail |
|------------|--------|
| URL        | https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx |
| Access     | Scrape (no public API) |
| Cost       | Free (search), paid (certified documents) |
| Fields     | Entity name, file number, entity type, incorporation date, registered agent, status |
| Auth       | None for basic search |
| Rate limit | Undocumented; anti-scraping measures present |
| Notes      | Delaware is home to ~60% of Fortune 500 companies. No bulk download. File number is the key identifier. API access exists only via third-party vendors. |

### California Secretary of State (bizfile Online)
| Field      | Detail |
|------------|--------|
| URL        | https://bizfileonline.sos.ca.gov/search/business |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Entity name, entity number, type, status, formation date, agent of service, address |
| Auth       | None for bulk download |
| Rate limit | N/A (bulk) |
| Notes      | Bulk download available at https://bizfileonline.sos.ca.gov/api/Records/businesssearch (CSV). ~5M+ records. Updated periodically. Other large state registries (TX, NY, FL) have varying access methods. |

## Commercial Providers

### Dun & Bradstreet (D&B)
| Field      | Detail |
|------------|--------|
| URL        | https://developer.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing; developer sandbox available |
| Fields     | DUNS number, legal name, trade style, address, phone, SIC/NAICS, employee count, revenue, corporate family tree, credit risk indicators |
| Auth       | API key (OAuth 2.0) |
| Rate limit | Per contract |
| Notes      | DUNS is the de facto US business identifier. Most comprehensive private-company coverage. Data Exchange program offers reciprocal data sharing. |

### LexisNexis / Accurint
| Field      | Detail |
|------------|--------|
| URL        | https://risk.lexisnexis.com/products/accurint |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Business name, address, phone, principals, liens, UCC filings, court records |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Strong for compliance and KYB use cases. Aggregates state registry + public records. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Aggregates all 50 US state registries; useful for national coverage without direct state integrations |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; good complement to EDGAR for international entities operating in the US |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SEC EDGAR for public companies (free); OpenCorporates or D&B for private company coverage
- Priority: High
