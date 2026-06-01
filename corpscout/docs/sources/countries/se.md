# Sweden (SE) — GDP Rank #24

> **Summary:** Bolagsverket (Swedish Companies Registration Office) has an API but it is paid with no free tier. SCB (Statistics Sweden) offers free bulk company data with industry and geographic breakdowns. Org number format is XXXXXX-XXXX. The Allabolag.se web aggregator is widely used for free lookups.

## Official Registry

### Bolagsverket — Swedish Companies Registration Office
| Field      | Detail |
|------------|--------|
| URL        | https://bolagsverket.se/en/us/services-api |
| Access     | API (paid) |
| Cost       | Paid — subscription required; pricing contact via Bolagsverket |
| Fields     | Organisation number (format: XXXXXX-XXXX, 10 digits), company name, legal form, registered address (county), registration date, status (active/dissolved/bankrupt), board members, authorized signatories, share capital, articles of association reference, SNI industry code |
| Auth       | API key (contract required) |
| Rate limit | Per subscription |
| Notes      | Bolagsverket is the official authority for company registration. Organisation number is the universal Swedish identifier used across tax (Skatteverket), VAT, and bank KYC. No free tier. Developer documentation available but production access requires contract. A test environment may be available for evaluation. ~1.1M companies. |

### SCB — Statistics Sweden (Statistiska centralbyrån)
| Field      | Detail |
|------------|--------|
| URL        | https://www.scb.se/en/finding-statistics/statistics-by-subject-area/business-activities/ |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Organisation number, company name, SNI industry code, county, municipality, employee size bracket, legal form (aggregated/statistical) |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | SCB publishes company statistics including the Business Register (Företagsregistret) in aggregated form. Individual company data available via FOB (Företags- och Bebyggelseregistret) but some data requires licensing agreement. Free bulk datasets available for download on SCB open data portal. |

### Allabolag.se (unofficial aggregator — widely used)
| Field      | Detail |
|------------|--------|
| URL        | https://www.allabolag.se |
| Access     | Scrape / Web portal (no official API) |
| Cost       | Free |
| Fields     | Organisation number, company name, board members, annual reports, financial key figures, address, SNI codes |
| Auth       | None for basic search |
| Rate limit | Undocumented; scraping feasible with care |
| Notes      | Not official but widely used as a free company data source in Sweden. Sources from Bolagsverket + annual reports. Useful for prototype/enrichment; not for production compliance use. |

## Commercial Providers

### Bisnode Sweden (Dun & Bradstreet)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.se |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Org number, credit score, financials (annual accounts), directors, group structure, payment behaviour, compliance screening |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Dominant commercial credit bureau in Sweden. Bisnode is a D&B affiliate. Strong financial data including digitized annual report data. |

### Creditsafe Sweden
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/se |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Org number, credit score, financials, directors, shareholders, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good alternative to Bisnode; developer-friendly API. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Bolagsverket; no advantage over direct Bolagsverket API for paid users |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Nasdaq Stockholm listed companies and financial institutions well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Bolagsverket API (paid) for full data; SCB bulk for free statistical/directory data; Allabolag scrape for enrichment
- Priority: High
