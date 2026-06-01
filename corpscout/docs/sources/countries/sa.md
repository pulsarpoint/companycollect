# Saudi Arabia (SA) — GDP Rank #18

> **Summary:** Saudi Arabia's Ministry of Commerce manages the Commercial Registration (CR) system but provides no public bulk API or data download. The Maroof platform offers business verification. Programmatic access requires commercial data providers or manual approaches.

## Official Registry

### Ministry of Commerce — Commercial Registration (السجل التجاري)
| Field      | Detail |
|------------|--------|
| URL        | https://mc.gov.sa |
| Access     | Web portal (no public API) |
| Cost       | Free search |
| Fields     | CR number (10-digit), company name (Arabic/English), legal form, owner/partners, capital, address, activity (ISIC code), registration date, status, expiry date |
| Auth       | None for basic CR number search |
| Rate limit | Undocumented |
| Notes      | CR number is the primary business identifier. Companies must renew CR annually. No bulk download or programmatic API. eServices portal at mc.gov.sa allows individual lookups. Unified National Platform (UAE-style portal) being developed. |

### Maroof Platform (معروف)
| Field      | Detail |
|------------|--------|
| URL        | https://maroof.sa |
| Access     | Web verification (no public API) |
| Cost       | Free |
| Fields     | Business name, CR number, verification status, business type, location |
| Auth       | None for verification check |
| Rate limit | N/A |
| Notes      | Government-backed e-commerce and SME trust verification platform. Useful for quick legitimacy checks but limited data depth. |

### Monsha'at — General Authority for Small and Medium Enterprises
| Field      | Detail |
|------------|--------|
| URL        | https://www.monshaat.gov.sa |
| Access     | Web portal (limited API) |
| Cost       | Free |
| Fields     | SME company name, CR number, sector, city, employee size (for registered SMEs) |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | Covers SMEs specifically. Some open data initiatives underway. Not a comprehensive registry. |

### Tadawul / Saudi Exchange (CMA)
| Field      | Detail |
|------------|--------|
| URL        | https://www.saudiexchange.sa |
| Access     | API (limited, free) |
| Cost       | Free |
| Fields     | Listed company name, ISIN, ticker, financials, disclosures, shareholder structure |
| Auth       | None for market data |
| Rate limit | None documented |
| Notes      | Covers ~200 Tadawul-listed companies. CMA (Capital Market Authority) mandates disclosures. Good for listed company data. |

## Commercial Providers

### IEC (International Expansion Consultants) / Dun & Bradstreet MENA
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, CR number, company name, address, employees, revenue, credit risk |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Saudi Arabia private-company coverage; better for listed/large companies. |

### Thomson Reuters / LSEG (Refinitiv)
| Field      | Detail |
|------------|--------|
| URL        | https://www.refinitiv.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | LEI, company name, CR number, financials, ownership, regulatory filings |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Best for listed company data and cross-border entity resolution. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited coverage for Saudi Arabia; data quality and completeness uncertain |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Tadawul-listed companies and financial institutions covered; overall adoption moderate |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Ministry of Commerce portal for individual CR lookups; Tadawul API for listed company data; no good free bulk option
- Priority: Medium
