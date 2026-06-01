# Netherlands (NL) — GDP Rank #17

> **Summary:** KVK (Kamer van Koophandel) has a well-documented REST API but it is fully paid — there is no free tier. Basic name search is free on the website. ~2.5M registrations. KVK number is the key 8-digit identifier. For free programmatic access, OpenCorporates is the only realistic option.

## Official Registry

### KVK — Kamer van Koophandel (Chamber of Commerce)
| Field      | Detail |
|------------|--------|
| URL        | https://developers.kvk.nl |
| Access     | API (paid) |
| Cost       | Paid — subscription required; pricing at https://developers.kvk.nl/products (verify URL) |
| Fields     | KVK number (8-digit), company name, trade names, legal form, address, SBI activity codes, date of incorporation, status (active/dissolved), annual report, branch (vestiging) details, directors/authorized signatories |
| Auth       | API key (subscription required) |
| Rate limit | Per subscription tier |
| Notes      | ~2.5M registrations including branches. KVK number is distinct for each entity; vestigingsnummer (12-digit) identifies each business location. The basic search endpoint has a free trial/sandbox. The Search v2 API and Profile APIs are production-paid. Data quality is excellent. Netherlands registers both HQ companies and EU branches. |

### KVK Handelsregister (Web Search)
| Field      | Detail |
|------------|--------|
| URL        | https://www.kvk.nl/zoeken/?source=handelsregister |
| Access     | Scrape (free web search) |
| Cost       | Free (web UI) |
| Fields     | KVK number, company name, city, legal form, activity |
| Auth       | None |
| Rate limit | Undocumented; bot protection present |
| Notes      | Free web search for individual lookups; no bulk access. Official paid documents (uittreksel) cost EUR 14.95. |

## Commercial Providers

### Creditsafe Netherlands
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/nl |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | KVK number, credit score, financials, directors, group structure, payment behaviour, legal events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Strong for credit risk. Uses KVK data as base and enriches with financial/credit data. |

### Bureau van Dijk (Orbis / Amadeus)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | KVK, financials, shareholders, group structure, M&A history, global cross-referencing |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Best for corporate group/family tree analysis. Orbis covers 400M+ companies globally. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from KVK; may be the most cost-effective option for basic NL company data without a KVK subscription |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | AEX/AMX-listed companies well covered; Netherlands has strong LEI adoption in financial sector |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: KVK API (paid subscription) for full data; OpenCorporates for basic free coverage
- Priority: High
