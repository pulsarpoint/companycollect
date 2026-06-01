# Qatar (QA) — GDP Rank #55

> **Summary:** MOCI (Ministry of Commerce and Industry) manages commercial registrations with CR (Commercial Registration) number as the identifier; no public bulk API is available. The Invest Qatar portal provides some company information. Commercial providers are the best programmatic path.

## Official Registry

### MOCI (Ministry of Commerce and Industry)
| Field      | Detail |
|------------|--------|
| URL        | https://www.moci.gov.qa |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CR number (Commercial Registration number), company name, legal form, status, activity |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Portal at moci.gov.qa; no bulk export; no API; services also available through Hukoomi (Qatar Government Portal) |

### Invest Qatar (formerly QFC)
| Field      | Detail |
|------------|--------|
| URL        | https://investqatar.qa |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, CR number, sector, activity |
| Auth       | None |
| Rate limit | Low |
| Notes      | Covers QFC (Qatar Financial Centre) and investment-focused entities; separate from MOCI registry |

## Commercial Providers

### Dun & Bradstreet (GCC coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, CR number, address, sector, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Qatar coverage; D&B GCC desk handles Gulf data |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Qatar coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; financial sector entities often have LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MOCI portal scrape for basic lookups; D&B for enrichment
- Priority: Low
