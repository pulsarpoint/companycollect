# Singapore (SG) — GDP Rank #32

> **Summary:** ACRA's BizFile+ is the authoritative registry; a paid API is available through data.gov.sg. Free basic entity search is available at bizfile.gov.sg. Data.gov.sg offers some free company datasets. Strong data quality and good coverage.

## Official Registry

### ACRA BizFile+ (Accounting and Corporate Regulatory Authority)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bizfile.gov.sg |
| Access     | API (paid) / Free portal search |
| Cost       | Free basic search; paid API via data.gov.sg |
| Fields     | UEN (Unique Entity Number), entity name, entity type, status, registration date, primary SSIC activity, address |
| Auth       | API key (for paid API) |
| Rate limit | Per contract (paid API) |
| Notes      | Free search at bizfile.gov.sg returns basic fields per entity; full company profile (directors, shareholders, charges) requires paid purchase per report (~SGD 5–11); ACRA API at developer.data.gov.sg requires subscription |

### Data.gov.sg — ACRA Datasets
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.sg/collections/18/view |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | UEN, entity name, entity type, status, primary SSIC, registration date, postal code |
| Auth       | None |
| Rate limit | None |
| Notes      | Static dataset releases; updated periodically (not real-time); good for bulk lookup of UEN/name/status |

## Commercial Providers

### Dun & Bradstreet Singapore
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, UEN, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Singapore coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Singapore coverage sourced from ACRA data |

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
- Recommended source: data.gov.sg bulk download for initial load; ACRA paid API for real-time lookups
- Priority: High
