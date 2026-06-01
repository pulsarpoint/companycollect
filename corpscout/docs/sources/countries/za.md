# South Africa (ZA) — GDP Rank #34

> **Summary:** CIPC provides a free bulk CSV download at opendata.cipc.co.za covering ~2M companies; this is the best path for bulk data. The registration number is the primary identifier. Good free coverage for a developing-market registry.

## Official Registry

### CIPC (Companies and Intellectual Property Commission)
| Field      | Detail |
|------------|--------|
| URL        | https://opendata.cipc.co.za |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | registration number, company name, company type, status, registration date, province |
| Auth       | None |
| Rate limit | None |
| Notes      | Free CSV bulk download; ~2M companies; updated periodically; full company details (directors, financials) require paid CIPC e-Services; also at cipc.co.za for individual lookups |

### CIPC e-Services (Individual Lookups)
| Field      | Detail |
|------------|--------|
| URL        | https://eservices.cipc.co.za |
| Access     | Portal search |
| Cost       | Free basic search; paid for full reports |
| Fields     | registration number, name, status, registered address, directors |
| Auth       | Registration required |
| Rate limit | Low |
| Notes      | Director details and full company reports require paid extraction; registration is free |

## Commercial Providers

### Dun & Bradstreet South Africa
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, registration number, address, directors, financials, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good South Africa coverage |

### TransUnion South Africa
| Field      | Detail |
|------------|--------|
| URL        | https://www.transunion.co.za/business |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, number, status, credit score, directors, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Strong local credit data; good for SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good South Africa coverage; data sourced from CIPC open data |

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
- Recommended source: CIPC opendata.cipc.co.za bulk CSV download — free, ~2M companies
- Priority: High
