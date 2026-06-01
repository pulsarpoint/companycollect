# Albania (AL) — GDP Rank #92

> **Summary:** QKB (Qendra Kombëtare e Biznesit / National Business Center) provides a free API at qkb.gov.al with NIPT (business tax ID) as the identifier. Good free programmatic access for a small Balkan country.

## Official Registry

### QKB (Qendra Kombëtare e Biznesit — National Business Center)
| Field      | Detail |
|------------|--------|
| URL        | https://www.qkb.gov.al |
| Access     | API (free) / Portal search |
| Cost       | Free |
| Fields     | NIPT (Numri i Identifikimit për Personin e Tatueshëm — taxpayer ID), company name, legal form, status, registration date, address, activity, capital |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Free API and search portal; covers all registered companies; also accessible at open.data.al; REST API available; NIPT is both the tax number and business identifier |

### Open Data Albania
| Field      | Detail |
|------------|--------|
| URL        | https://open.data.al |
| Access     | Bulk download / API (free) |
| Cost       | Free |
| Fields     | NIPT, company name, legal form, status, registration date, address |
| Auth       | None |
| Rate limit | None |
| Notes      | Open data portal publishing QKB data; downloadable datasets |

## Commercial Providers

### Dun & Bradstreet (Balkans coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, NIPT, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Albania coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Albania coverage; data from QKB |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Albania LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: QKB free API at qkb.gov.al — free, no auth, good coverage
- Priority: High
