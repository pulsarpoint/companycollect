# Tanzania (TZ) — GDP Rank #64

> **Summary:** BRELA (Business Registrations and Licensing Agency) at brela.go.tz manages company registrations; no public API or bulk download is available. Online search portal is the main access path.

## Official Registry

### BRELA (Business Registrations and Licensing Agency)
| Field      | Detail |
|------------|--------|
| URL        | https://www.brela.go.tz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, company name, legal form, status, registration date, registered office |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; full company certificates require paid extraction; registration also handled via Tanzania Business Registration and Licensing Agency (e-registration) |

## Commercial Providers

### Dun & Bradstreet (Sub-Saharan Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Tanzania coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Tanzania coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Tanzania LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: BRELA portal scrape for basic lookups; very limited options
- Priority: Low
