# Armenia (AM) — GDP Rank #96

> **Summary:** Armenia's e-register at e-register.am provides a free API with HSTIN (state registration number) as the identifier. Good free programmatic access for a small Caucasus country.

## Official Registry

### e-Register (State Register of Legal Entities)
| Field      | Detail |
|------------|--------|
| URL        | https://www.e-register.am |
| Access     | API (free) / Portal search |
| Cost       | Free |
| Fields     | HSTIN (հ.գ.փ.հ — state registration number), company name, legal form, status, registration date, address, directors, shareholders |
| Auth       | None for basic search |
| Rate limit | None documented |
| Notes      | Free search portal and API; covers all registered legal entities; Armenian and Russian language interfaces; API available at e-register.am; good coverage |

## Commercial Providers

### Dun & Bradstreet (Caucasus)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, HSTIN, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Armenia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Armenia coverage; data from e-register.am |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Armenia LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: e-register.am free API — free, no auth, good coverage
- Priority: High
