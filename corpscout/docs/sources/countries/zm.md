# Zambia (ZM) — GDP Rank #94

> **Summary:** PACRA (Patents and Companies Registration Agency) at pacra.org.zm manages company registrations; no public bulk API or download is available. Online search portal provides basic lookup capability.

## Official Registry

### PACRA (Patents and Companies Registration Agency)
| Field      | Detail |
|------------|--------|
| URL        | https://www.pacra.org.zm |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; company certificates require paid extraction; e-services available for registration and searches |

## Commercial Providers

### Dun & Bradstreet (Southern Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Zambia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Zambia coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Zambia LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: PACRA portal scrape for basic lookups
- Priority: Low
