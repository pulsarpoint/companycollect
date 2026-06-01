# Azerbaijan (AZ) — GDP Rank #76

> **Summary:** The Ministry of Economy e-register at e-gov.az uses VÖEN (tax ID) as the primary business identifier; no public bulk API or comprehensive online search portal is available for company data. Limited digital infrastructure.

## Official Registry

### e-Government Portal — Business Registry
| Field      | Detail |
|------------|--------|
| URL        | https://www.e-gov.az |
| Access     | Portal search (limited) |
| Cost       | Free |
| Fields     | VÖEN (Vergi ödəyicisinin eyniləşdirmə nömrəsi — taxpayer identification number), company name, status |
| Auth       | AzərGold digital certificate for some services |
| Rate limit | Low |
| Notes      | Limited online search capability; most registration processes require in-person or notary processes; no bulk export; no API |

### taxes.gov.az (State Tax Service)
| Field      | Detail |
|------------|--------|
| URL        | https://www.taxes.gov.az |
| Access     | Portal search |
| Cost       | Free |
| Fields     | VÖEN, taxpayer name, type, status |
| Auth       | None for basic VÖEN lookup |
| Rate limit | Low |
| Notes      | Tax service VÖEN verification; limited business information |

## Commercial Providers

### Dun & Bradstreet (CIS/Caucasus)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, VÖEN, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Azerbaijan coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Azerbaijan coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Azerbaijan LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: taxes.gov.az for VÖEN verification; very limited programmatic options
- Priority: Low
