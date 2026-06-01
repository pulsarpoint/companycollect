# Algeria (DZ) — GDP Rank #53

> **Summary:** CNRC (Centre National du Registre de Commerce) manages commercial registrations with NRC number as the identifier; online search is limited and no public API or bulk data is available. Very restricted programmatic access.

## Official Registry

### CNRC (Centre National du Registre de Commerce)
| Field      | Detail |
|------------|--------|
| URL        | https://www.cnrc.org.dz |
| Access     | Portal search (limited) |
| Cost       | Free |
| Fields     | NRC (Numéro du Registre de Commerce), company name, legal form, activity, address, wilaya (province) |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal at cnrc.org.dz; limited functionality; no bulk export; no API; company extracts require in-person or notary process; Arabic and French languages |

## Commercial Providers

### Dun & Bradstreet (MENA coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Algeria coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Algeria coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Algeria coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: CNRC portal scrape for basic lookups; very limited options overall
- Priority: Low
