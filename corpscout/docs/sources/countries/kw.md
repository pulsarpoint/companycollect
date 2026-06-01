# Kuwait (KW) — GDP Rank #60

> **Summary:** The Ministry of Commerce and Industry (MOCI) at moci.gov.kw manages commercial registrations with CR (Commercial Registration) number as the identifier; no public API or bulk data. eGovernment portal offers limited lookup services.

## Official Registry

### MOCI (Ministry of Commerce and Industry Kuwait)
| Field      | Detail |
|------------|--------|
| URL        | https://www.moci.gov.kw |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CR number (Commercial Registration), company name, legal form, status, activity |
| Auth       | Civil ID or eGovernment account for some services |
| Rate limit | Low |
| Notes      | No public API; no bulk export; company documents require in-person or paid extraction; some services available via Sahel app (government services app) |

### eGovernment Kuwait — PACI
| Field      | Detail |
|------------|--------|
| URL        | https://www.e.gov.kw |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, CR number, status |
| Auth       | Civil ID for some services |
| Rate limit | Low |
| Notes      | Provides access to various MOCI services; limited company data accessible without authentication |

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
| Notes      | Limited Kuwait coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Kuwait coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; Kuwait Finance House and listed entities have LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MOCI portal for basic lookups; D&B for enrichment
- Priority: Low
