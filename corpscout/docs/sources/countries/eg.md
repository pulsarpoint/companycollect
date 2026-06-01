# Egypt (EG) — GDP Rank #35

> **Summary:** GAFI (General Authority for Investment and Free Zones) manages the commercial registry; there is no public API or bulk data download. Access is limited to portal search. Commercial providers are the best programmatic path.

## Official Registry

### GAFI — General Authority for Investment and Free Zones
| Field      | Detail |
|------------|--------|
| URL        | https://www.gafi.gov.eg |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, commercial registration number, legal form, activity, status |
| Auth       | None |
| Rate limit | Low |
| Notes      | No public API or bulk export; portal search only; some services require Egypt ID or business login; Arabic-language primary interface |

### Commercial Registry (Sijil Tijari) — Ministry of Trade and Industry
| Field      | Detail |
|------------|--------|
| URL        | https://www.mti.gov.eg |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, registration date, address |
| Auth       | None |
| Rate limit | Low |
| Notes      | Separate from GAFI for non-investment entities; limited online search capability |

## Commercial Providers

### Dun & Bradstreet Egypt
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, directors, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Egypt coverage; better for large enterprises |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Very limited Egypt coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; limited Egypt coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: GAFI portal scrape for basic lookups; D&B for enrichment
- Priority: Low
