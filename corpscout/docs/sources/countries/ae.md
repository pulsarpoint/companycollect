# United Arab Emirates (AE) — GDP Rank #30

> **Summary:** The UAE has no unified national business registry; each emirate operates its own department (Dubai DED, Abu Dhabi ADDED, Sharjah SEDD, etc.). Best path is emirate-specific portals or a commercial data aggregator with UAE coverage.

## Official Registry

### Dubai Department of Economy and Tourism (DET/DED)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dubaided.gov.ae |
| Access     | Scrape / Portal search |
| Cost       | Free (basic lookup) |
| Fields     | trade licence number, company name, activity, status, expiry date |
| Auth       | None for basic search |
| Rate limit | Moderate |
| Notes      | eServices portal at services.ded.ae; no bulk export; Dubai-licensed entities only |

### Abu Dhabi Department of Economic Development (ADDED)
| Field      | Detail |
|------------|--------|
| URL        | https://www.added.gov.ae |
| Access     | Portal search |
| Cost       | Free |
| Fields     | trade licence number, company name, activity, status |
| Auth       | UAE Pass or portal account |
| Rate limit | Low (manual portal) |
| Notes      | Abu Dhabi entities only; no API |

### Ministry of Economy — Commercial Register
| Field      | Detail |
|------------|--------|
| URL        | https://www.economy.gov.ae |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, commercial registration number, legal form |
| Auth       | Registration required for some features |
| Rate limit | Low |
| Notes      | Federal-level registration for free-zone and federal entities; no bulk API |

## Commercial Providers

### Dun & Bradstreet UAE
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S number, address, directors, activity, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Best programmatic coverage for UAE; spans multiple emirates |

### Bureau van Dijk (Orbis)
| Field      | Detail |
|------------|--------|
| URL        | https://orbis.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, registration number, shareholders, financials, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good for larger UAE entities; limited SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited UAE coverage; partial data from Dubai and Abu Dhabi sources |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; free-zone entities often have LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: D&B or Bureau van Dijk for programmatic access; DED portal scrape for Dubai entities
- Priority: Medium
