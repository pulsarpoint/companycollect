# Jordan (JO) — GDP Rank #82

> **Summary:** The Companies Control Department under the Ministry of Industry, Trade and Supply at mit.gov.jo manages company registrations; no public API exists. Portal search provides basic lookup capability.

## Official Registry

### Companies Control Department (CCD) — MIT
| Field      | Detail |
|------------|--------|
| URL        | https://www.mit.gov.jo |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Portal at ccd.gov.jo (Companies Control Department); no bulk export; no API; full company documents require paid extraction; Jordan Company Data available via eCCD portal |

### eCCD Portal (Companies Control Department)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ccd.gov.jo |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, legal form, status, capital |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Dedicated companies registry portal; limited online services |

## Commercial Providers

### Dun & Bradstreet (Levant coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Jordan coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Jordan coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Jordan coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: CCD portal scrape for basic lookups
- Priority: Low
