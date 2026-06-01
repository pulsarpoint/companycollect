# Bahrain (BH) — GDP Rank #85

> **Summary:** MOICT's Sijilat platform at sijilat.bh provides the national business registry with CR (Commercial Registration) number as the identifier. The Bahrain eGovernment portal offers some company lookup. No public bulk API.

## Official Registry

### Sijilat (MOICT — Ministry of Industry, Commerce and Tourism)
| Field      | Detail |
|------------|--------|
| URL        | https://www.sijilat.bh |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CR number (Commercial Registration), company name, legal form, status, activity, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | National business registry platform; no bulk export; no API; some services require Bahrain national ID or business login; integration with Bahrain ID (Eid) |

### Bahrain eGovernment Portal
| Field      | Detail |
|------------|--------|
| URL        | https://www.bahrain.bh |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, CR number, status |
| Auth       | Bahrain ID for some services |
| Rate limit | Low |
| Notes      | Government portal aggregating multiple ministry services; company search available |

## Commercial Providers

### Dun & Bradstreet (GCC coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, CR number, address, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Bahrain coverage; more relevant for financial sector (Bahrain is a regional banking hub) |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Bahrain coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; banking sector entities have LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Sijilat portal scrape for basic lookups
- Priority: Low
