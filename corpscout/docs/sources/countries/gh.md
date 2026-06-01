# Ghana (GH) — GDP Rank #69

> **Summary:** ORC (Office of the Registrar of Companies) at orc.gov.gh manages company registrations; no public bulk API or download is available. Portal search is the main access path. Limited programmatic access.

## Official Registry

### ORC (Office of the Registrar of Companies)
| Field      | Detail |
|------------|--------|
| URL        | https://www.orc.gov.gh |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; company certificates require paid extraction; registration also via GHIPSS RegistrationGhana platform |

### GRA (Ghana Revenue Authority) — TIN Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.gra.gov.gh |
| Access     | Portal search |
| Cost       | Free |
| Fields     | TIN (Tax Identification Number), taxpayer name, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax identification number lookup; useful for cross-referencing |

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
| Notes      | Very limited Ghana coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Ghana coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Ghana LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: ORC portal scrape for basic lookups
- Priority: Low
