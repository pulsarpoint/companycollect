# Uganda (UG) — GDP Rank #78

> **Summary:** URSB (Uganda Registration Services Bureau) at ursb.go.ug manages company registrations; no public bulk API or download is available. Online portal provides search capability with limited fields.

## Official Registry

### URSB (Uganda Registration Services Bureau)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ursb.go.ug |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; company certificates require paid extraction; e-services at ereg.ursb.go.ug |

### e-Registration Portal (URSB)
| Field      | Detail |
|------------|--------|
| URL        | https://ereg.ursb.go.ug |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, status |
| Auth       | None for search |
| Rate limit | Low |
| Notes      | Digital registration portal; search available without login |

## Commercial Providers

### Dun & Bradstreet (East Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Uganda coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Uganda coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Uganda LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: URSB e-registration portal for basic lookups
- Priority: Low
