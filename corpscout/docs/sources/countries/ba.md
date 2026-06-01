# Bosnia & Herzegovina (BA) — GDP Rank #100

> **Summary:** Bosnia has two entity-level registries: APIF (FBiH — Federation) and JPRS (RS — Republika Srpska), reflecting the country's divided political structure. JIB (ID number) is the common identifier. APIF has some open data available. Partial free access.

## Official Registry

### APIF (Agencija za posredničke, informatičke i finansijske usluge) — FBiH Entity
| Field      | Detail |
|------------|--------|
| URL        | https://www.apif.gov.ba |
| Access     | Bulk download / Portal search |
| Cost       | Free |
| Fields     | JIB (Jedinstveni identifikacijski broj — Unique ID number), company name, legal form, status, registration date, address, municipality |
| Auth       | None |
| Rate limit | None |
| Notes      | APIF publishes open data for the Federation of BiH entity; bulk download available; covers Federation companies; also provides financial data; portal at sud-registar.ba |

### JPRS (Jedinstveni sistem registracije, kontrole i nadzora poreskih obveznika) — RS Entity
| Field      | Detail |
|------------|--------|
| URL        | https://www.rjrs.org |
| Access     | Portal search |
| Cost       | Free |
| Fields     | JIB, company name, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Republika Srpska entity company register; separate from APIF; similar JIB identifier; no bulk API |

### Brčko District — Company Registry
| Field      | Detail |
|------------|--------|
| URL        | https://www.sud.brckodistrikta.gov.ba |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, status |
| Auth       | None |
| Rate limit | Low |
| Notes      | Brčko District has its own separate company register; covers entities registered in Brčko |

## Commercial Providers

### Dun & Bradstreet (Balkans)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, JIB, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Bosnia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Bosnia coverage; primarily Federation data |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Bosnia LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: APIF open data bulk download for FBiH entity; JPRS portal for RS entity
- Priority: Medium
