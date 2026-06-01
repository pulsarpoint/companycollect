# Tunisia (TN) — GDP Rank #81

> **Summary:** The RNE (Registre National des Entreprises) at rne.tn manages all business registrations with MF (matricule fiscale) as the identifier; no public API is available. Portal search is the main free access path.

## Official Registry

### RNE (Registre National des Entreprises)
| Field      | Detail |
|------------|--------|
| URL        | https://www.registre-entreprises.tn |
| Access     | Portal search |
| Cost       | Free |
| Fields     | MF (matricule fiscale — fiscal registration number), company name, legal form, status, registration date, address, activity (NAT code) |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Unified national enterprise register; no bulk export; no API; supersedes previous RNE at rne.tn; launched 2021; Arabic and French interfaces |

### APII (Agence de Promotion de l'Industrie et de l'Innovation)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tunisieindustrie.nat.tn |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, sector, region |
| Auth       | None |
| Rate limit | Low |
| Notes      | Industrial company directory; limited coverage; focused on manufacturing sector |

## Commercial Providers

### Dun & Bradstreet (North Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, MF, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Tunisia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Tunisia coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Tunisia coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: RNE portal scrape for basic lookups
- Priority: Low
