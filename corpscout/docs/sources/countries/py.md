# Paraguay (PY) — GDP Rank #87

> **Summary:** The Ministry of Industry and Commerce (MIC) handles company registration and SET (Subsecretaría de Estado de Tributación) manages RUC identification; no public API or bulk download is available.

## Official Registry

### MIC (Ministerio de Industria y Comercio) — DNIT
| Field      | Detail |
|------------|--------|
| URL        | https://www.mic.gov.py |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Commercial registry; no bulk export; no API; DNIT (Dirección Nacional de la Industria y el Trabajo) handles local registration |

### SET (Subsecretaría de Estado de Tributación) — RUC Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.set.gov.py |
| Access     | Portal search |
| Cost       | Free |
| Fields     | RUC (Registro Único de Contribuyentes), taxpayer name, type, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority RUC verification; RUC is the primary business identifier; searchable at set.gov.py |

## Commercial Providers

### Dun & Bradstreet (Southern Cone)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RUC, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Paraguay coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Paraguay coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Paraguay LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SET RUC portal for basic lookups
- Priority: Low
