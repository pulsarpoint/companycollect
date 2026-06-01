# Chile (CL) — GDP Rank #45

> **Summary:** Chile has two main systems: SII (tax authority) for RUT/RUC lookups and RES (Registro de Empresas y Sociedades) for company registration. Limited free programmatic access; SII provides some public RUT verification but no bulk API.

## Official Registry

### RES (Registro de Empresas y Sociedades) — Ministerio de Economía
| Field      | Detail |
|------------|--------|
| URL        | https://www.emprendeenlinea.cl |
| Access     | Portal search |
| Cost       | Free |
| Fields     | RUT (Rol Único Tributario), company name, legal form, status, registration date, address |
| Auth       | ClaveÚnica (government ID) for some features |
| Rate limit | Low |
| Notes      | Online business registration portal; basic company search available; no bulk export or API; also at registroempresas.cl |

### SII (Servicio de Impuestos Internos) — RUT Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.sii.cl |
| Access     | Portal search / Limited scrape |
| Cost       | Free |
| Fields     | RUT, legal name, tax situation, activity code |
| Auth       | None for basic RUT verification |
| Rate limit | Moderate |
| Notes      | RUT is both tax ID and business identifier in Chile; verification portal at sii.cl; no bulk export; contribuyente activity data partially accessible |

## Commercial Providers

### Dun & Bradstreet Chile
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RUT, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Moderate Chile coverage |

### Equifax Chile (Dicom)
| Field      | Detail |
|------------|--------|
| URL        | https://www.equifax.cl |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, RUT, credit score, payment history, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local credit bureau; strong business credit data coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Chile coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SII portal for RUT lookup; Equifax/Dicom for enrichment
- Priority: Medium
