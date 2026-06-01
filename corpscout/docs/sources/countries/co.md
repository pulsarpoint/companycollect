# Colombia (CO) — GDP Rank #41

> **Summary:** Confecámaras runs the RUE (Registro Único Empresarial y Social) at rues.org.co with a NIT identifier; no public bulk API is available. The Chamber of Commerce search portal is the best free access point.

## Official Registry

### Confecámaras — RUE (Registro Único Empresarial y Social)
| Field      | Detail |
|------------|--------|
| URL        | https://www.rues.org.co |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | NIT (Número de Identificación Tributaria), company name, legal form, status, registration date, municipality, activity (CIIU code) |
| Auth       | None for basic search |
| Rate limit | Moderate |
| Notes      | National unified registry aggregating data from regional Cámaras de Comercio; no bulk export; no API; CAPTCHA on some searches; NIT is the primary business identifier |

### CCB (Cámara de Comercio de Bogotá)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ccb.org.co |
| Access     | Portal search |
| Cost       | Free |
| Fields     | NIT, company name, status, registered address |
| Auth       | None |
| Rate limit | Low |
| Notes      | Covers Bogotá-registered entities; certificates require paid extraction |

## Commercial Providers

### Dun & Bradstreet Colombia
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, NIT, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Moderate Colombia coverage |

### DataCrédito Experian Colombia
| Field      | Detail |
|------------|--------|
| URL        | https://www.datacreditoempresas.com.co |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, NIT, credit score, payment history, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local credit bureau; strong SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Colombia coverage |

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
- Recommended source: RUES portal scrape for basic lookups; DataCrédito for enrichment
- Priority: Medium
