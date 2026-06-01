# Ecuador (EC) — GDP Rank #61

> **Summary:** Superintendencia de Compañías (SUPERCIAS) offers a free bulk CSV download at its information portal, with RUC as the identifier. Excellent free bulk access for a South American country.

## Official Registry

### SUPERCIAS (Superintendencia de Compañías, Valores y Seguros)
| Field      | Detail |
|------------|--------|
| URL        | https://appscvs.supercias.gob.ec/portaldeinformacion/ |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | RUC (Registro Único de Contribuyentes), company name, legal form, status, province, canton, registration date, capital |
| Auth       | None |
| Rate limit | None |
| Notes      | Free CSV bulk downloads available at the information portal; covers all companies registered with SUPERCIAS; also searchable at supercias.gob.ec; full documents require portal account |

### SRI (Servicio de Rentas Internas) — RUC Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://srienlinea.sri.gob.ec/sri-en-linea/SriRucWeb/ConsultaRuc/Consultas/consultaRuc |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | RUC, taxpayer name/company name, type, status, activity |
| Auth       | None |
| Rate limit | Moderate |
| Notes      | Tax authority RUC lookup; RUC doubles as both tax ID and business identifier; useful for verification |

## Commercial Providers

### Dun & Bradstreet Ecuador
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RUC, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Ecuador coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Ecuador coverage |

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
- Recommended source: SUPERCIAS bulk CSV download — free, comprehensive coverage
- Priority: High
