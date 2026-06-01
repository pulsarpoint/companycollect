# Mexico (MX) — GDP Rank #14

> **Summary:** Mexico lacks a unified national free company registry API. The SAT manages RFC (tax IDs) without a public lookup API. SIGER is the federal commercial registry system but has no bulk data access. Provincial registries add further fragmentation. Commercial providers are the practical route for programmatic access.

## Official Registry

### SIGER — Integrated System for Commercial Registry (Sistema Integral de Gestión Registral)
| Field      | Detail |
|------------|--------|
| URL        | https://siger.gob.mx |
| Access     | Scrape / Web portal (no public API) |
| Cost       | Free search; paid for certified documents |
| Fields     | Company name, folio mercantil, RFC, legal form, registration date, registered agent, state of registration |
| Auth       | None for basic search |
| Rate limit | Undocumented |
| Notes      | Federal system aggregating state-level Registro Público de Comercio (RPC) entries. Coverage incomplete — many states are not yet integrated. No bulk download or API documented. Folio mercantil is the registry identifier, varies by state. |

### Registro Público de Comercio (State Level)
| Field      | Detail |
|------------|--------|
| URL        | Varies by state (e.g., CDMX: https://www.registropublico.cdmx.gob.mx) |
| Access     | Scrape / Paid document |
| Cost       | Free search; fees for certified documents |
| Fields     | Company name, folio, RFC, directors, shareholders, capital, articles of incorporation |
| Auth       | None for search |
| Rate limit | Undocumented |
| Notes      | Each state maintains its own registry; quality and online access varies significantly. CDMX (Mexico City) has the best digital access. |

### SAT — Servicio de Administración Tributaria (Tax Authority)
| Field      | Detail |
|------------|--------|
| URL        | https://www.sat.gob.mx |
| Access     | Limited (no public company lookup API) |
| Cost       | N/A |
| Fields     | RFC (13-char for individuals, 12-char for legal entities), company name (via constancia de situación fiscal) |
| Auth       | FIEL digital signature or CURP required |
| Rate limit | N/A |
| Notes      | RFC format for legal entities: 3-letter name code + 6-digit date + 3-char homoclave. SAT does expose a constancia de situación fiscal (tax status certificate) but it requires the company's own credentials to access. No public company RFC lookup API exists. |

## Commercial Providers

### Buró de Crédito Empresarial
| Field      | Detail |
|------------|--------|
| URL        | https://www.burodecredito.com.mx |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | RFC, company name, credit history, payment behaviour, debt levels, outstanding balances |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Mexico's primary commercial credit bureau for business credit scoring. Main use case is credit decisioning, not company discovery. |

### INEGI (Statistics — partial company data)
| Field      | Detail |
|------------|--------|
| URL        | https://www.inegi.org.mx/datos/ |
| Access     | Bulk download (free, aggregated) |
| Cost       | Free |
| Fields     | DENUE (Directorio Estadístico Nacional de Unidades Económicas): business name, economic unit ID, SCIAN industry code, employee range, municipality, locality, coordinates |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | DENUE is Mexico's national directory of economic units (~5M records), not a legal registry. No RFC included due to confidentiality. Useful for geographic/sectoral analysis. API also available at https://www.inegi.org.mx/app/api/denue/v1/doc/ (free with API key). |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage for Mexico is limited due to fragmented state registries and SIGER incompleteness |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies on BMV (Bolsa Mexicana de Valores) covered; limited overall |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: INEGI DENUE API for geographic/sectoral discovery (free); commercial providers for RFC-based KYB
- Priority: Medium
