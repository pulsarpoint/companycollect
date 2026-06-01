# Angola (AO) — GDP Rank #67

> **Summary:** IGAPE (Instituto de Gestão de Activos e Participações do Estado) and the Guichet Único (single business registration window) handle company registrations in Angola; no public API or bulk data access exists. Very limited digital infrastructure.

## Official Registry

### Guichet Único (MAPTSS — Single Business Registration Window)
| Field      | Detail |
|------------|--------|
| URL        | https://www.guichetempresarial.gov.ao |
| Access     | Portal (limited) |
| Cost       | — (verify) |
| Fields     | company name, NIF (Número de Identificação Fiscal), legal form, status |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Single window for business registration; limited online search capability; no API; registration processes are largely manual |

### IGAPE (Instituto de Gestão de Activos e Participações do Estado)
| Field      | Detail |
|------------|--------|
| URL        | https://www.igape.ao |
| Access     | Portal (limited) |
| Cost       | — (verify) |
| Fields     | company name, registration information |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | State asset management body; limited company search; primary role is managing state participations |

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
| Notes      | Very limited Angola coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Angola coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Angola LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: No viable programmatic source; manual in-country research required
- Priority: Low
