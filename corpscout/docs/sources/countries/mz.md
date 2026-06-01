# Mozambique (MZ) — GDP Rank #93

> **Summary:** CPAR (Conservatória do Registo de Entidades Legais) manages legal entity registration with NUIT (tax number) as the identifier; no public API or bulk data download exists. Very limited digital infrastructure.

## Official Registry

### CPAR (Conservatória do Registo de Entidades Legais)
| Field      | Detail |
|------------|--------|
| URL        | https://www.cpar.gov.mz |
| Access     | Portal (limited) |
| Cost       | — (verify) |
| Fields     | NUIT (Número Único de Identificação Tributária), company name, legal form, status, registration date |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Limited online presence; registration process largely in-person; no bulk export; no API; Portuguese-language interface |

### AT (Autoridade Tributária) — NUIT Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.at.gov.mz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | NUIT, taxpayer name, type, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority; NUIT verification available; NUIT is the primary business/tax identifier |

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
| Notes      | Very limited Mozambique coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Mozambique coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Mozambique LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: AT NUIT lookup for tax verification; no strong programmatic option
- Priority: Low
