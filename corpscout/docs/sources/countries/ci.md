# Côte d'Ivoire (CI) — GDP Rank #71

> **Summary:** CEPICI (Centre de Promotion des Investissements en Côte d'Ivoire) operates the Guichet Unique for business registration with RCCM number as the identifier; no public API exists. Very limited digital access to company data.

## Official Registry

### CEPICI — Guichet Unique
| Field      | Detail |
|------------|--------|
| URL        | https://www.cepici.gouv.ci |
| Access     | Portal (limited) |
| Cost       | Free |
| Fields     | RCCM number (Registre du Commerce et du Crédit Mobilier), company name, legal form, activity |
| Auth       | None for basic information |
| Rate limit | Low |
| Notes      | Single business registration window; limited online search capability; registration follows OHADA law; no bulk data; no API |

### Tribunal de Commerce d'Abidjan — RCCM
| Field      | Detail |
|------------|--------|
| URL        | — (verify) |
| Access     | In-person / Portal (very limited) |
| Cost       | Paid |
| Fields     | RCCM number, company name, legal form, status |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Commercial court maintains RCCM register; primarily manual process; limited online access |

## Commercial Providers

### Dun & Bradstreet (West Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Côte d'Ivoire coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Côte d'Ivoire coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Côte d'Ivoire LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: No viable programmatic source; CEPICI portal for basic information only
- Priority: Low
