# Senegal (SN) — GDP Rank #91

> **Summary:** APIX (Agence de Promotion des Investissements et des Grands Travaux) handles investment entity registration; RCCM registration goes through commercial courts (tribunaux de commerce). No public API exists and digital access is very limited.

## Official Registry

### APIX — Guichet Unique
| Field      | Detail |
|------------|--------|
| URL        | https://www.investinsenegal.com |
| Access     | Portal (limited) |
| Cost       | Free |
| Fields     | company name, NINEA (Numéro d'Identification Nationale des Entreprises et Associations), legal form, activity |
| Auth       | None for basic information |
| Rate limit | Low |
| Notes      | Single window for investment-oriented registrations; OHADA framework; NINEA is the unique identifier; limited online search |

### Tribunal du Commerce — RCCM
| Field      | Detail |
|------------|--------|
| URL        | — (verify) |
| Access     | In-person |
| Cost       | Paid |
| Fields     | RCCM number, company name, legal form, status |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Commercial courts maintain RCCM register; primarily paper-based; Dakar commercial court is main registry |

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
| Notes      | Very limited Senegal coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Senegal coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Senegal LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: APIX portal for investment entities; no viable bulk programmatic source
- Priority: Low
