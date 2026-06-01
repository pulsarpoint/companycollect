# Cameroon (CM) — GDP Rank #83

> **Summary:** CFCE (Centre de Formalités de Création d'Entreprises) handles business registration with RCCM number as the identifier under the OHADA legal framework; no public API or digital search portal of significance exists.

## Official Registry

### CFCE (Centre de Formalités de Création d'Entreprises)
| Field      | Detail |
|------------|--------|
| URL        | https://www.cfce.cm |
| Access     | Portal (very limited) |
| Cost       | — (verify) |
| Fields     | RCCM number (Registre du Commerce et du Crédit Mobilier), company name, legal form, activity |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Single window for business registration under OHADA framework; limited online presence; primarily in-person process; French and English official languages |

### Tribunal de Commerce — RCCM
| Field      | Detail |
|------------|--------|
| URL        | — (verify) |
| Access     | In-person |
| Cost       | Paid |
| Fields     | RCCM number, company name, legal form, status |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Commercial court maintains the RCCM register; primarily paper-based |

## Commercial Providers

### Dun & Bradstreet (Central Africa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Cameroon coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Cameroon coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Cameroon LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: No viable programmatic source; in-country RCCM research required
- Priority: Low
