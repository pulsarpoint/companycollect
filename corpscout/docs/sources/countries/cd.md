# DR Congo (CD) — GDP Rank #95

> **Summary:** The Guichet Unique (ANAPI) handles business registration under the OHADA framework with RCCM number as the identifier; no public API or digital search portal of significance exists. Very limited digital infrastructure.

## Official Registry

### Guichet Unique / ANAPI (Agence Nationale pour la Promotion des Investissements)
| Field      | Detail |
|------------|--------|
| URL        | https://www.anapi.cd |
| Access     | Portal (very limited) |
| Cost       | — (verify) |
| Fields     | RCCM number (Registre du Commerce et du Crédit Mobilier), company name, legal form, activity |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Single window for investment and business registration; OHADA framework; primarily in-person processes; no online search portal; no API; NRC (Numéro de Registre de Commerce) is also used |

### Tribunal du Commerce
| Field      | Detail |
|------------|--------|
| URL        | — (verify) |
| Access     | In-person |
| Cost       | Paid |
| Fields     | RCCM number, company name, legal form, status |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Commercial courts maintain RCCM register; Kinshasa Tribunal de Grande Instance handles main registrations; paper-based |

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
| Notes      | Very limited DRC coverage; mostly large mining/energy sector entities |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | No meaningful DRC coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few DRC LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: No viable programmatic source; in-country RCCM research required
- Priority: Low
