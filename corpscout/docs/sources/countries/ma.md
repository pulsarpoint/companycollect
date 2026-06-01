# Morocco (MA) — GDP Rank #56

> **Summary:** OMPIC (Office Marocain de la Propriété Industrielle et Commerciale) manages the commercial register with RC (Registre de Commerce) number as the identifier; no free public API exists. CRI (Regional Investment Centers) handle registration. Limited programmatic access.

## Official Registry

### OMPIC (Office Marocain de la Propriété Industrielle et Commerciale)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ompic.ma |
| Access     | Portal search |
| Cost       | Free (basic); paid for full certificates |
| Fields     | RC number (Registre de Commerce), company name, legal form, status, city, activity |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Search at ompic.ma/fr/commerce; no bulk export; no API; company certificates require paid extraction; ICE number (Identifiant Commun de l'Entreprise) is the unified identifier since 2013 |

### CRI (Centres Régionaux d'Investissement)
| Field      | Detail |
|------------|--------|
| URL        | https://www.invest.gov.ma |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, RC number, activity, region |
| Auth       | None |
| Rate limit | Low |
| Notes      | Regional investment centers handle company creation; limited online search capability |

## Commercial Providers

### Dun & Bradstreet Morocco
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RC number, address, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Morocco coverage |

### Inforisk
| Field      | Detail |
|------------|--------|
| URL        | https://www.inforisk.ma |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, ICE, RC number, address, directors, financial data, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local Moroccan business information provider; best local coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Morocco coverage |

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
- Recommended source: OMPIC portal scrape for basic lookups; Inforisk for enrichment
- Priority: Low
