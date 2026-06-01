# Romania (RO) — GDP Rank #43

> **Summary:** ONRC (National Trade Register Office) is the official registry at onrc.ro with CUI (fiscal code) as the primary identifier; scraping via recom.ro is possible. No free bulk API exists. ListaFirme.ro is a useful commercial aggregator with good coverage.

## Official Registry

### ONRC (Oficiul Național al Registrului Comerțului)
| Field      | Detail |
|------------|--------|
| URL        | https://www.onrc.ro |
| Access     | Portal search / Scrape |
| Cost       | Free (basic search); paid for full extracts |
| Fields     | CUI (Cod Unic de Identificare — fiscal code), company name, legal form, status, registration number (J code), registration date, county, registered address |
| Auth       | None for basic search |
| Rate limit | Moderate |
| Notes      | Main portal at recom.ro for search; no bulk API; full company extracts (directors, shareholders) cost ~RON 10–50; recom.ro provides name/CUI search free |

### recom.ro (ONRC Search Portal)
| Field      | Detail |
|------------|--------|
| URL        | https://www.recom.ro |
| Access     | Scrape |
| Cost       | Free |
| Fields     | CUI, J-number, company name, legal form, registration date, address |
| Auth       | None |
| Rate limit | Moderate (anti-scrape measures) |
| Notes      | Dedicated ONRC search portal; best entry point for scraping Romanian companies |

## Commercial Providers

### ListaFirme.ro
| Field      | Detail |
|------------|--------|
| URL        | https://www.listafirme.ro |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | CUI, name, address, activity (CAEN code), turnover, employees, status, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local Romanian aggregator with comprehensive coverage including financial data |

### Creditreform Romania
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.ro |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, CUI, address, financial data, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of Creditreform network; good coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Romania coverage sourced from ONRC data |

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
- Recommended source: recom.ro scrape for basic data; ListaFirme.ro API for enrichment
- Priority: Medium
