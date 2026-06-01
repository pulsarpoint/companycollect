# Spain (ES) — GDP Rank #15

> **Summary:** BORME (official commercial gazette) provides free downloadable XML for all commercial registry announcements. The Registro Mercantil Central has no bulk API. NIF/CIF is the key identifier. Axesor and Informa are the main commercial providers for enriched data.

## Official Registry

### BORME — Boletín Oficial del Registro Mercantil
| Field      | Detail |
|------------|--------|
| URL        | https://www.boe.es/diario_borme/ |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Company name, NIF/CIF, registered section, act type (incorporation, dissolution, director appointment, capital change), province, registration date |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | All commercial registry acts published as daily XML files, available back to 1986. BORME is the official gazette of the Commercial Registry. Data is act-based (announcements) rather than current state snapshots. Requires parsing and building company state from sequential acts. API also documented at https://www.boe.es/datosabiertos/api/borme.php. |

### Registro Mercantil Central (RMC)
| Field      | Detail |
|------------|--------|
| URL        | https://www.rmc.es |
| Access     | Paid per-document (no bulk API) |
| Cost       | EUR 3–25 per document |
| Fields     | NIF, company name, legal form, registered office, directors, capital, activity, registration date, status |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | Central aggregator of all provincial mercantile registries (registros mercantiles provinciales). NIF (Número de Identificación Fiscal) for legal entities starts with letter (B=SL, A=SA, etc). No free API. Certified nota simple from RMC is the standard company status document. |

## Commercial Providers

### Informa D&B (Spain)
| Field      | Detail |
|------------|--------|
| URL        | https://www.informa.es |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | NIF, company name, financials (annual accounts), credit score, directors, shareholders, group structure, payment behaviour, solvency |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Informa is the leading Spanish commercial credit bureau, part of D&B network. Covers ~3.5M Spanish companies. Best option for financial data. |

### Axesor
| Field      | Detail |
|------------|--------|
| URL        | https://www.axesor.es |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | NIF, company name, credit score, annual accounts, executives, shareholders, corporate events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Spain-focused credit intelligence. Good developer API documentation. Positioned as alternative to Informa. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from BORME gazette and RMC; useful for cross-country queries but limited vs. direct BORME access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | IBEX 35 and large-cap companies well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: BORME bulk XML for historical/announcement data (free); Informa API for enriched current company data
- Priority: High
