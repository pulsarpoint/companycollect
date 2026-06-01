# Poland (PL) — GDP Rank #22

> **Summary:** Poland has excellent free programmatic access to company data. KRS (National Court Register) provides a free REST API for registered companies, and CEIDG provides a free API for sole traders. Together they cover virtually all Polish businesses. KRS, NIP, and REGON are the three key identifiers.

## Official Registry

### KRS — National Court Register (Krajowy Rejestr Sądowy)
| Field      | Detail |
|------------|--------|
| URL        | https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs} |
| Access     | API (free) |
| Cost       | Free |
| Fields     | KRS number (10-digit), NIP (tax number, 10-digit), REGON (9-digit statistical number), company name, legal form, registered address, share capital, representatives/management board, supervisory board, shareholders (for smaller companies), PKD activity codes, date of registration, status |
| Auth       | None |
| Rate limit | None documented |
| Notes      | REST API returns current extract (odpis aktualny) in JSON format. Endpoint pattern: https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr=P&format=json. Covers limited liability companies (sp. z o.o.), joint-stock companies (S.A.), partnerships, associations, and foundations. Search by KRS number, NIP, or REGON at https://wyszukiwarka-krs.ms.gov.pl. Excellent reliability and data quality. Updated in real time. |

### CEIDG — Central Registration and Information on Business (Centralna Ewidencja i Informacja o Działalności Gospodarczej)
| Field      | Detail |
|------------|--------|
| URL        | https://aplikacja.ceidg.gov.pl/CEIDG/CEIDG.Public.UI/Search.aspx |
| Access     | API (free) |
| Cost       | Free |
| Fields     | NIP, REGON, PESEL (for individual), first/last name or business name, address, PKD codes, registration date, status, suspension periods |
| Auth       | API key (free registration) |
| Rate limit | None documented |
| Notes      | Covers sole traders and civil partnerships (~2.5M entries). CEIDG API documented at https://dane.gov.pl/en/dataset/175,ceidg-firmy-zarejestrowane (verify URL). Bulk data also available on dane.gov.pl. |

### GUS REGON (Central Statistical Office)
| Field      | Detail |
|------------|--------|
| URL        | https://api.stat.gov.pl/Home/BdlApi |
| Access     | API (free) |
| Cost       | Free |
| Fields     | REGON, company name, legal form, PKD activity, address, voivodeship, creation date |
| Auth       | API key (free registration) |
| Rate limit | None documented |
| Notes      | REGON database covers all entities including public sector, NGOs, and micro-businesses not in KRS/CEIDG. API at https://api-regon.stat.gov.pl/ (verify URL). Complements KRS and CEIDG for complete coverage. |

## Commercial Providers

### Coface Poland / Dun & Bradstreet Poland
| Field      | Detail |
|------------|--------|
| URL        | https://www.coface.pl |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | KRS, NIP, REGON, credit score, financials, payment behaviour, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Coface is a major credit insurer with a strong Polish presence. Best for credit risk beyond registry data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from KRS; no advantage over free direct KRS API |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | WSE (Warsaw Stock Exchange) listed companies covered; financial sector well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: KRS REST API for registered companies + CEIDG API for sole traders (both free, no auth or free key)
- Priority: High
