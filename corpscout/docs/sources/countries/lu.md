# Luxembourg (LU) — GDP Rank #73

> **Summary:** LBR (Luxembourg Business Registers) at lbr.lu provides a paid API with RCS (Registre de Commerce et des Sociétés) number as the identifier. Excellent data quality but access is paid only. Important for EU financial sector entities.

## Official Registry

### LBR (Luxembourg Business Registers) — RCS
| Field      | Detail |
|------------|--------|
| URL        | https://www.lbr.lu |
| Access     | API (paid) / Portal search |
| Cost       | Free basic search; paid API |
| Fields     | RCS number, company name, legal form, status, registration date, registered address, directors, shareholders, capital |
| Auth       | API key (paid subscription) |
| Rate limit | Per contract |
| Notes      | High-quality data; covers all Luxembourg-registered companies; free search at lbr.lu; API subscription required for programmatic access; also covers Economic Interest Groups and legal publications |

### data.public.lu (Luxembourg Open Data)
| Field      | Detail |
|------------|--------|
| URL        | https://data.public.lu |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | RCS number, company name, legal form, status, registration date |
| Auth       | None |
| Rate limit | None |
| Notes      | Some LBR datasets published as open data; check for current availability; not as comprehensive as paid API |

## Commercial Providers

### Bureau van Dijk (Orbis)
| Field      | Detail |
|------------|--------|
| URL        | https://orbis.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, RCS number, shareholders, financials, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Excellent Luxembourg coverage; important for fund structures and holding companies |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Luxembourg coverage; important for fund/SOPARFI structures |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Very good Luxembourg coverage; many financial entities have LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: LBR paid API for comprehensive data; GLEIF for listed/financial entities (free)
- Priority: Medium
