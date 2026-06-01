# Cyprus (CY) — GDP Rank #97

> **Summary:** The Registrar of Companies provides free bulk CSV downloads at efiling.drcor.mcit.gov.cy with registration number as the identifier. Good free bulk access — especially relevant given Cyprus's role as a holding company jurisdiction.

## Official Registry

### Registrar of Companies and Official Receiver
| Field      | Detail |
|------------|--------|
| URL        | https://efiling.drcor.mcit.gov.cy |
| Access     | Bulk download / Portal search |
| Cost       | Free |
| Fields     | registration number, company name, company type, status, registration date |
| Auth       | None |
| Rate limit | None |
| Notes      | Free bulk CSV download available; covers all registered companies; full company details (directors, shareholders, charges) require paid e-filing extraction; also searchable via efiling portal |

### eFiling Portal
| Field      | Detail |
|------------|--------|
| URL        | https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx |
| Access     | Portal search |
| Cost       | Free (basic) |
| Fields     | registration number, company name, type, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | None |
| Notes      | Free public search; director and shareholder details require paid extraction; no API |

## Commercial Providers

### Bureau van Dijk (Orbis)
| Field      | Detail |
|------------|--------|
| URL        | https://orbis.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, registration number, shareholders, financials, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Cyprus coverage; important for holding company structures |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Cyprus coverage; important for shell/holding company research |

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
- Recommended source: Registrar of Companies free bulk CSV download at efiling.drcor.mcit.gov.cy
- Priority: High
