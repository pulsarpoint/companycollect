# Hong Kong (HK) — GDP Rank #42

> **Summary:** The Companies Registry offers free bulk CSV download (company number, name, type, status, incorporation date) at cr.gov.hk, making it a strong free data source. Full details and filings require paid e-Search. Excellent coverage for a major financial hub.

## Official Registry

### Companies Registry — Bulk Data Download
| Field      | Detail |
|------------|--------|
| URL        | https://www.cr.gov.hk/en/e-services/business-register-search/RegistrySearch/ |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | company number, company name, company type, date of incorporation, status |
| Auth       | None |
| Rate limit | None |
| Notes      | Free CSV bulk download available; periodic updates; covers all registered companies (~1.5M entities); full filings and director data require paid e-Search |

### Companies Registry — e-Search (Full Details)
| Field      | Detail |
|------------|--------|
| URL        | https://www.icris.cr.gov.hk/csci/ |
| Access     | Portal search (paid per document) |
| Cost       | Paid (HKD 10–100 per document) |
| Fields     | directors, shareholders, charges, annual returns, registered address |
| Auth       | Registration required |
| Rate limit | Per transaction |
| Notes      | Individual company documents; no bulk API; ICRIS system |

## Commercial Providers

### Dun & Bradstreet Hong Kong
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, company number, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Hong Kong coverage |

### Bureau van Dijk (Orbis)
| Field      | Detail |
|------------|--------|
| URL        | https://orbis.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, registration number, shareholders, financials, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Strong HK listed company coverage; good for financial sector entities |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Hong Kong coverage; data sourced from Companies Registry bulk data |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; good HK financial sector coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Companies Registry bulk CSV download — free, ~1.5M entities
- Priority: High
