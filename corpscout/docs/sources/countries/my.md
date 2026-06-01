# Malaysia (MY) — GDP Rank #33

> **Summary:** SSM (Companies Commission of Malaysia) is the official registry; no free public API exists, and the e-Info paid service charges per lookup. Best path for programmatic access is through commercial data providers or OpenCorporates.

## Official Registry

### SSM (Suruhanjaya Syarikat Malaysia / Companies Commission of Malaysia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ssm.com.my |
| Access     | Portal search / API (paid) |
| Cost       | Free basic search; paid e-Info service per report |
| Fields     | company number (XXXXXX-X format), company name, status, registration date, registered address, business activity |
| Auth       | MyKad / portal login for e-Info |
| Rate limit | Low (manual portal) |
| Notes      | e-Info service at einfo.ssm.com.my charges per company report (MYR 5–50 depending on report type); no bulk download; no free public API; SSM also maintains CCM (Companies Commission of Malaysia) business entity search |

### MyCoID 2016 Portal
| Field      | Detail |
|------------|--------|
| URL        | https://mycoid.ssm.com.my |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, status |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Basic company name/number lookup; limited fields |

## Commercial Providers

### Dun & Bradstreet Malaysia
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, company number, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Malaysia coverage |

### CTOS Data Systems
| Field      | Detail |
|------------|--------|
| URL        | https://www.ctosdata.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, number, status, credit score, payment history, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Malaysian credit bureau; strong local coverage especially for SMEs |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Moderate Malaysia coverage; data from SSM scrapes |

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
- Recommended source: CTOS or D&B for programmatic access; SSM portal scrape for basic lookup
- Priority: Medium
