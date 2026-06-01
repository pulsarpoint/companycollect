# United Kingdom (GB) — GDP Rank #6

> **Summary:** Companies House provides a fully free REST API with bulk download. Best official registry API in the world — use it directly.

## Official Registry

### Companies House
| Field      | Detail |
|------------|--------|
| URL        | https://developer.company-information.service.gov.uk |
| Access     | API (free) |
| Cost       | Free |
| Fields     | company name, company number, status, company type, incorporation date, registered address, SIC codes, officers, filing history |
| Auth       | API key (free registration at developer.company-information.service.gov.uk) |
| Rate limit | 600 requests/5 min per key |
| Notes      | Bulk data snapshots also available at https://download.companieshouse.gov.uk/. Full accounts and confirmation statements accessible. |

## Commercial Providers

### Creditsafe
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/gb/en.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | All Companies House fields + credit score, financials, directors, CCJs |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Adds credit risk scoring on top of official data |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Companies House directly — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `companies_house`
- Recommended source: Companies House API (free, excellent quality)
- Priority: Complete
