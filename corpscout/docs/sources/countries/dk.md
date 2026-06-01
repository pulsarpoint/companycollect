# Denmark (DK) — GDP Rank #38

> **Summary:** CVR (Central Business Register) provides a free ElasticSearch-based API with comprehensive company data. Requires free registration to get a token.

## Official Registry

### CVR (Centrale Virksomhedsregister)
| Field      | Detail |
|------------|--------|
| URL        | https://cvrapi.dk / https://data.virk.dk |
| Access     | API (free) |
| Cost       | Free |
| Fields     | cvr_number, name, status, company_type, address, phone, email, industry_code, founded_date, employees, owners, directors |
| Auth       | HTTP Basic auth with free CVR account (register at data.virk.dk) |
| Rate limit | None documented for registered users |
| Notes      | Full bulk dump available. ElasticSearch query endpoint: https://distribution.virk.dk/cvr-permanent/virksomhed/_search. Includes P-units (production units) separately. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from CVR — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `cvr`
- Recommended source: CVR API (free with registration, ElasticSearch)
- Priority: Complete
