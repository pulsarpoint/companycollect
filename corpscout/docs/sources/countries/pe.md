# Peru (PE) — GDP Rank #49

> **Summary:** SUNARP handles company registration and SUNAT manages the RUC (tax ID) which doubles as the business identifier. SUNAT provides a public RUC lookup portal that can be scraped; no unified free bulk API exists.

## Official Registry

### SUNAT — RUC Consultation Portal
| Field      | Detail |
|------------|--------|
| URL        | https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias |
| Access     | Scrape |
| Cost       | Free |
| Fields     | RUC (Registro Único de Contribuyentes), legal name / trade name, taxpayer type, status, activity (CIIU code), address, ubigeo |
| Auth       | None |
| Rate limit | Moderate (CAPTCHA on some queries) |
| Notes      | RUC is the primary business identifier in Peru; SUNAT issues RUC for all economic activities; scraping is possible but CAPTCHA present on volume queries |

### SUNARP (Superintendencia Nacional de los Registros Públicos)
| Field      | Detail |
|------------|--------|
| URL        | https://www.sunarp.gob.pe |
| Access     | Portal search (paid) |
| Cost       | Paid per certificate |
| Fields     | company name, registration number, legal form, capital, directors, status |
| Auth       | Registration required |
| Rate limit | Low |
| Notes      | Corporate registration authority; company documents require paid extraction; no free API; search at sunarp.gob.pe/busquedaIndex.aspx |

## Commercial Providers

### Dun & Bradstreet Peru
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RUC, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Peru coverage |

### Equifax Peru
| Field      | Detail |
|------------|--------|
| URL        | https://www.equifax.com.pe |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, RUC, credit score, payment history, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local credit bureau; good SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Peru coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; limited Peru coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SUNAT RUC portal scrape for basic data (CAPTCHA is a blocker at scale)
- Priority: Medium
