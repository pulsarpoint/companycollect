# Czech Republic (CZ) — GDP Rank #44

> **Summary:** The ARES system provides a free REST API with no authentication required, returning company data by IČO number or name search. The OR (Obchodní rejstřík) at or.justice.cz is also free. Excellent programmatic access — one of the best in Eastern Europe.

## Official Registry

### ARES (Administrativní registr ekonomických subjektů)
| Field      | Detail |
|------------|--------|
| URL        | https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/ |
| Access     | API (free) |
| Cost       | Free |
| Fields     | IČO (Identifikační číslo osoby), company name, legal form, status, registration date, registered address, NACE activity code, VAT number |
| Auth       | None |
| Rate limit | None documented; reasonable use expected |
| Notes      | REST API with JSON responses; search by IČO or company name; covers all registered entities; updated regularly; documentation at ares.gov.cz |

### OR (Obchodní rejstřík — Business Register)
| Field      | Detail |
|------------|--------|
| URL        | https://or.justice.cz |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | IČO, company name, legal form, directors, shareholders, registered capital, filings |
| Auth       | None |
| Rate limit | Low |
| Notes      | Court-maintained company register; includes full company documents and filing history; no bulk API but ARES covers the main data |

## Commercial Providers

### Creditreform Czech Republic
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.cz |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, IČO, address, financial data, credit score, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of Creditreform network; good for financial enrichment beyond ARES |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Czech Republic coverage; data sourced from ARES and OR |

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
- Recommended source: ARES REST API — free, no auth, excellent coverage
- Priority: High
