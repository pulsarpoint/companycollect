# Croatia (HR) — GDP Rank #74

> **Summary:** The Sudski registar (Court Register) at sudreg.pravosudje.hr is free with MBS identifier and OIB (tax ID); open data is available for download. Good free programmatic access for a Southeast European country.

## Official Registry

### Sudski registar (Court Company Register)
| Field      | Detail |
|------------|--------|
| URL        | https://sudreg.pravosudje.hr |
| Access     | Portal search / Bulk download |
| Cost       | Free |
| Fields     | MBS (Matični broj subjekta — company register number), OIB (Osobni identifikacijski broj — tax/personal ID), company name, legal form, status, registration date, address, directors, capital |
| Auth       | None |
| Rate limit | None |
| Notes      | Free public access with open data download option; covers all registered companies; comprehensive field coverage; also at rgfi.hr for financial data |

### FINA (Financijska agencija) — Financial Data
| Field      | Detail |
|------------|--------|
| URL        | https://www.fina.hr |
| Access     | Portal search / API (paid) |
| Cost       | Free basic; paid API |
| Fields     | OIB, company name, financial statements, turnover, employees, credit score |
| Auth       | None for basic; API key for paid |
| Rate limit | Moderate for free |
| Notes      | Financial agency; provides financial statements for all companies; some data free to view; API subscription available |

## Commercial Providers

### Bisnode Croatia (now Dun & Bradstreet)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.hr |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, OIB, MBS, address, financial data, credit score |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Good Croatia coverage; now part of D&B network |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Croatia coverage; data from Sudski registar |

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
- Recommended source: Sudski registar free portal/bulk download; FINA for financial enrichment
- Priority: High
