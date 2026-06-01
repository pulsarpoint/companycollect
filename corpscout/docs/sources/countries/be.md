# Belgium (BE) — GDP Rank #25

> **Summary:** Belgium's Crossroads Bank for Enterprises (CBE) provides free bulk data downloads with excellent coverage. The enterprise number (0XXX.XXX.XXX, 10-digit with leading zero) is the key identifier. kbopub.economie.fgov.be offers free web search. This is one of the best free open data registries in Europe.

## Official Registry

### CBE — Crossroads Bank for Enterprises (Banque-Carrefour des Entreprises / Kruispuntbank van Ondernemingen)
| Field      | Detail |
|------------|--------|
| URL        | https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises/services-everyone/cbe-open-data |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Enterprise number (BE0XXX.XXX.XXX), company name (NL/FR/DE variants), legal form, status (active/ceased), start date, cessation date, registered address (street, municipality, postal code), activity codes (NACE-BEL), type of enterprise, contact email, phone, website |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | Full bulk download available as ZIP (CSV files) at the URL above. ~3M enterprise numbers including active and historical entities. Updated quarterly. Covers companies, sole traders, non-profit associations, and public entities. Enterprise number format: 0XXX.XXX.XXX (10 digits, always starts with 0). Data split into multiple CSV files: enterprise, address, activity, denomination, contact. |

### KBO Public Search (kbopub)
| Field      | Detail |
|------------|--------|
| URL        | https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html |
| Access     | Scrape / Web portal (per-company lookup) |
| Cost       | Free |
| Fields     | Enterprise number, company name, legal form, status, address, activity, establishment units, contacts |
| Auth       | None |
| Rate limit | Undocumented; some anti-scraping measures |
| Notes      | Per-company web search. Includes establishment units (vestigingseenheden) as sub-entities with their own unit numbers. Multi-language support (NL/FR/DE/EN). |

### NBB — National Bank of Belgium (Annual Accounts)
| Field      | Detail |
|------------|--------|
| URL        | https://www.nbb.be/en/central-balance-sheet-office/services-and-data/financial-data-belgian-companies |
| Access     | Bulk download (free) / API (partially free) |
| Cost       | Free for standardized annual account data |
| Fields     | Enterprise number, annual accounts (balance sheet, P&L), financial ratios |
| Auth       | None for open data |
| Rate limit | N/A |
| Notes      | NBB Central Balance Sheet Office publishes standardized annual accounts for all Belgian companies required to file. Excellent financial data complement to CBE. |

## Commercial Providers

### Graydon Belgium
| Field      | Detail |
|------------|--------|
| URL        | https://www.graydon.be |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Enterprise number, credit score, financials, directors, payment behaviour, group structure, legal events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Belgium's leading commercial credit bureau. Part of Atradius group. Strong for credit risk and due diligence. |

### Creditsafe Belgium
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/be |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Enterprise number, credit score, directors, financials, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Alternative to Graydon; international coverage for cross-border queries. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from CBE; no advantage over free direct CBE bulk download |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Euronext Brussels listed companies and financial institutions well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: CBE Open Data bulk download (free, comprehensive, excellent quality — among the best in Europe)
- Priority: High
