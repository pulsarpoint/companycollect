# Italy (IT) — GDP Rank #8

> **Summary:** Italy's Registro Imprese is managed by Infocamere (the IT consortium of Chambers of Commerce) and has no free public API. Scraping registroimprese.it is the only free option but is fragile. Infocamere offers a paid B2B API. REA number and codice fiscale are the key identifiers.

## Official Registry

### Registro Imprese (via registroimprese.it)
| Field      | Detail |
|------------|--------|
| URL        | https://www.registroimprese.it |
| Access     | Scrape (no public API) |
| Cost       | Free search; paid certified documents (EUR 5–20) |
| Fields     | Company name, REA number, codice fiscale / P.IVA, legal form, registered address, province, activity code (ATECO), registration date, status, capital, directors |
| Auth       | None for basic search; registration for document downloads |
| Rate limit | Undocumented; CAPTCHA and session-based access |
| Notes      | REA is the Repertorio Economico Amministrativo number (chamber-specific, format: XX-NNNNNN). Codice fiscale (tax code) doubles as company identifier. CCIAA (Camere di Commercio) manage regional entries. No national bulk API. Infocamere is the official technology provider for the entire network. |

### ATOKA (by Cerved)
| Field      | Detail |
|------------|--------|
| URL        | https://atoka.io |
| Access     | API (paid) |
| Cost       | Paid — tiered subscription |
| Fields     | Company name, codice fiscale, REA, address, ATECO code, directors, financials, credit score, corporate structure |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Built on Cerved's data sourced from Infocamere. Most developer-friendly Italian company data API. Free trial with limited calls. |

## Commercial Providers

### Infocamere (official)
| Field      | Detail |
|------------|--------|
| URL        | https://www.infocamere.it |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing (B2B agreements) |
| Fields     | Full Registro Imprese data: company details, directors, shareholders, balance sheets, certified documents |
| Auth       | API key via contract |
| Rate limit | Per contract |
| Notes      | Official technology arm of the Italian Chamber of Commerce network. Authoritative data. Requires formal B2B agreement; not self-service. |

### Cerved
| Field      | Detail |
|------------|--------|
| URL        | https://company.cerved.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Company profile, credit score, financial statements, directors, group structure, insolvency events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Italy's dominant credit bureau. Cerved data underlies many Italian KYB solutions including Atoka. Strong financial data coverage. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Registro Imprese via Infocamere; no direct API advantage but useful for cross-country queries |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed Italian companies; FTSE MIB constituents well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Atoka API for developer-friendly paid access; Infocamere direct for enterprise agreements
- Priority: High
