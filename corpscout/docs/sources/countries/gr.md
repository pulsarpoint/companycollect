# Greece (GR) — GDP Rank #51

> **Summary:** GEMI (General Commercial Registry) provides a free API and bulk data download at businessportal.gr, with GEMI number and AFM (tax ID) as identifiers. Good free programmatic access for a Southern European country.

## Official Registry

### GEMI (Γενικό Εμπορικό Μητρώο — General Commercial Registry)
| Field      | Detail |
|------------|--------|
| URL        | https://www.businessportal.gr |
| Access     | API (free) / Bulk download |
| Cost       | Free |
| Fields     | GEMI number, AFM (ΑΦΜ — tax identification number), company name, legal form, status, registration date, registered address, capital, activity (KAD code) |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Free REST API and downloadable bulk data; businessportal.gr is the main entry point; covers ~1.1M entities; GEMI also accessible at publicity.businessportal.gr |

### GEMI Publicity Portal
| Field      | Detail |
|------------|--------|
| URL        | https://publicity.businessportal.gr |
| Access     | Portal search |
| Cost       | Free |
| Fields     | GEMI number, company name, status, directors, announcements, filings |
| Auth       | None |
| Rate limit | None |
| Notes      | Displays company documents and announcements; complementary to the API |

## Commercial Providers

### ICAP Group
| Field      | Detail |
|------------|--------|
| URL        | https://www.icap.gr |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, GEMI number, AFM, address, financial data, credit score, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Major Greek business information provider; strong coverage of financials |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good Greece coverage; data sourced from GEMI |

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
- Recommended source: GEMI API at businessportal.gr — free, bulk download available
- Priority: High
