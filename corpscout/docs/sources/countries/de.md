# Germany (DE) — GDP Rank #3

> **Summary:** Offenes Handelsregister (offeneregister.de) provides a free JSON bulk download of ~5.5M companies from the official Handelsregister, making it the best programmatic entry point. The official Unternehmensregister.de has no bulk API; full certified documents require paid access via individual state courts.

## Official Registry

### Handelsregister (via Unternehmensregister.de)
| Field      | Detail |
|------------|--------|
| URL        | https://www.unternehmensregister.de |
| Access     | Scrape / Paid document retrieval |
| Cost       | Free search; EUR 4.50 per certified document |
| Fields     | Company name, registration number (HRB/HRA), court, registered address, legal form, managing directors, capital, status |
| Auth       | None for basic search |
| Rate limit | Undocumented; bot protection present |
| Notes      | Official federal aggregation of state-level Handelsregister entries. No bulk API. Registration number format: HRB XXXXX [CourtName]. Managed by individual state courts (Amtsgerichte). |

### Offenes Handelsregister
| Field      | Detail |
|------------|--------|
| URL        | https://offeneregister.de |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Company name, registration number, registered court, address, legal form, managing directors, status, registration date |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | Community-maintained mirror of all Handelsregister data. Full JSON dump ~5.5M companies downloadable at https://daten.offeneregister.de. Updated periodically (not real-time). Excellent for bootstrapping; verify current status against official source. |

## Commercial Providers

### Creditreform
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.de |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Company name, CrefoNumber (Creditreform ID), credit score, balance sheet, revenue, employees, directors, payment behaviour |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Germany's dominant commercial credit bureau. CrefoNumber is widely used in B2B credit checks. Member-network model: companies that join get better rates. |

### Bisnode / Dun & Bradstreet Germany
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.de |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, Handelsregister number, financials, credit risk, directors, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | D&B subsidiary in Germany. Good for multinational corporate family trees. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from offeneregister.de and official Handelsregister; no significant advantage over free direct access for basic data |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; useful for cross-border entity resolution |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Offenes Handelsregister bulk download for broad coverage; Unternehmensregister for live verification
- Priority: High
