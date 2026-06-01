# India (IN) — GDP Rank #5

> **Summary:** The MCA21 master data CSV (Ministry of Corporate Affairs) is the best free bulk source covering ~2.3M companies. Detailed views require CAPTCHA bypass, making Karza or similar paid APIs preferable for enriched, director-level data.

## Official Registry

### MCA21 — Ministry of Corporate Affairs
| Field      | Detail |
|------------|--------|
| URL        | https://www.mca.gov.in/content/mca/global/en/mca/master-data.html |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | CIN (Corporate Identity Number), company name, ROC code, registration number, company category, class, sub-category, date of incorporation, registered state, registered office address, listed/unlisted, status, paid-up capital, authorized capital |
| Auth       | None for bulk CSV |
| Rate limit | N/A (bulk) |
| Notes      | ~2.3M+ companies. CSV updated monthly. CIN format: L/U + 5-digit industry code + 2-char state + 4-digit year + C/S/F + 6-digit sequential. Detailed company view (directors, charges) on mca.gov.in requires CAPTCHA. DIN (Director Identification Number) data also partially available. |

### MCA21 — Company Search / Detailed View
| Field      | Detail |
|------------|--------|
| URL        | https://efiling.mca.gov.in/CompanyLLPMasterData/companyLLPMasterData |
| Access     | Scrape (CAPTCHA gated) |
| Cost       | Free |
| Fields     | Directors (DIN, name), charges, annual returns, balance sheet filings |
| Auth       | None; CAPTCHA required |
| Rate limit | Heavy CAPTCHA; rate limiting |
| Notes      | Not suitable for bulk programmatic access without CAPTCHA-solving service. Use paid APIs for director data. |

## Commercial Providers

### Karza Technologies
| Field      | Detail |
|------------|--------|
| URL        | https://karza.in |
| Access     | API (paid) |
| Cost       | Paid — per-query pricing; contact for volume rates |
| Fields     | CIN, company name, directors (DIN, name, address), charges, status, filings, GST number, PAN, incorporation documents |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Leading Indian KYB/KYC API provider. Returns director-level data including DIN lookups. Widely used in fintech for onboarding. Also covers GST, GSTIN verification, ITR. |

### Tofler
| Field      | Detail |
|------------|--------|
| URL        | https://www.tofler.in |
| Access     | API (paid) / Web portal |
| Cost       | Paid — per-report pricing |
| Fields     | CIN, directors, shareholders, financial statements, charges, compliance status |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Good for financial data and annual report retrieval. Smaller scale than Karza but well-structured API. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from MCA21 master data; no director data; limited advantage over free CSV for basic fields |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed Indian companies only; growing LEI adoption on NSE/BSE-listed entities |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MCA21 bulk CSV for broad coverage; Karza API for enriched director-level data
- Priority: High
