# Vietnam (VN) — GDP Rank #40

> **Summary:** The National Business Registration Portal (dangkykinhdoanh.gov.vn) is the official registry with MST (tax code / business registration number) as the identifier; no public API exists. Limited CSV datasets are published by the Ministry of Planning and Investment.

## Official Registry

### National Business Registration Portal (Cổng thông tin quốc gia về đăng ký doanh nghiệp)
| Field      | Detail |
|------------|--------|
| URL        | https://dangkykinhdoanh.gov.vn |
| Access     | Portal search / Scrape |
| Cost       | Free |
| Fields     | MST (Mã số thuế — tax code / business registration number), company name, legal form, status, registration date, address, charter capital |
| Auth       | None |
| Rate limit | Moderate |
| Notes      | Vietnamese-language portal; no bulk export or API; MST is the primary identifier (10 or 13 digits); also accessible at masothue.com (third-party aggregator) |

### Ministry of Planning and Investment Open Data
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.vn |
| Access     | Bulk download |
| Cost       | Free |
| Fields     | business registration number, name, type, province, industry code |
| Auth       | None |
| Rate limit | None |
| Notes      | Limited datasets; not comprehensive; periodic updates |

## Commercial Providers

### Dun & Bradstreet Vietnam
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, MST, address, directors, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Vietnam coverage |

### Vietnam Credit (VietnamCredit)
| Field      | Detail |
|------------|--------|
| URL        | https://vietnamcredit.com.vn |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, tax code, address, legal representative, charter capital, financials |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local provider with good Vietnam SME coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Vietnam coverage |

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
- Recommended source: dangkykinhdoanh.gov.vn scrape for basic lookup; VietnamCredit for enrichment
- Priority: Medium
