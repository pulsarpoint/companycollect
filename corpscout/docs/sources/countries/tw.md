# Taiwan (TW) — GDP Rank #21

> **Summary:** Taiwan's MOEA (Ministry of Economic Affairs) publishes a free bulk CSV of all registered companies on data.gov.tw, covering company ID (統一編號, 8-digit), name, address, capital, status, and industry. This is an excellent free programmatic source with regular updates.

## Official Registry

### MOEA Company Registration Bulk Data (經濟部商業司)
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.tw/dataset/6074 |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | 統一編號 (Unified Business Number, 8-digit), company name, address, capital (authorized), status, industry code (行業代碼), establishment date, dissolution date, responsible person (representative name) |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | Full dataset updated regularly (verify frequency — typically monthly). Covers all registered companies including active and dissolved. CSV format. ~1.5M records. The 統一編號 (GUI / Tax ID) is the universal business identifier used across tax, banking, and commercial systems. |

### MOEA — Business Registration Search (公司登記查詢)
| Field      | Detail |
|------------|--------|
| URL        | https://gcis.nat.gov.tw/mainNew/subclassNAction.do?method=getDetail |
| Access     | Scrape / Web portal |
| Cost       | Free |
| Fields     | Company ID, name, responsible person, directors, shareholders, capital, registered address, purpose, status, company type (有限公司/股份有限公司/etc.) |
| Auth       | None |
| Rate limit | Undocumented |
| Notes      | GCIS (Government Company & Legal Entity Information System) provides per-company lookup including directors. No bulk API but per-query scraping feasible for enrichment. |

### TWSE / TPEx — Taiwan Stock Exchange (Listed Companies)
| Field      | Detail |
|------------|--------|
| URL        | https://www.twse.com.tw/en/page/trading/exchange/BWIBBU_d.html |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Company name, ticker, ISIN, stock code, industry, market cap, financial disclosures |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Covers ~1,000 TWSE-listed + ~800 TPEx-listed companies. Good for public company financial data. |

## Commercial Providers

### Taiwan Credit Information Center (JCIC / 聯合徵信中心)
| Field      | Detail |
|------------|--------|
| URL        | https://www.jcic.org.tw |
| Access     | Restricted (financial institutions only) |
| Cost       | Paid — financial institution membership required |
| Fields     | Credit history, loans, defaults, directors |
| Auth       | Institutional membership |
| Rate limit | Per contract |
| Notes      | Not accessible to general public or foreign entities. Credit bureau for licensed financial institutions. |

### D&B Taiwan / Dun & Bradstreet
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, 統一編號, company name, financials, credit risk, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Coverage for mid/large Taiwanese companies; less comprehensive for SMEs. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from MOEA bulk data; no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | TWSE/TPEx listed companies and financial institutions covered; growing LEI adoption |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MOEA bulk CSV on data.gov.tw (free, comprehensive, no auth)
- Priority: High
