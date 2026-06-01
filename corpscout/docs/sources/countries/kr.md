# South Korea (KR) — GDP Rank #12

> **Summary:** DART (Data Analysis, Retrieval and Transfer System) provides a free API with API key for public company financial disclosures. BizInfo covers all companies including small businesses. The Business Registration Number (사업자등록번호, BRN) is the 10-digit key identifier.

## Official Registry

### DART — Financial Supervisory Service
| Field      | Detail |
|------------|--------|
| URL        | https://opendart.fss.or.kr |
| Access     | API (free, API key required) |
| Cost       | Free |
| Fields     | Corporation code, company name, stock code, legal form, CEO, address, phone, industry code, fiscal year, filings (annual reports, audits, disclosures), shareholder information |
| Auth       | API key (free registration at opendart.fss.or.kr) |
| Rate limit | 20,000 API calls/day on free plan |
| Notes      | Covers KOSPI/KOSDAQ/KONEX listed companies and significant non-listed companies that file with FSS (~100,000 companies). Excellent for financial statements and corporate governance data. DART code is the internal identifier; maps to BRN. Korean-language API with some English support. |

### BizInfo (중소기업현황정보시스템)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bizno.net |
| Access     | Scrape / API (partial) |
| Cost       | Free search |
| Fields     | Business registration number (BRN), company name, CEO, address, establishment date, industry code (KSIC), employee count, revenue range, type |
| Auth       | None for basic search |
| Rate limit | Undocumented |
| Notes      | Covers all Korean businesses including small/micro companies. Operated by Korea Credit Information Services (KCIS). Comprehensive coverage ~6M businesses. No official bulk API documented; scraping feasible. |

### Supreme Court Registry (법원 등기)
| Field      | Detail |
|------------|--------|
| URL        | https://www.iros.go.kr |
| Access     | Paid document retrieval |
| Cost       | KRW 1,200 per corporate registry extract |
| Fields     | Corporate registration number, directors, shareholders, capital, establishment date, articles of incorporation |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | Authoritative source for corporate structure/governance. Corporate registration number (법인등록번호, 12-digit) distinct from BRN. |

## Commercial Providers

### Korea Credit Rating (Korea Investors Service / NICE)
| Field      | Detail |
|------------|--------|
| URL        | https://www.niceinfo.co.kr |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | BRN, company name, credit rating, financials, directors, litigation history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | NICE (National Information & Credit Evaluation) is Korea's dominant credit bureau. Strong financial and risk data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Korean registry data; coverage may be incomplete for smaller entities |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | KOSPI-listed companies generally well covered; good for listed entity identification |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: DART API for public/listed companies (free with API key); BizInfo scrape for broader SME coverage
- Priority: High
