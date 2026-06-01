# Serbia (RS) — GDP Rank #72

> **Summary:** APR (Agencija za privredne registre) is the authoritative source. No public API yet (listed as "under preparation" as of May 2026). Free registration at portal-info.apr.gov.rs gives access to data downloads. Direct web scraping of pretraga.apr.gov.rs is the current best free option.

## Official Registry

### APR — portal-info.apr.gov.rs
| Field      | Detail |
|------------|--------|
| URL        | https://portal-info.apr.gov.rs |
| Access     | Free (registration required) + data downloads |
| Cost       | Free |
| Fields     | name, MB (registration number), PIB (tax ID), address, status, legal form, beneficial owners, financial reports, pledges, insolvency |
| Auth       | Free account registration |
| Rate limit | Unknown |
| Notes      | 133,519 companies + 378,442 entrepreneurs + 263,102 financial reports + 208,946 beneficial owners. API listed as "under preparation" as of May 2026. Bulk export available via portal. |

### APR — pretraga.apr.gov.rs (public search)
| Field      | Detail |
|------------|--------|
| URL        | https://pretraga.apr.gov.rs |
| Access     | Scrape |
| Cost       | Free |
| Fields     | name, MB, address, status, legal form |
| Auth       | None |
| Rate limit | Unknown — anti-bot measures present |
| Notes      | Public search portal. Certificate issues as of May 2026 (self-signed cert). This is the same source OpenCorporates uses to get Serbian data. |

### APR — Web Service (contracted)
| Field      | Detail |
|------------|--------|
| URL        | https://www.apr.gov.rs (contact via portal.pomoc@apr.gov.rs) |
| Access     | API (paid) |
| Cost       | Paid — requires contract with APR |
| Fields     | Full database |
| Auth       | Contract-based credentials |
| Rate limit | Per contract |
| Notes      | Automated data delivery service. Pricing not published. Contact APR directly. |

## Commercial Providers

### CompanyWall Serbia
| Field      | Detail |
|------------|--------|
| URL        | https://www.companywall.rs |
| Access     | Web portal only (no API) |
| Cost       | Paid — ~€290/6mo, ~€376/year, ~€615/year (Ultimate) |
| Fields     | name, MB, PIB, address, credit score, financial reports, ownership, insolvency, tax debt, import/export |
| Auth       | Subscription account |
| Rate limit | N/A — web portal only |
| Notes      | Bulk export up to 5,000 companies. All data sourced from APR — middleman with no added value for programmatic access. |

### CompanyWall European API
| Field      | Detail |
|------------|--------|
| URL        | https://www.companywall.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing (not published) |
| Fields     | name, address, credit score, financials, directors, ownership |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Covers Serbia + 8 other Balkan/European countries. Enterprise pricing. Same underlying APR data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Scrapes pretraga.apr.gov.rs directly. Low quality (openness score 40/100). No financial data. More expensive than going to APR directly. |

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
- Recommended source: APR portal-info (free registration + downloads) now; switch to APR API when it launches
- Priority: High
