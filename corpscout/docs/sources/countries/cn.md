# China (CN) — GDP Rank #2

> **Summary:** SAMR's National Enterprise Credit Information Publicity System (gsxt.gov.cn) is the official source but has heavy anti-bot protection and no public API. Commercial providers Qichacha and Tianyancha offer the most practical API access but require a Chinese business entity for registration. GLEIF is the best free option for LEI-registered entities.

## Official Registry

### SAMR — National Enterprise Credit Information Publicity System (NECIPS)
| Field      | Detail |
|------------|--------|
| URL        | https://www.gsxt.gov.cn |
| Access     | Scrape (no public API) |
| Cost       | Free (web UI) |
| Fields     | Company name, USCC (18-digit Unified Social Credit Code), registration date, registered capital, legal representative, business scope, status, address |
| Auth       | None for basic search; CAPTCHA on repeated queries |
| Rate limit | Heavy anti-bot; IP throttling, CAPTCHA |
| Notes      | USCC (统一社会信用代码) is the key 18-character identifier. Bulk download not available. Site blocks overseas IPs intermittently. Considered authoritative source. |

### SAMR — National Corporate Registration System (全国企业登记)
| Field      | Detail |
|------------|--------|
| URL        | https://xin.mofcom.gov.cn |
| Access     | Scrape (no public API) |
| Cost       | Free |
| Fields     | Basic company details, import/export status, foreign trade registration |
| Auth       | None |
| Rate limit | Rate-limited; CAPTCHA |
| Notes      | Ministry of Commerce portal supplementing SAMR. Useful for trade-focused data. |

## Commercial Providers

### Qichacha (企查查)
| Field      | Detail |
|------------|--------|
| URL        | https://www.qichacha.com / https://openapi.qichacha.com |
| Access     | API (paid) |
| Cost       | Paid — tiered pricing; requires Chinese business license for API access |
| Fields     | USCC, company name, legal representative, registered capital, shareholders, directors, subsidiaries, judicial risk, patents, trademarks |
| Auth       | API key; registration requires Chinese business entity |
| Rate limit | Per plan |
| Notes      | One of the two dominant Chinese company data providers. Requires mainland China business registration to obtain API credentials. Non-Chinese entities must use resellers. |

### Tianyancha (天眼查)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tianyancha.com / https://www.tianyancha.com/cloud-other-information/openApi.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing; Chinese business registration required |
| Fields     | USCC, shareholders, equity structure, court filings, tenders, news, brand info, supply chain links |
| Auth       | API key; Chinese entity required |
| Rate limit | Per contract |
| Notes      | Strong for supply chain and risk intelligence. More comprehensive judicial/litigation data than Qichacha. Same access barrier for foreign entities. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage is partial; sourced from SAMR via periodic scrapes. Not authoritative; may lag official data by months |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Best free programmatic option for large/listed Chinese companies; LEI coverage is growing but not universal |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: GLEIF for LEI-registered entities; Qichacha/Tianyancha via reseller for broader coverage; direct SAMR scraping is high-friction
- Priority: High
