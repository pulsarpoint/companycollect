# Japan (JP) — GDP Rank #4

> **Summary:** The NTA Corporate Number Publication Site provides a free, unauthenticated REST API covering ~6M corporations with daily updates. This is the best programmatic entry point. Ministry of Justice registry holds director details but lacks a public API. Teikoku Databank and Tokyo Shoko Research are the main commercial providers.

## Official Registry

### NTA Corporate Number Publication Site (国税庁法人番号公表サイト)
| Field      | Detail |
|------------|--------|
| URL        | https://www.houjin-bangou.nta.go.jp/en/ |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Corporate number (13-digit), company name (Japanese + romanized), address (prefecture, city, street), update date, change history, status, type |
| Auth       | None |
| Rate limit | None documented |
| Notes      | ~6M corporations. REST API returns XML or JSON. Daily update files also available for bulk download. English interface available. Corporate number is the key identifier (法人番号). API endpoint: https://api.houjin-bangou.nta.go.jp/4/name?name=...&type=12&mode=1&target=1&address=&kind=&group= (verify URL). Excellent reliability as NTA is the tax authority. |

### Ministry of Justice — Commercial and Corporate Registry (商業・法人登記)
| Field      | Detail |
|------------|--------|
| URL        | https://www.touki-kyoutaku-online.moj.go.jp |
| Access     | Paid document retrieval (no bulk API) |
| Cost       | JPY 335 per abstract; JPY 480 per full extract |
| Fields     | Company name, address, directors, representative directors, capital, purpose, establishment date, fiscal year |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | Authoritative source for director/officer details. No free API. Online extraction portal (登記情報提供サービス) requires account. Only option for director-level data without commercial providers. |

## Commercial Providers

### Teikoku Databank (帝国データバンク)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tdb.co.jp/en/ |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | TDB code, company name, address, directors, financials (revenue, capital, employees), credit score, industry, corporate group links |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Japan's largest private credit bureau. TDB code is widely used in B2B. Covers ~3M companies including unlisted. Strong financial data. |

### Tokyo Shoko Research (東京商工リサーチ)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tsr-net.co.jp |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | TSR code, company name, financials, credit score, directors, subsidiaries, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Second major Japanese credit bureau. Comparable coverage to TDB. Used heavily in domestic due diligence. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from NTA/MOJ data; no significant advantage over free NTA API for basic fields |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed Japanese companies only; reasonable LEI adoption among Nikkei-listed firms |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: NTA Corporate Number API (free, no auth, daily updates, excellent coverage)
- Priority: High
