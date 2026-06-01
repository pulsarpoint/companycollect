# Russia (RU) — GDP Rank #11

> **Summary:** EGRUL (Unified State Register of Legal Entities) at egrul.nalog.ru offers free per-company lookup using OGRN, INN, or name, but no free bulk API. Bulk XML via FNS is paid. SPARK-Interfax is the main commercial provider. Sanctions context should be noted for compliance use cases.

## Official Registry

### EGRUL — Unified State Register of Legal Entities (ЕГРЮЛ)
| Field      | Detail |
|------------|--------|
| URL        | https://egrul.nalog.ru |
| Access     | Scrape / Paid bulk XML |
| Cost       | Free (per-company web lookup); paid for bulk |
| Fields     | OGRN (13-digit main registration number), INN (10-digit tax number), KPP (9-digit tax registration reason code), company name, legal form, registered address, date of registration, status (active/liquidated), directors, authorized capital, OKVED (activity codes), founders |
| Auth       | None for per-company lookup |
| Rate limit | No documented limit for individual queries; bulk not available for free |
| Notes      | OGRN, INN, and KPP are the three key identifiers. OGRN is the primary company ID. INN is the taxpayer ID (shared with the individual tax register). KPP changes when a company moves. Bulk XML dumps available via FNS (Federal Tax Service) subscription service — paid and requires Russian legal entity. Data quality is authoritative but interface is Russian-only. |

### EGRIP — Unified State Register of Individual Entrepreneurs (ЕГРИП)
| Field      | Detail |
|------------|--------|
| URL        | https://egrul.nalog.ru (same portal, different dataset) |
| Access     | Scrape |
| Cost       | Free (per-query) |
| Fields     | OGRNIP (15-digit), INN, entrepreneur name, registration date, status, activity codes |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Separate register for individual entrepreneurs (sole traders). Same FNS portal. |

## Commercial Providers

### SPARK-Interfax
| Field      | Detail |
|------------|--------|
| URL        | https://spark-interfax.ru |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | OGRN, INN, KPP, company name, address, directors, shareholders, financials (bухотчётность), court cases, contracts, credit risk, news, affiliations, sanctions screening |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Russia's leading commercial business intelligence provider. Operated by Interfax group. Most comprehensive Russian private-company data. Sanctions and PEP screening integrated. |

### Kontur.Focus (SKB Kontur)
| Field      | Detail |
|------------|--------|
| URL        | https://focus.kontur.ru |
| Access     | API (paid) |
| Cost       | Paid — per-query and subscription models |
| Fields     | OGRN, INN, financials, directors, founders, court records, government contracts, enforcement proceedings |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Popular with Russian SMEs and accountants. Developer-friendly API. More accessible pricing than SPARK for smaller volumes. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from EGRUL; coverage is reasonable for active companies but may lag updates |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Limited LEI adoption for Russian companies post-2022 sanctions; existing LEIs may be stale |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: EGRUL per-company scrape for basic data; SPARK-Interfax for enriched commercial intelligence; note sanctions screening requirements
- Priority: Medium
