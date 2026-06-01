# Belarus (BY) — GDP Rank #75

> **Summary:** The Ministry of Justice register (portal.nalog.gov.by) provides company data with UNP (tax ID) as the identifier; access is technically available but the sanctions environment makes commercial use problematic. No public API.

## Official Registry

### Единый государственный регистр (Unified State Register)
| Field      | Detail |
|------------|--------|
| URL        | https://portal.nalog.gov.by/grp/getData |
| Access     | Portal search / Limited API |
| Cost       | Free |
| Fields     | УНП (UNP — учётный номер плательщика / taxpayer registration number), company name, legal form, status, registration date, address |
| Auth       | None for basic search |
| Rate limit | Moderate |
| Notes      | Tax ministry portal; UNP is the primary business identifier; no bulk export; sanctions by EU/US/UK may restrict commercial use of data; JSON-based lookup API exists but unofficial |

## Commercial Providers

### Dun & Bradstreet (Eastern Europe)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, UNP, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Belarus coverage; sanctions considerations apply |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Limited Belarus coverage; sanctions may affect data availability |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Belarus coverage due to sanctions |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: portal.nalog.gov.by for UNP lookups; sanctions compliance required
- Priority: Low
