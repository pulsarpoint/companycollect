# Uzbekistan (UZ) — GDP Rank #63

> **Summary:** The Unified State Register of Legal Entities at my.gov.uz uses STIR/TIN as the identifier; limited online search is available with no public bulk API. Digital infrastructure is developing but access is restricted.

## Official Registry

### Unified State Register of Legal Entities — my.gov.uz
| Field      | Detail |
|------------|--------|
| URL        | https://my.gov.uz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | STIR (Soliq to'lovchining identifikatsion raqami — TIN), company name, legal form, status, registration date, address |
| Auth       | ID card or EDS (electronic signature) for some services |
| Rate limit | Low |
| Notes      | Government e-services portal; company lookup available; no bulk export; no API; limited functionality without authentication |

### Soliq.uz (Tax Authority)
| Field      | Detail |
|------------|--------|
| URL        | https://my.soliq.uz |
| Access     | Portal search |
| Cost       | Free |
| Fields     | TIN/STIR, company name, type, status, activity code |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority portal; TIN verification service available |

## Commercial Providers

### Dun & Bradstreet (CIS coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, TIN, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Uzbekistan coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Uzbekistan coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Uzbekistan coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: my.gov.uz / soliq.uz portal for basic TIN lookups; no strong programmatic option
- Priority: Low
