# Oman (OM) — GDP Rank #66

> **Summary:** MOCIIP (Ministry of Commerce, Industry and Investment Promotion) manages commercial registrations with CR number as the identifier. The Invest Easy portal at investeasy.gov.om provides some company lookup capability; no public bulk API exists.

## Official Registry

### MOCIIP (Ministry of Commerce, Industry and Investment Promotion)
| Field      | Detail |
|------------|--------|
| URL        | https://www.mociip.gov.om |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CR number (Commercial Registration), company name, legal form, status, activity, address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | No bulk export; no API; full documents require paid extraction |

### Invest Easy (Bizoman)
| Field      | Detail |
|------------|--------|
| URL        | https://www.investeasy.gov.om |
| Access     | Portal search |
| Cost       | Free |
| Fields     | CR number, company name, status, activity |
| Auth       | Oman ID or registration for some services |
| Rate limit | Low |
| Notes      | E-services portal for business registration and lookup; more comprehensive than MOCIIP direct; no API |

## Commercial Providers

### Dun & Bradstreet (GCC coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, CR number, address, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Oman coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Oman coverage |

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
- Recommended source: Invest Easy portal scrape for basic lookups
- Priority: Low
