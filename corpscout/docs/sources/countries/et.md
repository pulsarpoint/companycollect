# Ethiopia (ET) — GDP Rank #58

> **Summary:** The Ministry of Trade and Regional Integration manages commercial registrations; the Ethiopian Investment Commission handles investment-related entities. No public API or online search portal exists in a meaningful way. Digital infrastructure is very limited.

## Official Registry

### Ministry of Trade and Regional Integration
| Field      | Detail |
|------------|--------|
| URL        | https://www.mot.gov.et |
| Access     | Portal (very limited) |
| Cost       | — (verify) |
| Fields     | company name, registration number, legal form, activity |
| Auth       | — (verify) |
| Rate limit | — (verify) |
| Notes      | Very limited online presence; commercial registration is largely manual; no public search portal; regional trade bureaus handle local registrations |

### Ethiopian Investment Commission (EIC)
| Field      | Detail |
|------------|--------|
| URL        | https://www.investethiopia.gov.et |
| Access     | Portal (limited) |
| Cost       | Free |
| Fields     | investor name, registration number, sector, region |
| Auth       | None |
| Rate limit | Low |
| Notes      | Only covers investment-licensed entities; not comprehensive for all businesses |

## Commercial Providers

### Dun & Bradstreet (Sub-Saharan Africa coverage)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Ethiopia coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Ethiopia coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Ethiopia LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: No viable programmatic source; EIC portal for investment entities only
- Priority: Low
