# Nigeria (NG) — GDP Rank #31

> **Summary:** The Corporate Affairs Commission (CAC) runs the national business registry with an online search portal; there is no public bulk API or free data export. Manual portal search is the only free programmatic path; commercial providers are limited for Nigeria.

## Official Registry

### Corporate Affairs Commission (CAC)
| Field      | Detail |
|------------|--------|
| URL        | https://search.cac.gov.ng |
| Access     | Scrape |
| Cost       | Free |
| Fields     | company name, RC number (Registration of Company), status, registration date, registered address |
| Auth       | None |
| Rate limit | Moderate (CAPTCHA present) |
| Notes      | Search portal at search.cac.gov.ng; no bulk export; no public API; CAPTCHA limits automated scraping; verification certificates require paid extraction |

## Commercial Providers

### Dun & Bradstreet Nigeria
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S number, address, sector, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited Nigerian coverage compared to other markets |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Partial coverage; data sourced from CAC scrapes; not comprehensive |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very limited Nigeria coverage |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: CAC portal scrape (CAPTCHA is a blocker); OpenCorporates for partial bulk data
- Priority: Low
