# Indonesia (ID) — GDP Rank #16

> **Summary:** Indonesia's AHU Online (Ministry of Law and Human Rights) is the official company registry but offers no public bulk API. The OSS (Online Single Submission) system issues NIB (Nomor Induk Berusaha) as the new unified business ID. Programmatic access is limited; commercial providers or manual approaches are required.

## Official Registry

### AHU Online — Ministry of Law and Human Rights (Kemenkumham)
| Field      | Detail |
|------------|--------|
| URL        | https://ahu.go.id |
| Access     | Scrape / Web portal (no public API) |
| Cost       | Free search |
| Fields     | Company name, company number, legal form (PT, CV, Yayasan, etc.), registration date, registered address, notary, status |
| Auth       | None for basic search |
| Rate limit | Undocumented; anti-bot measures present |
| Notes      | AHU is the authoritative registry for legal entities (Perseroan Terbatas/PT). Company number (Nomor AHU) is the registry identifier. No bulk download. Notary-based registration system means data quality depends on notary submissions. |

### OSS — Online Single Submission (Ministry of Investment/BKPM)
| Field      | Detail |
|------------|--------|
| URL        | https://oss.go.id |
| Access     | Web portal (no public API) |
| Cost       | Free |
| Fields     | NIB (13-digit Nomor Induk Berusaha), company name, business type, KBLI industry classification, business scale, investment value, address |
| Auth       | Registration required (business entity account) |
| Rate limit | N/A |
| Notes      | NIB replaced the old SIUP/TDP/API licenses with a single business ID. Issued since 2018. Not all legacy companies have NIB; transition ongoing. OSS data not publicly queryable. |

### BKPM — Investment Coordinating Board
| Field      | Detail |
|------------|--------|
| URL        | https://www.bkpm.go.id |
| Access     | Limited web data (no public API) |
| Cost       | Free |
| Fields     | Foreign investment approvals, project names, investment value, sector |
| Auth       | None for public data |
| Rate limit | N/A |
| Notes      | Useful for tracking foreign direct investment. Not a company registry per se. Some statistical data available. |

## Commercial Providers

### Dun & Bradstreet Indonesia
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, company name, address, directors, financials (limited), credit risk |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Coverage for Indonesian companies is limited compared to more developed markets. Best for listed/large companies. |

### PERSEROAN.com / Infobisnis
| Field      | Detail |
|------------|--------|
| URL        | verify URL |
| Access     | Web portal (paid) |
| Cost       | Paid — per-report |
| Fields     | Company profile, AHU registration details, shareholders, notary documents |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | Local Indonesian business data providers; coverage and API availability should be verified. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage for Indonesia is limited; data sourced from AHU scrapes; may be incomplete |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | IDX (Indonesia Stock Exchange) listed companies partially covered; overall LEI adoption low |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: AHU Online scrape for basic data (fragile); OpenCorporates for managed coverage; no good free API option exists
- Priority: Medium
