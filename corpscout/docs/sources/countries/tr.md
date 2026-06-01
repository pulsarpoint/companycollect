# Turkey (TR) — GDP Rank #19

> **Summary:** Turkey's MERSİS (Central Registration System) is the official company registry with per-company lookup available online, but no public bulk API. MERSİS number and tax number (vergi kimlik numarası) are the key identifiers. Commercial providers Creditsafe Turkey and GüvenkRating offer paid APIs.

## Official Registry

### MERSİS — Central Trade Registry System (Merkezi Sicil Kayıt Sistemi)
| Field      | Detail |
|------------|--------|
| URL        | https://mersis.gtb.gov.tr |
| Access     | Web portal (no public bulk API) |
| Cost       | Free search |
| Fields     | MERSİS number (16-digit), company name, trade registry number, tax number (vergi no), legal form, registered address, trade registry office, activity code (NACE), registration date, status, directors, capital |
| Auth       | None for basic search |
| Rate limit | Undocumented; session-based access |
| Notes      | MERSİS is managed by Ministry of Trade (Ticaret Bakanlığı). MERSİS number is the unique 16-digit identifier replacing the old trade registry number. Trade registry still uses local chamber registry numbers (SİCİL no). No bulk download or API. Turkish-language interface. |

### UYAP — National Judicial Network
| Field      | Detail |
|------------|--------|
| URL        | https://www.uyap.gov.tr |
| Access     | Limited access (judiciary/lawyer accounts) |
| Cost       | Requires professional account |
| Fields     | Court records, enforcement proceedings, insolvency, judicial decisions affecting companies |
| Auth       | e-signature / professional account |
| Rate limit | N/A |
| Notes      | Not public-facing. Relevant for litigation and enforcement data. |

### TCMB / GKS — Central Bank Turkey (for financial entities)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tcmb.gov.tr |
| Access     | Web data (no API for company registry) |
| Cost       | Free |
| Fields     | Licensed banks, insurance companies, financial institutions |
| Auth       | None |
| Rate limit | N/A |
| Notes      | Not a general company registry; useful for financial sector entity identification only. |

## Commercial Providers

### Creditsafe Turkey
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/tr |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | MERSİS number, tax number, company name, credit score, directors, financials, payment behaviour, legal events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | International coverage for Turkish companies; reasonable data quality for mid/large enterprises. |

### GüvenkRating / Bisnode Turkey
| Field      | Detail |
|------------|--------|
| URL        | verify URL |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | MERSİS number, tax number, company data, credit score, financials, directors |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Local Turkish credit intelligence providers. Bisnode operates in Turkey. Verify current branding/ownership as market has consolidated. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage for Turkey sourced from MERSİS scrapes; completeness uncertain |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | BIST 100 listed companies and financial institutions covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MERSİS web for individual lookups; Creditsafe API for bulk/enriched commercial access
- Priority: Medium
