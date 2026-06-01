# Myanmar (MM) — GDP Rank #77

> **Summary:** DICA (Directorate of Investment and Company Administration) manages the MyCO (Myanmar Companies Online) system; no public bulk API exists. The MyCO portal provides search capability. Political instability since the 2021 coup limits data infrastructure development.

## Official Registry

### DICA — MyCO (Myanmar Companies Online)
| Field      | Detail |
|------------|--------|
| URL        | https://www.myco.dica.gov.mm |
| Access     | Portal search |
| Cost       | Free (basic); paid for full extracts |
| Fields     | company registration number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | MyCO is the online company register; basic search free; full company extracts (directors, shareholders) require paid account; no API; data quality and availability affected by political instability |

## Commercial Providers

### Dun & Bradstreet (Southeast Asia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Myanmar coverage; situation complex due to political environment |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Myanmar coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Myanmar LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: MyCO portal for basic lookups; limited options due to political situation
- Priority: Low
