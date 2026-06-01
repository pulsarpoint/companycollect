# Nepal (NP) — GDP Rank #90

> **Summary:** OCR (Office of the Company Registrar) at ocr.gov.np manages company registrations with company registration number and PAN as identifiers; no public bulk API or download is available. Very limited digital infrastructure.

## Official Registry

### OCR (Office of the Company Registrar)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ocr.gov.np |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company registration number, company name, legal form, status, registration date, registered address |
| Auth       | None for basic search |
| Rate limit | Low |
| Notes      | Online search portal; no bulk export; no API; company documents require paid extraction; PAN (Permanent Account Number) is the tax identifier |

### IRD (Inland Revenue Department) — PAN Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.ird.gov.np |
| Access     | Portal search |
| Cost       | Free |
| Fields     | PAN, taxpayer name, type, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority PAN verification; useful cross-reference |

## Commercial Providers

### Dun & Bradstreet (South Asia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Nepal coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Nepal coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Nepal LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: OCR portal scrape for basic lookups; very limited options
- Priority: Low
