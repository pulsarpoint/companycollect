# Australia (AU) — GDP Rank #13

> **Summary:** Australia has two complementary identifiers: ABN (Australian Business Number, from ATO/ABR) with a free lookup API, and ACN (Australian Company Number, from ASIC) with bulk download on data.gov.au. ABN Lookup is the easiest free programmatic entry point.

## Official Registry

### ABN Lookup — Australian Business Register (ABR)
| Field      | Detail |
|------------|--------|
| URL        | https://abr.business.gov.au |
| Access     | API (free, API key for bulk; no auth for basic) |
| Cost       | Free |
| Fields     | ABN (11-digit), entity name, entity type, status, state, postcode, main business location, ACN (if applicable), GST registration, ANZSIC industry code |
| Auth       | None for single lookups; API GUID required for web services (free registration) |
| Rate limit | None documented for web service users |
| Notes      | ABN is the primary Australian business identifier (~10M ABNs). REST + SOAP APIs available. Bulk ABN data available for free download (ABR bulk data extract). Covers sole traders, companies, partnerships, trusts, government entities, superannuation funds. |

### ASIC Companies Register (via data.gov.au)
| Field      | Detail |
|------------|--------|
| URL        | https://data.gov.au/data/dataset/asic-companies-and-managed-investment-schemes |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | ACN (9-digit), company name, type, class, subclass, status, registered date, deregistered date, registered state, ABN |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | ~3.5M companies. Updated monthly on data.gov.au. ACN is ASIC's identifier; different from ABN. Certified ASIC documents (company extract) cost AUD 9 via ASIC Connect. Full company details with officeholders available via ASIC Connect portal (paid per search). |

### ASIC Connect
| Field      | Detail |
|------------|--------|
| URL        | https://connectonline.asic.gov.au |
| Access     | Paid per-document |
| Cost       | AUD 9 per company extract; AUD 26 per current officeholder search |
| Fields     | Officeholders (directors, secretaries), shareholders, charges, registered office, share capital |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | No bulk API. Authoritative source for director/officeholder information. |

## Commercial Providers

### Illion (formerly Dun & Bradstreet Australia)
| Field      | Detail |
|------------|--------|
| URL        | https://www.illion.com.au |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | ABN, ACN, company name, credit score, directors, financials, payment behaviour, adverse events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Dominant commercial credit bureau in Australia. Rebranded from D&B Australia. Strong for credit risk and KYB. |

### ASIC Edge / InfoTrack
| Field      | Detail |
|------------|--------|
| URL        | https://www.infotrack.com.au |
| Access     | API (paid) |
| Cost       | Paid — per-search pricing |
| Fields     | ASIC company extract, officeholders, charges, PPSR registrations |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Legal industry-focused company search and document retrieval. Useful for compliance and due diligence workflows. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from ASIC bulk data; no advantage over free direct ASIC/ABR access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | ASX-listed companies well covered; good for financial services sector |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: ABN Lookup API (free, no auth for single lookups) + ASIC bulk CSV for ACN-based data
- Priority: High
