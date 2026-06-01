# Canada (CA) — GDP Rank #9

> **Summary:** Corporations Canada provides a free federal bulk download covering federally incorporated companies. Provincial registries (Ontario, BC, Alberta) maintain their own data with varying access levels. The Business Number (BN) from CRA is the tax identifier; the corporation number is the registry identifier.

## Official Registry

### Corporations Canada (Federal)
| Field      | Detail |
|------------|--------|
| URL        | https://ised-isde.canada.ca/cc/lgcy/fdrl/srch/ |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | Corporation number, company name, legal form, status (active/dissolved), incorporation date, province/territory, registered office address |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | Covers federally-incorporated companies only (~300k entities). CSV bulk export available. Many businesses incorporate provincially and don't appear here. Search UI also available at https://ised-isde.canada.ca/cc/lgcy/fdrl/srch/. |

### Ontario Business Registry (ServiceOntario)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ontario.ca/page/ontario-business-registry |
| Access     | Scrape / API (limited) |
| Cost       | Free search; paid document retrieval |
| Fields     | Business name, Ontario corporation number, business type, status, registered address, directors |
| Auth       | None for basic search |
| Rate limit | Undocumented |
| Notes      | Ontario is home to the largest share of Canadian corporations. Basic search free; detailed records require account and fees. No official bulk download. |

### BC Registry (BC Registries and Online Services)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bcregistry.gov.bc.ca |
| Access     | API (free, registration required) |
| Cost       | Free for basic; paid for certified documents |
| Fields     | Company name, BC company number, legal type, status, registered address, directors, officers |
| Auth       | BC Services Card / account registration |
| Rate limit | None documented |
| Notes      | BC has an API (verify URL and access details at https://www.bcregistry.gov.bc.ca). Among the more developer-friendly provincial registries. |

### Alberta Corporations Registry (CORES)
| Field      | Detail |
|------------|--------|
| URL        | https://www.alberta.ca/corporate-registry-search.aspx |
| Access     | Scrape / Paid document retrieval |
| Cost       | Free search; CAD 10/document |
| Fields     | Company name, Alberta corporate access number, legal type, status, address |
| Auth       | None for search |
| Rate limit | Undocumented |
| Notes      | Alberta has a large resource/energy sector. No bulk download or public API. |

## Commercial Providers

### Dun & Bradstreet Canada
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com/en-ca.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, company name, BN, address, financials, credit risk, corporate family tree |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Best option for private company financials and cross-border corporate family data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Aggregates federal + all provincial registries; useful alternative to integrating each province separately |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed Canadian companies; TSX-listed firms well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Corporations Canada bulk download for federal companies; OpenCorporates for pan-provincial coverage
- Priority: High
