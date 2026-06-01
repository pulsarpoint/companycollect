# Thailand (TH) — GDP Rank #26

> **Summary:** Thailand's DBD (Department of Business Development, Ministry of Commerce) is the official company registry. No free public bulk API exists. The DBD DataWarehouse has some paid data products. The e-Registration system exists for company registration but is not open for programmatic queries by third parties.

## Official Registry

### DBD — Department of Business Development (กรมพัฒนาธุรกิจการค้า)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dbd.go.th |
| Access     | Web portal (no public bulk API) |
| Cost       | Free search; paid certified documents |
| Fields     | Juristic person ID (เลขทะเบียนนิติบุคคล, 13-digit), company name (Thai/English), legal form (บริษัทจำกัด/ห้างหุ้นส่วน/etc.), registered address, capital, objectives, directors, registration date, status |
| Auth       | None for basic search |
| Rate limit | Undocumented |
| Notes      | Juristic person ID is the primary identifier (13-digit, same format as Thai national ID). DBD manages company registration for all Thai juristic persons. Website: https://www.dbd.go.th/main.php?filename=index. Company search available at https://www.dbd.go.th/main.php?filename=co_search. No bulk download or public API documented. |

### DBD DataWarehouse (คลังข้อมูลธุรกิจ)
| Field      | Detail |
|------------|--------|
| URL        | https://datawarehouse.dbd.go.th |
| Access     | Web portal (paid data products) |
| Cost       | Free basic access; paid for detailed reports |
| Fields     | Juristic person ID, company name, financial statements (for companies that file), capital, industry sector, director list (for paid reports) |
| Auth       | Registration required |
| Rate limit | N/A |
| Notes      | DBD DataWarehouse provides statistical and business intelligence data products. Some free dashboards. Financial data limited to companies that submit annual financial statements to DBD. No public API. |

### SET — Stock Exchange of Thailand (Listed Companies)
| Field      | Detail |
|------------|--------|
| URL        | https://www.set.or.th/en/market/product/stock/quote |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Company name, ticker, industry group, market sector, financial disclosures, annual reports |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Covers ~700 SET/mai-listed companies. SET Open API available at https://developer.settrade.com/ (verify URL). Good for listed company financial data. |

## Commercial Providers

### D&B Thailand / Dun & Bradstreet
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS, juristic person ID, company name, address, directors, financials (limited), credit risk |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Limited private-company coverage for Thailand; better for listed/large companies with filed financials. |

### Bureau van Dijk (Orbis)
| Field      | Detail |
|------------|--------|
| URL        | https://orbis.bvdinfo.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | Company profile, financials, shareholders, group structure |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Covers major Thai companies with annual report data. Better coverage than D&B for regional Asian companies. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage for Thailand is limited; data sourced from DBD scrapes; completeness uncertain |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | SET-listed companies and financial institutions covered; overall LEI adoption low |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: DBD web for individual lookups; SET Open API for listed companies; no good free bulk option
- Priority: Medium
