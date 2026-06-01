# Switzerland (CH) — GDP Rank #20

> **Summary:** Zefix (Zentraler Firmenindex) provides an excellent free REST API with no authentication required, covering all Swiss commercial register entries with the UID (CHE-xxx.xxx.xxx) as the key identifier. This is one of the best free national registries in the world for programmatic access.

## Official Registry

### Zefix — Central Business Name Index (Zentraler Firmenindex)
| Field      | Detail |
|------------|--------|
| URL        | https://www.zefix.ch / https://www.zefix.ch/ZefixREST/api/v1/firm/search.json |
| Access     | API (free) |
| Cost       | Free |
| Fields     | UID (CHE-xxx.xxx.xxx format), company name (German/French/Italian/Romansh variants), legal form, registered address (municipality, canton), registration date, deletion date, status, canton of registration, SOGC/FOSC publication reference |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Zefix aggregates all 26 cantonal commercial registers (Handelsregister). UID format: CHE-123.456.789 (CHE prefix + 9 digits in groups of 3). REST API at https://www.zefix.ch/ZefixREST/api/v1/. Full text search, UID lookup, and list endpoints available. Bulk XML export also available. Excellent data quality. Updated in real time from cantonal inputs. |

### Cantonal Commercial Registers (Handelsregisterämter)
| Field      | Detail |
|------------|--------|
| URL        | Varies by canton (e.g., Zurich: https://handelsregister.zh.ch, Bern: https://www.bernregistration.ch) |
| Access     | Web portal (per canton) |
| Cost       | Free search; CHF 17–35 for certified excerpts |
| Fields     | Full company detail: directors, capital structure, purpose, signatures, commercial purposes |
| Auth       | None for search |
| Rate limit | N/A |
| Notes      | Cantons maintain the authoritative records; Zefix aggregates their current state. For director-level data and certified documents, cantonal registers are the source. HR-Auszug (commercial register excerpt) is the standard certified document. |

### UID Register (Federal Statistical Office)
| Field      | Detail |
|------------|--------|
| URL        | https://www.uid.admin.ch |
| Access     | API (free) |
| Cost       | Free |
| Fields     | UID, entity name, legal form, municipality, canton, status, category (company/foundation/association) |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Broader than Zefix — includes non-commercial entities (foundations, associations, public bodies). UID is Switzerland's universal enterprise identifier used across tax, VAT, and statistical systems. REST API available (verify endpoint at uid.admin.ch). |

## Commercial Providers

### Creditreform Switzerland
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.ch |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | UID, credit score, financials, directors, payment behaviour, legal events |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of the European Creditreform network. Primary Swiss commercial credit bureau. Essential for credit risk assessments. |

### Bisnode Switzerland
| Field      | Detail |
|------------|--------|
| URL        | verify URL |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | UID, financials, shareholders, group structure, compliance screening |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | D&B/Bisnode Switzerland offers enriched financial and corporate structure data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Zefix; no advantage over free direct Zefix API |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Swiss financial sector has very high LEI adoption; SMI-listed companies well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Zefix REST API (free, no auth, excellent quality — top-tier free registry)
- Priority: High
