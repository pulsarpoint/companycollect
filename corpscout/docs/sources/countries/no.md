# Norway (NO) — GDP Rank #27

> **Summary:** Brreg (Brønnøysundregistrene) provides a free REST API with full company data including roles, addresses, and industry codes. Excellent quality and reliability.

## Official Registry

### Brreg (Brønnøysundregistrene)
| Field      | Detail |
|------------|--------|
| URL        | https://data.brreg.no/enhetsregisteret/api/enheter |
| Access     | API (free) |
| Cost       | Free |
| Fields     | organisasjonsnummer, name, status, orgform, naeringskode (NACE), forretningsadresse, postadresse, stiftelsesdato, antallAnsatte, overordnetEnhet, registreringsdatoEnhetsregisteret |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Full bulk download also available. Pagination via `?size=100&page=0`. Date-based filtering with `?fraRegistreringsdatoEnhetsregisteret=YYYY-MM-DD`. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Brreg — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `brreg`
- Recommended source: Brreg API (free, no auth, paginated)
- Priority: Complete
