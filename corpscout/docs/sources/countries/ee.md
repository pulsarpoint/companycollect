# Estonia (EE) — GDP Rank #89

> **Summary:** Ariregister (Estonian Business Register) provides a free REST API. Estonia's e-governance infrastructure is excellent — clean API, good documentation.

## Official Registry

### Ariregister (Estonian Business Register)
| Field      | Detail |
|------------|--------|
| URL        | https://ariregister.rik.ee/api |
| Access     | API (free) |
| Cost       | Free |
| Fields     | registration_code, name, status, legal_form, address, founded_date, deleted_date, activities, persons (board members, shareholders) |
| Auth       | None |
| Rate limit | None documented |
| Notes      | Also available at https://avaandmed.ariregister.rik.ee/ for open data bulk downloads. Full company history accessible. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Ariregister — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `ariregister`
- Recommended source: Ariregister API (free, no auth)
- Priority: Complete
