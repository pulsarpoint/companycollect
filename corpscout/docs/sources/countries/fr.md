# France (FR) — GDP Rank #7

> **Summary:** INSEE SIRENE API is the gold standard — free with a free API key, covering 30M+ legal units (SIREN/SIRET), updated daily. Infogreffe provides certified legal documents. For broad programmatic access, SIRENE is the first choice.

## Official Registry

### INSEE SIRENE (Système National d'Identification et du Répertoire des Entreprises)
| Field      | Detail |
|------------|--------|
| URL        | https://api.insee.fr/entreprises/sirene/V3/ |
| Access     | API (free, registration required) |
| Cost       | Free |
| Fields     | SIREN (9-digit), SIRET (14-digit = SIREN + NIC), company name, trade name (enseigne), legal category, NAF/APE activity code, address (full), creation date, cessation date, status, employee size bracket, VAT number |
| Auth       | API key (free registration at api.insee.fr) |
| Rate limit | 30 requests/minute on free tier; higher limits available |
| Notes      | 30M+ legal units including associations, public bodies, and sole traders. SIREN identifies legal entities; SIRET identifies each establishment. Bulk download (fichier Sirene) available at https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/ (open data, monthly CSV). Excellent data quality and coverage. |

### Infogreffe
| Field      | Detail |
|------------|--------|
| URL        | https://www.infogreffe.fr |
| Access     | API (paid) / Paid document retrieval |
| Cost       | Paid — per-document fees (EUR 3–15); API via subscription |
| Fields     | Kbis extract (certified company status), RCS number, directors, shareholders, capital, activity, filing history |
| Auth       | Registration required; API key for programmatic access |
| Rate limit | Per plan |
| Notes      | Infogreffe is operated by the network of commercial court registries (greffes). Kbis is the official French company certificate. Required for many legal/compliance processes. Provides real-time certified data. |

### BODACC (Bulletin Officiel des Annonces Civiles et Commerciales)
| Field      | Detail |
|------------|--------|
| URL        | https://www.bodacc.fr / https://api.piste.gouv.fr/dila/bodacc/v1/annonces |
| Access     | API (free) |
| Cost       | Free |
| Fields     | Company name, SIREN, registration, liquidation, procedure collective (insolvency), sale of business announcements |
| Auth       | Free API key via PISTE platform |
| Rate limit | None documented |
| Notes      | Official legal gazette for commercial announcements. Excellent for insolvency and status change monitoring. Data goes back to 1985. |

## Commercial Providers

### Société.com / Ellisphere
| Field      | Detail |
|------------|--------|
| URL        | https://www.societe.com / https://www.ellisphere.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | SIREN, financials (annual accounts), executives, shareholders, credit score, sectoral analysis |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Société.com is a popular web aggregator; Ellisphere is the professional B2B data arm. Financial statement data (bilans) is a key differentiator. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from SIRENE; no advantage over the free direct SIRENE API |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed French companies only; CAC 40 and SBF 120 well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: INSEE SIRENE API (free with API key, 30M+ entities, daily updates)
- Priority: High
