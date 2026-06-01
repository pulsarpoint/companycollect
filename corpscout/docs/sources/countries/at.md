# Austria (AT) — GDP Rank #29

> **Summary:** The Firmenbuch (Austrian Company Register) provides free per-company search via firmenbuch.at; there is no public bulk API or free data dump. Commercial providers KSV1870 and Creditreform Austria cover enrichment needs.

## Official Registry

### Firmenbuch (Austrian Company Register)
| Field      | Detail |
|------------|--------|
| URL        | https://www.firmenbuch.at |
| Access     | Scrape |
| Cost       | Free (basic search); paid for full extract |
| Fields     | company name, FN (Firmenbuchnummer), legal form, status, registered address, directors |
| Auth       | None for basic search |
| Rate limit | Low (manual search portal) |
| Notes      | Official search at firmenbuch.at/fb/faces/index.xhtml; full extracts cost ~€5–14 per company; no bulk export or API |

### Justiz.gv.at (Unternehmensregister)
| Field      | Detail |
|------------|--------|
| URL        | https://www.justiz.gv.at/home/service/firmenbuch.2c94848542d6f0250143219d4c370007.de.html |
| Access     | Scrape |
| Cost       | Free search |
| Fields     | company name, FN number, legal form, address |
| Auth       | None |
| Rate limit | Low |
| Notes      | Alternative portal; links through to same underlying Firmenbuch data |

## Commercial Providers

### KSV1870
| Field      | Detail |
|------------|--------|
| URL        | https://www.ksv.at |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, FN number, address, directors, credit score, payment history |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Major Austrian credit bureau; strong local coverage |

### Creditreform Austria
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.at |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, number, legal form, address, financials, credit rating |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Part of Creditreform network; good pan-European integration |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Good coverage of Austrian companies scraped from Firmenbuch |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Firmenbuch scrape for basic data; KSV1870 API for enrichment
- Priority: Medium
