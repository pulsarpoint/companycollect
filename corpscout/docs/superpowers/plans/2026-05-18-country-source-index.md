# Country Source Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `docs/sources/countries/` with one markdown file per country for the top 100 countries by GDP, documenting all known company data sources (access method, cost, fields, auth) plus a master README index.

**Architecture:** Pure documentation — no code changes. Each country file follows a fixed markdown template with sections for Official Registry, Commercial Providers, and Aggregators, plus a Corpscout Status footer. A master README links all 100 files. Research is done via web search for each country's official business register and known commercial providers.

**Tech Stack:** Markdown, web search, git

---

## File Map

```
docs/sources/
├── README.md                        # Master index table
└── countries/
    ├── us.md   ├── cn.md   ├── de.md   ├── jp.md   ├── in.md
    ├── gb.md   ├── fr.md   ├── it.md   ├── ca.md   ├── br.md
    ├── ru.md   ├── kr.md   ├── au.md   ├── mx.md   ├── es.md
    ├── id.md   ├── nl.md   ├── sa.md   ├── tr.md   ├── ch.md
    ├── tw.md   ├── pl.md   ├── ar.md   ├── se.md   ├── be.md
    ├── th.md   ├── no.md   ├── il.md   ├── at.md   ├── ae.md
    ├── ng.md   ├── sg.md   ├── my.md   ├── za.md   ├── eg.md
    ├── bd.md   ├── ph.md   ├── dk.md   ├── pk.md   ├── vn.md
    ├── co.md   ├── hk.md   ├── ro.md   ├── cz.md   ├── cl.md
    ├── nz.md   ├── fi.md   ├── pt.md   ├── pe.md   ├── kz.md
    ├── gr.md   ├── iq.md   ├── dz.md   ├── hu.md   ├── qa.md
    ├── ma.md   ├── sk.md   ├── et.md   ├── ke.md   ├── kw.md
    ├── ec.md   ├── do.md   ├── uz.md   ├── tz.md   ├── gt.md
    ├── om.md   ├── ao.md   ├── bg.md   ├── gh.md   ├── lt.md
    ├── ci.md   ├── rs.md   ├── lu.md   ├── hr.md   ├── by.md
    ├── az.md   ├── mm.md   ├── ug.md   ├── lk.md   ├── si.md
    ├── tn.md   ├── jo.md   ├── cm.md   ├── ly.md   ├── bh.md
    ├── bo.md   ├── py.md   ├── lv.md   ├── ee.md   ├── np.md
    ├── sn.md   ├── al.md   ├── mz.md   ├── zm.md   ├── cd.md
    └── am.md   └── cy.md   └── hn.md   └── is.md   └── ba.md
```

## Template (copy exactly for each file)

```markdown
# [Country] ([ISO2]) — GDP Rank #[N]

> **Summary:** [one or two sentences on the best path to company data]

## Official Registry

### [Registry Name]
| Field      | Detail |
|------------|--------|
| URL        | https://... |
| Access     | API (free) / Bulk download / Scrape / Free (registration required) |
| Cost       | Free / Paid / Contact for pricing |
| Fields     | name, registration number, address, status, ... |
| Auth       | None / API key / Registration |
| Rate limit | None / [specifics] |
| Notes      | ... |

## Commercial Providers

### [Provider Name]
| Field      | Detail |
|------------|--------|
| URL        | https://... |
| Access     | API (paid) |
| Cost       | Paid — [price if known, else "contact for pricing"] |
| Fields     | ... |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | ... |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | ... |

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
- Recommended source: [which source and why]
- Priority: High / Medium / Low
```

---

## Task 1: Scaffold folder structure + README skeleton

**Files:**
- Create: `docs/sources/README.md`
- Create: `docs/sources/countries/` (directory)

- [ ] Create `docs/sources/README.md` with this exact content:

```markdown
# Corpscout — Country Source Index

Top 100 countries by GDP (IMF 2024 nominal). One file per country documenting
all known company data sources: access method, cost, fields, and adapter status.

To update: edit the country file directly and update the row below if the
"Best Free Option" or "API?" column changes.

| Rank | Country | ISO2 | Best Free Option | Free API? | Adapter |
|------|---------|------|-----------------|-----------|---------|
| 1 | United States | US | SEC EDGAR / state registries | Partial | — |
| 2 | China | CN | SAMR search | No | — |
| 3 | Germany | DE | Handelsregister (offeneregister.de bulk) | Bulk DL | — |
| 4 | Japan | JP | National Tax Agency (CSV) | Bulk DL | — |
| 5 | India | IN | MCA21 (Ministry of Corporate Affairs) | Partial | — |
| 6 | United Kingdom | GB | Companies House | Yes | `companies_house` |
| 7 | France | FR | Infogreffe / INSEE SIRENE | Yes (SIRENE) | — |
| 8 | Italy | IT | Registro Imprese (scrape) | No | — |
| 9 | Canada | CA | Corporations Canada (federal) | Bulk DL | — |
| 10 | Brazil | BR | CNPJ (Receita Federal bulk) | Bulk DL | — |
| 11 | Russia | RU | EGRUL (Federal Tax Service) | Bulk DL | — |
| 12 | South Korea | KR | DART / BizInfo | Partial | — |
| 13 | Australia | AU | ASIC / ABN Lookup | Yes | — |
| 14 | Mexico | MX | SAT / Registro Público Comercio | No | — |
| 15 | Spain | ES | BORME / Registro Mercantil | Scrape | — |
| 16 | Indonesia | ID | AHU Online (Ministry of Law) | No | — |
| 17 | Netherlands | NL | KVK (Chamber of Commerce) | API (paid) | — |
| 18 | Saudi Arabia | SA | Ministry of Commerce | No | — |
| 19 | Turkey | TR | MERSİS / Trade Registry | No | — |
| 20 | Switzerland | CH | Zefix (federal) | Yes | — |
| 21 | Taiwan | TW | Ministry of Economic Affairs | Bulk DL | — |
| 22 | Poland | PL | KRS (National Court Register) | Yes | — |
| 23 | Argentina | AR | IGJ / provincial registries | No | — |
| 24 | Sweden | SE | Bolagsverket | API (paid) | — |
| 25 | Belgium | BE | CBE (Crossroads Bank) | API (free) | — |
| 26 | Thailand | TH | DBD (Department of Business Development) | No | — |
| 27 | Norway | NO | Brreg | Yes | `brreg` |
| 28 | Israel | IL | Companies Registrar | No | — |
| 29 | Austria | AT | Firmenbuch | Scrape | — |
| 30 | UAE | AE | MoCIAT / Dubai DED | No | — |
| 31 | Nigeria | NG | CAC (Corporate Affairs Commission) | No | — |
| 32 | Singapore | SG | ACRA (BizFile+) | API (paid) | — |
| 33 | Malaysia | MY | SSM (Companies Commission) | No | — |
| 34 | South Africa | ZA | CIPC | Bulk DL | — |
| 35 | Egypt | EG | GAFI | No | — |
| 36 | Bangladesh | BD | RJSC | No | — |
| 37 | Philippines | PH | SEC (scrape) | No | — |
| 38 | Denmark | DK | CVR | Yes | `cvr` |
| 39 | Pakistan | PK | SECP | No | — |
| 40 | Vietnam | VN | National Business Registration Portal | No | — |
| 41 | Colombia | CO | Confecámaras / RUE | No | — |
| 42 | Hong Kong | HK | Companies Registry | Bulk DL | — |
| 43 | Romania | RO | ONRC | Scrape | — |
| 44 | Czech Republic | CZ | OR (Obchodní rejstřík) | Yes | — |
| 45 | Chile | CL | CBR / SII | Partial | — |
| 46 | New Zealand | NZ | Companies Office | Yes | — |
| 47 | Finland | FI | PRH (Finnish Patent and Registration Office) | Yes | — |
| 48 | Portugal | PT | Racius / IRN | Scrape | — |
| 49 | Peru | PE | SUNARP | No | — |
| 50 | Kazakhstan | KZ | Business Information System | No | — |
| 51 | Greece | GR | GEMI (General Commercial Registry) | Yes | — |
| 52 | Iraq | IQ | Ministry of Trade | No | — |
| 53 | Algeria | DZ | CNRC | No | — |
| 54 | Hungary | HU | Cégbíróság (Company Court) | Yes | — |
| 55 | Qatar | QA | Ministry of Commerce | No | — |
| 56 | Morocco | MA | RC (Registre de Commerce) | No | — |
| 57 | Slovakia | SK | Obchodný register | Yes | — |
| 58 | Ethiopia | ET | Ministry of Trade | No | — |
| 59 | Kenya | KE | eCitizen / BRS | No | — |
| 60 | Kuwait | KW | Ministry of Commerce | No | — |
| 61 | Ecuador | EC | Superintendencia de Compañías | Bulk DL | — |
| 62 | Dominican Republic | DO | Registro Mercantil | No | — |
| 63 | Uzbekistan | UZ | My.gov.uz | No | — |
| 64 | Tanzania | TZ | BRELA | No | — |
| 65 | Guatemala | GT | Registro Mercantil | No | — |
| 66 | Oman | OM | Ministry of Commerce | No | — |
| 67 | Angola | AO | IGAPE | No | — |
| 68 | Bulgaria | BG | Commercial Register (BRRA) | Yes | — |
| 69 | Ghana | GH | ORC (Registrar General) | No | — |
| 70 | Lithuania | LT | Registrų centras (RC) | Yes | — |
| 71 | Côte d'Ivoire | CI | CEPICI | No | — |
| 72 | Serbia | RS | APR portal (free registration) | No* | — |
| 73 | Luxembourg | LU | LBR (Luxembourg Business Register) | API (paid) | — |
| 74 | Croatia | HR | Sudski registar | Yes | — |
| 75 | Belarus | BY | Ministry of Justice | No | — |
| 76 | Azerbaijan | AZ | Ministry of Economy | No | — |
| 77 | Myanmar | MM | DICA | No | — |
| 78 | Uganda | UG | URSB | No | — |
| 79 | Sri Lanka | LK | ROC | No | — |
| 80 | Slovenia | SI | AJPES | Yes | — |
| 81 | Tunisia | TN | RNE | No | — |
| 82 | Jordan | JO | Ministry of Industry & Trade | No | — |
| 83 | Cameroon | CM | CFCE | No | — |
| 84 | Libya | LY | — | No | — |
| 85 | Bahrain | BH | MOICT Sijilat | No | — |
| 86 | Bolivia | BO | SEPREC | No | — |
| 87 | Paraguay | PY | MIC / SET | No | — |
| 88 | Latvia | LV | Lursoft / UR | Partial | — |
| 89 | Estonia | EE | Ariregister | Yes | `ariregister` |
| 90 | Nepal | NP | OCR | No | — |
| 91 | Senegal | SN | APIX | No | — |
| 92 | Albania | AL | QKB | Yes | — |
| 93 | Mozambique | MZ | CPAR | No | — |
| 94 | Zambia | ZM | PACRA | No | — |
| 95 | DR Congo | CD | OHADA / GUICHET UNIQUE | No | — |
| 96 | Armenia | AM | e-register.am | Yes | — |
| 97 | Cyprus | CY | Registrar of Companies | Bulk DL | — |
| 98 | Honduras | HN | Registro Mercantil | No | — |
| 99 | Iceland | IS | Hlutafélagaskrá (RSK) | Yes | — |
| 100 | Bosnia & Herzegovina | BA | APIF / JPRS | Partial | — |
```

- [ ] Commit:

```bash
git add docs/sources/README.md
git commit -m "docs: add country source index README skeleton"
```

---

## Task 2: Known countries — GB, NO, DK, EE, RS

**Files:**
- Create: `docs/sources/countries/gb.md`
- Create: `docs/sources/countries/no.md`
- Create: `docs/sources/countries/dk.md`
- Create: `docs/sources/countries/ee.md`
- Create: `docs/sources/countries/rs.md`

- [ ] Create `docs/sources/countries/gb.md`:

```markdown
# United Kingdom (GB) — GDP Rank #6

> **Summary:** Companies House provides a fully free REST API with bulk download. Best official registry API in the world — use it directly.

## Official Registry

### Companies House
| Field      | Detail |
|------------|--------|
| URL        | https://developer.company-information.service.gov.uk |
| Access     | API (free) |
| Cost       | Free |
| Fields     | company name, company number, status, company type, incorporation date, registered address, SIC codes, officers, filing history |
| Auth       | API key (free registration at developer.company-information.service.gov.uk) |
| Rate limit | 600 requests/5 min per key |
| Notes      | Bulk data snapshots also available at https://download.companieshouse.gov.uk/. Full accounts and confirmation statements accessible. |

## Commercial Providers

### Creditsafe
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/gb/en.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | All Companies House fields + credit score, financials, directors, CCJs |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Adds credit risk scoring on top of official data |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Companies House directly — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `companies_house`
- Recommended source: Companies House API (free, excellent quality)
- Priority: Complete
```

- [ ] Create `docs/sources/countries/no.md`:

```markdown
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
```

- [ ] Create `docs/sources/countries/dk.md`:

```markdown
# Denmark (DK) — GDP Rank #38

> **Summary:** CVR (Central Business Register) provides a free ElasticSearch-based API with comprehensive company data. Requires free registration to get a token.

## Official Registry

### CVR (Centrale Virksomhedsregister)
| Field      | Detail |
|------------|--------|
| URL        | https://cvrapi.dk / https://data.virk.dk |
| Access     | API (free) |
| Cost       | Free |
| Fields     | cvr_number, name, status, company_type, address, phone, email, industry_code, founded_date, employees, owners, directors |
| Auth       | HTTP Basic auth with free CVR account (register at data.virk.dk) |
| Rate limit | None documented for registered users |
| Notes      | Full bulk dump available. ElasticSearch query endpoint: https://distribution.virk.dk/cvr-permanent/virksomhed/_search. Includes P-units (production units) separately. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from CVR — no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only |

## Corpscout Status
- [x] Adapter implemented
- Source name: `cvr`
- Recommended source: CVR API (free with registration, ElasticSearch)
- Priority: Complete
```

- [ ] Create `docs/sources/countries/ee.md`:

```markdown
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
```

- [ ] Create `docs/sources/countries/rs.md`:

```markdown
# Serbia (RS) — GDP Rank #72

> **Summary:** APR (Agencija za privredne registre) is the authoritative source. No public API yet (listed as "under preparation" as of May 2026). Free registration at portal-info.apr.gov.rs gives access to data downloads. Direct web scraping of pretraga.apr.gov.rs is the current best free option.

## Official Registry

### APR — portal-info.apr.gov.rs
| Field      | Detail |
|------------|--------|
| URL        | https://portal-info.apr.gov.rs |
| Access     | Free (registration required) + data downloads |
| Cost       | Free |
| Fields     | name, MB (registration number), PIB (tax ID), address, status, legal form, beneficial owners, financial reports, pledges, insolvency |
| Auth       | Free account registration |
| Rate limit | Unknown |
| Notes      | 133,519 companies + 378,442 entrepreneurs + 263,102 financial reports + 208,946 beneficial owners. API listed as "under preparation" as of May 2026. Bulk export available via portal. |

### APR — pretraga.apr.gov.rs (public search)
| Field      | Detail |
|------------|--------|
| URL        | https://pretraga.apr.gov.rs |
| Access     | Scrape |
| Cost       | Free |
| Fields     | name, MB, address, status, legal form |
| Auth       | None |
| Rate limit | Unknown — anti-bot measures present |
| Notes      | Public search portal. Certificate issues as of May 2026 (self-signed cert). This is the same source OpenCorporates uses to get Serbian data. |

### APR — Web Service (contracted)
| Field      | Detail |
|------------|--------|
| URL        | https://www.apr.gov.rs (contact via portal.pomoc@apr.gov.rs) |
| Access     | API (paid) |
| Cost       | Paid — requires contract with APR |
| Fields     | Full database |
| Auth       | Contract-based credentials |
| Rate limit | Per contract |
| Notes      | Automated data delivery service. Pricing not published. Contact APR directly. |

## Commercial Providers

### CompanyWall Serbia
| Field      | Detail |
|------------|--------|
| URL        | https://www.companywall.rs |
| Access     | Web portal only (no API) |
| Cost       | Paid — ~€290/6mo, ~€376/year, ~€615/year (Ultimate) |
| Fields     | name, MB, PIB, address, credit score, financial reports, ownership, insolvency, tax debt, import/export |
| Auth       | Subscription account |
| Rate limit | N/A — web portal only |
| Notes      | Bulk export up to 5,000 companies. All data sourced from APR — middleman with no added value for programmatic access. |

### CompanyWall European API
| Field      | Detail |
|------------|--------|
| URL        | https://www.companywall.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing (not published) |
| Fields     | name, address, credit score, financials, directors, ownership |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Covers Serbia + 8 other Balkan/European countries. Enterprise pricing. Same underlying APR data. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Scrapes pretraga.apr.gov.rs directly. Low quality (openness score 40/100). No financial data. More expensive than going to APR directly. |

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
- Recommended source: APR portal-info (free registration + downloads) now; switch to APR API when it launches
- Priority: High
```

- [ ] Commit:

```bash
git add docs/sources/countries/gb.md docs/sources/countries/no.md \
        docs/sources/countries/dk.md docs/sources/countries/ee.md \
        docs/sources/countries/rs.md
git commit -m "docs: add country source files for GB, NO, DK, EE, RS"
```

---

## Task 3: Tier 1 research — US, CN, DE, JP, IN (ranks 1–5)

**Research instructions for each country:**
- Search: `"[country] official business register API" site:gov OR site:business`
- Search: `"[country] company registry open data bulk download"`
- Check if country has a GLEIF bulk data participant
- Note: For US, federal + state registries exist separately

**Files:** Create `docs/sources/countries/us.md`, `cn.md`, `de.md`, `jp.md`, `in.md`

- [ ] Research and create `docs/sources/countries/us.md` — key sources to verify:
  - SEC EDGAR: https://efts.sec.gov/LATEST/search-index?q=%22companies%22&dateRange=custom — free API, federal public companies
  - OpenSecrets/FEIN: state-level business registries (each state has own)
  - IRS EIN registry: not public
  - OpenCorporates covers all 50 states

```markdown
# United States (US) — GDP Rank #1

> **Summary:** No single national company registry. Federal level: SEC EDGAR covers public companies free. State level: each of 50 states runs its own registry, most with free search portals and some with bulk downloads. Best free bulk coverage via OpenSyllabus or state-specific downloads.

## Official Registry

### SEC EDGAR (federal — public companies only)
| Field      | Detail |
|------------|--------|
| URL        | https://efts.sec.gov/LATEST/search-index?q=&dateRange=custom&startdt=2024-01-01 |
| Access     | API (free) |
| Cost       | Free |
| Fields     | company name, CIK, SIC code, state of incorporation, filings |
| Auth       | None (User-Agent header required) |
| Rate limit | 10 requests/second |
| Notes      | Public companies and investment funds only (~12,000 active filers). Full bulk download at https://www.sec.gov/Archives/edgar/full-index/. |

### State registries (varies per state)
| Field      | Detail |
|------------|--------|
| URL        | Varies — e.g. https://data.delaware.gov (DE), https://www.sos.ca.gov/business-programs (CA) |
| Access     | Varies — scrape / bulk download / API per state |
| Cost       | Free (most states) |
| Fields     | name, entity number, status, registered agent, state of formation, date |
| Auth       | None (most) |
| Rate limit | Varies |
| Notes      | Delaware most important (~67% of Fortune 500 incorporated there). California has bulk download. ~6M+ total active entities across all states. |

## Commercial Providers

### Dun & Bradstreet
| Field      | Detail |
|------------|--------|
| URL        | https://developer.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | DUNS number, name, address, SIC, employees, revenue, credit score, subsidiaries |
| Auth       | API key (OAuth2) |
| Rate limit | Per plan |
| Notes      | Most comprehensive US commercial data. DUNS number is de facto standard for B2B identity. |

### LexisNexis / Dun & Bradstreet / Creditsafe
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditsafe.com/us/en.html |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, address, credit score, financials, officers, filings |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Alternative to D&B with similar coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Aggregates all 50 state registries. Good coverage but expensive vs going direct per state. |

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
- Recommended source: SEC EDGAR for public companies; Delaware bulk download for broad coverage; state-by-state approach for full coverage
- Priority: High
```

- [ ] Research and create `docs/sources/countries/cn.md` — key sources to verify:
  - SAMR (State Administration for Market Regulation): national business register
  - CNIPA for trademarks
  - Check for bulk download or API at gsxt.gov.cn

```markdown
# China (CN) — GDP Rank #2

> **Summary:** SAMR (国家市场监督管理总局) is the national registry. Public search available at gsxt.gov.cn but no public bulk API. Data access for foreigners is heavily restricted. Commercial providers (Qichacha, Tianyancha) offer APIs but require Chinese business registration for full access.

## Official Registry

### SAMR — National Enterprise Credit Information System
| Field      | Detail |
|------------|--------|
| URL        | https://gsxt.gov.cn |
| Access     | Scrape (difficult — CAPTCHA, rate limiting) |
| Cost       | Free |
| Fields     | company name, unified social credit code (USCC), status, legal representative, registered capital, address, business scope |
| Auth       | None (public search) |
| Rate limit | Heavy anti-scraping measures |
| Notes      | No official API or bulk download for foreigners. USCC is the 18-digit national business identifier. ~150M+ registered entities. |

## Commercial Providers

### Qichacha (企查查)
| Field      | Detail |
|------------|--------|
| URL        | https://www.qichacha.com |
| Access     | API (paid) |
| Cost       | Paid — requires Chinese business entity for full access |
| Fields     | name, USCC, status, legal rep, capital, shareholders, officers, financials, judicial records |
| Auth       | API key + Chinese business registration |
| Rate limit | Per plan |
| Notes      | Most widely used Chinese business data API. Restricted to entities with Chinese business registration. |

### Tianyancha (天眼查)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tianyancha.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, USCC, status, shareholders, officers, investments, legal cases |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Alternative to Qichacha. Similar restrictions on foreign access. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address |
| Notes  | Limited coverage for China due to access restrictions |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only — better China coverage than most free sources |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: GLEIF for large entities; Qichacha API for full coverage (requires Chinese entity)
- Priority: Medium (access restrictions make full coverage impractical)
```

- [ ] Research and create `docs/sources/countries/de.md`:

```markdown
# Germany (DE) — GDP Rank #3

> **Summary:** Offenes Unternehmensregister (offeneregister.de) provides free JSON bulk downloads of Handelsregister data. This is the best free path. Official Unternehmensregister has per-company document access but no bulk API.

## Official Registry

### Unternehmensregister (official)
| Field      | Detail |
|------------|--------|
| URL        | https://www.unternehmensregister.de |
| Access     | Scrape / per-document download |
| Cost       | Free (basic data) / Paid (certified documents) |
| Fields     | name, registration number, court, legal form, address, status, officers |
| Auth       | None for search; registration for documents |
| Rate limit | Unknown |
| Notes      | No bulk API. Official source. Handelsregister entries by local court (Amtsgericht). |

### Offenes Handelsregister / offeneregister.de (community mirror)
| Field      | Detail |
|------------|--------|
| URL        | https://offeneregister.de / https://github.com/datenguide/offeneregister |
| Access     | Bulk download (JSON) |
| Cost       | Free |
| Fields     | name, registration number, court, legal form, status, officers, registered address, current status |
| Auth       | None |
| Rate limit | N/A — static file download |
| Notes      | Community mirror of official Handelsregister data. ~5.5M companies. Updated periodically. Full JSON dump downloadable. Best free programmatic option. |

## Commercial Providers

### Creditreform
| Field      | Detail |
|------------|--------|
| URL        | https://www.creditreform.de |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, address, credit score, financials, directors, employees, revenue |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Most complete German commercial data including credit risk scoring. |

### SCHUFA / Bisnode
| Field      | Detail |
|------------|--------|
| URL        | https://www.bisnode.de |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, address, credit score, payment history, insolvency flags |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Alternative commercial provider |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Handelsregister. No advantage over free offeneregister.de bulk download. |

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
- Recommended source: offeneregister.de bulk JSON download (free, ~5.5M companies)
- Priority: High
```

- [ ] Research and create `docs/sources/countries/jp.md`:

```markdown
# Japan (JP) — GDP Rank #4

> **Summary:** The National Tax Agency provides a free bulk CSV download of all ~6M registered corporations. Best free option — no API key needed, direct download from NTA.

## Official Registry

### National Tax Agency — Corporate Number Publication Site
| Field      | Detail |
|------------|--------|
| URL        | https://www.houjin-bangou.nta.go.jp/en/ |
| Access     | Bulk download (CSV/XML) + API (free) |
| Cost       | Free |
| Fields     | corporate number (法人番号), name (Japanese + furigana), address (prefecture, city, full), update date, close date, close cause, successor corporate number, change history |
| Auth       | None (API key optional for API access, no key for bulk) |
| Rate limit | None for bulk download |
| Notes      | ~6M corporations. Full bulk download available by prefecture. REST API at https://api.houjin-bangou.nta.go.jp/4/. Updated daily. English name field available for ~10% of entities. |

### Ministry of Justice — Commercial and Corporation Registry
| Field      | Detail |
|------------|--------|
| URL        | https://www.moj.go.jp/MINJI/minji60.html |
| Access     | Scrape / per-document fee |
| Cost       | Free search / Paid certified copies |
| Fields     | directors, shareholders, capital, company history |
| Auth       | None for search |
| Rate limit | Unknown |
| Notes      | More detailed than NTA but no bulk access. Use NTA for bulk coverage. |

## Commercial Providers

### Teikoku Databank (TDB)
| Field      | Detail |
|------------|--------|
| URL        | https://www.tdb.co.jp/en/ |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, address, financials, credit score, directors, shareholders |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Largest Japanese commercial credit data provider. Very comprehensive. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address |
| Notes  | Sources from NTA. No advantage over free direct NTA bulk download. |

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
- Recommended source: NTA Corporate Number API (free, no auth, daily updates, ~6M corporations)
- Priority: High
```

- [ ] Research and create `docs/sources/countries/in.md`:

```markdown
# India (IN) — GDP Rank #5

> **Summary:** Ministry of Corporate Affairs (MCA21) is the authoritative registry. Free company search available at mca.gov.in. No public bulk download API — each company record requires a separate query. Commercial providers (Razorpay Rize, Karza) offer comprehensive APIs.

## Official Registry

### MCA21 — Ministry of Corporate Affairs
| Field      | Detail |
|------------|--------|
| URL        | https://www.mca.gov.in/content/mca/global/en/mca/master-data.html |
| Access     | Bulk download (CSV — master data) + scrape for details |
| Cost       | Free |
| Fields     | CIN (Corporate Identity Number), company name, status, company category, class, date of incorporation, registered state, registered office address, authorized capital, paid-up capital |
| Auth       | None for master data download |
| Rate limit | N/A for bulk download |
| Notes      | Master data CSV available at mca.gov.in/content/mca/global/en/mca/master-data.html — covers ~2.3M companies. Detailed filings require per-company scrape with CAPTCHA. |

## Commercial Providers

### Karza Technologies
| Field      | Detail |
|------------|--------|
| URL        | https://www.karza.in |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | CIN, PAN, GST, name, address, directors, shareholders, financials, credit |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Most widely used KYC/company data API in India. Combines MCA, GST, PAN data. |

### SignalX / IndiaFilings
| Field      | Detail |
|------------|--------|
| URL        | https://www.signalex.ai |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | CIN, name, directors, financials, charges |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Alternative commercial provider |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, CIN, status, address, incorporation date |
| Notes  | Sources from MCA21. No advantage over free MCA master data download. |

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
- Recommended source: MCA21 master data bulk CSV (free, ~2.3M companies); Karza API for enriched data
- Priority: High
```

- [ ] Commit:

```bash
git add docs/sources/countries/us.md docs/sources/countries/cn.md \
        docs/sources/countries/de.md docs/sources/countries/jp.md \
        docs/sources/countries/in.md
git commit -m "docs: add country source files for US, CN, DE, JP, IN (Tier 1 ranks 1-5)"
```

---

## Task 4: Tier 1 research — FR, IT, CA, BR, RU (ranks 7–11)

**Files:** Create `docs/sources/countries/fr.md`, `it.md`, `ca.md`, `br.md`, `ru.md`

**Research approach per country:**
- FR: SIRENE (INSEE) free API, Infogreffe for registry docs
- IT: Registro Imprese — scrape only, Infocamere commercial
- CA: Corporations Canada federal + provincial registries, some bulk downloads
- BR: CNPJ from Receita Federal — free bulk download
- RU: EGRUL from Federal Tax Service — free bulk XML

- [ ] Research and create each file following the template. Key facts:

  **France (fr.md):** SIRENE API by INSEE — free, 30M+ legal units, covers companies + self-employed. `https://api.insee.fr/entreprises/sirene/V3/` — requires free API key. Fields: SIREN, SIRET, name, address, APE code, status, creation date.

  **Italy (it.md):** Registro Imprese via Infocamere — no public API, scrape only via `https://www.registroimprese.it`. Commercial: Infocamere API (paid), CRIF (paid). Fields via scrape: REA number, tax code, name, address, status.

  **Canada (ca.md):** Corporations Canada at `https://ised-isde.canada.ca/cc/lgcy/fdrl/srch/index` — bulk download available. Provincial: Alberta, BC, Ontario each have separate registries with downloads. Fields: corporation number, name, status, incorporation date, registered office.

  **Brazil (br.md):** Receita Federal CNPJ bulk download at `https://dadosabertos.rfb.gov.br/CNPJ/` — free, full database monthly. Fields: CNPJ, company name (razão social), fantasy name, status, CNAE (activity code), address, phone, email, capital, partners.

  **Russia (ru.md):** EGRUL (Единый государственный реестр юридических лиц) from Federal Tax Service at `https://egrul.nalog.ru` — free per-company search; bulk XML download available for fee. Fields: OGRN, INN, KPP, name, status, address, directors.

- [ ] Commit:

```bash
git add docs/sources/countries/fr.md docs/sources/countries/it.md \
        docs/sources/countries/ca.md docs/sources/countries/br.md \
        docs/sources/countries/ru.md
git commit -m "docs: add country source files for FR, IT, CA, BR, RU (Tier 1 ranks 7-11)"
```

---

## Task 5: Tier 1 research — KR, AU, MX, ES, ID (ranks 12–16)

**Files:** Create `docs/sources/countries/kr.md`, `au.md`, `mx.md`, `es.md`, `id.md`

- [ ] Research and create each file. Key facts:

  **South Korea (kr.md):** DART (Data Analysis, Retrieval and Transfer System) at `https://opendart.fss.or.kr` — free API for public companies (financial disclosures). BizInfo (`http://www.bizno.net`) for all companies. Fields: company registration number, name, CEO, address, business type, capital.

  **Australia (au.md):** ASIC Companies Register — bulk download at `https://data.gov.au/dataset/ds-dga-asic-companies`. ABN Lookup at `https://abr.business.gov.au/json/AbnDetails.aspx?abn=...` — free API. Fields: ACN/ABN, name, status, type, address, registration date.

  **Mexico (mx.md):** SAT (tax authority) maintains RFC. Registro Público de Comercio — no public API, state-level registries only. Scraping `https://siger.gob.mx` possible. Fields: RFC, company name, address, status.

  **Spain (es.md):** BORME (Boletín Oficial del Registro Mercantil) at `https://www.boe.es/diario_borme/` — free download of official gazette entries. Registro Mercantil Central — no bulk API. Commercial: Axesor, Informa (paid). Fields: NIF, name, address, legal form, activity.

  **Indonesia (id.md):** AHU Online (Ministry of Law) at `https://ahu.go.id` — no public API. Ministry of Investment BKPM has some data. Scrape possible. Fields: NIB (Business ID), name, address, status.

- [ ] Commit:

```bash
git add docs/sources/countries/kr.md docs/sources/countries/au.md \
        docs/sources/countries/mx.md docs/sources/countries/es.md \
        docs/sources/countries/id.md
git commit -m "docs: add country source files for KR, AU, MX, ES, ID (Tier 1 ranks 12-16)"
```

---

## Task 6: Tier 1 research — NL, SA, TR, CH, TW (ranks 17–21)

**Files:** Create `docs/sources/countries/nl.md`, `sa.md`, `tr.md`, `ch.md`, `tw.md`

- [ ] Research and create each file. Key facts:

  **Netherlands (nl.md):** KVK (Kamer van Koophandel) — API available but **paid** (no free tier). `https://developers.kvk.nl`. Fields: KVK number, name, address, SBI code, status, directors. ~2.5M registrations.

  **Saudi Arabia (sa.md):** Ministry of Commerce at `https://mc.gov.sa` — no public API or bulk data. Maroof platform has some business verification. Fields: CR number, name, address, status.

  **Turkey (tr.md):** MERSİS (Merkezi Sicil Kayıt Sistemi) at `https://mersis.gtb.gov.tr` — no public bulk API. UYAP judicial data. Scrape possible. Fields: MERSİS number, tax ID, name, address, status.

  **Switzerland (ch.md):** Zefix (Zentraler Firmenindex) at `https://www.zefix.ch/en/search/entity/list` — **free REST API** with full federal register. `https://www.zefix.ch/ZefixREST/api/v1/firm/search.json`. Fields: UID (CHE-xxx.xxx.xxx), name, legal form, address (canton), status, SHAB entry date.

  **Taiwan (tw.md):** Ministry of Economic Affairs (MOEA) at `https://data.gov.tw/dataset/6074` — free bulk CSV download. Fields: company ID, name, address, capital, status, industry code.

- [ ] Commit:

```bash
git add docs/sources/countries/nl.md docs/sources/countries/sa.md \
        docs/sources/countries/tr.md docs/sources/countries/ch.md \
        docs/sources/countries/tw.md
git commit -m "docs: add country source files for NL, SA, TR, CH, TW (Tier 1 ranks 17-21)"
```

---

## Task 7: Tier 1 research — PL, AR, SE, BE, TH (ranks 22–26)

**Files:** Create `docs/sources/countries/pl.md`, `ar.md`, `se.md`, `be.md`, `th.md`

- [ ] Research and create each file. Key facts:

  **Poland (pl.md):** KRS (Krajowy Rejestr Sądowy) — **free REST API** at `https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}`. Also CEIDG for sole traders. Fields: KRS number, NIP, REGON, name, address, legal form, status, directors, shareholders.

  **Argentina (ar.md):** IGJ (Inspección General de Justicia) for Buenos Aires + provincial registries — no national public API. AFIP/CUIT for tax IDs. Scrape possible via `https://www.argentina.gob.ar/igj`. Fields: CUIT, name, address, status.

  **Sweden (se.md):** Bolagsverket — **paid API** (`https://bolagsverket.se/en/us/services-api`). Free bulk download of basic data at Statistika Centralbyrån (SCB). Fields: org number, name, address, SNI code, status.

  **Belgium (be.md):** CBE (Crossroads Bank for Enterprises) — **free API** at `https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html`. Open data at `https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises/services-everyone/cbe-open-data`. Fields: enterprise number, name, address, status, start date, activities.

  **Thailand (th.md):** DBD (Department of Business Development) at `https://www.dbd.go.th` — no public API. Limited open data portal. Scrape possible. Fields: juristic ID, name, type, address, registration date.

- [ ] Commit:

```bash
git add docs/sources/countries/pl.md docs/sources/countries/ar.md \
        docs/sources/countries/se.md docs/sources/countries/be.md \
        docs/sources/countries/th.md
git commit -m "docs: add country source files for PL, AR, SE, BE, TH (Tier 1 ranks 22-26)"
```

---

## Task 8: Tier 2 research — IL, AT, AE, NG, SG, MY, ZA (ranks 28–34)

**Files:** Create `docs/sources/countries/il.md`, `at.md`, `ae.md`, `ng.md`, `sg.md`, `my.md`, `za.md`

- [ ] Research and create each file following the template. Key facts:

  **Israel (il.md):** Companies Registrar at `https://www.gov.il/en/departments/bureaus/justice-companies-registrar` — no bulk API. Data Israel portal has some open data. Fields: company number, name, status, type, address.

  **Austria (at.md):** Firmenbuch (Company Register) — no free public API. Justiz.gv.at has per-company search. RIS (Rechtsinformationssystem) has some open legal data. Commercial: Creditreform Austria, KSV1870. Fields: FN number, name, address, legal form, status, directors.

  **UAE (ae.md):** Multiple emirate-level registries (Dubai DED, Abu Dhabi ADDED, etc.) — no unified national API. Dubai DED has `https://services.dubaided.gov.ae` with limited lookup. Fields: trade license number, name, activity, status, expiry.

  **Nigeria (ng.md):** CAC (Corporate Affairs Commission) at `https://search.cac.gov.ng` — no public API. Manual search only. Fields: RC number, name, status, address, directors.

  **Singapore (sg.md):** ACRA (BizFile+) — **paid API** at `https://developer.data.gov.sg/acra`. Free basic info via `https://data.gov.sg/dataset/acra-information-on-corporate-entities`. Fields: UEN, name, entity type, status, registration date, address.

  **Malaysia (my.md):** SSM (Companies Commission) at `https://www.ssm.com.my` — no free public API. e-Info paid service. Limited open data. Fields: company number, name, status, address.

  **South Africa (za.md):** CIPC (Companies and Intellectual Property Commission) — bulk download available at `https://opendata.cipc.co.za`. Fields: registration number, name, status, type, registration date, province.

- [ ] Commit:

```bash
git add docs/sources/countries/il.md docs/sources/countries/at.md \
        docs/sources/countries/ae.md docs/sources/countries/ng.md \
        docs/sources/countries/sg.md docs/sources/countries/my.md \
        docs/sources/countries/za.md
git commit -m "docs: add country source files for IL, AT, AE, NG, SG, MY, ZA (Tier 2 ranks 28-34)"
```

---

## Task 9: Tier 2 research — EG, BD, PH, PK, VN, CO, HK (ranks 35–42)

**Files:** Create `docs/sources/countries/eg.md`, `bd.md`, `ph.md`, `pk.md`, `vn.md`, `co.md`, `hk.md`

- [ ] Research and create each file. Key facts:

  **Egypt (eg.md):** GAFI (General Authority for Investment) at `https://www.gafi.gov.eg` — no public API. Investment Map has some data. Fields: registration number, name, status, address.

  **Bangladesh (bd.md):** RJSC (Registrar of Joint Stock Companies) at `https://www.rjsc.gov.bd` — no public API. Manual search only. Fields: registration number, name, type, status.

  **Philippines (ph.md):** SEC (Securities and Exchange Commission) at `https://efiling.sec.gov.ph` — no bulk API. Scrape possible. Fields: SEC number, name, status, type, address.

  **Pakistan (pk.md):** SECP (Securities and Exchange Commission) at `https://efiling.secp.gov.pk` — no public API. Limited search available. Fields: CUIN, name, status, type, address.

  **Vietnam (vn.md):** National Business Registration Portal at `https://dangkykinhdoanh.gov.vn` — no public API. Ministry of Planning open data has some CSV. Fields: company ID, name, address, business lines, status.

  **Colombia (co.md):** Confecámaras / RUE (Registro Único Empresarial) at `https://www.rues.org.co` — no public API. NIT (tax ID) lookup available. Fields: NIT, name, registration number, chamber, status.

  **Hong Kong (hk.md):** Companies Registry — **bulk download** at `https://www.cr.gov.hk/en/e-services/free-biz-name-search/bulk-search.htm`. Free CSV. Fields: company number, name, type, date of incorporation, status.

- [ ] Commit:

```bash
git add docs/sources/countries/eg.md docs/sources/countries/bd.md \
        docs/sources/countries/ph.md docs/sources/countries/pk.md \
        docs/sources/countries/vn.md docs/sources/countries/co.md \
        docs/sources/countries/hk.md
git commit -m "docs: add country source files for EG, BD, PH, PK, VN, CO, HK (Tier 2 ranks 35-42)"
```

---

## Task 10: Tier 2 research — RO, CZ, CL, NZ, FI, PT, PE (ranks 43–49)

**Files:** Create `docs/sources/countries/ro.md`, `cz.md`, `cl.md`, `nz.md`, `fi.md`, `pt.md`, `pe.md`

- [ ] Research and create each file. Key facts:

  **Romania (ro.md):** ONRC (National Trade Register Office) at `https://www.onrc.ro` — no free API. ListaFirme.ro is a commercial aggregator. Scrape via recom.ro possible. Fields: CUI, name, status, address, activity code.

  **Czech Republic (cz.md):** OR (Obchodní rejstřík) — **free REST API** at `https://or.justice.cz/ias/ui/rejstrik`. ARES system at `https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/` — free, no auth. Fields: IČO, name, address, legal form, status, date founded.

  **Chile (cl.md):** CBR (Conservador de Bienes Raíces) + SII (Internal Revenue) at `https://zeus.sii.cl/cvc/stc/stc.html` — limited free lookup by RUT. No bulk API. Fields: RUT, name, address, activity.

  **New Zealand (nz.md):** Companies Office — **free API** at `https://api.business.govt.nz/api/v1/companies`. API key required (free registration). Fields: company number, name, status, registered office, directors, shareholders.

  **Finland (fi.md):** PRH (Finnish Patent and Registration Office) — **free API** at `https://avoindata.prh.fi/tr/v1/companies`. No auth required. Fields: businessId (Y-tunnus), name, registrationDate, companyForm, detailsUri.

  **Portugal (pt.md):** IRN (Instituto dos Registos e do Notariado) — no free public API. Racius.com is a commercial aggregator. ePortugal has some open data. Fields: NIPC, name, address, status.

  **Peru (pe.md):** SUNARP (National Superintendency of Public Registries) — no public API. RUC lookup via SUNAT possible. Fields: RUC, name, address, status.

- [ ] Commit:

```bash
git add docs/sources/countries/ro.md docs/sources/countries/cz.md \
        docs/sources/countries/cl.md docs/sources/countries/nz.md \
        docs/sources/countries/fi.md docs/sources/countries/pt.md \
        docs/sources/countries/pe.md
git commit -m "docs: add country source files for RO, CZ, CL, NZ, FI, PT, PE (Tier 2 ranks 43-49)"
```

---

## Task 11: Tier 2 research — KZ, GR, IQ, DZ, HU, QA, MA (ranks 50–56)

**Files:** Create `docs/sources/countries/kz.md`, `gr.md`, `iq.md`, `dz.md`, `hu.md`, `qa.md`, `ma.md`

- [ ] Research and create each file. Key facts:

  **Kazakhstan (kz.md):** Business Information System at `https://www.salyk.kz` or `https://egov.kz` — limited open data. BIN lookup possible. Fields: BIN, name, address, status.

  **Greece (gr.md):** GEMI (General Commercial Registry) — **free API** at `https://www.businessportal.gr`. Open data bulk download available. Fields: GEMI number, AFM (tax ID), name, address, legal form, status.

  **Iraq (iq.md):** Ministry of Trade — no public API or open data. Manual process only. Fields: registration number, name, address.

  **Algeria (dz.md):** CNRC (Centre National du Registre de Commerce) — no public API. Limited web search. Fields: NRC, name, address, activity.

  **Hungary (hu.md):** Cégbíróság (Company Court) — **free search** at `https://www.e-cegjegyzek.hu`. Open data downloads available. ÁNYK system. Fields: cégjegyzékszám, name, address, legal form, status.

  **Qatar (qa.md):** Ministry of Commerce (MOCI) — no public API. Kahramaa / QFC have some data. Fields: CR number, name, activity, status.

  **Morocco (ma.md):** RC (Registre de Commerce) at `https://www.ompic.ma` — no free public API. OMPIC has trademark data. Fields: RC number, name, address, activity.

- [ ] Commit:

```bash
git add docs/sources/countries/kz.md docs/sources/countries/gr.md \
        docs/sources/countries/iq.md docs/sources/countries/dz.md \
        docs/sources/countries/hu.md docs/sources/countries/qa.md \
        docs/sources/countries/ma.md
git commit -m "docs: add country source files for KZ, GR, IQ, DZ, HU, QA, MA (Tier 2 ranks 50-56)"
```

---

## Task 12: Tier 2 research — SK, ET, KE, KW, EC, DO, UZ (ranks 57–63)

**Files:** Create `docs/sources/countries/sk.md`, `et.md`, `ke.md`, `kw.md`, `ec.md`, `do.md`, `uz.md`

- [ ] Research and create each file. Key facts:

  **Slovakia (sk.md):** Obchodný register — **free** at `https://www.orsr.sk`. Open data download at `https://www.justice.gov.sk`. Fields: IČO, name, address, legal form, status.

  **Ethiopia (et.md):** Ministry of Trade and Regional Integration — no public API. Manual registration. Fields: registration number, name, address.

  **Kenya (ke.md):** eCitizen BRS (Business Registration Service) — no public API. Online portal. Fields: registration number, name, directors, status.

  **Kuwait (kw.md):** Ministry of Commerce (MOCI) — no public API. eGovernment portal has limited lookup. Fields: CR number, name, activity, status.

  **Ecuador (ec.md):** Superintendencia de Compañías — **free bulk download** at `https://appscvs.supercias.gob.ec/portaldeinformacion/`. CSV available. Fields: RUC, name, status, address, activity.

  **Dominican Republic (do.md):** Registro Mercantil — no public API. RNCC system. Fields: registration number, name, address, status.

  **Uzbekistan (uz.md):** Unified State Register (my.gov.uz) — no public API. Limited online search. Fields: STIR/TIN, name, address, status.

- [ ] Commit:

```bash
git add docs/sources/countries/sk.md docs/sources/countries/et.md \
        docs/sources/countries/ke.md docs/sources/countries/kw.md \
        docs/sources/countries/ec.md docs/sources/countries/do.md \
        docs/sources/countries/uz.md
git commit -m "docs: add country source files for SK, ET, KE, KW, EC, DO, UZ (Tier 2 ranks 57-63)"
```

---

## Task 13: Tier 3 research — TZ, GT, OM, AO, BG, GH, LT (ranks 64–70)

**Files:** Create `docs/sources/countries/tz.md`, `gt.md`, `om.md`, `ao.md`, `bg.md`, `gh.md`, `lt.md`

- [ ] Research and create each file. Key facts:

  **Tanzania (tz.md):** BRELA (Business Registrations and Licensing Agency) — no public API. Online portal.

  **Guatemala (gt.md):** Registro Mercantil — no public API. Portal search only.

  **Oman (om.md):** MOCIIP business register — no public API. Invest Easy portal has limited lookup.

  **Angola (ao.md):** IGAPE (Instituto de Apoio e Promoção do Investimento) — no public API.

  **Bulgaria (bg.md):** BRRA (Commercial Register and Register of Non-Profit Legal Entities) — **free API** at `https://portal.registryagency.bg/CR/en/`. Bulk download available. Fields: UIC, name, address, legal form, status.

  **Ghana (gh.md):** ORC (Office of the Registrar of Companies) — no public API. Online portal search.

  **Lithuania (lt.md):** Registrų centras — **free API** at `https://www.registrucentras.lt/en/`. Open data at `https://www.registrucentras.lt/en/atviri-duomenys/`. Fields: company code, name, address, legal form, status.

- [ ] Commit:

```bash
git add docs/sources/countries/tz.md docs/sources/countries/gt.md \
        docs/sources/countries/om.md docs/sources/countries/ao.md \
        docs/sources/countries/bg.md docs/sources/countries/gh.md \
        docs/sources/countries/lt.md
git commit -m "docs: add country source files for TZ, GT, OM, AO, BG, GH, LT (Tier 3 ranks 64-70)"
```

---

## Task 14: Tier 3 research — CI, LU, HR, BY, AZ, MM, UG (ranks 71–78)

**Files:** Create `docs/sources/countries/ci.md`, `lu.md`, `hr.md`, `by.md`, `az.md`, `mm.md`, `ug.md`

- [ ] Research and create each file. Key facts:

  **Côte d'Ivoire (ci.md):** CEPICI (Centre de Promotion des Investissements en Côte d'Ivoire) — no public API.

  **Luxembourg (lu.md):** LBR (Luxembourg Business Registers) — **paid API** at `https://www.lbr.lu`. Registration required, pricing per query. Fields: RCS number, name, legal form, address, status, directors.

  **Croatia (hr.md):** Sudski registar (Court Register) — **free access** at `https://sudreg.pravosudje.hr`. Open data available. Fields: MBS, OIB, name, address, legal form, status.

  **Belarus (by.md):** Ministry of Justice register — limited online search. No public API. Sanctions and access restrictions apply.

  **Azerbaijan (az.md):** Ministry of Economy e-register — no public API. Limited online search.

  **Myanmar (mm.md):** DICA (Directorate of Investment and Company Administration) — no public API. MyCO online portal.

  **Uganda (ug.md):** URSB (Uganda Registration Services Bureau) — no public API. Online portal.

- [ ] Commit:

```bash
git add docs/sources/countries/ci.md docs/sources/countries/lu.md \
        docs/sources/countries/hr.md docs/sources/countries/by.md \
        docs/sources/countries/az.md docs/sources/countries/mm.md \
        docs/sources/countries/ug.md
git commit -m "docs: add country source files for CI, LU, HR, BY, AZ, MM, UG (Tier 3 ranks 71-78)"
```

---

## Task 15: Tier 3 research — LK, SI, TN, JO, CM, LY, BH (ranks 79–85)

**Files:** Create `docs/sources/countries/lk.md`, `si.md`, `tn.md`, `jo.md`, `cm.md`, `ly.md`, `bh.md`

- [ ] Research and create each file. Key facts:

  **Sri Lanka (lk.md):** ROC (Registrar of Companies) — no public API. Portal search.

  **Slovenia (si.md):** AJPES (Agency of the Republic of Slovenia for Public Legal Records) — **free API** and bulk download at `https://www.ajpes.si/prs/`. Fields: registration number, name, address, legal form, status.

  **Tunisia (tn.md):** RNE (Registre National des Entreprises) — no public API. Online search.

  **Jordan (jo.md):** Ministry of Industry and Trade — no public API. Jordan Enterprise Development Corporation has some data.

  **Cameroon (cm.md):** CFCE (Centre de Formalités de Création d'Entreprises) — no public API.

  **Libya (ly.md):** Severely limited public data infrastructure. No public registry API.

  **Bahrain (bh.md):** MOICT Sijilat platform at `https://www.sijilat.bh` — online registration system. No public bulk API.

- [ ] Commit:

```bash
git add docs/sources/countries/lk.md docs/sources/countries/si.md \
        docs/sources/countries/tn.md docs/sources/countries/jo.md \
        docs/sources/countries/cm.md docs/sources/countries/ly.md \
        docs/sources/countries/bh.md
git commit -m "docs: add country source files for LK, SI, TN, JO, CM, LY, BH (Tier 3 ranks 79-85)"
```

---

## Task 16: Tier 3 research — BO, PY, LV, NP, SN, AL, MZ (ranks 86–93)

**Files:** Create `docs/sources/countries/bo.md`, `py.md`, `lv.md`, `np.md`, `sn.md`, `al.md`, `mz.md`

- [ ] Research and create each file. Key facts:

  **Bolivia (bo.md):** SEPREC (Servicio Plurinacional de Registro de Comercio) — no public API.

  **Paraguay (py.md):** MIC (Ministry of Industry and Commerce) + SET (tax authority) — no public API.

  **Latvia (lv.md):** UR (Uzņēmumu reģistrs) and Lursoft — UR has **free search** at `https://www.ur.gov.lv/en/`. Lursoft is commercial with API. Fields: registration number, name, address, legal form, status.

  **Nepal (np.md):** OCR (Office of the Company Registrar) — no public API. Manual portal.

  **Senegal (sn.md):** APIX (Agence de Promotion des Investissements et des Grands Travaux) — no public API. RCCM system.

  **Albania (al.md):** QKB (National Business Center) — **free API** at `https://qkb.gov.al/en/`. Open data available. Fields: NIPT, name, address, legal form, status.

  **Mozambique (mz.md):** CPAR (Conservatória do Registo de Entidades Legais) — no public API.

- [ ] Commit:

```bash
git add docs/sources/countries/bo.md docs/sources/countries/py.md \
        docs/sources/countries/lv.md docs/sources/countries/np.md \
        docs/sources/countries/sn.md docs/sources/countries/al.md \
        docs/sources/countries/mz.md
git commit -m "docs: add country source files for BO, PY, LV, NP, SN, AL, MZ (Tier 3 ranks 86-93)"
```

---

## Task 17: Tier 3 research — ZM, CD, AM, CY, HN, IS, BA (ranks 94–100)

**Files:** Create `docs/sources/countries/zm.md`, `cd.md`, `am.md`, `cy.md`, `hn.md`, `is.md`, `ba.md`

- [ ] Research and create each file. Key facts:

  **Zambia (zm.md):** PACRA (Patents and Companies Registration Agency) — no public API. Online portal.

  **DR Congo (cd.md):** GUICHET UNIQUE under OHADA framework — no public API. Very limited infrastructure.

  **Armenia (am.md):** e-register.am — **free API** at `https://www.e-register.am/en/`. Fields: HSTIN, name, address, legal form, status, directors.

  **Cyprus (cy.md):** Registrar of Companies — **bulk download** (CSV) at `https://efiling.drcor.mcit.gov.cy`. Fields: registration number, name, type, status, incorporation date.

  **Honduras (hn.md):** Registro Mercantil (varies by chamber) — no unified public API.

  **Iceland (is.md):** Fyrirtækjaskrá (RSK — Directorate of Internal Revenue) — **free search and API** at `https://skatturinn.is/atvinnurekstur/skraningar/`. Fields: kennitala (ID number), name, address, legal form, status.

  **Bosnia & Herzegovina (ba.md):** APIF (FBiH) and PJRS (RS entity) — **partial free access**. APIF has some open data. Fields: ID number, name, address, legal form, status.

- [ ] Commit:

```bash
git add docs/sources/countries/zm.md docs/sources/countries/cd.md \
        docs/sources/countries/am.md docs/sources/countries/cy.md \
        docs/sources/countries/hn.md docs/sources/countries/is.md \
        docs/sources/countries/ba.md
git commit -m "docs: add country source files for ZM, CD, AM, CY, HN, IS, BA (Tier 3 ranks 94-100)"
```

---

## Task 18: Finalize README and verify counts

**Files:** Modify `docs/sources/README.md`

- [ ] Verify all 100 country files exist:

```bash
ls docs/sources/countries/ | wc -l
# Expected: 100
```

- [ ] Update README intro line to reflect completion date:

```markdown
> Last updated: 2026-05-18. All 100 files present. Update a country file directly when new sources are found.
```

- [ ] Final commit:

```bash
git add docs/sources/README.md
git commit -m "docs: finalize country source index — all 100 countries complete"
```
