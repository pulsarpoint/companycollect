# Italy Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Italy's defining trait: a clean
single join key (`codice_fiscale`)** — but the **authoritative spine and full financials are PAID**
(planning-only). The OPEN layer is a **subset** (startup/PMI innovative + GLEIF + ANAC), not a master.

## Identity / legal / activity / location

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.codice_fiscale | registro_imprese / startup_pmi_innovative / gleif_lei | codice_fiscale | **PK** | continuous/weekly/daily | paid / open / CC0 | Universal key; open via startup+GLEIF+ANAC |
| registration.partita_iva | registro_imprese | partita_iva | — | continuous | **paid** | often = CF, not always |
| registration.numero_rea | registro_imprese / gleif_lei | numero_rea / entity.registeredAs | — | continuous/daily | paid / CC0 | province-scoped |
| registration.lei | gleif_lei | data[].attributes.lei | LEI↔CF | daily | **CC0** | subset (holders) |
| legal_identity.denominazione | registro_imprese / startup / gleif_lei | denominazione / legalName | — | — | paid/open/CC0 | open via startup/GLEIF |
| legal_identity.forma_giuridica | registro_imprese (else gleif ELF) | forma_giuridica / legalForm.id | — | continuous | paid (ELF open) | |
| status.stato_attivita | registro_imprese | stato_attivita | — | continuous | **paid** | attiva/liquidazione/fallita/cessata |
| activity.ateco | registro_imprese / startup_pmi_innovative | codice_ateco | — | continuous/weekly | paid / open | open only for the startup subset |
| registered_location.* | startup_pmi_innovative (comune) / registro_imprese (full) | sede_legale / comune | — | weekly/continuous | open(part)/paid | full address paid |
| capital.capitale_sociale | registro_imprese | capitale_sociale | — | continuous | **paid** | register capital, not accounts |
| officers[] | registro_imprese | amministratori | codice_fiscale | continuous | **paid** · PII | GDPR |
| ownership.lei_*_parent | gleif_lei | relationships (L2) | LEI | daily | **CC0** | partial open group links |

## Financial statements (multi-source)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] (full) | registro_imprese_bilanci_xbrl | stato patrimoniale + conto economico | codice_fiscale | annual | **paid** | Exact figures; XBRL; planning-only |
| financial_statements[] (bands) | startup_pmi_innovative | classe_valore_produzione / classe_addetti | codice_fiscale | weekly | **open** | Revenue/employee RANGES only (subset) |

### Financial source precedence
1. **registro_imprese_bilanci_xbrl** (or a commercial aggregator: AIDA/Cerved/Atoka) — exact figures,
   full statement, XBRL — but **paid**. The realistic financial route at scale.
2. **startup_pmi_innovative bands** — the only **open** per-company financial signal, but **ranges**, and
   only for the innovative subset.

Dedupe on `codice_fiscale + fiscal_year + accounts_type`. `revenue`/`net_income`/`employees` exact figures
are **nullable** (micro/abbreviato disclose fewer lines; or only a band is available). Currency EUR.

## Join & precedence summary

- **Single key `codice_fiscale`** across all sources (reconcile with **Partita IVA** and **numero REA**;
  bridge **LEI↔CF** via GLEIF `registeredAs`). Clean key like France — but most fields are **paid**.
- **Authority**: Registro Imprese (InfoCamere) authoritative for identity/status/capital/officers/financials
  — paid. Open sources are a convenient **seed** (CF + name + ATECO + bands + LEI), not a master.
- **Freshness**: register continuous; startup weekly; GLEIF daily; bilanci annual.

## Missing / restricted data

- **Open per-company master**: none — paid Registro Imprese or a commercial aggregator (Cerved/AIDA/Atoka).
- **Open exact financials**: none — only paid bilanci XBRL; open data gives **bands** (startup subset).
- **Beneficial ownership (titolare effettivo)**: not openly modeled (the registro titolari effettivi has
  regulated access); GLEIF Level-2 gives only partial LEI↔LEI parent links.
- **PII**: amministratori (paid) — GDPR.
- **Aggregate-only** open data (InfoCamere/ISTAT) does **not** contribute to the per-company profile.
