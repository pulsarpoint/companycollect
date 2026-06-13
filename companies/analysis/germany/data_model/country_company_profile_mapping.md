# Germany Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated, with join keys, freshness,
license/access, and precedence. **Germany's defining trait: there is no shared numeric key between the
registry spine and the financial sources** — financials join by register number or name+seat.

## Registry spine (open)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.company_number | offeneregister_companies | company_number | PK | stale 2017-2019 | CC-BY (confirm NC) / public | Primary key of the open dataset; synthetic |
| registration.register_type | offeneregister_companies | all_attributes._registerArt | part of natural key | stale | CC-BY / public | HRB/HRA/GnR/VR/PR |
| registration.register_number | offeneregister_companies | all_attributes._registerNummer | part of natural key | stale | CC-BY / public | **Court-scoped, not unique alone** |
| registration.registrar | offeneregister_companies | all_attributes.registrar | part of natural key | stale | CC-BY / public | Amtsgericht |
| registration.native_company_number | offeneregister_companies | all_attributes.native_company_number | human ref | stale | CC-BY / public | e.g. "Düsseldorf HRB 150148" |
| registration.natural_key | derived | registrar+_registerArt+_registerNummer | **financial join key** | stale | — | Compose for matching financials |
| legal_identity.name | offeneregister_companies | name | — | stale | CC-BY / public | Current legal name |
| legal_identity.company_type | derived | name suffix + _registerArt | — | — | — | GmbH/AG/UG/KG/e.V./eG |
| legal_identity.previous_names | offeneregister_companies | previous_names[] | — | stale | CC-BY / public | No per-name dates |
| status.current_status | offeneregister_companies | current_status | — | stale | CC-BY / public | Free text; no dissolution date |
| registered_location.registered_address | offeneregister_companies | registered_address | — | stale | CC-BY / public | Unparsed free text |
| registered_location.municipality | offeneregister_companies | all_attributes.registered_office | name+seat join | stale | CC-BY / public | Seat (Sitz) |
| registered_location.federal_state | offeneregister_companies | all_attributes.federal_state | — | stale | CC-BY / public | English value |
| officers[] | offeneregister_companies | officers[] | company_number | stale | CC-BY / public · **PII** | Split to side table; GDPR |
| related_registrations | offeneregister_companies | subsequent_registrations | — | stale | CC-BY / public | Sparse |
| available_documents | offeneregister_companies | all_attributes.additional_data | — | stale | CC-BY / public | Availability booleans only |

## Financial statements (planning-only)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] (official) | unternehmensregister_financials | Bilanz/GuV concepts | register no / name+seat | current | free view, no reuse | Authoritative; **no bulk/API** → not directly ingestable |
| financial_statements[] (scale) | openregister_financials_api | vendor JSON | vendor register ref | daily | paid | **Preferred for structured financials at scale** |
| financial_statements[] (free) | bundesanzeiger_reports | report content (parsed) | name+seat | live | free, captcha-limited | Targeted enrichment only; HTML→figures extraction |

### Financial source precedence
1. **openregister_financials_api** (or comparable paid vendor) — when budget exists: structured JSON,
   daily, multi-year, no XBRL parsing burden. Best at scale.
2. **bundesanzeiger_reports** — free, per-company, captcha-limited; use for a bounded target set; needs
   an HTML/XBRL figure extractor.
3. **unternehmensregister_financials** — the authoritative *definition* of the fields and the lawful
   free-view source, but with **no bulk/API** it is a reference/manual-lookup source, not a pipeline input.

Deduplicate financial records on `register/name+seat + period_end + consolidated`. Prefer
`consolidated=false` (Einzelabschluss) for the entity's own figures; keep `consolidated=true`
(Konzernabschluss) as the group view. Never assume currency = EUR; store `currency` with every figure.

## Join & precedence summary

- **Registry internal join**: `company_number` (officers, related_registrations, documents all hang off it).
- **Registry ↔ financials join**: NO shared numeric key. Compose `registration.natural_key`
  (`registrar_registerType_registerNumber`) or fall back to normalized `name` + `municipality` (seat).
  This match step is the main engineering risk and should be built as an explicit, auditable matcher.
- **Freshness conflict**: the registry spine is **stale (2017-2019)** while financials are **current** —
  a company may have current financials but an outdated name/officer set, or be missing from the spine
  entirely. Flag spine staleness; consider the 2022 SQLite snapshot or a commercial registry API to
  refresh the spine before attaching fresh financials.

## Missing data (kept as notes, not invented fields)

- **tax_id / vat_id**: not in open data. VAT can be *validated* via EU VIES but VIES does not *list* —
  `not_available_in_open_sources`.
- **activity / NACE (WZ) code**: not present in OffeneRegister → the `activity` section is **DERIVED /
  planning-only**, not sourced from the spine. Populate via (1) LLM classification of the company
  **website** (`industry_source=website_llm`, best fit for this platform), (2) LLM/classifier over the
  Handelsregister **"Gegenstand des Unternehmens"** purpose text (`purpose_text`; text needs SI/
  announcements or a commercial extract — not in the open bulk), or (3) a commercial API
  (`vendor`: North Data / Implisense / Creditreform). Destatis URS holds WZ but is confidential
  (aggregate only); GLEIF/VIES carry no industry. Always store `industry_source` + `confidence`.
- **incorporation_date / dissolution_date**: not clean fields; only textual `current_status`.
- **beneficial ownership** (Transparenzregister): not modeled (access-restricted) — out of scope here.
