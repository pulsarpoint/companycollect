# ESEF: register-verified entity link, people per filing, registered-office addresses

Date: 2026-09-08, after basic-info slice 5. Owner rulings the same day: the company_serving
models are not patched around bad ESEF rows ("for now just properly extract people from esef
and do proper lei mapping"); addresses from ESEF are wanted too; the people pass runs on
DeepSeek.

## Facts (prod and repo, 2026-09-08)

### Entity link

- 1,415 filings on the Swedish OAM from 417 LEIs. Every LEI is in `esef_entity_registry_map`
  (`match_source = gleif_registered_as`, built by `esef_entity_registry_map_clickhouse` from
  GLEIF's registered-as id; `country_iso2` there is GLEIF's primary country). 404 resolve to a
  Swedish org number; 403 of those exist in `se_company_basic_info`. The one that does not is
  Rizzo Group AB (5565401493): mapped correctly, LEI LAPSED, absent from both register files
  (bankrupt), so no anchor can ever accept it.
- The other 16 filers are foreign-jurisdiction companies listed in Stockholm (AstraZeneca GB,
  Autoliv US-DE, Lundin Mining, Lundin Gold, Lucara, Meren, Josemaria, IPC CA, Fenix Outdoor
  and Cavotec CH, CoinShares JE, Sotkamo Silver FI, Fiven NO, SGL DK, Gentoo MT, "SKF Group"
  AU). The manifest stamps a company id only when the map's country equals the filing's OAM
  country (`segment_assets.py`, `_company_id_for_index_row`), so they get `country_iso2 = 'SE'`
  and `company_id = ''`. Contact candidates still emit them (367 rows); the LLM stage skips
  every empty id. That is the whole "LEI mapping" problem: the map is right, the stamping is
  OAM-bound, and nothing checks the register.
- `company_identifier` already implements the register-verified LEI link (jurisdiction check,
  digits-normalised id, must exist in the register) for SE, NO, FI and FR
  (`company_identifier/rules.py`).
- The company_serving publish (`company_serving_current`) fails on exactly those rows: 140
  contact rows with an empty id, 5 contact rows and 24 people rows for Rizzo.

### People

- One LLM enrichment per (country, company), the latest filing only
  (`llm_enrichment_assets.py`, `latest_company_report_rank = 1`, prompt
  `esef-company-enrichment-v2`). 824 Swedish documents enriched: 2023 and 2024 filings are
  covered (327 of 360, 301 of 355), 2021 and 2022 almost not (13 of 347, 33 of 352).
- `esef_document_people` is a projection of `people_json`
  (`company_information_projections.py`). After the projection refresh of 2026-09-08 it holds
  9,634 Swedish rows for 637 documents and 385 companies. `candidate_uid` hashes the raw JSON
  item, so a re-run inserts new rows instead of replacing. People may cite only the
  `people_and_audit` segment; at most 100 people per document; visible sections are capped at
  6,000 characters and two per type.
- 1,064 Swedish documents carry people sections (`board_composition` 1,063,
  `auditor_appointment` 1,060, `executive_management` 1,026, `annual_report_signatures`
  1,018, `person_profiles` 1,004, `board_committees` 945), about 40,000 capped characters per
  document.
- `se_company_person_esef` is a view over `esef_document_people` (`country_code = 'SE'`) and
  is what the people entity reads.

### Addresses

- Nothing extracts an address from ESEF. The parser isolates `company_contact` visible
  sections (1,050 Swedish documents) but never sends them anywhere.
- The filings tag the address: `AddressOfRegisteredOfficeOfEntity` is present in 1,404 of the
  1,415 Swedish filings (`esef_facts`, LEI-keyed, undimensioned, mostly tagged `sv`, e.g.
  "Fleminggatan 20, 112 26 Stockholm, Sverige"), with `DomicileOfEntity`, `CountryOfIncorporation`, `LegalFormOfEntity` (1,404 each)
  and `PrincipalPlaceOfBusiness` (1,140).
- The address entity (`2026-09-06-se-company-address-entity-design.md`) has the kind
  `registered` in its vocabulary, publishes every address any extractor delivers (precedence
  orders, never filters), merges equal normalised addresses across sources, and named ESEF
  address extraction as the separate work it is.

## Design

### 1. Register-verified entity link

- `esef_entity_registry_map` keeps one row per filer LEI and gains `link_status`
  (`register_verified` when the country has a rule in `COUNTRY_IDENTITY_RULES` and the
  normalised id exists in that register; `unverified` when the country has a rule and the id
  does not; `gleif` when the country has no register here). `registry_id` stays the normalised
  GLEIF id in every case; `country_iso2` stays GLEIF's primary country. The builder does the
  verification with one LEFT JOIN per rule country, the same normalisation as
  `company_identifier`.
- The manifest stamps every document from the map alone: `country_iso2` = the entity's
  jurisdiction, `company_id` = `registry_id` when `link_status != 'unverified'`, else `''`.
  The OAM-country equality check goes. The OAM country remains available as
  `esef_filings.country`.
- Consequence: AstraZeneca's rows become `GB` / `02723534`, Rizzo's become `SE` / `''`; the
  company-keyed outputs (`esef_document_company_information`, people, business items, group
  relationships, contact candidates) never carry a Swedish id the register does not know, and
  Swedish consumers keep filtering `country_iso2 = 'SE'`.
- One-off cleanup, owner-run after the deploy: delete the company-keyed rows that the old
  stamping produced (`company_id = ''` in contact candidates; `company_id = '5565401493'` in
  the five company-keyed tables), then re-run the parsing partitions that hold the 17 filers'
  60 documents (12 processed-week partitions) so their rows come back under the new stamps. The company_serving publish is then
  green with its models untouched.

### 2. People per filing

- A new paid asset `esef_document_people_extraction_clickhouse` and job
  `esef_document_people_job`, config-shaped like the enrichment (provider and model named
  explicitly, country filter, `max_documents`, `refresh_existing`), provider `deepseek`, model
  `deepseek-v4-flash` (owner's choice 2026-09-08; the v2 enrichment mostly ran on
  `z-ai/glm-5.3-flash`, 674 documents, and `stealth/ox-alpha`, 149, with DeepSeek on 172). Prompt `esef-people-v1`: the people part of the
  enrichment prompt only, over the `people_and_audit` tagged facts and the six people visible
  sections of one filing, with the same budgets and citation rules. Output: the existing
  `PersonCandidate` list, at most 100.
- Selection: every filing with a linked company, one extraction per (document, prompt
  version, model), newest filings first, Sweden first through the country filter. Cost for
  Sweden: 1,064 documents, roughly 11 million input tokens.
- Storage: a new table `esef_document_people_extraction` with one row per document and the
  enrichment's bookkeeping columns (status, artifact keys, response text and hashes, tokens,
  model, prompt version). `esef_document_people` is projected from this table only; the
  enrichment's `people_json` stays in `esef_document_company_information` as an artifact and is
  no longer projected. The projection replaces the table (stage plus EXCHANGE) instead of
  appending.
- Identity: `candidate_uid` = the company-source-record observation hash over
  `source_record_uid`, `esef_person`, prompt version, the normalised name and the role
  category, so a re-run with the same prompt replaces its rows; the materialised
  `person_profile_hash` and `person_role_hash` are unchanged. `fiscal_year` is the filing's,
  so a company's rosters accumulate per year.
- `se_company_person_esef` is unchanged.

### 3. Registered-office addresses

- A deterministic extractor `se_company_address_suggestions_esef` on the address entity:
  source `esef`, slot `''`, kind `registered`, `raw_address` = the
  `AddressOfRegisteredOfficeOfEntity` fact of the company's latest linked filing,
  `observed_at` = the filing's processed time, `source_record_uid` = the filing package's
  record uid (the `esef_report_package` hash every `esef_document_*` row carries). The
  normaliser parses the string; "Sverige" resolves to `SE` as any other country word does.
- The extractor list becomes scb, bolagsverket, ratsit, esef; address precedence gains
  `esef: 500` (display order only). No table changes on the address entity.
- Head office and principal place of business from the `company_contact` sections are a
  later slice.

## Rollout order

1. Slice 1: migration for `link_status`, deploy, rebuild the map, run the cleanup deletes,
   re-run the affected parsing partitions, run the two company_serving jobs (green).
2. Slice 3: deploy, run the esef address extractor and the address fold for the touched
   companies.
3. Slice 2: migration for the extraction table, deploy, run the people job for Sweden on
   DeepSeek, rebuild the people projection.

Slice 1 unblocks serving, slice 3 is cheap and deterministic, slice 2 spends money.

## Out of scope

- Any change to the company_serving models, the people entity fold or the basic-info fold.
- People passes for other countries (the config allows them later).
- The `economic_activity` field.
