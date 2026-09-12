# ESEF: country-agnostic products, the `se_esef_*` views, people per filing, registered-office addresses

Date: 2026-09-08, revised 2026-09-09. Owner rulings: the company_serving models are not patched
around bad ESEF rows ("for now just properly extract people from esef and do proper lei
mapping"); addresses from ESEF are wanted; the people pass runs on DeepSeek; and, on 2026-09-09,
the ESEF pipeline does no country-specific work at all: "the product of the esef pipeline is
just tables with information produced by esef documents. Then we can create specific view or
views that will filter these entries based on country and map these views to specific
country." The views are named `se_esef_<table>`.

## Facts (prod and repo, 2026-09-08)

### Entity link

- 1,415 filings on the Swedish OAM (Officially Appointed Mechanism, the national store of
  regulated information; Finansinspektionen's in Sweden) from 417 LEIs. Every LEI is in
  `esef_entity_registry_map` (`match_source = gleif_registered_as`, built by
  `esef_entity_registry_map_clickhouse` from GLEIF's registered-as id; `country_iso2` there is
  GLEIF's primary country). 404 resolve to a Swedish org number; 403 of those exist in
  `se_company_basic_info`. The one that does not is Rizzo Group AB (5565401493): mapped
  correctly, LEI LAPSED, absent from both register files (bankrupt).
- The other 16 filers are foreign-jurisdiction companies listed in Stockholm (AstraZeneca GB,
  Autoliv US-DE, Lundin Mining, Lundin Gold, Lucara, Meren, Josemaria, IPC CA, Fenix Outdoor
  and Cavotec CH, CoinShares JE, Sotkamo Silver FI, Fiven NO, SGL DK, Gentoo MT, "SKF Group"
  AU). A filing on the Swedish OAM is not a Swedish company: any company admitted to a
  regulated market in Sweden files there.
- The pipeline stamps `country_iso2` (the OAM country) and `company_id` on every parsed row at
  manifest time, and only when the map's country equals the OAM country
  (`segment_assets.py`, `_company_id_for_index_row`). So the 16 get `SE` and `''`; the model
  stage skips empty ids; contact candidates still emit them (367 rows). Rizzo is stamped with
  an id no register knows. A mapping fix means re-parsing partitions, because the link is
  frozen into the data.
- Stamped columns today: `esef_disclosures`, `esef_document_concept_labels`,
  `esef_document_contact_candidates`, `esef_document_company_information` carry
  `country_iso2` + `company_id` outside their sorting keys; `esef_document_people`,
  `esef_document_business_items`, `esef_document_group_relationships` carry `country_code` +
  `company_id` as the first two sorting-key columns (18,239, 33,779 and 14,514 rows).
  `esef_filings`, `esef_facts`, `esef_financial_metrics` are LEI-keyed and clean.
- `company_identifier` already implements the register-verified LEI link (jurisdiction check,
  digits-normalised id, must exist in the register) for SE, NO, FI and FR
  (`company_identifier/rules.py`).
- Consumers of the stamps outside the ESEF module: the basic-info `esef` extractor
  (`company_information WHERE country_iso2 = 'SE'`), `se_company_person_esef` (a view over
  `esef_document_people WHERE country_code = 'SE'`, rendered by
  `company_people/source_views.py`), company_serving dbt (`esef_sources` joins the map to
  `se_company_basic_info` and `esef_filings`; the domains, contacts, description and
  management legs read the stamped `company_id`), the backoffice (the ESEF tab in
  `se-company-esef.server.ts` and `se-company-esef-llm.server.ts`, the public company page's
  ESEF sections in `queries.server.ts`, the notes page in `esef-report-notes.server.ts`: every
  one queries a product by the stamped `company_id`), and `clickhouse_checks.py`.
- The company_serving publish fails on exactly the stamped rows: 140 contact rows with an
  empty id, 5 contact rows and 24 people rows for Rizzo.

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

### Addresses

- Nothing extracts an address from ESEF. The parser isolates `company_contact` visible
  sections (1,050 Swedish documents) but never sends them anywhere.
- The filings tag the address: `AddressOfRegisteredOfficeOfEntity` is present in 1,404 of the
  1,415 Swedish filings (`esef_facts`, LEI-keyed, undimensioned, mostly tagged `sv`, e.g.
  "Fleminggatan 20, 112 26 Stockholm, Sverige"), with `DomicileOfEntity`,
  `CountryOfIncorporation`, `LegalFormOfEntity` (1,404 each) and `PrincipalPlaceOfBusiness`
  (1,140).
- The address entity (`2026-09-06-se-company-address-entity-design.md`) has the kind
  `registered` in its vocabulary, publishes every address any extractor delivers (precedence
  orders, never filters), merges equal normalised addresses across sources, and named ESEF
  address extraction as the separate work it is.

## Design

### 1. Country-agnostic products, one map, the `se_esef_*` views

- **Keys.** Every ESEF product is keyed by what it is: LEI plus document (`source_document_id`
  / `fxo_id`, `package_sha256`, `period_end`, `fiscal_year`). No product carries a country or
  a company id. The parse row builders stop writing `country_iso2` / `country_code` /
  `company_id`; the manifest no longer needs the map at all.
- **The map.** `esef_entity_registry_map` stays the one link: one row per filer LEI,
  `country_iso2` = GLEIF's primary country, `registry_id` = the normalised registered-as id,
  plus a new `link_status`: `register_verified` when the country has a rule in
  `COUNTRY_IDENTITY_RULES` and the id exists in that register; `unverified` when the country
  has a rule and the id does not; `gleif` when the country has no register here. The builder
  does the verification with one LEFT JOIN per rule country, the same normalisation as
  `company_identifier`. Rebuilt whenever GLEIF or a register changes; nothing else needs a
  re-run.
- **The views.** One view per product Sweden reads, named `se_esef_<table>`, defined in the
  ESEF module and created by migration: `se_esef_filings`, `se_esef_document_company_information`,
  `se_esef_document_people`, `se_esef_document_business_items`,
  `se_esef_document_group_relationships`, `se_esef_document_contact_candidates`,
  `se_esef_facts`, `se_esef_disclosures`. Each is the product joined to the map on `lei`
  with `country_iso2 = 'SE' AND link_status = 'register_verified'`, exposing `company_id` (the
  registry id) beside the product's own columns. A dead company has no verified link, so it
  is in no view; a foreign filer appears in another country's view if one is ever created.
- **Consumers move to the views.** The basic-info `esef` extractor reads
  `se_esef_document_company_information`; `se_company_person_esef` reads
  `se_esef_document_people`; company_serving's ESEF legs read the `se_esef_*` views and the
  `esef_sources` CTE reads `se_esef_filings` (the map join is inside the view); every backoffice
  query that looks a product up by `company_id` reads the matching `se_esef_*` view;
  `clickhouse_checks.py` keeps checking the map.
- **The model stage** selects per LEI (latest filing per LEI, one document per LEI, prompt
  and model), reads no country. Which LEIs get a paid pass is run configuration: a
  `link_statuses` filter (default `register_verified`) and a `country_iso2s` filter, both
  resolved through the map at selection time.
- **Existing data and DDL.** The stamped columns leave the DDL per the ledger policy and are
  dropped by hand: a plain `DROP COLUMN` on disclosures, concept labels, contact candidates
  and company information; the three small model-output tables are recreated with
  `ORDER BY (source_record_uid, fiscal_year, candidate_uid)` (a `_next` table filled from
  the old one, then `EXCHANGE TABLES`). The company_serving publish is then green with its
  models untouched: the empty-id and Rizzo rows are not in any Swedish view.

### 2. People per filing

- A new paid asset `esef_document_people_extraction_clickhouse` and job
  `esef_document_people_job`, config-shaped like the enrichment (provider and model named
  explicitly, `link_statuses`, `country_iso2s`, `max_documents`, `refresh_existing`),
  provider `deepseek`, model `deepseek-v4-flash` (owner's choice 2026-09-08; the v2
  enrichment mostly ran on `z-ai/glm-5.3-flash`, 674 documents, and `stealth/ox-alpha`, 149,
  with DeepSeek on 172). Prompt `esef-people-v1`: the people part of the enrichment prompt
  only, over the `people_and_audit` tagged facts and the six people visible sections of one
  filing, with the same budgets and citation rules. Output: the existing `PersonCandidate`
  list, at most 100.
- Selection: every filing of a LEI the config admits, one extraction per (document, prompt
  version, model), newest filings first. Cost for Sweden: 1,379 documents (1,044 with a
  people section plus 335 whose only people evidence is `people_and_audit` tagged facts,
  cheap), roughly 11-13 million input tokens. 61 (LEI, period_end) pairs carry two filings
  (re-filings and NO/SE dual filings) and are paid and projected twice under "every filing"
  as written above -- an owner ruling on deduping them is pending; a
  `row_number() OVER (PARTITION BY lei, period_end)` filter in the selection query is a
  one-line follow-up if that duplication turns out to be unwanted. Newest-first also
  surfaces a 2029-dated filing first (`549300GU5OHTR1T5IY68-2029-05-01-ESEF-SE-0`, an
  upstream period bug, not a selection bug); it is the first document the rollout smoke run
  processes.
- Storage: a new table `esef_document_people_extraction` with one row per document and the
  enrichment's bookkeeping columns (status, artifact keys, response text and hashes, tokens,
  model, prompt version), keyed by document. `esef_document_people` is projected from this
  table only, keyed by (`source_record_uid`, `fiscal_year`, `candidate_uid`); the
  enrichment's `people_json` stays in `esef_document_company_information` as an artifact and
  is no longer projected. The projection replaces the table (stage plus EXCHANGE) instead of
  appending.
- Identity: `candidate_uid` = the company-source-record observation hash over
  `source_record_uid`, `esef_person`, prompt version, the normalised name, the role
  category and the normalised role text, so a re-run with the same prompt replaces its rows;
  the materialised `person_profile_hash` and `person_role_hash` are unchanged. `fiscal_year`
  is the filing's, so a company's rosters accumulate per year.
- `se_company_person_esef` no longer exists: the SE person slice 0 dropped that view on
  2026-09-09. The reader of `se_esef_document_people` is now the SE person entity's extractor,
  `se_company/person/esef.py`, which tombstones vanished slots itself and is re-run after the
  projection rebuild.

**Rulings (2026-09-10):**

- `esef_document_people` keeps its 000395 key, `(lei, fiscal_year, source_record_uid,
  candidate_uid)`.
- The extraction table `esef_document_people_extraction` has no Swedish view: it has no
  consumer.
- `se_company_person_esef` is dropped, not merely unchanged (corrected above).
- Migration number 000397 (000396 went to the person entity, merged 2026-09-09).
- An `invalid_response` from either LLM pass keeps its raw text beside the
  artifact for inspection; still no row, retried on the next run.
- 2026-09-12: the role text joins the identity -- a person's committee seat is its own row
  (owner: name and positions matter, not only the person); duplicate filings for one (LEI,
  period end) are kept, both extracted and projected (owner: fine to be duplicated).

### 3. Registered-office addresses

- A deterministic extractor `se_company_address_suggestions_esef` on the address entity:
  source `esef`, slot `''`, kind `registered`, `raw_address` = the
  `AddressOfRegisteredOfficeOfEntity` fact of the company's latest filing in `se_esef_facts`,
  `observed_at` = the filing's processed time, `source_record_uid` = the filing package's
  record uid (the `esef_report_package` hash every `esef_document_*` row carries). The
  normaliser's `raw_address` path parses only the Bolagsverket packed form, so the extractor
  cleans the fact (tags, whitespace, trailing punctuation and country word) and re-packs it
  when a Swedish postcode is found; a fact without one is delivered as street and town
  components with `raw_address` NULL. A trailing "Sverige"/"Sweden" resolves to `SE`.
- The extractor list becomes scb, bolagsverket, ratsit, esef; address precedence gains
  `esef: 500` (display order only). No table changes on the address entity.
- Head office and principal place of business from the `company_contact` sections are a
  later slice.

## Rollout order

1. Slice 1: migration (map `link_status`, the eight `se_esef_*` views, the stamped columns
   leaving the DDL), deploy, rebuild the map, switch the consumers, run the company_serving
   jobs (green), then the owner-run column drops and the three table rebuilds.
2. Slice 3: deploy, run the esef address extractor and the address fold for the touched
   companies.
3. Slice 2: migration for the extraction table and the people table's new key, deploy, run
   the people job for Sweden on DeepSeek, rebuild the people projection.

Slice 1 unblocks serving, slice 3 is cheap and deterministic, slice 2 spends money.

## Out of scope

- Any change to the company_serving models, the people entity fold or the basic-info fold.
- Views for other countries (the pattern is ready; each is one migration).
- People passes for other countries (the config allows them later).
- The `economic_activity` field (done, slice 6).
