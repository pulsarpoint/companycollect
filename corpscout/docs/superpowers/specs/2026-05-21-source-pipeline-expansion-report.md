# Corpscout Source Pipeline Expansion Report

Date: 2026-05-21

## Goal

Move the remaining corpscout company sources into source-specific data-pipeline modules, following the direction already taken for UK Companies House and Norway Brreg. The target sources are:

- GLEIF
- CVR (Denmark)
- Ariregister (Estonia)
- OpenCorporates

The report answers:

- whether each source supports an initial bulk load like Brreg;
- whether the current corpscout pull method is the right long-term method;
- what company, contact, ownership, domain, and financial data we can reasonably capture;
- which sources need translation or English normalization;
- how each source should be modeled as a separate data-pipeline module.

## Executive Summary

| Source | Bulk first load | Incremental path | Best long-term method | Financial data | Website/email | Owners/officers | Translation needed |
|---|---:|---|---|---|---|---|---|
| GLEIF | Yes | Golden Copy delta files or API filters | Data-pipelines bulk import from Golden Copy, not crawler pagination | No operating financials | No | Parent/child LEI relationships only | No, only code-list mapping |
| CVR (Denmark) | Yes | Datafordeler file download events/GraphQL, or full refresh by entity | Official Datafordeler Fildownload plus GraphQL/API for targeted refresh | Capital/financial/accounting structures and annual-report documents are available, but XBRL parsing may be needed for full P&L | Yes, in CVR entities | Yes, management, owners, beneficial owners, participants | Yes for Danish labels/free text |
| Ariregister (Estonia) | Yes | Daily open-data refresh; XML API/change list if contract is available | Official open-data download modules, with optional API for individual refresh | Yes, annual-report key indicators | Contacts in general data | Registry-card persons, shareholders, beneficial owners | Partial, mostly field/enum mapping; LLM only for Estonian free text |
| OpenCorporates | Yes, but licensed/commercial | API search/detail for targeted refresh, or licensed bulk deliveries | Use only with a bulk/license agreement or targeted enrichment, not current `q=*` crawl | Limited "latest accounts" and filings metadata, not reliable full financials | Sometimes through data/statements | Officers and relationships in API/bulk | Mixed source languages; do not use generic LLM translation by default |

Main recommendation: build source-specific Temporal workflows for all four sources. Do not extend the legacy `SourcePullWorker` path for these high-volume sources. For GLEIF, CVR, and Ariregister, the right first step is official bulk import into raw-input tables. For OpenCorporates, bulk is viable only if the project has the appropriate OpenCorporates data license and delivery access; otherwise use OpenCorporates as targeted enrichment or skip it for bulk.

## Current Corpscout State

The existing system now has two ingestion styles:

- Legacy crawler path: `SourcePullWorker` calls the Python crawler `/crawl/{source}`, then writes source-specific raw-input rows.
- Temporal data-pipeline path: `DataTaskWorker` starts external workflows, and data-pipelines writes raw rows directly into corpscout.

The Temporal integration currently handles only:

- `companies_house` -> `PullCompaniesHouse`
- `brreg` -> `PullBrreg`

Relevant local code:

- `scheduler/internal/workers/data_task.go` maps source names to Temporal workflows and currently includes only Companies House and Brreg.
- `data-pipelines/services/go-worker/workflows/pull_companies_house.go` implements paginated Companies House list pulling with `ContinueAsNew`.
- `data-pipelines/services/go-worker/workflows/pull_brreg.go` implements Brreg bulk download first, then incremental mode after a bulk checkpoint exists.
- `data-pipelines/services/go-worker/activities/activities.go` writes raw inputs for only `companies_house` and `brreg`.
- `scheduler/internal/service/raw_input_approval.go` supports approving raw inputs from only `gleif`, `companies_house`, and `brreg`.

Important gaps:

- `cvr`, `ariregister`, and `opencorporates` exist in `data_sources`, but their raw-input tables are not present.
- `SourceProcessWorker` only supports `gleif`, `companies_house`, and `brreg`.
- Migration `000038_retire_source_process.up.sql` sets `processor_task_type = NULL`, so the newer operational direction is raw-input approval rather than automatic processor suggestions.
- GLEIF has a raw-input table and a legacy Python crawler adapter, but not a Temporal data-pipeline workflow.
- Current country docs for Denmark and Estonia are stale relative to current official access. Denmark should prefer Datafordeler Fildownload/GraphQL over third-party `cvrapi.dk`; Estonia should prefer official open-data downloads.

## Source Analysis

### 1. GLEIF

Official access:

- GLEIF publishes Concatenated Files with LEI record and reference data from all LEI issuers worldwide, free of charge, in ZIP/XML form.
- GLEIF Golden Copy files and delta files are available through an automated download API. The API lists published files and supports latest full files and deltas in CSV, XML, and JSON.
- The GLEIF REST API supports LEI record search, full-text/fuzzy matching, ownership relationship lookup, code lists, and mapped identifiers.

Bulk feasibility:

- Yes. This is a strong bulk-first source.
- Use Golden Copy full files for first load.
- Use Golden Copy delta files for incremental updates if the operational cadence needs to be lower cost than API crawling.
- Use the API only for targeted lookup, relationship exploration, or fuzzy matching.

Data value:

- Strong for global deduplication by LEI.
- Legal name, legal jurisdiction, entity status, legal form, legal and headquarters addresses.
- Direct and ultimate parent/child relationships where reported through Level 2/RR data.
- No reliable website, email, phone, owner email, revenue, profit, or employee data.

Current approach assessment:

- Current corpscout `GLEIFAdapter` uses the REST API and cursor pagination. This is workable for small pages and testing, but not the best full-corpus path.
- For production, the full Golden Copy import is better because it avoids API pagination over millions of records and gives deterministic snapshots.

Recommended data-pipeline module:

- Python activity: `download_gleif_golden_copy`
  - Calls `https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.{json|csv}` for Level 1.
  - Also downloads `rr/latest` and `repex/latest` if relationship and exception data are in scope.
  - Stores large downloaded files on the worker filesystem.
- Go activity: `ImportGLEIFGoldenCopy`
  - Streams JSON/CSV/XML into `gleif_company_raw_inputs`.
  - Extracts `lei`, legal name, entity status, legal jurisdiction, headquarters country, legal form, parent LEI fields when present.
- Workflow: `PullGLEIF`
  - Mode `bulk`: full latest import.
  - Mode `delta`: after a checkpoint, apply `IntraDay`, `LastDay`, `LastWeek`, or `LastMonth` deltas.

Translation:

- No LLM translation should be required.
- Do not translate legal names.
- Normalize code-list values into English labels using GLEIF code lists or static mapping tables.

Schema needs:

- Existing `gleif_company_raw_inputs` can be reused, but add nullable `run_id TEXT` if it has not already been added by deployed migrations.
- Consider adding:
  - `legal_jurisdiction`
  - `legal_form_code`
  - `legal_form_name`
  - `registration_authority_id`
  - `entity_category`
  - `entity_creation_date`
  - relationship raw-input tables for RR and reporting exceptions if those are not kept inside the same raw payload.

### 2. CVR (Denmark)

Official access:

- The Danish Business Authority says CVR contains current and historical information about Danish businesses and that CVR data is public.
- Datafordeler provides CVR Fildownload as pre-generated JSON/CSV entity files, with API-key or OAuth access.
- CVR Fildownload includes entities such as address, participant, employment, industry, unit, fully liable participant relation, credit information, name, production unit, marketing protection, fax, phone, company, company form, and email address.
- Datafordeler states that only `CVRPerson` requires access approval; other CVR entities are not access restricted, but users still need Datafordeler setup with a user and IT system/API key or OAuth.
- Datafordeler REST services are marked as being phased out by the end of 2026, so new work should not be built around legacy REST.

Bulk feasibility:

- Yes. The official first-load path should be Datafordeler CVR Fildownload, not the current `cvrapi.dk` approach.
- It is not as frictionless as Brreg because Datafordeler requires account/IT-system setup and API-key/OAuth handling.
- It is still the right model for a durable corpscout integration.

Data value:

- Core company identity: CVR number, name, legal form, status, start/end dates, industry, addresses.
- Contact: website, email, phone, fax, marketing-protection flags.
- People and ownership: fully liable participants, management, legal owners, beneficial owners, founders, auditors, roles.
- Financial/capital: Datafordeler CVR model includes financial and capital-related objects. Annual reports and XBRL documents may require a second document-fetching/parsing pipeline.
- Employment: employee-related entities exist in the file download model.

Current approach assessment:

- Current crawler adapter uses `https://cvrapi.dk/api`, a third-party API with token support and a free quota. It is useful for single lookup experiments but not for a full national sync.
- Local docs mention the old `distribution.virk.dk/cvr-permanent/virksomhed/_search` Elasticsearch endpoint. That path should not be treated as the long-term integration unless official current access is confirmed during implementation.
- Best current path is Datafordeler Fildownload plus GraphQL/API for targeted individual refresh after bulk.

Recommended data-pipeline module:

- Python activity: `download_cvr_file_set`
  - Authenticates to Datafordeler with API key or OAuth.
  - Downloads configured entity files for company identity, names, addresses, email, phone, websites, industry, employment, company form, capital/financial, roles/ownership, and annual-report metadata.
  - Saves files by snapshot date and entity type.
- Go activity: `ImportCVRBulk`
  - Loads the company identity file first.
  - Joins/enriches related entity files by CVR number or entity IDs where practical.
  - Writes one company-level raw payload per company into `cvr_company_raw_inputs`, preserving the raw source entity fragments in nested form.
- Workflow: `PullCVR`
  - Mode `bulk`: download and import all configured entities.
  - Mode `incremental`: use Datafordeler change/event mechanisms when available; otherwise scheduled full refresh for changed entity partitions.
- Optional workflow: `PullCVRFinancials`
  - Fetches annual-report documents/XBRL for companies that need financial data.
  - Writes `company_financials` suggestions after parsing revenue, profit, employee count, assets, equity, liabilities.

Translation:

- Required for an operator-facing English normalized payload.
- Do not translate company names.
- Structural field names should be mapped directly to English.
- Deterministic mapping should handle common enums and code labels: legal form, company status, role type, industry labels, marketing protection, credit status.
- LLM translation should be reserved for free-text fields such as company purpose, signing rule, role notes, and narrative annual-report text if we choose to display it.
- Add a Danish translation cache using the same model as Brreg:
  - `source_lang = 'da'`
  - `target_lang = 'en'`
  - categories such as `legal_form`, `industry_code`, `status`, `role`, `purpose`, `signing_rule`, `financial_note`.

Schema needs:

- New table `cvr_company_raw_inputs`.
- Optional raw side tables if the first version keeps entity files separate:
  - `cvr_person_role_raw_inputs`
  - `cvr_financial_raw_inputs`
  - `cvr_document_raw_inputs`
- Translation columns should mirror Brreg if normalized English payload is required before approval:
  - `raw_payload_en`
  - `translation_status`
  - `translation_attempts`
  - `translation_error`
  - `translation_model`
  - `translation_prompt_version`
  - lease columns and translated timestamp.

### 3. Ariregister (Estonia)

Official access:

- Estonia's e-Business Register has an open-data download environment for larger volumes of registry data.
- Public data can be downloaded in machine-readable JSON and XML. Some datasets also provide CSV, including simple/general data and annual-report files.
- The downloadable datasets are updated once per day.
- The open-data files contain public data for legal entities currently in the register with statuses entered, in liquidation, or in bankruptcy.
- Available datasets include basic data, general data, registry cards, persons on registry card, persons not on registry card, shareholders, beneficial owners, and annual-report key indicators.
- Real-time XML API services exist, but use of those services requires a contract with RIK.

Bulk feasibility:

- Yes. This source is a very good bulk-first source.
- Use the official open-data downloads for first load and daily refresh.
- Use XML API/change-list services only if a contract is in place.

Data value:

- Basic data: legal-person name, registry code, legal form/subtype, VAT number, current status, first-entry date, address.
- General data: internal legal-person ID, accounting obligation, capital, shares without nominal value, legal succession, annual-report list, articles of association, contacts, areas of activity, registry cards.
- Persons/ownership: persons on registry cards, shareholders, beneficial owners.
- Financials: annual-report key indicators, including cash, current/non-current assets, liabilities, equity, revenue/total revenue, employee expense, depreciation/impairment, operating profit/loss, annual period profit/loss, average employee count, retained earnings, and profit/loss before tax.
- Contact: general-data contacts should be used for website/email/phone when available.

Current approach assessment:

- The existing Python `EstoniaAdapter` downloads only a simple CSV ZIP and returns a single giant response through the crawler service. This is acceptable as a prototype, but not a good production architecture.
- It ignores richer JSON/XML datasets that are needed for ownership, contacts, annual-report metadata, and financial indicators.
- It should move to a file-based Temporal import like Brreg bulk, not continue as an HTTP response payload through the crawler.

Recommended data-pipeline module:

- Python activity: `download_ariregister_dataset`
  - Downloads one configured dataset at a time from the open-data environment.
  - Supports basic, general, registry cards, persons on card, shareholders, beneficial owners, annual-report indicators.
  - Stores files by snapshot date and dataset name.
- Go activity: `ImportAriregisterBulk`
  - Streams files and assembles a company-level raw payload keyed by registry code.
  - Writes to `ariregister_company_raw_inputs`.
  - Writes annual-report key indicators either into raw payload first or directly into `company_financials` suggestions after company approval mapping exists.
- Workflow: `PullAriregister`
  - Mode `bulk`: daily full dataset import.
  - Mode `refresh`: compare downloaded file hashes or record hashes and only insert changed company raw rows.

Translation:

- Partial.
- The XML API supports an English language parameter for some query outputs, but bulk datasets still need field-name normalization and may include Estonian labels/free text.
- Do not translate legal names or person names.
- Use deterministic mapping for legal forms, statuses, role names, and EMTAK industry codes where possible.
- LLM translation is only needed for remaining Estonian free text, such as representation-right text or activity descriptions not covered by classifier mappings.

Schema needs:

- New table `ariregister_company_raw_inputs`.
- Translation columns can be added if operator-facing English payload is required before approval, but the approval gate can be lighter than Brreg if most fields are mapped without LLM.
- Add financial-import support:
  - either raw rows in `ariregister_financial_raw_inputs`
  - or direct `company_financials` suggestions keyed by approved company/registry code.

### 4. OpenCorporates

Official access:

- OpenCorporates API provides data from the OpenCorporates website as JSON/XML and requires an API key.
- Default limits are very low unless the account plan is upgraded.
- API pagination returns up to 100 records per page, and the page parameter is limited to 100.
- OpenCorporates bulk data exists as regular CSV deliveries. Bulk files are delivered in thematic files: companies, officers, non-registered addresses, alternative names, additional identifiers, and relationships. Delivery includes control files and can be made via SFTP.
- OpenCorporates data is either share-alike attribution open data or commercial, depending on account/license.

Bulk feasibility:

- Yes, but only with a bulk data agreement/license.
- It is not a public free "download latest snapshot" source like GLEIF or Estonia.
- Without a bulk agreement, this should not be implemented as a full global source pull.

Data value:

- Global legal-entity coverage across many jurisdictions.
- Core company identity, jurisdiction, company number, status, incorporation/dissolution dates, registered address, source/provenance.
- Officers, alternative names, additional identifiers, non-registered addresses, and relationships in bulk files.
- API company detail can expose filings and data/statements, where examples include website, sales tax number, addresses, official register entries, filings, and relationship statements.
- Financial data is inconsistent. Bulk companies mention "latest accounts", and filings may reference financial documents, but this is not a reliable full financial source.
- Website/email coverage is opportunistic, not a primary domain source.

Current approach assessment:

- Current crawler adapter calls `/companies/search` with `q="*"` and page pagination. This is not a valid full global sync strategy because API pagination is intentionally limited and account limits are low.
- For production, use OpenCorporates either as:
  - a licensed bulk import;
  - targeted enrichment for jurisdictions where official sources are not yet integrated;
  - reconciliation by known company name/number/jurisdiction.

Recommended data-pipeline module:

- If licensed bulk is available:
  - Python activity: `download_opencorporates_bulk_drop`
    - Reads from configured SFTP or object-store delivery.
    - Validates control files and partition counts.
  - Go activity: `ImportOpenCorporatesBulk`
    - Imports companies first, then officers, addresses, alternative names, additional identifiers, relationships.
    - Writes to `opencorporates_company_raw_inputs`, with optional side raw tables for officers and relationships.
  - Workflow: `PullOpenCorporates`
    - Mode `bulk`: import new delivery snapshot.
    - Mode `delta`: if delivered by OpenCorporates, process changed partitions.
- If no bulk license:
  - Do not schedule bulk.
  - Implement `EnrichOpenCorporatesCompany` for targeted lookups by `(jurisdiction_code, company_number)` or reconciliation by name.

Translation:

- Do not run a generic LLM translation pass over OpenCorporates globally.
- Preserve original source values and use code-level normalization for country/jurisdiction/status.
- Translate only a small subset of operator-facing free-text fields if a source-specific jurisdiction justifies it.

Schema needs:

- New table `opencorporates_company_raw_inputs`.
- Consider side tables:
  - `opencorporates_officer_raw_inputs`
  - `opencorporates_relationship_raw_inputs`
  - `opencorporates_identifier_raw_inputs`
- Capture provenance fields aggressively: source URL, publisher, retrieved_at, source type, confidence.

## Recommended Shared Pipeline Architecture

Each source should be a separate data-pipeline module with a common contract at the edges:

```text
corpscout scheduler
  DataTaskWorker
    -> source-specific Temporal workflow
      -> Python download/fetch activities
      -> Go import/write activities
      -> raw-input tables in corpscout DB
      -> optional translation workflow
      -> operator raw-input approval
      -> companies, company_locations, company_emails, company_phones,
         company_financials, company_relationships, company_domains
```

Module layout in `data-pipelines`:

```text
services/go-worker/
  workflows/
    pull_gleif.go
    pull_cvr.go
    pull_ariregister.go
    pull_opencorporates.go
    translate_cvr.go
    translate_ariregister.go
  activities/
    import_gleif.go
    import_cvr.go
    import_ariregister.go
    import_opencorporates.go

services/python-worker/
  activities/
    download_gleif_golden_copy.py
    download_cvr_file_set.py
    download_ariregister_dataset.py
    download_opencorporates_bulk.py
    fetch_opencorporates_company.py
    llm_translation.py

services/go-worker/contracts/
services/python-worker/contracts.py
```

Scheduler changes:

- Extend `sourceWorkflowType`:
  - `gleif` -> `PullGLEIF`
  - `cvr` -> `PullCVR`
  - `ariregister` -> `PullAriregister`
  - `opencorporates` -> `PullOpenCorporates` only when bulk licensed/configured.
- Extend `sourceDefaultCountry`:
  - `gleif` -> empty/global
  - `cvr` -> `DK`
  - `ariregister` -> `EE`
  - `opencorporates` -> empty/global
- Change `handleTriggerSource` to detect Temporal-backed sources by workflow type, not by non-empty country. The current check uses `country != ""`, which would skip global workflows such as GLEIF and OpenCorporates.
- Add source configuration validation so disabled or unlicensed sources cannot be bulk-triggered accidentally.
- Extend raw-input list/detail/approval service to support the new raw tables.

Database changes:

- Create raw-input tables for `cvr`, `ariregister`, and `opencorporates`.
- Ensure all Temporal-written raw-input tables have a nullable `run_id TEXT`.
- Add unique constraints on `(native_id, payload_hash)` for deterministic idempotency.
- Add processing/approval status fields consistently.
- Add translation columns only where required.
- Add source-specific financial raw tables only when a source has financial data that should be staged separately from identity data.

## Raw Payload Strategy

Use raw JSON as the durable contract. Avoid over-normalizing at import time.

Per-source import should write:

- stable native ID;
- best display name;
- country code;
- status;
- payload hash;
- run ID;
- source snapshot metadata;
- full raw payload;
- optional English-normalized payload if translation/normalization has run.

Approval and promotion should extract:

- company identity into `companies`;
- registry website into `companies.website` and `company_contact_suggestions` or direct raw-input approval output;
- phone/email into `company_phones` and `company_emails`;
- registered/headquarters addresses into `company_locations`;
- industry codes into `company_industries`;
- financial yearly values into `company_financials`;
- officers, owners, beneficial owners, and parent/subsidiary links into dedicated relationship/officer tables or suggestions.

## Domain Discovery Strategy

Registry-provided website/email should be the first signal, because it is the highest-confidence domain evidence.

Recommended confidence order:

1. Registry website from CVR, Ariregister, Brreg: 90-95.
2. Registry email domain when no website is present: 75-85, lowered for public email providers.
3. OpenCorporates website data/statements with provenance: 65-80 depending on source.
4. Wikidata/official social profile links: 60-80.
5. Search/cert heuristics from `discover_company_domains`: 40-70.

For each approved company, enqueue domain discovery only if:

- no official website is present; or
- the user explicitly asks for additional domains; or
- the company has a high-value source profile and the website evidence is stale or weak.

## Financial Data Strategy

Use `company_financials` as the target for structured yearly financial values.

Source-specific expectations:

- Brreg: current worker fetches annual accounts and extracts revenue/profit. This should eventually move into data-pipelines, because it is source-specific external fetch logic.
- CVR: use Datafordeler financial/capital/accounting objects first. For richer P&L/balance sheet, add a document/XBRL parser for annual reports.
- Ariregister: use annual-report key-indicator datasets. This is the best near-term financial source among the new sources.
- OpenCorporates: treat latest accounts/filings as metadata unless a licensed dataset contains structured financial fields for target jurisdictions.
- GLEIF: no operating financials.

Normalize monetary amounts:

- store original amount and currency when present;
- convert to USD cents using the existing ECB-based `fxrates` package;
- store exchange-rate source/date in evidence JSON;
- do not invent revenue/profit from text or filings unless a parser has extracted a concrete structured value.

## Translation and English Normalization

Reuse the Brreg pattern, but make it source-aware.

Sources needing translation:

- CVR: yes, for Danish labels and free text.
- Ariregister: partial, for Estonian labels/free text.
- OpenCorporates: not globally; only targeted fields.
- GLEIF: no LLM translation, only code mapping.

Recommended pattern:

```text
raw_payload          -> authoritative source record, never modified
raw_payload_en       -> curated English-normalized operator payload
translation_status   -> pending | translating | translated | failed
translation_cache    -> keyed by category, normalized text hash, source_lang,
                        target_lang, prompt_version, model
```

Do not translate:

- company legal names;
- person names;
- addresses except country/city labels when a classifier provides canonical English;
- identifiers;
- legal codes.

Translate or map:

- status labels;
- legal form descriptions;
- industry descriptions;
- role names;
- company purpose;
- signing rules;
- statutory purpose or activity descriptions;
- notes from financial or ownership records that operators need to read.

## Proposed Implementation Order

1. GLEIF data-pipeline bulk module
   - Lowest legal/auth friction.
   - Existing raw table and approval path already exist.
   - Replaces API pagination with deterministic snapshots.

2. Ariregister bulk module
   - Official open-data downloads are public and daily.
   - Strong value from annual-report indicators and beneficial-owner datasets.
   - Requires new raw table and approval support.

3. CVR bulk module
   - High value, especially website/email/owners/financials.
   - Requires Datafordeler setup, authentication, and translation.
   - Should not be built on third-party `cvrapi.dk` for production.

4. OpenCorporates module
   - Implement only after licensing/delivery decision.
   - Without bulk access, keep as targeted enrichment and reconciliation.

## Source-by-Source Module Design

### PullGLEIF

Inputs:

- `mode`: `bulk` or `delta`
- `file_type`: `lei2`, `rr`, `repex`, or all
- `format`: prefer JSON or CSV for import speed
- `force`

Activities:

- `download_gleif_golden_copy`
- `import_gleif_golden_copy`
- `save_sync_checkpoint`
- `mark_execution_complete`

Output:

- rows in `gleif_company_raw_inputs`
- optional relationship raw rows or relationship fields

### PullAriregister

Inputs:

- `datasets`: default `basic,general,persons_on_card,shareholders,beneficial_owners,annual_report_key_indicators`
- `snapshot_date`
- `force`

Activities:

- `download_ariregister_dataset`
- `import_ariregister_dataset`
- `compose_ariregister_company_payloads`
- `write_ariregister_raw_inputs`
- `mark_execution_complete`

Output:

- rows in `ariregister_company_raw_inputs`
- optional `company_financials` suggestions after approval mapping

### PullCVR

Inputs:

- `datasets`: configured Datafordeler entities
- `auth_method`: API key or OAuth
- `snapshot_date`
- `force`

Activities:

- `download_cvr_file_set`
- `import_cvr_entity_files`
- `compose_cvr_company_payloads`
- `write_cvr_raw_inputs`
- optional `prepare_cvr_translation_batch`
- `mark_execution_complete`

Output:

- rows in `cvr_company_raw_inputs`
- optional financial/document raw rows
- translation-gated approval queue

### PullOpenCorporates

Inputs:

- `mode`: `bulk` or `targeted`
- `delivery_path`: SFTP/object-store prefix for bulk
- `jurisdiction_code`
- `company_number`
- `force`

Activities:

- Bulk:
  - `download_opencorporates_bulk_drop`
  - `validate_opencorporates_control_files`
  - `import_opencorporates_companies`
  - `import_opencorporates_officers`
  - `import_opencorporates_relationships`
- Targeted:
  - `fetch_opencorporates_company`
  - `fetch_opencorporates_officers`
  - `fetch_opencorporates_filings`

Output:

- rows in `opencorporates_company_raw_inputs`
- optional side raw tables for officers and relationships

## Corrections To Current Source Registry Docs

The current `docs/sources/countries/dk.md` says CVR has a free ElasticSearch API and points to `cvrapi.dk` / `distribution.virk.dk`. For production, update it to:

- official source: Datafordeler CVR Fildownload and GraphQL;
- auth: Datafordeler account plus IT system API key/OAuth;
- note REST services are being phased out by end of 2026;
- note CVRPerson is protected, but other CVR entities are not access restricted.

The current `docs/sources/countries/ee.md` says the Ariregister API is free/no-auth. Update it to:

- official bulk: open-data downloads, no contract, daily;
- API/XML services: real-time but require contract;
- useful datasets: basic, general, registry-card persons, shareholders, beneficial owners, annual-report key indicators.

## Risks and Decisions Needed

- CVR access: implementation needs Datafordeler credentials and an IT-system/API key or OAuth setup.
- CVRPerson: avoid protected personal identifier fields unless the project has an approved need and OAuth access.
- OpenCorporates license: decide whether corpscout will purchase/use bulk delivery. Without that, do not schedule global bulk.
- Translation cost: CVR and Ariregister can be made cheap by mapping enums/classifiers first and sending only free text to the LLM.
- Financial parsing: CVR annual-report XBRL parsing is valuable but should be a second phase after core identity/contact/ownership ingestion.
- Raw table design: choose whether officer/owner/financial side data is embedded in company raw payloads or staged in separate source-specific raw tables. Embedding is simpler for first approval; side tables are better for high-volume relationship updates.

## References

- GLEIF Concatenated Files: https://www.gleif.org/en/lei-data/gleif-concatenated-file/about-the-concatenated-file
- GLEIF API overview: https://www.gleif.org/en/lei-data/gleif-api
- GLEIF Golden Copy and Delta Files API manual: https://www.gleif.org/lei-data/gleif-golden-copy/2022-02-23_gleif-golden-copy-and-delta-files_v2.2-final.pdf
- Danish Business Authority CVR overview: https://erhvervsstyrelsen.dk/det-centrale-virksomhedsregister-cvr
- Datafordeler CVR Fildownload: https://datafordeler.dk/dataoversigt/det-centrale-virksomhedsregister-cvr/cvr-fildownload/
- Datafordeler CVR access requirements: https://datafordeler.dk/vejledning/brugeradgang/anmodning-om-adgang/det-centrale-virksomhedsregister-cvr/
- Datafordeler CVR search service: https://datafordeler.dk/dataoversigt/det-centrale-virksomhedsregister-cvr/soegcvrdata/
- CVR domain model / object catalog: https://grunddatamodel.datafordeler.dk/objekttypekatalog/CentraleVirksomhedsregister/package-frame.html
- Estonian e-Business Register open data downloads: https://avaandmed.ariregister.rik.ee/en/node/13
- RIK company registration API overview: https://www.rik.ee/en/e-business-register/company-registration-api
- Estonian open-data API simple data service: https://avaandmed.ariregister.rik.ee/en/open-data-api/enterprise-simple-data-request-status-query
- OpenCorporates API overview: https://api.opencorporates.com/
- OpenCorporates API reference: https://api.opencorporates.com/documentation/API-Reference
- OpenCorporates bulk files: https://knowledge.opencorporates.com/knowledge-base/bulk-files-explained/
- OpenCorporates delivery mechanisms: https://knowledge.opencorporates.com/knowledge-base/which-delivery-mechanism-is-right-for-you/
