# ESEF: documents plus independent extractors, first slice `esef_domains`

Owner ruling 2026-09-13: "static parsing needs to be separate tasks. We have documents, and
many parsers that can parse that document to extract some information — not one big parser
with a version. Domain extraction is one asset connected to the `esef_domains` table, that's
it; same for people; some of these require an LLM, some don't."

This design replaces the monolithic per-document artifact (facts + disclosures + contact
candidates + concept labels behind one `ARTIFACT_SCHEMA_VERSION`, where any extractor fix is a
three-day arelle re-parse of 15,600 documents and a re-spend of the LLM passes) with a
document store and independent extractor assets. The first slice delivers the pattern and one
extractor, `esef_domains`, which also fixes the wrong domains found on 2026-09-13
(`banken.com`, `bankenfonder.se` from hyphenated line breaks; `svanen.se`, `ipcc.ch` third-party
referrals promoted to company domains).

## 1. Shape

- **Documents.** `corpscout.esef_filings` is the index (one row per filing, `fxo_id` =
  `source_document_id`, `package_sha256`, `lei`, `period_end`, `fiscal_year`). The filing
  packages are archived in the object store under `report_package_object_key(package_sha256)`
  (bucket `source-esef-filings`) by the weekly parse; a document counts as available for an
  extractor when it has `esef_facts` rows (proof that its package was archived and parsed).
- **Extractor.** One Dagster asset, one ClickHouse table, one `extractor_version` constant.
  It selects available documents whose row in its table is missing or carries an older
  version, downloads each package, opens the report members, runs a pure extraction function,
  and replaces its rows for the processed documents. No partitions, no shared artifact, no
  schema version. Deterministic extractors (domains, later contacts and addresses) and LLM
  extractors (people, company information — already shaped this way) are siblings that differ
  only in cost and config.
- **Re-run.** A per-extractor sensor (30-minute ticks) launches the asset when stale
  documents exist and no run of it is in flight. Bumping an extractor's version re-runs that
  extractor over every document, nothing else. The arelle facts parse stays as it is: it is the
  facts extractor, versioned by the artifact schema as today.
- **Unchanged this slice.** `esef_document_contact_candidates` keeps being produced by the
  artifact parser (websites included) until a later slice moves e-mails and phones onto an
  extractor of their own; consumers simply stop reading its website rows.

## 2. `esef_domains`

Migration `000404_corpscout_esef_domains` (ledger at 403 on 2026-09-13; re-check main and the
prod ledger at merge time):

```
CREATE TABLE IF NOT EXISTS corpscout.esef_domains
(
    domain_id FixedString(64),          -- sha256 of source_document_id + '\n' + registrable_domain
    source_document_id String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(toString(package_sha256)))))),
    package_sha256 String,
    lei String,
    period_end Date32,
    fiscal_year UInt16,
    registrable_domain String,
    hosts_json String,                  -- JSON array of hosts
    normalized_urls_json String,        -- JSON array of normalized URLs
    roles_json String,                  -- JSON array of suggested roles (company_website, investor_relations, auditor, corporate_responsibility, report_disclosure, social_media, external_reference, unknown)
    evidence_json String,               -- JSON array of EsefWebsiteEvidence
    evidence_count UInt32,
    corroborated UInt8,                 -- 1 when a tagged website fact, a known e-mail domain, or a repeated unbroken mention backs the domain
    extractor_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, source_document_id, registrable_domain);
```

plus `CREATE OR REPLACE VIEW corpscout.se_esef_domains` rendered by `country_views.py` from a
new `SeEsefView("esef_domains", ..., final=True)` entry in `tables.SE_ESEF_VIEWS` (the map join
on `lei` with `country_iso2 = 'SE' AND link_status = 'register_verified'`, exposing
`company_id`). Export columns: everything except `source_record_uid` and `resolved_at`.

Identity: one row per (document, registrable domain). A run replaces all rows of the documents
it processed (stage table + `EXCHANGE TABLES`, predicate `source_document_id IN (...)`), so a
domain that disappears from a re-extraction disappears from the table.

## 3. The extractor asset

`esef_domains_clickhouse` in `defs/esef_filings/domains_extractor.py` (group `esef`, kinds
python/s3/clickhouse/xbrl, pool `esef_domains_clickhouse`, `RetryPolicy(max_retries=3,
delay=60, EXPONENTIAL)`, deps: `esef_facts_clickhouse`, `esef_filings_clickhouse`,
`esef_document_contact_candidates_clickhouse`).

- `ESEF_DOMAINS_EXTRACTOR_VERSION = "esef-domains-v1"`.
- Config `EsefDomainsConfig`: `max_documents: int | None` (1..100 000), `source_document_ids:
  list[str]`, `refresh_existing: bool = False`, `workers: int = 4` (1..8), `batch_size: int =
  250` (documents per replace), `parse_timeout_seconds: int = 300` (60..3600).
- Selection (pure `stale_documents_sql()`): documents of `esef_filings FINAL` with
  `package_sha256 != ''` that have rows in `esef_facts`, LEFT JOIN
  `esef_domains` grouped by document (`max(extractor_version)`), keeping those with no row or
  a version below the current one (or all when `refresh_existing`), ordered by `period_end
  DESC, source_document_id`; `max_documents` applied in Python.
- Inputs per document: the package from the object store (downloaded to a temp dir, hash
  verified with `_verify_package_hash`), the report members selected by the existing
  `_extract_report_package` helper (moved or re-exported from `segment_parser` without
  behaviour change), the tagged website facts for the document from `esef_facts`
  (`concept_local_name IN ('WebsitesOfLegalEntity',
  'WebsiteAtWhichTheFinancialStatementsOfTheEntityAreDisclosedTogetherWithTheAuditorsReport',
  'WebsiteOfTheAuditEntity')`, non-numeric, as `TaggedWebsiteValue`s), and the known e-mail
  domains for the document from `esef_document_contact_candidates` (`candidate_kind = 'email'`
  → registrable domain of the address).
- Extraction: `website_candidates.extract_website_candidates(report_paths,
  tagged_values=..., known_email_domains=...)` — the merged fix (hyphen rejoin with
  corroboration, `external_reference`). `corroborated` = the candidate's registrable domain is
  a tagged website fact, a known e-mail domain, or mentioned unbroken more than once (the same
  set the extractor uses for the hyphen decision; exposed by `extract_website_candidates` on the
  candidate).
- Execution: documents processed in a `ProcessPoolExecutor` of `workers`, each document's
  extraction inside the per-document timeout wrapper generalised from
  `segment_assets._parse_document_package_worker` (`run_in_child_with_timeout(fn, args,
  timeout)`); a timed-out or failed document is logged, counted (`failed_document_count`,
  `timed_out_document_count`) and skipped (no row; retried on the next run). Rows are written per
  `batch_size` documents so progress survives a mid-run failure. Temp files are removed per
  document.
- Metadata: `candidate_document_count`, `attempted_document_count`, `processed_document_count`,
  `failed_document_count`, `timed_out_document_count`, `row_count` (rows written this run),
  `documents_without_domains`, `extractor_version`, `workers`, `wall_seconds`,
  `extract_seconds`, `table`.
- Writer: `replace_document_rows(clickhouse, *, table, columns, source_document_ids, rows)` —
  the stage + `EXCHANGE TABLES` writer factored out of `llm_enrichment_assets` with a
  document-only predicate (the LLM writer keeps its provider/model/prompt predicate on top).

## 4. The sensor

`esef_domains_stale_sensor` in `domains_extractor.py` (`minimum_interval_seconds=1800`,
`default_status=RUNNING`, `clickhouse: ClickhouseResource`): counts stale documents with the
selection query; if any and no run of the asset is in flight (`get_run_records` on the asset's
job with active statuses) and no failed run in the last hour, launches one run of the asset
(`max_documents = 5000`, `workers = 4`) with tags `dagster/priority: 5`,
`launched_by: esef_domains_stale_sensor`. The stale-weeks sensor stays as it is for the facts
parse.

## 5. Consumers

- `company_serving/dbt/models/company_domains_build.sql`, `esef_sources` leg: reads
  `se_esef_domains` (new source in `sources.yml`). Confidence: `company_website` in roles →
  0.95 (`explicit_company_website`); else `corroborated = 1 OR evidence_count >= 2` → 0.90
  (`repeated_filing_website`); else if `external_reference` in roles → the row is excluded;
  else 0.50 (`filing_website_mention`). Everything else in the model unchanged.
- `company_contact_current_build.sql`: excludes `candidate_kind = 'website'` from the contact
  candidates (websites are domains now, not contacts).
- Backoffice ESEF tab (`se-company-esef.server.ts`): the websites section reads
  `se_esef_domains` (domain, roles, evidence count, corroborated) instead of the website rows of
  the contact candidates. Other backoffice readers are untouched.

## 6. Rollout

1. Owner applies 000404 (`make clickhouse-migrate-up-one`), verifies `esef_domains` and
   `se_esef_domains` exist.
2. Owner deploys from main after `dg utils refresh-defs-state` (the dbt source and model
   change) — the deploy also ships the extraction fix merged on 2026-09-13 (636f30042) to the
   weekly parse.
3. The sensor starts RUNNING and extracts every available document (about 15,600) in runs of
   up to 5,000, lxml only, roughly 2–3 hours at 4 workers; the bad domains disappear as each
   batch lands. Verify: `se_esef_domains` for Handelsbanken (5020077862) lists
   `handelsbanken.com` (company_website), `handelsbanken.se`, `handelsbankenfonder.se`, and
   `svanen.se`/`ipcc.ch` only as `external_reference`; no `banken.com` / `bankenfonder.se`
   anywhere in the table.
4. The company_serving build + publish (the owner's or the serving track's launch) picks up
   the new leg; verify no `esef_filing` domain row for Handelsbanken carries `banken.com`.

## 7. Testing

- `website_candidates` tests (merged) cover the extraction; the extractor module gets: the
  selection SQL text and a clickhouse-local run (fixture rows in `esef_filings`, `esef_facts`,
  `esef_document_contact_candidates`, `esef_domains` at an older version → the stale set);
  the asset run with the fake object store holding a small real package from
  `tests/fixtures/esef_filings/` and the fake ClickHouse (rows written with the 17 export
  columns in order, `corroborated`, batch replace statements, failed/timed-out documents
  skipped and counted); the timeout wrapper on a sleeping function; the sensor's selection
  and launch (fakes); migration column-order contract and `SE_ESEF_VIEWS` drift pins; the dbt
  model's confidence rule (a SQL-text pin plus the existing company_serving harness if one
  exists); the backoffice reader's query pin.

## 8. Out of scope (later slices)

- Removing website extraction from the artifact parser and moving e-mails/phones to an
  `esef_contacts` extractor; then dropping `esef_document_contact_candidates`.
- Moving the people pass and the company-information enrichment onto the same module layout
  (they already are independent assets with their own tables and versions; only naming and
  the shared helpers module change).
- A cached visible-text product shared by several deterministic extractors, if a second text
  extractor makes the per-document lxml parse worth sharing.
- Addresses from the `company_contact` sections (spec 2026-09-08, section 3 note).
