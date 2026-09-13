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
  `source_document_id`, `package_sha256`, `lei`, `period_end`). The filing packages are
  archived in the object store under `report_package_object_key(package_sha256)` (bucket
  `source-esef-filings`) by the weekly parse; a document counts as available for an extractor
  when it has `esef_facts` rows (proof that its package was archived and parsed).
- **Extractor.** One Dagster asset, one ClickHouse table, one `extractor_version` constant.
  It selects available documents whose rows in its table are missing or carry another
  version, downloads each package, opens the report members, runs a pure extraction function,
  and replaces its rows for the processed documents. No partitions, no shared artifact, no
  schema version. Deterministic extractors (domains, later contacts and addresses) and LLM
  extractors (people, company information — already shaped this way) are siblings that differ
  only in cost and config.
- **Every attempted document leaves a row.** A document whose extraction found nothing, or
  failed, or timed out, gets one marker row (`registrable_domain = ''`, `extraction_status`
  `empty` / `failed` / `timed_out` with the error text) at the current version, so it is not
  selected again until the version changes or an operator re-runs it explicitly
  (`refresh_existing`, `source_document_ids`). A run interrupted before a batch was written
  leaves those documents stale, and the next run retries them.
- **Re-run.** A per-extractor sensor (30-minute ticks) launches the asset when stale
  documents exist and no run of it is in flight. Bumping an extractor's version re-runs that
  extractor over every document, nothing else. The arelle facts parse stays as it is: it is the
  facts extractor, versioned by the artifact schema as today.
- **Light modules.** A deterministic extractor's per-document function runs in a spawned,
  killable child with a wall-clock budget (the guard the artifact parser gained on
  2026-09-12), so the modules it imports must not pull in dagster, arelle or the artifact
  parser: every import is paid once per document. Three small modules carry this:
  `defs/common/child_timeout.py` (the timeout wrapper, stdlib only),
  `defs/esef_filings/report_package.py` (opening a package and selecting its report members,
  moved out of `segment_parser.py`, which keeps importing the same names) and the extractor's
  own `defs/esef_filings/domains_extraction.py`.
- **Unchanged this slice.** `esef_document_contact_candidates` keeps being produced by the
  artifact parser (websites included) until a later slice moves e-mails and phones onto an
  extractor of their own; consumers simply stop reading its website rows. The artifact's
  candidate shape (`EsefWebsiteCandidate`) does not change.

## 2. `esef_domains`

Migration `000404_corpscout_esef_domains` (ledger at 403 on 2026-09-13; the SE financial track
also plans a 000404 — re-check main and the prod ledger at merge time and renumber if needed):

```
CREATE TABLE IF NOT EXISTS corpscout.esef_domains
(
    domain_id FixedString(64),          -- sha256 of source_document_id + '\n' + registrable_domain
    source_document_id String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(toString(package_sha256)))))),
    package_sha256 String,
    lei String,
    period_end Date32,
    fiscal_year UInt16,                 -- year(period_end), as the artifact parser derives it
    extraction_status LowCardinality(String),  -- ok | empty | failed | timed_out
    registrable_domain String,          -- '' on a marker row
    hosts_json String,                  -- JSON array of hosts
    normalized_urls_json String,        -- JSON array of normalized URLs
    roles_json String,                  -- JSON array of suggested roles (company_website, investor_relations, auditor, corporate_responsibility, report_disclosure, social_media, external_reference, unknown)
    evidence_json String,               -- JSON array of EsefWebsiteEvidence
    evidence_count UInt32,
    corroborated UInt8,                 -- 1 when a tagged website fact, a known e-mail domain, or a repeated unbroken mention backs the domain
    error_message String,               -- failed / timed_out marker rows only
    extractor_version LowCardinality(String),
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
`company_id`). `ESEF_DOMAINS_EXPORT_COLUMNS` (18): everything except `source_record_uid` and
`resolved_at`; the view lists all 20 with `source_record_uid` spliced after
`source_document_id`, as the contact-candidates view does.

Identity: one row per (document, registrable domain). A run replaces all rows of the documents
it attempted (stage table + `EXCHANGE TABLES`, predicate `source_document_id NOT IN (...)`), so
a domain that disappears from a re-extraction disappears from the table.

## 3. The extractor asset

`esef_domains_clickhouse` in `defs/esef_filings/domains_extractor.py` (group `esef`, kinds
python/s3/clickhouse/xbrl, pool `esef_domains_clickhouse`, `RetryPolicy(max_retries=3,
delay=60, EXPONENTIAL)`, deps: `esef_filings_clickhouse`, `esef_facts_clickhouse` and
`esef_document_contact_candidates_clickhouse` through `AllPartitionMapping`), plus
`esef_domains_job` selecting it (the sensor's job).

- `ESEF_DOMAINS_EXTRACTOR_VERSION = "esef-domains-v1"` (in `domains_extraction.py`).
- Config `EsefDomainsConfig`: `max_documents: int | None` (1..100 000), `source_document_ids:
  list[str]` (re-extract exactly these, stale or not), `refresh_existing: bool = False`,
  `workers: int = 4` (1..8), `batch_size: int = 250` (documents per replace),
  `parse_timeout_seconds: int = 300` (60..3600).
- Selection (pure `stale_documents_sql()`): documents of `esef_filings FINAL` with
  `package_sha256 != ''` and `period_end <= today()` that have rows in `esef_facts`, LEFT JOIN
  `esef_domains` grouped by document (`max(extractor_version)`), keeping those with no row or
  a version different from the current one (or all when `refresh_existing` or listed), ordered
  by `period_end DESC, fxo_id`; `max_documents` applied in Python; `fiscal_year =
  period_end.year`.
- Inputs per run (one query each, ids chunked): the tagged website facts from `esef_facts`
  (`concept_local_name IN` the three website concepts of `website_candidates`, `raw_value`,
  deduplicated; `esef_facts` does not record the report member, so their evidence carries
  `report_member = ''`) and the known e-mail domains from `esef_document_contact_candidates`
  (`candidate_kind = 'email'`, the part after `@`).
- Per document: the package is read from the object store (`read_bytes`), its SHA-256 checked
  against `package_sha256`, written to the run's temp dir, and handed to a process pool of
  `workers`; downloads run at most `2 × workers` ahead of the extraction so the temp dir never
  holds more than a handful of packages. The pool worker runs `extract_package_domains` inside
  `run_in_child_with_timeout` (spawned child, `parse_timeout_seconds`, terminate then kill):
  `report_package.extract_report_package` selects the report members and
  `website_candidates.extract_website_candidates_with_corroboration` (the merged fix: hyphen
  rejoin with corroboration, `external_reference`) returns the candidates plus the set of
  corroborated registrable domains (tagged fact, e-mail domain, or more than one unbroken
  mention); `corroborated` on a row is membership in that set. A download failure, a timed-out
  or failed child yields the marker row described in section 1 and the run continues.
- Rows are written per `batch_size` documents so progress survives a mid-run failure; the
  temp package is removed as soon as its extraction settles.
- Metadata: `candidate_document_count`, `attempted_document_count`, `processed_document_count`
  (ok + empty), `documents_without_domains`, `failed_document_count`,
  `timed_out_document_count`, `row_count` (rows written this run), `extractor_version`,
  `workers`, `wall_seconds`, `extract_seconds`, `table`.
- Writer: `document_rows.replace_document_rows(clickhouse, *, table, columns,
  source_document_ids, rows)` — the stage + `EXCHANGE TABLES` recipe of the LLM writer with a
  document-only predicate, in its own module so later extractors share it (the LLM writer is
  left as it is this slice).

## 4. The sensor

`esef_domains_stale_sensor` in `defs/esef_filings/domains_sensor.py`
(`minimum_interval_seconds=1800`, `default_status=RUNNING`, `clickhouse: ClickhouseResource`):
counts stale documents with the selection query; if any and no run of `esef_domains_job` is in
flight (`get_run_records` with the active statuses) and none failed in the last hour, launches
one run of the asset (`max_documents = 5000`, `workers = 4`) with tags `dagster/priority: 5`,
`launched_by: esef_domains_stale_sensor`; otherwise a `SkipReason` naming the three counts.
The stale-weeks sensor stays as it is for the facts parse.

## 5. Consumers

- `defs/company_serving/dbt/models/company_domains_build.sql`, `esef_sources` leg: reads
  `se_esef_domains` (new source in `sources.yml`, asset key `esef_domains_clickhouse`), rows
  with `extraction_status = 'ok'`, `registrable_domain != ''` and roles other than exactly
  `["external_reference"]`. Confidence: `company_website` among the roles → 0.95
  (`explicit_company_website`); else `corroborated = 1 OR evidence_count >= 2` → 0.90
  (`repeated_filing_website`); else 0.50 (`filing_website_mention`). `observed_at` stays the
  view's `resolved_at`. Everything else in the model unchanged.
- `company_contact_current_build.sql`: excludes `candidate_kind = 'website'` from the contact
  candidates (websites are domains now, not contacts).
- Backoffice ESEF tab (`se-company-esef.server.ts` + `admin-se-company-esef.tsx`): a new
  "Websites" card reads `se_esef_domains` (domain, roles, evidence count, corroborated, fiscal
  year); the contact-candidates query excludes website rows. Other backoffice readers are
  untouched.

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

- `website_candidates` tests (merged) cover the extraction; new: the corroboration set. The
  child-timeout wrapper on a sleeping, a raising and a hard-exiting function; `report_package`
  on synthetic zips (standard `reports/` members, a non-standard member with the inline-XBRL
  namespace, unsafe paths, backslash repair); `domains_extraction` on a synthetic package
  (candidates, corroboration, marker rows, a bad package as `failed`) and a subprocess check
  that importing it does not import dagster or arelle; the extractor module: selection SQL
  text, `select_documents` / the two loaders on a fake ClickHouse, the writer's statement
  sequence, and a run with the fake object store holding a synthetic package (rows in the 18
  export columns in order, `corroborated`, batch replace statements, a missing package counted
  as failed with a marker row); the sensor's pure decision and its `evaluate_tick` with fakes;
  migration column-order contract and `SE_ESEF_VIEWS` drift pins (the new view pinned to
  000404); the dbt model's SQL-text pins plus the existing `dbt parse` test; the backoffice
  reader's query pin and loader mapping.

## 8. Out of scope (later slices)

- Removing website extraction from the artifact parser and moving e-mails/phones to an
  `esef_contacts` extractor; then dropping `esef_document_contact_candidates`.
- Moving the people pass and the company-information enrichment onto the same module layout
  (they already are independent assets with their own tables and versions; only naming and
  the shared helpers module change), and pointing the LLM writer at `replace_document_rows`.
- Switching the artifact parser's per-document guard to `common/child_timeout.py`.
- A cached visible-text product shared by several deterministic extractors, if a second text
  extractor makes the per-document lxml parse worth sharing.
- Addresses from the `company_contact` sections (spec 2026-09-08, section 3 note).
