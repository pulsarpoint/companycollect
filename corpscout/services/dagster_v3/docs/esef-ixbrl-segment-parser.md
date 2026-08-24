# ESEF iXBRL segment parser

The core parser resolves each content-addressed ESEF report package once with
Arelle and writes a deterministic, versioned JSON artifact. It does not call an
LLM or update canonical company profiles. Deterministic DuckDB/ClickHouse
projections are derived from this artifact, while a separate downstream stage
can use a bounded subset of its evidence to produce reviewable LLM enrichment
candidates.

The artifact contains:

- every fact with its canonical value, entity, period, unit, language, decimals,
  dimensions, report member, and source fact ID;
- concept labels, documentation, data types, extension status, anchors, and
  presentation relationships;
- document-level namespaces, entities, periods, units, languages, and taxonomy
  entry points;
- references into semantic review segments such as identity, business profile,
  contacts, products and markets, people and audit, group structure, workforce,
  securities, and financial highlights;
- deterministic email and phone candidates from tagged contact facts,
  `mailto:`/`tel:` links, and visible XHTML text. Values are syntax-validated,
  emails are normalized, and phone numbers are stored in E.164 form;
- deterministic website candidates from tagged website facts, visible links,
  and visible XHTML text. URLs and hosts are grouped by a Public Suffix
  List-validated eTLD+1 registrable domain;
- bounded visible-report sections for board composition, executive management,
  board committees, auditor appointments, annual-report signatures, company
  contacts, person profiles, ownership/listing information, and company
  overviews. Semantic XHTML is read by heading/container order; PDF-style XHTML
  is reconstructed from its page, `left`, and `bottom` CSS coordinates;
- validation messages and completeness counters.

Segment entries reference facts by deterministic key. They never duplicate or
rewrite fact text, so the original parsed evidence remains auditable.
Contact candidates likewise retain every distinct extraction method, raw value,
report member, XPath, source line, nearby text, and a keyword-based suggested
role. A candidate is not a claim that the value is the company's primary
contact. Scripts, styles, SVG content, inline XBRL headers/hidden content, and
invalid phone-shaped financial numbers are excluded without an LLM call.
Website evidence additionally records normalized URLs and hosts. Single-label
hosts, IP addresses, unknown suffixes, email-only domains, XBRL infrastructure,
static assets, and file-like `.zip` domains are rejected. External websites are
kept as candidates with role evidence; they are not assumed to be company-owned.

Each visible excerpt is capped at 6,000 characters and each section type at two
excerpts. It records the report member, page wrapper ID, printed page number when
detectable, anchor XPath and visual order, extraction method, language, original
and included character counts, truncation state, and SHA-256 of the included
text. These are report-period observations. In particular, an address printed
in an annual report is not promoted over current Bolagsverket-derived company
data.

## Parse one package locally

```bash
uv run python -m dagster_v3.defs.esef_filings.segment_cli \
  report-package.zip artifact.json \
  --fxo-id 549300GK4LGIDDWJWL07-2024-12-31-ESEF-SE-0 \
  --country SE \
  --expected-package-sha256 0b0f7d2be2a348b27bed441782394b55cec9084eec8b9190837bc6250347e0d1 \
  --reference-oim aak-reference.json
```

Omit `--reference-oim` for sources that do not publish xBRL-JSON. Use
`--skip-esef-validation` only for fast parser development; production artifacts
run the ESEF validation plugin.

When `--reference-oim` is supplied, the command emits a `fact_parity` report and
returns exit status 1 unless all cutover gates pass. The report compares:

- namespace-expanded concept, entity, unit, and dimension identities;
- canonical values, decimals, periods, and languages as a multiset, so duplicate
  facts remain visible;
- the current `esef_facts` storage contract after normalizing mathematically
  equivalent numeric spellings such as `32969000000` and `32969000000.0`;
- the IFRS metric-selection result produced by the current concept priority,
  current-period, dimension, decimals, and fact-ID tie-break rules.

Exact lexical row parity and source fact-ID parity are reported separately. They
are not cutover blockers when the stored values and metrics match: OIM exporters
may invent IDs for XHTML facts that have no source `id`, while the Arelle artifact
uses a deterministic report-member/fact-key fallback.

The report also exposes narrowly approved semantic corrections instead of hiding
them as normalization. The only approved rule is
`ifrs-average-number-of-employees-pure-unit`: Arelle may promote
`ifrs-full:AverageNumberOfEmployees` from legacy unitless text to a numeric
`xbrli:pure` fact when the value, decimals, entity, period, language, and
dimensions are otherwise identical. The raw mismatch and affected `employees`
metric remain visible in the report. A different employee value or any other
field change still fails the cutover gate.

## Materialize source documents and deterministic candidates

Launch `esef_filings_refresh_job` or `esef_filings_backfill_job` for a processed-week
partition such as `2025-03-30`. The graph snapshots the compact source manifest, parses
each unique SHA-256 package outside the DuckDB pool, and then publishes
`esef_facts`, `esef_document_contact_candidates`,
`esef_document_concept_labels`, and `esef_disclosures` through one
four-output ClickHouse publication operation. Concept-label rows contain only concepts used by
the document and retain the submitted-language and English taxonomy labels,
namespace URI, extension status, source document, and source-record identity.
Its run config accepts:

```yaml
ops:
  esef_document_extraction_manifest_s3:
    config:
      source_document_ids: ["549300GK4LGIDDWJWL07-2024-12-31-ESEF-SE-0"]
      max_documents: 5
  esef_document_artifacts_s3:
    config:
      refresh_existing: false
      validate_esef: true
      parse_workers: 4
```

`source_document_ids` and `max_documents` are optional development controls.
Their production defaults select every filing first processed in the week. A single week
can contain documents from several fiscal years; each document retains its own reporting
period and fiscal year. `parse_workers`
defaults to four and is bounded to eight. The document row retains `fxo_id` as
`source_document_id`, `(country_iso2, company_id)` from the GLEIF registry mapping,
the package URL/hash/object key, parser versions, quality counts, and the
parsed-artifact key. Existing `esef_facts.fxo_id` rows join to the same document
without reprocessing financial facts.

Raw packages use this content-addressed layout:

```text
esef_filings/report_packages/package_sha256=<hash>/report-package.zip
```

Artifacts use this content-addressed key layout:

```text
esef_filings/ixbrl_segments/schema=v5/parser=arelle-<version>/candidates=v3/package_sha256=<hash>/artifact.json
```

## Extract company enrichment with DeepSeek

Materialize `esef_document_company_information_clickhouse` after the four
parsing outputs. It selects each company's latest report from canonical
`esef_disclosures`, joins document-scoped concept labels, and reconstructs the
bounded evidence input directly from ClickHouse. It uses the existing `DEEPSEEK_URL`,
`DEEPSEEK_MODEL`, and `DEEPSEEK_API_KEY` environment variables. This paid asset is
unpartitioned and is never included in the routine refresh or backfill. It reads the
final ClickHouse disclosure state and selects the newest report for each resolved
`(country_iso2, company_id)`. Schema-v5 rows include both selected tagged facts and
visible sections. Migrated legacy disclosure rows remain queryable but do not become
model inputs until their processed-week partition is rebuilt from the existing parsed
artifact, because the legacy table did not retain semantic segment references.
For example,
this configuration processes the latest eligible report for selected Swedish companies:

```yaml
ops:
  esef_document_company_information_clickhouse:
    config:
      country_iso2: "SE"
      company_ids: ["5566692850", "5565200028"]
      source_document_ids: []
      max_documents: 5
      refresh_existing: false
      max_evidence_chars: 64000
      timeout_seconds: 180
```

With no filters, the asset processes the latest unprocessed report for every linked
company. `country_iso2`, `company_ids`, `source_document_ids`, and `max_documents` are
optional operational bounds. `company_ids` requires `country_iso2`, because company
identity is country-scoped. A `source_document_ids` filter is applied after latest-report
ranking, so it cannot accidentally select an older filing for a company. A document is
skipped when the current model and prompt already processed the same canonical request,
unless `refresh_existing` is true.

The model stage extracts these candidate fields:

- a concise English company description;
- explicitly named people with explicit roles, role category, current or
  historical status, and dates when the report states them;
- products and services;
- explicit customer groups or industries;
- operating geographies;
- named reporting or operating segments;
- at most ten explicitly named significant parent, subsidiary, associate, or
  joint-venture relationships.

It does not ask the model to rediscover emails, phone numbers, URLs, domains,
listing facts, or financial metrics that can be read deterministically.

The input is not the multi-megabyte fact artifact. It contains the selected
identity, business-profile, people-and-audit, products/markets/segments, and
group-structure text facts, plus schema-v5 visible board, management, committee,
auditor, signature, profile, and company-overview excerpts. The default filing
budget is 64,000 characters. Visible evidence is capped at 32,000 characters,
6,000 characters per excerpt, and two excerpts per section type; the remaining
budget is available to tagged facts, which retain their 32,000-character
per-fact cap. Inline XBRL text-block facts legitimately contain XHTML fragments;
`lxml` converts those fragments to visible text before the request. This is
markup decoding, not regex-based information extraction. Company-contact and
ownership/listing sections remain in the v5 artifact for report-period queries
but are not sent to the current output schema.

Every returned item must cite one or more `E0001`-style evidence IDs. The
application validates that each ID exists and belongs to a section allowed for
that field. For example, a person cannot cite company-description evidence, and
a group relationship cannot cite a product segment. Unknown or unrelated
citations fail the run. The artifact also stores the source fact keys, cleaned evidence,
prompt version, model, token usage, response ID, raw response and its SHA-256 hash.
Before calling DeepSeek, the exact canonical JSON request sent to the API is hashed and
written to S3. The same request object is then passed to the client, making the archived
input independently reproducible and verifiable.

Request and result artifacts use these content-addressed key layouts:

```text
esef_filings/llm_company_enrichment_requests/schema=v1/prompt=esef-company-enrichment-v2/model=<model>/request_sha256=<request-hash>/request.json
esef_filings/llm_company_enrichment/schema=v1/prompt=esef-company-enrichment-v2/model=<model>/package_sha256=<package-hash>/request_sha256=<request-hash>/artifact.json
```

These remain source-document observations. The four-output parsing publisher writes
the deterministic facts, contacts, labels, and disclosures. The separate direct
`esef_document_company_information_clickhouse` asset atomically writes the request
key/hash and exact raw response/hash for each XBRL source document.
`esef_document_observations_clickhouse`
normalizes descriptions, people, products, markets, geographies, segments, and group
relationships for UI joins. A separate resolver can later rank values
across sources; these assets never update canonical company descriptions,
contacts, people, or relationships.

## Real-report deterministic-candidate verification

The deterministic extractor was run against the five report packages listed in
`tests/fixtures/esef_ixbrl_segments/real_reports.json`. AAK produced two emails
and two Swedish phone numbers. Each Volvo language report produced two emails
and four Swedish phone numbers from its contact page. The Crédit Agricole report
produced three emails and four French phone numbers. Finnvera produced no
contact candidates. The reviewed normalized values are recorded in the fixture
catalog.

The same reports produced 2 reviewed website domains for AAK, 6 for each Volvo
language report, 1 for Finnvera, and 1 for Crédit Agricole. These include useful
external evidence such as auditor, governance, sustainability, and publishing
sites; the role metadata prevents them from being silently promoted as the
company's canonical domain.

This result is intentionally report-specific: a zero means no supported contact
evidence was present in that ESEF XHTML, not that the company has no contact
information.

The downstream LLM stage consumes only these artifacts. Email, phone, URL,
host, and eTLD+1 discovery stays in the deterministic stage.

## Arelle-to-OIM fact parity verification

On 2026-08-04 the cutover validator was run against every report in
`tests/fixtures/esef_ixbrl_segments/real_reports.json`:

| report | semantic facts | storage-equivalent rows | exact lexical rows | fact IDs identical | metrics |
|---|---:|---:|---:|:---:|:---:|
| AAK AB 2024 Swedish | 436 / 436 | 436 / 436 | 436 / 436 | yes | match |
| AB Volvo 2023 English | 1,078 / 1,078 | 1,078 / 1,078 | 158 / 1,078 | yes | match |
| AB Volvo 2023 Swedish | 1,080 / 1,080 | 1,080 / 1,080 | 160 / 1,080 | yes | match |
| Finnvera 2024 Finnish | 807 / 807 | 807 / 807 | 807 / 807 | yes | match |
| Crédit Agricole SA 2023 French | 1,181 / 1,181 | 1,181 / 1,181 | 22 / 1,181 | no | match |

In total, 4,582 of 4,582 facts matched semantically and at the storage-value
contract, with no metric differences. Volvo's exact-row difference is integral
decimal spelling (`...000` versus `...000.0`). Crédit Agricole additionally has
OIM-generated fact IDs for many XHTML facts without explicit IDs. Neither changes
concept identity, value, period, unit, dimensions, language, or the financial
metrics.

The production `esef_filing_facts_duckdb` asset consumes the schema-v5 Arelle
artifact while retaining its existing asset key and DuckDB/ClickHouse schemas. It
downloads every content-addressed artifact once per partition, projects one fact set
per source document, spools bounded Parquet batches outside DuckDB, and replaces only
the affected filing IDs. Ingestion checkpoints carry `arelle-artifact-v5`; an older
OIM checkpoint therefore triggers the required one-time rewrite instead of silently
skipping the filing. `esef_filing_facts_json_s3` remains registered but is not selected
by routine refresh or backfill jobs.
