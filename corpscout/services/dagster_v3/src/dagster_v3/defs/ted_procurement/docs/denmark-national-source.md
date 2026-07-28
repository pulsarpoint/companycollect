# Denmark procurement national-source follow-up

Status: design-ready follow-up with an access prerequisite. The current
delivery adds Denmark to the shared TED ingestion pipeline. It does not ingest
the national source or expose a Denmark company signal.

## Selected source: udbud.dk

Prioritize the official Danish public-procurement portal, udbud.dk:

- Portal: `https://udbud.dk`
- Production API: `https://api.udbud.dk/udbud`
- OpenAPI:
  `https://rep.erst.dk/git/udbud-dk/sdk/-/raw/master/openapi.yml`
- Incremental endpoint:
  `/ekstern-data/bekendtgoerelse/v1/fraKilde/{kilde}`
- National source selector: `DKUDBUD`

The API exposes notices by source with `page`, `size`, and `since` parameters.
Each response contains the notice identifier, `noticeVersion`, publication
number, registration timestamp, and base64-encoded eForms XML. The source
supports national tenders, awards, direct awards, contract modifications, and
planned procurement in addition to a separate TED branch.

Access is not anonymous. An approved external user with the
`MU_API_DATASYNK` role is required. Registration uses MitID Erhverv; the issued
Basic credentials obtain a client-credentials token from
`https://erst.virk.dk/auth/token?grant_type=client_credentials`, and API calls
use that token as Bearer authentication. Obtain and test those credentials
before implementing the source package.

The official terms permit reproduction with attribution to udbud.dk. Record
the exact terms and attribution text in source metadata when access is granted.

## Snapshot and publication

Create a `denmark_udbud_procurement` package after credentials are available:

1. Run one full `DKUDBUD` snapshot from the current platform boundary. The
   public archive separates notices from 1 March 2022 through 12 November 2024;
   measure the API's actual earliest record rather than assuming it imported
   that archive.
2. Store every response page and decoded XML document as immutable S3 objects.
   Key XML by `(noticeId, noticeVersion)` and retain the encoded payload,
   registration timestamp, source URL, checksum, retrieval time, and run ID.
3. After the full snapshot, poll incrementally from the last successful
   registration timestamp with an overlap window. The endpoint has a lower
   `since` bound but no upper bound, so a fixed monthly backfill would repeatedly
   scan future records.
4. Use the documented `total` count to detect changes during pagination. If it
   changes between pages, discard that attempt and restart from page one.
5. Load the response catalogue through a dlt resource with an explicit schema,
   parse the eForms XML into DuckDB, then publish with atomic ClickHouse
   replacement.

The ingest must use `kilde=DKUDBUD` and must not ingest the `TED` API branch.
TED is already loaded by `ted_procurement`; ingesting `TED` or `ALLE` here
would create a second copy of the same directive-level awards.

## Normalized grains and versions

Retain immutable version history and publish a canonical latest-version view:

- `dk_udbud_notice_versions`: one row per `(noticeId, noticeVersion)`, including
  notice category, source registration time, supersession links, raw object
  key, and payload hash.
- `dk_udbud_notices`: one row per current notice, with buyer CVR, title, legal
  basis, procedure and publication dates, CPV, URLs, and notice-grain values.
- `dk_udbud_notice_lots`: one row per (notice, lot), preserving estimates,
  framework ceilings, tender ranges, and awarded lot values separately.
- `dk_udbud_notice_winners`: one row per (notice, lot, winner), preserving the
  raw organization identifier, normalized CVR, tender and contract identifiers,
  award dates, and only values published at a winner-attributable grain.

Use the current TED eForms parser only after credentialed fixtures prove the
Danish national XML uses the same winner linkage. Add fixtures for a national
award, direct award, modification, cancellation/no-winner result, consortium,
and multi-winner framework before implementing the normalized tables.

Normalize a plain eight-digit CVR and the equivalent `DK`-prefixed VAT form.
Preserve consortium identifiers such as `79095311/ 26369126`, names published
as identifiers, foreign numbers, and malformed values without guessing a
single company.

Notice, lot, framework, tender-range, and winner amounts remain separate.
Company spend may sum only winner-attributable values. A direct-award notice is
an award category, not a second record for an already-linked contract; use
published procedure, notice, lot, tender, and contract identifiers for
deduplication.

## CVR ClickHouse spine prerequisite

The Denmark CVR pipeline currently ends at
`data/denmark_cvr_source.duckdb`, table `denmark_cvr.companies`, keyed by
eight-digit `cvr`. There is no `corpscout.dk_companies` table or ClickHouse
export asset. Before adding `dk_government_contracts`,
`dk_government_contract_summary`, or
`dk_government_contract_signals_job`, implement and validate that CVR ClickHouse
spine and include Denmark in `companies_all`.

Until then, Denmark TED rows are queryable in the shared TED tables but are not
joined into company government-contract fields. Do not add a signal rule that
declares a nonexistent ClickHouse company table.

## Evaluated alternatives

- The KFST EU-tender workbook is a quality-controlled, 57-variable dataset
  derived from Danish TED notices from 2017 onward. It is useful for validation
  and historical analysis, but it duplicates the selected TED source and does
  not close the national-coverage gap.
- The legacy udbud.dk archive contains below-threshold opportunities and
  purchasing plans shown between 1 March 2022 and 12 November 2024. Treat it as
  an opportunity-history follow-up, not as evidence of an identified winner
  unless record-level inspection proves an award grain.
