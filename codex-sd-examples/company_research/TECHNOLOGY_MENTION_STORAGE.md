# Submit complete research JSON to RustFS/S3

Accepted direction, 16 September 2026. The crawler submits **all research results
as JSON to the existing RustFS S3 service**. Parsing those submissions and storing
them in ClickHouse is a later task. This supersedes the earlier proposal to build
four ClickHouse storage objects and a database ingestion endpoint now.

This document records the submission contract; an uploader has not been
implemented or run by this design update.

## Submission boundary

```text
Crawl4AI → page agents → collected page results
                              ↓
                 final technology classification
                              ↓
                  complete result.json → RustFS/S3

Later: read submitted JSON → normalize/validate → ClickHouse
```

The S3 JSON is the durable submitted record. Successful submission means the
object was uploaded successfully; it does not mean it was parsed, imported into
ClickHouse, or approved for the technology catalog.

Use one complete JSON result per run/revision in a configured existing bucket.
An illustrative object key is:

```text
company-research/{run_id}/{revision_id}/result.json
```

The bucket and connection settings come from the existing RustFS configuration;
this design does not create a bucket or choose new credentials.

## What the JSON contains

Preserve the whole research output, not a filtered technology-only submission:

| Content | Required information |
|---|---|
| Run metadata | Schema version, run/revision IDs, input/final site URL, capture/processing times, configuration and model/prompt versions. |
| All objective results | Company description and services/products, jobs, people, contacts, locations, ownership/other company relationships, credentials and financial/other document links. |
| Page results | Page provenance, source sections and their original text, headings, source representation, locators and snapshot hashes. |
| Technology mentions | All source names with their original text/context references, including known, unknown, ambiguous and subsequently excluded mentions. |
| Final classifications | Every available decision, relationship list, actor/scope, explanation and supporting mention/section IDs, plus revision and processing status. |
| Navigation | Full observed links, source pages, surrounding context, scores and visit outcomes, including external links that were not crawled. |
| Coverage and diagnostics | Per-page/objective completion, missing information, unresolved/review items, errors, stop reason, model calls and usage. |

The exact schema will follow the page-agent and final-classifier contract; the
table above defines what must not be lost. Reuse existing output fields where
they fit instead of remapping results into premature database columns.

Original section text must be present in the JSON, not only an LLM summary or a
reference to a future ClickHouse row. Several mentions may refer to one shared
section in that JSON. References must resolve within the submission or to an
explicitly identified immutable supporting artifact.

Large supporting files such as captured HTML and documents can remain separate
S3 objects. Preserve their original website URLs, durable bucket/key references
and hashes in the result. Do not include credentials or temporary signed links.

## Completeness and revisions

An interrupted crawl or incomplete classifier can still submit a partial result,
with its status and unprocessed work explicitly recorded. Do not interpret an
empty array on an unprocessed page as evidence that the company lacks information.

Upload the result when processing finishes or stops, and acknowledge success only
after the storage service confirms the write. Keep the local result available
for retry if uploading fails. Retry the same immutable payload/key; changed
classification or extraction output gets a new revision rather than overwriting
the evidence from an earlier submission. Return the object reference and a
checksum in the upload receipt.

Reclassification retains raw mentions and source sections, with new decisions
in the new result revision. Catalog matches/proposal suggestions may be included
when already available, but unresolved identity does not block submission.

## Existing proposal endpoint

`submitTechnologyProposals` currently stores only catalog proposals in `new_tech`
and counts known matches. It is not the submission path for this new complete
research output. Its limited persistence is therefore not a blocker for S3 JSON
submission. Keep the existing optional proposal workflow separate.

Defer ClickHouse tables/views, JSON-to-table mapping, importer scheduling,
database publication and automatic routing of proposals into administrator review
until the user resumes downstream ingestion work. The saved JSON must contain
enough original evidence and processing information to support those decisions
without crawling again.
