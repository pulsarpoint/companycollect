# NOVELIC scan — 17 September 2026

The scan recovered all 16 checked jobs and 16 management names, plus 31 of 32
selected technologies. It remains unsuitable for unattended company-technology
ingestion: four platform-cookie usage claims and three format classifications are
wrong, while quotation/reference checks hold many valid facts. Guided analysis cost
an estimated $2.81 and took almost 55 minutes; total reported API spend including
the interrupted autonomous diagnostic is approximately $3.12.

After this evaluation, v0.18.0 removed RustFS/S3 saving at the user's request.
The page runner now returns the complete JSON object, with optional local
diagnostics. Uploads and receipts described below are historical evidence from
the evaluation, not current crawler behavior. All remote crawler objects were
subsequently deleted on September 17 at the user's request; local results and
archives remain. See [the verified deletion receipt](RUSTFS_CLEANUP_RECEIPT.json).

## Scope and interpretation

This evaluation uses direct DeepSeek `deepseek-flash`, high reasoning, and fresh
Crawl4AI native cleaned HTML. It measures the current implementation rather than
silently correcting its outputs. All original captures, requests, responses,
retries and code snapshots are retained locally.

Two separate runs were necessary:

1. **Interrupted autonomous diagnostic.** The existing URL crawler passed the
   homepage company gate and discovered 143 sitemap URLs, but used 32 model calls
   after fetching only the homepage and mechanical-engineering page. It was
   explicitly interrupted because repeated navigation extraction, review and link
   scoring dominated progress. The raw checkpoint is unchanged; the adjacent
   `interruption.json` records why it stopped. This is not a completed crawl.
2. **Guided 26-page evaluation.** The newer mention-first page unit processed those
   two captures plus 24 newly fetched pages: about, management, careers, contact,
   quality, embedded engineering, semiconductor engineering, the India expansion
   announcement, and all 16 job-description URLs found on the Careers page.
   Selection was guided by observed links. It does **not** establish autonomous
   discovery or whole-site completeness. The saved-page runner correctly reports
   `crawl_status: saved_pages_only` and `site_coverage: not_established`.

The first shared-browser capture attempt failed after three new pages; recovery
used independent browser instances and retained the original failure. All 26
selected inputs were eventually captured successfully. PDFs were discovered but
not downloaded or parsed. Technology catalog lookups use the pinned September 16
snapshot; there were no catalog, proposal or company-database writes.

## Usage

| Run | Calls | Input tokens | Output tokens | Estimated API cost |
|---|---:|---:|---:|---:|
| Guided 26-page run | 108 | 4,729,866 | 1,224,883 | $2.81205 |
| Interrupted autonomous diagnostic | 32 | 372,872 | 192,111 | $0.30838 |
| Combined reported usage | 140 | 5,102,738 | 1,416,994 | $3.12043 |

The guided analysis took **54 minutes 56 seconds**,
including final classification and upload verification, excluding page capture.
The autonomous diagnostic ran for 17 minutes before interruption. Guided usage
includes **897,288 reasoning tokens**, 261,120
cached input tokens and 4,468,746 uncached input tokens.
Only 5.5% of input tokens were cache hits.
All 108 guided requests returned usage; none ended because of the output-token
limit. The single invalid-JSON collection response is charged and included.

| Guided stage | Calls | Input tokens | Output tokens | Estimated cost |
|---|---:|---:|---:|---:|
| collection initial | 26 | 2,080,932 | 534,477 | $1.22456 |
| collection retry | 8 | 876,332 | 195,223 | $0.48934 |
| classification | 74 | 1,772,602 | 495,183 | $1.09815 |

Reasoning tokens are already included in output tokens. Price estimates use the
[official DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/)
captured on September 17: peak rates of $0.006 per million cached input tokens,
$0.30 per million uncached input tokens and $1.20 per million output tokens.
These are estimates from returned usage, not invoice totals. The autonomous run's
timeout and interrupted request have unknown usage and are excluded from its
estimate. A reported provider-cost field of zero must not be interpreted as free.

## Quality against the captured sources

The controls below were prepared from the fresh source pages and were not sent to
the model. They are selected checks, not an exhaustive precision/recall benchmark.
Matching source text proves that text is present; it is not independent verification
of the company's claims or their current validity.

| Check | Result | Interpretation |
|---|---|---|
| Job detail recovery | 16/16 | All captured job titles recovered; 12/16 detail records source-matched, with listing evidence for all 16 retained separately |
| Management recovery | 16/16 | Names/roles correct on source review; all 16 held by actor-quote validation |
| Selected technology recovery | 31/32 | Source-linked mentions; Autodesk Robot disappeared in a whole-page retry |
| Frozen exact relationship/scope controls | 27/32 | Four additional tools use a supported narrower team scope |
| After explicit source review of scope variants | 31/32 | Creo Parametric, OpenFOAM, MathCAD and Windchill team-expertise interpretations are accepted; original controls unchanged |
| Cookie attribution negative checks | 24/28 avoid the unsupported claim | Four vendor occurrences incorrectly assert NOVELIC company use |
| Selected generic/format mentions | 3 of 31 wrongly eligible | Protobuf, FlatBuffers and one MCAP occurrence; this is not overall precision |
| Job credentials | 16/16 pages avoid company-certification claims | Candidate standards/qualifications remain distinct |
| Source/capture integrity | No mismatches | All 52 native/rendered captures and 8,497 indexed text sections match their saved hashes |

The final JSON is explicitly **partial**. It contains 407 raw technology mentions:
381 source-linked and 26 held at collection. The final classifier returns 364
classified and 43 review-needed outcomes. Thirteen mentions exceed the application
context limit; 25 decisions are rejected for omitting a primary mention-section
reference. These causes overlap with collection holds and must not be added as
independent counts. A `classified` status is structural validity, not a guarantee
of correct company attribution: the seven semantic failures above still passed it.

There are 153 `specific_technology` decisions, including duplicates and neutral
platform mentions; this is **not 153 technologies used by NOVELIC**. Catalog lookup
across all 407 raw mentions returns 45 matched, 267 ambiguous and 95 not found.
These counts also include generic/ineligible mentions. Fuzzy candidates remain
ambiguous rather than being promoted to canonical matches.

All 1,245 observed link occurrences have assessments, representing 158 distinct
URLs. Ranking usefulness was not independently scored and the autonomous queue did
not consume these assessments in this run.

Useful findings include:

- **Company activities:** radar/perception products, embedded hardware and software
  engineering, mechanical engineering, and semiconductor design. Products and
  services are kept separate from technology mentions.
- **Jobs:** all 16 detail-page titles were recovered. The Careers page also produced
  a separate general internship record. Listing/detail duplicates must not be
  counted as additional vacancies. Five detail results use an observed application
  URL; this is allowed by the current prompt, but a final job entity needs separate
  source, description and application URLs. Title variants also need reconciliation.
- **People:** all 16 management names and roles agree with the management page,
  including Darko Tasovac, Veljko Mihajlović and the functional management team.
- **Ownership:** the India announcement and several jobs explicitly identify
  NOVELIC as part of Sona Comstar Group since 2023. No ownership percentage was
  established by these captures.
- **Offices and contacts:** eight location records on Contact cover four Serbian
  offices plus Munich, Skopje, Bucharest and Chennai. Local entities and identifiers
  remain separate, including NOVELIC India Private Ltd. Telephone, email, contact
  form and social-link evidence is retained.
- **Credentials/documents:** ISO 9001:2015 and ISO 14001:2015 website claims and PDF
  links were retained; IATF 16949:2016 is correctly described as prerequisites being
  pursued, not achieved certification. The PDFs have not been examined. A 60 GHz
  radar-chip case-study PDF was also found. No financial-report result was returned
  from this selected set; this does not establish that none exists on the site.

## Problems exposed by this run

### Recruitment-platform vendors leak into company usage

The Production Test Technician page's OneAssessment cookie inventory produced
`stated_use`, actor `Novelic`, scope `company`, for Microsoft Clarity, Google
Analytics, Google AdSense and Facebook Pixel. The same vendors on the Analog IC
Layout Engineer page were correctly left as unattributed `mentioned` observations.
The company-usage claims are unsupported transfers from the recruitment platform.
Matching the vendor name and source reference does not validate that attribution.
This is a blocker for automatically consuming the result as a company technology
inventory; the raw mentions should remain available with explicit review flags.

### Some format mentions pass the technology filter

The Data Engineer page explicitly discusses data serialization formats. The final
classifier nevertheless labels Protobuf, FlatBuffers and one MCAP occurrence as
`specific_technology`. A different MCAP occurrence on the same page is correctly
`technical_context`. Required/preferred-experience attribution does not make a
format eligible for the technology catalog. These are semantic false positives,
even when source-reference validation succeeds. Selected technology recovery must
not be presented as overall accuracy or precision.

### Evidence validation holds valid facts

All 16 correct management records are `needs_review` because their quoted fragments
omit the company name. The same issue affects contacts and many offerings. Across
the nontechnology objectives, 220 of 680 source records are marked `source_matched`;
the other 460 remain visible for review. These are source-record counts, including
repeated navigation and multiple pages describing the same entity.

Four of the 16 job-detail records also omit employer evidence, although the jobs
were correctly identified. The Careers page retains source-matched listing records
for all 16 URLs. ISO 14001 is retained with a review flag rather than silently lost.
Evidence repair should cite verified actor/container context through stable source
IDs; it should not relax attribution requirements or assume every name on a page
belongs to NOVELIC.

### Whole-page retries can remove correct information

Eight pages required a second collection request. One contact response contained
valid JSON followed by an extra closing brace; the other retries addressed source
reference errors. The unchanged-prompt retry recovered a parseable contact result.
Four pages still have mention-reference issues after their second attempt.
All 26 held mention names can be found elsewhere in their own page's section
inventory. That supports investigating incorrect references, but a substring
match alone is not sufficient to repair actor/context attribution automatically.

On Mechanical Engineering, the first response included Autodesk Robot, Inventor,
Mechanical Desktop, AutoPLANT and other tools that disappeared from the replacement
response. Autodesk Robot is the missing item in the selected 32-technology controls.
Both attempts remain in the audit trail, but the final result uses the last response.
A repair should preserve valid records and fix only identified failures. Do not
interpret regeneration as a guaranteed improvement.

### Context and repeated navigation dominate tokens

The old crawler's first homepage and mechanical-page chunks have 99.78% identical
visible text, yet undergo separate extraction and review. The newer collector also
sends a large representation: native HTML, contextual link inventories, and source
sections from both native and rendered HTML. For About, these components occupy
approximately 65.7k, 87.2k and 223.8k serialized characters respectively, before the
rest of the instructions/schema.

Across the 26 pages, 8,497 sections contain 520,376 text characters, but there are
only 1,039 distinct exact texts totaling 82,342 characters. This is a duplication
diagnostic, not permission to discard repeated facts or actor context. Keep complete
captures and provenance in storage; use compact IDs and relevant original text in
model input, with an explicit map between equivalent native/rendered sections.

The classifier's page context includes many repeated offering passages. Its
64,000-character batch budget produces 87 batches, including 44 singleton batches.
Thirteen individual embedded-page mentions exceed that budget even alone and are
retained for review without a classification call. This is an application context
construction problem, not a model output-token limit.

One 20-mention classification request took 137.5 seconds and returned 33,016 output
tokens, including reasoning. Output verbosity and sequential execution both matter
for latency. Smaller source payloads should be evaluated alongside concise decision
descriptions; a high-reasoning setting does not remove the need for semantic guards.

### Aggregation and discovery remain unfinished

The result concatenates per-page records rather than resolving a final company
entity. It preserves a real source disagreement: most pages say founded in 2013,
while the India announcement says 2012. A downstream company summary must surface
or resolve that disagreement, not select whichever record happens to come first.

The model returned 406 product/service records, largely repeated navigation items,
and 33 job records across listing/detail pages. These are not 406 unique offerings
or 33 vacancies. Final entity merging, relationship direction normalization, source
conflicts and job URL reconciliation need a separate deterministic/reviewed step.

The newer page unit has still not been connected to the autonomous frontier. This
guided run demonstrates extraction behavior on selected pages, not improved URL
selection. No low/high reasoning A/B comparison was performed here.

## Recommended next iteration

1. Enforce the company/platform attribution and format-eligibility boundaries, with
   the cookie-vendor and data-format failures saved as negative regression cases.
2. Preserve successful records and repair only failed evidence references. Add the
   management actor omission and mechanical retry loss as regression fixtures.
3. Compact page inputs and classifier context: avoid repeated native/rendered text,
   navigation offerings and verbose per-section metadata while retaining original
   context and all captures in the output. Independent classification batches can
   use bounded concurrency; the current sequential loop adds substantial latency.
4. Replay these same frozen pages before making another live scan. Compare the
   selected fact checks, relationship checks, call count, tokens and review holds.
5. Once those checks pass, connect the page unit to the visited/unvisited priority
   queue, then measure an autonomous NOVELIC run separately from this guided one.

## Artifacts and verification

- [Complete raw result](data/novelic-guided-20260917/analysis/company-b1dbd6b65ef6/result.json)
- [Machine-readable quality audit](data/novelic-guided-20260917/quality-audit.json), including affected mention IDs and the four explicit scope reviews
- [Guided input controls](data/novelic-guided-20260917/quality-controls.json) and [mechanical controls](data/novelic-direct-high-20260917/quality-controls.json)
- [Interrupted diagnostic and usage audit](data/novelic-direct-high-20260917/crawl/usage-audit.json); see its [interruption record](data/novelic-direct-high-20260917/interruption.json)
- [Archive and upload receipts](NOVELIC_SCAN_RECEIPT.json)

The complete raw result (**23,303,067 bytes**) and separate quality audit
were uploaded to the existing RustFS `crawls` bucket. Both uploads passed byte-for-byte
SHA-256 read-back verification. The raw model result was not rewritten to hide
failures or manually remove erroneous claims. These remote copies have since been
deleted; the following locations are historical identifiers only.

```text
s3://crawls/company-research/9aafb182096f451b8f184b217f28dd1d/00af6a2ba39247ee9ab8c22588b5b7ba/result.json
s3://crawls/company-research/9aafb182096f451b8f184b217f28dd1d/00af6a2ba39247ee9ab8c22588b5b7ba/quality-audit.json
```

The local run directories also contain capture manifests, all API calls, retries,
source-code snapshots, audit scripts and failure diagnostics. A verified archive
preserves both runs and the report; its location and checksum are in the receipt.
API credentials are not included.

The only tracked code changes for this evaluation expose reasoning effort and
external-page limits in the direct benchmark runner and add a read-only legacy
usage audit. Production extraction prompts and validation behavior were unchanged
during the run. Ruff lint/format and type checks passed for both benchmark files.
