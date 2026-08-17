# Address resolution design

## Purpose

Address resolution is a shared data capability, not a geocoder-specific string
join. Country pipelines retain ownership of their source observations and
reference data, while one versioned engine owns search-document construction,
candidate evidence, scoring, ambiguity handling, and result explanations.

Sweden is the reference implementation. New countries must emit the same raw,
parsed, and reference search-document contracts instead of copying Swedish SQL
or adding country-specific fuzzy joins.

## Data flow

```text
source observations
    -> parsed address observations
    -> canonical/shared addresses
    -> query search documents

reference source records
    -> parsed reference addresses and contextual street records
    -> reference search documents

query + reference search documents
    -> candidate evidence
    -> ranked resolution results
    -> evaluation and serving publication
```

Candidate generation and resolution are intentionally separate. Index changes
must not require reparsing source observations, and scoring changes must not
require rebuilding either source snapshot.

## Phase 1: raw observations

Raw observations preserve the source contract exactly:

- source-provided address text and fields;
- address type and country hint;
- owning entity and source-record identifiers;
- source snapshot, retrieval time, and immutable object provenance; and
- a stable observation identifier.

Raw values are never silently corrected. A display address and search text are
derived fields and must not replace the source text.

## Phase 2: parsed observations

The production parser is libpostal until another parser is proven better on a
labeled corpus. Each parsed observation records:

- street name;
- house number and suffix;
- unit or apartment;
- postcode;
- locality, district, region, and country;
- PO box or property/cadastral identifier when present;
- parser name, model version, country hint, and parse run; and
- normalized component values used by the search index.

Country post-processing may classify syntax that a general parser cannot infer,
such as Swedish cadastral identifiers. It must preserve the parser output and
record the post-processing rule instead of rewriting the raw observation.

Canonicalization groups parsed evidence only after raw and parsed observations
have stable identities. Company-to-address links retain all member evidence.

## Phase 3: search documents and indexes

Query and reference records use one search-document schema. A document contains
both raw/search representations and parsed components:

- normalized source text and normalized derived search text;
- normalized street, house number, unit, postcode, and locality;
- language-scoped libpostal street variants with their original value and
  variant provenance;
- raw and street tokens;
- character trigrams for partial retrieval;
- one-character deletion signatures for edit-distance candidate retrieval;
- address kind and maximum reference precision;
- reference coordinates, coordinate spread, and supporting-record count; and
- source/provider provenance.

Normalization uses Unicode NFC, lowercase, accent folding, whitespace
normalization, and punctuation removal. Original values remain alongside every
normalized value.

Indexes retrieve candidates; they do not decide matches. The initial candidate
strategies are:

1. exact normalized source or derived search text;
2. exact street, house number, and postcode;
3. exact street and house number with agreeing locality but conflicting or
   missing postcode;
4. exact street and house number within the country when stronger address
   context disagrees, with ambiguity enforced across distinct targets;
5. fuzzy street with exact house number and strong postcode/locality context;
6. exact street and postcode/locality without a query house number; and
7. exact street context when the requested building is absent from the
   reference source;
8. exact street and locality with a conflicting non-empty postcode, but only
   when the query has no candidate from any stronger retrieval strategy.

Street variants are generated from the parsed street component during index
materialization, not written back to raw or parsed observations. The parsed
street is always the rank-zero variant. Libpostal expansions are additional
retrieval keys and remain corrections in the result contract. They may match a
building only with an exact house number plus an agreeing postcode, or an
agreeing locality when either postcode is absent. Conflicting non-empty
postcodes and localities cannot use an expanded-street fallback.

Fuzzy candidates are retrieved through normalized-street and one-character
deletion-signature postings. Posting joins are blocked by country, exact
normalized house number, and postcode or locality before edit distance is
evaluated. Locality is only a postcode fallback when either side has no
postcode; two conflicting non-empty postcodes never become fuzzy candidates.
A fuzzy function is a feature, never an authority by itself.
Exact raw and derived-text paths are separate equality joins so databases can
use their normalized keys instead of evaluating a country-wide `OR` join.
Street/postcode retrieval and missing-postcode locality retrieval are likewise
separate keyed paths; locality never creates a same-street cross product across
conflicting non-empty postcodes.
When both paths produce candidates, exact-postcode street evidence has a
decisive score margin over locality-only evidence. The outcome remains street
precision; stronger context only resolves the textual tie.

The postcode-conflict fallback groups all OSM street references for the same
country, normalized street, and normalized locality into one logical target.
It returns `matched_street` only when the evidence spans at most the policy's
area threshold; geographically separated evidence remains `ambiguous`.
Building-only address points do not synthesize a street target because sparse
building coverage is not road geometry. Promotion rejects a postcode-conflict
fallback that would override a result produced by another strategy, while
allowing unmatched results and existing postcode-conflict fallbacks to refresh.

## Resolution contract

Every result separates three concepts:

- **resolution status**: exact, corrected, site, area, street, ambiguous,
  unmatched, invalid, foreign, postal box, or property identifier;
- **geographic precision**: building, site, area, street, or locality; and
- **confidence**: a policy-versioned score derived from explicit evidence.

Hard precision rules take precedence over the score:

- a missing query house number cannot resolve more precisely than street;
- a requested house absent from the reference source cannot be presented as a
  building;
- a postcode mismatch may still resolve to a building only when street, house,
  locality, and candidate uniqueness support it;
- conflicting postcode and locality may fall back to a country-level exact
  street/house key only when the resulting reference target is unique;
- a spelling correction must remain visible and must not overwrite source
  text; and
- tied textual candidates are ambiguous even when their coordinates are close.

The result records the selected strategy, matched reference components,
component agreements, corrections, candidate count, runner-up score margin,
coordinates, source URLs, parser version, index version, and scoring-policy
version. Commonly queried evidence is stored in explicit columns rather than an
opaque JSON score blob.

## Shared engine and country responsibilities

The shared engine owns:

- search-document normalization and derived index terms;
- candidate strategy execution;
- component-agreement features;
- score calculation and ranking;
- site/area spread classification;
- ambiguity and runner-up rules; and
- evaluation metrics.

A country adapter owns only real country/source differences:

- mapping raw registry fields into the observation contract;
- parser country hint and documented post-processing rules;
- postcode validation and locality aliases;
- mapping reference providers into reference search documents; and
- country policy values that have been justified by evaluation.

There is one concrete engine. Parser, scorer, and provider interfaces are not
introduced until multiple production implementations exist.

## Evaluation corpus and tuning

Each country has a versioned golden corpus containing real reviewed cases and
controlled perturbations of known matches. The Sweden v2 corpus covers:

- exact structured and raw matches;
- apartment details that do not change building precision;
- one-character insertion, deletion, substitution, and transposition errors;
- missing and conflicting postcodes;
- missing and unavailable house numbers;
- rural and cadastral addresses;
- duplicated/site candidates; and
- geographically separated ambiguous candidates.

Evaluation reports at least:

- candidate recall;
- top-result accuracy by resolution status;
- building false-positive rate;
- corrected-match precision;
- ambiguous and unmatched rates; and
- confidence calibration by score band.

Synthetic cases cannot replace reviewed production examples. Threshold changes
require a new policy version and a corpus comparison against the previous
version.

## Sweden shadow rollout

The Sweden shadow asset reads the current shared-address table and the current
Geofabrik reference snapshot, writes only shadow search/candidate/result tables
inside the Sweden DuckDB file, and compares its results with the current OSM
geocode table. It does not publish to ClickHouse and cannot change serving
mappings.

The Sweden reference adapter expands comma- or semicolon-separated OSM house
number tags into auditable component documents and derives postcode context for
road-only street references from nearby address-point centroids. Those are
provider mappings; scoring and ambiguity remain in the shared engine.

Promotion requires:

1. all golden cases passing;
2. reviewed samples of every new corrected-building strategy;
3. no unexplained regression in existing exact matches;
4. explicit UI treatment for corrections and approximate precision; and
5. versioned ClickHouse migrations and a separate production cutover run.
