# Persistence of source mentions and later classifications

Accepted direction, 16 September 2026. Companion to
[the page-collection/classification contract](TECHNOLOGY_MENTION_DESIGN.md).
This is the database contract to implement with that workflow. **No migration,
database write, or ingestion change has been applied by this design update.**

## Current ingestion gap

The current backoffice `submitTechnologyProposals` implementation validates known
catalog matches and increments a counter, but only inserts proposed technologies
into `corpscout.new_tech`. It is not a storage endpoint for all research mentions,
accepted known technologies, or excluded technical context.

Retain `new_tech` and its administrator review/mapping tables for catalog proposals.
Add storage for the research evidence and classifications independently. Receiving
a mention must not depend on finding a catalog match or passing proposal review.

## Four storage objects

| Object | One logical row | Essential content |
|---|---|---|
| `company_research_sections` | One referenced section in an immutable page representation | Original/final page URLs, snapshot ID/hash, fetched time, native/rendered representation, heading path, locator, original extracted text, RustFS artifact reference. |
| `technology_mentions` | One collected occurrence of a source name | Mention ID, collection/extraction revision, source name, section references for the mention and its context, source-stated actor/job information, reference-validation status. |
| `technology_mention_classifications` | One decision for a mention in a particular publication revision | Eligibility, catalog resolution if available, all relationship interpretations, supporting references, explanation, semantic review state. |
| `technology_classification_revisions` | One bounded classification/publication revision of an explicit input collection | Input manifest and expected IDs/counts, model/prompt and catalog/resolver versions, processing status, completion/publication information, artifact references. |

Use the existing `technology_catalog.technology` string key for canonical matches.
Do not invent a new numeric technology key. Resolved company identity remains the
existing pair `(country_code, company_id)`, populated together or left absent.

### Source sections

Store the relevant text in ClickHouse so evidence can be displayed and passed to
the final agent without downloading and reparsing the whole site. Large HTML,
screenshots and full model exchanges remain in RustFS; store durable bucket/key
references and hashes, not temporary signed download URLs. Retain the original
website URL separately from the stored artifact reference.

Several mentions can reference the same section, such as a list of job
requirements. Store that section once per immutable snapshot/representation and
reference it from all mentions. This preserves shared context without duplicating
the paragraph for every technology. Keep table headers, heading paths and explicit
continuations for long sections. Record the text extraction/whitespace method.

Capture time is provenance. It must not become a technology-adoption date or the
date of a partnership claim.

### Raw mentions

`source_name` remains the source spelling, including an unresolved abbreviation.
Use separate `source_section_ids` and `context_section_ids`: the first locate the
name, and the second retain actor identity, job title, table headers or surrounding
statements needed to interpret it. References identify the source representation
as well as the page, so rendered-link evidence is not checked against native HTML
by mistake.

Any extracted actor/name/context description is provisional and references its
source. Keep the research target distinct from the source-described actor. A job
on an external platform can refer to a company whose own website has another
domain; a consulting page can describe a client instead of the provider.

Raw mentions do not require a canonical technology or a company registry match.
Do not force a `technology` foreign-key value before classification. They also do
not carry a final `used_by_company` flag. Their validation status concerns source
references, not acceptance of a usage claim.

### Classification decisions

Every mention receives a decision in each complete classification revision,
including technical-context, other-objective and needs-review outcomes. Store:

- `eligibility` and its reason;
- nullable canonical `technology`, nullable `proposal_id`, and catalog resolution
  status/reference/version, independently from eligibility;
- a `relationships` array of typed objects;
- model-generated explanation, supporting mention/section IDs, and review status;
- classifier execution and publication revision identifiers.

Each relationship object contains the relationship type, original subject name,
resolved company key when available, scope, relevant team/job/product/client or
other company, source-stated time, alternatives and its evidence references.
Different relationships can have different supporting sections.

Use named tuples or an equivalent typed representation for the relationship
array; avoid parallel arrays of types/actors/evidence that can become misaligned.
One row holds the complete decision and relationship list for that mention and
revision. Reclassification can therefore remove a formerly asserted relationship
without leaving an older flattened relationship row active.

A vendor partnership is a corporate relationship. Store the counterparty and
source support; connect a specific technology only if that connection is stated.
It is not evidence of every product from that vendor being used internally.

Distinguish company operations, team, job role, client project and offered product
context. Preserve requirements, preferences, plans, expertise, development,
offerings, compatibility, components, history and explicit negative claims. One
mention may support several of these. No proven use means unknown, not false.

If the classifier needs support from another collected mention, it must identify
that mention and its sections explicitly. Do not attach a company-wide summary as
undifferentiated evidence. References must resolve within the declared collection
or its explicitly included, versioned source set.

### Revisions and publication

Keep source mentions immutable and classifications versioned. Separate the model
execution ID from the publication revision: a later catalog mapping can republish
the same interpretation without calling the model again or changing the evidence.
Record both classifier/prompt and catalog/company-resolver versions.

The revision's input manifest defines the exact mention set. Classification may
run in batches, but a complete revision accounts for every input mention with a
valid decision or explicit needs-review outcome. Missing, malformed or failed
decisions are not equivalent to excluded mentions.

Persist all decisions and validate completeness before publishing a revision.
A failed/partial reclassification must not replace a prior complete publication.
Keep partial results inspectable and show pending coverage. Do not select “latest
row per mention” across arbitrary executions: that could mix different prompts
and leave superseded claims active.

ClickHouse does not enforce cross-table foreign keys or transactional writes
across these objects. The publisher validates references, company-key pairs,
catalog/proposal identities, complete mention coverage and ID uniqueness. Query
the selected complete revision, or publish a validated serving snapshot through
the repository's stage/exchange pattern. Completion metadata alone must not be
treated as proof that asynchronously replicated dependent rows are visible.

Retries reuse host-generated IDs and identical payloads. A changed raw payload
requires a new extraction revision; do not silently overwrite evidence under the
same identity. Sorting keys alone do not establish uniqueness. Implement retry
deduplication and conflicting-payload rejection at ingestion/publication, with
consistent reads before exposing results.

## Analytical projections

Expose `technology_observations` by flattening relationships from the selected
classification revisions and joining their mentions/sections. This becomes an
analytical projection of preserved evidence and decisions, rather than the only
copy of an already interpreted extraction. It can be a view initially or a
validated serving table when scale justifies it.

For example:

```text
source section: "Example Labs — Backend Engineer. Python experience required."
    → mention: Python + source/context references
    → classification: specific_technology
        relationship: required_experience
        subject: Example Labs
        scope: role / Backend Engineer
    → observation: canonical Python + those references and qualifications
```

An application can then offer distinct filters for confirmed company use, hiring
requirements, expertise, client implementations and vendor relationships. A
generic company–technology association must not imply all of these are use.

The existing domain-detection path remains separate:

```text
company → associated domains → existing web/DNS technology detections
company → classified mentions → source-supported technology relationships
```

Combine them in the planned company-technology evidence/summary projections with
their provenance and relationship type intact. Do not move raw detector history
into mention rows. A source domain is not automatically an applicable company
domain, and a detector hit does not establish company-wide internal deployment.

Count distinct source/job identities according to the metric. Repeated mentions,
overlap windows, retries, revisions and repeated footer sections must not inflate
adoption or hiring counts. Preserve historical observations; a removed ad or a
new classification omission does not establish that a technology is no longer used.

## Proposal approval

Only eligible specific technologies that are absent from the catalog enter the
existing `new_tech` review flow with its category/description requirements. Keep
the raw mention and decision regardless of proposal outcome.

On approval or mapping to an existing entry, resolve `proposal_id` through the
existing reviewed mapping and catalog publication flow. Republish the analytical
projection with the canonical key. Do not delete/rewrite the source evidence or
convert a job requirement/partnership into a usage claim during catalog approval.
Catalog identity approval and semantic relationship review are independent.

## Implementation checks

Freeze the page/final-agent payload contract before implementing migrations and
ingestion together. Test these behaviors at the database boundary:

1. Both known and unknown names persist, including technical-context/review items.
2. Two mentions can share a section without losing provenance or duplicating text.
3. Retry is idempotent; conflicting content cannot overwrite a raw mention.
4. A complete new revision replaces its predecessor's relationship set, including
   removals; a partial revision cannot replace it.
5. Different actors, job roles, client projects, source times and relationship
   types survive flattening and summary generation.
6. Proposal mapping updates the canonical identity without changing source text
   or the meaning of the relationship.
7. Unknown company/technology identities and unresolved evidence remain visible
   without appearing as confirmed company usage.

No existing proposal endpoint should silently accept this richer payload while
discarding its mentions/decisions. Introduce a versioned research-ingestion
contract and keep proposal submission as a distinct downstream operation.
