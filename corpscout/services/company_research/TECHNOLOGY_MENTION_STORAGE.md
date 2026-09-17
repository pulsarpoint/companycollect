# Complete research JSON output

Current direction, 17 September 2026: the crawler returns a complete JSON object.
Storage belongs to its caller. The earlier RustFS/S3 submission design is superseded;
the upload module, command, bucket option and storage dependency have been removed.

```text
Crawl4AI → page agents → collected page results
                              ↓
                 final technology classification
                              ↓
                      complete JSON object
```

See [the running guide](PAGE_RESEARCH.md). For one company, the saved-page command
emits `company-research-result/1.0` directly. For multiple companies it emits one
object with a `results` array. Progress goes to stderr, so stdout can be parsed
directly or redirected by the caller. Local diagnostic files are opt-in with
`--output`; otherwise temporary working files are removed after processing.

## What the JSON preserves

| Content | Required information |
|---|---|
| Run metadata | Schema/run/revision IDs, target URL, processing times, configuration, model usage and catalog version. |
| All objectives | Company description, services/products, jobs, people, contacts, locations, ownership and other relationships, credentials and document links. |
| Sources | Original page captures, text sections, headings, representations, locators, source URLs and hashes. |
| Technology mentions | Source spelling, original context and actor/job references, including known, unknown, ambiguous and excluded mentions. |
| Classifications | Dispositions, relationships, actor/scope, explanations, supporting section IDs and review status. |
| Navigation | Full observed links, source pages, surrounding context, scores and visit outcomes. |
| Coverage | Per-page/objective status, missing information, unresolved items, errors and stop reason. |

References to original text resolve within the returned object. A model summary
alone does not replace the source passage. Shared sections may be referenced by
multiple mentions. Do not include credentials or require storage receipts to
interpret the result.

## Partial results and revisions

Incomplete extraction/classification remains an explicit partial result. An empty
array on an unprocessed page does not establish company-wide absence. Keep raw
mentions and failed attempts available even when validation rejects a decision.

Reclassification preserves original captures and mentions and produces a new
revision with changed decisions. The caller decides whether and where to retain
the returned JSON. The crawler performs no object-store writes.

## Central technology lookup

The crawler reads the central technology catalog or a pinned local snapshot. Keep
catalog identity separate from whether a company uses the technology:

- Matched: include the existing canonical identity.
- Not found: retain the unmatched source name.
- Ambiguous or failed: retain that outcome without inventing an identity or absence.

The active crawler does not submit catalog proposals or company observations.
Descriptions and lookup results remain in the JSON. Downstream database storage,
normalization and review workflows remain separate work.
