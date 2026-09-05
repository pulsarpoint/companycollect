# Liquid repeatability and failure patterns

The second whole-page run used the same 40 Markdown snapshots and identical model
settings, instructions, and schema. The comparison verified matching input hashes
for every page. No pages were recrawled, and the original results remain unchanged.
The first run is `jobs-v2`; the repeat is `jobs-v2-repeat`.

**The outputs do not reliably repeat.** Only one of the 17 pages completed in both
runs had identical full records, even after ignoring record order. Five of those
17 pages had identical values for the six extracted fields when evidence quotations
were excluded.

## What the errors mean

| Outcome | First run | Repeat |
|---|---:|---:|
| Complete, schema-valid output | 20 | 22 |
| Output token limit reached | 18 | 18 |
| Provider 504 error | 2 | 0 |
| Returned records from completed pages | 214 | 217 |

The token-limit errors are page-level failures, not statements that a particular
job cannot be extracted. In the first run, 12 ended with incomplete JSON. Six
returned no answer text: their reported reasoning consumed all 8,192 output tokens.
None of those 18 contained a complete schema-valid result that the runner discarded.

The two provider errors were MotherDuck and PlanetScale. Both completed on repeat,
supporting the interpretation that these were transient service failures. Their
error code arrived in a JSON body, so the existing HTTP-status retry policy did not
retry them within the first pass.

## Is it a specific kind of job?

There is no clear evidence of a profession-specific failure. Requests contain whole
boards, often mixing engineering, sales, design, and support openings. Engineering
roles appear on both completed and failed boards. This corpus does not independently
vary job type while controlling page size, so it cannot establish a causal effect
of job type.

Page size is a much clearer association:

| Observed job links per page | Pages | First-run output-limit failures | Repeat output-limit failures |
|---|---:|---:|---:|
| 2–9 | 12 | 0 | 0 |
| 10–19 | 13 | 6 | 6 |
| 20+ | 15 | 12 | 12 |

First-run output-limit failures had a median of 25 observed job links and 4,786
Markdown characters. Completed pages had medians of 8.5 links and 2,108.5 characters.
The repeat showed almost the same size pattern. Observed links include general
applications, so these are workload-size indicators rather than verified job counts.

Text per job also matters. SearchAPI has only 12 links but 16,615 Markdown characters:
each opening repeats a long list of eligible countries. Both runs hit the output
limit. This is a reason to cap text size as well as the number of listings per window.

There are exceptions and unstable outcomes: Encord, with 35 observed links, completed
first and failed on repeat. Algolia, with 30 links, failed first and completed on
repeat. A fixed listing-count cutoff alone is therefore insufficient.

## What changed between runs?

- **17 pages completed both times.**
- **15 pages hit the output limit both times.**
- **Three recovered from the output limit:** Algolia, Dust, and Tavus.
- **Three changed from complete to output-limited:** Warp, Firecrawl, and Encord.
- **Both provider errors recovered:** MotherDuck and PlanetScale.

Among the 17 completed pairs, the first run returned 141 records and the second
returned 131. There were 129 matched openings; only 82 (63.6%) agreed on all six
normalized fields. Evidence text agreed on nine. Case and whitespace normalization
are used for these per-field counts; exact page equality uses the original values.

Concrete examples:

- Apify changed from **12 records to eight**, despite the identical saved page.
- RunPod changed from **23 records to 15**.
- Resend changed from **nine records to 11**.
- Calendly returned ten records both times, but only five matched on all six fields.
- Steel was the only page with identical complete records. Matching a previous
  answer does not establish correctness; both answers can preserve the same mistake.

Temperature was zero, but this experiment still observed different outputs. The
model ID and client inputs were fixed; provider implementation details and backend
versions were not pinned or recorded, so this test cannot attribute variability to
a specific serving component.

## Separate segmented experiment

The next experiment preserves the original corpus and divides it into overlapping
windows of complete Markdown listings. It caps windows at four distinct job links
and 3,000 characters, repeats preceding section context, and overlaps one complete
listing where the size budget allows. The 40 pages produce 228 windows. Original
lines and all Markdown job links remain represented; concatenated Markdown headings
receive separating whitespace.

Each window is extracted independently. The merge deduplicates by job URL, retains
distinct openings with the same title, and records disagreements between overlapping
windows. Any provisional selection between conflicting field sets is retained with
its alternatives for review. This is an experiment in handling input/output budgets,
not evidence that overlap makes model answers correct.

## Artifacts

- [Whole-page repeat comparison](data/runs/jobs-v2-repeat/repeat-comparison.md)
- [Detailed field differences, failures, and cohorts](data/runs/jobs-v2-repeat/repeat-comparison.json)
- [Original comparison and checked examples](RESULTS.md)
- [Run and repeat commands](README.md)

The repeat comparator includes tests for ordering changes, evidence-only changes,
duplicate predictions, changed job identities, failed outputs, and mismatched input
hashes.

The subsequent 228-window run is complete; see [SEGMENTED_RESULTS.md](SEGMENTED_RESULTS.md)
for its completion, recovery, and field-quality findings.
