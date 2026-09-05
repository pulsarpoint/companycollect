# Overlapping-window extraction results

**Splitting the Markdown largely solved output truncation in this run. It did not
make the extracted field values reliable.** The complete workflow is implemented
in `segment.py`: prepare overlapping windows, extract each with Liquid, and merge
records with provenance, link validation, and conflict reporting.

## Workflow tested

The same 40 saved pages were split into **228 windows**, each with at most four
recognized job links and 3,000 characters. Windows keep complete listing lines,
repeat preceding section context, and overlap one complete listing where the size
budget allows. For example, windows can cover listings 1–4, 4–7, and 7–10. The text
limit takes precedence on unusually long listings such as SearchAPI's country lists.

All source lines and recognized Markdown job links remain represented. Separating
whitespace was added between job headings that Crawl4AI concatenated. The source
snapshots were preserved, and the Liquid model, prompt, schema, temperature, and
8,192-token output limit were unchanged. Window requests used concurrency three;
two successful smoke requests were reused unchanged.

The merge accepts job URLs observed in the same window, deduplicates by URL, and
preserves separate openings with identical titles and different URLs. Conflicting
field sets remain recorded with their window IDs. The provisional representative
is supported by the most distinct windows, with ties retaining the earliest window.
No larger model is used for extraction or conflict resolution.

## Results

| Measurement | Result |
|---|---:|
| Complete window responses | **227/228 (99.6%)** |
| Pages with every window complete | **39/40** |
| Merged unique job records, including the partial page | **650** |
| Matched Codex-reference job URLs | **617/620 (99.5%)** |
| Matched records equal on all six fields | **35/617 (5.7%)** |
| Jobs with conflicting predictions between windows | **136** |
| Job URLs returned by one window but omitted by another containing window | **7** |
| Predictions rejected for missing/unrecognized job URLs | **19** |
| Literal-source flags on selected merged records | **293** |

The 650 merged records include Modal, for which the original Codex request timed
out. The 620-record reference comes from the other 39 pages. Matching a known URL
does not establish that the title, department, location, or employment fields are
correct. Extra general applications can also have valid job-board URLs.

One four-listing window on Linear still hit the output limit. Its neighboring
windows recovered two boundary listings, but two URLs inside that window were not
returned. Linear therefore has 27 merged records and remains explicitly incomplete.
The original failed response is preserved; it was not silently retried or repaired.

Overlap also recovered two other Linear URLs and three Contentsquare URLs omitted
by a containing window. These seven are recovery relative to individual window
outputs, not independently verified true positives.

Two Contentsquare windows contained navigation/filter text. Liquid incorrectly
treated some team filters as jobs. The recognized-link check rejected 18 such
predictions; one additional prediction was rejected on Dust. All rejected records
remain available for inspection.

## Did splitting improve field extraction?

It improved completion and coverage, but field agreement did not improve in this
pass. On the **same 212 reference openings found by both methods**, the first
whole-page run matched all six fields on **50 records (23.6%)**, while segmentation
matched on **22 (10.4%)**. Holding the openings constant avoids comparing different
page subsets. The repeatability experiment already showed model variability, so
this is an observed result of this configuration, not proof that segmentation always
reduces field quality.

The largest problem is still the title boundary. Across the 617 matched openings,
555 titles differed from the Codex reference. For example, CircleCI's `Software
Engineer` became `Software Engineer Toronto, Ontario`; another title included the
entire multi-city location. The model often treats the whole Markdown link label
as the job title. Shorter windows alone do not teach the missing field separation.

The 136 overlap conflicts expose another limitation: independently processing the
same listing can produce different field values. Majority voting is a deterministic
merge policy, not a correctness check. Both conflicting alternatives are retained.
Even a record with no automated flags can be wrong, as the earlier Zed example
demonstrated.

## Resource tradeoff

The 228 requests reported 204,963 input tokens and 785,894 output tokens, including
654,709 reasoning tokens. Their reported API charge was **$0.00** on the requested
free endpoint. There were no transport retries. Summed request time was 3,546.7
seconds; requests ran concurrently, so that sum is not wall-clock duration.

More requests and overlap add prompt overhead and duplicate work. These results
demonstrate better completion, not proven cost savings on a paid deployment.

## Recommended next change

Keep segmentation, checkpointing, and overlap, then improve how fields are presented
to the model. Preserve explicit title/metadata boundaries from the original DOM
where available, or give the Markdown extractor a precise parsing example that
separates titles from department/location text. Validate that change against labeled
jobs before deploying it. A bounded recovery step could also split a failed window
further; that was not part of this first segmented run.

The current implementation intentionally keeps incomplete pages, rejected
predictions, and conflicting field sets visible. It is an experiment harness and
workflow prototype, not a verified source of production job data.

## Files and validation

- [Run instructions](README.md#extract-overlapping-windows-with-the-small-model)
- [Generated comparison](data/segmented-v1/runs/liquid-windows-v1/segmented-comparison.md)
- [Complete records, differences, conflicts, and recovery evidence](data/segmented-v1/runs/liquid-windows-v1/segmented-comparison.json)
- [Window manifest and prepared-text ranges](data/segmented-v1/windows.json)
- [Whole-page repeatability results](REPEAT_RESULTS.md)

All 159 tests passed, including 26 experiment tests; lint and type checks passed.
All API responses and both earlier whole-page runs were preserved separately.
