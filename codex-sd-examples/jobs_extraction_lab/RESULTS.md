# Results: 40 job-list pages

Run: `jobs-v2`, captured and evaluated on 2026-09-04.

**The tested Liquid endpoint is not ready to replace the current Codex extraction
step with this whole-page prompt.** Small models are still worth testing, especially
with clearer listing boundaries and smaller output batches. This experiment tests
one model endpoint, prompt, schema, and corpus; it does not establish the limits of
small models generally.

## Completion and coverage

| Measurement | Codex SDK | Liquid through OpenRouter |
|---|---:|---:|
| Pages attempted | 40 | 40 |
| Complete, schema-valid responses | 39 | 20 |
| Failed responses | 1 | 20 |
| Returned job records across successful pages | 620 | 214 |
| Pages with literal-source flags | 0 | 10 |
| Literal-source flags | 0 | 89 |
| Median request duration, including failures | 59.7 s | 30.0 s |
| Reported input tokens | 700,888 | 73,132 |
| Reported output tokens | 75,027 | 254,661 |
| Reported API charge | Not measured | $0.00 |

The Codex baseline used the configured `gpt-6-astra` model with `xhigh` reasoning
through the existing Python SDK and the compatible app CLI. OpenRouter used exactly
`liquid/lfm-2.5-2.6b:free`; there was no model fallback. Both received the same full
Markdown, extraction instructions, and output schema, although the SDK and direct
API have different runtime context and inference settings. This compares the two
application paths, not isolated model architectures under identical compute.

The Codex failure was a 300-second timeout on Modal. Liquid had 18 output-limit
failures and two provider 504 errors, on MotherDuck and PlanetScale. Those two errors
arrived inside response JSON rather than as HTTP error statuses; the current HTTP
retry policy did not retry them. They are service failures, not evidence of field
extraction mistakes. All 40 outcomes remain recorded, including these failures.

The exact Liquid endpoint requires reasoning and advertises an 8,192-token output
limit. Every truncated response reached that limit. It reported **211,897 reasoning
tokens**, 83.2% of its reported output tokens. Excluding reasoning from the returned
text does not free that budget. The larger advertised input context therefore does
not solve these output failures. See the saved
[model metadata](data/runs/jobs-v2/environment.json) and OpenRouter's
[reasoning documentation](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens).

Token totals sum available usage records, including unsuccessful responses when
usage was returned; the two 504 responses supplied no usage. Median durations include
failures, retries, and SDK overhead, so they are not a quality-adjusted speed
comparison. No dollar cost was inferred for Codex subscription usage.

## Agreement on the 20 pages both completed

Codex returned 218 records and Liquid returned 214. **212 openings matched by job
URL**, representing 97.2% of the Codex records on these paired pages. Only **50 of
those 212 matched records (23.6%)** agreed on all six compared fields.

| Field | Equal values | Agreement on 212 matched openings |
|---|---:|---:|
| Title | 65 | 30.7% |
| Location | 174 | 82.1% |
| Department | 183 | 86.3% |
| Employment type | 188 | 88.7% |
| Workplace type | 203 | 95.8% |
| Job URL | 212 | 100.0% |

Job URLs are also the primary matching key, so their agreement is not an independent
accuracy measure. Equal missing values contribute to field agreement. Evidence
quotations are checked separately and are not part of the six-field exact-match
count.

The high URL overlap applies only to the 20 completed pairs. Across all 620 records
from successful Codex pages, only 212 (34.2%) had a matching usable Liquid record.
There is no verified extraction for the page where both backends failed. Neither
percentage is independently measured accuracy: Codex is a reference, not ground
truth.

## Checked examples

These examples were checked against saved Markdown; Zed's separate title and
metadata elements were also confirmed in the saved HTML. They illustrate real
failure modes rather than constituting a complete labeled test set.

- **Zed:** The HTML title is `Designer`. Liquid returned
  `Designer Engineering • The Americas (North, Central, South); Europe • Full time • Remote`
  as the title. Both openings on this two-job page had the same problem. The source
  checker raised no flags because the entire copied label appears in the Markdown.
  [Saved Markdown](data/markdown/ashby-zed.md).
- **Calendly:** For `Senior Full Stack Engineer, AI Scheduling`, Liquid assigned
  `Customer Experience` as the department instead of the `Engineering` section,
  included `Remote - US` in the title, and used `Remote` as employment type. These
  words all occur on the page, so literal-source checks passed despite incorrect
  field assignment. [Saved Markdown](data/markdown/greenhouse-calendly.md).
- **Hedra:** The filter summary says `On-site (2)`, and only the two sales listings
  explicitly carry `On-site`. Liquid also assigned it to the backend-engineer
  listing, where it is unstated. Its evidence was a JSON string such as
  `{ "title": "Senior Backend Engineer" }`, not a source quotation. Codex left the
  unstated workplace type null. [Saved Markdown](data/markdown/ashby-hedra.md).
- **Chromatic:** Liquid included `General Interest - Future Roles`, despite the
  instruction to exclude general applications. It also copied whole link labels
  as titles. Some literal-source flags here additionally reflect removal of the
  space before a Markdown closing bracket; the 89 flags must not be interpreted
  as 89 invented facts. [Saved Markdown](data/markdown/ashby-chromatic.md).
- **Warp:** Liquid returned 16 openings where Codex returned 19, omitting the
  visible `Account Executive`, `Revenue Operations Specialist`, and
  `Sales Development Representative` entries. It still produced valid JSON and
  ended normally. [Saved Markdown](data/markdown/ashby-warp.md).

## What I would try next

1. Preserve the original snapshot, then isolate each job's title, link, metadata,
   and inherited heading. The rendered HTML already separates many fields that
   the Markdown generator joins together.
2. Send approximately 3–5 intact listings per small-model request, with a compact
   schema and a worked example of correct title/metadata separation. A long job
   list should produce multiple bounded results, not one enormous JSON response.
3. Add field-level checks: a value must be grounded in its own listing or section,
   employment type must not become workplace type, and general applications must
   stay excluded. Keep an explicit incomplete-page state when any block fails.
4. Compare against manually labeled examples, then validate the chosen approach
   on fresh held-out pages. Use a larger model only for unresolved blocks.
5. For known recruitment platforms, prefer their available structured data or
   stable DOM fields when that meets the application's needs; keep the LLM for
   text that still needs interpretation.

The roughly tenfold difference in reported input tokens between these application
paths also supports separating ordinary extraction from the Codex agent runtime.
However, this Liquid endpoint's mandatory reasoning and incorrect field boundaries
need to be addressed before expecting reliable savings per correct record.

## Artifacts and validation

- [Full comparison](data/runs/jobs-v2/comparison.md) and
  [per-record differences](data/runs/jobs-v2/comparison.json).
- [Corpus manifest](data/manifest.json): 40 pages, 31 Ashby / 7 Greenhouse / 2 Lever,
  154,772 Markdown characters, mostly English technology companies. This platform
  concentration limits generalization.
- [Run instructions](README.md) and [crawler review](DESIGN_NOTES.md).
- All 40 Markdown hashes and HTML snapshots verified; all 80 backend outcome files
  saved. One unchanged successful Codex smoke result was reused from `jobs-v1`;
  the earlier incompatible-runtime and reasoning-setting smoke tests remain in
  their separate run directories.
- 148 tests passed, including 15 experiment tests. Ruff and ty checks passed.

The existing crawler examples and dependency configuration were not modified.
The authorized API key is confined to the ignored, owner-readable `.env`; it was
not written into prompts or results. Generated corpus and run files are local
artifacts ignored by Git.
