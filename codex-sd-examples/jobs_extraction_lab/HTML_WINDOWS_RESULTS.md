# Valid-record retention and generic HTML chunks

Completed 6 September 2026 on the same 40 saved job-list pages. Keeping individually
valid records helped. Splitting every page did not improve extraction accuracy on
pages where both approaches completed without service errors, and increased cost
and metadata conflicts. Full-page native Crawl4AI HTML remains the better default
for these page sizes; generic chunks are a useful fallback to evaluate further.

## Comparison

Both arms used partial retention and one optional correction. Correct title + URL
means a unique source URL with its normalized title matching the frozen catalog.
The denominator includes all 650 reviewed jobs, including pages affected by errors.

| Measure | Full-page HTML | Overlapping HTML chunks |
| --- | ---: | ---: |
| Pages | 40 | 40 |
| Initial inputs processed | 40 | 98 |
| Correction calls | 3 | 4 |
| Total calls | 43 | 102 |
| Request timeouts | 6 | 8 |
| Correct title + URL, first attempts | 539 / 650 | 584 / 650 |
| Correct title + URL, after retention/correction | 571 / 650 (87.8%) | 600 / 650 (92.3%) |
| Matching job URLs after retention | 573 / 650 | 602 / 650 |
| Unexpected URLs in retained results | 0 | 0 |
| Pages with every expected URL | 34 / 40 | 35 / 40 |
| Jobs with conflicting observations | 2 | 19 |
| Recorded cost, including corrections | $0.02661544 | $0.04628776 |
| Calls without usage/cost data | 6 | 8 |

All 138 inputs were attempted; this does not mean every input succeeded. All 14
service errors reached the configured 180-second total request deadline. Their
cost is unknown, not zero. The $0.07290320 combined recorded cost is therefore not
a verified final bill. Completed failures were preserved when resuming pending
work after transient-service stops; they were not silently rerun until successful.

The aggregate coverage difference is largely a difference in which requests timed
out. It is not evidence that chunks extract more accurately:

| Same 30 pages with no service error in either arm | Full-page HTML | Chunks |
| --- | ---: | ---: |
| Expected jobs | 469 | 469 |
| Matching URLs | 469 / 469 | 469 / 469 |
| Correct title + URL | 468 / 469 (99.8%) | 467 / 469 (99.6%) |
| Calls, including corrections | 32 | 72 |
| Recorded cost | $0.02229122 | $0.03316053 |

This cohort has no missing cost records. Chunking cost 48.8% more on those same
pages. Excluding timeouts also selects a subset of pages, so both this table and
the all-page table are needed. This was one run per arm, not a statistical estimate
of failure rates or proof that input length caused the timeouts.

## What retention changed

The report also applies the earlier rule—use only the final response if every
record passes validation—to these exact same new responses. This requires no
additional inference and isolates the effect of preserving valid observations.

| Same responses, different acceptance rule | Full-page HTML | Chunks |
| --- | ---: | ---: |
| Correct title + URL with final-valid-response rule | 551 | 600 |
| Correct title + URL with retention | 571 | 600 |
| Matching URLs with final-valid-response rule | 554 | 602 |
| Matching URLs with retention | 573 | 602 |

Mintlify returned 19 correct jobs and one malformed URL in its full-page response.
Its correction timed out. Retention kept all 19 correct jobs; discarding the final
invalid response would lose them. Braintrust initially returned 25 valid jobs and
one malformed URL. Correction recovered the missing job, but shortened another
previously correct title from `Software Engineer, Systems` to `Software Engine`.
Keeping the initial valid observation preserved that title as well.

Both arms ultimately found all 26 Braintrust jobs. Its earlier 24-job omission did
not recur in the new full-page control, so this run cannot credit chunks with
solving that earlier omission. Modal returned all 30 jobs in both arms. Poolside
returned all 15 through chunks, while its full-page request timed out.

Full-page correction succeeded as a schema/URL repair in 2 of 3 calls: Cartesia's
invalid JSON and Braintrust's invalid URL. Mintlify timed out. All four chunk
corrections passed: Contentsquare's misspelled schema keys, Granola's invalid JSON,
Dust's malformed URLs, and Runpod's missing evidence field. Validation success
still permits misspelled titles, omissions, and incorrect metadata.

## What overlap changed

Six expected URLs were returned only by a neighboring chunk, without an accepted
observation from the chunk whose core contained the link: three Langfuse jobs and
three Encord jobs. The owner chunks had timed out. This demonstrates recovery from
failed requests within these overlapping inputs. There were no such recoveries on
pages without service errors.

The diagnostic compares core-owned versus all observations from the same requests.
It is not an independent no-overlap experiment: removing overlap from prompts could
change the model's answers. It also does not establish that this overlap size is
optimal.

Missing URLs after retention were:

| Page | Full-page missing | Chunked missing | Service failure affecting the page |
| --- | ---: | ---: | --- |
| Chromatic | 6 | 0 | Full page |
| Dust | 24 | 0 | Full page |
| Mintlify | 1 | 2 | Full-page correction and one chunk |
| Poolside | 15 | 0 | Full page |
| Warp | 19 | 0 | Full page |
| SearchAPI | 12 | 0 | Full page |
| FINN | 0 | 12 | Two chunks |
| Airtable | 0 | 16 | Both chunks |
| Langfuse | 0 | 4 | One chunk |
| Encord | 0 | 14 | Two chunks |

All other pages had every expected URL after retention in both arms.

## Title and metadata review

Each arm had two remaining title mismatches, verified against the native source:

| Input | Source title | Returned title |
| --- | --- | --- |
| Full FINN | `(Senior) B2B Sales Manager (m/w/d)` | `(Se)nior) B2B Sales Manager (m/w/d)` |
| Full Cartesia | `Applied Researcher, Audio Post-Training` | `Appled Researcher, Audio Post-Training` |
| Chunked Cartesia | `Technical Sourcer` | `Technical Sorcer` |
| Chunked Cartesia | `Scaled Customer Success Manager` | `Scaled Customer Sucess Manager` |

These are copying errors despite complete, valid JSON. URL membership checks do
not catch them. Some erroneous evidence strings repeat the same title typo, so
agreement with the model's own evidence is insufficient.

The 19 chunked conflicts comprise 18 department disagreements and one workplace
disagreement. Examples include Railway's `Engineering` versus `Platform Engineering`,
FINN's parent `Core Functions` versus `Finance & Legal` or `Tech`, and Dust's
`Sales Sales Account Executive` versus `Sales`. Splitting can change which heading
the model treats as context even when links remain intact. The two full-page
conflicts were Braintrust's title rewrite and `Full time` versus `Fulltime`; a flag
does not necessarily indicate a meaningful semantic error.

Among 525 job URLs returned by both arms, 479 (91.2%) agreed on all six normalized
fields. The other 46 differed in titles, departments, workplace, or location.
Examples include `Dubai` versus `Duba` and a missing location versus `Remote`.
These are consistency measurements, not a gold-standard metadata accuracy score.
The independent source catalog covers titles and URLs only.

The merger keeps all conflicting alternatives and their provenance. Its preference
for a core-owned observation, then an initial response, then source order is
deterministic; it does not certify the chosen value as correct.

## Input and cost details

Full-page controls are unchanged native `cleaned_html` from the earlier Crawl4AI
0.9.3 run on saved rendered HTML. Chunk inputs are a new generic postprocessing
step: literal source ranges plus copied preceding headings. They are not themselves
a native Crawl4AI output field. No platform CSS selector, known job URL, reference
title, or reference job count was used to construct or extract from them.

The splitter targets 6,000 core characters, 2,000-character source atoms, and 1,200
characters of preceding overlap. It keeps anchors, table rows, and headings whole
and copies up to 1,500 characters of preceding heading context. Whole elements can
exceed these targets. Fragment boundaries can leave enclosing containers unmatched;
the code does not reserialize HTML into balanced documents. The audit confirmed
that the cores concatenate to the exact original HTML and preserve all page links.

| Measure | Full-page HTML | Chunks |
| --- | ---: | ---: |
| Input content characters, before prompt/corrections | 438,962 | 543,803 |
| Largest input | 26,259 | 8,345 |
| Reported prompt tokens, all metered calls | 280,025 | 507,602 |
| Reported completion tokens, including reasoning | 173,762 | 326,817 |
| Reported reasoning tokens | 119,556 | 258,867 |
| Reported cached prompt tokens | 118,784 | 293,888 |
| Recorded input cost | $0.00924619 | $0.01361913 |
| Recorded output cost | $0.01736925 | $0.03266863 |
| Recorded correction cost, included above | $0.00178462 | $0.00198772 |
| Median response time excluding service errors | 20.770 s | 15.254 s |

Chunk content grew 23.9%, but each chunk repeats instructions/examples and generates
its own response and reasoning. The overall recorded cost grew 73.9%; missing
timeout charges and differing failed pages limit that comparison. More calls also
offset the shorter median response time. Both arms were interleaved with concurrency
three; their summed request times are not separate end-to-end wall-clock benchmarks.

Inference used `deepseek/deepseek-v4-flash-0731`, Baidu FP8 only, no provider fallback,
temperature zero, low reasoning, and the same frozen base prompt, examples, schema,
and 32,768-token output ceiling. All 131 returned completions reported `stop`; none
reported token-limit truncation. Maximum reported completion usage was 13,749 for
full pages and 27,162 for chunks. The character targets are unrelated to that output
ceiling. The input-format label differs between arms. The copied baseline settings'
`input_policy` describes the original full-page benchmark; the window manifest and
its per-input `variant` describe this experiment's actual inputs.

## Implementation and next use

The isolated lab now contains `html_windows.py` for source-range preparation,
`partial_html.py` for extraction/retention, and `html_windows_report.py` for auditing
and comparison. CLI commands are in [README.md](README.md#partial-records-and-generic-html-windows).
The production crawler has not been switched to this experimental runner.

Use per-record retention by default. For the next integration, start with full-page
native HTML, keep valid records, and use bounded chunk processing for large inputs
or incomplete/failed pages. This run tested full versus chunked extraction, not that
hybrid strategy. Completeness detection, transient-request recovery, title copying
checks, and department conflicts remain necessary work before unattended use.

Artifacts are under `data/html-windows-v1/`: frozen inputs and `manifest.json`, all
145 attempt records and 138 unit outputs under `runs/deepseek-partial-v1/`, and
`comparison.json` containing merged jobs, alternatives, provenance, scores, and
request accounting. `status.json` records completion; `stopped.json` retains a
historical stop event. Data and credentials remain ignored by Git. Evaluation used
the earlier frozen title/URL catalog only after inference; no new reference-model
calls or web crawls were made.

Validation: 49 offline tests cover existing extraction plus lossless ranges, overlap,
partial rejection, correction retention, deduplication, conflicts, resumability,
reference isolation, cost accounting, and detection of modified inputs/results.
Ruff and ty checks pass. The real-data report verifies source/input/request hashes,
record validation, link coverage, and merged provenance across all saved attempts.
