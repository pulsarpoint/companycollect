# DeepSeek on native Crawl4AI HTML and Markdown

**Follow-up:** [Validation and retry results](VALIDATED_HTML_RESULTS.md) repeat the
40-page native-HTML run twice with schema/link checks and one correction attempt.
They expose omissions and persistent URL errors that the first experiment alone
could not establish.

**Latest comparison:** [Partial retention and generic HTML chunks](HTML_WINDOWS_RESULTS.md)
measures valid-record preservation and selector-free overlapping fragments against
a fresh full-page native-HTML control.

DeepSeek can extract jobs from Crawl4AI's flattened Markdown, but it still confuses
some title boundaries. On the same 35 full pages with valid outputs in both formats,
native cleaned HTML returned the correct title and URL for **551/563 jobs (97.9%)**;
native Markdown returned **513/563 (91.1%)**. Markdown was 64.7% smaller in characters,
yet cost 18.4% more in this run because its completion and reasoning usage was higher.

Including failed responses across all 40 pages, coverage was nearly tied:
**573/650 (88.2%) for HTML** and **572/650 (88.0%) for Markdown**. HTML had three
invalid responses; Markdown had two. This is one run per format, so the difference
in request reliability is not established beyond these observations.

## Complete-page comparison

The corpus contains 40 saved rendered job-list pages: 31 Ashby, seven Greenhouse,
and two Lever. The existing reference catalog contains 654 listing links. Four
reviewed general-interest/talent-pool entries are excluded, leaving **650 jobs**.
Both native inputs retain all 654 listing URLs. No jobs were removed by preprocessing.

| Metric | Crawl4AI cleaned HTML | Crawl4AI native Markdown |
| --- | ---: | ---: |
| Completed requests | 40/40 | 40/40 |
| Valid JSON and schema | 37/40 | 38/40 |
| Expected jobs on valid pages | 589 | 624 |
| Returned records on valid pages | 589 | 624 |
| Correct job URLs, all 650 expected jobs | 581/650 | 603/650 |
| Correct title + URL, all 650 expected jobs | 573/650 (88.2%) | 572/650 (88.0%) |
| Correct title + URL, each format's valid pages | 573/589 (97.3%) | 572/624 (91.7%) |
| Title differences among correctly linked jobs | 8 | 31 |
| Unexpected URLs | 8 | 21 |
| Duplicate predictions | 0 | 0 |
| Returned general-interest/talent-pool entries | 0 | 0 |

A correct title requires one record with the expected URL and the title from the
saved HTML title element. Comparison normalizes Unicode, case, and whitespace;
it does not remove suffixes, punctuation, or emojis. Missing jobs and all jobs on
invalid-response pages count as misses in the 650-job totals. Matching record counts
alone are insufficient: URL transcription errors still lose jobs.

To compare extraction on identical jobs separately from response validity, use
the **563 jobs on the 35 pages where both formats succeeded**:

| Metric on shared valid pages | Cleaned HTML | Native Markdown |
| --- | ---: | ---: |
| Correct URLs | 557/563 (98.9%) | 544/563 (96.6%) |
| Correct title + URL | 551/563 (97.9%) | 513/563 (91.1%) |

The shared-page table excludes failed responses and therefore does not replace
the first-pass totals above.

## What flattening changes

There were **29 Markdown title errors caused by including adjacent department
text**. Examples checked against the native HTML title elements:

| Actual title | DeepSeek from native Markdown |
| --- | --- |
| Security Engineer, Platform | Security Engineer, Platform Engineering |
| Forward Deployed Engineer - ML | Forward Deployed Engineer - ML Engineering |
| Account Executive, Mid Market | Account Executive, Mid Market Sales |
| Fullstack Engineer, Store | Fullstack Engineer, Store Engineering |

For example, Crawl4AI emits a link label beginning
`Security Engineer, Platform Engineering • Europe • Full time • Remote`.
The original HTML still has a separate `<h3>Security Engineer, Platform</h3>`.
DeepSeek resolves many such cases, but the missing boundary is sometimes ambiguous.
The other two Markdown title errors dropped `(Contract Basis)` and an avocado emoji.

HTML's eight title differences included two `Enterprise` → `Enterpise` typos,
two removed Browserbase city suffixes, three removed `- Public Sector` suffixes,
and the same omitted emoji. HTML preserves the boundary but does not guarantee
that the model copies every title faithfully.

URL errors were also copying mistakes, not missing links in Crawl4AI's output.
Markdown changed `roboflow` to `roboflo` in 18 URLs and damaged three other job URLs.
HTML damaged eight URLs, including five with `jobsashbyhq.com` instead of
`jobs.ashbyhq.com`. Outputs are preserved unchanged; none of these errors were repaired.

## Invalid responses

| Input | Page | Failure | Expected jobs lost |
| --- | --- | --- | ---: |
| HTML | PlanetScale | Extra trailing quote after JSON | 12 |
| HTML | Braintrust | Prose inside JSON / invalid delimiter | 26 |
| HTML | Firecrawl | Invalid JSON delimiter | 23 |
| Markdown | Mintlify | Invalid JSON delimiter | 20 |
| Markdown | MotherDuck | `work_place_type` instead of required `workplace_type` | 6 |

All five responses reported `finish_reason: stop`; none reached the output limit.
Four failed JSON parsing, and MotherDuck failed schema validation. Strict JSON
schema was requested, but the provider still returned these invalid outputs.
All 80 requests completed on their first HTTP attempt. No schema-repair calls or
retries of invalid model output were made, so these are first-pass results.

## Input sizes and observed usage

These sizes cover the same 40 complete pages and exclude the common prompt,
examples, and output schema.

| Input | Characters | UTF-8 bytes | Median page characters | Largest page characters |
| --- | ---: | ---: | ---: | ---: |
| Native cleaned HTML | 438,962 | 442,618 | 9,492 | 26,259 |
| Native Markdown | 154,772 | 158,218 | 3,380.5 | 16,615 |

Native Markdown is **64.7% smaller in characters** and **64.3% smaller in bytes**.
The earlier custom structured Markdown totals 140,083 characters on these full
pages, but it requires platform adapters and is not a native Crawl4AI output.
The native Markdown tested here is `result.markdown.raw_markdown`; it is not the
earlier reconstructed card format or a relevance-filtered `fit_markdown` output.

| Usage across all 40 calls, including invalid outputs | Cleaned HTML | Native Markdown |
| --- | ---: | ---: |
| Prompt tokens | 290,471 | 183,001 |
| Completion tokens | 179,919 | 287,229 |
| Of those, reported reasoning tokens | 122,821 | 224,349 |
| Total tokens | 470,390 | 470,230 |
| Largest completion | 11,780 | 17,373 |
| Median valid call duration | 19.55 s | 29.67 s |
| Reported OpenRouter cost | $0.027835 | $0.032945 |

Combined reported cost: **$0.060779**. Every outcome includes cost. The common
prompt/examples reduce the relative input-token saving compared with raw content
size. Markdown's greater completion usage then offsets that saving. These are
observed charges, including any provider caching, not projected uncached prices.
The arms overlapped in time on the same provider; latency is descriptive rather
than a controlled speed benchmark.

## Metadata comparison with the previous Astra reference

Only the earlier selected job subset has saved Astra/low metadata outputs. Within
the 35 shared valid pages, this gives **131 jobs**, rather than all 563 jobs above.
The six fields are title, location, department, employment type, workplace type,
and job URL.

| Input | Correct title + URL on selected jobs | All-six agreement with saved Astra/low |
| --- | ---: | ---: |
| Native cleaned HTML, full page | 124/131 | 112/131 (85.5%) |
| Native Markdown, full page | 122/131 | 113/131 (86.3%) |
| Previous custom HTML, selected job group | 131/131 | 123/131 (93.9%) |

This is **agreement, not independent metadata accuracy**. The old Astra and custom
DeepSeek calls saw cropped, reconstructed job groups, while the native calls see
full pages with more jobs and more context. The comparison does not isolate just
serialization, and its smaller sample does not represent all full-page title errors.

Source inspection explains some disagreements. FINN's full page explicitly maps
`Tech` to department `Core Functions` and `Account Management` to department
`Growth` in filter links. Native HTML DeepSeek used those departments; the cropped
Astra reference used team names. Contentsquare similarly distinguishes `Sales`
from `All Sales Roles`. These differences should not automatically count as model
mistakes. Both native formats also left Letta's workplace type null where Astra
inherited the board-level `Fully in-person` policy. Markdown's Apify location
disagreement was just spacing around semicolons. Full metadata accuracy needs
source-reviewed labels and explicit inheritance/normalization policies.

## How the native inputs were generated

Preparation uses installed **Crawl4AI 0.9.3** with
`CrawlerRunConfig(verbose=False)` and its default `LXMLWebScrapingStrategy`.
It replays each previously saved rendered HTML document through
`AsyncWebCrawler.aprocess_html`, then saves `result.cleaned_html` and
`result.markdown.raw_markdown` unchanged. These are the outputs exposed by the
[Crawl4AI result API](https://docs.crawl4ai.com/core/crawler-result/).

No site-specific CSS selectors, custom card extraction, known job links, title
labels, or reference model outputs enter preparation or inference. Each request
contains only the source URL, input format, and entire native page, alongside the
same instructions/schema and six synthetic examples. There is no truncation,
segmentation, overlap, or detail-page fetch. Native cleaned HTML retains some
classes, navigation, and filter context; it is not the earlier minimal custom HTML.

This is an extraction benchmark on frozen rendered pages, not a new live crawl.
It tests generic Crawl4AI processing on these saved boards, not general website
discovery, pagination, JavaScript loading, or recall outside the captured content.
The old adapter-generated title/URL catalog is read only by the separate evaluator.
The four exclusions are recorded in
[non_job_listings.json](fixtures/non_job_listings.json).

Both arms use `deepseek/deepseek-v4-flash-0731`, pinned to `baidu/fp8` without
provider fallback, temperature 0, low reasoning, and a 32,768-token output budget.
All 80 responses report the requested model and Baidu provider. No new Astra,
Liquid, GLM, or Codex SDK inference was performed for this experiment.

## Practical next step

Native cleaned HTML currently has the stronger title result on shared valid pages.
Before drawing a production conclusion, test generic output safeguards: validate
each returned URL against the page's links and retry JSON/schema failures. Those
checks need no job-specific selectors. If pursuing Markdown's size advantage,
compare a generic conversion that preserves HTML heading/text boundaries and
overlapping chunks on these same frozen pages. Those follow-up tests have not run.

The reproducible preparation, inference, and report commands are in the
[README](README.md#test-native-crawl4ai-outputs-without-site-selectors). Saved
[comparison.json](data/crawl4ai-html-v1/comparison.json) contains per-page outcomes,
title differences, URL errors, reference disagreements, usage, and hashes.
[size-comparison.json](data/crawl4ai-html-v1/size-comparison.json) contains per-page
sizes. Data and credentials remain ignored by Git.

Validation: 41 tests pass; Ruff formatting/lint and `ty` checks pass for the three
new Python modules. The report verifies frozen input/response hashes, source HTML
hashes, equal inference settings, and retention of all 654 listing URLs. A boundary
test uses an unfamiliar layout and deliberately incorrect reference metadata to
verify that neither native preprocessing nor the submitted prompt depends on it.
