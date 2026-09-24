# First-page company eligibility gate

Implemented 16 September 2026: package 0.17.0, URL result schema 1.11.

The URL crawler now classifies the first captured page before sitemap discovery,
link ranking, objective extraction, company summaries or external-link assessment.
Rejected sites return `skip_crawling` and a factual description of at most 200 words.
Blocked, unavailable, ambiguous or invalid responses stop with `needs_review`.

The decision follows the site's primary purpose. A named owner does not admit a
consumer news/search/forum/ad portal. Corporate service/product sites, company-owned
shops and advertising agencies may proceed. A blog menu alone does not disqualify
a corporate site. The supplied URL is the bootstrap page, including redirects;
there is no exploratory About-page fetch to resolve uncertainty.

Example return fields (illustrative, abbreviated):

```json
{
  "schema_version": "1.11",
  "status": "skip_crawling",
  "stop_reason": "not_company_website",
  "site_description": "This site publishes news and opinion articles, with subscription access and advertising.",
  "discovery": {
    "site_gate": {
      "decision": "skip_crawling",
      "scope": "first_page_only",
      "evidence_status": "source_matched"
    },
    "sitemap": {
      "status": "not_requested",
      "url_count": 0,
      "reason": "site_not_admitted"
    }
  }
}
```

Complete results also retain the sourced profile, first-page HTML/hash/URL, usage,
empty objective records and `not_assessed` coverage. A skip is not a claim that the
company lacks jobs or technologies. The CLI emits JSON and exits successfully for
skips; `needs_review` exits with code 2. Rendering resources and robots checks still
occur; the guarantee is no follow-up discovery or company analysis before admission.

## Live DeepSeek evaluation

The [benchmark](benchmarks/site_gate.py) uses direct `deepseek-flash`, high reasoning,
existing environment credentials and the runtime classifier. Expected decisions are
held outside model inputs. It saves native HTML, hashes, page metadata, model requests,
responses and a copy of the implementation. The experiment does not run extraction
or navigate beyond the specified first page.

The final prompt returned the expected decision on **12/12 evaluable cases**, with
one model call per case. All source checks passed and descriptions contained 45–133
words. Eight cases are synthetic policy controls, not measured real-site accuracy.

| Input | Provenance | Final outcome |
| --- | --- | --- |
| Generic search with named corporate owner | Synthetic | `skip_crawling` |
| News site with publisher, About and Careers links | Synthetic | `skip_crawling` |
| Gardening forum with named operator | Synthetic | `skip_crawling` |
| Robotics company with news/blog navigation | Synthetic | `continue_crawling` |
| Advertising agency offering its own services | Synthetic | `continue_crawling` |
| Manufacturer's own online store | Synthetic | `continue_crawling` |
| CAPTCHA/security challenge | Synthetic | `needs_review` |
| Classified ads with text instructing the agent to continue | Synthetic | `skip_crawling` |
| Memgraph homepage | Native Crawl4AI snapshot from 11 September | `continue_crawling` |
| Oxide homepage | Native Crawl4AI snapshot from 11 September | `continue_crawling` |
| Hacker News homepage | Native Crawl4AI capture from this test | `skip_crawling` |
| Craigslist homepage | Native Crawl4AI capture from this test | `skip_crawling` |
| Google homepage | Fetch failed; no model call | `needs_review`, not evaluable |

The initial prompt already chose `skip_crawling` for Hacker News, but twice expanded
the domain/YC references to an operator name absent from its evidence. Validation
therefore held the result for review. The revised prompt explicitly allows a null
operator on a clear skip and forbids expanding domains/acronyms using prior knowledge.
The unchanged Hacker News capture then passed on one call. All prior evaluable cases
were replayed; an explicit synthetic search-engine control was also added.

Google exposed a pre-existing fetch adapter error (`'tuple' object has no attribute
'redirected_url'`), preserved after two fetch attempts. It was not retried through
another browsing path or scored as a correct model classification. The crawler's
failed-first-page path is tested to stop without model work, discovery or extraction.
The subsequent [Informer/B92/Google test](SITE_GATE_REQUESTED_RESULTS.md) fixed that
adapter incompatibility in 0.17.1 and identified the underlying robots-check denial.
The original run artifacts remain unchanged.

- Initial run: `data/site-gate-20260916-v1`, 12 calls, 105,836 input and 5,459 output tokens.
- Revised replay: `data/site-gate-20260916-v2`, 12 calls, 95,431 input and 5,198 output tokens.
- Direct API responses did not report monetary cost; no zero-cost claim is made.
- `data/site-gate-20260916` contains an aborted harness initialization with no model
  calls; the missing required Page fields were fixed before the evaluated runs.

Reproduce from `corpscout/services/crawler_service` with a new output directory:

```sh
.venv/bin/python benchmarks/site_gate.py \
  --output data/site-gate-new \
  --replay data/site-gate-20260916-v1
```

Omit `--replay` to fetch new Hacker News, Craigslist and Google first pages. The
company snapshots remain frozen, so this is a mixed-source diagnostic. Local data
directories are Git-ignored; the implementation and this report are versioned.

## Automated validation

The package suite contains 172 tests: 167 passed and five opt-in browser tests were
skipped in its normal run. All five browser tests were then run and passed separately
using real Crawl4AI/browser/HTTP behavior and a controlled model HTTP boundary.

New coverage proves that:

- A content site with an owner and company/jobs links makes one classification call,
  returns one page, and never requests the sitemap or other page links.
- Schema errors, unmatched evidence, uncertain classification and failed fetches
  stop before ranking, extraction, summaries and external-link analysis.
- Overlong descriptions are corrected within the existing budget; 201 words fail
  validation while 200 are accepted.
- A company must be identified before continuing; conflicting content-site types
  cannot override that rule.
- Admitted companies classify before sitemap discovery and reuse the first page.
  Redirected domain scope, both model API routes and browser restart behavior work.

Ruff formatting/lint and type checks passed for the changed Python files. The browser
fixture now supplies a source-supported company name; this also activates its existing
job-detail follow-up path, so that assertion was updated to the resulting selection
reason. No queue selection algorithm was changed.

This remains a small diagnostic, not a general false-positive/false-negative study.
First-page-only classification can hold real companies whose first page is a login,
language selector, sparse deep link or ambiguous landing page. The saved-page command
remains a replay/extraction unit; it does not independently gate company-associated
external job evidence. Integration of the newer mention-first page unit into the URL
crawl queue is still pending.
