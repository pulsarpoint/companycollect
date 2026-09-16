# Page analysis experiment

Compare one combined extraction request with a router followed by independent
objective-specific requests. This saved-page experiment sits alongside
company_research; the main crawler's scheduling is still separate from this lab.

The [completed seven-page comparison](RESULTS.md) retained the selected facts in
both variants. Routing cost more and introduced additional mistakes; the page
boundary is useful, but all-page specialist routing is not the current recommendation.
The user accepted one-pass as the integration baseline. The
[follow-up](FOLLOW_UP.md) fixes phone evidence validation in 0.15.3 and prepares
revised lab prompts/controls; those prompt changes have not had a model run.

Each analysis returns data, links and a schema version. Data includes objective
arrays, source provenance, per-objective processing status, rejected records and
call diagnostics. Links retain every observed occurrence: full URL or page-local
control, source page, DOM context and an optional visit assessment. Repeated
destinations keep distinct IDs; a future queue may deduplicate fetches.

The ten objectives are company profile, services/products, jobs, technologies,
contacts, people, locations, relationships, certifications/compliance and report
links. Prompts and illustrative examples are in [prompts.py](prompts.py).

## Run

Run from codex-sd-examples using the existing Python 3.12 environment with
company_research installed editable. The lab additionally requires
jsonschema>=4.23,<5, already available in the research environment.

~~~sh
.venv/bin/python -m unittest page_agent_lab.test_agent -v
.venv/bin/python -m page_agent_lab.benchmark \
  --output page_agent_lab/data/my-new-comparison
.venv/bin/python -m page_agent_lab.audit page_agent_lab/data/my-new-comparison
~~~

The runner reads DEEPSEEK from the environment or jobs_extraction_lab/.env.
Use --env for another dotenv file. Credentials are excluded from call logs.
The output directory must be new; existing experiments are never overwritten.
--prepare-only freezes a separate fixture set without API calls; use a different
output directory for a later live run.

The default corpus is company_research/data. Seven saved fixtures cover two IT
ads, contacts, subsidiaries, a 30-entry careers listing with pagination,
NOVELIC credentials, and a report hub with an embedded archive. These local data
are not in Git. Missing snapshots cause an explicit preparation error.

## Execution and measurement

- Both variants receive identical whole native Crawl4AI cleaned HTML, source
  metadata and link inventories. No HTML is truncated or rewritten. Rendered
  HTML from the saved capture supplies navigation when available; this matters
  for the iframe omitted from the report hub's cleaned HTML. Full rendered HTML
  is retained on disk but is not the model's extraction body.
- Both use direct DeepSeek Flash with high reasoning, a 300-second deadline,
  one HTTP attempt and a shared cap of three in-flight calls per variant. The
  65,536 output-token allowance is an experiment setting, not a model limit.
- One-pass requests all objective arrays plus link scores. Routed requests all
  ten decisions plus links, then dispatches each run or uncertain objective.
  Workers receive the original page and only their objective's rules, without
  other worker answers or company history.
- Missing or malformed routing decisions become uncertain. Observed mail/phone
  links can override skipped contact extraction; available JobPosting data can
  override skipped jobs. Raw decisions and overrides remain separate. Heading
  references are diagnostic; this version sends the whole page to each worker.
- Schema errors are recorded. Per-record validation retains valid records
  alongside rejected raw items. The existing source-presence validator checks
  quotations and field evidence. Source-matched does not establish semantic
  truth. No corrections, semantic review agents or catalog calls run here.
- One-pass runs first, routed second. Pages and specialists run concurrently
  within an arm. This single sample cannot isolate provider load/cache effects.
- Forty selected factual checks, nineteen negative checks and four link groups are
  frozen from source review before calls. They are not exhaustive ground truth
  or an overall accuracy score. A separate diagnostic runs only known-positive
  objective pairs skipped by the router. The original completed pilot used four
  negative checks; its saved controls and results are unchanged.

Experiments save hashes of HTML, input inventories, controls and frozen code.
Call artifacts retain prompts, schemas, responses, tokens and errors. Cost is
estimated from returned usage at dated provider rates; requests without usage
retain unknown cost.

## Scope

This tests a page as the unit of work. It does not fetch pages, operate pagination,
follow iframes, schedule a queue, merge entities, resolve catalog identities,
parse PDFs, submit records or deploy. It cannot establish autonomous coverage.
Long-page section processing, bounded recovery, a persistent coordinator and
final merge remain integration work after page-level quality is evaluated.

Not-selected means an uncalled analysis; failed means unsuccessful processing;
processed with an empty array means no facts were returned. None establishes
that the company lacks that information.
