# One-pass versus routed page specialists — 16 September 2026

**Keep the page-agent boundary. This pilot does not justify making routed
specialists the default.** Both approaches retained the selected important facts.
Routing increased total calls, elapsed time and cost, and some specialists added
incorrect records. It did improve quotation completeness in some outputs and
discover additional useful product names, so targeted specialists remain worth
testing.

## Completed experiment

Seven frozen pages: Handelsbanken careers listing, two IT ads, contact page,
subsidiaries and report hub; NOVELIC quality/certification page. No new crawling.
Both used direct DeepSeek Flash with high reasoning, identical original cleaned
HTML and link inventories, shared objective rules, three concurrent calls, no
correction chains, no semantic-review model and no technology catalog calls.

One-pass requested all objectives and link scores. Routed requested all ten
decisions and link scores, then called each selected specialist with the original
page and that objective's prompt/schema. Prior findings and other specialist
answers were never supplied. The one-pass arm ran first.

| Measurement | One pass | Router + specialists |
|---|---:|---:|
| Finished pages | 7 | 7 |
| Model calls | 7 | 67 |
| Total wall time, concurrency 3 | 3.18 min | 8.96 min |
| Estimated API cost | $0.10676 | $0.48565 |
| Frozen exact-name controls | 38/40 | 38/40 |
| Source-reviewed fact retention, with two matching corrections | 40/40 | 40/40 |
| Careers entries with matching source title and job URL | 30/30 | 30/30 |
| Observed link occurrences assessed | 278/278 | 278/278 |
| Selected negative checks passed | 4/4 | 4/4 |
| Schema-valid records retained, including review items | 255 | 336 |
| Records passing existing source-presence checks | 75 | 189 |
| Individually rejected records | 1 | 1 |
| API/whole-response schema failures | 0 | 0 |
| Output truncations | 0 | 0 |

Record counts and the 40 positive checks are **not overall accuracy or precision**.
More output included both additional useful records and additional errors.
Source-presence checks are also not semantic approval.

The two original matching misses were evaluation issues, preserved transparently:
the ownership paragraph uses the supported short name **Handelsbanken Liv**, while
the frozen control expected its full legal name; the Data Warehouse vacancy
control omitted the source title's **till Handelsbanken** suffix, which both models
correctly retained. Original controls/comparison were not overwritten.
Post-run corrections and corresponding record IDs are in source-review.json.

Both retained the tested named job tools, team-scoped Azure Pipelines expertise,
OneLake/Power BI/Data Factory requirements, recruiter contacts, all five stated
subsidiaries, the two ISO claims and IATF working-toward status. Both preserved
all 30 listing titles and URLs. Both ranked the observed next-page control,
job links, embedded archive and NOVELIC careers links above routine policy links.
No iframe was followed and no document contents were read.

## What routing actually did

All **14 source-control-required page/objective pairs** were dispatched. No
diagnostic recovery calls were needed. This establishes selected routing coverage,
not exhaustive recall across all objectives or companies.

The router selected **60 of 70 possible specialist analyses**. Eighteen workers
returned empty arrays; ten analyses were skipped. No deterministic safeguard
overrides were needed. Empty results are legitimate, but this routing policy saves
little work compared with calling every specialist.

On NOVELIC's quality page, the router explicitly said Careers and Management Team
were only navigation links with no openings or named people. It still selected
both analyses as uncertain. All ten specialists ran on that page. Stage 1 needs
to distinguish a useful destination from facts currently available for extraction.

Specialists had a lower median response time (16.1 seconds across routed calls,
versus 58.2 seconds for combined calls), but the complete routed arm took **2.82×**
as long and cost **4.55×** as much. It sent 1,751,322 prompt tokens and received
381,331 completion tokens, versus 213,083 and 130,834. Reported reasoning tokens
were 320,245 versus 92,343; they are included in completion tokens.

## Source-review findings

Useful additional extraction included NOVELIC's specific named radar products
and Handelsbanken's GlobalOn-Line/Nordic-I applications. These are offered
products, which must remain distinct from the company's internally deployed stack.

The routed outputs also exposed concrete failures:

- **False technology identities:** the report hub yielded vp292.alertir.com and
  shb-iframe as technologies. These are an observed iframe host/element name,
  without a supported specific product identity.
- **Wrong type of entity:** six market/presence countries became office addresses;
  four people's contact descriptions became company-profile records with the
  person in the company field; a list of countries became a company relationship.
- **Relationship overstatement:** Jobylon's powered-by footer became a supplier
  relationship with the employer. Keep hosting/platform hints separate from
  explicitly stated commercial relationships.
- **Claim dates:** the data-ad technology worker and some relationship workers
  copied the capture date into as_of. Claim dates must be source-stated;
  fetched_at already records when the evidence was observed.
- **Service versus product:** AURIX development/testing became offers AURIX.
  The one-pass output used advertised expertise. Services involving a product do
  not establish that the company sells the product itself.

Both approaches also need improvements. They promoted the Fabric/Databricks
target-architecture language to stated company use in extra observations,
classified report-ordering actions as services, and included general purchase
conditions among company reports. A report hub/order form needs a navigation
category distinct from an actual document. Both proposed RISC-V, which the
existing specific-technology validator rejected.

The four frozen negative checks did not cover all these new cases. Passing them
does not mean no false positives occurred. Source-review.json records selected
issues and record IDs without claiming an exhaustive error count.

## Evidence validation is a separate problem

Many quotations contain the technology/contact value but omit the actor or full
job title required by the existing validator. These are attachment failures even
when the page establishes the attribution. The routed version improved some
attachments, but neither output is ready for automatic acceptance.

There is also a concrete pre-existing phone-validation bug in
company_research/content.py: the phone-specific digit comparison runs for every
anchor field, including owner. A correctly quoted owner such as Peter Grabe has
no digits and therefore fails. The saved phone record shows the exact name and
number both matching the source, yet receives owner_not_in_evidence.
Zero-width characters inside published email labels cause separate false misses.

Keep this experiment unchanged. Next fix phone value matching independently from
owner matching, define safe invisible-character normalization, and attach
page-local actor/job evidence explicitly. Never remove actor checks or treat any
name appearing somewhere on the page as proof of attribution. Composite facts
should reference their own field evidence and a verified page/section identity.

## Recommendation and limits

Use the combined page request as the next integration baseline. Keep the concrete
PageAgent and separate specialist prompts available for measured, targeted cases:
failed objectives, ambiguous technology signals, or complex ownership/credential
sections. This hybrid has **not** been tested yet.

Before another comparison, strengthen objective boundaries, source-only dates,
navigation-versus-document rules and negative fixtures using the failures above.
Then repeat on a broader saved-page set and measure targeted/grouped specialists.
People and contacts are a plausible group; job identity and technology evidence
also need shared attribution without sharing model-generated answers as evidence.

Only after that should the Python coordinator persist page results, upsert scored
links and dispatch unvisited targets. Final company merge/catalog normalization
belongs after page extraction. An uninterrupted URL-started crawl is still needed
to test discovery, pagination, budgets and stopping behavior.

This was one run per arm on seven selected historical captures from two sites.
It excludes catalog resolution, semantic-review/correction chains and fetch time,
so its cost/runtime cannot be compared directly with the older full pipeline.
Sequential arm order and provider caching/load were not controlled with repeats.
The main crawler, its defaults and backend data remain unchanged.

## Saved artifacts and checks

Run directory: page_agent_lab/data/two-pass-20260916/.
It contains fixtures, original controls, frozen implementations, 74 complete call
artifacts, page JSON, comparison.json, source-review.json and verification.json.
The [archive receipt](RUN_RECEIPT.json) records a verified 146-file backup of the
complete run (6.78 MB compressed), with its archive hash and per-file manifest.
The routing diagnostic finished with zero calls. Files under data are Git-ignored;
the report, prompts, runner and unit tests are versioned separately.

All 74 requests were verified to contain the identical full source payload for
their page, direct DeepSeek/high settings, two initial messages and no catalog
tools. Twenty-nine frozen code files matched their hashes, and the main package
still matched the saved implementation. Credential scan passed.
Six HTTP-boundary/contract tests, Ruff and type checks passed.

Cost uses returned usage and the provider's rates checked on 16 September:
[DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/).
All calls had usage; total estimated cost was **$0.59240**. This is an estimate,
not a billing statement. The 65,536 output-token allowance was an experiment
setting, not the model's maximum; neither arm reached it.
