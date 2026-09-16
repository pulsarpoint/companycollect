# Revised one-pass evaluation — 16 September 2026

The revised prompt improved the seven-page regression, and the page boundary
remains useful. **The gate for crawler integration has not passed.** Additional
sites exposed malformed JSON, evidence/provenance gaps and technology-category
ambiguity. The main crawler and its queue are unchanged; no autonomous crawl was
started after this gate failed.

All calls used direct DeepSeek `deepseek-flash`, high reasoning, the same prompt,
native Crawl4AI cleaned HTML and a separate observed-link inventory. No source
HTML was simplified or truncated. The six additional pages were not used to tune
this prompt. Controls were frozen from source review before their model calls.
These are selected checks, not exhaustive accuracy measurements.

## Results

| Measure | Original one-pass baseline | Revised seven pages | Six additional pages, first attempt |
|---|---:|---:|---:|
| Model requests | 7 | 7 | 6 |
| Wall time | 191.0 s | 168.2 s | 231.2 s |
| Estimated cost | $0.10676 | $0.10273 | $0.14379 |
| Selected facts, source-reviewed names | 40/40 | 40/40 | 21/32 |
| Negative checks passed / evaluated | 15/19 | 19/19 | 16/18 |
| Negative checks unavailable | 0 | 0 | 3 |
| Observed links assessed | 278/278 | 278/278 | 410/507 |
| Records passing schema validation | 255 | 238 | 132 |
| Records passing source-presence checks | 81 | 118 | 53 |

The original baseline uses the previously saved 0.15.3 phone-validation replay
for source-presence accounting and the expanded controls for negative checks.
It used an earlier prompt. Timing/cost differences are single observations with
provider/cache effects, not a statistically established speed improvement.

The revised regression's **frozen automated score is 39/40**. Source review
confirms the remaining ownership fact: the model returned `Stadshypotek`, exactly
as the ownership paragraph does, while the control expected `Stadshypotek AB`.
Both names occur in the same company section. The source-reviewed score is 40/40;
the frozen controls and score remain unchanged. All 30 careers-list titles and
their observed URLs were retained, with all four navigation-priority groups
above routine policy links, including pagination and the report iframe.

The additional sources were Memgraph's homepage/careers page, Oxide's homepage/
networking job and RT-RK's jobs list/industrial IoT case study. All 21 selected
facts on the five successfully decoded pages passed their qualifiers. They
included Google Meet use, M12/HeavyBit investor relationships, Rust/C/P4 job
context, OPTE, RT-RK's Linux/C++/OBLO project and Endress+Hauser as a customer.

## RT-RK failure and separate retry

The first jobs-list response contained a schema-valid JSON object with all seven
jobs, followed by **one extra `}`**. Its finish reason was `stop`, not `length`.
The strict parser rejected it. No output-token limit was reached and no factual
data was silently salvaged into the accepted result.

One separately saved retry used identical page inputs and the unchanged prompt:

- 1 call, 108.2 seconds, estimated $0.01742;
- 11/11 selected facts, including all seven jobs;
- 3/3 negative checks and all 97 observed links assessed;
- 27 schema-valid records, 21 passing source-presence checks.

Combining the five successful original pages with that retry gives **32/32
selected facts, 19/21 negative checks, and 507/507 assessed links**. It yields 159
schema-valid records, 74 source matched. This is explicitly a result *after a
retry*, not a successful six-page first attempt. The first failure remains saved.

All three runs together used **14 calls, estimated $0.26395**. Costs use returned
token/cache usage and the dated DeepSeek pricing model recorded in each report.
They are estimates, not invoice amounts. Every request returned usage.

## Remaining problems

1. **Evidence completeness.** In the seven-page run, 95 records lacked the
   supplied company name in their quoted evidence. Other failures included
   missing owners/job titles and normalized names that differ from source
   wording. This is distinct from whether a fact is actually supported. The
   model still omits quotations despite the prompt; adding another general
   instruction alone is unlikely to solve the contract problem.

2. **Evidence from two representations.** The model could see the rendered
   link label `Handelsbanken - till startsidan`, but the native cleaned HTML
   lacked it. Two records quoting that label failed the native-only validator.
   Evidence needs an explicit origin and source-fragment reference. Do not solve
   this by accepting any company name found somewhere on the page.

3. **Technology policy is underspecified.** BGP/IPsec and Bluetooth failed two
   holdout negatives. Manual review additionally found NVMe, DDR5, `E+H’s
   protocol` and the broad `Atlassian Cloud Tools` label. The current schema
   explicitly allows `protocol_standard`; a source-supported named protocol
   therefore does not imply model hallucination. Align the schema, prompt and
   catalog policy around specific products/tools, retaining other useful
   technical information separately. The RT-RK retry also inferred Talentlyft
   from the embedded widget rather than an explicit textual product name.

4. **Scope and signal retention.** RT-RK's case-study technologies were mostly
   classified at company scope although the evidence describes a client's
   gateway project. Oxide's hardware components describe its offered product,
   which should not be flattened into its internal IT stack. On the original
   data job, Fabric/Databricks requirements remained, but separate planned-
   adoption observations disappeared. Current positive controls accept a
   requirement alone; passing them does not establish complete signal recall.
   NOVELIC's quality page returned offerings but no technology signals, so a
   passing negative about AURIX sales does not prove retention of AURIX expertise.

5. **Contact/application URLs.** One Handelsbanken form URL was fabricated by
   appending `Ansök nu` to the job URL, and assigned to the recruiting manager.
   The validator correctly held it. Memgraph's real open-application email was
   placed in `job_url`, triggering the HTTP-only validator. Preserve the job's
   page URL and application destination/method separately; use observed link
   references to construct destinations.

6. **A failed page is not a negative pass.** The auditor previously left an
   empty violations list for a completely failed response. It now reports
   evaluated/pass/total counts separately. The failed RT-RK attempt has three
   unavailable negatives, rather than three passes. Original frozen reports
   remain unchanged; the separate review applies corrected accounting.

## Next implementation

Keep one initial combined request. Add bounded, separately logged recovery for
malformed responses and targeted repair of records with evidence failures.
The repair receives the same page and the flagged records, preserves successful
records and link scores, and does not count unverified repairs as accepted facts.
Use source fragments with stable IDs and explicit native/rendered provenance;
leave ambiguous actor associations for review.

Tighten technology eligibility and preserve client/product scope, then expand
positive controls to catch lost planned-adoption and expertise signals. Freeze
the new prompt/controls before evaluating again. Only after that gate passes,
connect persistence/priority scheduling/final merge and run Handelsbanken from
its homepage to a declared stopping condition without guided follow-ups.

## Reproducibility

Saved under `page_agent_lab/data/`:

- `one-pass-v2-20260916`: seven-page frozen run;
- `one-pass-holdout-20260916`: six-page frozen run and retry dataset;
- `one-pass-holdout-retry-20260916`: independent RT-RK retry;
- `one-pass-review-20260916/review.json`: reviewed metrics, issues and gate decision;
- `one-pass-review-20260916/review.py`: offline verification/accounting script.

The verifier checked all input/control/frozen-code hashes and all 14 actual
request payloads. Every request used the identical instruction prefix, page-only
source, direct DeepSeek/high, no catalog tools and no truncation. It also verified
that all 146 files from the original pilot remain byte-identical to its archive,
and that none of the 171 new run files contains the configured API credential.

Seven lab tests passed, including custom fixture freezing, single-arm auditing,
tamper rejection and failure-aware negative accounting. Ruff formatting/lint
and type checks passed. Main runtime code was not changed; browser tests and
package rebuilds were unnecessary. No backend submission or deployment occurred.
The verified archive is recorded in [the receipt](ONE_PASS_V2_RECEIPT.json).
