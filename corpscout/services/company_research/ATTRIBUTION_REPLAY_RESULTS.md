# Attribution, service relationships and evidence replay

Package 0.14.3, optional output schema `page-statements/1.3`; regular research schema
remains 1.9. This is a targeted replay of saved failures, not another website crawl
or an independent verification of company facts.

## Changes

- Description review reconstructs the actor, actor kind and named item separately
  from its supported/unsupported verdict. Code compares those fields to the claim.
  Correct prose cannot compensate for a tool stored as the company.
- A bounded correction can repair the actor or narrow a service label to an exact
  source technology span. It needs source quotations and a separate review. Original
  fields and sources remain in revision history; job and section stay fixed.
  Unchanged heading/job anchors are carried forward and checked in the same source
  window. If proposed quotations omit words, a separate evidence-only request repairs
  fragments for the fixed candidate. The original remains unchanged until the
  fragments pass; the corrected description then still needs source review.
- Normalization cannot promote a person, product or service into a company technology
  observation. Technology source review separately checks company/employer, signal
  and scope. Loading a previously accepted technology with a required meaning review
  now also requires those reconstructed fields; old verdicts need revalidation.
- Prompts distinguish offering software itself from offering development/integration
  services using it. Rails development can support expertise; building Android apps
  can support use in client development. Neither means selling Rails or Android.
  Client workflow work is not evidence of the provider's own internal deployment.
- Evidence matching tolerates typographic versus straight quotation marks alongside
  existing case/whitespace/HTML-entity normalization. Saved HTML and quoted strings
  remain unchanged. It does not remove intervening words or ignore negation.
  Missing company evidence still needs checked fragments and source attribution.
- Results expose extraction status per page and `pending_extraction_page_ids`.
  No generated statement IDs does not imply that a failed page was fully examined.
- Strict review schemas require all object fields, including nullable ones. Duplicate
  JSON keys are rejected rather than silently letting the last value win.

Description approvals are bound to exact fields and sources and require review version
2. The optional statement workflow remains separate from direct crawl navigation.

## Reproducible inputs

The [fresh benchmark](FRESH_COMPANY_BENCHMARK.md) supplies 16 DMC descriptions
(all 14 earlier accepted observations with wrong actors, plus React and Next.js),
10 thoughtbot descriptions representing its 17 earlier accepted observations,
and four held Plausible records: founded year, employee count and two cofounders.
The saved NOVELIC MCAP description checks preservation of use plus preferred experience.
That is 31 selected input records. Expected controls are never sent to the model.

The harness copies and hash-checks native Crawl4AI HTML, reuses the frozen technology
catalog, and writes original inputs, prompts, responses, review/correction history and
final records. It performs zero page fetches, zero new page extraction passes, and no
database writes, proposal submissions or deployment. These selected records do not
measure complete company coverage, navigation quality or all jobs.

```sh
.venv/bin/python benchmarks/replay_attribution_failures.py
.venv/bin/python benchmarks/audit_attribution_replay.py \
  data/attribution-replay-v0141
```

Model: `deepseek/deepseek-v4-flash-0731`, Wafer, low reasoning, 65,536 output-token
ceiling, 120-second total deadline per request. Maximum calls per case: DMC 40,
thoughtbot 28, Plausible 6, MCAP 8; two cases run at a time. Missing provider cost is
reported as unknown, never zero.

The preceding 0.14.0 pilot in `data/attribution-replay-v014/` was interrupted after
repeated optional attribution fields and missing review items appeared. It is retained
as a failed pilot, with its own costs. Its nullable fields were made required for the
0.14.1 replay. The first pilot's outputs must not be promoted.

## Results and manual audit

Start with [the audit summary](data/attribution-audit-v0143/summary.json) and its
per-case JSON files. These are review artifacts, not backend submissions. They retain
catalog-pending records and manual holds separately from accepted records. No proposal
has been approved by an administrator or submitted to the database.

| Case | Result |
| --- | --- |
| DMC | Nine observations now name DMC as the company and describe advertised expertise. Seven remain after manual identity review; AVEVA and Beckhoff Motion Control are held as broad/ambiguous source labels. Seven of the 16 selected descriptions still need recovery. |
| thoughtbot | All ten descriptions passed. The five earlier incorrect `offers` relationships disappeared. Fourteen observations passed source review; ten passed catalog/proposal checks. Hotwire (two observations), iOS and Android remain catalog/proposal-pending. |
| Plausible | All four selected records recovered: since 2018, team of 10, Uku and Marko as cofounders. Company facts passed meaning review; people passed quotation checks and manual inspection of the saved homepage. Full names were not inferred. |
| MCAP | Role use passed, but preferred experience was omitted. The earlier two-relationship success did not repeat. This remains a completeness failure. |

The thoughtbot replay retained 11 of 12 previously manually retained relationships
before catalog checks, and eight after them. One Rails past-use observation was
omitted while expertise remained. Three additional expertise observations explain
why 14 source-reviewed observations do not mean all previous observations survived.

DMC normalization initially invented eight `explicitly_not_used` claims from positive
service listings. Source review rejected them and corrected their signals to expertise.
No unsupported negative or technology-sale claim remains in the audited accepted
outputs. This demonstrates the value of source review, and also that prompt wording
alone does not guarantee correct relationship classification.

The complete four-case run is frozen in `data/attribution-replay-v0141/`. DMC recovery
with unchanged heading anchors is frozen in `data/attribution-recovery-v0142/`.
The final five-case quotation recovery is in `data/attribution-recovery-v0143/`: five
saved correction proposals, ten new model calls, all five descriptions and source
claims passed; AVEVA is still manually held for identity specificity. The final audit
uses those five latest records in place of the same statements from the earlier DMC
recovery. It does not choose whichever version gives a better score.

```sh
.venv/bin/python benchmarks/recover_attribution_corrections.py \
  --statement-id 2fae0da1f9b5f0683522add6 \
  --statement-id 17e18b214f987ee7f476a18e \
  --statement-id 7850c52260c7477420fe7e74 \
  --statement-id 4176dc28db94a2961766b5fd \
  --statement-id a5cc2d09264b48485864cfe6
.venv/bin/python benchmarks/audit_attribution_recoveries.py
```

| Phase | Calls | Reported cost | Calls with unknown cost |
| --- | ---: | ---: | ---: |
| Interrupted 0.14.0 pilot | 12 | $0.01602260 | 5 |
| Four-case 0.14.1 replay | 26 | $0.03606235 | 6 |
| DMC 0.14.2 recovery | 14 | $0.02285105 | 2 |
| Focused 0.14.3 quotation recovery | 10 | $0.01678615 | 0 |
| Total for this work | 62 | $0.09172215 | 13 |

These are overlapping development/recovery experiments, not a per-company production
cost estimate. Unknown costs include six deadlines, five HTTP 429 responses and two
interrupted requests. All runs have ended. Source HTML hashes match their manifests;
the final focused run's source implementation matches package 0.14.3.

Validation: 130 regular tests and three browser tests pass; Ruff, source-package and
new-harness type checks, diff whitespace checks, and wheel/source builds pass. Historical
broad benchmark/test type diagnostics are outside the source-package check.

## Remaining work

Keep this workflow optional. The next bounded test should verify relationship
completeness against reviewed statements, starting with the missed MCAP preference
and Rails relationship. Recover only pending DMC descriptions and catalog decisions;
do not repeat the five-site crawl. Vendor/category labels still need stricter identity
review. Missing descriptions and transport failures must not be reported as “not found.”
Fresh job-detail attribution, ownership and financial-document discovery still need
their own coverage test; this replay did not exercise them.
