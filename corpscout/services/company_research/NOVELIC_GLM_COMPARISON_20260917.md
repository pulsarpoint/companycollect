# NOVELIC: GLM 5.3 Flash versus DeepSeek Flash

GLM low completed the same 26 saved pages for **$0.623 in 24m22s**, compared with
the DeepSeek high baseline's **estimated $2.812 in 54m56s**. It recovered every
checked job and management name and avoided the selected cookie-vendor and
data-format errors. It introduced different technology-normalization errors,
and both results remain partial. The runtime default is still DeepSeek.

This is a **model-and-settings comparison**, not proof that one model is better
at equal reasoning effort. Initial GLM high attempts encountered provider rate
limits and request timeouts; the completed corpus run used low reasoning and a
longer deadline. The detailed [JSON comparison](NOVELIC_GLM_COMPARISON_20260917.json)
contains metrics, qualifications, source hashes and artifact paths.

## Conditions held fixed and changed

All 26 initial prompt-message pairs match the baseline exactly. Both runs use the
same native Crawl4AI cleaned HTML, rendered evidence, schemas in the system prompt,
host validation, source controls and pinned catalog. Core collection/classification
code is unchanged. Output allowance is 65,536 tokens, page concurrency is three,
and classification batches run sequentially. These are application settings, not
claims about a model's native limits.

| Setting | DeepSeek baseline | Completed GLM run |
|---|---|---|
| Model | `deepseek-flash` | `z-ai/glm-5.3-flash` |
| API/provider | Direct DeepSeek | OpenRouter, Parasail FP8 only |
| Reasoning | High | Low |
| Request deadline | 300 seconds | 900 seconds |
| Temperature | Omitted in thinking mode | 0 |
| JSON mode | JSON object + host schema validation | Same |

Model/provider metadata was saved before the experiment from
[OpenRouter](https://openrouter.ai/z-ai/glm-5.3-flash). No pages were recrawled,
PDFs opened, or RustFS/database writes performed. This does not test the first-page
eligibility gate, autonomous discovery or complete-site coverage.

## Usage and runtime

| Measure | DeepSeek high | GLM low |
|---|---:|---:|
| Calls, including retries | 108 | 78 |
| Input tokens | 4,729,866 | 3,617,798 |
| Output tokens | 1,224,883 | 240,591 |
| Total tokens | 5,954,749 | 3,858,389 |
| Reasoning tokens, already included in output | 897,288 | 21,401 |
| Cost | $2.81205 estimated | $0.62288 provider-reported |
| Run-manifest elapsed time | 54m56s | 24m22s |

For these configurations, GLM used **35.2% fewer tokens**, cost **77.8% less**, and
finished **55.6% sooner**. Both runs returned usage for every call. The GLM total
includes two page retries, seven classifier retries and one invalid-JSON response.
Its 43 classifier batches all eventually passed schema checks; nine individual
decisions were subsequently rejected for source-reference problems, one decision
was missing, and one explicitly requested review.

DeepSeek pricing is the dated peak-period estimate documented in the
[baseline report](NOVELIC_SCAN_20260917.md); GLM uses actual returned OpenRouter
charges. Prices, reasoning settings and caching differ. At the captured rates with
all input hypothetically uncached, totals would be $2.88882 and $0.66297 respectively.
That calculation is a comparison aid, not an invoice or a future-price guarantee.

## Selected source checks

| Check | DeepSeek | GLM |
|---|---:|---:|
| Job detail titles recovered | 16/16 | 16/16 |
| Job detail records passing evidence validation | 12/16 | 14/16 |
| Management names/roles recovered on source review | 16/16 | 16/16 |
| Management records passing evidence validation | 0/16 | 4/16 |
| Independent technology names recovered | 31/32 | 28/32 |
| Original strict relationship/scope controls | 27/32 | 27/32 |
| After explicit review of supported narrower scopes | 31/32 | 28/32 |
| Unsupported NOVELIC use claims from checked cookie vendors | 4/28 | 0/32 |
| Selected generic/format false positives | 3 | 0 |
| Raw technology mentions needing final review | 43/407 | 11/316 |
| Link occurrences assessed / distinct URLs | 1,245 / 158 | 1,245 / 158 |

These are selected development controls, not exhaustive precision/recall. A
classified mention can be excluded technical context or an unattributed vendor;
the 140 GLM `specific_technology` decisions are not 140 company technologies.
All saved captures and indexed source sections passed hash checks. Link-score
quality was not independently evaluated.

**Technology differences matter.** GLM recovered Autodesk Robot, which DeepSeek's
whole-page retry dropped from the selected final mentions. GLM also retained all
four names in `VHDL/Verilog` and `Matlab/Simulink`, but as two combined labels.
Thus all 32 checked names appear in its text, while only 28 have independent
entries. It incorrectly classified VHDL/Verilog as generic context despite the
prompt explicitly allowing named languages. Matlab/Simulink was eligible but
remained a combined identity. Both combined catalog queries were ambiguous;
unrelated fuzzy suggestions were retained without accepting them as identities.

GLM's CATIA team-expertise relationship is supported by the source's mechanical
engineering team description. That manual review adds one valid scope variant to
its strict score. DeepSeek's four previously reviewed scope variants remain valid.
The original controls and model outputs were not changed to improve scores.

GLM correctly excluded Protobuf, FlatBuffers and MCAP from specific technologies.
All 16 job pages avoided promoting candidate standards to company certifications.
It retained the Sona Comstar subsidiary relationship without inventing an ownership
percentage, ISO 9001:2015 and ISO 14001:2015 certificate links, IATF 16949 as
`working_toward`, and the radar case-study PDF link. These remain website claims;
document contents were not examined.

**Evidence validation is still a major weakness.** Across all nontechnology
objectives, only 133/520 GLM source records pass, versus 220/680 for DeepSeek.
Service records are particularly affected: 3/287 versus 54/406 pass. Many holds
concern company attribution in quotations, and repeated menu/service records make
counts larger than the number of distinct offerings. A hold does not establish
that the underlying fact is false. Both outputs preserve records and reasons for
review. GLM's better job-detail score does not mean all its evidence handling is
better; most career-listing records are held.

## High-reasoning and provider diagnostics

The failed attempts remain separate from the completed low-reasoning run:

- Baseten: five HTTP 429 responses; a diagnostic repeat confirmed a shared upstream
  capacity limit. Wafer: four HTTP 429 responses and one 300-second timeout.
- Automatic provider routing: five full-page requests timed out at 300 seconds;
  the failure cutoff marked the other pages unavailable without model requests.
- A minimal availability request completed on Parasail in about one second.
- The full About-page high-reasoning probe completed in **450.933 seconds** on
  Parasail, with 127,737 input and 18,830 output tokens. JSON parsed, but one
  certificate used `subject` instead of required `subject_name`.
- The same About-page prompt at low reasoning completed in **212.171 seconds**,
  with 127,737 input and 8,796 output tokens, and passed schema validation.
- An isolated short job at high reasoning completed collection and classification
  in two calls for $0.00961, with the correct title/employer/location and a
  source-supported hybrid-work option.

There were **21 preliminary/diagnostic calls**, with $0.04683 in known charges and
16 calls without returned usage/cost. Known charges including the completed corpus
run are $0.66971; the total experiment charge is not fully known. Failed calls must
not be treated as free. The long high-reasoning probe was more heavily input-cached
than the low probe, so its lower charge is not evidence that high reasoning is
cheaper. These pilots do not establish a general high-versus-low quality ranking.

## Next changes to test

1. Require one technology per mention, with explicit slash-separated examples and
   named-language eligibility examples, preserving the original source phrase.
2. Send schema errors into classifier retries. Currently only page/source-name
   retries receive corrective feedback; classifier retries repeat the prompt.
3. Test provider-enforced JSON Schema separately for GLM. Both models used JSON
   object mode here to keep schema delivery identical.
4. Repair only invalid evidence/mentions while preserving valid records, and reduce
   repeated source context. Then compare GLM low against DeepSeek low on these same
   frozen controls before changing the default.

## Artifacts and validation

- [Complete GLM JSON](data/novelic-glm-low-20260917/result.json)
- [Quality audit](data/novelic-glm-low-20260917/quality-audit.json)
- [Explicit manual reviews](data/novelic-glm-low-20260917/manual-review.json)
- [Prompt and code parity](data/novelic-glm-low-20260917/prompt-parity.json)
- [Exact run settings and command](data/novelic-glm-low-20260917/experiment.json)
- [Source-reviewed short-job comparison](data/novelic-glm-job-probe-20260917/comparison.json)

The `novelic-glm*20260917` data directories retain provider metadata, captures,
requests, responses, failures and code snapshots. The JSON-only runner now supports
explicit API/model/provider/reasoning/deadline selection. Its default is unchanged.

Validation: 173 package tests, six skipped; Ruff passed; runtime and changed-file
type checks passed. The reusable audit reproduces the frozen DeepSeek control
results and cost before being applied to GLM. Broader pre-existing benchmark/test
type diagnostics described in the checkpoint remain outside this change.
