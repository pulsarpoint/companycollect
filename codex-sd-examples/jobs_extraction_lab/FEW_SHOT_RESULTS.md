# Teaching Liquid to separate job fields

The main extraction prompt has now also been rewritten, at the user's request.
The latest tested configuration combines those expanded explanations with the six
shorter examples. It returned all 24 observations with no literal-source flags,
but title agreement was 13/24 versus 16/24 for the earlier examples-only version.
See the rewritten-prompt results below; longer explanations did not consistently
improve semantic extraction.

Six synthetic examples are in [prompt_examples.md](prompt_examples.md). They teach
the existing JSON schema using title/metadata boundaries, duplicate titles at
different URLs, remote locations, concatenated employment/city labels, a title
within a longer description, and a chunk with no identifiable opening. This is
prompting at inference time, not model fine-tuning.

The shorter first example set is supplied with the revised base prompt. In this development
pilot it improved title separation and retained every reference opening. An
expanded revision improved agreement on complete records but reduced title
agreement and missed an opening. Neither version is reliable enough to replace
validation or establish extraction accuracy on unseen sites.

## Test setup

The exact model was `liquid/lfm-2.5-2.6b:free` through the existing OpenRouter
extractor. Seven frozen Markdown windows from six boards were selected before the
new requests, using previously inspected difficult cases. These windows contain
24 job observations and 23 distinct job URLs: one Calendly opening appears in two
overlapping windows. They are development cases, not a held-out evaluation set.

The synthetic prompt examples use different job titles, locations, and URLs from
the pilot inputs. The new runs used the same schema, base instructions, temperature
0, required reasoning settings, and 8,192-token output limit as the original
segmented run. Only the teaching examples and their prompt delimiters changed.
Each new variant made seven requests, all successful without retries. No new
downloads or Codex requests were made.

The reference is the existing whole-page Codex output, restricted to job URLs
present in each window. Title boundaries were checked against the saved Markdown.
Other field scores measure agreement with Codex, not independently labeled truth.
Matching uses the existing comparator: normalized URL first, then exact normalized
title and location. The latter identifies four CircleCI records whose URLs lost
query parameters in the first revision; those URLs still count as disagreements.

## Results

Counts use all 24 reference observations, including missing jobs. Field comparison
normalizes case and whitespace. Full-record agreement covers title, location,
department, employment type, workplace type, and URL; evidence is checked separately.

| Measure | Original prompt | Six examples, v1 | Expanded examples, v2 |
| --- | ---: | ---: | ---: |
| Successful windows | 7/7 | 7/7 | 7/7 |
| Reference jobs found | 24/24 | 24/24 | 23/24 |
| Title agreement | 3/24 | 16/24 | 13/24 |
| Location agreement | 17/24 | 23/24 | 19/24 |
| Department agreement | 20/24 | 20/24 | 20/24 |
| Employment type agreement | 21/24 | 24/24 | 23/24 |
| Workplace type agreement | 22/24 | 24/24 | 22/24 |
| URL agreement | 24/24 | 20/24 | 23/24 |
| All six fields agree | 3/24 | 8/24 | 11/24 |
| Literal-source validation flags | 4 | 6 | 4 |
| Input tokens | 6,484 | 17,061 | 19,273 |
| Output tokens, including reasoning | 20,817 | 20,475 | 24,767 |
| Summed request seconds | 96.6 | 106.7 | 108.8 |

All three runs reported zero OpenRouter charge. Summed request duration is not
elapsed wall time. Example text is repeated for every chunk, so the additional
input cost matters when scaling even if this endpoint currently reports no charge.

| Window | Reference observations | Original titles | v1 titles | v2 titles |
| --- | ---: | ---: | ---: | ---: |
| `ashby-zed--w001` | 2 | 0 | 1 | 1 |
| `ashby-steel--w001` | 2 | 0 | 0 | 0 |
| `ashby-hedra--w001` | 4 | 3 | 4 | 4 |
| `greenhouse-circleci--w001` | 4 | 0 | 4 | 4 |
| `greenhouse-calendly--w001` | 4 | 0 | 0 | 0 |
| `greenhouse-calendly--w002` | 4 | 0 | 4 | 4 |
| `lever-finn--w002` | 4 | 0 | 3 | 0 |

## What improved, and what remains

- CircleCI: `Software Engineer  Toronto, Ontario` became title `Software Engineer`
  with location `Toronto, Ontario`. However, v1 dropped the `gh_jid` query parameter
  from all four URLs. V2 preserved those URLs but assigned unsupported remote work
  to one opening.
- Hedra: both revisions separated all four titles and preserved missing workplace
  values as `null`, despite the page's `On-site` filter option.
- Steel: both revisions still appended the department to both titles, such as
  `Member of Technical Staff - Agents Engineering`. V1 also reconstructed Markdown
  evidence with missing spaces, causing two source-quotation flags.
- Calendly: the same overlapping `Staff Platform Engineer` opening had a clean
  title in window 2 and retained `Remote - US` in its title in window 1. Even with
  examples, chunk context changes the answer. Overlap still requires conflict
  handling during merge.
- FINN: v1 removed useful `Growth / E-Commerce` text from one title. V2 retained
  metadata in three titles and omitted `(Junior) Account Manager (m/w/d)` entirely.
  The window also lacks the parent `Core Functions` group for its first jobs;
  Codex saw that group in the full page. Its department disagreement with `Tech`
  cannot be treated simply as a model error. The example's nearest-group policy
  also differs from selecting the outer department in a nested hierarchy.

The expanded revision added a longer role example, a plain remote-location example,
query-parameter preservation, and a speciality after `(m/f/x)`. Its mixed result
shows why adding examples needs measurement. There was only one new pass per
variant; earlier repeat tests already showed variation at temperature 0. These
observations do not establish a repeatable improvement of this size.

For subsequent experiments, a useful hypothesis is to send only the examples that
match the chunk's layout, and preserve title/metadata boundaries from the rendered
HTML before generating Markdown. That needs its own test. A title-plus-description
example is included, but this pilot evaluated job-list windows rather than full
job-detail pages. An unidentified continuation chunk should be deferred for overlap
or context recovery; an empty extraction does not prove that the source has no job.

## Artifacts and replay

- [Detailed comparison](data/few-shot-pilot-v1/few-shot-comparison.json): settings,
  counts, individual field differences, missing jobs, and source-validation flags.
- [Pilot manifest](data/few-shot-pilot-v1/manifest.json): selected windows and hashes;
  Markdown files are copies of the original segmented inputs.
- Original results: `data/segmented-v1/runs/liquid-windows-v1/openrouter/`.
- New results: `data/few-shot-pilot-v1/runs/liquid-examples-v1/openrouter/` and
  `data/few-shot-pilot-v1/runs/liquid-examples-v2/openrouter/`.
- Exact prompt snapshots: `data/few-shot-pilot-v1/prompt-examples-v1.md` and
  `prompt-examples-v2.md`; each run's `settings.json` also stores its full examples.

## Rewritten main prompt: v3

The instructions in [extract.py](extract.py) now explain:

- How Markdown conversion flattens a title and its metadata into one link label.
- Why a title is part of the posting but should be a separate output field from its
  description, location, department, and work types.
- Which role qualifiers to preserve, how department headings apply, and why filter
  options must not supply job attributes.
- How to process overlapping fragments without omitting repeated jobs, inventing
  titles for unidentified continuations, or assuming missing context.
- How to preserve complete location labels, exact URLs, explicit nulls, and source
  quotations, then check completeness before returning JSON.

The run `liquid-explained-prompt-v3` used these instructions and the unchanged shorter
v1 examples on the same seven windows. Every other recorded model setting and each
Markdown hash matched the examples-only v1 run. The new instructions apply to both
backends, including runs that omit the optional examples file.

| Measure | Examples-only v1 | Rewritten instructions + v1 examples |
| --- | ---: | ---: |
| Successful windows | 7/7 | 7/7 |
| Reference jobs found | 24/24 | 24/24 |
| Title agreement | 16/24 | 13/24 |
| Location agreement | 23/24 | 22/24 |
| Department agreement | 20/24 | 17/24 |
| Employment type agreement | 24/24 | 24/24 |
| Workplace type agreement | 24/24 | 23/24 |
| URL agreement | 20/24 | 24/24 |
| All six fields agree | 8/24 | 7/24 |
| Literal-source validation flags | 6 | 0 |
| Input tokens | 17,061 | 22,353 |
| Output tokens, including reasoning | 20,475 | 22,545 |
| Summed request seconds | 106.7 | 100.5 |

The rewritten prompt fixed URL copying in this pass, and both Zed titles matched
the reference. It still appended metadata to Steel and Calendly titles. CircleCI
had correct titles but three jobs received the next section's department; FINN
still lost a role speciality and assigned the wrong group to the final job. There
were no retries, missing jobs, extra jobs, or reported OpenRouter charges.

Zero source flags do not mean zero extraction errors: a wrong department or an
overlong title can still occur literally somewhere in the input. All semantic
scores retain the same development-set and Codex-reference limitations described
above. A single pass does not establish that the prompt caused these differences.

The [detailed v3 comparison](data/few-shot-pilot-v1/explained-prompt-comparison.json)
stores the full instructions, scores, and disagreements. Raw outcomes are in
`data/few-shot-pilot-v1/runs/liquid-explained-prompt-v3/openrouter/`; an instruction
snapshot is in `data/few-shot-pilot-v1/extraction-instructions-v3.txt`.

The README command now uses the rewritten base instructions with the supplied v1
examples. The examples-only v1/v2 runs above retain their earlier instructions in
`settings.json`; they cannot be resumed using the changed base prompt. Their full
settings and responses remain available for inspection. Use fresh run IDs to make
another measurement. Raw artifacts remain in ignored `data/`; source examples and
this report can be reviewed separately.

The optional examples flag adds demonstrations without changing the base prompt's
actual input payload. Saved runs cannot be reused after instruction or example edits.
Tests validate every demonstration against the schema and its source text, check
prompt construction and changed-example cache rejection, and reject paired-backend
comparisons with different examples.
