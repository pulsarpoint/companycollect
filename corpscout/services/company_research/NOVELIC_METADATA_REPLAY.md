# NOVELIC technology validation replay — 8 September 2026

The package now separates source support from catalog metadata, adds
`advertised_expertise`, and repairs rejected draft categories/descriptions without
repeating HTML extraction. The final code is **0.10.2, output schema 1.8**.

The saved-source experiment recovered all **19 previously overstated engineering
observations** with the advertised-expertise signal and preserved all **28 accepted
job controls**. Automated gates now accept **28 of 32 previously metadata-blocked
observations**. This count includes quality problems identified below; it is not a
verified precision score or proof that all recovered catalog drafts are correct.

## Frozen inputs and results

The input is `data/novelic-autonomous-v17-completed/result.json`, SHA-256
`7c8d1b0d76896c683cf258f5f938a932fe70f584e2521dc11e4503964e9b2e4f`.
The replay pins its 7,981-entry catalog and unchanged Crawl4AI HTML. It selects 86
observations: both engineering pages, the 32 rejected proposals, and 28 previously
accepted job observations. These groups overlap. Expectations were written before
model calls. Seventy-nine records passed renewed quotation checks; seven remained
blocked without attempting quotation repair.

| Check | Previous result | Final replay |
|---|---:|---:|
| Correct advertised-expertise signal among the 19 known overstatements | 0/19 | 19/19 |
| Original signal and scope preserved for accepted job controls | 28/28 | 28/28 |
| Automated acceptance of the 32 metadata-rejection cases | 0/32 | 28/32 |
| Accepted engineering observations incorrectly labeled stated use | 19 | 0 |
| New crawler page fetches / HTML extractions | — | 0 / 0 |

The replay retains 123 record versions, including rejected originals and 37 linked
interpretation corrections. It has 78 source-supported records, of which 75 pass
all automated gates: 34 advertised expertise, 21 required experience, 19 preferred
experience, and one role-scoped stated-use observation. The latter is AWS in the
Data Engineer responsibility to design and maintain pipelines on AWS. This is not
an inference that every technology in a hiring requirement is deployed company-wide.

The separate main job, company, people and overview results were not regenerated.
This is a focused technology replay, not a new complete company crawl.

## What the experiment changed

1. **Separate gates.** Failed catalog resolution or proposal review leaves the
   source-supported observation intact. Accepted summaries and submission still
   require all applicable gates. Pending metadata is listed separately from pending
   extraction chunks. A successful resolution clears a stale catalog error.
2. **Source-only meaning review.** Technology review receives the fixed claim fields,
   quotations and saved source neighborhoods. It no longer receives the extraction
   model's generated context or proposed catalog definition, which can bias meaning.
   Examples explain governing skills-table headings and acceptance of correctly
   labeled hiring requirements. Signal corrections keep originals and unchanged quotes.
3. **Bounded metadata repair.** Description and category decisions are separate from
   name identity. One correction is the default; the name, source claim, website and
   licensing fields cannot change. A suitable free-text category is valid even when
   the catalog has no corresponding ID. Every changed draft is reviewed again.
4. **Review exact drafts.** Metadata approvals include a SHA-256 fingerprint. An
   unchanged accepted draft is skipped on resume. Rejected/failed reviews can be
   retried independently; an unchanged draft can also be reconsidered within the
   correction budget when its rejection was mistaken. Repair history survives merges.
5. **Backend enforcement.** The submission boundary accepts the new signal, checks
   required review statuses and rejects metadata changed since its review. An
   administrator still decides whether any proposed technology enters the catalog.

## Iterations, calls and cost

All model calls used `deepseek/deepseek-v4-flash-0731` through OpenRouter's Wafer
provider, temperature zero, reasoning disabled, a 120-second request deadline and
65,536 maximum output tokens. The experiment had a 60-call ceiling; it used 28.

| Saved phase | Additional calls | Additional reported cost | Result |
|---|---:|---:|---|
| `novelic-metadata-v18` / 0.10.0 | 18 | $0.0216774 | Recovered 27 blocked observations, but source review still confused some table rows and two requirements. |
| `novelic-metadata-v18-followup` / 0.10.1 | 7 | $0.0057315 | Rechecked 21 selected source-meaning failures; preserved all 28 controls and removed accepted engineering stated-use errors. |
| `novelic-metadata-v18-final` / 0.10.2 | 3 | $0.0011757 | Rechecked six remaining metadata failures; all 19 original expertise cases passed and 28/32 blocked observations passed automated gates. |

**Total reported cost: $0.0285846**, plus two attempts with unknown cost: one HTTP 429
and one 120-second deadline. There were 26 responses with reported usage, 225,371
input tokens and 32,638 output tokens. Elapsed time was about ten minutes, including
manual inspection and code adjustments between phases. This is not a controlled
latency benchmark or an estimate for a fresh company crawl.

Calls comprise 12 source reviews, 11 proposal reviews and five metadata-repair
attempts. No new catalog resolution was required because the saved records already
had matches/proposals. Protocol-boundary tests cover resolution-only recovery.
All three phases and their implementation snapshots remain available. The final
post-replay merge guard preserves repair history and clears an obsolete proposal
gate when a canonical match arrives; it was tested separately and was not invoked
by these saved-record harnesses.

## Manual audit: remaining problems

- **AutoPLANT's proposal has the wrong vendor.** The source supports advertised
  expertise with AutoPLANT, but its accepted draft calls it Autodesk software.
  AutoPLANT is a Bentley product. The administrator should correct the draft before
  approval. This external verification was performed after the replay and was never
  added to its model inputs. [Bentley's AutoPLANT documentation](https://bentleysystems.service-now.com/community?id=kb_article_view&sysparm_article=KB0101406).
- **“Cadence Design Flow Tools” is too generic.** This is an unspecified vendor tool
  collection, not a specific application. Its automated acceptance is a false
  positive under the agreed specificity policy. Hold the proposed identity; retain
  it as capability context unless a specific application can be sourced.
- **Simulink, Plan3D and E3D remain metadata-blocked for their expertise observations.**
  Some rejection reasons are inconsistent or overly strict. Simulink's job observation
  passes with a different draft, showing why metadata should eventually be reviewed
  once per proposed identity and exact draft, then reused across observations.
  E3D also needs care because its source abbreviation does not itself establish AVEVA.
- **Norasoft remains source-review blocked.** The reviewer describes the named NOVELIC
  perception software yet marks the identity non-specific. This is a likely false
  negative requiring a focused identity/relationship check; an offered product and
  an internally deployed stack should remain distinguishable.
- **Seven quotation failures remain unchanged.** This replay intentionally did not
  repair the missing/spelling-mismatched evidence for OpenFOAM, Ansys, Abacus,
  Autodesk Robot, Dlubal RSTAB/RFEM and one MATLAB observation.

No model output was silently replaced with manual judgments. Known quality holds
and remaining failures are in `manual-audit.json`; the unsubmitted preview is the
raw automated output and must be read with that audit. No database writes,
administrator decisions, deployment, new site crawl or PDF/OCR experiment occurred.

## Validation and next work

There are 96 Python tests: 93 regular tests passed, and three opt-in real browser tests
passed separately. The seven new stage-boundary regressions cover metadata-only
repair, exact draft hashes, identity rejection, invented category IDs, recovery after
an HTTP failure, expertise correction, canonical rematching and unchanged-draft
re-review. The backend suite has 36 passing tests. Ruff, ty, wheel/source builds and
`git diff --check` passed. The saved-data integrity audit found no unpermitted claim changes, changed quotations/HTML/proposal identities, or
accepted engineering stated-use errors.

The next focused task should address **proposal identity/description quality and
consistent reuse of metadata decisions**. In particular, distinguish a specific
product from an unnamed vendor tool collection, verify vendor assertions before
canonical approval, and prevent different observations of the same technology from
receiving conflicting drafts. After that, test these frozen rules on several other
companies before expanding crawling or deployment work.

Final artifacts: `data/novelic-metadata-v18-final/result.json`, `comparison.json`,
`manual-audit.json`, `selected-rechecks.json`, `calls/`, and `verification/`.
The replay and targeted follow-up harnesses are in `benchmarks/replay_technology_metadata.py`
and `benchmarks/recheck_technology_meaning.py`; the audit is reproducible with
`benchmarks/audit_technology_metadata.py`.
