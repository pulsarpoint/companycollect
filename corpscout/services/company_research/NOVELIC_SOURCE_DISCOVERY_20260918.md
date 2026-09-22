# NOVELIC source-discovery verification — 18 September 2026

Version: 0.30.0. Collection only; financial interpretation remains offline.

The crawler can now follow evidence-supported parent-company and filing sources,
plan bounded web searches from the caller's objective and captured source text,
and retain document links with their nearby labels. The live NOVELIC test reached
Sona Comstar. A subsequent model-selection replay and fresh page capture collected
four NOVELIC document references from the official subsidiary accounts index.

## What changed

- Parent navigation requires a relationship quotation found on the target's own
  website. Filing sources require supplied report/registry context. These are
  navigation hypotheses, not independently verified legal relationships.
- Approved source evidence follows child candidates into later model calls.
- Source domains, pages per domain and link depth have separate limits, within
  the existing global and external-page budgets.
- Equal-role/equal-potential candidates have an explicit model-assigned priority.
  A subsidiary accounts index can therefore precede general investor gateways.
- Source page/domain limits now report `source_page_budget` or
  `source_domain_budget`, with partial status rather than apparent exhaustion.
- Search plans use the objective, previous results and bounded excerpts from
  recently captured pages. Ordinary Brave result cards nominate sources; generated
  search answers are excluded. Queries, snippets, timestamps and failures survive
  JSON-only output. Search does not establish ownership or financial facts.
- Document references preserve source URL, row text, page title and heading where
  available. A generic `Download` label no longer loses the adjacent entity label.
- `--crawl full` enables search by default; targeted discovery uses `--web-search`.
  Explicit page lists and `--site-info` alone do not launch search.

The options are shared by CLI, REST and JetStream. See
[configuration and output](CRAWL_AND_ANALYZE.md#source-discovery) and
[financial request example](examples/financial-discovery-job.json).

## End-to-end discovery measurement

Artifacts: [result.json](data/novelic-financial-discovery-20260918-release/result.json),
[manifest](data/novelic-financial-discovery-20260918-release/crawl-manifest.json),
[queue](data/novelic-financial-discovery-20260918-release/queue.json).

This run used DeepSeek `deepseek-flash`, high reasoning, a financial-only objective,
24 total pages, 12 external pages, three search queries and four pages per approved
source. It ran before the final explicit-priority and inherited-evidence fixes.
The stored original result is unchanged.

| Measurement | Result |
| --- | ---: |
| Elapsed | 372.238 s (6 min 12 s) |
| Attempted pages | 18 |
| Captured pages | 15 |
| Failed pages / duplicate redirects | 1 / 2 |
| Model calls | 20 |
| Prompt tokens | 114,534 |
| Completion tokens | 67,332 |
| Total tokens | 181,866 |
| Search queries | 2 successful, 1 HTTP 429 |
| Discovered document references across sources | 106 |
| Final status | Partial |

The query refinement learned Sona Comstar's name from captured source text and
found official parent-company report PDFs and acquisition coverage. NOVELIC's
own India expansion announcement supplied the explicit quotation:
“in collaboration with its parent company Sona Comstar”. That allowed bounded
navigation into Sona's investor pages. A generic invitation to visit Sona's
website was correctly rejected as insufficient relationship evidence.

The subsidiary accounts index was discovered and assessed as highly useful, but
alphabetical tie-breaking selected broader investor pages first and exhausted
the four-page source limit. The original manifest says `no_matching_candidates`;
this was misleading and is corrected by the new source-budget stop reasons.
The final search was also rate-limited. These artifacts do not establish complete
coverage or the absence of other reports. The 106 references are a broad source
inventory, not 106 NOVELIC financial statements.

## Verification after the final fixes

Artifacts: [verification.json](data/novelic-source-priority-verification-20260918/verification.json),
[fresh capture result](data/novelic-source-priority-verification-20260918/capture/result.json),
[replay script](data/novelic-source-priority-verification-20260918/replay.py).

This was a focused replay, not a second fresh end-to-end crawl: twelve previously
useful Sona candidates were re-ranked with the real model, using the recorded
source approval and one remaining source-page slot. The model selected the
[subsidiary accounts index](https://sonacomstar.com/investor/subsidiary-companies-financial-statements)
at priority 95, ahead of the general investor gateway (60) and annual reports (50).
The crawler then fetched that chosen page afresh using its explicit-page mode.

| Measurement | Result |
| --- | ---: |
| Selection time | 11.316 s |
| Fresh capture time | 5.816 s |
| Total verification time | 17.145 s |
| Model calls | 1 |
| Prompt / completion tokens | 4,712 / 2,513 |
| Total tokens | 7,225 |
| NOVELIC financial document references | 4 |
| Model errors / capture status | None / finished |

The capture retained these source labels and URLs:

| Source row label | Document reference |
| --- | --- |
| Novelic d.o.o Consolidated Financials. | [Serbia consolidated financials](https://sonacomstar.com/files/documents/novelic-serbia-document-wMpyMY.pdf) |
| Novelic d.o.o Beograd. | [Beograd report, first entry](https://sonacomstar.com/files/documents/novelic-d-o-o-beograd--document-aWNkDI.pdf) |
| Novelic d.o.o.Beograd | [Beograd report, second entry](https://sonacomstar.com/files/documents/novelic-d-o-o-beograd-document-eCZdgN.pdf) |
| Novelic India Private Limited-2026 | [Separate Indian entity](https://sonacomstar.com/files/documents/novelic-india-private-limited-2026-document-PLmLJm.pdf) |

PDF bodies were not downloaded or analyzed in this verification. Reporting periods,
amounts, currencies and legal-entity matching remain for the offline processor.
One link matched surrounding financial wording; the other three matched the
financial page title while retaining their distinct row labels.

## Validation and limits

The automated suite covers source evidence, inherited approval, selection priority,
source budgets, search parsing/failure handling, JSON-only provenance, document row
context, explicit-page behavior and site-info-only behavior. Real local NATS tests
exercise service delivery. The final suite ran 277 tests in 77.322 seconds:
264 passed and 13 optional checks were skipped. Log: `/tmp/company-discovery-tests-release.log`.

Ruff, type checking, lockfile validation and `git diff --check` pass. No dependencies
were added for search. The browser search path is suitable for this local prototype;
its observed HTTP 429 means it needs a reliable search-provider integration before
production-scale use. Search failures remain visible and do not discard captures.

These measurements are financial discovery tests, not a like-for-like comparison
with the previous all-sections full crawl. The final priority/evidence changes have
been verified through replay plus a fresh destination fetch; no new full-crawl time
or token total is claimed. The provider returned no dollar cost, so token counts
are reported without a cost estimate.
