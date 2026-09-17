# Package verification — 6 September 2026

The combined package was exercised with a real website and real OpenRouter requests.
This is a functional smoke test, not a recall benchmark or a claim that all website
information was collected.

## Final live run

```sh
.venv/bin/python -m company_research https://www.kongsberg.com/ \
  --max-pages 5 --max-model-calls 20 \
  --env-file .env \
  --output-dir data/kongsberg-v2 \
  > data/kongsberg-v2.stdout.json
```

Local artifacts: [result.json](data/kongsberg-v2/result.json),
[page queue](data/kongsberg-v2/queue.json),
[configuration](data/kongsberg-v2/settings.json). The ignored `data/` directory
contains HTML snapshots, request/response artifacts and validation details.

The controller selected the home page, contact page, management page, vacancies
page and about page. Every extracted HTML window received the same seven-objective
prompt. Native Crawl4AI `cleaned_html` was stored unchanged and passed as exact,
overlapping source slices; no job-specific selectors or HTML simplification were used.

| Objective | Records | Source matched | Needs review |
|---|---:|---:|---:|
| Company profile | 12 | 12 | 0 |
| Business contacts | 7 | 6 | 1 |
| Locations | 3 | 2 | 1 |
| Products/services | 13 | 13 | 0 |
| People | 10 | 10 | 0 |
| Company relationships | 2 | 1 | 1 |
| Jobs | 55 | 54 | 1 |
| **Total** | **102** | **98** | **4** |

The 55 job records have distinct job URLs. One job's URL/evidence did not match the
snapshot, so its data remains explicitly reviewable. Source matching checks textual
presence and required anchors/URLs; these counts do not establish semantic accuracy
or exhaustive recall.

- Run status: **partial**, stopped at the five-page budget.
- Four pages fully extracted; one of two contact-page windows timed out.
- Ten model HTTP attempts: seven responses and three 180-second timeouts
  (one extraction and two link assessments).
- Reported cost: **$0.010706**, excluding the three calls with unknown usage/cost.
- Elapsed: **15 minutes 38 seconds**. Provider latency is a material limitation.
- All seven objectives have findings; promising unvisited links remain.

All 105 record-to-source references were independently checked against snapshot
hashes, source URLs, window boundaries and reproducible evidence-check results.
Saved extraction inputs were verified to be exact slices of their snapshots.
The final stdout JSON equals saved `result.json` and validates as `ResearchResult`.

## Checks and changes prompted by the smoke tests

The earlier four-page run is preserved at `data/kongsberg-v1/`. It exposed missing
propagation of link-assessment errors into the run status. That was fixed before
the final run: failed assessments now make the run partial and remain visible in
the queue and report. The earlier artifact's `finished` status predates that fix.

Selection batches were reduced from 40 to 20 candidates. This did **not** eliminate
provider timeouts; both runs recorded three timeouts. Failed requests do not become
negative facts, and their unknown charges are not reported as zero.

Automated verification covers evidence fragments with intervening labels/dates,
overlapping windows, relative URLs, record/source merging, objective balance,
external scope, sitemap indexes/gzip/images, malformed model responses, request
budgets, partial extraction, failed link assessment and refreshed link metadata.
The real-browser integration test also verifies JavaScript rendering, sitemap-driven
selection, a failed fetch, secondary contacts on a jobs page, scoped negatives and
missing-information statuses. Only its model HTTP boundary is replaced.

Ruff and ty pass. The wheel builds and imports from an unrelated working directory,
with the public Python API and module CLI available independently of the benchmark
and example modules.

This initial package does not resume interrupted runs, interact with load-more
controls, or extract PDFs. Successful source checks do not replace semantic review;
finite budgets and provider failures can leave collections incomplete.
