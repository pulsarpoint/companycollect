# cc-enrich-worker `tech` mode — full analysis & code review (2026-07-02)

Pre-run review of the technology-collection path (`cc-enrich-worker tech`), requested before starting the
~300-part tech pass. **Report only — no code was changed.** Every finding was verified against the source
(`file:line` cited); the false-positive claims were tested empirically against the actual regexes.

---

## Verdict

The pipeline is **structurally sound and safe to run** — clean produce/load split, disciplined concurrency
(no data races found), a well-designed FetchChunk/Finalize seam, and the Aho-Corasick fast matcher's
"never miss vs upstream" claim **holds for HTML/scriptSrc/header/meta** (verified against upstream
wappalyzergo v0.2.86). The real risks for a big run are **failure semantics** (an all-fetches-fail part
still "succeeds" and is permanently marked done) and **data quality** (email/LEI/VAT false-positive floods,
identifiers extracted from the capped body). None are blockers; C1 + the extractor caps are the two worth
fixing *before* burning 300 parts of compute.

---

## How tech mode works (end-to-end)

1. **Worklist** (`index-builder/worklist.py:48-86`): per part, DuckDB selects `fetch_status=200` HTML pages
   from the CC columnar index, ranks per domain (homepage → legal/contact/about → shallowest), keeps
   `rn <= 25`. ⚠️ No outer `ORDER BY` — domain-row contiguity in the shard is an accident, not a contract.
2. **Orchestration** (`cc-crawl/main.go:209-255`): per part — skip on `.loaded` marker; build shard if
   absent; `RemoveAll` out dir; run the worker; gate on exit 0 + `domains.parquet` existing; `load --dir`;
   touch the marker.
3. **Worker main** (`cmd/cc-enrich-worker/main.go:256-456`): empty-out-dir rule; FastMatcher built
   (`main.go:300-314`); worklist read, first row/domain marked `Primary`; chunked loop — `FetchChunk` for
   chunk N while chunk N+1 prefetches; all rows accumulate in memory; parquets written once at the end.
4. **FetchChunk** (`worker.go:166-280`): one goroutine per domain (semaphore `--concurrency`); pages fetched
   **sequentially** per domain, 30 s timeout each. Per page: WARC range-fetch + gzip + HTTP parse →
   `DetectTech(headers, body[:tech-max-bytes])` → email regex over the **full** body →
   `ExtractProfile`/`ExtractLEIs`/`ExtractVATs` over the **capped** body. Dedup per domain (tech by name,
   ids by value, emails by string). First surviving page = `primaryURL`. Fetch errors `continue` (atomic
   counter only).
5. **Finalize** (`worker.go:284-408`): per surviving domain — 1 `DomainRow`, N `TechRow` (Confidence
   hardcoded 100), N `IdentifierRow`, ≤1 `MetadataRow` (first non-empty page profile), contact rows — all
   keyed to `primaryURL` as `url` and `source_url`.
6. **Fast matcher** (`internal/tech/techfast.go`): longest required literal per HTML/scriptSrc pattern
   (≥4 runes) → Aho-Corasick dictionary gates which regexes run; headers/cookies/meta evaluated directly;
   implies applied transitively.
7. **Load** (`load.go:153-191`): inserts whichever `<kind>.parquet` files exist; missing kinds silently
   skipped.

---

## Findings

### Critical

- **C1 — A part where every fetch fails still "succeeds", is loaded, and is permanently marked `.loaded`.**
  `FetchChunk` drops failed pages with `continue` (`worker.go:205-212`); no error propagates. If S3 auth
  expires or throttling spikes mid-run, every page errors → `Finalize` emits 0 rows → worker logs
  `done: 0 domains` and **exits 0**, writing valid empty parquets. cc-crawl's gate (exit 0 +
  `domains.parquet` exists — an empty file passes, `cc-crawl/main.go:229-236`) loads 0 rows and writes the
  marker; the part is never redone. Proportional too: a 60%-errors part is indistinguishable from a sparse
  one. **No error-rate threshold exists anywhere.**
- **C2 — Email false-positive flood into contacts (empirically confirmed).** `parse.go:10`
  `[\w.+-]+@[\w-]+\.[\w.-]+` over raw full HTML matches `logo@2x.png`, `vue@3.2.31`, `react@18.2.0` — every
  retina image and every jsdelivr/unpkg script tag becomes an "email" contact row. At millions of domains ×
  25 pages this will likely be the dominant content of `commoncrawl_domain_contact_info`. No TLD sanity
  check; trailing dots allowed; no case normalization (M10).
- **C3 — `--chunk` slices pages, not domains → duplicate rows with conflicting provenance.** Documented as
  "domains per chunk" (`main.go:97`) but the loop slices worklist *items* (`main.go:418-435`). A domain
  straddling a boundary aggregates twice (~2.5% of domains at chunk 1024 / 25 pages): two `DomainRow`s,
  duplicated tech/contact/metadata rows, second half keyed to a different "primary" URL. The missing
  worklist `ORDER BY` makes contiguity itself unguaranteed.

### Important

- **I1 — Identifiers/JSON-LD extracted from the CAPPED body; emails from the full body — exactly inverted.**
  The 128 KiB cap is justified for Wappalyzer regexes (~1.2 s/page uncapped) but `ExtractProfile`/
  `ExtractLEIs`/`ExtractVATs` also get the capped body (`worker.go:234-252`) while the cheap email regex
  gets the full body (`worker.go:228`). LEI/VAT/imprint data lives in page **footers** — the bytes the cap
  throws away — and a JSON-LD block truncated at the boundary is silently dropped. The worklist's whole
  legal-page ranking exists to find exactly this data.
- **I2 — LEI text regex has no word boundaries; ~1% of 20-char uppercase-alnum windows pass the checksum**
  (`lei.go:11`; ISO 7064 base rate 1/97, measured 0.95%). Hashes/CSRF tokens/S3 signatures in minified JS
  become confident `Valid=1` LEIs attributed to the wrong domains — poison for registry linking.
- **I3 — VAT format-only countries validate junk** (`vat.go:64-70`: only DE/IT have checksums). `RO12`,
  `RO2024`, `FI12345678` all emit `Valid: 1`, indistinguishable from checksummed hits.
- **I4 — Fast-matcher cookie channel diverges from upstream. ✅ FIXED (same day).** Upstream matches the
  pattern against the cookie **value** (`wappalyzergo fingerprint_cookies.go:21-31`); fast matched against
  the whole `name=value; path=/…` string, so 13 anchored cookie fingerprints (Bynder, DigitalRiver,
  CodeIgniter…) could never match, and unanchored ones could false-match on attributes. **Fixed**: fast now
  normalizes Set-Cookie exactly like upstream and evaluates against the value only; `TestFastMatcherCookieParity`
  (existence + anchored + mismatch + multi-cookie) locks it in. The 430 empty-pattern existence cookies were
  verified fine all along. Doc comment corrected.
- **I5 — Otherwise the AC-gate soundness claim HOLDS (verified against upstream):** literal extraction is
  conservative (`+`→`{1,250}` rewrites can't remove requirements; `OpStar/OpQuest/OpAlternate/OpRepeat`
  treated as non-required), casefolding matches, unparseable patterns fall to always-evaluate, <4-rune
  literals never gated, meta/header names all lowercase. Fast emits a *superset* (transitive implies;
  no confidence-0 suppression).
- **I6 — Version/confidence quality below upstream:** `record()` is first-match-wins (`techfast.go:196-203`)
  so version-bearing patterns lose to earlier versionless ones and implies-recorded apps never get a
  version; per-domain dedup by name collapses differing versions across pages; `Confidence: 100` hardcoded
  (`worker.go:369`) — the column is meaningless.
- **I7 — Effective fetch concurrency ≈ `chunk/25`, far below `--concurrency`. ✅ FIXED (Level 1 + 2).**
  Level 1: cc-crawl passes `--chunk` (`-tech-chunk`, default 16384). Level 2 (refactor): `FetchChunk` is
  now a two-level pool — a domain pool (`--concurrency` = DOMAINS in flight) of `processDomain` units,
  each fanning its pages into its own 8-wide page pool (`worker.PageConcurrency`), built on
  `errgroup.SetLimit`. `processPage` is pure (one page → `pageResult`), `mergePageResults` folds results
  deterministically in worklist order (primary = **lowest-rank survivor** — preserves the old sequential
  semantics under parallel completion). Total fetches = `concurrency × 8`; the S3/CDN transport is sized to
  that product; `-tech-conc` default lowered to 32 (= 256 sockets). Unit-tested (merge semantics,
  all-failed domain, parallel-page determinism ×20). This also fixed the ~70-line five-job closure the
  code-quality section flagged.
- **I8 — S3 client: default 10 idle conns/host under 128-way concurrency → TLS churn. ✅ FIXED** —
  `NewS3Getter` now takes `concurrency` and sizes the transport like `NewHTTPGetter`
  (`MaxIdleConnsPerHost = 2×conc`, `MaxConnsPerHost = conc`). Still open from I8: no app-level
  whole-request retry (SDK request retry only); a mid-`ReadAll` stream error drops the page.
- **I9 — `load --dir` silently loads partial dirs** (missing kinds skipped, `load.go:177-180`): a crash
  between the five sequential writes → part loads domains-only and is marked done forever.

### Minor

- **M1** `Content-Encoding` never handled (`fetch.go:45-54`) — a gzip/br-preserved CC record yields a binary
  body → zero detections, silently. Worth a one-part empirical check before the run.
- **M2** Identifier dedup by value only (`idSeen[id.Value]`) — same string as `vatID`+`taxID` keeps one type.
- **M3** Metadata = first non-empty page's profile wins — a logo-only stub blocks the full imprint profile;
  per-field merge would be strictly better.
- **M4** Social extraction effectively dead in tech mode (only JSON-LD `sameAs` is live; `extractSocials`
  unreachable when `runEmbed=false`); `socialHosts` substring match would count `?share=facebook.com`.
- **M5** Primary page failed but a later page survived → `primaryURL` silently becomes e.g. `/privacy`;
  all rows carry it as `source_url` → per-page provenance lost (a VAT found on page 17 attributed to page 2).
- **M6** S3 upload key embeds the local path (`o.s3Prefix + "../data/crawl/…"`, `main.go:521`) — broken keys
  (unused by cc-crawl).
- **M7** Out-dir-is-a-file passes the empty check, dies later confusingly (`main.go:293`).
- **M8** ~4 body-sized allocations/page (email string copy, ToLower copy, tokenizer) — fine now; first GC
  knob if profiles show pressure. Bodies are NOT retained across chunks; memory is otherwise sound.
- **M9** `Insert` sends a whole file as one native batch — fine now; contacts (C2) could blow this up.
- **M10** Emails not case-normalized before dedup.

---

## Code-quality assessment

**Good:** clean package boundaries; FetchChunk/Finalize with an opaque `FetchedChunk` is a proper pipeline
seam; concurrency disciplined (per-index writes, atomics, `errOnce`; `wg.Wait` gives the happens-before for
final reads) — **no data races found**; produce/load with fixed filenames + the tag-pinning test is a good
contract; `fetch.RangeGetter` keeps the worker testable; comments explain *why*.

**Weak:** the fast-vs-upstream parity test is 6 hand-picked samples — no Set-Cookie case (exactly where the
real divergence is), no corpus differential test, so the soundness claim rests on reading, not testing. No
test covers all-fetches-fail or the chunk-boundary split. The ~70-line per-domain closure in `FetchChunk`
does five jobs — extracting a per-page function would have made the capped-vs-full-body inversion (I1)
obvious. Error philosophy is "log and continue" with no aggregate contract (C1). Flag docs drift
(`--chunk` "domains"); `techfast.go:24` overclaims parity.

---

## Prioritized recommendations (no code changed — for discussion)

1. **Failure contract (do before the big run; cheapest highest-value):** exit non-zero when part-level fetch
   error rate > ~20% or 0 domains survive; cc-crawl then treats it as failed (no marker). Emit
   `errCount/pageCount` to a `run_stats` sidecar for post-hoc audit of 300 parts.
2. **Extract identifiers/JSON-LD/emails from the FULL body** (cap only the Wappalyzer regexes) and tighten
   extractors: email — require a plausible TLD, reject `\dx`/all-digit domains (kills `@2x.png`,
   `vue@3.2.31`); LEI — `\b` anchors + context ("LEI" nearby) or restrict bare-text LEIs to legal pages;
   VAT — add public checksums (AT/BE/DK/FI/NL/PL/SE/SI…) or a three-state `valid`.
3. **Chunk by domain, not item** + add `ORDER BY root_domain, rn` to the worklist as the contiguity contract.
4. **Throughput:** `--chunk ≥ 25×concurrency` for tech (or parallel pages per domain);
   `MaxIdleConnsPerHost ≈ concurrency` on the S3 client + a whole-request retry like the CDN path.
5. **Per-page provenance:** thread the actual page URL onto tech/identifier/contact rows (`source_url`
   column already exists) — also fixes M5 and makes identifier hits auditable.
6. **Fast-matcher polish:** cookie value-only matching (I4); allow version upgrade in `record()` (I6); fix
   the `techfast.go:24` doc claim; add a ~1k-page corpus differential test + a Set-Cookie parity case.
7. **Intra-part checkpointing** (nice-to-have): periodic parquet flush + completion marker, so a crash at
   95% of a 2M-page part doesn't restart from zero.
8. **Metadata/id quality:** per-field profile merge across pages (M3); dedup ids by `type:value` (M2).

---

## Box readiness (wappalyzer, checked 2026-07-02)

| Check | State |
|---|---|
| Embeddings | **300/300 parts have vectors**, all fp16 (236 GB) — embedding phase complete |
| Tech progress | zero (`out_tech_*`: 0, markers: 0) — fresh start; 1 tech shard cached from earlier testing |
| Disk | 659 GB free (tech output is small; pages stream through memory) |
| Binaries | rebuilt Jul 1 20:52 (current code incl. the fp16-skip fix) |
| Processes | idle — full CPU available for the CPU-bound tech pass |

Baseline run command (as-is, accepting the findings above):

```bash
cd /opt/companycollect/corpscout/commoncrawl
./cc-crawl/bin/cc-crawl -mode tech -parts 0-299 -crawl CC-MAIN-2026-25
```

Given **I7**, add `-max-pages`-aware chunk sizing via the worker flag pass-through if/when the chunk fix is
made; today the practical lever is accepting ~41-way effective fetch concurrency (CPU is the bottleneck for
tech anyway, so this mostly costs fetch latency, not detection throughput). Given **C1**, spot-check early
parts (`rows_to_clickhouse` in the cc-crawl JSON log vs expectations, and
`grep -c '"errors"' data/logs/crawl_tech_*.log`) before letting the full range run unattended.
