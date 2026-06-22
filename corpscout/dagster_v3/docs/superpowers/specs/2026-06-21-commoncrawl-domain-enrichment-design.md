# CommonCrawl domain enrichment — design

**Status:** Phase-0 spike package (`commoncrawl_enrich`) built + extraction validated on live
CommonCrawl data. **Scale architecture revised 2026-06-22 → whole-segment WET-first (see §0),
benchmark-backed.** Full run targets AWS (us-east-1). Sections §1–§9 describe the original
index-driven spike (which validated the *extraction logic*); §0 supersedes its *resolver/scale*
approach.
**Author:** brainstormed 2026-06-21; revised 2026-06-22.

---

## 0. Revision 2026-06-22 — whole-segment WET-first architecture (benchmark-validated)

The Phase-0 spike package validated the **extraction logic** end-to-end on live data. A series of
experiments then pivoted the **scale architecture** from per-domain index lookup to **whole-segment
WET processing**. Every decision below is backed by a measured benchmark (all on CC-MAIN-2025-21).

### Experiments & results

| experiment | result | implication |
|---|---|---|
| **CDX index lookup** (per-domain, `index.commoncrawl.org`) | rate-limited: 503s under light concurrency; ~7/30 resolved; **0.24 dom/s** | unusable at scale (~11,400 h for 10M) |
| **CommonCrawl S3 columnar index** (`s3://commoncrawl/…`) | **403** — not anonymously readable (confirmed via DuckDB *and* boto3-unsigned) | fast batch path needs a real AWS account |
| **WARC fetch + extract** (per page) | works; IČO/emails/socials/tech extracted correctly | extraction logic sound |
| **LLM contacts, think-ON** | **23 s/page** | thinking ruinous for extraction |
| **LLM contacts, think-OFF** (20 pages) | **1.9 s/page**; emails DET=6 vs LLM=4; phones/addresses LLM-only | regex beats LLM on emails; LLM wins phones/addresses |
| **LLM industry, OFF vs ON** (real businesses) | think-off matched NACE on 3/4 exactly (47.61, 62.01, 29.10), right division on 4th; **1.3 s vs ~18 s** | **non-thinking is good enough for industry** |
| **WET text pass** (parse + email regex, 5,000 recs) | **2,723 records/sec**; 26% have an email; ~153 MB/file (vs ~1 GB WARC) | bulk pass is cheap + fast |

### Recomputed full-crawl cost (≈90k files/crawl)
- **WET text + email extraction over the *entire* crawl: ~19 core-days, ~14 TB download** (vs ~90 TB WARC) — trivially parallel.
- **Industry (per-domain homepage, think-off):** bounded by #domains, batchable — ~5 days for 10M domains with modest vLLM concurrency (single-stream 163 core-days ÷ batch).
- Net: WET + per-domain/think-off discipline cut the earlier WARC+LLM-every-page estimate (~50,000+ core-days) by **~1000×**.

### Validated architecture (supersedes §3/§6 for scale)

**Format follows data need:**

| extract | needs | source |
|---|---|---|
| industry, emails, phones, addresses | page text | **WET** (pre-extracted plaintext; no HTML parse; ~5× smaller) |
| socials | `<a href>` | **WARC** (WET strips links) |
| technologies (Wappalyzer) | HTML + headers | **WARC** |

**Three tiers:**
1. **WET bulk pass (whole crawl, credential-free over `https://data.commoncrawl.org`):** stream every
   `*.warc.wet.gz`, **regex emails** (regex beats the LLM here and is free) + stage per-domain
   homepage text. ~19 core-days, ~14 TB.
2. **Industry tier (per *domain*, homepage only):** LLM **think-off** (verified equal to thinking on
   industry), batched on the local vLLM or a **cheap commercial model** (DeepSeek-V3 / Gemini Flash —
   *non-reasoning* suffices); **confidence-gated escalation** to thinking only when confidence is low /
   the label is only division-level.
3. **WARC selective (optional):** only to add tech + href-socials, and only for the
   **homepage-per-domain** set — never the bulk.

**Extraction discipline (validated):** **hybrid** — regex for emails (exhaustive + free), LLM
(think-off) for phones/addresses/socials and industry. **Think-off everywhere.**

**Access reality:** off-AWS, read all public data over **`https://data.commoncrawl.org`** (no
rate-limit on the data CDN — only the CDX *index API* throttles). The fast S3 columnar index needs an
**AWS account**; the clean scale plan is to **run whole-segment processing on AWS (us-east-1)** where
S3/CDN reads are free + fast (per commoncrawl.org/get-started). "Our S3" (the `~/.aws` `minio`/`rustfs`
profiles) is a self-hosted store with no CommonCrawl data.

### Carry-over from the built `commoncrawl_enrich` package
- **Reused as-is:** extractors (`extract.py` IČO/emails/phones/socials — now urlparse-hardened against
  WARC garbage), `tech.py`, `models.py`, the provider-agnostic LLM client (`llm.py`, think toggle),
  Parquet output, metrics.
- **Changes for scale:** the index resolver (CDX/columnar) is replaced by **whole-segment WET/WARC
  streaming**; add `tldextract` (host → registrable-domain + subdomain); add a WET reader (warcio
  `conversion` records).

### Open items
- **Batched-concurrent industry throughput** on the vLLM (to firm up the ~5-day figure) — not yet measured.
- **Commercial industry model** comparison (DeepSeek/Gemini) — needs an API key in `.env`.
- **WET coverage caveat:** WET strips `mailto:`/href, so emails-only-in-links + socials + tech need the
  selective-WARC tier; quantify the WET-vs-WARC email loss.
- Align `run.py` to read `.env`'s `COMMONCRAWL_LLM_BASE_MODEL` (it reads `COMMONCRAWL_LLM_MODEL`);
  note `.env`'s `qwen3:6b` aliases the served `RedHatAI/Qwen3.6-35B-A3B-NVFP4`.

---

## 1. Goal & motivation

For a bounded set of domains, fetch each homepage from **CommonCrawl** (not by crawling
the live web — we hit bot-walls there, e.g. `martinus.sk` → "Access Denied/BotStopper")
and extract per-domain intelligence into ClickHouse:

- **Contacts** (emails, phones, socials) — registers in most countries carry none.
- **IČO / DIČ** — where present, the exact key to join a domain to a company in
  `cz_companies` / `sk_companies` (Slovak/Czech business sites must publish their IČO:
  SK §3a Obchodný zákonník; CZ §435 občanský zákoník).
- **Technologies** — via Wappalyzer over the page's HTML + HTTP headers.
- **Industry** — an **LLM** classifies the page text into an industry label + coarse NACE
  hint (the one signal regex can't extract); title/meta/language/text are also captured raw.

Two payoffs: (1) **domain→company linking** where an IČO joins a registry; (2) **standalone
domain intel** (contacts/tech/industry) for the many countries — France, Germany, US — where
our registers give us no website or contact data at all.

### Why this is worth doing as a spike first
The value depends on empirical hit-rates (does CommonCrawl have the homepage? does the page
publish an IČO? a contact?). The spike measures those on 100k domains before we commit to the
full top-10M pipeline.

---

## 2. Scope

**In (Phase 0 spike) — standalone package only, single process, no Dagster/Temporal/ClickHouse:**
- Target: **~10,000** domains by Open PageRank (from `open_page_rank_domains`), all TLDs, for
  the first run — to read **both the hit-rate numbers and the end-to-end speed** (especially
  local-LLM throughput). Extend to ~100k once speed is acceptable.
- Resolve each domain's homepage record in the CommonCrawl index; range-fetch the **WARC**
  record (full HTTP response + HTML).
- **Two extraction arms** (§4), both run on the 100k: a **deterministic arm** (IČO+checksum,
  contacts, technologies, metadata) and a **measured LLM arm** (industry classification +
  contact recall on the deterministic-miss residual).
- Emit the 5 enrichment tables as **Parquet** (the §5 schema) + a metrics JSON, and produce the
  hit-rate **and regex-vs-LLM uplift** report. (The schema in §5 is the load target; ClickHouse
  landing is Phase 1.)

**Out (Phase 1+, noted but not built now):**
- The Dagster glue: ClickHouse load of the Parquet + `company_website_domains` linking where an
  IČO joins `cz_companies`/`sk_companies`.
- **Temporal-orchestrated chunked execution** for the full top-10M run; weekly scheduling.
- Full **NACE mapping** of the LLM industry guess (the spike keeps free-text label + coarse hint).
- Registry joins beyond CZ/SK.
- Crawling pages beyond the homepage (e.g. `/kontakt`) to raise IČO coverage.

---

## 3. CommonCrawl mechanics (the operational answers)

**You never download whole WET/WARC files.** A monthly crawl is ~90,000 WET files ×
~82 MB gzipped ≈ 7–8 TB (WARC is ~10× that). Instead we go **index-first, then range-fetch
single records**:

1. **Index lookup (domain → record location).** The CommonCrawl **columnar index**
   (`s3://commoncrawl/cc-index/table/cc-main/warc/`, Parquet) exposes `url_host_name`,
   `url_host_tld`, `warc_filename`, `warc_record_offset`, `warc_record_length`, `fetch_status`,
   `content_mime_type`, `url_path`. **DuckDB reads it directly** over anonymous `httpfs`
   (no AWS account). One set-based query joins our 100k domain list on `url_host_name`,
   keeps `subset='warc'`, homepage (`url_path='/'`), HTTP 200, HTML, latest capture per
   domain → `(warc_filename, offset, length)`.
   - Fallback / alternative: the **CDX API**
     `https://index.commoncrawl.org/CC-MAIN-<id>-index?url=<domain>/&output=json` (HTTP,
     no AWS) for per-domain lookups.
2. **Range-fetch (one record).**
   `GET https://data.commoncrawl.org/<warc_filename>` with header
   `range: bytes=<offset>-<offset+length-1>` → a single **gzipped WARC record**
   (~tens–hundreds of KB). Gunzip → HTTP headers + HTML. Free CloudFront mirror.
3. **Process & discard.** Extract structured rows, keep them, throw the HTML away.

**Disk:** ≈ zero. We stream each record, never persist HTML. **No disk extension needed.**
**Time:** 100k range-fetches at ~20–50/s with modest concurrency ≈ **well under an hour**;
the DuckDB index query for 100k domains ≈ minutes to tens of minutes (column projection +
row-group pruning; the index is sorted by SURT key).

**Why not "whole-segment streaming" (download a WET, process, delete, repeat)?** For a
*targeted* 100k/10M set it's strictly worse — you'd process ~27k mostly-irrelevant domains
per 82 MB segment to find your targets. The index→range-fetch path fetches *only* the
domains we want. (Whole-segment streaming only wins if the goal becomes "enrich every domain
in the crawl," which is explicitly out of scope.)

**WARC vs WET:** we fetch **WARC** (full HTTP response + HTML) because Wappalyzer needs HTML +
headers. WET (extracted plaintext) would be smaller but can't drive technology detection.

Sources: [CC columnar index](https://commoncrawl.org/columnar-index) ·
[index→WARC byte ranges](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format) ·
[CDXJ index](https://commoncrawl.org/cdxj-index).

---

## 4. Extraction logic — two arms (deterministic + a measured LLM arm)

Each tool stays where it's strongest. The **deterministic arm** owns keyed extraction (IČO,
contacts, tech) — exact, free, scales. The **LLM arm** does the things regex can't (industry)
and boosts recall where regex came up empty (obfuscated/prose contacts). Both run on the full
100k so the spike measures the LLM's *incremental* value, not a guess.

### Deterministic arm (primary)
- **IČO** — regex anchored on `IČO`/`IC`/`IČ`/`ICO` labels, capture the 8-digit number,
  then validate the **mod-11 checksum** (CZ and SK share the algorithm). Reject numbers that
  fail the checksum (kills false positives). A page may show several IČOs (agency footer,
  group) — keep all, mark the one matching the page's legal-name/host as primary.
- **DIČ** — regex on `DIČ`/`IČ DPH` labels (not checksum-validated; informational).
- **Emails** — RFC-ish regex; drop obvious noise (`example@`, image-sprite junk, `@2x`),
  flag role addresses (`info@`, `sales@`, `podpora@`), record local/domain parts.
- **Phones** — regex for SK/CZ/intl formats; best-effort E.164 normalization.
- **Socials** — match links to known platforms (facebook, linkedin, instagram, x/twitter,
  youtube, tiktok…); capture URL + handle.
- **Technologies** — **Wappalyzer** over HTML + response headers → list of
  `(technology, category, version, confidence)`.
- **Metadata** — `<title>`, `<meta description>`, content language, final URL, capture date,
  HTTP status, TLD, country guess (TLD → else language/signals).

### LLM arm (Phase 0, measured)
The LLM client is **OpenAI-compatible and provider-agnostic** (base-URL + model from config).
Default = the **local** qwen endpoint already wired in the monorepo (the `openai.OpenAI` client
from `uk_companies_house/pdf_extract`), so there's **no API cost**, only local GPU time. We run
the model in **thinking mode** — industry classification is a judgment task where reasoning
improves quality, so we *drop* the `enable_thinking:False` flag the PDF path uses; the existing
`_parse_json_object` helper already strips the `<think>…</think>` trace before JSON parsing.
(Thinking mode generates more tokens → higher per-page latency, which the §7 speed metric
captures and feeds the local-vs-hosted decision.) **If the 10k run shows local throughput is too
slow, point the same client at a hosted OpenAI-compatible server — a config change, no code
change.** Per page, on the already-fetched text (no extra fetch):
- **Industry (primary use)** — classify into a short industry label + a **coarse NACE hint**
  (e.g. section/division) + confidence; **thinking mode** helps this judgment. Regex cannot do
  this; full NACE mapping is Phase 1.
- **Contact recall (secondary use)** — run only on the **deterministic-miss residual** (pages
  where regex found no email/phone) to catch obfuscated (`info [at] x [dot] sk`) / prose contacts.
- **IČO is NOT delegated to the LLM** — it stays deterministic (checksum-exact = the join key).
- **Structured JSON output**, **truncated** input (~first 2–4k tokens, contact/footer-biased).
- Every contact row is tagged `source_method` (`regex` | `llm`) so the uplift is directly
  queryable. Run on the full ~10k first (HTML already fetched) and **time it** to project the
  100k / 10M cost and decide local-vs-hosted.

---

## 5. ClickHouse schema (normalized: spine + child tables)

One spine row per domain per crawl snapshot; emails/phones/socials/technologies are
one-to-many → separate child tables keyed on `root_domain`. All tables
`ReplacingMergeTree(resolved_at)`; non-nullable `String` columns coalesced to `''`; sort keys
non-nullable. New `commoncrawl` migrations.

**`domain_enrichment`** (spine) — `ORDER BY (root_domain)`:
`root_domain, source_rank UInt32, open_page_rank Float64, tld LowCardinality(String),
country_guess LowCardinality(String), crawl_id LowCardinality(String), homepage_url String,
capture_date Nullable(Date), http_status UInt16, content_language LowCardinality(String),
title String, meta_description String, ico String, dic String, ico_checksum_valid UInt8,
ico_matched_company UInt8, matched_country_iso2 LowCardinality(String), matched_ico String,
industry_label String, industry_nace_hint LowCardinality(String), industry_confidence UInt8,
industry_method LowCardinality(String) /* none | llm */,
email_count UInt16, phone_count UInt16, social_count UInt16, technology_count UInt16,
fetch_status LowCardinality(String) /* ok | not_in_index | fetch_failed | non_html */,
source_system LowCardinality(String), source_run_id String, source_url String,
resolved_at DateTime64(3,'UTC')`.

**`domain_emails`** — `ORDER BY (root_domain, email)`:
`root_domain, email, email_local String, email_domain String, is_role UInt8,
source_method LowCardinality(String) /* regex | llm */,
source_system, source_run_id, resolved_at`.

**`domain_phones`** — `ORDER BY (root_domain, phone_e164)`:
`root_domain, phone_raw String, phone_e164 String, country_guess LowCardinality(String),
source_method LowCardinality(String) /* regex | llm */,
source_system, source_run_id, resolved_at`.

**`domain_socials`** — `ORDER BY (root_domain, platform, url)`:
`root_domain, platform LowCardinality(String), url String, handle String,
source_system, source_run_id, resolved_at`.

**`domain_technologies`** — `ORDER BY (root_domain, technology)`:
`root_domain, technology LowCardinality(String), category LowCardinality(String),
version String, confidence UInt8, source_system, source_run_id, resolved_at`.

**Consumer — `company_website_domains`** (existing): where `ico_checksum_valid=1` AND the IČO
joins `cz_companies`/`sk_companies`, emit a link with `domain_source='commoncrawl_ico'`,
`company_id_type='ico'`, `company_id=<ico>`, country from the matched registry.

---

## 6. Execution architecture — heavy package + thin Dagster glue

This workload is **not** like the other corpscout sources (bounded "download a file → DuckDB
→ ClickHouse" jobs that fit a single Dagster op). It is **millions of tiny network fetches +
per-page HTML parsing + Wappalyzer** — an embarrassingly-parallel, hours-to-days batch.
Running that inside one Dagster op fights the tool: high-concurrency async doesn't map to a
single op; a run held open for hours/days creates the Postgres/event-log connection pressure
our own scaling notes warn about; and the heavy deps (async HTTP, WARC parsing, Wappalyzer)
would bloat the Dagster image. So we split heavy compute from orchestration, with **Parquet as
the seam**:

**(A) Standalone package `commoncrawl_enrich` — no Dagster/Temporal/ClickHouse deps.**
Lives in the `dagster_v3` repo but **outside `src/dagster_v3/defs/`** (so Dagster's defs-loader
doesn't treat it as assets); importable by both a Temporal activity and the Dagster glue.
- Input: a **domain-batch manifest** (Parquet/CSV of `root_domain` + rank).
- Does: index resolution (DuckDB over the columnar Parquet index / CDX fallback) → async
  range-fetch (e.g. `aiohttp`, bounded concurrency) → WARC record parse → extract
  (IČO + checksum, DIČ, emails, phones, socials, title/meta/lang) + Wappalyzer.
- Output: **partitioned Parquet** — `domain_enrichment` + `domain_emails` / `domain_phones`
  / `domain_socials` / `domain_technologies` — to a staging dir/S3, plus a per-batch
  **metrics JSON** (the hit-rate counts).
- Pure, async, independently runnable & testable; throughput-tunable; heavy deps stay out of
  the Dagster image.
- Internals: `index_client.py`, `warc.py` (range-fetch + gunzip + WARC parse), `extract.py`
  (IČO/contacts/socials/meta), `tech.py` (Wappalyzer), `enrich.py` (orchestrate + write
  Parquet), `metrics.py` (report counts).

**(B) Temporal — durable chunk orchestration for the full run (Phase 1).**
At 10M scale the run is multi-day with per-chunk retries/resume — Temporal's home turf, and
already in this stack (it shares the `companycollect` Postgres; the Norway translation path
already bridges Dagster↔Temporal). A workflow fans the manifest into chunks; **one activity per
batch of ~1k domains** (heavy async concurrency *inside* the activity) calls package (A),
writing a Parquet shard + per-chunk metrics. **Coarse chunking is deliberate** — one activity
per domain would flood Temporal's shared-Postgres history (10M events); ~1k/activity keeps it
to ~10k activities. (This fan-out is also the load to validate against the planned PgBouncer
ceiling.)

**(C) Thin Dagster glue (`defs/commoncrawl`) — manifests, trigger, load.**
- `manifests` asset — shard `open_page_rank_domains` into batch manifests (Parquet).
- Trigger the Temporal workflow + a **completion sensor** (the existing Norway pattern).
- `load` asset(s) — **Parquet → DuckDB → ClickHouse** using the existing export pattern
  (DuckDB reads Parquet natively; append; `ReplacingMergeTree` dedups) + the
  `company_website_domains` link for IČO-matched domains.
- `tables.py`, migrations for the 5 tables, ClickHouse export glue, tests (checksum,
  extractors with HTML fixtures, fake index + fake WARC end-to-end, Parquet→CH load, migration
  column contracts).

Reuses: `open_page_rank_domains` (input), `company_website_domains` (output link),
`cz_companies`/`sk_companies` (IČO join), a shared IČO-checksum util.

**Spike:** run only package (A), single process, and inspect its Parquet/metrics — no Temporal,
no Dagster, no ClickHouse. Build (B) Temporal orchestration + (C) Dagster glue in Phase 1, when
durability and scale actually matter.

---

## 7. Success metrics (the spike's deliverable)

**Speed (first-class, decides local-vs-hosted LLM):** end-to-end domains/sec, median fetch ms,
median LLM inference ms/page, total wall-clock for the ~10k run, and the **projected wall-clock
for 100k and 10M**. If the local LLM dominates wall-clock, switch the (provider-agnostic) client
to a hosted endpoint and re-time.

Over the sample, report and break down by TLD/country:
- % **found in the CC index**; % **fetched OK** (200 + HTML).
- % with a **checksum-valid IČO**; % whose IČO **joins a registry**.
- % with **≥1 email**; % with **≥1 phone**; % with **≥1 social** (deterministic arm).
- % with **≥1 detected technology**; top technologies.
- **Regex-vs-LLM uplift** — on the deterministic-miss residual, how many *extra* emails/phones
  the LLM recovered (count + % of the residual), via the `source_method` tag.
- **Industry coverage** — % of pages the LLM classified at confidence ≥ threshold, label
  distribution, and a spot-check of plausibility on a manual sample.

These numbers decide go/no-go for the full top-10M build, and whether the LLM arm earns its
GPU cost at scale.

---

## 8. Open items & risks
- **Wappalyzer in Python** — choose `python-Wappalyzer` vs shelling to a Go binary
  (`wappalyzergo`, already used elsewhere in the monorepo) vs the raw fingerprint dataset.
  Pin this in the implementation plan.
- **Homepage selection** in the index — `url_path='/'`, latest 200/HTML capture per host;
  decide handling of `www.` vs apex and redirects.
- **CommonCrawl coverage** — top domains are almost always captured; the long tail and very
  fresh sites may be missing or homepage-only (IČO may live on `/kontakt`, uncaptured). This
  is precisely what the spike measures.
- **Index query cost** — the AWS-free DuckDB scan of the columnar index streams a lot over the
  network; if it's slow, Athena (server-side, small cost) is the scale-up. Spike can also read
  a subset of index Parquet files.
- **Compliance ≠ presence** — the IČO-on-website obligation is statute-backed but not
  universally followed (micro/personal sites); real hit-rate is empirical.
- **Identifier ambiguity** — multiple IČOs on one page; pick the host/name-matching one as
  primary, keep the rest.
- **Temporal chunk granularity (Phase 1)** — ~1k domains/activity to bound Temporal's
  shared-Postgres history; this 10M fan-out is a load test for the planned PgBouncer ceiling.
- **LLM throughput → local vs hosted** — the 10k run measures local-model speed; if it
  dominates wall-clock, switch the provider-agnostic client to a hosted OpenAI-compatible
  endpoint (config-only). **Thinking mode** (enabled for industry quality) generates more tokens
  and raises per-page latency — a deliberate quality/speed trade the 10k timing quantifies. Bound
  input with truncation + structured-output prompting; confirm the model returns valid JSON.
- **Industry→NACE mapping** — deferred to Phase 1; the spike keeps the LLM's free-text label +
  coarse NACE hint (full mapping likely via embeddings against `nace_categories`).

---

## 9. Execution plan (phases)
- **Phase 0a — mechanics (≈1k domains), package only:** index lookup + range-fetch + WARC parse
  in the standalone package; confirm CC coverage and fetch reliability.
- **Phase 0b — extraction + report (~10k first), package only, single process:** deterministic
  arm (IČO/contacts/socials/tech + Wappalyzer) **and** the LLM arm (industry + contact recall on
  the residual); emit the 5 Parquet tables + metrics JSON; produce the hit-rate **+ regex-vs-LLM
  uplift + speed** report. Decide local-vs-hosted LLM from the timings, then optionally extend to
  ~100k. **Decision gate.** (No Temporal/Dagster/ClickHouse — inspect the Parquet directly.)
- **Phase 1 (if go):** **Temporal** orchestration of the chunked 10M run (~1k domains/activity
  calling the package); **Dagster** glue — manifest sharding, workflow trigger + completion
  sensor, Parquet→ClickHouse load, `company_website_domains` linking; weekly schedule; then full
  **NACE mapping** of the industry guess (embeddings against `nace_categories`).

**Gating dependency:** the `open_page_rank` dataset must be loaded (it supplies the target
list/manifests). Until then this stays a design.
