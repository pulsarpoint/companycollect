# CommonCrawl domain enrichment — design

**Status:** design (execution gated on the `open_page_rank` dataset being loaded).
**Author:** brainstormed 2026-06-21.
**Scope of this doc:** Phase 0 — a **100k-domain spike** that validates feasibility,
architected as the seed of a reusable enrichment layer over the top-10M domains.

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
- **Industry / site signals** — title, meta description, language, raw text snippet
  (for later NACE inference; *capture only* in this phase).

Two payoffs: (1) **domain→company linking** where an IČO joins a registry; (2) **standalone
domain intel** (contacts/tech/industry) for the many countries — France, Germany, US — where
our registers give us no website or contact data at all.

### Why this is worth doing as a spike first
The value depends on empirical hit-rates (does CommonCrawl have the homepage? does the page
publish an IČO? a contact?). The spike measures those on 100k domains before we commit to the
full top-10M pipeline.

---

## 2. Scope

**In (Phase 0 spike) — standalone package only, no Dagster/ClickHouse yet:**
- Target: top **100,000** domains by Open PageRank (from `open_page_rank_domains`), all TLDs.
- Resolve each domain's homepage record in the CommonCrawl index; range-fetch the **WARC**
  record (full HTTP response + HTML); extract contacts / IČO / technologies / metadata.
- Emit the 5 enrichment tables as **Parquet** (the §5 schema) + a metrics JSON, and produce the
  **hit-rate report** from them. (The schema in §5 is the load target; ClickHouse landing is
  Phase 1.)

**Out (Phase 1+, noted but not built now):**
- The Dagster glue: ClickHouse load of the Parquet + `company_website_domains` linking where an
  IČO joins `cz_companies`/`sk_companies`.
- Scaling to the full top-10M; weekly scheduling.
- LLM/embedding-based **industry inference** (we only capture raw title/meta/text now).
- LLM **fallback extraction** for pages where regex finds no IČO/contact.
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

## 4. Extraction logic — regex/deterministic first, LLM later

Identity & contacts are **keyed extraction**, not semantic similarity, so deterministic
parsing wins on precision, cost, and scale. The spike uses **no LLM**; we *measure* the gap
an LLM fallback could close.

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

**LLM (future, out of scope):** only as a *fallback* on pages where regex finds no IČO/contact,
and for industry classification from the captured text. We decide its worth from the spike's
"regex found nothing" residual.

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
email_count UInt16, phone_count UInt16, social_count UInt16, technology_count UInt16,
fetch_status LowCardinality(String) /* ok | not_in_index | fetch_failed | non_html */,
source_system LowCardinality(String), source_run_id String, source_url String,
resolved_at DateTime64(3,'UTC')`.

**`domain_emails`** — `ORDER BY (root_domain, email)`:
`root_domain, email, email_local String, email_domain String, is_role UInt8,
source_system, source_run_id, resolved_at`.

**`domain_phones`** — `ORDER BY (root_domain, phone_e164)`:
`root_domain, phone_raw String, phone_e164 String, country_guess LowCardinality(String),
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

**(A) Standalone package `commoncrawl_enrich` — no Dagster, no ClickHouse deps.**
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

**(B) Thin Dagster glue (`defs/commoncrawl`) — the boring, reliable, scheduled part.**
- `manifests` asset — shard `open_page_rank_domains` into batch manifests (Parquet).
- Orchestration — trigger the package per batch, **partitioned by batch** for bounded,
  resumable runs (plain schedule, or **Dagster Pipes** to invoke the external process *with*
  lineage). For the spike this can be a manual package run.
- `load` asset(s) — **Parquet → DuckDB → ClickHouse** using the existing export pattern
  (DuckDB reads Parquet natively; append; `ReplacingMergeTree` dedups) + the
  `company_website_domains` link for IČO-matched domains.
- `tables.py`, migrations for the 5 tables, ClickHouse export glue, tests (checksum,
  extractors with HTML fixtures, fake index + fake WARC end-to-end, Parquet→CH load, migration
  column contracts).

Reuses: `open_page_rank_domains` (input), `company_website_domains` (output link),
`cz_companies`/`sk_companies` (IČO join), a shared IČO-checksum util.

**Trade-off:** we give up built-in Dagster observability on the heavy step; we mitigate with
per-batch manifests + metrics JSON (and optionally Dagster Pipes for lineage). **Spike:** run
only package (A) and inspect its Parquet/metrics; build the Dagster glue (B) in Phase 1.

---

## 7. Success metrics (the spike's deliverable)

Over the 100k sample, report and break down by TLD/country:
- % **found in the CC index**; % **fetched OK** (200 + HTML).
- % with a **checksum-valid IČO**; % whose IČO **joins a registry**.
- % with **≥1 email**; % with **≥1 phone**; % with **≥1 social**.
- % with **≥1 detected technology**; top technologies.
- size of the **"regex found nothing" residual** → tells us whether an LLM fallback is worth it.

These numbers decide go/no-go for the full top-10M build.

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

---

## 9. Execution plan (phases)
- **Phase 0a — mechanics (≈1k domains), package only:** index lookup + range-fetch + WARC parse
  in the standalone package; confirm CC coverage and fetch reliability.
- **Phase 0b — extraction + report (100k), package only:** add IČO/contacts/socials/tech
  extraction + Wappalyzer; emit the 5 Parquet tables + metrics JSON; produce the hit-rate
  report. **Decision gate.** (No Dagster/ClickHouse yet — inspect the Parquet directly.)
- **Phase 1 (if go):** Dagster glue — manifest sharding, batch-partitioned orchestration of the
  package (Pipes), Parquet→ClickHouse load assets, `company_website_domains` linking; scale to
  top-10M; weekly schedule; then LLM/embedding industry inference + LLM fallback extraction.

**Gating dependency:** the `open_page_rank` dataset must be loaded (it supplies the target
list/manifests). Until then this stays a design.
