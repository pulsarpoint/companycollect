# Content analysis — capture per-page signals (design)

Harvest the cheap-to-extract signals from bytes the **tech pass already fetches** (`processPage` has
`http.Header` + body in hand and discards most of it). Companion to
[`tech-mode-review-2026-07-02.md`](tech-mode-review-2026-07-02.md); schema conventions per
[`schema.md`](schema.md).

## Principle: collection-first, capture everything cheap, analyze later

The S3 fetch + gzip + HTML parse is the **one-time expensive cost**, paid once per page. Extracting one more
thing from bytes already in memory is ≈ free. The asymmetry is brutal: a signal you **skip** now is one you
must **re-crawl** for later. So:

- **Capture completely now, defer all analysis.** Store the raw structured signal (full header map, all meta,
  all IDs); scoring, the `PP_WEBAPP_*` issue mapping, and CVE/EOL joins are **downstream CPU passes over the
  stored data** (like re-classification runs over stored embeddings without re-embedding). Not in scope here.
- **Don't re-derive what CommonCrawl already publishes.** CC ships the **URL index** (→ worklists) and the
  **webgraph** (domain↔domain links + ranks). The graph is CC's job and is built from the *whole* crawl —
  extracting links from our ~25 fetched pages/domain would be a worse, redundant subset. So this pass
  captures **only per-page content signals CC does *not* publish** (§ below); the graph is a **separate
  direct ingest** (§4).
- **The one storage line:** cheap *structured* signals (headers, meta, ids) → store fully; they're tiny and
  compress hard. The big blobs — full page HTML / visible text — **are not stored**; they're re-fetchable
  exactly anytime via the WARC coords we already keep (`warc_filename/offset/length`). Nothing is skipped —
  it's either captured cheaply or reconstructable.

**Scope line:** observable facts from the **static snapshot** only. **No active probing** — that's
`pulsarprotectrunner2`'s job (needs a live target). Everything is *as-of-crawl*.

## What to capture (per-page content CC does not publish)

| Layer | Capture | Status |
|---|---|---|
| HTTP | **all response headers** (map), all Set-Cookie flags | new |
| HTML head | title, **all `<meta>`** (description/keywords/generator/robots/viewport/og:*/twitter:*), canonical, **all hreflang**, charset, favicon | new |
| Structured data | **all JSON-LD `@type`s** + Organization firmographics | partly have (Organization only) |
| Identifiers | **all analytics/ad/tag IDs** (GA/UA/GTM/AdSense/FB/Hotjar/Segment/Matomo/Mixpanel/Yandex/TikTok) + LEI/VAT | LEI/VAT have |
| Tech / contacts | Wappalyzer, emails/phones/social | have |
| ~~Outbound links~~ | ~~linked domains~~ — **CC webgraph, pulled directly (§4), not parsed here** | — |

---

## 1. Security signals → `commoncrawl_domain_security` (migration 078)

**Capture the full response-header map**, not a hand-picked set of flags — if next month we care about a
header we didn't flag, it's already there, no re-crawl. **1 row / domain**, from the primary
(lowest-rank-survivor) page's headers.

```sql
CREATE TABLE corpscout.commoncrawl_domain_security (
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,                                  -- the page these headers came from (the primary)
    headers Map(LowCardinality(String), String),        -- ALL response headers, lowercased names
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
```

Everything derives from `headers` later (all in ClickHouse SQL, no re-crawl): hygiene score
(`arrayExists`/`mapContains` over `content-security-policy`, `strict-transport-security`,
`x-frame-options`, …), version disclosure (`headers['server']`, `headers['x-powered-by']`), cookie flags
(parse `headers['set-cookie']`). Header maps are small and highly compressible — storing all of them costs
almost nothing and skips nothing.

**Extraction:** `security.HeaderMap(http.Header) map[string]string` — lowercase each name, join multi-values
with `, ` (join Set-Cookie with `\n`). Pure, no body scan.

## 2. HTML-head signals → `commoncrawl_domain_page_meta` (migration 079)

**Capture all `<meta>` + head links**, not a fixed subset. **1 row / domain** (primary page).

```sql
CREATE TABLE corpscout.commoncrawl_domain_page_meta (
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,
    title String,
    meta Map(LowCardinality(String), String),   -- name/property -> content (description, og:*, twitter:*, generator, robots, viewport, …)
    canonical String,
    hreflang Array(LowCardinality(String)),      -- declared language/region alternates
    jsonld_types Array(LowCardinality(String)),  -- every @type seen (Organization, Product, JobPosting, …)
    charset LowCardinality(String),
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
```

`jsonld_types` is the cheap win over today's Organization-only extraction (a `JobPosting` present = hiring
signal; `Product` = ecommerce) without storing the raw JSON-LD blocks. **Extraction:** `parse.HeadMeta(body)
→ HeadMeta{title, meta, canonical, hreflang, charset}` (one `x/net/html` head walk) + collect `@type`s while
`extract.ExtractProfile` already walks the JSON-LD blocks.

## 3. Tracker / analytics IDs → `commoncrawl_domain_identifiers` (no migration)

Don't hand-maintain a tracker list — two maintained sources cover the two halves:

**Half A — which trackers exist (presence): Wappalyzer, already vendored.** `fingerprints_data.json`
(7,540 apps) has the maintained categories that *are* the tracker list: **Analytics (426), Advertising
(224), Marketing automation (544)**. Presence comes free from the tech detection already running — refreshed
by bumping `wappalyzergo`, zero maintenance.

**Half B — the account ID value (the ownership signal): Wappalyzer version-capture + a small curated
fallback.** Some Wappalyzer fingerprints capture the id via a version group (verified: Google Analytics ✅,
Facebook Pixel ✅), many don't (GTM ❌, Hotjar ❌). So: **reuse Wappalyzer's captured `version` as the id
where present**, and add a **~15-provider curated regex fallback** only for the ones it misses. These id
shapes are fixed and stable, so a short hardcoded table is more precise than a generic feed:

| id_type | value shape | source |
|---|---|---|
| `ga` | `G-XXXXXXX` / `ua` `UA-NNNNN-N` | Wappalyzer version-capture |
| `fb_pixel` | numeric | Wappalyzer version-capture |
| `gtm` | `GTM-XXXXX` | curated regex |
| `adsense` | `ca-pub-NNNN…` | curated regex |
| `hotjar` / `segment` / `matomo` / `mixpanel` / `yandex` / `tiktok_pixel` / `clarity` / `linkedin_insight` / `pinterest_tag` / `snap_pixel` | provider id | curated regex |

Storage: **existing `commoncrawl_domain_identifiers`** — the table already carries
`id_type / id_value / valid / source / url / subdomain`, `ORDER BY (root_domain, id_type, id_value, url,
crawl_id)`, so ids ride the current fan-out. **Extraction:** the tech pass already returns Wappalyzer
`Technology{Name, Version}`; map the id-bearing ones to `Identifier{Type, Value: Version, Source:
"wappalyzer"}`, plus `extract.Trackers(body) []model.Identifier` for the curated fallback
(`Source: "html"`). Appended to `pageResult.ids` — **no new code path**.

**Ownership tie-in (derived, later): DDG Tracker Radar for owner mapping.** Raw "shared id" is refined two
ways: (a) rarity — `GROUP BY id_type, id_value HAVING count(distinct root_domain) BETWEEN 2 AND ~20` (a
larger cluster is an agency / shared GTM container, not ownership); (b) join tracker *domains* against a
`commoncrawl_tracker_owners` reference table (§3a) to lift "same id" → "same **owner entity**" and to
**exclude** known third-party trackers/CDNs/consent-managers that would otherwise fabricate sibling edges.

### 3a. `commoncrawl_tracker_owners` — reference, from DDG Tracker Radar (migration 080)

A small reference table (tens of thousands of rows), loaded from **[DuckDuckGo Tracker
Radar](https://github.com/duckduckgo/tracker-radar)** (permissive license, updated regularly) — the
best-maintained tracker-domain → owning-company map (also powers DDG's blocker).

```sql
CREATE TABLE corpscout.commoncrawl_tracker_owners (
    tracker_domain String,                 -- e.g. google-analytics.com
    owner_name String,                      -- owning company
    owner_display String,
    categories Array(LowCardinality(String)),
    prevalence Float32,                     -- how common (used to gate 3rd-party exclusion)
    resolved_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (tracker_domain);
```

Loaded by a small script (like `load-domain-ranks.sh`) that walks the Tracker Radar `domains/*.json`. Not
part of the tech pass — a reference dataset refreshed on its own cadence.

---

## 4. Graph edges — CommonCrawl webgraph, pulled directly (NOT this pass)

The domain↔domain edge graph is **CC's**, from the same webgraph release as the `*-domain-ranks` you already
load. Pull the companion **edges** file the same way as
[`load-domain-ranks.sh`](../load-domain-ranks.sh) (clickhouse-local, un-reverse hosts), **filtered to your
domains + 1 hop** (the full edge list is billions of rows), into a `commoncrawl_domain_links` edge table.
This is a separate ingest — decoupled from the tech pass — and together with the tracker-ID edges (§3) is the
ownership/sibling-discovery substrate. Design: the graph section of `embeddings-design.md` / the earlier
graph discussion. **Do not parse links from pages** — it would be a redundant, ~25-page subset of this.

---

## Integration (fits the `processPage`/`mergePageResults` seams)

- **`pageResult`** gains `security map[string]string`, `meta HeadMeta` (populated only on the primary page);
  its `ids` already accept the tracker `Identifier`s.
- **`processPage`** (tech/both): `extract.Trackers(body)` → append to `r.ids`; if `it.Primary`:
  `r.security = security.HeaderMap(headers)`, `r.meta = parse.HeadMeta(body)`.
- **`mergePageResults`**: capture `security`/`meta` from the same lowest-rank-survivor page that sets
  `primaryURL` (trackers already merge via the id dedup).
- **`Finalize`**: fan one `SecurityRow` + one `PageMetaRow` per domain; tracker ids ride the existing
  `IdentifierRow` fan-out.
- **`output`**: add `SecurityRow`/`PageMetaRow` + `WriteSecurity`/`WritePageMeta`. **`load`**: add
  `Tables["security"]`/`Tables["page_meta"]` + to `Kinds`. Trackers: nothing.

## Migrations & tests

- **078** `commoncrawl_domain_security`, **079** `commoncrawl_domain_page_meta`, **080**
  `commoncrawl_tracker_owners` (`.up`/`.down`); register all in `EXPECTED_MIGRATIONS`; column-order contract
  tests grep each migration (mirror the other export tests). ClickHouse `Map`/`Array` columns are fine
  (native driver `AppendStruct` handles `map[string]string` / `[]string`).
- Trackers into `commoncrawl_domain_identifiers`: **no migration** (new `id_type` values only).
- Unit tests: `HeaderMap` (multi-value join, Set-Cookie), `HeadMeta` (title/meta/canonical/hreflang/charset +
  a malformed-HTML case), `Trackers` (each provider + a negative), and a `mergePageResults` case asserting
  security/meta come from the primary-survivor page.

## Deferred (explicitly out of scope — later CPU passes over stored data)

- Hygiene **scoring** + `PP_WEBAPP_*` issue-template mapping (there is far more to systematize here; do it
  once collection is complete, not inline).
- Server/tech **version → EOL/CVE** join.
- Cookie-flag breakout, full JSON-LD block storage, per-page (vs per-domain) capture.
