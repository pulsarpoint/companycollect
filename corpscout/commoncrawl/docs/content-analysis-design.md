# Content analysis — security signals + tracker IDs (design)

Two cheap, high-fit additions to the **tech pass**, harvested from bytes it *already* fetches and parses
(`processPage` has `http.Header` + body in hand and discards most of it). Marginal cost ≈ one header scan
and a few regexes per page. Companion to [`tech-mode-review-2026-07-02.md`](tech-mode-review-2026-07-02.md);
schema conventions per [`schema.md`](schema.md).

**Scope line (important):** extract **observable facts from the static crawl snapshot** (header
present/absent, disclosed version string, tracker id). **No active probing** — that's
`pulsarprotectrunner2`'s job and needs a live target; CommonCrawl is a historical snapshot. Everything here
is *as-of-crawl* — good for coverage/trend analytics, not real-time posture.

---

## Addition 1 — security signals → `commoncrawl_domain_security` (migration 078)

**What:** per-domain HTTP security-header hygiene + software-version disclosure, read from the response
headers we already parse. This is the security product's home turf — a posture dataset for tens of millions
of domains the active scanner can't produce cheaply, and it maps straight onto the existing issue-template
model (`PP_WEBAPP_CSP_MISSING`, `PP_WEBAPP_HSTS_MISSING`, …).

**Grain:** **1 row / domain**, taken from the **primary (representative) page's** response headers — the same
lowest-rank-survivor page that sets `primaryURL`. (Headers can vary by path; the homepage is the honest
domain-level representative. Per-page header capture is a possible v2, not v1.)

**Table** (`ReplacingMergeTree`, read with `FINAL`):

```sql
CREATE TABLE corpscout.commoncrawl_domain_security (
    crawl_id LowCardinality(String),
    root_domain String,
    source_url String,                       -- the page these headers came from (the primary)
    -- header presence (the hygiene score is sum of these)
    has_hsts UInt8,                          -- Strict-Transport-Security
    has_csp UInt8,                           -- Content-Security-Policy
    has_x_frame_options UInt8,
    has_x_content_type_options UInt8,
    has_referrer_policy UInt8,
    has_permissions_policy UInt8,
    -- version / fingerprint disclosure (verbatim, '' if header absent -> never NULL, see CLAUDE.md)
    server String,                           -- Server: Apache/2.4.29
    x_powered_by String,                     -- X-Powered-By: PHP/5.6
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, crawl_id);
```

**Extraction** — a pure `security.FromHeaders(http.Header) SecurityInfo` (new `internal/security` or a
function in `internal/extract`): six `h.Get(name) != ""` presence checks + the two verbatim version strings.
Trivial, no body scan.

**Analytics it unlocks** (and why it fits): `sum(has_*)` = a header-hygiene score; `server`/`x_powered_by`
join to a known-EOL/CVE list → **vuln-surface analytics for free** (we already extract `(tech, version)` via
Wappalyzer, so this extends the same idea to the server layer); coverage queries like "% of DE ecommerce
domains missing HSTS." Each `has_*=0` is a candidate row for the existing issue templates.

---

## Addition 2 — tracker / analytics IDs → `commoncrawl_domain_identifiers` (no schema change)

**What:** the marketing/analytics IDs embedded in page HTML — Google Analytics (`G-…`, legacy `UA-…`),
GTM container (`GTM-…`), AdSense publisher (`ca-pub-…` / `pub-…`), Facebook Pixel (numeric `fbq('init', …)`).

**Why this one matters most:** a **shared tracker id across two domains is a far stronger "same operator"
signal than a hyperlink** — it's the missing strong edge for the sibling/ownership graph
([graph discussion → `commoncrawl_domain_graph_signals` / `commoncrawl_domain_related`]). It turns
entity-resolution from "argued by links" into "proven by a shared account."

**Storage: reuse `commoncrawl_domain_identifiers` — zero migration.** The table already has
`id_type / id_value / valid / source / url / subdomain`, `ORDER BY (root_domain, id_type, id_value, url,
crawl_id)`, so it holds many per domain with per-page provenance. Just emit new `id_type` values:

| id_type | value | valid | source |
|---|---|---|---|
| `ga` | `G-XXXXXXX` | 1 (format) | `html` |
| `ua` | `UA-NNNNN-N` | 1 | `html` |
| `gtm` | `GTM-XXXXX` | 1 | `html` |
| `adsense` | `ca-pub-NNNNNNNN` | 1 | `html` |
| `fb_pixel` | `NNNNNNNNNN` | 1 | `html` |

**Extraction** — `extract.Trackers(body []byte) []model.Identifier` (one anchored regex per provider over the
full body; these ids have fixed, low-false-positive shapes). Appended to `pageResult.ids`, so they flow
through the existing id dedup + `IdentifierRow` write + load with **no new code path**.

**The ownership-graph tie-in (derived, later):** a query/materialized table pairing domains that share a
tracker id →candidate same-operator edges into `commoncrawl_domain_related`. **Caveat — weight by rarity:**
an id on 2 domains is a strong owner signal; an id on 500 is a marketing agency / shared GTM container →
noise. So: `GROUP BY id_type, id_value HAVING count(distinct root_domain) BETWEEN 2 AND ~20`, and treat
larger clusters as agency/network, not ownership.

---

## Integration (the `processPage`/`mergePageResults` refactor makes this clean)

The two-level FetchChunk split we just landed is exactly what makes this a small change:

- **`pageResult`** gains a `security SecurityInfo` field (populated only for the primary page) and its `ids`
  already accept the tracker `Identifier`s.
- **`processPage`** (tech/both): call `extract.Trackers(body)` → append to `r.ids`; if `it.Primary`, set
  `r.security = security.FromHeaders(headers)`.
- **`mergePageResults`**: capture `security` from the same lowest-rank-survivor page that sets `primaryURL`
  (trackers already merge via the existing id dedup).
- **`Finalize`**: fan one `SecurityRow` per domain (from `agg.security`); tracker ids ride the existing
  `IdentifierRow` fan-out.
- **`output`**: add `SecurityRow` + `WriteSecurity`; **`load`**: add `Tables["security"] =
  "commoncrawl_domain_security"` + to `Kinds`. Trackers need nothing (identifiers path unchanged).

## Migrations & tests

- **078** `commoncrawl_domain_security` (`.up`/`.down`); register in `EXPECTED_MIGRATIONS`; column-order
  contract test greps the migration (mirrors the other export-column tests).
- Trackers: **no migration** (new `id_type` values only).
- Unit tests: `FromHeaders` (present/absent matrix), `Trackers` (each provider + a negative), and a
  `mergePageResults` case asserting security comes from the primary-survivor page.

## Open decisions

1. **Security grain** — 1/domain from the primary page (recommended, v1) vs per-page (captures path-specific
   CSP; more rows). Start domain-level.
2. **Cookie security flags** (`Secure`/`HttpOnly`/`SameSite`) — cheap now that we parse cookies, but per-page
   × per-cookie; defer to v2 unless wanted in the first table.
3. **Tracker → graph** materialization — a `cc-crawl` step vs a standalone query, and the rarity threshold.
   Lean standalone + `2..20` distinct domains, tunable.
4. **Version→CVE join** — out of scope here (just store `server`/`x_powered_by`); the EOL/CVE mapping is a
   separate analytical layer.
