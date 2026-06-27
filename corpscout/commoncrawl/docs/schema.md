# CommonCrawl enrichment — ClickHouse schema

All tables live in the `corpscout` database and are prefixed `commoncrawl_`. They hold **only
crawl-derived, domain-level data** — what the `cc-enrich-worker` extracted from CommonCrawl pages for a
domain. We never have verified *company* data here; authoritative company facts (legal name,
jurisdiction, …) are a **separate, general concern** resolved from an identifier (LEI→GLEIF) and keyed
by that identifier — see [External company master](#external-company-master-out-of-scope).

## Conventions

- **Engine:** every table is `ReplacingMergeTree(resolved_at)` — re-running a crawl/run replaces a row
  with the newer `resolved_at`. Read with `FINAL` (or a `GROUP BY`/`argMax`) to collapse duplicates.
- **Grain** is stated per table: *one row per domain* (master/profile-style) vs *many rows per domain*
  (normalized: industries, contacts, identifiers, technologies).
- **Two worker passes** produce the data:
  - **industry pass** — fetches one primary page/domain, embeds it, classifies NACE.
  - **tech pass** — fetches many pages/domain, runs Wappalyzer + scrapes JSON-LD / regex.
  - `commoncrawl_domains` (the identity master) is written by **every** pass.
- **Lineage columns** (on every table):
  - `crawl_id` `LowCardinality(String)` — the CommonCrawl snapshot, e.g. `CC-MAIN-2026-05`.
  - `source_url` `String` — the page URL the row was derived from (constant-ish, compresses well).
  - `source_run_id` `String` — the worker run that produced it.
  - `resolved_at` `DateTime64(3,'UTC')` — when produced (also the ReplacingMergeTree version).
- **Identity columns:** `root_domain` (registered domain, the join key), `subdomain` (labels before the
  root, `''` for apex/www), `url` (the specific page) where relevant.
- Non-nullable `String`/`LowCardinality(String)` columns are written `''`, never `NULL`.

## Tables at a glance

| Table | Grain | Pass | What it holds |
|---|---|---|---|
| `commoncrawl_domains` | 1 / domain | every | identity master (the spine everything joins to) |
| `commoncrawl_industries` | many / domain | industry | NACE classification, one row per candidate code |
| `commoncrawl_page_signals` | 1 / domain | industry | page classification + NACE ranking quality |
| `commoncrawl_domain_metadata` | 1 / domain | tech | self-reported JSON-LD "about" (name, description, logo, country, founding, size) |
| `commoncrawl_domain_contact_info` | many / domain | tech | emails / phones / social links |
| `commoncrawl_domain_identifiers` | many / domain | tech | scraped registry codes (LEI/VAT/…) — the domain→identifier bridge |
| `commoncrawl_technologies` | many / domain | tech | detected technologies (Wappalyzer) |

`commoncrawl_company_profile` is **dropped** (its fields move to `domain_metadata` + `domain_contact_info`).
`commoncrawl_company_identifiers` is **renamed** to `commoncrawl_domain_identifiers` (same columns).

---

## `commoncrawl_domains`

The identity master / spine. One row per domain per crawl, written by every pass — guarantees every
enriched domain has a row to join to, regardless of which pass ran.

**Grain:** one row per `(root_domain, crawl_id)`. **Sort key:** `(root_domain, url, crawl_id)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | CommonCrawl snapshot id |
| `url` | `String` | primary page URL chosen for the domain |
| `root_domain` | `String` | registered domain (join key) |
| `subdomain` | `String` | sub-labels before the root, `''` for apex/www |
| `source_url` | `String` | same primary page URL (lineage) |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

---

## `commoncrawl_industries`

NACE industry classification from the industry pass. **One row per candidate code** — a domain can
legitimately map to several industries, so the top-N embedding matches are fanned into ranked rows
instead of arrays.

**Grain:** one row per `(root_domain, crawl_id, nace_code)`. **Sort key:** `(root_domain, crawl_id, nace_code)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id |
| `root_domain` | `String` | domain (join key) |
| `nace_code` | `String` | NACE code for this candidate, e.g. `62.01` |
| `nace_label` | `String` | human label for the code |
| `nace_division` | `LowCardinality(String)` | 2-digit division (prefix before the `.`), e.g. `62` |
| `rank` | `UInt8` | 1 = best match, 2, 3, … by score |
| `is_primary` | `UInt8` | 1 for the headline industry (rank 1), else 0 |
| `score` | `Float32` | cosine similarity of the page embedding to the code prototype |
| `nace_method` | `LowCardinality(String)` | how it was classified: `embedding` or `keyword` |
| `source_url` | `String` | page classified |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

Junk/parked pages (`nace_method='keyword'`, no embedding candidates) produce **no** rows here — only a
`page_signals` row.

---

## `commoncrawl_page_signals`

Per-domain page classification and the *quality* of the NACE decision from the industry pass. One row
per domain (the industry headline decision), separate from the multi-row industry candidates.

**Grain:** one row per `(root_domain, crawl_id)`. **Sort key:** `(root_domain, crawl_id)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id |
| `root_domain` | `String` | domain |
| `subdomain` | `String` | sub-labels / `''` |
| `source_url` | `String` | page classified |
| `page_type` | `LowCardinality(String)` | classified page type, e.g. `homepage`, `parked`, `for_sale`, `junk` |
| `page_type_score` | `Float32` | confidence of the page-type match |
| `nace_confident` | `UInt8` | 1 if the top NACE match cleared the confidence threshold |
| `nace_margin` | `Float32` | score gap between the top-1 and top-2 NACE candidates |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

---

## `commoncrawl_domain_metadata`  *(new)*

What a domain's pages say **about themselves** — schema.org/JSON-LD `Organization` + meta tags scraped
by the tech pass. **Self-reported, not verified** (a site can claim any `numberOfEmployees`). One row
per domain.

**Grain:** one row per `(root_domain, crawl_id)`. **Sort key:** `(root_domain, crawl_id)`.

| Column | Type | What goes in it | JSON-LD source |
|---|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id | |
| `root_domain` | `String` | domain | |
| `subdomain` | `String` | sub-labels / `''` | |
| `name` | `String` | site/brand name as the page declares it | `Organization.name` |
| `description` | `String` | self-description / tagline | `Organization.description` |
| `logo` | `String` | logo image URL | `Organization.logo` |
| `country` | `LowCardinality(String)` | ISO-2 when given as a code, else raw | `address.addressCountry` |
| `founding_year` | `UInt16` | self-reported founding year (`0` if absent) | `Organization.foundingDate` |
| `employee_count` | `UInt32` | self-reported headcount (`0` if absent) | `Organization.numberOfEmployees` |
| `source` | `LowCardinality(String)` | `jsonld` | |
| `source_url` | `String` | page scraped | |
| `source_run_id` | `String` | worker run id | |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version | |

---

## `commoncrawl_domain_contact_info`  *(new — renamed from migration 067 `company_contacts`)*

Contacts scraped from a domain's pages. **Many rows per domain** — a domain can have multiple emails,
phones, and social links. Replaces the old `commoncrawl_domains.emails` array and the profile's
single `email`/`phone`/`same_as`.

**Grain:** one row per `(root_domain, contact_type, value)`. **Sort key:** `(root_domain, contact_type, value)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id |
| `root_domain` | `String` | domain |
| `contact_type` | `LowCardinality(String)` | `email` \| `phone` \| `social` |
| `value` | `String` | the email address, phone number, or social-profile URL |
| `source` | `LowCardinality(String)` | `regex` (scraped from page text) or `jsonld` (structured) |
| `source_url` | `String` | page the contact came from |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

- `email` — from page-text regex (`source=regex`) and/or JSON-LD `email` (`source=jsonld`).
- `phone` — from JSON-LD `telephone` (`source=jsonld`).
- `social` — from JSON-LD `sameAs` URLs (LinkedIn / X / Wikidata / Crunchbase / …), `source=jsonld`.

Dedup is by `(root_domain, contact_type, value)`; the same email found by both regex and JSON-LD
collapses to one row.

---

## `commoncrawl_domain_identifiers`  *(renamed from `commoncrawl_company_identifiers`)*

Registry codes scraped from a domain's pages — the **domain→identifier bridge**. These are *raw codes*,
not resolved company records: `id_value` is the LEI/VAT/… string, `valid` is a format/checksum check.
Join `id_value` to the external company master to get authoritative company facts.

**Grain:** one row per `(root_domain, id_type, id_value, url)`. **Sort key:** `(root_domain, id_type, id_value, url, crawl_id)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id |
| `root_domain` | `String` | domain |
| `url` | `String` | page the code was found on |
| `subdomain` | `String` | sub-labels / `''` |
| `id_type` | `LowCardinality(String)` | `lei` \| `vat` \| `tax` \| `duns` \| `naics` |
| `id_value` | `String` | the raw code (e.g. an LEI like `CFD35BE52353A4AA3C06`) |
| `valid` | `UInt8` | 1 if it passed format/checksum validation |
| `source` | `LowCardinality(String)` | `jsonld` (structured) or `text` (regex over page text) |
| `source_url` | `String` | page the code came from |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

---

## `commoncrawl_technologies`

Technologies detected on a domain's pages by Wappalyzer (tech pass). **Many rows per domain** — one per
detected technology, unioned across the domain's crawled pages.

**Grain:** one row per `(root_domain, url, technology)`. **Sort key:** `(root_domain, url, technology, crawl_id)`.

| Column | Type | What goes in it |
|---|---|---|
| `crawl_id` | `LowCardinality(String)` | snapshot id |
| `url` | `String` | page the tech was detected on |
| `root_domain` | `String` | domain |
| `subdomain` | `String` | sub-labels / `''` |
| `technology` | `LowCardinality(String)` | technology name, e.g. `WordPress`, `Nginx` |
| `category` | `LowCardinality(String)` | Wappalyzer category, e.g. `CMS`, `Web servers` |
| `version` | `String` | detected version, `''` if unknown |
| `confidence` | `UInt8` | detection confidence (0–100) |
| `source_url` | `String` | page the tech came from |
| `source_run_id` | `String` | worker run id |
| `resolved_at` | `DateTime64(3,'UTC')` | produced-at / version |

---

## External company master *(out of scope here)*

Authoritative company facts — legal name, legal form, jurisdiction, HQ address, parent LEIs — are **not**
a CommonCrawl table and **not** domain-scoped. They are resolved from an identifier against a registry
(LEI→GLEIF, VAT→VIES, DUNS→D&B) and are **keyed by the identifier**: the same GLEIF record is valid no
matter how the LEI was found. That belongs in a **general/shared table** alongside the country-register
sources (`lv_companies`, `ee_companies`, …), populated by a dedicated resolver.

To attribute a domain to a company, join:

```
commoncrawl_domain_identifiers.id_value  =  <company_master>.lei   (where id_type = 'lei')
```

Nothing in `cc-enrich-worker` resolves identifiers today — it only scrapes the raw codes into
`commoncrawl_domain_identifiers`.
