# Canonical Contact/Domain Tables — Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the canonical-table standard's shared machinery (vocabularies, unified denylist, title-labels guard, primary-domain election, canonical row builders) and convert Czech + Latvia to the canonical `<src>_company_contacts` / `<src>_company_domains` pair, re-materialized live.

**Architecture:** The shared `contact_extraction.py` gains the spec-owned vocabularies and two canonical row builders (facts vs derived domains) plus an `is_primary` election helper; the old fused 9-tuple builder is deleted once both consumers migrate. Czech and Latvia each get one migration that drops the old fused contacts table and creates the canonical pair (data is fully recomputable per run), and their thin orchestrators write both tables. A schema-conformance test helper makes future sources unable to drift from the standard.

**Tech Stack:** Python 3.14 (`uv run`), ClickHouse (golang-migrate), existing shared module patterns.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` — canonical DDL, vocabularies, and decisions live THERE; this plan implements Phase A of its migration strategy.

## Global Constraints

- Work dir `corpscout/dagster_v3` (`uv run`); migrations in `corpscout/clickhouse/migrations/` (number = highest existing + 1 at execution time; was 000086 at planning).
- Canonical DDL exactly as in the spec's "Canonical DDL" section (column names, order, types, engine `ReplacingMergeTree(resolved_at)`, ORDER BY `(registry_id, contact_type, contact_value)` / `(registry_id, domain)`).
- Vocabularies (closed sets): contact_type `email|phone|mobile|fax|website|domain_in_name|other`; domain_source `website|email|name_embedded`; validation_method `''|commoncrawl|dns`. Confidence constants: website 1.0, unique-email 0.9, commoncrawl 0.95 (existing), dns 0.70 (existing).
- `is_primary` election: prefer website-sourced, then `is_current`, then highest confidence, then shortest domain, then alphabetical — exactly one winner per `registry_id` that has rows.
- Czech: `registry_id` = ico, `country_iso2='CZ'`, `source_field='name'`, slug `czech_ares`. Latvia: regcode, `'LV'`, `'legal_name'`, `latvia_ur`. `source_run_id=''` (not applicable for these full-recompute pipelines).
- Facts vs conclusions: `<src>_company_contacts` receives ALL guard-surviving candidates (validation-independent facts); `<src>_company_domains` receives only CommonCrawl/DNS-validated domains, deduped to one row per `(registry_id, domain)`.
- Estonia/Brazil code NOT touched except subset drift-guard tests (their import swaps are Phases B/C).
- Domain-graph assets NOT touched (Phase E).
- Regression: `uv run pytest tests/test_contact_extraction.py tests/test_czech_ares.py tests/test_latvia_ur_contacts.py tests/test_latvia_ur_assets.py -q` green after every task; `uv run dg check defs` green after asset-touching tasks; ruff clean on changed files. Known env quirks: dbt-manifest-dependent czech job test may fail in a fresh worktree (pre-existing/environmental); full-suite exclusions `--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py`.
- Live work happens against the lab ClickHouse (env from the MAIN checkout's `corpscout/dagster_v3/.env`); migrations wipe the cz/lv contact tables — Tasks 3/4 re-materialize them, so sequence matters.
- Conventional Commits.

---

### Task 1: Shared module — vocabularies, denylist, title guard, election, canonical builders

**Files:**
- Modify: `src/dagster_v3/contact_extraction.py`
- Test: `tests/test_contact_extraction.py`

**Interfaces:**
- Produces (consumed by Tasks 2–4):

```python
CONTACT_TYPE_VALUES = frozenset({"email", "phone", "mobile", "fax", "website", "domain_in_name", "other"})
DOMAIN_SOURCE_VALUES = frozenset({"website", "email", "name_embedded"})
VALIDATION_METHOD_VALUES = frozenset({"", "commoncrawl", "dns"})
WEBSITE_CONFIDENCE = 1.0
EMAIL_UNIQUE_CONFIDENCE = 0.9
EMAIL_PROVIDER_DENYLIST: frozenset[str]   # union of the Estonia + Brazil lists (30 entries)
EMAIL_DOMAIN_MAX_COMPANIES = 1

COMPANY_CONTACTS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "contact_type", "contact_type_raw", "contact_value",
    "source_field", "is_current", "valid_to", "source_url", "resolved_at",
)
COMPANY_DOMAINS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "domain", "domain_source", "validation_method", "confidence",
    "website_url", "website_normalized_url", "website_host",
    "is_current", "is_primary", "resolved_at",
)

def iter_contact_fact_rows(candidates_by_domain, *, country_iso2, source_slug,
                           source_field, resolved_at, source_run_id="") -> Iterator[tuple]
def iter_company_domain_rows(candidates_by_domain, *, commoncrawl_domains,
                             nameservers_by_domain, country_iso2, source_slug,
                             resolved_at, source_run_id="") -> Iterator[tuple]
def elect_primary_domains(rows: Iterable[tuple]) -> list[tuple]
```

- Keeps (deleted only in Task 4): `iter_valid_contact_rows` — Czech/Latvia still consume it until Tasks 3/4.
- Existing consumed pieces: `_validated_domain`, `_rejected_as_abbreviation`, `_LEGAL_FORM_LABELS`, `COMMONCRAWL_CONFIDENCE`, `DNS_CONFIDENCE`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_contact_extraction.py`:

```python
def test_vocabularies_are_closed_sets():
    assert contact_extraction.CONTACT_TYPE_VALUES == {
        "email", "phone", "mobile", "fax", "website", "domain_in_name", "other"
    }
    assert contact_extraction.DOMAIN_SOURCE_VALUES == {"website", "email", "name_embedded"}
    assert contact_extraction.VALIDATION_METHOD_VALUES == {"", "commoncrawl", "dns"}
    assert contact_extraction.WEBSITE_CONFIDENCE == 1.0
    assert contact_extraction.EMAIL_UNIQUE_CONFIDENCE == 0.9


def test_shared_denylist_superset_of_country_copies():
    # Drift guards until Estonia (Phase B) and Brazil (Phase C) swap their imports.
    from dagster_v3.defs.brazil_rfb import contacts as br
    from dagster_v3.defs.estonia_ar import resources as ee

    assert ee.EMAIL_PROVIDER_DENYLIST <= contact_extraction.EMAIL_PROVIDER_DENYLIST
    assert br.EMAIL_PROVIDER_DENYLIST <= contact_extraction.EMAIL_PROVIDER_DENYLIST
    assert ee.EMAIL_DOMAIN_MAX_COMPANIES == contact_extraction.EMAIL_DOMAIN_MAX_COMPANIES
    assert br.EMAIL_DOMAIN_MAX_COMPANIES == contact_extraction.EMAIL_DOMAIN_MAX_COMPANIES


def test_title_labels_guard_rejects_academic_title_domains():
    for text in ("Dr.Ing. Jan Novák", "Josef Svoboda, dipl.Ing.", "EUR.ING Karel Dvořák"):
        assert extract_contact_candidates(record_id="1", text=text, home_tlds=frozenset({"cz"})) == [], text


def test_title_labels_guard_keeps_brandlike_ing_domains():
    # all-labels rule: only fires when EVERY label is a title token.
    kept = extract_contact_candidates(record_id="1", text="boe.ing", home_tlds=frozenset())
    assert [c.domain for c in kept] == ["boe.ing"]
    kept = extract_contact_candidates(record_id="1", text="ing.cz s.r.o.", home_tlds=frozenset({"cz"}))
    assert [c.domain for c in kept] == ["ing.cz"]


def _fact_kwargs():
    return dict(country_iso2="CZ", source_slug="czech_ares", source_field="name",
                resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC))


def test_contact_fact_rows_cover_all_candidates_regardless_of_validation():
    candidates = {
        "asseco.cz": [
            ContactCandidate("123", "email", "info@asseco.cz", "asseco.cz"),
            ContactCandidate("123", "domain", "www.asseco.cz", "asseco.cz"),
        ],
        "never-validates.cz": [
            ContactCandidate("456", "domain", "never-validates.cz", "never-validates.cz"),
        ],
    }
    rows = list(contact_extraction.iter_contact_fact_rows(candidates, **_fact_kwargs()))
    assert len(rows) == 3  # facts are validation-independent
    by_value = {row[7]: row for row in rows}
    email = by_value["info@asseco.cz"]
    assert email[:7] == ("CZ", "czech_ares", "", "123", "123", "email", "")
    assert email[8:12] == ("name", 1, None, "")
    domain_fact = by_value["www.asseco.cz"]
    assert domain_fact[5] == "domain_in_name"
    assert len(email) == len(contact_extraction.COMPANY_CONTACTS_COLUMNS)


def test_company_domain_rows_validated_only_and_deduped_per_registry_domain():
    candidates = {
        "asseco.cz": [
            ContactCandidate("123", "email", "info@asseco.cz", "asseco.cz"),
            ContactCandidate("123", "domain", "www.asseco.cz", "asseco.cz"),
        ],
        "dnsonly.cz": [ContactCandidate("456", "domain", "dnsonly.cz", "dnsonly.cz")],
        "dead.cz": [ContactCandidate("789", "domain", "dead.cz", "dead.cz")],
    }
    rows = list(contact_extraction.iter_company_domain_rows(
        candidates,
        commoncrawl_domains={"asseco.cz"},
        nameservers_by_domain={"dnsonly.cz": ("ns1.x.cz",), "dead.cz": ()},
        country_iso2="CZ", source_slug="czech_ares",
        resolved_at=dt.datetime(2026, 7, 4, tzinfo=dt.UTC),
    ))
    assert len(rows) == 2  # asseco deduped to ONE row despite two candidates; dead dropped
    by_domain = {row[5]: row for row in rows}
    cc = by_domain["asseco.cz"]
    assert cc[6:9] == ("name_embedded", "commoncrawl", contact_extraction.COMMONCRAWL_CONFIDENCE)
    assert cc[9:12] == ("", "", "")   # no website columns for name-embedded
    assert cc[12:14] == (1, 0)        # is_current=1, is_primary decided by election
    dns = by_domain["dnsonly.cz"]
    assert dns[7:9] == ("dns", contact_extraction.DNS_CONFIDENCE)
    assert len(cc) == len(contact_extraction.COMPANY_DOMAINS_COLUMNS)


def test_elect_primary_domains_one_winner_per_registry():
    def row(registry, domain, source, confidence, current=1):
        return ("CZ", "s", "", registry, registry, domain, source, "commoncrawl",
                confidence, "", "", "", current, 0,
                dt.datetime(2026, 7, 4, tzinfo=dt.UTC))

    rows = [
        row("1", "bbb.cz", "name_embedded", 0.95),
        row("1", "aa.cz", "name_embedded", 0.95),    # shorter domain wins at equal confidence
        row("2", "low.cz", "name_embedded", 0.70),
        row("2", "site.cz", "website", 0.70),        # website source beats higher-ranked others
        row("3", "only.cz", "name_embedded", 0.70),
    ]
    elected = contact_extraction.elect_primary_domains(rows)
    primaries = {r[4]: r[5] for r in elected if r[13] == 1}
    assert primaries == {"1": "aa.cz", "2": "site.cz", "3": "only.cz"}
    assert sum(1 for r in elected if r[13] == 1) == 3
    assert len(elected) == len(rows)  # non-winners kept with is_primary=0
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_contact_extraction.py -q 2>&1 | tail -2` → FAIL (new names missing).

- [ ] **Step 3: Implement**

In `src/dagster_v3/contact_extraction.py`:

(a) Constants block (near the existing confidence constants):

```python
# Spec-owned vocabularies (docs/superpowers/specs/2026-07-04-company-contacts-
# domains-standard-design.md). Closed sets — extend only via the spec.
CONTACT_TYPE_VALUES = frozenset(
    {"email", "phone", "mobile", "fax", "website", "domain_in_name", "other"}
)
DOMAIN_SOURCE_VALUES = frozenset({"website", "email", "name_embedded"})
VALIDATION_METHOD_VALUES = frozenset({"", "commoncrawl", "dns"})
WEBSITE_CONFIDENCE = 1.0
EMAIL_UNIQUE_CONFIDENCE = 0.9

# Union of the Estonia + Brazil provider denylists (18 common + 6 each unique at
# consolidation time) — one source of truth; countries swap imports in their
# conversion phases (drift-guarded by tests until then).
EMAIL_PROVIDER_DENYLIST = frozenset({
    # ... implementer: exact union of estonia_ar/resources.py:104 and
    # brazil_rfb/contacts.py:17 frozensets — compute it, paste it sorted, and the
    # subset tests pin correctness ...
})
EMAIL_DOMAIN_MAX_COMPANIES = 1

COMPANY_CONTACTS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "contact_type", "contact_type_raw", "contact_value",
    "source_field", "is_current", "valid_to", "source_url", "resolved_at",
)
COMPANY_DOMAINS_COLUMNS = (
    "country_iso2", "source_slug", "source_run_id", "source_record_id",
    "registry_id", "domain", "domain_source", "validation_method", "confidence",
    "website_url", "website_normalized_url", "website_host",
    "is_current", "is_primary", "resolved_at",
)

_CONTACT_TYPE_BY_CANDIDATE_TYPE = {"domain": "domain_in_name", "email": "email"}
```

(b) Title guard — next to `_LEGAL_FORM_LABELS`:

```python
# Academic/professional titles that form junk domains under real TLDs (.ing is a
# gTLD, so "Dr.Ing." parses as dr.ing) — same all-labels rule as legal forms.
_TITLE_LABELS = frozenset(
    {"dr", "dipl", "eur", "rndr", "mudr", "judr", "phdr", "ing", "sc",
     "phd", "mba", "bsc", "msc", "mgr", "bc"}
)
```

In `_rejected_as_abbreviation`, wherever the legal-form clause tests "all labels ∈ `_LEGAL_FORM_LABELS`" (both the raw-match-labels check and the registrable-labels check added for co.ltd), extend to `all(label in _LEGAL_FORM_LABELS for ...) or all(label in _TITLE_LABELS for ...)` — read the current implementation and mirror its exact structure; do NOT merge the two frozensets (a mixed "co.dr" should not be rejected by accident of set union... actually mixed labels fail both all() checks either way — keep the sets separate for readability and per-clause tests).

(c) Canonical builders + election:

```python
def iter_contact_fact_rows(
    candidates_by_domain: dict[str, list[ContactCandidate]],
    *,
    country_iso2: str,
    source_slug: str,
    source_field: str,
    resolved_at: datetime,
    source_run_id: str = "",
) -> Iterator[tuple]:
    """Canonical <src>_company_contacts rows (COMPANY_CONTACTS_COLUMNS order) for
    ALL guard-surviving candidates — facts are validation-independent.
    """
    for domain in sorted(candidates_by_domain):
        for candidate in candidates_by_domain[domain]:
            yield (
                country_iso2,
                source_slug,
                source_run_id,
                candidate.record_id,
                candidate.record_id,
                _CONTACT_TYPE_BY_CANDIDATE_TYPE[candidate.contact_type],
                "",                      # contact_type_raw: no source label distinct from ours
                candidate.contact_value,
                source_field,
                1,                       # is_current
                None,                    # valid_to
                "",                      # source_url
                resolved_at,
            )


def iter_company_domain_rows(
    candidates_by_domain: dict[str, list[ContactCandidate]],
    *,
    commoncrawl_domains: set[str],
    nameservers_by_domain: dict[str, tuple[str, ...]],
    country_iso2: str,
    source_slug: str,
    resolved_at: datetime,
    source_run_id: str = "",
) -> Iterator[tuple]:
    """Canonical <src>_company_domains rows (COMPANY_DOMAINS_COLUMNS order) for
    validated domains only, one row per (registry_id, domain). is_primary is 0 —
    apply elect_primary_domains() on the collected rows.
    """
    for domain in sorted(candidates_by_domain):
        validation = _validated_domain(
            domain,
            commoncrawl_domains=commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
        )
        if validation is None:
            continue
        validation_method, confidence = validation
        seen_registry_ids: set[str] = set()
        for candidate in candidates_by_domain[domain]:
            if candidate.record_id in seen_registry_ids:
                continue
            seen_registry_ids.add(candidate.record_id)
            yield (
                country_iso2,
                source_slug,
                source_run_id,
                candidate.record_id,
                candidate.record_id,
                domain,
                "name_embedded",
                validation_method,
                confidence,
                "", "", "",              # website columns: not a website-sourced domain
                1,                       # is_current
                0,                       # is_primary — election pass sets the winner
                resolved_at,
            )


def elect_primary_domains(rows: Iterable[tuple]) -> list[tuple]:
    """Set is_primary=1 on exactly one row per registry_id (spec election rule:
    website-sourced first, then is_current, then confidence, then shortest domain,
    then alphabetical). Rows are COMPANY_DOMAINS_COLUMNS-ordered tuples.
    """
    materialized = [list(row) for row in rows]
    best_index_by_registry: dict[str, int] = {}
    for index, row in enumerate(materialized):
        registry_id = row[4]
        current_best = best_index_by_registry.get(registry_id)
        if current_best is None or _election_key(row) > _election_key(materialized[current_best]):
            best_index_by_registry[registry_id] = index
    for index in best_index_by_registry.values():
        materialized[index][13] = 1
    return [tuple(row) for row in materialized]


def _election_key(row: Sequence[Any]) -> tuple:
    domain = row[5]
    return (row[6] == "website", bool(row[12]), row[8], -len(domain), _reverse_alpha(domain))


def _reverse_alpha(domain: str) -> tuple[int, ...]:
    # max() with this key picks the alphabetically FIRST domain on final tie.
    return tuple(-ord(ch) for ch in domain)
```

NOTE for the `iter_valid_contact_rows` docstring: add one line "DEPRECATED — replaced by iter_contact_fact_rows/iter_company_domain_rows; deleted when the last consumer migrates (Phase A Tasks 3-4)." Do not delete it yet.

- [ ] **Step 4: Verify + commit**

```bash
uv run pytest tests/test_contact_extraction.py tests/test_czech_ares.py tests/test_latvia_ur_contacts.py -q
uv run ruff check src/dagster_v3/contact_extraction.py tests/test_contact_extraction.py
git add -A src/dagster_v3/contact_extraction.py tests/test_contact_extraction.py
git commit -m "feat(dagster): canonical contact/domain vocabularies, builders, election, title guard"
```

All green (old builder untouched, so Czech/Latvia suites still pass).

---

### Task 2: Canonical cz/lv migrations + schema-conformance test helper

**Files:**
- Create: `corpscout/clickhouse/migrations/0000NN_corpscout_cz_canonical_contacts.up.sql` + `.down.sql` (NN = highest+1)
- Create: `corpscout/clickhouse/migrations/0000NN+1_corpscout_lv_canonical_contacts.up.sql` + `.down.sql`
- Create: `tests/canonical_contact_tables.py` (importable helper, NOT test-prefixed)
- Test: `tests/test_canonical_contact_migrations.py`
- Modify: `tests/test_clickhouse_migrations.py` (two `EXPECTED_MIGRATIONS` entries)

**Interfaces:**
- Consumes: spec's Canonical DDL section (authoritative shapes).
- Produces: live tables `cz_company_contacts` (canonical), `cz_company_domains`, `lv_company_contacts` (canonical), `lv_company_domains`; helper `assert_canonical_contacts_ddl(sql: str, table: str)` / `assert_canonical_domains_ddl(sql: str, table: str)` used by Tasks 3–4 tests and all future phases.

- [ ] **Step 1: Write the cz migration**

Up (`0000NN_corpscout_cz_canonical_contacts.up.sql`):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.cz_company_contacts;

CREATE TABLE IF NOT EXISTS corpscout.cz_company_contacts
(
    country_iso2      LowCardinality(String),
    source_slug       LowCardinality(String),
    source_run_id     String,
    source_record_id  String,
    registry_id       String,
    contact_type      LowCardinality(String),
    contact_type_raw  LowCardinality(String),
    contact_value     String,
    source_field      LowCardinality(String),
    is_current        UInt8,
    valid_to          Nullable(Date),
    source_url        String,
    resolved_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, contact_type, contact_value);

CREATE TABLE IF NOT EXISTS corpscout.cz_company_domains
(
    country_iso2           LowCardinality(String),
    source_slug            LowCardinality(String),
    source_run_id          String,
    source_record_id       String,
    registry_id            String,
    domain                 String,
    domain_source          LowCardinality(String),
    validation_method      LowCardinality(String),
    confidence             Float32,
    website_url            String,
    website_normalized_url String,
    website_host           String,
    is_current             UInt8,
    is_primary             UInt8,
    resolved_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, domain);
```

Down: drop both canonical tables, recreate the OLD fused `cz_company_contacts` shape (copy the CREATE from `000083_corpscout_cz_company_contacts.up.sql` verbatim). The DROP is safe: the table is fully recomputed on every run (Task 3 re-materializes immediately after).

The lv migration is identical with `lv_` table names (old shape source for the down: `000086_corpscout_lv_company_contacts.up.sql`, `regcode` fused shape).

- [ ] **Step 2: Conformance helper + tests**

`tests/canonical_contact_tables.py`:

```python
"""Schema-conformance helpers for the canonical contact/domain table standard.

Any <src>_company_contacts / <src>_company_domains migration must match the
canonical DDL modulo table name — new sources cannot drift (spec: Testing).
"""

import re

from dagster_v3.contact_extraction import COMPANY_CONTACTS_COLUMNS, COMPANY_DOMAINS_COLUMNS

_CONTACTS_TYPES = {
    "country_iso2": "LowCardinality(String)", "source_slug": "LowCardinality(String)",
    "source_run_id": "String", "source_record_id": "String", "registry_id": "String",
    "contact_type": "LowCardinality(String)", "contact_type_raw": "LowCardinality(String)",
    "contact_value": "String", "source_field": "LowCardinality(String)",
    "is_current": "UInt8", "valid_to": "Nullable(Date)", "source_url": "String",
    "resolved_at": "DateTime64(3, 'UTC')",
}
_DOMAINS_TYPES = {
    "country_iso2": "LowCardinality(String)", "source_slug": "LowCardinality(String)",
    "source_run_id": "String", "source_record_id": "String", "registry_id": "String",
    "domain": "String", "domain_source": "LowCardinality(String)",
    "validation_method": "LowCardinality(String)", "confidence": "Float32",
    "website_url": "String", "website_normalized_url": "String", "website_host": "String",
    "is_current": "UInt8", "is_primary": "UInt8", "resolved_at": "DateTime64(3, 'UTC')",
}


def _extract_create(sql: str, table: str) -> str:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS corpscout\.{re.escape(table)}\s*\((.*?)\)\s*ENGINE\s*=\s*([^;]+);",
        sql, re.DOTALL,
    )
    assert match, f"no canonical CREATE for corpscout.{table}"
    return match


def _assert_ddl(sql, table, columns, types, order_by):
    match = _extract_create(sql, table)
    body, engine = match.group(1), " ".join(match.group(2).split())
    parsed = [line.strip().rstrip(",") for line in body.strip().splitlines() if line.strip()]
    names = [p.split()[0] for p in parsed]
    assert names == list(columns), f"{table}: column order mismatch: {names}"
    for parsed_line, name in zip(parsed, names):
        declared = " ".join(parsed_line.split()[1:])
        assert declared == types[name], f"{table}.{name}: {declared!r} != {types[name]!r}"
    assert engine == f"ReplacingMergeTree(resolved_at) ORDER BY {order_by}", engine


def assert_canonical_contacts_ddl(sql: str, table: str) -> None:
    _assert_ddl(sql, table, COMPANY_CONTACTS_COLUMNS, _CONTACTS_TYPES,
                "(registry_id, contact_type, contact_value)")


def assert_canonical_domains_ddl(sql: str, table: str) -> None:
    _assert_ddl(sql, table, COMPANY_DOMAINS_COLUMNS, _DOMAINS_TYPES,
                "(registry_id, domain)")
```

(Adapt `_assert_ddl`'s engine/ORDER BY parsing to the real file formatting — the assertion CONTRACT is: exact column names in canonical order, exact types, ReplacingMergeTree(resolved_at), exact ORDER BY. If regex parsing fights the formatting, normalize whitespace first; keep the helper strict.)

`tests/test_canonical_contact_migrations.py`:

```python
from pathlib import Path

from tests.canonical_contact_tables import (
    assert_canonical_contacts_ddl,
    assert_canonical_domains_ddl,
)

_MIGRATIONS = Path(__file__).joinpath("../../../clickhouse/migrations").resolve()


def _read(pattern: str) -> str:
    matches = sorted(_MIGRATIONS.glob(pattern))
    assert matches, pattern
    return matches[-1].read_text()


def test_cz_canonical_migration_conforms():
    sql = _read("*_corpscout_cz_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "cz_company_contacts")
    assert_canonical_domains_ddl(sql, "cz_company_domains")


def test_lv_canonical_migration_conforms():
    sql = _read("*_corpscout_lv_canonical_contacts.up.sql")
    assert_canonical_contacts_ddl(sql, "lv_company_contacts")
    assert_canonical_domains_ddl(sql, "lv_company_domains")
```

(Import path note: `tests/` is not a package in some layouts — check how existing tests import test-local helpers; if plain `from tests....` fails, use the repo's conftest/pythonpath convention. The helper must remain importable by future phases' tests.)

- [ ] **Step 3: Ledger entries, contract test, live apply, smoke**

Append both migration names to `EXPECTED_MIGRATIONS`; `uv run pytest tests/test_clickhouse_migrations.py tests/test_canonical_contact_migrations.py -q` green. Apply live (`cd corpscout && make clickhouse-migrate-up`, creds from main checkout's `dagster_v3/.env` — see the Makefile's `CLICKHOUSE_MIGRATE_URL`). Smoke: all four tables exist with 0 rows (`cz/lv_company_contacts` were dropped+recreated — expected; Tasks 3/4 refill). Old czech/latvia writer tests will now be red against the live schema but NOT against unit fakes — the pytest suites must still pass (they use fakes; if any test asserts the OLD migration file shape, that file still exists on disk unchanged, so it still passes — verify).

- [ ] **Step 4: Commit**

```bash
git add corpscout/clickhouse/migrations/ corpscout/dagster_v3/tests/
git commit -m "feat(clickhouse): canonical cz/lv company contacts and domains tables"
```

---

### Task 3: Czech writer → canonical pair + live re-materialization

**Files:**
- Modify: `src/dagster_v3/defs/czech_ares/contacts.py`, `src/dagster_v3/defs/czech_ares/tables.py` (add `QUALIFIED_COMPANY_DOMAINS_TABLE = "corpscout.cz_company_domains"`, `REGISTRY_ID_TYPE = "ico"`; retire `CZ_COMPANY_CONTACTS_EXPORT_COLUMNS` in favor of the shared column tuples — read tables.py first and keep its conventions)
- Modify: `src/dagster_v3/defs/czech_ares/assets.py` (metadata keys from the new counts dict)
- Test: `tests/test_czech_ares.py`

**Interfaces:**
- Consumes: Task 1's builders/election/columns; Task 2's live tables.
- Produces: `replace_czech_company_contacts_clickhouse(...) -> dict` with keys `{"contact_facts", "domains", "primary_domains", "commoncrawl_validated", "dns_validated"}`.

- [ ] **Step 1: Rework the orchestrator**

Replace the tail of `replace_czech_company_contacts_clickhouse` (from `contact_rows = iter_valid_contact_rows(...)` onward) with:

```python
    fact_rows = iter_contact_fact_rows(
        candidates_by_domain,
        country_iso2="CZ",
        source_slug=CONTACTS_SOURCE_SLUG,
        source_field="name",
        resolved_at=resolved_timestamp,
    )
    domain_rows = elect_primary_domains(
        iter_company_domain_rows(
            candidates_by_domain,
            commoncrawl_domains=found_commoncrawl_domains,
            nameservers_by_domain=nameservers_by_domain,
            country_iso2="CZ",
            source_slug=CONTACTS_SOURCE_SLUG,
            resolved_at=resolved_timestamp,
        )
    )

    contact_facts = replace_contact_table(
        clickhouse_client,
        qualified_table=tables.QUALIFIED_COMPANY_CONTACTS_TABLE,
        columns=COMPANY_CONTACTS_COLUMNS,
        rows=fact_rows,
        log=log,
    )
    replace_contact_table(
        clickhouse_client,
        qualified_table=tables.QUALIFIED_COMPANY_DOMAINS_TABLE,
        columns=COMPANY_DOMAINS_COLUMNS,
        rows=domain_rows,
        log=log,
    )
    return {
        "contact_facts": contact_facts,
        "domains": len(domain_rows),
        "primary_domains": sum(1 for row in domain_rows if row[13] == 1),
        "commoncrawl_validated": sum(1 for row in domain_rows if row[7] == "commoncrawl"),
        "dns_validated": sum(1 for row in domain_rows if row[7] == "dns"),
    }
```

Delete `_tally_contact_rows` (superseded — counts come from the materialized `domain_rows` list) and its test; update imports. Update the asset's MaterializeResult metadata in `assets.py` to the new keys (read the current asset code and keep its shape).

- [ ] **Step 2: Update tests**

In `tests/test_czech_ares.py`: replace the old columns-match-migration test with the conformance helper (`assert_canonical_contacts_ddl`/`assert_canonical_domains_ddl` against the NEW cz migration file); add an orchestrator test with a fake ClickHouse client (existing fake pattern) asserting BOTH tables are replaced (two stage/EXCHANGE sequences, correct qualified names, correct column lists) and the counts dict shape; drop `test_tally_contact_rows_*`.

- [ ] **Step 3: Verify + live re-materialize**

```bash
uv run pytest tests/test_czech_ares.py tests/test_contact_extraction.py -q
uv run dg check defs 2>&1 | tail -1
uv run ruff check src/dagster_v3/defs/czech_ares/ tests/test_czech_ares.py
```

Then re-run the Czech orchestrator live (same pattern as previous re-materializations: `uv run python` script building a `clickhouse_driver.Client` from the main checkout's `.env`, calling `replace_czech_company_contacts_clickhouse`). Report counts; sanity: contact_facts ≥ domains; primary_domains == distinct registry_ids in cz_company_domains; `SELECT count() FROM corpscout.cz_company_domains WHERE is_primary = 1` equals `SELECT countDistinct(registry_id)` from same. Expect domains ≈ 4,600–4,700 minus the ~60 title-guard rows (dr.ing etc. now rejected).

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/czech_ares/ tests/test_czech_ares.py
git commit -m "feat(dagster): czech contacts write canonical contact/domain pair"
```

---

### Task 4: Latvia writer → canonical pair, old builder deleted, docs, full verification

**Files:**
- Modify: `src/dagster_v3/defs/latvia_ur/contacts.py` (same rework as Czech: country `'LV'`, `source_field='legal_name'`, add `QUALIFIED_LV_DOMAINS_TABLE = "corpscout.lv_company_domains"`, `REGISTRY_ID_TYPE = "regcode"`; counts dict same keys — read the post-Task-3 czech module and mirror exactly)
- Modify: `src/dagster_v3/contact_extraction.py` (DELETE `iter_valid_contact_rows` — Latvia was its last consumer)
- Modify: `src/dagster_v3/defs/latvia_ur/assets.py` if the asset carries metadata keys (read it)
- Test: `tests/test_latvia_ur_contacts.py` (conformance-helper test for the lv migration; orchestrator both-tables test), `tests/test_contact_extraction.py` (remove the old fused-builder test)
- Modify: `docs/data-source-guidelines.md` (§8b: contacts sections now say "write the canonical `<src>_company_contacts` + `<src>_company_domains` pair — shapes owned by the standard spec; conformance helper `tests/canonical_contact_tables.py`") and the standard spec's Phase A row if it tracks status (1 line)

**Interfaces:**
- Consumes: everything above.
- Produces: Phase A complete; `iter_valid_contact_rows` gone from the shared module.

- [ ] **Step 1: Rework Latvia** exactly like Czech Task 3 Step 1 (substitute LV constants + table names — the czech module is the reference implementation after Task 3; repeat of the code intentionally omitted only because the czech file IS the template to read, not from an earlier plan section).

- [ ] **Step 2: Delete the deprecated builder** `iter_valid_contact_rows` + its test; `rg -n 'iter_valid_contact_rows' src tests` must return nothing.

- [ ] **Step 3: Docs touches** per Files above.

- [ ] **Step 4: Verify everything + live re-materialize Latvia**

```bash
uv run pytest tests/test_contact_extraction.py tests/test_czech_ares.py tests/test_latvia_ur_contacts.py tests/test_latvia_ur_assets.py tests/test_canonical_contact_migrations.py -q
uv run pytest --ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py -q 2>&1 | tail -2
uv run dg check defs 2>&1 | tail -1
uv run ruff check src/dagster_v3/ tests/ 2>&1 | tail -1
```

Live re-materialize Latvia (same pattern); sanity checks as Czech (expect ≈1,600–1,650 facts; domains slightly fewer; one primary per registry_id). Also verify the OLD junk stays gone: `SELECT count() FROM corpscout.lv_company_domains WHERE domain IN ('v.ltd','co.ltd','a.group')` → 0, and `SELECT count() FROM corpscout.cz_company_domains WHERE domain IN ('dr.ing','dipl.ing')` → 0 (title guard live).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/ tests/ docs/
git commit -m "feat(dagster): latvia canonical contact/domain pair; retire fused contact rows builder"
```

---

## Deployment note (not a code task)

After merge + deploy to the dagster box: both register jobs now write the canonical pairs on schedule. The domain graph still reads NOTHING from cz/lv (unchanged until Phase E) — no downstream impact. Phases B (Estonia), C (Brazil), D (NO/FI/wikidata), E (graph switch + deprecations) follow as separate plans against the standard spec.
