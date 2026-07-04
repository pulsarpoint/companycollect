# Latvia Company Contacts + Shared Contact Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract company domains embedded in Latvian legal names into a new `corpscout.lv_company_contacts` table, powered by a new shared contact-extraction module (with IDN support) that Czech is rewired onto.

**Architecture:** Generic machinery moves out of `czech_ares/contacts.py` into `src/dagster_v3/contact_extraction.py` (sibling of the existing shared `domains.py`): candidate regexes (IDN-extended), CommonCrawl/DNS domain validation (idna-aware), and the atomic stage/EXCHANGE table replace. Czech becomes a thin consumer (its full test suite is the regression guarantee). Latvia gets a mirrored migration (`regcode` for `ico`) and a `defs/latvia_ur/contacts.py` asset as the third leaf of the register job.

**Tech Stack:** Python 3.14 (dagster, clickhouse via `ClickhouseResource`/clickhouse_driver, tldextract, dnspython, idna), ClickHouse (golang-migrate migrations).

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-lv-company-contacts-shared-extraction-design.md`

## Global Constraints

- Work dir: `corpscout/dagster_v3` (`uv run` for everything); migrations in `corpscout/clickhouse/migrations/`.
- Regression guarantee: `uv run pytest tests/test_czech_ares.py` fully green after every task.
- IDN behavior (user-confirmed mandatory): domain/email label character classes accept `À-ſ` (Latin-1 Supplement + Latin Extended-A — covers Latvian/Czech diacritics) in addition to `[A-Z0-9-]`; domains stored lowercase unicode; DNS validation idna-encodes (`import idna`; on encode failure the domain is dropped); CommonCrawl lookup tries both unicode and idna forms. ASCII inputs must extract byte-identically to the old Czech regex (test-pinned).
- Validation semantics unchanged from Czech: CommonCrawl hit → `confidence 0.95, domain_source 'commoncrawl'`; DNS parent-zone NS fallback → `0.70, 'dns'`; unresolvable dropped.
- Write semantics unchanged: full recompute per run, batched insert into a stage table, `EXCHANGE TABLES`, drop stage in `finally`.
- Latvia table exactly per spec: `lv_company_contacts` with `regcode` replacing `ico`, `ReplacingMergeTree(resolved_at)`, `ORDER BY (regcode, contact_type, contact_value)`; `source_slug='latvia_ur'`.
- Migration rules: `.up.sql` starts `CREATE DATABASE IF NOT EXISTS corpscout;`; up+down; 6-digit number = highest existing + 1 at execution time; entry appended to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.
- Python verification excludes known-broken pre-existing files: `--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py` (plus any test_sweden_company_* failures shown to pre-exist on the base commit).
- Conventional Commits; `uv run ruff check` clean on changed files; `uv run dg check defs` green after asset changes.

---

### Task 1: Shared module `contact_extraction.py` + tests

**Files:**
- Create: `src/dagster_v3/contact_extraction.py`
- Test: `tests/test_contact_extraction.py`
- (No existing file modified — Czech keeps its private copies until Task 2, so the module is briefly duplicated by design.)

**Interfaces:**
- Produces (consumed by Tasks 2 and 4):

```python
COMMONCRAWL_CONFIDENCE = 0.95
DNS_CONFIDENCE = 0.70
DNS_RESOLVE_WORKERS = 32
DNS_QUERY_TIMEOUT_SECONDS = 2.0
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000
EMAIL_RE: re.Pattern
DOMAIN_RE: re.Pattern
CANDIDATE_TEXT_FILTER: str   # ClickHouse RE2 match() prefilter, IDN-aware

@dataclass(frozen=True)
class ContactCandidate:
    record_id: str            # was `ico` in the Czech original
    contact_type: str         # "domain" | "email"
    contact_value: str
    domain: str

def extract_contact_candidates(*, record_id: str, text: str) -> list[ContactCandidate]
def extract_contact_candidates_by_domain(rows) -> dict[str, list[ContactCandidate]]
def merge_domain_candidates(into, new) -> None
def idna_ascii(domain: str) -> str | None          # None when unencodable
def commoncrawl_domains(clickhouse_client, domains) -> set[str]   # tries unicode + idna forms
def nameservers_for_domain(domain: str) -> tuple[str, ...]        # idna-encodes internally
def resolve_nameservers_concurrently(domains) -> dict[str, tuple[str, ...]]
def iter_valid_contact_rows(domain_candidates, *, clickhouse_client, source_slug,
                            resolved_at) -> Iterator[tuple]
    # yields 9-tuples: (source_slug, source_record_id, record_id, contact_type,
    #                   contact_value, domain, domain_source, confidence, resolved_at)
def replace_contact_table(clickhouse_client, *, qualified_table: str,
                          columns: Sequence[str], rows: Iterable[tuple],
                          batch_size: int = CLICKHOUSE_INSERT_BATCH_SIZE,
                          log=None) -> int
```

- Consumes: `dagster_v3.domains.root_domain` / `website_host` (existing shared lib) — the module must NOT create its own `tldextract.TLDExtract` instance.

- [ ] **Step 1: Move-and-generalize the implementation**

Source of truth to move from: `src/dagster_v3/defs/czech_ares/contacts.py` (read it fully first). Create `src/dagster_v3/contact_extraction.py` with a module docstring stating: shared contact-candidate extraction from free text (emails + domains, IDN-aware), CommonCrawl/DNS domain validation, and the atomic stage/EXCHANGE contact-table replace; per-country modules own their candidate SQL, id semantics, and table names.

Move the following, applying ONLY the listed changes (everything else verbatim — behavior preservation is the point):

| Czech original (contacts.py line) | Shared name | Changes |
|---|---|---|
| `_EMAIL_RE` (26) | `EMAIL_RE` | label classes gain `À-ſ` (see below) |
| `_DOMAIN_RE` (30) | `DOMAIN_RE` | same |
| `_CANDIDATE_NAME_FILTER` (34) | `CANDIDATE_TEXT_FILTER` | RE2 form gains the unicode range (see below) |
| `_TLD_EXTRACT` (37) | — dropped | use `domains.root_domain`/`website_host` exclusively (the Czech file already calls them for normalization; if any call site uses `_TLD_EXTRACT` directly, route it through `domains.root_domain`) |
| `ContactCandidate` (44) | same | field `ico` → `record_id` |
| `extract_contact_candidates` (54) | same | kwargs `ico, company_name` → `record_id, text` |
| `extract_contact_candidates_by_domain` (85) | same | adjust to renamed fields |
| `_merge_domain_candidates` (398) | `merge_domain_candidates` | public |
| `iter_valid_contact_rows_from_domain_candidates` (110) | `iter_valid_contact_rows` | gains `source_slug` and `resolved_at` params (Czech hardcodes its slug and computes resolved_at at its call site — hoist both); yields the 9-tuple above |
| `_validated_domain` (361), `_normalized_domain_contact_value` (355), `_append_candidate` (332) | same names (private) | idna-aware where they touch DNS/CC |
| `_commoncrawl_domains` (417) | `commoncrawl_domains` | public; for each input domain also look up `idna_ascii(domain)`; a hit on either form counts |
| `nameservers_for_domain` (189) + `_parent_zone_for_domain`, `_parent_nameserver_addresses`, `_recursive_nameservers`, `_nameserver_addresses`, `_resolve_domain_nameservers_from_parent`, `_answer_nameservers`, `_authority_nameservers` (202–288) | same (public entry + private helpers) | `nameservers_for_domain` first maps the domain through `idna_ascii`; `None` → return `()` |
| `_resolve_nameservers_concurrently` (289) | `resolve_nameservers_concurrently` | public |
| `_replace_contact_table` (435), `_insert_contact_rows` (458), `_column_list` (509), `_batches` (513) | `replace_contact_table` (public) + private helpers | parameterized by `qualified_table` and `columns` instead of Czech constants; returns rows written |
| Constants (18–24) | as in the interface block | `CLICKHOUSE_COMPANY_BATCH_SIZE`/`CLICKHOUSE_QUERY_BATCH_SIZE` do NOT move (Czech scan specifics) |

New helper:

```python
def idna_ascii(domain: str) -> str | None:
    """ASCII (punycode) form of a possibly-IDN domain; None if unencodable."""
    try:
        import idna

        return idna.encode(domain).decode("ascii")
    except Exception:  # noqa: BLE001 - any encode failure means "not a resolvable IDN"
        return None
```

(If `idna.encode` rejects already-ASCII domains with hyphen edge cases, fall back to returning the input unchanged when `domain.isascii()` — decide empirically in the tests and document with a comment.)

The IDN regex change, exactly: wherever the Czech patterns use the label class `[A-Z0-9]` / `[A-Z0-9\-]`, extend to `[A-Z0-9À-ſ]` / `[A-Z0-9À-ſ\-]` (with `re.IGNORECASE` retained). `CANDIDATE_TEXT_FILTER` (RE2 for ClickHouse `match()`): extend its bare-domain alternative from `[A-Za-z0-9][A-Za-z0-9-]*` to `[\p{L}0-9][\p{L}0-9-]*` (RE2 supports `\p{L}`); keep the rest verbatim. The final gate against garbage stays `domains.root_domain`'s public-suffix check — the regex only nominates candidates.

- [ ] **Step 2: Write the shared tests**

Create `tests/test_contact_extraction.py`. Port these tests from `tests/test_czech_ares.py`, adjusted to the new names/fields (`ico=` → `record_id=`, import from `dagster_v3.contact_extraction`) and keeping their assertions otherwise intact:

- `test_contact_candidates_extract_domains_and_emails_from_company_name` (line 133)
- `test_contact_rows_keep_commoncrawl_and_dns_validated_domains_only` (159) — now passes `source_slug`/`resolved_at` explicitly
- `test_contact_extraction_returns_domain_dictionary_before_validation` (198)
- `test_replace_contact_table_inserts_contact_rows_in_batches` (216) — now passes `qualified_table=`/`columns=`
- `test_nameservers_for_domain_uses_parent_zone_authority` (283)
- `test_concurrent_nameserver_resolution_reuses_parent_zone_addresses` (319)
- `test_authoritative_nameserver_lookup_uses_dns_resolver` (357)

Add the new IDN tests:

```python
def test_idn_domain_extracts_and_normalizes_lowercase_unicode():
    candidates = extract_contact_candidates(
        record_id="40003xxxxx",
        text='Sabiedrība ar ierobežotu atbildību "Metinājumi.lv"',
    )
    assert [(c.contact_type, c.contact_value) for c in candidates] == [
        ("domain", "metinājumi.lv")
    ]
    assert candidates[0].domain == "metinājumi.lv"


def test_ascii_extraction_unchanged_by_idn_extension():
    # Byte-compatibility with the pre-IDN Czech regex for ASCII inputs.
    candidates = extract_contact_candidates(
        record_id="123", text="Asseco a.s. - www.asseco.cz, info@asseco.cz"
    )
    assert [(c.contact_type, c.contact_value) for c in candidates] == [
        ("email", "info@asseco.cz"),
        ("domain", "www.asseco.cz"),
    ]
    assert {c.domain for c in candidates} == {"asseco.cz"}


def test_idna_ascii_encodes_idn_and_passes_ascii_through():
    assert idna_ascii("metinājumi.lv") == "xn--metinjumi-y2b.lv"
    assert idna_ascii("example.com") == "example.com"
    assert idna_ascii("") is None


def test_nameservers_for_domain_idna_encodes_before_dns(monkeypatch):
    seen = {}

    def fake_resolve(domain):
        seen["domain"] = domain
        return ("ns1.example.com.",)

    monkeypatch.setattr(
        "dagster_v3.contact_extraction._resolve_domain_nameservers_from_parent",
        lambda domain, **kwargs: fake_resolve(domain),
    )
    nameservers_for_domain("metinājumi.lv")
    assert seen["domain"] == "xn--metinjumi-y2b.lv"


def test_commoncrawl_lookup_tries_unicode_and_idna_forms():
    class _FakeClient:
        def __init__(self):
            self.queries = []

        def execute(self, sql, params=None):
            self.queries.append((sql, params))
            return [("xn--metinjumi-y2b.lv",)]

    client = _FakeClient()
    found = commoncrawl_domains(client, ["metinājumi.lv"])
    assert found == {"metinājumi.lv"}  # hit on the idna form counts for the unicode domain
    queried = str(client.queries)
    assert "xn--metinjumi-y2b.lv" in queried
```

(Adapt `test_nameservers_for_domain_idna_encodes_before_dns`'s monkeypatch target and the `commoncrawl_domains` fake to the actual moved implementations — the assertions are the contract: DNS sees the idna form; a CC hit on either form validates the unicode domain. Exact adaptations documented in your report.)

- [ ] **Step 3: Run to verify fail, implement, verify pass**

Run: `uv run pytest tests/test_contact_extraction.py -v 2>&1 | head -5` → FAIL (module missing) before implementation; all PASS after. Then confirm the untouched Czech suite still passes: `uv run pytest tests/test_czech_ares.py -q` → green (nothing Czech changed yet).

- [ ] **Step 4: Lint, commit**

```bash
uv run ruff check src/dagster_v3/contact_extraction.py tests/test_contact_extraction.py
git add src/dagster_v3/contact_extraction.py tests/test_contact_extraction.py
git commit -m "feat(dagster): shared contact-extraction module with IDN support"
```

---

### Task 2: Rewire Czech onto the shared module

**Files:**
- Modify: `src/dagster_v3/defs/czech_ares/contacts.py` (shrink)
- Modify: `tests/test_czech_ares.py` (prune moved tests)

**Interfaces:**
- Consumes: everything in Task 1's interface block.
- Produces: unchanged Czech public surface — `replace_czech_company_contacts_clickhouse(...)`, `load_company_contact_candidate_batch(...)`, `export_czech_ares_clickhouse_company_contacts` (trace where this wrapper lives — `assets.py:11` imports it; it may be in `czech_ares/clickhouse.py` or `contacts.py`) — all with identical behavior and signatures.

- [ ] **Step 1: Shrink contacts.py**

Delete every moved item from `czech_ares/contacts.py` and import from `dagster_v3.contact_extraction` instead. What remains in the Czech module: `CONTACTS_SOURCE_SLUG`, `CLICKHOUSE_COMPANY_BATCH_SIZE`, `CLICKHOUSE_QUERY_BATCH_SIZE`, `load_company_contact_candidate_batch` (the cz_companies keyset-paginated candidate scan — update its prefilter to the shared `CANDIDATE_TEXT_FILTER`), and `replace_czech_company_contacts_clickhouse` re-assembled as a thin orchestrator: scan batches → `extract_contact_candidates_by_domain` (mapping `ico` → `record_id`) → `merge_domain_candidates` → `iter_valid_contact_rows(..., source_slug=CONTACTS_SOURCE_SLUG, resolved_at=...)` → `replace_contact_table(client, qualified_table=tables.QUALIFIED_COMPANY_CONTACTS_TABLE, columns=[...czech column list with 'ico'...], rows=...)`. The Czech column list keeps `ico` as the physical column name — only the *value* comes from `record_id`.

Behavioral pin: the IDN-extended regex means Czech may now extract diacritic domains it previously missed — that is the user-approved improvement, not a regression. Everything else must be behavior-identical.

- [ ] **Step 2: Prune moved tests**

Remove from `tests/test_czech_ares.py` exactly the seven tests ported in Task 1 Step 2 (they now live generalized in `tests/test_contact_extraction.py`). Keep and, where imports demand, minimally adjust: `test_clickhouse_candidate_batches_load_100k_company_names_after_ico`, `test_contacts_export_columns_match_migration`, `test_register_job_and_schedule`, and any orchestrator-level test. No assertion changes beyond import/name updates.

- [ ] **Step 3: Verify**

```bash
uv run pytest tests/test_czech_ares.py tests/test_contact_extraction.py -v 2>&1 | tail -4
uv run dg check defs 2>&1 | tail -1
uv run ruff check src/dagster_v3/defs/czech_ares/contacts.py tests/test_czech_ares.py
```

Expected: all green; definitions load.

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/czech_ares/contacts.py tests/test_czech_ares.py
git commit -m "refactor(dagster): czech contacts consume the shared extraction module"
```

---

### Task 3: Latvia migration `lv_company_contacts`

**Files:**
- Create: `corpscout/clickhouse/migrations/0000NN_corpscout_lv_company_contacts.up.sql` (NN = highest existing + 1 — check `ls corpscout/clickhouse/migrations | tail -4`; was 000085 at planning)
- Create: matching `.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (append entry)

**Interfaces:**
- Produces: table `corpscout.lv_company_contacts` (Task 4's write target).

- [ ] **Step 1: Write the migration**

Up (exactly — the spec's SQL):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_company_contacts
(
    source_slug LowCardinality(String),
    source_record_id String,
    regcode String,
    contact_type LowCardinality(String),
    contact_value String,
    domain String,
    domain_source LowCardinality(String),
    confidence Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (regcode, contact_type, contact_value);
```

Down: `DROP TABLE IF EXISTS corpscout.lv_company_contacts;`

- [ ] **Step 2: Contract test + live apply + smoke**

Append the migration name to `EXPECTED_MIGRATIONS`; `uv run pytest tests/test_clickhouse_migrations.py -q` → green (report any pre-existing missing entries that aren't yours). Apply live: `cd corpscout && make clickhouse-migrate-up 2>&1 | tail -2` (env from the main checkout's `corpscout/dagster_v3/.env`). Smoke via `uv run python` + clickhouse_connect: `SELECT count() FROM corpscout.lv_company_contacts` → 0.

- [ ] **Step 3: Commit**

```bash
git add corpscout/clickhouse/migrations/ corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): lv_company_contacts table"
```

---

### Task 4: Latvia contacts asset + wiring + live verification

**Files:**
- Create: `src/dagster_v3/defs/latvia_ur/contacts.py`
- Modify: `src/dagster_v3/defs/latvia_ur/assets.py` (register asset + check imports at TOP of file; extend register-job selection)
- Modify: `tests/test_latvia_ur_assets.py` (job pin + wiring test)
- Test: `tests/test_latvia_ur_contacts.py`
- Modify: `docs/data-source-guidelines.md` (contacts bullet references the shared module) and `src/dagster_v3/defs/latvia_ur/docs/latvia_ur-design.md` §6b/contacts row if present (read first; keep the edit one-to-three lines)

**Interfaces:**
- Consumes: the shared module (Task 1) and `corpscout.lv_company_contacts` (Task 3).
- Produces: asset `latvia_ur_clickhouse_company_contacts` (group `latvia_ur`, deps `latvia_ur_clickhouse_companies`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_latvia_ur_contacts.py`:

```python
"""Tests for Latvia company-contacts extraction (shared-module consumer)."""

from dagster_v3.defs.latvia_ur.contacts import (
    LV_CONTACTS_SOURCE_SLUG,
    LV_CONTACT_COLUMNS,
    build_candidate_scan_sql,
    extract_latvia_contact_candidates,
)


def test_candidate_scan_sql_prefilters_and_paginates_by_regcode():
    sql = build_candidate_scan_sql(after_regcode="40003000000", limit=1000)
    assert "FROM corpscout.lv_companies" in sql
    assert "legal_name" in sql
    assert "match(" in sql          # shared CANDIDATE_TEXT_FILTER prefilter
    assert "regcode >" in sql       # keyset pagination, no OFFSET
    assert "LIMIT" in sql


def test_real_latvian_names_extract_domains():
    cases = {
        'SIA "cenuklubs.lv"': "cenuklubs.lv",
        "IK Akmenkalis.com": "akmenkalis.com",
        'Sabiedrība ar ierobežotu atbildību "Metinājumi.lv"': "metinājumi.lv",
        "IK 24dressup.lv": "24dressup.lv",
    }
    for legal_name, expected_domain in cases.items():
        candidates = extract_latvia_contact_candidates(
            regcode="40003xxxxx", legal_name=legal_name
        )
        assert [c.domain for c in candidates] == [expected_domain], legal_name


def test_plain_legal_names_extract_nothing():
    for legal_name in (
        'Sabiedrība ar ierobežotu atbildību "Ozoli"',
        "Individuālais komersants JURIS BĒRZIŅŠ",
        'AS "Latvijas Gāze"',
    ):
        assert extract_latvia_contact_candidates(regcode="1", legal_name=legal_name) == []


def test_columns_match_migration():
    from pathlib import Path

    migration = next(
        Path(__file__).joinpath("../../../clickhouse/migrations").resolve().glob(
            "*_corpscout_lv_company_contacts.up.sql"
        )
    ).read_text()
    for column in LV_CONTACT_COLUMNS:
        assert column in migration
    assert LV_CONTACTS_SOURCE_SLUG == "latvia_ur"
```

Also add to `tests/test_latvia_ur_assets.py`: the register-job pin gains `"latvia_ur_clickhouse_company_contacts"` (comment like its siblings), and a wiring assertion mirroring `test_nace_classification_asset_deps_and_group` for the new asset (dep `latvia_ur_clickhouse_companies`, group `latvia_ur`).

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_latvia_ur_contacts.py 2>&1 | head -4` → FAIL (module missing).

- [ ] **Step 3: Implement the Latvia module and asset**

Create `src/dagster_v3/defs/latvia_ur/contacts.py` — mirror the (now thin) Czech orchestrator, with Latvia specifics. Read the post-Task-2 `czech_ares/contacts.py` and the Czech asset in `czech_ares/assets.py` first and mirror their client-acquisition and MaterializeResult patterns exactly. Shape:

```python
"""Latvia company contacts: domains embedded in legal names.

Latvia UR has no structured contact fields, but ~1.3k companies carry their
domain as the legal name ('SIA "cenuklubs.lv"'). Candidates are extracted
with the shared contact_extraction module (IDN-aware — Latvian domains use
diacritics), validated against CommonCrawl/DNS, and atomically replace
corpscout.lv_company_contacts. Full recompute per run.
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.contact_extraction import (
    CANDIDATE_TEXT_FILTER,
    extract_contact_candidates,
    iter_valid_contact_rows,
    merge_domain_candidates,
    replace_contact_table,
)

LV_CONTACTS_SOURCE_SLUG = "latvia_ur"
QUALIFIED_LV_CONTACTS_TABLE = "corpscout.lv_company_contacts"
LV_CONTACT_COLUMNS = (
    "source_slug", "source_record_id", "regcode", "contact_type",
    "contact_value", "domain", "domain_source", "confidence", "resolved_at",
)
SCAN_BATCH_SIZE = 100_000


def build_candidate_scan_sql(*, after_regcode: str, limit: int) -> str:
    return f"""
SELECT regcode, legal_name
FROM corpscout.lv_companies
WHERE match(legal_name, '{CANDIDATE_TEXT_FILTER}')
  AND regcode > '{after_regcode}'
ORDER BY regcode
LIMIT {limit}"""


def extract_latvia_contact_candidates(*, regcode: str, legal_name: str):
    return extract_contact_candidates(record_id=regcode, text=legal_name)


def replace_latvia_company_contacts_clickhouse(*, clickhouse_client, resolved_at, log=None) -> dict:
    domain_candidates: dict = {}
    scanned = 0
    after = ""
    while True:
        rows = clickhouse_client.execute(
            build_candidate_scan_sql(after_regcode=after, limit=SCAN_BATCH_SIZE)
        )
        if not rows:
            break
        scanned += len(rows)
        for regcode, legal_name in rows:
            for candidate in extract_latvia_contact_candidates(
                regcode=regcode, legal_name=legal_name
            ):
                merge_domain_candidates(domain_candidates, {candidate.domain: [candidate]})
        after = rows[-1][0]
        if len(rows) < SCAN_BATCH_SIZE:
            break

    contact_rows = list(
        iter_valid_contact_rows(
            domain_candidates,
            clickhouse_client=clickhouse_client,
            source_slug=LV_CONTACTS_SOURCE_SLUG,
            resolved_at=resolved_at,
        )
    )
    written = replace_contact_table(
        clickhouse_client,
        qualified_table=QUALIFIED_LV_CONTACTS_TABLE,
        columns=LV_CONTACT_COLUMNS,
        rows=contact_rows,
        log=log,
    )
    return {"scanned": scanned, "candidate_domains": len(domain_candidates), "written": written}
```

Adapt the exact `merge_domain_candidates` call shape and `iter_valid_contact_rows` argument names to Task 1's real signatures (they are the contract; the tests pin behavior). Then the asset (in the same file, mirroring the Czech asset decorator style):

```python
@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Extract domains embedded in Latvian legal names ('SIA \"cenuklubs.lv\"'), "
        "validate via CommonCrawl/DNS, and atomically replace "
        "corpscout.lv_company_contacts."
    ),
)
def latvia_ur_clickhouse_company_contacts(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    import datetime

    with clickhouse.get_connection() as client:
        counts = replace_latvia_company_contacts_clickhouse(
            clickhouse_client=client,
            resolved_at=datetime.datetime.now(datetime.UTC),
            log=context.log,
        )
    return dg.MaterializeResult(metadata=dict(counts))
```

Register in `latvia_ur/assets.py`: import at TOP (with the `.translation`/`.classification` imports), add to `defs.assets`, and extend the register job:

```python
latvia_ur_register_job = dg.define_asset_job(
    "latvia_ur_register_job",
    selection=dg.AssetSelection.assets(
        "latvia_ur_translation_load",
        "latvia_ur_nace_classification",
        "latvia_ur_clickhouse_company_contacts",
    ).upstream(),
)
```

- [ ] **Step 4: Verify + live smoke**

```bash
uv run pytest tests/test_latvia_ur_contacts.py tests/test_latvia_ur_assets.py tests/test_contact_extraction.py tests/test_czech_ares.py -q
uv run dg check defs 2>&1 | tail -1
uv run ruff check src/dagster_v3/defs/latvia_ur/ tests/test_latvia_ur_contacts.py
```

Live smoke (env from main checkout's `dagster_v3/.env`): run the candidate scan SQL against real ClickHouse via `uv run python` + clickhouse_connect — expect roughly 1,300–1,400 candidate rows (the planning probe found 1,330 with a narrower TLD list); extract candidates in-process and report the count and 5 samples. Full validation (DNS/CC) and real materialization are OPTIONAL — run the full `replace_latvia_company_contacts_clickhouse` end-to-end ONLY if DNS resolution works from this machine (`uv run python -c "import dns.resolver; print(dns.resolver.resolve('example.com','NS')[0])"` succeeds); if run, report the written count and `domain_source` split. Otherwise the register job does it in production.

- [ ] **Step 5: Docs + commit**

`docs/data-source-guidelines.md`: in the contacts bullet (§8b), add one sentence: name-embedded contact extraction uses the shared `dagster_v3/contact_extraction.py` (IDN-aware) — mirror `defs/latvia_ur/contacts.py` (thin) or `defs/czech_ares/contacts.py`. `latvia_ur-design.md` §6b: update the contacts row to reflect that domains-in-legal-names are extracted to `lv_company_contacts`.

```bash
git add src/dagster_v3/defs/latvia_ur/ tests/test_latvia_ur_contacts.py tests/test_latvia_ur_assets.py docs/
git commit -m "feat(dagster): latvia company contacts from legal names via shared extraction"
```

---

## Deployment note (not a code task)

Migration applies in Task 3 (lab). The asset runs with the next `latvia_ur_register_job` (04:30) or a manual materialization; DNS validation needs outbound DNS from the dagster host. Follow-up already agreed with the user (separate project): feed `cz_company_contacts` + `lv_company_contacts` into the shared `corpscout.domains` graph.
