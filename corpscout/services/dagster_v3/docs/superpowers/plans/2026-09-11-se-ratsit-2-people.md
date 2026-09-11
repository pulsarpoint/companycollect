# Ratsit slice 2 — the Ratsit person extractor

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 301,081 Ratsit responsible-people rows of the current reports into a fourth person extractor (`se_company_person_suggestions_ratsit`), map their Swedish role labels to the catalog, and run the extract → normalize → fold chain on prod so ~236,814 companies gain Ratsit people — ~105,759 of them their first person of any source.

**Architecture:** One new module, `se_company/person/ratsit.py`, built from the same four helpers every person source uses (`live_select_sql`, `person_changed_scope_sql`, `person_select_sql`, `define_person_suggestion_asset`). Its live branch picks the newest normalized Ratsit report per company (`se_ratsit_company FINAL`, `LIMIT 1 BY company_id`) inside the same `se_company_basic_info` universe the three siblings join, then joins `se_ratsit_responsible_people` on `(company_id, result_sha256, normalizer_version)`, so a superseded scan's people never appear; the slot is Ratsit's own person id (the trailing token of `profile_url`), role-qualified when one report repeats it and `idx:<person_index>` when the row carries no URL. Registration is one tuple entry (`assets.EXTRACTOR_SOURCES`), which carries the asset into the extract job, the STOPPED weekly's run config and the normalize asset's deps by construction. `roles.py` gains the Ratsit label map; `precedence.py`, `docs/person-design.md` and the backoffice's Source filter lose the "ratsit is reserved, no data yet" wording. No migration, no new dependency, no change to the shared state-hash scan or the shared tombstone branch.

**Tech Stack:** Python 3.14, Dagster 1.13.9 (`uv run pytest`, `uv run dg check defs`), ClickHouse 26.5 (`corpscout` database, `clickhouse-driver` `%(name)s` parameters), pytest 9 with the `integration` marker and `clickhouse-local` (binary or the `clickhouse/clickhouse-server:26.5` Docker image) for the real-engine test; React Router v7 + vitest 4.1 + pnpm for the backoffice; the prod Dagster GraphQL API over `ssh dagster` and `clickhouse-client` over `ssh companycollect` for the run.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` — this plan is section 8 item 2, and its content is section 4 (4.1 module and shape, 4.2 live rows, 4.3 roles, 4.4 wording/docs/backoffice, 4.5 tests, 4.6 prod run) resting on the facts of section 2, the rulings of section 6 and the names of section 7.

## Global Constraints

- Work only in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-ratsit-source`. **Never `git stash`.** Never `git add -A` or `git add .` — stage by explicit path, every time.
- Commit with a message file, never `-m`: `git commit -F "$MSGFILE"`. Every message ends with the two trailers, in this order:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- The state-hash change scan and the per-slot tombstone branch are **shared** (`person/suggestions.py`: `person_state_sql`, `person_changed_scope_sql`, `person_select_sql`, `stored_live_sql`, `LIVE_ROW_PREDICATE`). Nothing about them changes in this slice — the Ratsit module supplies a `live` SQL text and nothing else.
- `PERSON_PRECEDENCE` in `person/precedence.py` is **unchanged**: `{"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "wikidata": 600, "esef": 400}`. Ratsit was already exported to `corpscout.se_company_person_precedence` in person slice 2; re-exporting would move a fold watermark and re-fold every company. Only the docstring wording changes.
- No new dependencies, in either project. No ClickHouse migration: every table this slice reads already exists (`corpscout.se_ratsit_company` from 000343, `corpscout.se_ratsit_responsible_people` from 000343 + 000346, `corpscout.se_company_person_suggestion` from 000396).
- The three weeklies (`se_company_basic_info_weekly`, `se_company_address_weekly`, `se_company_person_weekly`) stay **STOPPED** (owner decision 2026-09-08). The prod runs are launched by hand.
- Extractor version `ratsit-person-v1`; asset `se_company_person_suggestions_ratsit`; source literal `ratsit`. These three strings never vary.
- Do not "fix" pre-existing failing tests. In the backoffice, `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx` fail on `main` today and are out of scope.
- Python style of this package: no `from __future__ import annotations` in modules that define `@dg.asset`; SQL is built as explicit string concatenation with `%(name)s` parameters, never f-string interpolation of user data.
- **Any test that loads the whole definitions tree needs the gitignored `.env` exported first.** `dg` auto-loads it; plain `pytest` does not, so `tests/test_se_company_person_jobs.py` fails 4 of 5 on `main` today with `ValidationError ... WebtechScannerComponent api_url Input should be a valid string` (measured on this worktree and on the main checkout, 2026-09-11). Before every `uv run pytest` that touches `_repo()` / `load_defs()`, and before `uv run dg check defs`, run from `corpscout/services/dagster_v3`:
  ```bash
  set -a && . ./.env && set +a
  ```
  With it, that file is `5 passed`. Never commit `.env`, and never "fix" the component to make the unexported case pass.

## File Structure

| file | what happens |
| --- | --- |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/ratsit.py` | **created** — `PERSON_SOURCE`, `RATSIT_PERSON_EXTRACTOR_VERSION`, `RATSIT_SELECT_PARAMS`, `RATSIT_COLUMN_SQL`, `ratsit_report_cte_sql`, `ratsit_live_sql`, `ratsit_current_sql`, `ratsit_changed_scope_sql`, `ratsit_select_sql`, and the asset `se_company_person_suggestions_ratsit` |
| `corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py` | modified — the `EXTRACTORS` dict gains ratsit, two new pinned tests, and the `EXTRACTOR_SOURCES` pin becomes the four-tuple |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py` | modified — `EXTRACTOR_SOURCES` becomes `("bolagsverket", "esef", "wikidata", "ratsit")`; the module docstring names `ratsit.py` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/roles.py` | modified — `RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE` + its `SOURCE_ROLE_MAPPINGS` entry; the `role_code_for` docstring no longer says ratsit has no map |
| `corpscout/services/dagster_v3/tests/test_se_company_person_roles.py` | **created** — the Ratsit label map, the `aktuarie` passthrough, `Extern VD` keeping the CEO code |
| `corpscout/services/dagster_v3/tests/test_se_company_common.py:466` | modified — `set(SOURCE_ROLE_MAPPINGS)` gains `"ratsit"` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/precedence.py` | modified — the docstring stops calling ratsit "reserved … no extractor yet" |
| `corpscout/services/dagster_v3/tests/test_se_company_person_precedence.py` | modified — the same wording in a test name and docstring; the pinned dict is untouched |
| `corpscout/services/dagster_v3/tests/test_se_company_person_jobs.py` | modified — "three extractors" wording in the module docstring and one test name; the assertions already read `EXTRACTOR_ASSET_NAMES` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md` | modified — the module table gains `ratsit.py`, the `jobs.py` and `precedence.py` rows, the extractor section heading, its sentence and its table |
| `corpscout/services/dagster_v3/tests/fixtures/se_company_person_source_tables.sql` | modified — the `corpscout.se_ratsit_responsible_people` CREATE with the 000346 columns inlined |
| `corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py` | modified — the Ratsit fixture rows, four new sections and three new assertions |
| `corpscout/services/backoffice/app/lib/se-person-fields.ts` | modified — `MAIN_PERSON_SOURCES` stops excluding `ratsit`; the comment follows |
| `corpscout/services/backoffice/tests/se-person-fields.test.ts` | modified — the expected filter list gains `"ratsit"`; the comment follows |
| `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` | modified in Task 5 — the Shipped record under section 8 item 2 |
| this plan | ticked in Task 5 |

Not touched, deliberately: `person/suggestions.py`, `person/normalize.py`, `person/normalize_se.py`, `person/fold.py`, `person/batch.py`, `person/tables.py` (its `SOURCES` tuple already lists `ratsit`), `person/jobs.py` (it reads `EXTRACTOR_ASSET_NAMES`), `address/ratsit.py` and `basic_info/ratsit.py` (slices 3 and 1), and every ClickHouse migration.

---

### Task 1: The `ratsit.py` person extractor and its SQL contract tests

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/ratsit.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py` (modified)

**Interfaces:**
- Consumes: from `person/suggestions.py` — `NULL_SQL: Mapping[str, str]`, `live_select_sql(*, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = "") -> str`, `person_changed_scope_sql(*, source: str, live_sql: str) -> str`, `person_select_sql(*, source: str, live_sql: str) -> str`, `define_person_suggestion_asset(**kwargs) -> dg.AssetsDefinition` (it forwards to `define_suggestion_asset(target=PERSON_TARGET, ...)`, whose keyword arguments are `source`, `extractor_version`, `current_sql`, `select_sql`, `select_params`, `deps`, `description`, `changed_scope_override`). From `sweden_ratsit/normalization.py` — `RATSIT_NORMALIZER_VERSION = "ratsit-normalizer-v2"`.
- Produces: module `dagster_v3.defs.se_company.person.ratsit` with `PERSON_SOURCE = "ratsit"`, `RATSIT_PERSON_EXTRACTOR_VERSION = "ratsit-person-v1"`, `RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}`, `UNIVERSE_JOIN_SQL: str`, `RATSIT_COLUMN_SQL: dict[str, str]` (all sixteen `PERSON_SELECT_COLUMNS`), `ratsit_report_cte_sql(*, scoped: bool = False) -> str`, `ratsit_live_sql(*, scoped: bool = False) -> str`, `ratsit_current_sql() -> str`, `ratsit_changed_scope_sql() -> str`, `ratsit_select_sql() -> str`, and the asset object `se_company_person_suggestions_ratsit`. Task 2 registers the source; Task 3 runs this SQL on a real engine.

- [ ] **Step 1: Record the baseline**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
git status --short                                   # must be empty
uv run pytest tests/test_se_company_person_extractors_sql.py -q 2>&1 | tail -3
```

Expected: `12 passed`. This is the file Step 2 extends; if it is not green before the change, stop.

- [ ] **Step 2: Write the failing tests**

Three edits to `tests/test_se_company_person_extractors_sql.py`.

(a) The import line 10 gains `ratsit`:

```python
from dagster_v3.defs.se_company.person import assets, bolagsverket, esef, ratsit, tables, wikidata
```

and a new import under it, for the version the select parameter carries:

```python
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
```

(b) The `EXTRACTORS` dict gains a fourth entry, after `"wikidata"` (the dict's five-tuple is `(columns, scoped live sql, select sql, changed scope sql, asset)` — every generic test in this file loops over it):

```python
    "ratsit": (
        ratsit.RATSIT_COLUMN_SQL,
        ratsit.ratsit_live_sql(scoped=True),
        ratsit.ratsit_select_sql(),
        ratsit.ratsit_changed_scope_sql(),
        ratsit.se_company_person_suggestions_ratsit,
    ),
```

(c) Two new tests at the end of the file:

```python
def test_ratsit_slot_is_the_profile_token_with_role_and_index_fallbacks() -> None:
    """Spec 2026-09-11 section 4.2. The slot is Ratsit's own person id -- the trailing token
    of profile_url, which the same person carries at every company -- so a re-scan rewrites
    the row in place instead of retiring the slot and inventing a new one. The 31 (company,
    token) pairs that carry two rows (a `Delgivningsbar person` who is also VD) get the role
    appended; a named row without a URL falls back to its person_index."""
    columns = ratsit.RATSIT_COLUMN_SQL
    assert columns["slot"] == (
        "multiIf("
        "r.token = '', concat('idx:', toString(r.person_index)), "
        "count() OVER (PARTITION BY r.company_id, r.token) > 1, "
        "concat(r.token, ':', lowerUTF8(trim(r.role_raw))), "
        "r.token)"
    )
    assert columns["source_record_id"] == (
        "concat('ratsit:', toString(r.result_sha256), ':', toString(r.person_index))"
    )
    # Ratsit delivers one name string; the normalizer splits it.
    assert columns["full_name"] == "nullIf(trim(r.name_raw), '')"
    assert columns["first_name"] == NULL_SQL["first_name"]
    assert columns["last_name"] == NULL_SQL["last_name"]
    # The birth date is the first eight digits of the profile URL's path (277,592 of the
    # 301,536 rows carry it); NULL when the row has no URL.
    assert columns["birth_year"] == (
        "toUInt16OrNull(substring(extract(r.profile_url, "
        "'^https://www\\.ratsit\\.se/(\\d{8})-'), 1, 4))"
    )
    assert columns["wikidata_id"] == NULL_SQL["wikidata_id"]
    assert columns["role_original"] == "nullIf(trim(r.role_raw), '')"
    # Ratsit has no machine role code: roles.py maps the Swedish label instead.
    assert columns["role_key"] == NULL_SQL["role_key"]
    # The role is current at the scan, so the scan date is the role year (the report's
    # normalized_at when Ratsit delivered no source_date_modified).
    assert columns["fiscal_year"] == (
        "toYear(ifNull(r.source_date_modified, toDate32(r.normalized_at)))"
    )
    assert columns["role_from"] == NULL_SQL["role_from"]
    assert columns["role_to"] == NULL_SQL["role_to"]
    assert columns["document_ref"] == NULL_SQL["document_ref"]
    # mapFilter over coalesced String values: a NULL age must leave the key OUT of the
    # object. A bare map() would render "age":null, which is a value no reader expects.
    assert columns["data"].startswith("toJSONString(mapFilter((k, v) -> v != '', map(")
    for key in ("'age'", "'identity_available'", "'profile_url'", "'display_name_raw'",
                "'ratsit_person_id'", "'external'"):
        assert key in columns["data"], key
    assert (
        "if(startsWith(lowerUTF8(trim(r.role_raw)), 'extern'), 'true', 'false')"
    ) in columns["data"]

    live = ratsit.ratsit_live_sql()
    # The current report per company: newest normalized_at, ties by the higher hash.
    assert live.startswith("WITH report AS (")
    assert "FROM corpscout.se_ratsit_company AS c FINAL" in live
    assert (
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id"
    ) in live
    # The same universe as the three siblings: a Ratsit company the entity does not know
    # yields no live row, so it is on neither side of the state hash and never visited.
    assert ratsit.UNIVERSE_JOIN_SQL in live
    assert (
        "    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
        "        ON universe.company_id = c.company_id"
    ) in live
    # People join the report's own key, so a superseded scan's rows never appear.
    assert "FROM corpscout.se_ratsit_responsible_people AS p FINAL" in live
    assert (
        "    INNER JOIN report\n"
        "        ON report.company_id = p.company_id\n"
        "        AND report.result_sha256 = p.result_sha256\n"
        "        AND report.normalizer_version = p.normalizer_version"
    ) in live
    # 23,432 nameless rows are role-only GDPR evidence, not identities.
    assert live.endswith("WHERE trim(r.name_raw) != ''")
    assert "%(company_ids)s" not in live
    assert ratsit.ratsit_live_sql(scoped=True).count("%(company_ids)s") == 1
    assert ratsit.RATSIT_PERSON_EXTRACTOR_VERSION == "ratsit-person-v1"
    assert ratsit.RATSIT_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    # run_extractor binds select_params into both the scope and every page.
    assert ratsit.ratsit_select_sql().count("%(normalizer_version)s") == 1
    assert ratsit.ratsit_changed_scope_sql().count("%(normalizer_version)s") == 1


def test_the_ratsit_asset_reads_the_ratsit_tables_and_the_basic_info_fold() -> None:
    """Spec 4.1: `se_ratsit_normalized` is the multi-asset's FUNCTION name, not an asset key
    -- a dep on it makes a phantom node. The keys the multi-asset declares are the table
    names, which is what this asset deps on, plus the fold that publishes the universe (the
    key `bolagsverket.py` and `wikidata.py` already carry)."""
    asset = ratsit.se_company_person_suggestions_ratsit
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_responsible_people"),
        dg.AssetKey("se_company_basic_info_fold"),
    }


def test_the_ratsit_current_sql_is_the_reports_own_stamp() -> None:
    """`current_sql` exists only for the `since` escape hatch (the change scan is the state
    hash). It is the newest report's normalized_at, picked exactly as the live branch picks
    the report -- a max() over every report would keep re-selecting companies whose older
    report was written later."""
    current = ratsit.ratsit_current_sql()
    assert "toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at" in current
    assert "    LIMIT 1 BY c.company_id" in current
    # The same universe as the live branch, so `since` and the page agree on who exists
    # (bolagsverket_current_sql carries its UNIVERSE_JOIN_SQL for the same reason).
    assert ratsit.UNIVERSE_JOIN_SQL in current
    assert current.count("%(normalizer_version)s") == 1
    assert "%(company_ids)s" not in current
```

- [ ] **Step 3: Run the tests and watch them fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_person_extractors_sql.py -q 2>&1 | tail -5
```

Expected: a collection error — `ImportError: cannot import name 'ratsit' from 'dagster_v3.defs.se_company.person'`. The whole file errors, which is the correct "not written yet" signal.

- [ ] **Step 4: Write `person/ratsit.py`**

Create `src/dagster_v3/defs/se_company/person/ratsit.py` with exactly this content:

```python
"""Ratsit's responsible people -> raw person suggestions (spec 2026-09-11 section 4).

One suggestion per NAMED person of the company's CURRENT Ratsit report. se_ratsit_company
holds one row per (company, result hash, normalizer version) and is never pruned -- a
re-scan appends -- so the current report is picked here: newest normalized_at, ties by the
higher result_sha256, LIMIT 1 BY company_id. The people rows join that report on
(company_id, result_sha256, normalizer_version), so a superseded scan's people can never
reach the suggestion table.

THE SLOT IS RATSIT'S OWN PERSON ID: the trailing token of profile_url
(https://www.ratsit.se/<YYYYMMDD>-<Name>_<Town>/<token>), which the same person carries at
every company -- 191,434 distinct tokens over 277,646 rows on 2026-09-10. Keeping it as the
slot means a re-scan rewrites the person's row in place instead of tombstoning it and
inventing a new one, which is what the fold's slot-keyed reviewer rules need. Two
exceptions: 31 (company, token) pairs carry two rows (a `Delgivningsbar person` who is also
VD or Vice VD), so a token a report repeats is qualified with the lowercased role; and a
named row with no URL falls back to `idx:<person_index>`.

23,432 rows are nameless -- GDPR-limited evidence: a role, no name, no URL. They carry no
identity, so the live branch drops them and they never become suggestions.

THE UNIVERSE IS se_company_basic_info, as it is for bolagsverket.py, esef.py and
wikidata.py: this entity's universe is the folded company, and a Ratsit company the entity
does not know is dropped here rather than published as a company id nothing downstream
recognises. The join sits in the `report` CTE, which is also what `ratsit_current_sql` reads,
so the `since` watermark and the page select agree on who exists.

Ratsit delivers no machine role code, so role_key is NULL and roles.py maps the Swedish
label (SOURCE_ROLE_MAPPINGS["ratsit"]).
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

PERSON_SOURCE = "ratsit"
RATSIT_PERSON_EXTRACTOR_VERSION = "ratsit-person-v1"
# The normalizer version the current report must carry; run_extractor binds select_params
# into the scope query and into every page select.
RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}

# extract() returns '' when nothing matches, which is what the slot's first branch tests.
TOKEN_SQL = "extract(ifNull(p.profile_url, ''), '/([A-Za-z0-9_-]+)$')"
# A window function, not a join: the page select already binds %(company_ids)s twice and the
# integration test runs under join_use_nulls 0 AND 1, so a second join is both a binding and
# a nullability risk. ClickHouse computes windows after WHERE, so the count covers the named
# rows only -- the same set the slot is drawn from.
TOKEN_ROWS_SQL = "count() OVER (PARTITION BY r.company_id, r.token)"

# The same universe join bolagsverket.py uses, against the report rather than the people
# rows: one INNER JOIN per statement, and ratsit_current_sql can reuse the identical text.
UNIVERSE_JOIN_SQL = (
    "    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "        ON universe.company_id = c.company_id"
)

RATSIT_COLUMN_SQL: dict[str, str] = {
    "company_id": "r.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": (
        "multiIf("
        "r.token = '', concat('idx:', toString(r.person_index)), "
        f"{TOKEN_ROWS_SQL} > 1, "
        "concat(r.token, ':', lowerUTF8(trim(r.role_raw))), "
        "r.token)"
    ),
    "source_record_id": (
        "concat('ratsit:', toString(r.result_sha256), ':', toString(r.person_index))"
    ),
    # Ratsit delivers one name string; the normalizer splits it.
    "full_name": "nullIf(trim(r.name_raw), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    # The profile URL's path starts with the person's birth date (YYYYMMDD).
    "birth_year": (
        "toUInt16OrNull(substring(extract(r.profile_url, "
        "'^https://www\\.ratsit\\.se/(\\d{8})-'), 1, 4))"
    ),
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(r.role_raw), '')",
    # No machine code from this source (spec 4.2); roles.py keys on the label.
    "role_key": NULL_SQL["role_key"],
    # The role is current at the scan date (spec 4.2 and risk 2 of section 6: a re-scan next
    # year adds a year to the same slot, as Wikidata's dateless roles do).
    "fiscal_year": "toYear(ifNull(r.source_date_modified, toDate32(r.normalized_at)))",
    "role_from": NULL_SQL["role_from"],
    "role_to": NULL_SQL["role_to"],
    "document_ref": NULL_SQL["document_ref"],
    # mapFilter over String values coalesced to '': a key whose source value is NULL or
    # empty is ABSENT from the object rather than present as "" or null. map() alone would
    # render "age":null for the 8% of rows without an age.
    "data": (
        "toJSONString(mapFilter((k, v) -> v != '', map("
        "'age', ifNull(toString(r.age), ''), "
        "'identity_available', if(r.identity_available, 'true', 'false'), "
        "'profile_url', r.profile_url, "
        "'display_name_raw', r.display_name_raw, "
        "'ratsit_person_id', r.token, "
        "'external', if(startsWith(lowerUTF8(trim(r.role_raw)), 'extern'), 'true', 'false'))))"
    ),
}


def ratsit_report_cte_sql(*, scoped: bool = False) -> str:
    """CTEs `report` (the current report per company, inside the basic-info universe) and
    `people` (its responsible-people rows, with the profile token computed once).

    `scoped=True` narrows `report` to %(company_ids)s up front, which binds the ids exactly
    once for this whole side of the select and keeps a page from picking the current report
    of all 947,200 companies.
    """
    company_filter = "\n        AND c.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH report AS (\n"
        "    SELECT\n"
        "        c.company_id AS company_id,\n"
        "        c.result_sha256 AS result_sha256,\n"
        "        c.normalizer_version AS normalizer_version,\n"
        "        c.normalized_at AS normalized_at,\n"
        "        c.source_date_modified AS source_date_modified\n"
        "    FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        f"    WHERE c.normalizer_version = %(normalizer_version)s{company_filter}\n"
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id\n"
        "),\n"
        "people AS (\n"
        "    SELECT\n"
        "        p.company_id AS company_id,\n"
        "        p.person_index AS person_index,\n"
        "        p.result_sha256 AS result_sha256,\n"
        "        ifNull(p.name, '') AS name_raw,\n"
        "        ifNull(p.display_name_raw, '') AS display_name_raw,\n"
        "        ifNull(p.role, '') AS role_raw,\n"
        "        ifNull(p.profile_url, '') AS profile_url,\n"
        "        p.age AS age,\n"
        "        p.identity_available AS identity_available,\n"
        f"        {TOKEN_SQL} AS token,\n"
        "        report.source_date_modified AS source_date_modified,\n"
        "        report.normalized_at AS normalized_at\n"
        "    FROM corpscout.se_ratsit_responsible_people AS p FINAL\n"
        "    INNER JOIN report\n"
        "        ON report.company_id = p.company_id\n"
        "        AND report.result_sha256 = p.result_sha256\n"
        "        AND report.normalizer_version = p.normalizer_version\n"
        ")\n"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=RATSIT_COLUMN_SQL,
        from_sql="FROM people AS r",
        where_sql="WHERE trim(r.name_raw) != ''",
        with_sql=ratsit_report_cte_sql(scoped=scoped),
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash.

    The stamp is the CURRENT report's normalized_at, picked the way the live branch picks the
    report -- universe join included, so `since` cannot offer a company the page then drops.
    A max() over every report would be the same number today (the newest report is also the
    newest stamp), but stays honest if Ratsit ever back-fills an older scan.
    """
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        c.company_id AS company_id,\n"
        "        toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "    FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        "    WHERE c.normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id\n"
        ")"
    )


def ratsit_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=ratsit_live_sql())


def ratsit_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=ratsit_live_sql(scoped=True))


se_company_person_suggestions_ratsit = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=RATSIT_PERSON_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_SELECT_PARAMS,
    changed_scope_override=ratsit_changed_scope_sql(),
    deps=[
        # The table-named keys the se_ratsit_normalized multi-asset declares. Never
        # dg.AssetKey("se_ratsit_normalized"): that is the function's name, not a key, and a
        # dep on it makes a phantom node in the graph (spec 4.1).
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_responsible_people"),
        # The universe, exactly as bolagsverket.py and wikidata.py declare it.
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every named person of a company's newest normalized Ratsit report as a raw person "
        "suggestion in se_company_person_suggestion (slot = the profile-URL token, "
        "role-qualified when one report repeats it, idx:<person_index> when the row has no "
        "URL): the name, the birth year from the URL's date, the Swedish role label, the "
        "scan year as the role year, and data with age, identity_available, profile_url, "
        "display_name_raw, the Ratsit person id and an external flag. Nameless rows are "
        "skipped; a slot the newest report no longer delivers is tombstoned. "
        "execute=false previews."
    ),
)
```

- [ ] **Step 5: Run the tests and watch them pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_person_extractors_sql.py -q 2>&1 | tail -3
```

Expected: `15 passed` (the 12 that were there, plus the 3 new ones). The generic loops — sixteen columns, two `%(company_ids)s` bindings, the UNION ALL tombstone shape, the state-hash scope — now cover ratsit too; if one of them fails, the module's SQL is wrong, not the test.

- [ ] **Step 6: Prove the definitions still load**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run dg check defs 2>&1 | tail -5
uv run dg list defs 2>&1 | rg se_company_person_suggestions
```

Expected: `dg check defs` reports no errors, and the list shows four `se_company_person_suggestions_*` assets — the new one is picked up by defs auto-discovery as soon as the module exists, before Task 2 registers it in the job.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE="$(git rev-parse --git-dir)/RATSIT_PERSON_EXTRACTOR_MSG"
cat > "$MSGFILE" <<'EOF'
feat(se_company/person): Ratsit responsible people as a person extractor

The current Ratsit report per company (se_ratsit_company FINAL, newest
normalized_at) joined to se_ratsit_responsible_people on the report key, one
suggestion per named person. The slot is Ratsit's own person id (the profile-URL
token), role-qualified when a report repeats it and idx:<person_index> when the
row carries no URL; the birth year comes from the URL's date and `data` carries
age, identity_available, profile_url, display_name_raw, the person id and an
external flag through mapFilter, so a NULL value leaves its key out.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/ratsit.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

---

### Task 2: Register the source, map the roles, retire the "reserved" wording

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py:1-3,42`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/roles.py:44-66,92-95`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/precedence.py:11-17`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md:17-19,110-137`
- Create: `corpscout/services/dagster_v3/tests/test_se_company_person_roles.py`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py:202`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_common.py:466`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_precedence.py:25-28`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_jobs.py:1-2,15`

**Interfaces:**
- Consumes: Task 1's module — `dagster_v3.defs.se_company.person.ratsit.se_company_person_suggestions_ratsit` (asset key `se_company_person_suggestions_ratsit`).
- Produces: `assets.EXTRACTOR_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")` and therefore `assets.EXTRACTOR_ASSET_NAMES == ("se_company_person_suggestions_bolagsverket", "se_company_person_suggestions_esef", "se_company_person_suggestions_wikidata", "se_company_person_suggestions_ratsit")`, which `jobs.py` reads for `se_company_person_extract_job`'s selection and `WEEKLY_RUN_CONFIG`, and `assets.py` reads for `se_company_person_normalize`'s `deps`. `roles.RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE: Mapping[str, str]` and `roles.SOURCE_ROLE_MAPPINGS["ratsit"]`, both keyed on the lowercased, trimmed Swedish label, so `role_code_for("ratsit", role_original="VD", role_key=None) == "chief_executive_officer"`. Task 3 relies on the registration (the normalize hand-off reads `role_code_for`); Task 5 launches `se_company_person_suggestions_ratsit`.

- [ ] **Step 1: Write the failing role tests**

Create `tests/test_se_company_person_roles.py`:

```python
"""The per-source role maps (spec 2026-09-09 section 4.3, and 2026-09-11 section 4.3 for
Ratsit).

`role_code_for` is otherwise exercised through the normalize tests. Ratsit is the first
source whose map is keyed on the human label ALONE -- it delivers no machine code, so its
suggestions carry `role_key` NULL and the label is the only key there is -- which is worth
its own file.
"""

import pytest

from dagster_v3.defs.se_company.person.roles import (
    SOURCE_ROLE_MAPPINGS,
    SOURCE_ROLELESS_CODES,
    role_code_for,
)


@pytest.mark.parametrize(
    ("label", "code"),
    [
        ("VD", "chief_executive_officer"),
        ("Extern VD", "chief_executive_officer"),
        ("Vice VD", "deputy_chief_executive_officer"),
        ("Extern vice VD", "deputy_chief_executive_officer"),
        ("Ställföreträdande VD", "deputy_chief_executive_officer"),
        ("Extern firmatecknare", "legal_representative"),
        ("Prokurist", "procurist"),
        ("Delgivningsbar person", "other_representative"),
    ],
)
def test_every_ratsit_label_maps_to_its_catalog_code(label: str, code: str) -> None:
    """The eight labels Ratsit delivers 301,081 times (prod 2026-09-10). The extractor
    passes role_key=None: Ratsit has no machine code, so the label is the key."""
    assert role_code_for("ratsit", role_original=label, role_key=None) == code


def test_extern_keeps_the_role_and_lives_in_the_data_instead() -> None:
    """`Extern VD` is a CEO who is not an employee. The outsider flag is `data.external` on
    the suggestion (spec 4.2), never a different role code."""
    assert role_code_for("ratsit", role_original="Extern VD") == role_code_for(
        "ratsit", role_original="VD"
    )
    assert role_code_for("ratsit", role_original="Extern vice VD") == role_code_for(
        "ratsit", role_original="Vice VD"
    )


def test_an_unmapped_ratsit_label_publishes_as_itself() -> None:
    """`Aktuarie` (2 rows on prod 2026-09-10) has no catalog code; the passthrough rule
    publishes the delivered label, lowercased and whitespace-collapsed (owner ruling
    2026-08-28: never dropped, never bucketed)."""
    assert role_code_for("ratsit", role_original="  Aktuarie ") == "aktuarie"


def test_ratsit_has_a_map_and_no_roleless_labels() -> None:
    assert set(SOURCE_ROLE_MAPPINGS) == {"bolagsverket", "esef", "wikidata", "ratsit"}
    assert set(SOURCE_ROLE_MAPPINGS["ratsit"].values()) == {
        "chief_executive_officer",
        "deputy_chief_executive_officer",
        "legal_representative",
        "procurist",
        "other_representative",
    }
    # Every Ratsit label is both person evidence AND a role: nothing is roleless (spec 4.3),
    # so the source has no SOURCE_ROLELESS_CODES entry at all.
    assert "ratsit" not in SOURCE_ROLELESS_CODES
```

Then update the two existing pins.

`tests/test_se_company_person_extractors_sql.py:202`, inside `test_assets_are_named_grouped_and_declared`:

```python
    assert assets.EXTRACTOR_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")
```

`tests/test_se_company_common.py:466`, the last line of `test_the_retired_people_chain_is_gone_from_the_source_tree`:

```python
    assert set(SOURCE_ROLE_MAPPINGS) == {"bolagsverket", "esef", "wikidata", "ratsit"}
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_person_roles.py tests/test_se_company_person_extractors_sql.py tests/test_se_company_common.py -q 2>&1 | tail -8
```

Expected: the eight parametrized label cases fail (`assert None == 'chief_executive_officer'` — the passthrough returns the lowercased label, so `role_code_for("ratsit", role_original="VD")` is `'vd'` today), `test_extern_keeps_the_role_and_lives_in_the_data_instead` passes accidentally (both sides are passthroughs — leave it, it is a real pin once the map exists), `test_ratsit_has_a_map_and_no_roleless_labels` fails on the three-key set, and the two pin edits fail. `test_an_unmapped_ratsit_label_publishes_as_itself` passes now and must keep passing.

- [ ] **Step 3: Add the Ratsit role map**

In `src/dagster_v3/defs/se_company/person/roles.py`, after the Wikidata block (line 52, `WIKIDATA_ROLELESS_PROPERTIES`) and before `SOURCE_ROLE_MAPPINGS`:

```python
# Ratsit's responsible-people labels (spec 2026-09-11 section 4.3), keyed on the lowercased,
# trimmed Swedish label: Ratsit delivers no machine code, so the extractor writes role_key
# NULL and role_code_for falls through to the label. `Extern` marks a role held by someone
# outside the company; that distinction lives in the suggestion's `data.external`, not in the
# code, so `extern vd` maps exactly where `vd` does. `aktuarie` (2 rows on prod 2026-09-10)
# is deliberately absent and publishes as itself.
RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "vd": "chief_executive_officer",
    "extern vd": "chief_executive_officer",
    "vice vd": "deputy_chief_executive_officer",
    "extern vice vd": "deputy_chief_executive_officer",
    "ställföreträdande vd": "deputy_chief_executive_officer",
    "extern firmatecknare": "legal_representative",
    "prokurist": "procurist",
    "delgivningsbar person": "other_representative",
}
```

Add the entry to `SOURCE_ROLE_MAPPINGS` (after `"wikidata"`):

```python
    "ratsit": RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE,
```

`SOURCE_ROLELESS_CODES` gets **no** ratsit entry — `_ROLELESS.get(source, frozenset())` already handles a source that has none.

Fix the `role_code_for` docstring (lines 91-95). Before:

```python
    Swedish label), and what comes back when neither maps is the delivered label, lowercased
    and trimmed. A key in the source's roleless set means "person evidence, no role" and
    returns None. Sources with no map at all -- reviewer, reviewer_draft, ratsit -- always
    take the passthrough.
```

After:

```python
    Swedish label), and what comes back when neither maps is the delivered label, lowercased
    and trimmed. A key in the source's roleless set means "person evidence, no role" and
    returns None. Sources with no map at all -- reviewer, reviewer_draft -- always take the
    passthrough, and so does a label a mapped source's map does not know (Ratsit's
    `aktuarie`).
```

- [ ] **Step 4: Register the source**

`src/dagster_v3/defs/se_company/person/assets.py:42`. Before:

```python
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata")
```

After:

```python
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata", "ratsit")
```

And the module docstring (lines 1-3). Before:

```python
"""Dagster assets of the person entity. Slice 0 ships the normalize asset; the extractors
and the stopped weekly live in bolagsverket.py, esef.py, wikidata.py and jobs.py (slice 1),
the fold and the precedence export in slice 2."""
```

After:

```python
"""Dagster assets of the person entity. Slice 0 ships the normalize asset; the extractors
and the stopped weekly live in bolagsverket.py, esef.py, wikidata.py and jobs.py (slice 1;
ratsit.py joined them 2026-09-11), the fold and the precedence export in slice 2."""
```

Nothing else changes: `se_company_person_normalize`'s `deps`, `se_company_person_extract_job`'s selection and `WEEKLY_RUN_CONFIG` are all derived from `EXTRACTOR_ASSET_NAMES`.

- [ ] **Step 5: Run the tests and watch them pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a          # test_..._jobs.py loads the whole defs tree
uv run pytest tests/test_se_company_person_roles.py tests/test_se_company_person_extractors_sql.py \
  tests/test_se_company_common.py tests/test_se_company_person_jobs.py \
  tests/test_se_company_person_normalize_se.py -q 2>&1 | tail -3
```

Expected: all green. `test_se_company_person_jobs.py` passes unchanged — it asserts against `EXTRACTOR_ASSET_NAMES`, which now has four entries, and `test_the_normalize_asset_runs_after_the_extractors` proves the new asset became a parent of the normalize asset.

- [ ] **Step 6: Retire the "reserved" wording**

Four edits, all prose. No assertion changes.

(a) `src/dagster_v3/defs/se_company/person/precedence.py`, the `WHY THESE NUMBERS` paragraph. Before:

```python
ranks them here). `ratsit` is reserved at 1000 -- the source has the highest trust of the
machine sources and no extractor yet. Bolagsverket delivers a first/last split from the
```

After:

```python
ranks them here). `ratsit` leads the machine sources at 1000 -- its names are register
spellings delivered with a birth date in the profile URL, the strongest identity evidence
any machine source gives us (extractor `person/ratsit.py` since 2026-09-11). Bolagsverket
delivers a first/last split from the
```

(b) `tests/test_se_company_person_precedence.py:25-28`. Before:

```python
def test_the_reviewer_outranks_every_source_including_the_reserved_ratsit() -> None:
    """Slice 3's backoffice writes reviewer rows at source `reviewer`; the fold already
    ranks them above ratsit, which is reserved and has no extractor yet."""
```

After:

```python
def test_the_reviewer_outranks_every_source_including_ratsit() -> None:
    """Slice 3's backoffice writes reviewer rows at source `reviewer`; the fold ranks them
    above ratsit, which since 2026-09-11 has an extractor of its own and the highest
    machine-source spelling precedence."""
```

(c) `tests/test_se_company_person_jobs.py`, the module docstring and one test name. Before:

```python
"""The person extract job and its STOPPED weekly (spec 2026-09-09 section 6), plus the
normalize asset's dependence on the three extractors."""
```

```python
def test_the_job_selects_the_three_extractors_and_the_normalize_asset() -> None:
```

After:

```python
"""The person extract job and its STOPPED weekly (spec 2026-09-09 section 6), plus the
normalize asset's dependence on every extractor."""
```

```python
def test_the_job_selects_every_extractor_and_the_normalize_asset() -> None:
```

(d) `src/dagster_v3/defs/se_company/person/docs/person-design.md`. Insert a module-table row after the `wikidata.py` row (line 17):

```markdown
| `ratsit.py` | The Ratsit responsible-people extractor `se_company_person_suggestions_ratsit`: the newest normalized report per company, one row per named person, slot = the profile-URL token (role-qualified when a report repeats it, `idx:<person_index>` without a URL), `role_key` NULL |
```

Line 18 (`jobs.py` row): `(the three extractors plus the normalize asset)` becomes `(the four extractors plus the normalize asset)`.

Line 19 (`precedence.py` row): `ratsit 1000 (reserved)` becomes `ratsit 1000`.

Line 110 heading and lines 112-114: `## Extractors (slice 1)` becomes `## Extractors (slice 1; ratsit 2026-09-11)`, and

```markdown
`se_company_person_suggestions_<source>` (`bolagsverket`, `esef`, `wikidata`), on the same
```

becomes

```markdown
`se_company_person_suggestions_<source>` (`bolagsverket`, `esef`, `wikidata`, `ratsit`), on the same
```

A fourth table row after line 120:

```markdown
| `ratsit.py` | `se_ratsit_company` + `se_ratsit_responsible_people`, joined on the report key, inside the basic-info universe | the `profile_url` token, `:<role>` when a report repeats it, else `idx:<person_index>` | one name string, birth year from the URL's date, the Swedish label as `role_original` with `role_key` NULL (no machine code), the scan year as the role year, `data` = age, identity_available, profile_url, display_name_raw, ratsit person id, external |
```

Line 122's universe sentence -- "The universe is `se_company_basic_info`: every extractor
joins the folded company and drops everything else" -- **stays exactly as it is**: the person
entity's universe is the basic-info entity by design, and `ratsit.py` joins it like its three
siblings (controller ruling 2026-09-11). Do not touch that paragraph.

Line 134: `selects the three extractors and` becomes `selects the four extractors and`.

- [ ] **Step 7: Full person suite and definitions check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run pytest tests -q -m "not integration" -k "se_company_person or se_company_common" 2>&1 | tail -3
uv run dg check defs 2>&1 | tail -3
rg -n "reserved" src/dagster_v3/defs/se_company/person tests/test_se_company_person_precedence.py
```

Expected: green; `dg check defs` clean; the `rg` finds no surviving "reserved" claim about ratsit (a hit in another context is fine — read it before deciding).

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE="$(git rev-parse --git-dir)/RATSIT_PERSON_REGISTER_MSG"
cat > "$MSGFILE" <<'EOF'
feat(se_company/person): register the Ratsit extractor and map its roles

EXTRACTOR_SOURCES gains ratsit, which carries the asset into the extract job,
the STOPPED weekly's run config and the normalize asset's deps by construction.
roles.py gains the eight Ratsit labels (VD/Extern VD -> chief_executive_officer,
the three deputy forms, Extern firmatecknare -> legal_representative, Prokurist,
Delgivningsbar person -> other_representative); Aktuarie keeps the passthrough
and the source has no roleless labels. The "ratsit is reserved, no extractor
yet" wording goes from precedence.py, person-design.md and the two test files.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/roles.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/precedence.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
  corpscout/services/dagster_v3/tests/test_se_company_person_roles.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py \
  corpscout/services/dagster_v3/tests/test_se_company_common.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_precedence.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_jobs.py
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

---

### Task 3: The Ratsit rows on a real ClickHouse

**Files:**
- Modify: `corpscout/services/dagster_v3/tests/fixtures/se_company_person_source_tables.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 1's `ratsit.ratsit_select_sql()`, `ratsit.ratsit_changed_scope_sql()`, `ratsit.RATSIT_PERSON_EXTRACTOR_VERSION`; Task 2's `roles.SOURCE_ROLE_MAPPINGS["ratsit"]` is **not** exercised here (this file stops at the raw suggestion rows).
- Produces: nothing importable. It is the proof that the module's SQL runs, that the universe join really drops a company the entity does not know, that the UNION ALL's two branches agree on all sixteen column types against the real table (`CONSTRAINT valid_data`, `Code: 469`, is the judge for `data`), and that the state-hash scope converges.

Everything in this file runs as ONE `clickhouse-local` script per `join_use_nulls` setting: `_script_statements()` returns the statements, the module-scoped `sections` fixture pipes them through `clickhouse_local_command()` (a `clickhouse-local` binary if the machine has one, else `docker run --rm -i clickhouse/clickhouse-server:26.5 clickhouse-local`, else `pytest.skip`), and `_sections()` splits the stdout on the `@@name` marker rows. Do not invent another harness.

- [ ] **Step 1: Add the source table to the fixture file**

Append to `tests/fixtures/se_company_person_source_tables.sql`:

```sql

-- corpscout.se_ratsit_responsible_people: migration 000343's CREATE with 000346's four v2
-- columns (display_name_raw, name, age, identity_available) inlined in their ALTER order;
-- CODECs and CONSTRAINTs stripped like the neighbouring snapshots. It cannot ride
-- WANTED_CREATES: that helper lifts a whole CREATE TABLE statement out of ONE named
-- migration, and 000346 only ALTERs this table. se_ratsit_company needs no entry -- its DDL
-- is already in se_basic_info_source_tables.sql, which this test also loads.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_responsible_people (
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `person_index` UInt16,
    `display_name` Nullable(String),
    `display_name_raw` Nullable(String),
    `name` Nullable(String),
    `age` Nullable(UInt16),
    `identity_available` Bool DEFAULT false,
    `role` Nullable(String),
    `profile_url` Nullable(String),
    `normalized_at` DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(normalized_at) ORDER BY (company_id, result_sha256, normalizer_version, person_index);
```

- [ ] **Step 2: Write the failing test**

Five edits to `tests/test_se_company_person_extractors_clickhouse_local.py`.

(a) The module docstring gains a claim, after claim 7:

```python
8. The Ratsit branch really picks the newest report per company (an older scan's people must
   not leak), drops a company outside the basic-info universe, skips the nameless rows, gives
   the two-row token a role-qualified slot and the URL-less row an `idx:` slot, and keeps a
   slot stable across a re-scan while tombstoning the one the new report dropped.
```

(b) Imports — line 29 gains `ratsit`, and the normalizer version joins the imports:

```python
from dagster_v3.defs.se_company.person import bolagsverket, esef, ratsit, tables, wikidata
```

```python
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
```

(c) Constants, after the `COMPANY_BV` / `COMPANY_WD` / `COMPANY_OUTSIDE` block:

```python
# Ratsit (spec 2026-09-11 section 4.5). Two companies: the first exercises the slot rules and
# the re-scan, the second proves an older report cannot leak.
COMPANY_RATSIT_A = "5565550001"
COMPANY_RATSIT_B = "5565550002"
# Scanned by Ratsit, absent from se_company_basic_info: the universe join must drop it.
COMPANY_RATSIT_OUTSIDE = "5565550003"
RATSIT_URL_A = "https://www.ratsit.se/19800101-Anna_Ek/abc123"
RATSIT_URL_B = "https://www.ratsit.se/19751212-Cecilia_Nord/xyz789"
RATSIT_URL_OUTSIDE = "https://www.ratsit.se/19700707-Nils_Utanfor/out999"
RATSIT_REPORT_A1 = "a" * 64        # the first scan of company A
RATSIT_REPORT_A2 = "d" * 64        # its re-scan, one person short
RATSIT_REPORT_B_OLD = "b" * 64     # company B's superseded scan
RATSIT_REPORT_B = "c" * 64         # company B's current report
RATSIT_REPORT_OUTSIDE = "e" * 64   # the out-of-universe company's report
RATSIT_OUTSIDE_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_RATSIT_OUTSIDE}'"
)
# toJSONString escapes '/' as '\/' and the TSV dump doubles the backslash, exactly as
# WIKIDATA_DATA below records it.
RATSIT_DATA_VD = (
    '{"age":"45","identity_available":"true",'
    '"profile_url":"https:\\\\/\\\\/www.ratsit.se\\\\/19800101-Anna_Ek\\\\/abc123",'
    '"display_name_raw":"Anna Ek, 45 år","ratsit_person_id":"abc123","external":"false"}'
)
# No age, no URL: mapFilter drops the three empty keys instead of rendering them null.
RATSIT_DATA_BO = '{"identity_available":"true","display_name_raw":"Bo Ek","external":"true"}'
RATSIT_ROWS_SQL = (
    f"SELECT {', '.join(PERSON_SELECT_COLUMNS)}, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE source = 'ratsit' "
    "ORDER BY company_id, slot"
)
RATSIT_COMPANY_COLUMNS = (
    "company_id, result_sha256, normalizer_version, schema_version, parser_version, "
    "requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
    "source_date_modified, industry_code_count, summary_count, responsible_people_count, "
    "establishment_count, financial_report_count, financial_period_count, "
    "people_at_address_count, normalized_at"
)
```

(d) The two shared helpers take a source's own select parameters, and two Ratsit row builders join them. Replace `_insert` and `_scope` with:

```python
def _insert(select_sql: str, ids: list[str], *, extractor_version: str, **params: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=PERSON_TARGET),
        {
            "company_ids": ids, "source_run_id": "run-1",
            "extractor_version": extractor_version, **params,
        },
    )


def _scope(scope_sql: str, source: str, **params: str) -> str:
    """`params` carries a source's own select parameters (Ratsit's normalizer_version);
    `render` asserts no `%(name)s` survives, so a forgotten one fails loudly right here."""
    return render(scope_sql, {"source": source, **params}) + "\nORDER BY company_id"
```

and add, next to `_signatory_insert` and `_register_row`:

```python
def _ratsit_report(company_id: str, sha: str, normalized_at: str, modified: str | None) -> str:
    """One se_ratsit_company row. The table is ReplacingMergeTree(normalized_at) ordered by
    (company_id, result_sha256, normalizer_version) and is never pruned, so a re-scanned
    company keeps both rows and only the newest normalized_at is the current report."""
    source_date = "NULL" if modified is None else f"toDate32('{modified}')"
    orgnr = company_id[-10:]
    return (
        f"INSERT INTO corpscout.se_ratsit_company ({RATSIT_COMPANY_COLUMNS}) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', 1, 'ratsit-parser-v1', "
        f"'https://www.ratsit.se/{orgnr}', 'https://www.ratsit.se/{orgnr}', 'bucket', "
        f"'sweden_ratsit/pilot/company_id={company_id}/report.json', 'Exempel AB', '{orgnr}', "
        f"{source_date}, 0, 0, 0, 0, 0, 0, 0, toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _ratsit_person(
    company_id: str,
    sha: str,
    index: int,
    *,
    name: str | None,
    role: str,
    url: str | None,
    age: int | None,
    display_raw: str | None,
    normalized_at: str,
) -> str:
    """One se_ratsit_responsible_people row. `name=None` is the GDPR-limited shape the
    normalizer writes for a role-only entry: no name, no age, no URL, identity_available
    false (migration 000346's CONSTRAINT se_ratsit_responsible_identity)."""

    def text(value: str | None) -> str:
        return "NULL" if value is None else "'" + value.replace("'", "''") + "'"

    return (
        "INSERT INTO corpscout.se_ratsit_responsible_people (company_id, result_sha256, "
        "normalizer_version, person_index, display_name, display_name_raw, name, age, "
        "identity_available, role, profile_url, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', {index}, {text(name)}, "
        f"{text(display_raw)}, {text(name)}, {'NULL' if age is None else age}, "
        f"{0 if name is None else 1}, '{role}', {text(url)}, "
        f"toDateTime64('{normalized_at}', 6, 'UTC'))"
    )
```

(e) `_script_statements()`: build the two Ratsit texts beside the others, at the top of the function —

```python
    ratsit_scope = _scope(
        ratsit.ratsit_changed_scope_sql(), "ratsit",
        normalizer_version=RATSIT_NORMALIZER_VERSION,
    )
    ratsit_insert = _insert(
        ratsit.ratsit_select_sql(),
        [COMPANY_RATSIT_A, COMPANY_RATSIT_B, COMPANY_RATSIT_OUTSIDE],
        extractor_version=ratsit.RATSIT_PERSON_EXTRACTOR_VERSION,
        normalizer_version=RATSIT_NORMALIZER_VERSION,
    )
```

The page is asked for all three ids on purpose: the out-of-universe company must be dropped
by the SQL, not by the caller's id list.

Give the two in-universe companies their `se_company_basic_info` row exactly as the file
already seeds the universe for `COMPANY_BV` and `COMPANY_WD` — two more lines directly under
those, and the comment extended:

```python
        # The universe: COMPANY_OUTSIDE and COMPANY_RATSIT_OUTSIDE deliberately have no
        # basic-info row.
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_BV}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_WD}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_RATSIT_A}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_RATSIT_B}')",
```

— and append this block at the END of the returned list, after `"SELECT '@@data_check_3'"` and `DATA_CHECK_SQL`:

```python
        # --- Ratsit (spec 2026-09-11 section 4.5) ----------------------------------
        # Company A's first scan: a VD with a dated URL, a Delgivningsbar person sharing
        # that token, a nameless GDPR row and a named row with no URL at all.
        _ratsit_report(COMPANY_RATSIT_A, RATSIT_REPORT_A1, "2026-09-09 00:00:00", "2026-08-30"),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 0, name="Anna Ek", role="VD",
            url=RATSIT_URL_A, age=45, display_raw="Anna Ek, 45 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 1, name="Anna Ek",
            role="Delgivningsbar person", url=RATSIT_URL_A, age=45,
            display_raw="Anna Ek, 45 år", normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 2, name=None, role="Extern firmatecknare",
            url=None, age=None, display_raw=None, normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 3, name="Bo Ek", role="Extern VD",
            url=None, age=None, display_raw="Bo Ek", normalized_at="2026-09-09 00:00:00",
        ),
        # Company B: a superseded scan that must not leak, then the current report (no
        # source_date_modified, so the role year comes from the scan's own stamp).
        _ratsit_report(COMPANY_RATSIT_B, RATSIT_REPORT_B_OLD, "2026-08-01 00:00:00", "2024-05-05"),
        _ratsit_person(
            COMPANY_RATSIT_B, RATSIT_REPORT_B_OLD, 0, name="Old Vd", role="VD",
            url="https://www.ratsit.se/19600101-Old_Vd/old111", age=None,
            display_raw="Old Vd", normalized_at="2026-08-01 00:00:00",
        ),
        _ratsit_report(COMPANY_RATSIT_B, RATSIT_REPORT_B, "2026-09-09 00:00:00", None),
        _ratsit_person(
            COMPANY_RATSIT_B, RATSIT_REPORT_B, 0, name="Cecilia Nord", role="Prokurist",
            url=RATSIT_URL_B, age=51, display_raw="Cecilia Nord, 51 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        # Scanned like the others, but the entity has never heard of it.
        _ratsit_report(
            COMPANY_RATSIT_OUTSIDE, RATSIT_REPORT_OUTSIDE, "2026-09-09 00:00:00", "2026-08-30"
        ),
        _ratsit_person(
            COMPANY_RATSIT_OUTSIDE, RATSIT_REPORT_OUTSIDE, 0, name="Nils Utanfor", role="VD",
            url=RATSIT_URL_OUTSIDE, age=55, display_raw="Nils Utanfor, 55 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        "SELECT '@@ratsit_scope_1'",
        ratsit_scope,
        ratsit_insert,
        "SELECT '@@ratsit_rows_1'",
        RATSIT_ROWS_SQL,
        "SELECT '@@ratsit_scope_2'",
        ratsit_scope,
        "SELECT '@@ratsit_outside_check'",
        RATSIT_OUTSIDE_CHECK_SQL,
        # The re-scan: Bo Ek is gone and both Anna rows keep their slots under a new hash.
        "SELECT sleep(0.01) FORMAT Null",
        _ratsit_report(COMPANY_RATSIT_A, RATSIT_REPORT_A2, "2026-09-10 00:00:00", "2026-09-09"),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A2, 0, name="Anna Ek", role="VD",
            url=RATSIT_URL_A, age=46, display_raw="Anna Ek, 46 år",
            normalized_at="2026-09-10 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A2, 1, name="Anna Ek",
            role="Delgivningsbar person", url=RATSIT_URL_A, age=46,
            display_raw="Anna Ek, 46 år", normalized_at="2026-09-10 00:00:00",
        ),
        "SELECT '@@ratsit_scope_3'",
        ratsit_scope,
        ratsit_insert,
        "SELECT '@@ratsit_rows_2'",
        RATSIT_ROWS_SQL,
        "SELECT '@@ratsit_scope_4'",
        ratsit_scope,
        "SELECT '@@data_check_4'",
        DATA_CHECK_SQL,
```

and the three assertions at the end of the file:

```python
def test_the_ratsit_rows_are_the_current_reports_named_people(sections) -> None:
    # Two companies, not three: COMPANY_RATSIT_OUTSIDE is outside the basic-info universe.
    assert sections["ratsit_scope_1"] == [[COMPANY_RATSIT_A], [COMPANY_RATSIT_B]]
    rows = {
        row["slot"]: row
        for row in _rows(sections, "ratsit_rows_1")
        if row["company_id"] == COMPANY_RATSIT_A
    }
    # person_index 2 is the nameless GDPR row: a role, no identity, never a person.
    assert set(rows) == {"abc123:vd", "abc123:delgivningsbar person", "idx:3"}
    vd = rows["abc123:vd"]
    assert vd["full_name"] == "Anna Ek"
    assert vd["first_name"] == vd["last_name"] == "\\N"
    assert vd["birth_year"] == "1980"                    # from the URL's 19800101
    assert vd["role_original"] == "VD"
    assert vd["role_key"] == "\\N"                       # Ratsit has no machine code
    assert vd["fiscal_year"] == "2026"                   # source_date_modified 2026-08-30
    assert vd["wikidata_id"] == vd["role_from"] == vd["role_to"] == "\\N"
    assert vd["document_ref"] == "\\N"
    assert vd["source_record_id"] == f"ratsit:{RATSIT_REPORT_A1}:0"
    assert vd["data"] == RATSIT_DATA_VD
    # The same token twice in one report: both slots carry the lowercased role.
    assert rows["abc123:delgivningsbar person"]["role_original"] == "Delgivningsbar person"
    assert rows["abc123:delgivningsbar person"]["birth_year"] == "1980"
    # Named, but no URL: no token, so the slot is the person index, the birth year is NULL
    # and `Extern VD` sets data.external.
    bo = rows["idx:3"]
    assert bo["full_name"] == "Bo Ek" and bo["birth_year"] == "\\N"
    assert bo["data"] == RATSIT_DATA_BO
    assert sections["ratsit_scope_2"] == []              # the scan converges in one pass


def test_a_ratsit_company_outside_the_basic_info_universe_is_never_written(sections) -> None:
    """The universe join of `report`: COMPANY_RATSIT_OUTSIDE has a current report and a named
    VD, and the page select was handed its id, but the entity has no basic-info row for it --
    so it produces no live row, no suggestion, and no state-hash scope hit either (it is on
    neither side of the comparison)."""
    assert sections["ratsit_outside_check"] == [["0"]]
    assert [COMPANY_RATSIT_OUTSIDE] not in sections["ratsit_scope_1"]
    assert not [
        r for r in _rows(sections, "ratsit_rows_1")
        if r["company_id"] == COMPANY_RATSIT_OUTSIDE
    ]


def test_only_the_newest_ratsit_report_reaches_the_suggestion_table(sections) -> None:
    """Company B carries two scans. `Old Vd` belongs to the superseded one and the people
    rows join the report's own (company_id, result_sha256, normalizer_version), so it never
    appears -- the report row itself is never deleted."""
    rows = [r for r in _rows(sections, "ratsit_rows_1") if r["company_id"] == COMPANY_RATSIT_B]
    assert len(rows) == 1
    [row] = rows
    assert row["slot"] == "xyz789"                       # a token seen once keeps it bare
    assert row["full_name"] == "Cecilia Nord"
    assert row["role_original"] == "Prokurist"
    assert row["source_record_id"] == f"ratsit:{RATSIT_REPORT_B}:0"
    # No source_date_modified on the current report: the role year is the scan's stamp.
    assert row["fiscal_year"] == "2026"


def test_a_ratsit_slot_survives_a_rescan_and_a_dropped_person_is_tombstoned(sections) -> None:
    """The token is Ratsit's own person id, so a re-scan rewrites the person's row in place
    (new source_record_id, same slot) while the slot the new report no longer delivers gets
    the shared per-slot tombstone."""
    assert sections["ratsit_scope_3"] == [[COMPANY_RATSIT_A]]
    before = {
        r["slot"]: r
        for r in _rows(sections, "ratsit_rows_1")
        if r["company_id"] == COMPANY_RATSIT_A
    }
    rows = {
        r["slot"]: r
        for r in _rows(sections, "ratsit_rows_2")
        if r["company_id"] == COMPANY_RATSIT_A
    }
    assert set(rows) == set(before)
    survivor = rows["abc123:vd"]
    assert survivor["full_name"] == "Anna Ek"
    assert survivor["source_record_id"] == f"ratsit:{RATSIT_REPORT_A2}:0"
    assert survivor["suggested_at"] > before["abc123:vd"]["suggested_at"]
    tombstone = rows["idx:3"]
    assert tombstone["data"] == "{}"
    assert tombstone["source_record_id"] == ""
    for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                   "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                   "document_ref"):
        assert tombstone[column] == "\\N", column
    assert sections["ratsit_scope_4"] == []
    assert sections["data_check_4"] == [["0"]]
```

- [ ] **Step 3: Run it and watch it fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_person_extractors_clickhouse_local.py -q -m integration 2>&1 | tail -15
```

Expected before Step 1's fixture DDL exists: every test in the file fails with the script's stderr in the assertion message — `Code: 60. DB::Exception: Unknown table expression identifier 'corpscout.se_ratsit_responsible_people'`. Run it in this order **once** (fixture first, then the test edits) and you will instead see the three new tests fail on missing sections; either failure is the right "not there yet" signal. If the file **skips** (`no clickhouse-local binary and no docker to run one`), start Docker — this task cannot be validated without it.

- [ ] **Step 4: Run it green**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_person_extractors_clickhouse_local.py -q -m integration 2>&1 | tail -5
```

Expected: `26 passed` — the file's 9 tests plus the 4 new ones, each run twice (the module-scoped `sections` fixture is parametrized `join_use_nulls` 0 and 1); it collects 18 today. The numbers that matter: both settings give identical Ratsit rows (verified on `clickhouse/clickhouse-server:26.5`, 2026-09-11), and `data_check_4` is `0`, i.e. every row — tombstone included — satisfies `CONSTRAINT valid_data`.

- [ ] **Step 5: Run the whole non-integration suite once**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run pytest tests -q -m "not integration" 2>&1 | tail -5
```

Expected: the same counts as on `main` plus this slice's new tests. A failure anywhere else is this slice's doing — the only shared objects touched are `EXTRACTOR_SOURCES` and `SOURCE_ROLE_MAPPINGS`, whose pins Task 2 updated.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE="$(git rev-parse --git-dir)/RATSIT_PERSON_INTEGRATION_MSG"
cat > "$MSGFILE" <<'EOF'
test(se_company/person): the Ratsit extractor on a real ClickHouse

Three companies through clickhouse-local under join_use_nulls 0 and 1: the slot
rules (bare token, role-qualified token, idx fallback), the nameless row that is
skipped, the birth year from the profile URL, the role year from
source_date_modified and from the scan stamp, data.external, an older report
that must not leak, a company outside the basic-info universe that produces no
row and no scope hit, and a re-scan that keeps the surviving slots and
tombstones the dropped one. The se_ratsit_responsible_people DDL joins the fixture file
(000343 plus 000346's v2 columns) because WANTED_CREATES can only lift a whole
CREATE TABLE out of one migration.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/dagster_v3/tests/fixtures/se_company_person_source_tables.sql \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

---

### Task 4: The backoffice Source filter offers Ratsit

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-person-fields.ts:44-50`
- Modify: `corpscout/services/backoffice/tests/se-person-fields.test.ts:47-50`

**Interfaces:**
- Consumes: nothing from the Dagster tasks at build time — this is the same repo, a different project. It is correct only once Task 5's fold has published Ratsit rows, which is why the smoke test lives in Task 5.
- Produces: `MAIN_PERSON_SOURCES: readonly SePersonSource[] = ["bolagsverket", "esef", "wikidata", "ratsit", "reviewer"]` (the order is `PERSON_SOURCES`' own, minus `DRAFT_SOURCE`). Its only reader is `app/components/admin/se-people-table.tsx:148`, which passes it as the Source facet's `options`; `SOURCE_LABELS.ratsit = "Ratsit"` already exists, so no label work.

- [ ] **Step 1: Record the baseline**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
pnpm typecheck 2>&1 | tail -5
CI=1 pnpm exec vitest run --reporter=dot 2>&1 | tail -20
```

Expected on this branch (slice 1 left it here): `pnpm typecheck` prints only the three `The \`envFile\` option is deprecated` lines and exits 0; `Test Files  2 failed | 123 passed (125)` and `Tests  5 failed | 1335 passed (1340)`, the two failing files being `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx`. Under load a run can show extra timeout failures; **the non-flaky numbers are 125 files and 1,340 tests** — this task changes neither.

- [ ] **Step 2: Write the failing test**

`tests/se-person-fields.test.ts`, lines 47-50. Before:

```ts
    // Minor 5: `reviewer_draft` never folds and `ratsit` is reserved with no data, so
    // both always return zero rows from the list's Source filter -- offer the other
    // four.
    expect([...MAIN_PERSON_SOURCES]).toEqual(["bolagsverket", "esef", "wikidata", "reviewer"]);
```

After:

```ts
    // Minor 5: `reviewer_draft` never folds into a published row, so it always returns
    // zero rows from the list's Source filter -- offer the other five. Ratsit joined
    // them on 2026-09-11 with the person extractor.
    expect([...MAIN_PERSON_SOURCES]).toEqual([
      "bolagsverket", "esef", "wikidata", "ratsit", "reviewer",
    ]);
```

- [ ] **Step 3: Run it and watch it fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-person-fields.test.ts 2>&1 | tail -12
```

Expected: one failing assertion, `- "ratsit"` missing from the received array.

- [ ] **Step 4: Let Ratsit through the filter**

`app/lib/se-person-fields.ts`, lines 44-50. Before:

```ts
/** The sources the main table can actually hold: `reviewer_draft` never folds into a
 * published row and `ratsit` is reserved with no data yet, so both always return zero
 * rows from the People list's Source filter. The list offers these four, not the whole
 * catalogue. */
export const MAIN_PERSON_SOURCES: readonly SePersonSource[] = PERSON_SOURCES.filter(
  (source) => source !== DRAFT_SOURCE && source !== "ratsit",
);
```

After:

```ts
/** The sources the main table can actually hold: `reviewer_draft` never folds into a
 * published row, so it always returns zero rows from the People list's Source filter.
 * The list offers the other five. `ratsit` joined them on 2026-09-11, when the Ratsit
 * person extractor shipped (dagster_v3 `se_company/person/ratsit.py`). */
export const MAIN_PERSON_SOURCES: readonly SePersonSource[] = PERSON_SOURCES.filter(
  (source) => source !== DRAFT_SOURCE,
);
```

- [ ] **Step 5: Run it green, then the whole suite and the typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run tests/se-person-fields.test.ts 2>&1 | tail -5
pnpm typecheck 2>&1 | tail -5
CI=1 pnpm exec vitest run --reporter=dot 2>&1 | tail -20
```

Expected: the file passes; typecheck exits 0 with only the deprecation lines; the suite is still 125 files / 1,340 tests with the same two pre-existing failing files. **A third failing file means this change broke something** — the only other reader of `MAIN_PERSON_SOURCES` is the people table's facet, so look there first.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
MSGFILE="$(git rev-parse --git-dir)/RATSIT_PERSON_BACKOFFICE_MSG"
cat > "$MSGFILE" <<'EOF'
feat(backoffice): offer Ratsit in the People list's Source filter

MAIN_PERSON_SOURCES stops excluding ratsit: the source has an extractor as of
2026-09-11, so the filter no longer offers a value that can only return zero
rows. reviewer_draft stays excluded -- it never folds into a published row.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/backoffice/app/lib/se-person-fields.ts \
  corpscout/services/backoffice/tests/se-person-fields.test.ts
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

---

### Task 5: Prod run (controller)

**The controller runs this task; a task subagent never touches prod.** Every step is a Dagster run, a read-only `SELECT`, or a deploy the owner approves. Nothing here is a code change until Step 11.

**Files:**
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` (the Shipped record under section 8 item 2, Step 11)
- Modify: this plan (ticked, Step 11)

**Interfaces:**
- Consumes: Tasks 1-4, merged to `main` and deployed. Asset names: `se_company_person_suggestions_ratsit` (`ExtractConfig`: `execute`, `company_ids`, `max_companies`, `since`, `page_size` ≤ 20,000), `se_company_person_normalize` (`PersonNormalizeConfig`: `changed_only`, `company_ids`, `page_size`), `se_company_person_fold` (64 static partitions `bucket_00`..`bucket_63`, `PersonFoldConfig`: `changed_only`, `page_size`; pool `se_company_person_fold`, limit 1).
- Produces: the Shipped record the next slice reads.

- [ ] **Step 1: Review, merge, deploy**

1. Review the branch end to end: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info diff main...se-ratsit-source`.
2. The owner merges to `main`. If the main checkout sits on another branch, merge through a worktree that has `main` checked out (memory `se-worktree-deploy-recipe`).
3. Deploy the dagster host from a **pristine worktree at the merge commit** — `light_sync` rsyncs the working tree with `--delete-after`, so a dirty tree ships someone else's WIP:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git worktree add /tmp/deploy-worktree HEAD
cp corpscout/services/dagster_v3/.env /tmp/deploy-worktree/corpscout/services/dagster_v3/.env
cd /tmp/deploy-worktree/corpscout/services/dagster_v3
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml; echo "RC=$?"
```

The dbt-state refresh is **mandatory** and `RC=` is captured explicitly (piping the playbook to `tail` masks its exit code). The missing `.env` copy fails `dg check defs` with a cryptic YAML/column error.

4. Prove the host carries the new asset:

```bash
cat > /tmp/asset-nodes.json <<'JSON'
{"query":"query Nodes { assetNodes(group: {groupName: \"se_company_person\", repositoryLocationName: \"dagster_v3\", repositoryName: \"__repository__\"}) { assetKey { path } } }"}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/asset-nodes.json \
  | python3 -c "import json,sys; [print('/'.join(n['assetKey']['path'])) for n in json.load(sys.stdin)['data']['assetNodes']]" | sort
```

Expected: the list contains `se_company_person_suggestions_ratsit` beside the three older extractors, `se_company_person_normalize`, `se_company_person_fold`, `se_company_person_fold_companies` and `se_company_person_precedence_clickhouse`.

- [ ] **Step 2: Confirm prod's state before anything runs**

```bash
cat > /tmp/instigators.json <<'JSON'
{"query":"query Instigators($repositorySelector: RepositorySelector!) { schedulesOrError(repositorySelector: $repositorySelector) { __typename ... on Schedules { results { name cronSchedule scheduleState { status } } } } }","variables":{"repositorySelector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__"}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/instigators.json \
  | python3 -c "import json,sys; [print(s['name'], s['cronSchedule'], s['scheduleState']['status']) for s in json.load(sys.stdin)['data']['schedulesOrError']['results'] if 'se_company' in s['name']]"
```

Expected: `se_company_person_weekly 25 7 * * 1 STOPPED` (and the basic-info and address weeklies STOPPED too). **RUNNING anywhere here means stop and tell the owner** (spec section 6).

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the person entity, before
SELECT
    count()                                   AS rows,
    countIf(active = 1)                       AS active_rows,
    uniqExactIf(company_id, active = 1)       AS companies_with_a_person,
    countIf(active = 1 AND has(sources, 'ratsit'))       AS ratsit_rows,
    countIf(active = 1 AND length(sources) > 1)          AS multi_source_rows,
    countIf(active = 1 AND birth_year IS NOT NULL)       AS rows_with_a_birth_year
FROM corpscout.se_company_person FINAL;

-- (b) the suggestion and normalized layers, by source
SELECT source, count() AS rows, uniqExact(company_id) AS companies
FROM corpscout.se_company_person_suggestion FINAL GROUP BY source ORDER BY source;

SELECT source, count() AS rows, countIf(parse_status = 'ok') AS ok
FROM corpscout.se_company_person_normalized FINAL GROUP BY source ORDER BY source;

-- (c) what the extractor is about to see: the current report's named people, inside the
--     basic-info universe the extractor joins
SELECT
    uniqExact(p.company_id)                                     AS companies,
    count()                                                     AS live_rows,
    countIf(ifNull(p.profile_url, '') != '')                    AS rows_with_a_url
FROM corpscout.se_ratsit_responsible_people AS p FINAL
INNER JOIN (
    SELECT c.company_id AS company_id, c.result_sha256 AS result_sha256,
           c.normalizer_version AS normalizer_version
    FROM corpscout.se_ratsit_company AS c FINAL
    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe
        ON universe.company_id = c.company_id
    WHERE c.normalizer_version = 'ratsit-normalizer-v2'
    ORDER BY c.normalized_at DESC, c.result_sha256 DESC
    LIMIT 1 BY c.company_id
) AS report
    ON report.company_id = p.company_id
   AND report.result_sha256 = p.result_sha256
   AND report.normalizer_version = p.normalizer_version
WHERE trim(ifNull(p.name, '')) != '';

-- (c2) and how many companies the universe join drops (expect 0 or a handful: the Ratsit
--      scan is dispatched from the register, so its ids are register ids)
SELECT count() AS ratsit_companies_outside_the_universe
FROM (
    SELECT DISTINCT company_id FROM corpscout.se_ratsit_company FINAL
    WHERE normalizer_version = 'ratsit-normalizer-v2'
) AS ratsit
LEFT ANTI JOIN (
    SELECT company_id FROM corpscout.se_company_basic_info FINAL
) AS universe ON universe.company_id = ratsit.company_id;

-- (d) the precedence rows are already exported and must NOT be rewritten
SELECT source, precedence, decided_at
FROM corpscout.se_company_person_precedence FINAL
WHERE company_id = '' AND removed = 0 ORDER BY precedence DESC;
SQL
```

Expected (spec section 2, prod 2026-09-10): (a) `ratsit_rows` **0**; (b) no `ratsit` row in either table; (c) ≈236,800 companies and ≈277,600 live rows, ~277,592 of them with a URL, minus whatever (c2) reports; (d) the five rows `reviewer 20000, ratsit 1000, bolagsverket 900, wikidata 600, esef 400` with a `decided_at` from person slice 2. **Do not materialize `se_company_person_precedence_clickhouse`** — the export is idempotent-by-comparison, but there is no reason to touch a fold watermark here.

Record (a), (b) and (c): the AFTER steps subtract from these numbers, not from the spec's.

- [ ] **Step 3: Preview the extract**

The asset's default is a preview (`execute: false`): the same change scan, counted and not written.

```bash
cat > /tmp/ratsit-person-preview.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_person_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_company_person_suggestions_ratsit":{"config":{"execute":false,"page_size":10000}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-person-preview.json | python3 -m json.tool
```

Expected: `"__typename": "LaunchRunSuccess"` with a `runId`. Poll it (one-shot polls every 60 s, never a long-lived local loop — memory `se-person-entity`: local pollers get OOM-killed during long runs):

```bash
cat > /tmp/run.json <<'JSON'
{"query":"query Run($runId: ID!) { runOrError(runId: $runId) { __typename ... on Run { runId status startTime endTime } } }","variables":{"runId":"REPLACE_WITH_RUN_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/run.json | python3 -m json.tool
```

Then read the materialization metadata:

```bash
cat > /tmp/person-ratsit-mat.json <<'JSON'
{"query":"query Mat($assetKeys: [AssetKeyInput!]!, $limit: Int!) { assetNodes(assetKeys: $assetKeys) { id assetMaterializations(limit: $limit) { runId timestamp metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on BoolMetadataEntry { boolValue } } } } }","variables":{"assetKeys":[{"path":["se_company_person_suggestions_ratsit"]}],"limit":1}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/person-ratsit-mat.json | python3 -m json.tool
```

Expected: `execute` false, `companies` ≈ **236,800** (Step 2(c)'s company count — a company whose Ratsit rows are all nameless appears on NEITHER side of the state hash and is never visited), `pages` **24** (ceiling of companies / 10,000), `candidates` ≈ **277,600** (Step 2(c)'s live rows; every company is new to this source, so there are no tombstones yet), `inserted` **0**, `stopped_at_cap` false. **If `candidates` is under 200,000 or over 400,000, stop and reconcile against Step 2(c) before writing anything.**

- [ ] **Step 4: Execute the extract**

The same launch with the gate open — only `"execute": true` differs.

```bash
cat > /tmp/ratsit-person-execute.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_person_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_company_person_suggestions_ratsit":{"config":{"execute":true,"page_size":10000}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-person-execute.json | python3 -m json.tool
```

Poll with `/tmp/run.json` until `SUCCESS`, then re-read the metadata. Expected: `execute` true, `companies` and `candidates` as in the preview, `inserted` **equal to `candidates`**, `stopped_at_cap` false (the cap is 5,000,000 companies). Record the wall time — the basic-info Ratsit run wrote 863,504 rows over 87 pages in 4 minutes, and this one writes a third of that through a heavier select, so single-digit minutes is unremarkable and half an hour is not alarming.

- [ ] **Step 5: Read out the suggestions and prove convergence**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- the shape of what landed
SELECT
    count()                                            AS rows,
    uniqExact(company_id)                              AS companies,
    countIf(full_name IS NOT NULL)                     AS live_rows,
    countIf(full_name IS NULL)                         AS tombstones,
    countIf(birth_year IS NOT NULL)                    AS with_birth_year,
    countIf(role_key IS NOT NULL)                      AS with_role_key,
    countIf(startsWith(slot, 'idx:'))                  AS index_slots,
    countIf(position(slot, ':') > 0 AND NOT startsWith(slot, 'idx:')) AS role_qualified_slots
FROM corpscout.se_company_person_suggestion FINAL
WHERE source = 'ratsit';

-- the role labels, against spec 4.3's table
SELECT role_original, count() AS rows
FROM corpscout.se_company_person_suggestion FINAL
WHERE source = 'ratsit' AND full_name IS NOT NULL
GROUP BY role_original ORDER BY rows DESC;

-- the data keys really are absent when the source value is missing
SELECT
    countIf(JSONHas(data, 'age'))                AS with_age,
    countIf(JSONHas(data, 'profile_url'))        AS with_profile_url,
    countIf(JSONHas(data, 'ratsit_person_id'))   AS with_person_id,
    countIf(JSONExtractString(data, 'external') = 'true') AS external_rows,
    countIf(JSONType(data) != 'Object')          AS malformed
FROM corpscout.se_company_person_suggestion FINAL
WHERE source = 'ratsit' AND full_name IS NOT NULL;
SQL
```

Expected: `rows = live_rows` ≈ 277,600 and `tombstones` **0** (nothing was stored before this run); `with_birth_year` ≈ 277,592 minus the rows whose URL carries no date; `with_role_key` **0** (Ratsit has no machine code); `index_slots` small (named rows without a URL); `role_qualified_slots` ≈ 62 (the 31 two-row tokens contribute two slots each); the role labels are spec 4.3's eight plus `Aktuarie` — the nine values section 2 counted, and nothing else; `malformed` **0**; `external_rows` ≈ 69,000 (the `Extern *` labels of section 2).

Then re-run the **preview** of Step 3. Expected: `companies` **0** — the state hash converges in one pass. A non-zero count means the two sides of the hash disagree about what is live; find out which before normalizing.

- [ ] **Step 6: Normalize**

```bash
cat > /tmp/person-normalize.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_person_normalize"]}]},"runConfigData":{"ops":{"se_company_person_normalize":{"config":{"changed_only":true}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/person-normalize.json | python3 -m json.tool
```

Poll with `/tmp/run.json`; read the metadata with `/tmp/person-ratsit-mat.json` after swapping the asset key to `se_company_person_normalize`.

Expected metadata (`NormalizeCounts.as_metadata`): `companies` ≈ 236,800, `rows` ≈ 277,600, `ok` the large majority, `partial` and `no_person` small, `normalizer_version` the current `normalize_se.py` version. `changed_only: true` means only rows never normalized or on a new `suggestion_id` are touched — the other three sources' 5.5M rows are not re-normalized; if `rows` comes back in the millions, the flag did not bite: stop.

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT source, parse_status, count() AS rows
FROM corpscout.se_company_person_normalized FINAL
WHERE source = 'ratsit' GROUP BY source, parse_status ORDER BY rows DESC;

-- the role map of Task 2, as the normalizer applied it
SELECT ifNull(role_code, '(none)') AS role_code, count() AS rows
FROM corpscout.se_company_person_normalized FINAL
WHERE source = 'ratsit' GROUP BY role_code ORDER BY rows DESC LIMIT 20;
SQL
```

Expected: `ok` dominant; the role codes are the five catalog codes of spec 4.3 plus `aktuarie` as itself (2 rows) — **no `vd`, `prokurist` written as a raw label where the map should have fired**, which would mean Task 2's map did not deploy.

- [ ] **Step 7: Back-fill the fold over all 64 buckets**

`se_company_person_fold` is `StaticPartitionsDefinition(["bucket_00" … "bucket_63"])` with `BackfillPolicy.multi_run(max_partitions_per_run=1)` and pool `FOLD_POOL = "se_company_person_fold"` (instance default limit 1), so the backfill produces one run per partition and the pool serializes them. A backfill carries no run config, which is what the defaults want: `changed_only: true` (the fold's watermark sees the newer normalized rows), `page_size: 20000`.

```bash
python3 - <<'PY' > /tmp/person-fold-backfill.json
import json
query = (
    "mutation LaunchBackfill($backfillParams: LaunchBackfillParams!) {"
    " launchPartitionBackfill(backfillParams: $backfillParams) { __typename"
    " ... on LaunchBackfillSuccess { backfillId }"
    " ... on PartitionSetNotFoundError { message }"
    " ... on PythonError { message } } }"
)
params = {
    "partitionNames": [f"bucket_{i:02d}" for i in range(64)],
    "assetSelection": [{"path": ["se_company_person_fold"]}],
    "fromFailure": False,
    "tags": [],
}
print(json.dumps({"query": query, "variables": {"backfillParams": params}}))
PY
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/person-fold-backfill.json | python3 -m json.tool
```

Expected: `LaunchBackfillSuccess` and a `backfillId`. Record it. Poll the runs by tag (one-shot per tick, every 2-5 minutes):

```bash
cat > /tmp/backfill-runs.json <<'JSON'
{"query":"query BackfillRuns($backfillId: String!) { runsOrError(filter: {tags: [{key: \"dagster/backfill\", value: $backfillId}]}) { __typename ... on Runs { results { runId status tags { key value } } } } }","variables":{"backfillId":"REPLACE_WITH_BACKFILL_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/backfill-runs.json \
  | python3 -c "
import collections, json, sys
runs = json.load(sys.stdin)['data']['runsOrError']['results']
print(len(runs), 'runs:', dict(collections.Counter(r['status'] for r in runs)))
for r in runs:
    if r['status'] not in ('SUCCESS', 'STARTED', 'STARTING', 'QUEUED'):
        print('  !', r['runId'], r['status'], {t['key']: t['value'] for t in r['tags'] if t['key'] == 'dagster/partition'})
"
```

**Read the first finished bucket's metadata before the rest complete** (the limit-1 pool gives a natural checkpoint) — `/tmp/person-ratsit-mat.json` with the asset key swapped to `se_company_person_fold`.

Expected per bucket (`FoldCounts.as_metadata` plus the asset's `bucket`, `changed_only`, `page_size`): `changed_only` true, `page_size` 20000, `considered` ≈ 236,800 / 64 ≈ **3,700**, `persons` the active rows the bucket wrote, `created` the new people, `updated` the existing people a Ratsit member joined, `withdrawn`/`hidden`/`reactivated` small, `stale_rules` 0, `sets_split_by_birth_year` a handful (a Ratsit birth year can split a set that was one person before). **If `considered` comes back near 9,000 (the whole bucket, as in person slice 2's first fold) the run is re-folding everything — stop, `changed_only` was not honoured.** `FoldCounts` has no `merged` field: a merge shows up as `updated` here and as a row whose `sources` gained an entry in Step 8.

Expect 64/64 `SUCCESS`. Person slice 2's first fold ran ~30 s per bucket over 578k companies behind this same pool (~30 min for all 64); this one folds fewer companies but writes more history rows, so budget 30-90 minutes and record the real total.

- [ ] **Step 8: Read out the entity (the spec 4.6 numbers)**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the same shape as Step 2(a): compare line by line
SELECT
    count()                                   AS rows,
    countIf(active = 1)                       AS active_rows,
    uniqExactIf(company_id, active = 1)       AS companies_with_a_person,
    countIf(active = 1 AND has(sources, 'ratsit'))       AS ratsit_rows,
    countIf(active = 1 AND length(sources) > 1)          AS multi_source_rows,
    countIf(active = 1 AND birth_year IS NOT NULL)       AS rows_with_a_birth_year
FROM corpscout.se_company_person FINAL;

-- (b) the source combinations
SELECT arrayStringConcat(arraySort(sources), '+') AS combo, count() AS rows
FROM corpscout.se_company_person FINAL
WHERE active = 1 GROUP BY combo ORDER BY rows DESC;

-- (c) which source spells the published name
SELECT text_source, count() AS rows
FROM corpscout.se_company_person FINAL
WHERE active = 1 GROUP BY text_source ORDER BY rows DESC;

-- (d) Ratsit companies that still have no person (expect ~0: the leftovers are companies
--     whose Ratsit rows are all nameless, which never became suggestions)
SELECT uniqExact(ratsit.company_id) AS ratsit_companies_without_a_person
FROM (
    SELECT DISTINCT company_id FROM corpscout.se_company_person_suggestion FINAL
    WHERE source = 'ratsit' AND full_name IS NOT NULL
) AS ratsit
LEFT ANTI JOIN (
    SELECT DISTINCT company_id FROM corpscout.se_company_person FINAL WHERE active = 1
) AS people ON people.company_id = ratsit.company_id;

-- (e) history written by this backfill
SELECT change_kind, count() AS rows
FROM corpscout.se_company_person_history
WHERE changed_at >= toDateTime64('REPLACE_WITH_BACKFILL_START', 3, 'UTC')
GROUP BY change_kind ORDER BY rows DESC;

-- (f) the spot check: ten people both Bolagsverket and Ratsit know
SELECT company_id, display_name, first_name, last_name, birth_year, sources,
       member_sources, member_names, text_source, role_codes, role_years
FROM corpscout.se_company_person FINAL
WHERE active = 1 AND has(sources, 'ratsit') AND has(sources, 'bolagsverket')
ORDER BY cityHash64(person_key) LIMIT 10;
SQL
```

Acceptance, against Step 2(a):

- `companies_with_a_person` grows by roughly **105,759** (spec section 2: the Ratsit companies with no person at all before).
- `ratsit_rows` goes 0 → a few hundred thousand; `multi_source_rows` grows by the Ratsit people the fold merged into an existing person.
- `rows_with_a_birth_year` grows sharply — Ratsit is the first source with a birth year for the general population (Wikidata's 504 rows aside).
- In (f) every row must show `text_source = 'ratsit'`: precedence 1000 beats Bolagsverket's 900, so the Ratsit spelling is published. A `bolagsverket` here means the precedence export or the fold read something else — stop and check Step 2(d). The `birth_year` must be filled and `member_sources` must list both sources for the same person.
- (e) is dominated by `created` and `updated`; a large `withdrawn` count would mean the fold retired people it should have kept — investigate before the serving refresh.

- [ ] **Step 9: Let the serving refresh land**

`corpscout.se_companies_serving` is a refreshable materialized view (hourly); `has_people` is `company_id IN (se_company_person FINAL WHERE active = 1)`, so it grows by the first-person companies. There are no per-source person flags for Ratsit — `people_bolagsverket` and `people_esef` are the only two, and neither changes (spec section 6).

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT view, status, last_success_time, next_refresh_time, exception
FROM system.view_refreshes WHERE view = 'se_companies_serving';

SELECT countIf(has_people) AS companies_with_people, count() AS companies
FROM corpscout.se_companies_serving;
SQL
```

Expected: `status` `Scheduled`, an `exception` that is empty, and `companies_with_people` up by about the same 105,759. Do **not** trigger the view by hand; if the refresh ran over a half-folded table it still succeeds and the next hour carries the rest (slice 1 saw exactly that).

- [ ] **Step 10: Smoke the backoffice on the owner's dev server**

The backoffice is not deployed; the owner runs `pnpm dev` from the **main checkout** at `http://localhost:5183` (memory `backoffice-runs-locally`). After the merge, that checkout carries Task 4.

```bash
curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/people?source=ratsit'
curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/company/REPLACE_WITH_SPOT_CHECK_COMPANY/people'
```

Expected `200` twice. Then look at both pages in a browser:

- `/admin/se/people` — the Source select now offers **Ratsit** (five options: Bolagsverket, ESEF, Wikidata, Ratsit, Reviewer) and picking it returns rows.
- `/admin/se/company/<id>/people` for the Step 8(f) company — the person shows the Ratsit spelling, a birth year, both sources, and the `data` block renders the Ratsit keys (`age`, `identity_available`, `profile_url`, `display_name_raw`, `ratsit_person_id`, `external`) as JSON. The tab renders `data` generically, so nothing should look broken; if a key renders as `null`, `mapFilter` did not do its job.

- [ ] **Step 11: Record the shipped slice and tick the plan**

1. Append a Shipped record under spec section 8 item 2, in the house style of item 1 — the plan file, the merge commit, what shipped, and the prod numbers from Steps 2, 4, 5, 6, 7, 8 and 9: the extract's `companies`/`candidates`/`inserted` and wall time, the convergence preview, the suggestion readout (live rows, birth years, slot kinds, role labels), the normalize counts and role codes, the backfill id with its wall time and summed metadata, the entity before → after for every line of Step 2(a), the companies gaining a first person, the `text_source` split, `has_people` before → after, and any ruling made on the way. Section 8 item 2 reads:

   ```
   2. The Ratsit person extractor, roles, wording, backoffice filter; prod extract, normalize,
      fold.
   ```

   and the record goes directly under it, beginning ``Shipped 2026-09-11 (plan `2026-09-11-se-ratsit-2-people.md`, main <merge commit>): …``.

2. Tick every `- [ ]` in this plan that was done.

3. Commit both files, by explicit path, from the main checkout (the branch is merged by now):

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
MSGFILE="$(git rev-parse --git-dir)/RATSIT_SLICE2_SHIPPED_MSG"
cat > "$MSGFILE" <<'EOF'
docs(spec): record Ratsit slice 2 on prod; plan ticked

The Ratsit person extractor ran over every company with a current report, the
normalize asset parsed its rows and the fold backfilled all 64 buckets. Numbers
in spec section 8 item 2.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md \
  corpscout/services/dagster_v3/docs/superpowers/plans/2026-09-11-se-ratsit-2-people.md
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

4. Update the project memory (`se-ratsit-source.md`, or `se-person-entity.md`): slice 2 is live with its after-numbers, ratsit is now a real person source with the top machine spelling precedence, and slice 3 (the addresses: the postal-town dictionary and the establishments as `workplace`) is next and unstarted.

5. Clean up the deploy worktree if Step 1 created one: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect worktree remove /tmp/deploy-worktree`.

---

## Self-review

**1. Spec coverage (section 4, plus the section 2 / 6 / 7 material it rests on)**

| spec | where |
| --- | --- |
| 4.1 module `person/ratsit.py` on the shape of `bolagsverket.py` / `wikidata.py` | Task 1 Step 4 — the full file |
| 4.1 `PERSON_SOURCE = "ratsit"`, `RATSIT_PERSON_EXTRACTOR_VERSION = "ratsit-person-v1"` | Task 1 Step 4; pinned in Task 1 Step 2 |
| 4.1 `RATSIT_COLUMN_SQL` covering the 16 `PERSON_SELECT_COLUMNS` | Task 1 Step 4; `live_select_sql` raises at import on a missing or extra key, and `test_every_extractor_maps_all_sixteen_columns_and_names_its_source_once` loops over the `EXTRACTORS` entry added in Step 2 |
| 4.1 `ratsit_live_sql(scoped)`, `ratsit_changed_scope_sql()`, `ratsit_select_sql()` (live UNION ALL per-slot tombstones through `suggestions.person_select_sql`), `ratsit_current_sql()` | Task 1 Step 4; the shape is proven by the file's generic tests plus `test_the_ratsit_current_sql_is_the_reports_own_stamp` |
| 4.1 asset `se_company_person_suggestions_ratsit` with the two table-named Ratsit deps, never `se_ratsit_normalized` | Task 1 Step 4 (with the comment saying why) and `test_the_ratsit_asset_reads_the_ratsit_tables_and_the_basic_info_fold` |
| 4.1 "on the shape of `bolagsverket.py` / `esef.py` / `wikidata.py`" — the `se_company_basic_info` universe and the `se_company_basic_info_fold` dep the siblings carry (controller ruling 2026-09-11) | `UNIVERSE_JOIN_SQL` in the `report` CTE and in `ratsit_current_sql` (Task 1 Step 4), pinned in Task 1 Step 2, proved by `test_a_ratsit_company_outside_the_basic_info_universe_is_never_written` (Task 3) |
| 4.1 the `normalizer_version` select parameter | `RATSIT_SELECT_PARAMS`, passed as `select_params`; pinned in Task 1 Step 2 and bound in Task 3's `_scope`/`_insert` |
| 4.1 `EXTRACTOR_SOURCES` becomes the four-tuple, carrying the asset into the job, the weekly's run config and the normalize deps | Task 2 Step 4; the pins in `test_se_company_person_extractors_sql.py` and the three `test_se_company_person_jobs.py` assertions |
| 4.1 the shared state-hash scan and per-slot tombstones are untouched | Global Constraints; Task 1 touches only `live`; Task 3 proves convergence and the tombstone on a real engine |
| 4.2 current report: `FINAL`, `normalizer_version`, newest `normalized_at`, ties by `result_sha256`, `LIMIT 1 BY company_id`, people joined on the report key | Task 1 Step 4 `ratsit_report_cte_sql`; pinned in Step 2; `test_only_the_newest_ratsit_report_reaches_the_suggestion_table` (Task 3) |
| 4.2 nameless rows skipped | `where_sql="WHERE trim(r.name_raw) != ''"`; the fixture's `person_index 2` row |
| 4.2 slot: token / `token:role` / `idx:<person_index>` | the `multiIf`; the three slots asserted in Task 3 |
| 4.2 `source_record_id`, `full_name` trimmed with `first_name`/`last_name` NULL | Task 1 Steps 2 and 4; Task 3's row assertions |
| 4.2 `birth_year` from the URL's date, NULL without one | the `toUInt16OrNull(substring(extract(...)))`; `1980` and `\N` in Task 3 |
| 4.2 `wikidata_id`, `role_from`, `role_to`, `document_ref` NULL; `role_key` NULL with the label mapped by `roles.py` | `NULL_SQL` entries, pinned in Task 1 Step 2 and read out on prod in Task 5 Step 5 (`with_role_key` 0) |
| 4.2 `role_original` = the delivered Swedish label, trimmed | `nullIf(trim(r.role_raw), '')` |
| 4.2 `fiscal_year` from `source_date_modified`, else `normalized_at` | the `toYear(ifNull(...))`; Task 3 asserts both cases (company A has a modified date, company B does not) |
| 4.2 `data` via `mapFilter` over coalesced String values so a NULL key is absent, with the six keys and the `external` flag | Task 1 Step 4; Task 3's `RATSIT_DATA_VD` / `RATSIT_DATA_BO`; Task 5 Step 5's `JSONHas` readout |
| 4.2 the slot keeps a person stable across scans | the module docstring's reasoning; `test_a_ratsit_slot_survives_a_rescan_and_a_dropped_person_is_tombstoned` |
| 4.3 the eight labels → five catalog codes, `aktuarie` passthrough, no `SOURCE_ROLELESS_CODES` entry, the `roles.py` sentence updated | Task 2 Steps 1, 3 and the new `tests/test_se_company_person_roles.py` |
| 4.4 `precedence.py` docstring, `person-design.md` line 19 + the module table + the extractor table + the `jobs.py` row, the precedence test wording | Task 2 Step 6 (a), (b), (d) |
| 4.4 backoffice `MAIN_PERSON_SOURCES`, its comment and `tests/se-person-fields.test.ts` | Task 4 Steps 2 and 4 |
| 4.5 `EXTRACTORS` entry + the pinned slot/nameless/birth-year/fiscal-year/data test | Task 1 Step 2 |
| 4.5 the clickhouse-local test: the `se_ratsit_responsible_people` CREATE in the fixture file (not `WANTED_CREATES`), two companies, the four row shapes, the second run's tombstone | Task 3 Steps 1 and 2 |
| 4.5 `tests/test_se_company_person_roles.py` (new) | Task 2 Step 1 |
| 4.5 "unit count pins update by construction" | Task 2 Step 5 runs `test_se_company_person_jobs.py`, whose three assertions read `EXTRACTOR_ASSET_NAMES` |
| 4.6 deploy; extract `execute: true, page_size: 10000`; normalize `changed_only: true`; fold over 64 buckets; the readouts; the People tab | Task 5 Steps 1, 3-4, 6, 7, 8, 10 |
| 4.6 "the serving refresh follows (`has_people` grows)" | Task 5 Step 9 |
| 2 — 236,814 companies / 301,081 joined rows / 23,432 nameless / 31 two-row tokens / 105,759 companies with no person | the expected values of Task 5 Steps 2, 3, 5 and 8 |
| 6 — a re-scan adds a role year to the same slot | the `fiscal_year` comment in Task 1 Step 4 |
| 6 — weeklies stay STOPPED | Global Constraints; Task 5 Step 2 checks all three |
| 6 — the serving view has only `has_people`, no per-source flags change | Task 5 Step 9 |
| 6 — 128-bucket normalize vs the unpartitioned extractor is a non-issue | nothing to do: the asset is unpartitioned and simply deps on the two partitioned keys, exactly as `basic_info/ratsit.py` already does |
| 7 — names: `se_company/person/ratsit.py`, asset `se_company_person_suggestions_ratsit`, version `ratsit-person-v1`, slot = the profile token | used throughout; `PERSON_PRECEDENCE` untouched |

Out of scope by the spec itself and therefore absent: the Ratsit addresses and establishments (slice 3), the description precedence question (section 6), scheduling the Ratsit scan, and Ratsit's financials / industry codes / summaries / legal form.

**2. Placeholder scan**

No `TBD`, `TODO`, "implement later", "add appropriate error handling", "similar to Task N" or bare "write tests for the above". Every edit shows the text before and after; every command runs as written; every test body is complete code. The only fill-ins are `REPLACE_WITH_RUN_ID`, `REPLACE_WITH_BACKFILL_ID`, `REPLACE_WITH_BACKFILL_START` and `REPLACE_WITH_SPOT_CHECK_COMPANY` in Task 5's poll and readout payloads — values prod hands back at run time and which cannot be known in advance.

**3. Name consistency**

Checked against the code on this branch, 2026-09-11. Helpers: `live_select_sql(columns, from_sql, where_sql, with_sql)`, `person_changed_scope_sql(source, live_sql)`, `person_select_sql(source, live_sql)`, `define_person_suggestion_asset(**kwargs)` forwarding to `define_suggestion_asset(source, extractor_version, current_sql, select_sql, select_params, deps, description, target, changed_scope_override)`, `NULL_SQL`, `LIVE_ROW_PREDICATE`, `PERSON_SELECT_COLUMNS`, `PERSON_TARGET`, `PERSON_STATE_COLUMNS`. Config classes: `ExtractConfig(execute, company_ids, max_companies, since, page_size ≤ 20_000)`, `PersonNormalizeConfig(changed_only, company_ids, page_size)`, `PersonFoldConfig(changed_only, page_size)`. Metadata labels: `ExtractCounts` → `companies, pages, candidates, inserted, execute, stopped_at_cap`; `NormalizeCounts` → `companies, pages, rows, ok, partial, no_person, normalizer_version`; `FoldCounts` → `companies, considered, pages, persons, created, updated, hidden, withdrawn, reactivated, unchanged, stale_rules, sets_split_by_birth_year, fold_version` (plus the asset's `bucket`, `changed_only`, `page_size`). Tables: `corpscout.se_company_person_suggestion`, `…_normalized`, `corpscout.se_company_person`, `…_history`, `…_rule`, `…_precedence`, `corpscout.se_ratsit_company`, `corpscout.se_ratsit_responsible_people`, `corpscout.se_company_basic_info` (the universe, migration 000377 — the same text `bolagsverket.py::UNIVERSE_JOIN_SQL` uses, retargeted to the report's alias `c`), `corpscout.se_companies_serving`. Asset key `se_company_basic_info_fold` is the one `bolagsverket.py` and `wikidata.py` already declare. Ratsit source columns used: `company_id, result_sha256, normalizer_version, person_index, display_name, display_name_raw, name, age, identity_available, role, profile_url, normalized_at` and the report's `normalized_at, source_date_modified` (000343 + 000346). Role codes: `chief_executive_officer`, `deputy_chief_executive_officer` (000290), `legal_representative`, `other_representative`, `procurist` (000319) — all five exist in `corpscout.company_person_role_type`. Backoffice symbols: `PERSON_SOURCES`, `DRAFT_SOURCE`, `MAIN_PERSON_SOURCES`, `SePersonSource`, `SOURCE_LABELS`, `personSourceLabel`. Assets: `se_company_person_suggestions_{bolagsverket,esef,wikidata,ratsit}`, `se_company_person_normalize`, `se_company_person_fold`, `se_company_person_fold_companies`, `se_company_person_precedence_clickhouse`; job `se_company_person_extract_job`; schedule `se_company_person_weekly` (`25 7 * * 1`, STOPPED).

The Ratsit SQL in Task 1 was executed against `clickhouse/clickhouse-server:26.5` while writing this plan, with the Task 3 fixture rows, under `join_use_nulls` 0 and 1 — and re-executed after the universe join was added: identical output both ways (the two transcripts differ only in their `suggested_at` stamps), the out-of-universe company produces `0` rows and never appears in the scope, and every other expected value quoted in Task 3 is unchanged from the pre-join run. Every string in Task 3 is that run's actual output.

**Choices made where the spec left room, or where it and the code disagreed**

- **The universe — settled by the controller, 2026-09-11.** Spec 4.1 lists only the two Ratsit deps, which read as "no universe join"; the ruling is that the 2026-09-08 lineage ruling binds basic-info's OWN extractors, while the person extractors' universe IS the basic-info entity by design (`docs/person-design.md:122` stays true and is **not** amended). So `ratsit.py` follows its three siblings exactly: `UNIVERSE_JOIN_SQL` in the `report` CTE (and therefore in the live SQL, the page select and the changed scope) and in `ratsit_current_sql`, plus `dg.AssetKey("se_company_basic_info_fold")` in the deps. A Ratsit company the entity does not know yields no live row, so it is on neither side of the state hash and is never visited — asserted on a real engine in Task 3.
- **`trim`/`extract` over Nullable columns.** Spec 4.2 writes the expressions against bare column names (`extract(profile_url, …)`, `trim(role)`), but `name`, `role`, `profile_url` and `display_name_raw` are all `Nullable(String)` and every one of those functions returns NULL on NULL — which would make `token = ''` and `trim(name) != ''` un-evaluable for exactly the GDPR-limited rows the filter exists for. Chosen: the `people` CTE coalesces them once (`ifNull(p.name, '') AS name_raw`, …) and every expression reads the coalesced column. Same semantics, one place.
- **`external` is case-insensitive.** Spec 4.2 says "`'true'` when the role starts with `Extern`". Implemented as `startsWith(lowerUTF8(trim(role_raw)), 'extern')`, so a future `EXTERN VD` or `extern vd` is still flagged; no label in today's data changes meaning.
- **The duplicate-token count.** Spec 4.2 says "when the same token appears on more than one row of the report". Implemented as a window count over the **named** rows (ClickHouse evaluates windows after `WHERE`), not over every row, because a nameless row has no URL and therefore no token at all — the two readings cannot differ on real data, and the window avoids a second join that `join_use_nulls` could colour.
- **`fiscal_year` is one expression.** Spec 4.2 phrases it as two (`toYear(source_date_modified)` … "else `toYear(normalized_at)`"); the module writes `toYear(ifNull(r.source_date_modified, toDate32(r.normalized_at)))`, which is the same value and cannot disagree between the scope and the page.
- **One pin the spec does not name.** `tests/test_se_company_common.py:466` asserts `set(SOURCE_ROLE_MAPPINGS) == {"bolagsverket", "esef", "wikidata"}` and breaks the moment the Ratsit map lands. Task 2 Step 1 updates it — found by `rg`, not by the spec.
- **Two wording edits the spec does not list.** `tests/test_se_company_person_jobs.py`'s module docstring and one test name say "the three extractors"; both become "every extractor" (assertions unchanged). Skipping them would leave a test whose name contradicts what it asserts.
- **The `.env` gotcha.** Any test that loads the definitions tree fails on `main` today without the gitignored `.env` exported (`WebtechScannerComponent api_url`). The plan records the baseline and the one-line fix rather than letting an implementer read it as a regression of this slice.
- **The fold has no `merged` count.** Spec 4.6 asks for "created/updated/merged counts from the run metadata"; `FoldCounts` carries `created`/`updated`/`hidden`/`withdrawn`/`reactivated`/`unchanged`/`stale_rules`/`sets_split_by_birth_year` and no merge counter. Task 5 Step 7 says so, and Step 8(b) reads the merges out of the entity instead (rows whose `sources` list more than one source).
- **Preview before execute.** Spec 4.6 says "`se_company_person_suggestions_ratsit` `execute: true`"; the asset's default is a preview and the change scan is the expensive half either way, so Task 5 previews first with an explicit stop rule on the candidate count, then executes.
