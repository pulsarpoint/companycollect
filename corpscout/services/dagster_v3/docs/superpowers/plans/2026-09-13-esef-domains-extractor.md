# ESEF `esef_domains` extractor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The first independent ESEF extractor: `esef_domains_clickhouse` re-extracts registrable domains from every archived ESEF package into `corpscout.esef_domains` (own version, own table, own sensor), and the serving/backoffice consumers switch to it.

**Architecture:** Documents (`esef_filings` index + archived packages in S3) plus one extractor asset per product. The domains extractor selects stale documents in ClickHouse, streams packages from the object store into a process pool whose workers run the pure per-document extraction in a killable spawned child, and replaces the table's rows batch by batch. A sensor launches it on the server whenever stale documents exist. Three light modules (timeout wrapper, package opener, per-document extraction) keep the child's import cost small.

**Tech Stack:** Python 3.14, Dagster (`dg`), clickhouse-driver via `ClickhouseResource`, boto3 via `ObjectStoreResource`, lxml + tldextract (existing `website_candidates`), dbt (company_serving), TypeScript/React Router (backoffice), golang-migrate ledger.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-13-esef-extractors-and-domains-design.md`

## Global Constraints

- Repo root: `/Users/graovic/pulsarpoint/ppoint/companycollect`. dagster_v3 module: `corpscout/services/dagster_v3` (all `uv run` commands run from there). Branch: `esef-domains-extractor` off `main`.
- **Commit by explicit path only** (`git add <paths>`; never `git add -A`/`-u`). Never stage `corpscout/services/backoffice/app/lib/technology-proposals.server.ts` or anything under `codex-sd-examples/` (unrelated WIP in the working tree). Every commit message ends with:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```
- No `from __future__ import annotations` in any module defining `@dg.asset`/`@dg.sensor` (Dagster inspects annotations directly).
- The migration owns the schema: Python never creates `esef_domains`; the asset asserts the table exists (`assert_clickhouse_tables_exist`) and replaces rows via stage table + `EXCHANGE TABLES`.
- `ESEF_DOMAINS_EXTRACTOR_VERSION = "esef-domains-v1"` — the only version constant of this extractor.
- Light modules (`defs/common/child_timeout.py`, `defs/esef_filings/report_package.py`, `defs/esef_filings/domains_extraction.py`) must not import `dagster`, `arelle` or `dagster_v3.defs.esef_filings.segment_parser`/`segment_assets` (a subprocess test pins this).
- The artifact parser's output does not change: `EsefWebsiteCandidate` keeps its five fields; `segment_parser.py` keeps the names it imports today (`_extract_report_package`, `_contains_inline_xbrl_namespace`, `MAX_PACKAGE_*`).
- Migration number `000405_corpscout_esef_domains` — before merge, re-check `ls corpscout/clickhouse/migrations | tail -3` and the prod ledger; renumber if another 000405 landed first (000404 went to the SE financial track, applied on prod 2026-09-13).
- Tests: `uv run pytest <files> -q`. Definitions check: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs`. Lint: `uv run ruff check <files>` and `uv run ruff format <files>`.
- Owner-run steps (migration apply, deploy, prod launches) are NOT part of any task: the plan ends with a rollout checklist the owner executes.

---

### Task 1: The child-timeout wrapper and the package opener

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/common/child_timeout.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/report_package.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_parser.py` (lines 57-63 constants/markers, 590-665 `_extract_report_package`, 735-744 `_contains_inline_xbrl_namespace`)
- Test: `corpscout/services/dagster_v3/tests/test_child_timeout.py`
- Test: `corpscout/services/dagster_v3/tests/test_esef_report_package.py`

**Interfaces:**
- Produces: `run_in_child_with_timeout(fn, args, *, timeout_seconds, terminate_grace_seconds=5.0) -> Any`, `ChildTimeoutError(timeout_seconds)`, `ChildFailedError(detail)`; `extract_report_package(package_path: Path, temp_dir: Path) -> tuple[list[str], int]`, `contains_inline_xbrl_namespace(report_path: Path) -> bool`, `report_paths_for(temp_dir: Path, report_members: Iterable[str]) -> dict[str, Path]`, constants `MAX_PACKAGE_FILES`, `MAX_PACKAGE_UNCOMPRESSED_BYTES`, `MAX_PACKAGE_MEMBER_BYTES`, `INLINE_XBRL_NAMESPACE_MARKERS`.

- [ ] **Step 1: Write the failing tests for the timeout wrapper**

`tests/test_child_timeout.py`:

```python
"""The spawned-child timeout wrapper shared by the per-document extractors.

The helper functions below are module-level on purpose: the child is a
``spawn`` process and unpickles the target by import path.
"""

import os
import time

import pytest

from dagster_v3.defs.common.child_timeout import (
    ChildFailedError,
    ChildTimeoutError,
    run_in_child_with_timeout,
)


def _double(value: int) -> int:
    return value * 2


def _sleep_forever() -> None:
    while True:
        time.sleep(1)


def _raise_value_error() -> None:
    raise ValueError("boom")


def _exit_hard() -> None:
    os._exit(3)


def test_returns_the_child_result() -> None:
    assert run_in_child_with_timeout(_double, (21,), timeout_seconds=60) == 42


def test_kills_a_child_that_outlives_its_budget() -> None:
    started = time.monotonic()
    with pytest.raises(ChildTimeoutError) as excinfo:
        run_in_child_with_timeout(_sleep_forever, (), timeout_seconds=1)
    assert excinfo.value.timeout_seconds == 1
    assert time.monotonic() - started < 30


def test_reports_an_exception_raised_in_the_child() -> None:
    with pytest.raises(ChildFailedError) as excinfo:
        run_in_child_with_timeout(_raise_value_error, (), timeout_seconds=60)
    assert "ValueError: boom" in excinfo.value.detail


def test_reports_a_child_that_died_without_a_result() -> None:
    with pytest.raises(ChildFailedError) as excinfo:
        run_in_child_with_timeout(_exit_hard, (), timeout_seconds=60)
    assert "code 3" in excinfo.value.detail


def test_errors_survive_pickling() -> None:
    import pickle

    timeout = pickle.loads(pickle.dumps(ChildTimeoutError(7)))
    failed = pickle.loads(pickle.dumps(ChildFailedError("x")))
    assert timeout.timeout_seconds == 7
    assert failed.detail == "x"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_child_timeout.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.common.child_timeout'`

- [ ] **Step 3: Write `child_timeout.py`**

```python
"""Run a picklable function in a killable child process bounded by a timeout.

Generalised from ``esef_filings/segment_assets._parse_document_package_worker``
(the ESEF artifact parser's per-document guard, 2026-09-12): a child that
outlives its budget is terminated, then killed if it ignores SIGTERM, so one
pathological input can never wedge a worker. Always a ``spawn`` context so the
child is independently killable even from inside a ProcessPoolExecutor worker
(nested spawn is fine). ``fn`` and ``args`` must be picklable; keep ``fn`` in a
light module -- every call pays that module's import cost in the child.
"""

from collections.abc import Callable
from multiprocessing import get_context
from multiprocessing.connection import Connection
from typing import Any

DEFAULT_TERMINATE_GRACE_SECONDS = 5.0


class ChildTimeoutError(Exception):
    """The child exceeded its wall-clock budget and was killed."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"child exceeded its {timeout_seconds}s budget and was killed"
        )

    def __reduce__(self) -> tuple[Any, tuple[float]]:
        # The default Exception.__reduce__ replays the formatted message into
        # __init__, whose signature is (timeout_seconds,) -- reconstruct from
        # the real field so the error survives a ProcessPoolExecutor result
        # queue (see segment_assets.DocumentParseTimeoutError).
        return (type(self), (self.timeout_seconds,))


class ChildFailedError(Exception):
    """The child raised, or exited without sending a result."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"child failed: {detail}")

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return (type(self), (self.detail,))


def _child_main(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    connection: Connection,
) -> None:
    try:
        result = fn(*args)
    except Exception as exc:  # noqa: BLE001 - reported to the parent, not swallowed
        connection.send(("error", f"{type(exc).__name__}: {exc}"))
    else:
        connection.send(("ok", result))
    finally:
        connection.close()


def run_in_child_with_timeout(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    *,
    timeout_seconds: float,
    terminate_grace_seconds: float = DEFAULT_TERMINATE_GRACE_SECONDS,
) -> Any:
    """Return ``fn(*args)`` computed in a spawned child, or raise.

    ``ChildTimeoutError`` after ``timeout_seconds`` (the child is terminated,
    then killed after ``terminate_grace_seconds``); ``ChildFailedError`` when
    the child raised or died without a result (os._exit, OOM-kill, segfault).
    """
    ctx = get_context("spawn")
    parent_connection, child_connection = ctx.Pipe(duplex=False)
    child = ctx.Process(target=_child_main, args=(fn, args, child_connection))
    child.start()
    child_connection.close()
    child.join(timeout=timeout_seconds)

    if child.is_alive():
        child.terminate()
        child.join(timeout=terminate_grace_seconds)
        if child.is_alive():
            child.kill()
            child.join()
        parent_connection.close()
        raise ChildTimeoutError(timeout_seconds)

    message: tuple[Any, ...] | None
    if parent_connection.poll():
        try:
            message = parent_connection.recv()
        except EOFError:
            # The child died without calling send(): the pipe only looks
            # ready because the read end hit EOF.
            message = None
    else:
        message = None
    parent_connection.close()

    if message is None:
        raise ChildFailedError(
            f"child exited with code {child.exitcode} without a result"
        )
    if message[0] == "error":
        raise ChildFailedError(str(message[1]))
    return message[1]
```

- [ ] **Step 4: Run the timeout tests to verify they pass**

Run: `uv run pytest tests/test_child_timeout.py -q`
Expected: 5 passed (the timeout test takes ~1-6 s).

- [ ] **Step 5: Write the failing tests for the package opener**

`tests/test_esef_report_package.py`:

```python
"""Opening an archived ESEF report package without arelle: member safety
limits and report-member selection, moved out of segment_parser."""

import zipfile
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import segment_parser
from dagster_v3.defs.esef_filings.report_package import (
    INLINE_XBRL_NAMESPACE_MARKERS,
    contains_inline_xbrl_namespace,
    extract_report_package,
    report_paths_for,
)

_IX_REPORT = (
    '<html xmlns="http://www.w3.org/1999/xhtml" '
    'xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body><p>x</p></body></html>'
)
_PLAIN_HTML = '<html xmlns="http://www.w3.org/1999/xhtml"><body><p>x</p></body></html>'


def _package(path: Path, members: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as package:
        for name, text in members.items():
            package.writestr(name, text)
    return path


def test_selects_report_members_under_a_reports_folder(tmp_path: Path) -> None:
    package = _package(
        tmp_path / "p.zip",
        {
            "pkg/META-INF/taxonomyPackage.xml": "<x/>",
            "pkg/reports/b.xhtml": _IX_REPORT,
            "pkg/reports/a.html": _IX_REPORT,
            "pkg/reports/styles.css": "p{}",
        },
    )
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, repaired = extract_report_package(package, temp_dir)

    assert members == ["pkg/reports/a.html", "pkg/reports/b.xhtml"]
    assert repaired == 0
    paths = report_paths_for(temp_dir, members)
    assert paths["pkg/reports/b.xhtml"] == temp_dir / "pkg" / "reports" / "b.xhtml"
    assert paths["pkg/reports/b.xhtml"].read_text() == _IX_REPORT


def test_falls_back_to_nonstandard_members_with_the_inline_xbrl_namespace(
    tmp_path: Path,
) -> None:
    package = _package(
        tmp_path / "p.zip",
        {"report.xhtml": _IX_REPORT, "readme.html": _PLAIN_HTML},
    )
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, _ = extract_report_package(package, temp_dir)

    assert members == ["report.xhtml"]


def test_repairs_backslash_member_names(tmp_path: Path) -> None:
    package = _package(tmp_path / "p.zip", {"pkg\\reports\\a.xhtml": _IX_REPORT})
    temp_dir = tmp_path / "out"
    temp_dir.mkdir()

    members, repaired = extract_report_package(package, temp_dir)

    assert members == ["pkg/reports/a.xhtml"]
    assert repaired == 1


def test_rejects_unsafe_paths_and_empty_packages(tmp_path: Path) -> None:
    unsafe = _package(tmp_path / "unsafe.zip", {"../evil.xhtml": _IX_REPORT})
    empty = _package(tmp_path / "empty.zip", {"pkg/META-INF/x.xml": "<x/>"})
    for package in (unsafe, empty):
        temp_dir = tmp_path / f"out-{package.stem}"
        temp_dir.mkdir()
        with pytest.raises(ValueError):
            extract_report_package(package, temp_dir)


def test_namespace_marker_scan(tmp_path: Path) -> None:
    with_marker = tmp_path / "a.xhtml"
    with_marker.write_text(_IX_REPORT)
    without = tmp_path / "b.xhtml"
    without.write_text(_PLAIN_HTML)
    assert contains_inline_xbrl_namespace(with_marker)
    assert not contains_inline_xbrl_namespace(without)
    assert b"http://www.xbrl.org/2013/inlineXBRL" in INLINE_XBRL_NAMESPACE_MARKERS


def test_segment_parser_keeps_its_names() -> None:
    assert segment_parser._extract_report_package is extract_report_package
    assert segment_parser._contains_inline_xbrl_namespace is contains_inline_xbrl_namespace
    assert segment_parser.MAX_PACKAGE_FILES == 10_000
```

- [ ] **Step 6: Run them to verify they fail**

Run: `uv run pytest tests/test_esef_report_package.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.esef_filings.report_package'`

- [ ] **Step 7: Create `report_package.py` and move the code**

Move, verbatim, from `segment_parser.py`: the three `MAX_PACKAGE_*` constants (lines 57-59), `_INLINE_XBRL_NAMESPACE_MARKERS` (60-63), the whole `_extract_report_package` function (590-665) and `_contains_inline_xbrl_namespace` (735-744) into the new module under public names. The module header and the one new helper:

```python
"""Opening an archived ESEF report package: the zip's safety limits and the
selection of its report XHTML members.

Split out of ``segment_parser.py`` (which keeps importing these names, so the
arelle parser's behaviour is unchanged) so that light per-document extractors
-- ``domains_extraction.py`` first -- can open a package without importing
arelle or the artifact parser: they run in spawned children and pay every
import per document.
"""

import shutil
import stat
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import zipfile_deflate64 as zipfile

MAX_PACKAGE_FILES = 10_000
MAX_PACKAGE_UNCOMPRESSED_BYTES = 1_500_000_000
MAX_PACKAGE_MEMBER_BYTES = 750_000_000
INLINE_XBRL_NAMESPACE_MARKERS = (
    b"http://www.xbrl.org/2008/inlineXBRL",
    b"http://www.xbrl.org/2013/inlineXBRL",
)


def contains_inline_xbrl_namespace(report_path: Path) -> bool:
    ...  # the moved body, with INLINE_XBRL_NAMESPACE_MARKERS


def extract_report_package(
    package_path: Path,
    temp_dir: Path,
) -> tuple[list[str], int]:
    ...  # the moved body, calling contains_inline_xbrl_namespace


def report_paths_for(
    temp_dir: Path,
    report_members: Iterable[str],
) -> dict[str, Path]:
    """The extracted path of each report member (segment_parser's mapping)."""
    return {
        member: temp_dir.joinpath(*PurePosixPath(member).parts)
        for member in report_members
    }
```

In `segment_parser.py` delete the moved definitions and add, next to the other `dagster_v3.defs.esef_filings` imports:

```python
from dagster_v3.defs.esef_filings.report_package import (
    MAX_PACKAGE_FILES,
    MAX_PACKAGE_MEMBER_BYTES,
    MAX_PACKAGE_UNCOMPRESSED_BYTES,
    contains_inline_xbrl_namespace as _contains_inline_xbrl_namespace,
    extract_report_package as _extract_report_package,
)
```

Check with `rg -n "_INLINE_XBRL_NAMESPACE_MARKERS|stat\.|shutil\." src/dagster_v3/defs/esef_filings/segment_parser.py` which imports remain used (`shutil` is still used at ~line 730 in `_write_arelle_compatible_package`; drop `stat` if nothing else uses it) and let `uv run ruff check --fix src/dagster_v3/defs/esef_filings/segment_parser.py` remove unused imports.

- [ ] **Step 8: Run the package tests plus the existing parser tests**

Run: `uv run pytest tests/test_esef_report_package.py tests/test_esef_ixbrl_segments.py tests/test_esef_segment_assets.py -q`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/common/child_timeout.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/report_package.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/segment_parser.py \
        corpscout/services/dagster_v3/tests/test_child_timeout.py \
        corpscout/services/dagster_v3/tests/test_esef_report_package.py
git commit -m "refactor(esef): child-timeout wrapper and package opener as light modules"
```

---

### Task 2: The corroboration set from `website_candidates`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/website_candidates.py` (lines 49-53 `_WEBSITE_FACT_ROLES`, 219-263 `extract_website_candidates`, 296-386 `_extract_report_websites`)
- Test: `corpscout/services/dagster_v3/tests/test_esef_website_candidates.py`

**Interfaces:**
- Produces: `WEBSITE_FACT_CONCEPTS: frozenset[str]` (the three website concept local names); `extract_website_candidates_with_corroboration(report_paths, *, tagged_values, known_email_domains) -> tuple[list[EsefWebsiteCandidate], frozenset[str]]`. `extract_website_candidates` keeps its signature and result (it now delegates). `EsefWebsiteCandidate` is unchanged (the artifact serialises it generically; a new field would change the artifact).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_esef_website_candidates.py`:

```python
from dagster_v3.defs.esef_filings.website_candidates import (  # noqa: E402
    WEBSITE_FACT_CONCEPTS,
    extract_website_candidates_with_corroboration,
)


def test_corroboration_set_names_tagged_email_and_repeated_domains(
    tmp_path: Path,
) -> None:
    report = tmp_path / "report.xhtml"
    report.write_text(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Mer information finns på www.example.com.</p>
        <p>Delårsrapporter publiceras på www.example.com/ir.</p>
        <p>Läs mer på www.once-only.org om ramverket.</p>
        </body></html>""",
        encoding="utf-8",
    )
    report_paths = {"reports/report.xhtml": report}
    tagged = [
        TaggedWebsiteValue(
            report_member="",
            concept_local_name="WebsitesOfLegalEntity",
            value="https://www.tagged.example.net",
        )
    ]

    candidates, corroborated = extract_website_candidates_with_corroboration(
        report_paths,
        tagged_values=tagged,
        known_email_domains=["mail.example.se"],
    )

    assert corroborated == frozenset({"example.com", "example.net", "example.se"})
    assert "once-only.org" not in corroborated
    assert [c.registrable_domain for c in candidates] == [
        "example.com",
        "example.net",
        "once-only.org",
    ]
    tagged_candidate = candidates[1]
    assert tagged_candidate.suggested_roles == ["company_website"]
    # The plain accessor is the same extraction.
    assert (
        extract_website_candidates(
            report_paths, tagged_values=tagged, known_email_domains=["mail.example.se"]
        )
        == candidates
    )


def test_website_fact_concepts_are_the_three_tagged_website_roles() -> None:
    assert WEBSITE_FACT_CONCEPTS == frozenset(
        {
            "WebsitesOfLegalEntity",
            "WebsiteAtWhichTheFinancialStatementsOfTheEntityAreDisclosedTogetherWithTheAuditorsReport",
            "WebsiteOfTheAuditEntity",
        }
    )
```

(Move the two new imports up into the module's existing `from ... import (...)` block instead of the `noqa` form if ruff objects.)

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_esef_website_candidates.py -q -k "corroboration_set or fact_concepts"`
Expected: FAIL with `ImportError: cannot import name 'WEBSITE_FACT_CONCEPTS'`

- [ ] **Step 3: Implement**

After `_WEBSITE_FACT_ROLES` (line 53):

```python
# The tagged concepts the extractor reads as website facts; the esef_domains
# extractor filters corpscout.esef_facts on these.
WEBSITE_FACT_CONCEPTS = frozenset(_WEBSITE_FACT_ROLES)
```

Replace `extract_website_candidates` (lines 219-263) with:

```python
def extract_website_candidates(
    report_paths: Mapping[str, Path],
    *,
    tagged_values: Iterable[TaggedWebsiteValue],
    known_email_domains: Iterable[str],
) -> list[EsefWebsiteCandidate]:
    """Extract auditable URL evidence grouped by registrable domain."""
    candidates, _corroborated = extract_website_candidates_with_corroboration(
        report_paths,
        tagged_values=tagged_values,
        known_email_domains=known_email_domains,
    )
    return candidates


def extract_website_candidates_with_corroboration(
    report_paths: Mapping[str, Path],
    *,
    tagged_values: Iterable[TaggedWebsiteValue],
    known_email_domains: Iterable[str],
) -> tuple[list[EsefWebsiteCandidate], frozenset[str]]:
    """As `extract_website_candidates`, plus the registrable domains the
    extraction treated as corroborated: backed by a tagged website fact, a
    known e-mail domain, or more than one unbroken mention in one report
    member. `esef_domains` persists membership as a row's `corroborated`
    flag; the artifact's candidate shape is unchanged.
    """
    corroborating_domains = {
        domain
        for value in known_email_domains
        if (domain := registrable_domain_for_host(value)) is not None
    }
    accumulators: dict[str, _WebsiteAccumulator] = {}
    for tagged_value in sorted(
        tagged_values,
        key=lambda item: (
            item.report_member,
            item.concept_local_name,
            item.value,
        ),
    ):
        _extract_tagged_value(
            tagged_value,
            corroborating_domains=corroborating_domains,
            accumulators=accumulators,
        )
    # Every domain in `accumulators` at this checkpoint came from a tagged
    # fact (report bodies haven't been parsed yet) -- used below so a
    # domain independently confirmed by an XBRL website tag is never
    # classified `external_reference`.
    tagged_fact_domains = frozenset(accumulators)

    corroborated: set[str] = set(corroborating_domains) | set(tagged_fact_domains)
    for report_member, report_path in sorted(report_paths.items()):
        corroborated |= _extract_report_websites(
            report_member=report_member,
            report_path=report_path,
            corroborating_domains=corroborating_domains,
            tagged_fact_domains=tagged_fact_domains,
            accumulators=accumulators,
        )

    return (
        [
            _finalize_candidate(accumulator)
            for _, accumulator in sorted(accumulators.items())
        ],
        frozenset(corroborated),
    )
```

In `_extract_report_websites`: change the return annotation from `-> None` to `-> frozenset[str]` and add `return frozenset(corroborated_domains)` as the function's last statement (after the `for element in tree.xpath(...)` loop). Update its docstring/comment if any mention `None`.

- [ ] **Step 4: Run the whole website-candidates test module**

Run: `uv run pytest tests/test_esef_website_candidates.py tests/test_esef_ixbrl_segments.py -q`
Expected: all pass (the end-to-end Handelsbanken test skips if its file is absent).

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/website_candidates.py \
        corpscout/services/dagster_v3/tests/test_esef_website_candidates.py
git commit -m "feat(esef): website extraction reports its corroborated domains"
```

---

### Task 3: The per-document extraction module

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_extraction.py`
- Test: `corpscout/services/dagster_v3/tests/test_esef_domains_extraction.py`

**Interfaces:**
- Consumes: Task 1 (`run_in_child_with_timeout`, `ChildTimeoutError`, `ChildFailedError`, `extract_report_package`, `report_paths_for`), Task 2 (`extract_website_candidates_with_corroboration`, `TaggedWebsiteValue`, `EsefWebsiteCandidate`).
- Produces: `ESEF_DOMAINS_EXTRACTOR_VERSION`, `STATUS_OK/STATUS_EMPTY/STATUS_FAILED/STATUS_TIMED_OUT`, `DomainsDocument(source_document_id, package_sha256, lei, period_end: date, fiscal_year: int)`, `DocumentDomains(status, candidates: tuple[EsefWebsiteCandidate, ...], corroborated_domains: frozenset[str], error_message, seconds)`, `extract_package_domains(package_path: str, tagged_values: tuple[tuple[str, str], ...], known_email_domains: tuple[str, ...], work_root: str) -> tuple[tuple[EsefWebsiteCandidate, ...], frozenset[str]]`, `extract_package_domains_bounded(package_path, tagged_values, known_email_domains, timeout_seconds: int, work_root: str) -> DocumentDomains`, `domain_id(source_document_id, registrable_domain) -> str`, `domain_rows(document, result, *, extractor_version, source_run_id, extracted_at: datetime) -> list[dict[str, object]]` (keys = the 18 export columns of Task 4).

- [ ] **Step 1: Write the failing tests**

`tests/test_esef_domains_extraction.py`:

```python
"""The esef_domains extractor's per-document function: package in, website
candidates + esef_domains rows out. Runs the real extraction in the real
spawned child for one synthetic package."""

import subprocess
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_OK,
    DocumentDomains,
    DomainsDocument,
    domain_id,
    domain_rows,
    extract_package_domains,
    extract_package_domains_bounded,
)
from dagster_v3.defs.esef_filings.tables import ESEF_DOMAINS_EXPORT_COLUMNS

REPORT = """<html xmlns="http://www.w3.org/1999/xhtml"
 xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>
<p>Mer information finns på www.example.com.</p>
<p>Delårsrapporter publiceras på www.example.com/ir.</p>
<p>Läs mer på www.once-only.org om ramverket.</p>
</body></html>"""

DOCUMENT = DomainsDocument(
    source_document_id="doc-1",
    package_sha256="a" * 64,
    lei="5493001KJTIIGC8Y1R12",
    period_end=date(2024, 12, 31),
    fiscal_year=2024,
)


def _package(path: Path, report: str = REPORT) -> Path:
    with zipfile.ZipFile(path, "w") as package:
        package.writestr("pkg/META-INF/taxonomyPackage.xml", "<x/>")
        package.writestr("pkg/reports/report.xhtml", report)
    return path


def test_extract_package_domains_reads_reports_tagged_facts_and_emails(
    tmp_path: Path,
) -> None:
    package = _package(tmp_path / "p.zip")

    candidates, corroborated = extract_package_domains(
        str(package),
        (("WebsitesOfLegalEntity", "https://www.tagged.example.net"),),
        ("example.se",),
        str(tmp_path),
    )

    assert [c.registrable_domain for c in candidates] == [
        "example.com",
        "example.net",
        "once-only.org",
    ]
    assert corroborated == frozenset({"example.com", "example.net", "example.se"})
    tagged = candidates[1]
    assert tagged.suggested_roles == ["company_website"]
    assert tagged.evidence[0].report_member == ""
    assert tagged.evidence[0].extraction_method == "tagged_fact"
    # The extraction directory is cleaned up.
    assert [p for p in tmp_path.iterdir() if p.is_dir()] == []


def test_bounded_extraction_runs_in_a_child_and_reports_ok(tmp_path: Path) -> None:
    package = _package(tmp_path / "p.zip")

    result = extract_package_domains_bounded(
        str(package), (), (), 120, str(tmp_path)
    )

    assert result.status == STATUS_OK
    assert [c.registrable_domain for c in result.candidates] == [
        "example.com",
        "once-only.org",
    ]
    assert result.corroborated_domains == frozenset({"example.com"})
    assert result.error_message == ""
    assert result.seconds > 0


def test_bounded_extraction_reports_a_bad_package_as_failed(tmp_path: Path) -> None:
    not_a_zip = tmp_path / "p.zip"
    not_a_zip.write_bytes(b"not a zip")

    result = extract_package_domains_bounded(
        str(not_a_zip), (), (), 120, str(tmp_path)
    )

    assert result.status == STATUS_FAILED
    assert result.candidates == ()
    assert "BadZipFile" in result.error_message


def test_domain_rows_one_row_per_candidate_in_export_column_order(
    tmp_path: Path,
) -> None:
    candidates, corroborated = extract_package_domains(
        str(_package(tmp_path / "p.zip")), (), ("example.se",), str(tmp_path)
    )
    result = DocumentDomains(STATUS_OK, candidates, corroborated, "", 0.5)
    extracted_at = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)

    rows = domain_rows(
        DOCUMENT,
        result,
        extractor_version=ESEF_DOMAINS_EXTRACTOR_VERSION,
        source_run_id="run-1",
        extracted_at=extracted_at,
    )

    assert [row["registrable_domain"] for row in rows] == [
        "example.com",
        "once-only.org",
    ]
    for row in rows:
        assert tuple(row) == ESEF_DOMAINS_EXPORT_COLUMNS
        assert row["source_document_id"] == "doc-1"
        assert row["package_sha256"] == "a" * 64
        assert row["lei"] == DOCUMENT.lei
        assert row["period_end"] == date(2024, 12, 31)
        assert row["fiscal_year"] == 2024
        assert row["extraction_status"] == STATUS_OK
        assert row["error_message"] == ""
        assert row["extractor_version"] == "esef-domains-v1"
        assert row["source_run_id"] == "run-1"
        assert row["extracted_at"] == extracted_at
    first = rows[0]
    assert first["domain_id"] == domain_id("doc-1", "example.com")
    assert len(first["domain_id"]) == 64
    assert first["corroborated"] == 1
    assert first["evidence_count"] == 2
    assert first["hosts_json"] == '["www.example.com"]'
    assert '"extraction_method"' in first["evidence_json"]
    assert rows[1]["corroborated"] == 0
    assert rows[1]["roles_json"] == '["external_reference"]'


def test_domain_rows_marker_rows_for_empty_and_failed_documents() -> None:
    extracted_at = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
    empty = domain_rows(
        DOCUMENT,
        DocumentDomains(STATUS_EMPTY, (), frozenset(), "", 0.1),
        extractor_version="esef-domains-v1",
        source_run_id="run-1",
        extracted_at=extracted_at,
    )
    failed = domain_rows(
        DOCUMENT,
        DocumentDomains(STATUS_FAILED, (), frozenset(), "BadZipFile: x", 0.1),
        extractor_version="esef-domains-v1",
        source_run_id="run-1",
        extracted_at=extracted_at,
    )

    assert len(empty) == 1 and len(failed) == 1
    for row in (empty[0], failed[0]):
        assert tuple(row) == ESEF_DOMAINS_EXPORT_COLUMNS
        assert row["registrable_domain"] == ""
        assert row["domain_id"] == domain_id("doc-1", "")
        assert row["hosts_json"] == "[]"
        assert row["roles_json"] == "[]"
        assert row["evidence_json"] == "[]"
        assert row["evidence_count"] == 0
        assert row["corroborated"] == 0
    assert empty[0]["extraction_status"] == STATUS_EMPTY
    assert empty[0]["error_message"] == ""
    assert failed[0]["extraction_status"] == STATUS_FAILED
    assert failed[0]["error_message"] == "BadZipFile: x"


def test_module_stays_light_no_dagster_or_arelle_import() -> None:
    probe = (
        "import sys; import dagster_v3.defs.esef_filings.domains_extraction; "
        "heavy = sorted(m for m in sys.modules if m == 'dagster' or m.startswith('dagster.') "
        "or m == 'arelle' or m.startswith('arelle.') "
        "or m.endswith('segment_parser') or m.endswith('segment_assets')); "
        "print(heavy); sys.exit(1 if heavy else 0)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
```

Note for the implementer: `rows[1]["roles_json"] == '["external_reference"]'` assumes the single referral-shaped mention ("Läs mer på www.once-only.org") is classified `external_reference` by the merged fix. If the extraction classifies it differently, assert the role the extractor actually produces and say so in the report rather than changing the extractor.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_esef_domains_extraction.py -q`
Expected: FAIL with `ModuleNotFoundError` (domains_extraction) — and `ImportError` on `ESEF_DOMAINS_EXPORT_COLUMNS` until Task 4 lands; for now add the tuple to `tables.py` as specified in Task 4 Step 3 (Task 4 keeps it).

- [ ] **Step 3: Write `domains_extraction.py`**

```python
"""Per-document domain extraction for the esef_domains extractor (spec
2026-09-13, section 3): one archived ESEF report package in, the website
candidates and the rows for corpscout.esef_domains out.

Light on purpose. `extract_package_domains` runs inside a spawned child
(`dagster_v3.defs.common.child_timeout`), so this module must not import
dagster, arelle or the artifact parser: every import here is paid once per
document (tests/test_esef_domains_extraction.py pins this).
"""

import json
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter

from dagster_v3.defs.common.child_timeout import (
    ChildFailedError,
    ChildTimeoutError,
    run_in_child_with_timeout,
)
from dagster_v3.defs.esef_filings.report_package import (
    extract_report_package,
    report_paths_for,
)
from dagster_v3.defs.esef_filings.website_candidates import (
    EsefWebsiteCandidate,
    TaggedWebsiteValue,
    extract_website_candidates_with_corroboration,
)

# Bump to re-extract every document (the sensor drains the stale set). The
# only version this extractor has; the artifact schema plays no part.
ESEF_DOMAINS_EXTRACTOR_VERSION = "esef-domains-v1"

STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"
STATUS_TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class DomainsDocument:
    """One row of the esef_filings index selected for extraction."""

    source_document_id: str
    package_sha256: str
    lei: str
    period_end: date
    fiscal_year: int


@dataclass(frozen=True)
class DocumentDomains:
    """What one document's extraction produced (picklable: crosses the pool)."""

    status: str
    candidates: tuple[EsefWebsiteCandidate, ...]
    corroborated_domains: frozenset[str]
    error_message: str
    seconds: float


def extract_package_domains(
    package_path: str,
    tagged_values: tuple[tuple[str, str], ...],
    known_email_domains: tuple[str, ...],
    work_root: str,
) -> tuple[tuple[EsefWebsiteCandidate, ...], frozenset[str]]:
    """Open the package, select its report members, extract. Runs in the child.

    `tagged_values` are (concept_local_name, raw_value) pairs read from
    corpscout.esef_facts, which does not record the report member a fact
    came from, so their evidence carries report_member ''.
    """
    work_dir = Path(tempfile.mkdtemp(prefix="esef-domains-", dir=work_root))
    try:
        report_members, _repaired = extract_report_package(
            Path(package_path), work_dir
        )
        candidates, corroborated = extract_website_candidates_with_corroboration(
            report_paths_for(work_dir, report_members),
            tagged_values=[
                TaggedWebsiteValue(
                    report_member="", concept_local_name=concept, value=value
                )
                for concept, value in tagged_values
            ],
            known_email_domains=known_email_domains,
        )
        return tuple(candidates), corroborated
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def extract_package_domains_bounded(
    package_path: str,
    tagged_values: tuple[tuple[str, str], ...],
    known_email_domains: tuple[str, ...],
    timeout_seconds: int,
    work_root: str,
) -> DocumentDomains:
    """Pool-worker entry: the extraction in a killable child with a budget.

    Never raises for a document's own problems -- a timeout, a bad package,
    an extraction error -- those become the document's status so the run
    continues; only the child machinery itself can raise.
    """
    started = perf_counter()
    try:
        candidates, corroborated = run_in_child_with_timeout(
            extract_package_domains,
            (package_path, tagged_values, known_email_domains, work_root),
            timeout_seconds=timeout_seconds,
        )
    except ChildTimeoutError as exc:
        return DocumentDomains(
            STATUS_TIMED_OUT, (), frozenset(), str(exc), perf_counter() - started
        )
    except ChildFailedError as exc:
        return DocumentDomains(
            STATUS_FAILED, (), frozenset(), exc.detail, perf_counter() - started
        )
    return DocumentDomains(
        STATUS_OK if candidates else STATUS_EMPTY,
        candidates,
        corroborated,
        "",
        perf_counter() - started,
    )


def domain_id(source_document_id: str, registrable_domain: str) -> str:
    return sha256(f"{source_document_id}\n{registrable_domain}".encode()).hexdigest()


def domain_rows(
    document: DomainsDocument,
    result: DocumentDomains,
    *,
    extractor_version: str,
    source_run_id: str,
    extracted_at: datetime,
) -> list[dict[str, object]]:
    """The esef_domains rows for one document, keyed in export-column order:
    one per registrable domain, or one marker row (registrable_domain '')
    when the document produced none or its extraction failed -- so the
    document is never selected as stale again at this extractor version.
    """

    def row(
        registrable_domain: str,
        hosts: list[str],
        normalized_urls: list[str],
        roles: list[str],
        evidence: list[dict[str, object]],
        corroborated: int,
    ) -> dict[str, object]:
        return {
            "domain_id": domain_id(document.source_document_id, registrable_domain),
            "source_document_id": document.source_document_id,
            "package_sha256": document.package_sha256,
            "lei": document.lei,
            "period_end": document.period_end,
            "fiscal_year": document.fiscal_year,
            "extraction_status": result.status,
            "registrable_domain": registrable_domain,
            "hosts_json": _json_text(hosts),
            "normalized_urls_json": _json_text(normalized_urls),
            "roles_json": _json_text(roles),
            "evidence_json": _json_text(evidence),
            "evidence_count": len(evidence),
            "corroborated": corroborated,
            "error_message": result.error_message,
            "extractor_version": extractor_version,
            "source_run_id": source_run_id,
            "extracted_at": extracted_at,
        }

    if not result.candidates:
        return [row("", [], [], [], [], 0)]
    return [
        row(
            candidate.registrable_domain,
            list(candidate.hosts),
            list(candidate.normalized_urls),
            list(candidate.suggested_roles),
            [asdict(item) for item in candidate.evidence],
            int(candidate.registrable_domain in result.corroborated_domains),
        )
        for candidate in result.candidates
    ]


def _json_text(value: object) -> str:
    # Same shape as segment_assets._json_text (the contact-candidates rows).
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
```

Check `segment_assets._json_text` (line ~1468) for any further keyword (e.g. `sort_keys`) and mirror it exactly.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_esef_domains_extraction.py -q`
Expected: 6 passed (two tests spawn a child, ~2-5 s).

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_extraction.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py \
        corpscout/services/dagster_v3/tests/test_esef_domains_extraction.py
git commit -m "feat(esef): per-document domain extraction in a killable child"
```

---

### Task 4: Table constants, migration 000405 and the `se_esef_domains` view

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py` (table constants near lines 28-51; export tuples near line 247; `SE_ESEF_VIEWS` at lines 442-451)
- Create: `corpscout/clickhouse/migrations/000405_corpscout_esef_domains.up.sql`
- Create: `corpscout/clickhouse/migrations/000405_corpscout_esef_domains.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_esef_country_views.py` (the view list at lines 19-25; the 000395 pin at lines 50-54)
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` after line 419)
- Test: `corpscout/services/dagster_v3/tests/test_esef_domains_tables.py`

**Interfaces:**
- Produces: `tables.ESEF_DOMAINS_TABLE = "esef_domains"`, `tables.QUALIFIED_ESEF_DOMAINS_TABLE = "corpscout.esef_domains"`, `tables.ESEF_DOMAINS_EXPORT_COLUMNS` (18, in the order below), `tables._ESEF_DOMAINS_VIEW_COLUMNS` (20), `SeEsefView("esef_domains", ..., final=True)` in `SE_ESEF_VIEWS`, view `corpscout.se_esef_domains`.

- [ ] **Step 1: Write the failing contract tests**

`tests/test_esef_domains_tables.py`:

```python
"""corpscout.esef_domains (migration 000405): the export-column tuple pins the
CREATE TABLE's column order, the view entry renders into the same migration."""

import re
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql

MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
UP = MIGRATIONS / "000405_corpscout_esef_domains.up.sql"
DOWN = MIGRATIONS / "000405_corpscout_esef_domains.down.sql"


def _normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


def test_export_columns_are_the_eighteen_insert_columns() -> None:
    assert tables.ESEF_DOMAINS_TABLE == "esef_domains"
    assert tables.QUALIFIED_ESEF_DOMAINS_TABLE == "corpscout.esef_domains"
    assert tables.ESEF_DOMAINS_EXPORT_COLUMNS == (
        "domain_id",
        "source_document_id",
        "package_sha256",
        "lei",
        "period_end",
        "fiscal_year",
        "extraction_status",
        "registrable_domain",
        "hosts_json",
        "normalized_urls_json",
        "roles_json",
        "evidence_json",
        "evidence_count",
        "corroborated",
        "error_message",
        "extractor_version",
        "source_run_id",
        "extracted_at",
    )


def test_migration_declares_the_table_columns_in_export_order() -> None:
    sql = UP.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS corpscout.esef_domains" in sql
    positions = [sql.index(f"\n    {column} ") for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS]
    assert positions == sorted(positions)
    assert "\n    source_record_uid String DEFAULT lower(hex(SHA256(concat(" in sql
    assert "\n    resolved_at DateTime64(3) DEFAULT now64(3)" in sql
    assert "ENGINE = ReplacingMergeTree(extracted_at)" in sql
    assert "ORDER BY (lei, source_document_id, registrable_domain)" in sql


def test_migration_embeds_the_rendered_view_and_the_down_drops_both() -> None:
    view = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_domains")
    assert view.view == "se_esef_domains"
    assert view.final is True
    assert view.columns == (
        "domain_id",
        "source_document_id",
        "source_record_uid",
        *tables.ESEF_DOMAINS_EXPORT_COLUMNS[2:],
        "resolved_at",
    )
    assert _normalized(build_se_esef_view_sql(view)) in _normalized(UP.read_text(encoding="utf-8"))
    down = DOWN.read_text(encoding="utf-8")
    assert "DROP VIEW IF EXISTS corpscout.se_esef_domains" in down
    assert "DROP TABLE IF EXISTS corpscout.esef_domains" in down
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_esef_domains_tables.py -q`
Expected: FAIL (`AttributeError: ... has no attribute 'ESEF_DOMAINS_TABLE'` or the missing tuple/file).

- [ ] **Step 3: Add the constants to `tables.py`**

Next to the other table names (line ~33) and qualified names (line ~51):

```python
ESEF_DOMAINS_TABLE = "esef_domains"
QUALIFIED_ESEF_DOMAINS_TABLE = f"{ESEF_DATABASE}.{ESEF_DOMAINS_TABLE}"
```

After `ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS`:

```python
# One row per (document, registrable domain) from the esef_domains extractor
# (migration 000405, spec 2026-09-13), or one marker row per document without
# domains. source_record_uid and resolved_at are DEFAULT-expression columns
# there and never part of the INSERT tuple, so both are excluded from this list.
ESEF_DOMAINS_EXPORT_COLUMNS = (
    "domain_id",
    "source_document_id",
    "package_sha256",
    "lei",
    "period_end",
    "fiscal_year",
    "extraction_status",
    "registrable_domain",
    "hosts_json",
    "normalized_urls_json",
    "roles_json",
    "evidence_json",
    "evidence_count",
    "corroborated",
    "error_message",
    "extractor_version",
    "source_run_id",
    "extracted_at",
)
```

Next to `_ESEF_DOCUMENT_CONTACT_CANDIDATES_VIEW_COLUMNS` (line ~427):

```python
_ESEF_DOMAINS_VIEW_COLUMNS = (
    ESEF_DOMAINS_EXPORT_COLUMNS[0],
    ESEF_DOMAINS_EXPORT_COLUMNS[1],
    "source_record_uid",
    *ESEF_DOMAINS_EXPORT_COLUMNS[2:],
    "resolved_at",
)
```

Append to `SE_ESEF_VIEWS`:

```python
    SeEsefView("esef_domains", _ESEF_DOMAINS_VIEW_COLUMNS, final=True),
```

- [ ] **Step 4: Write the migration**

Render the view with:

```bash
uv run python -c "from dagster_v3.defs.esef_filings import tables; from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql; v = next(v for v in tables.SE_ESEF_VIEWS if v.table == 'esef_domains'); print(build_se_esef_view_sql(v) + ';')"
```

`000405_corpscout_esef_domains.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- ESEF DOMAINS: the first independent ESEF extractor (spec 2026-09-13, owner ruling: documents
-- plus many extractors, each with its own table, version and re-run -- not one big versioned
-- parser). corpscout.esef_domains holds one row per (filing document, registrable domain) as
-- found in the archived report package by dagster_v3.defs.esef_filings.domains_extraction, or
-- one marker row (registrable_domain '') per document whose extraction found nothing, failed
-- or timed out, so the extractor never selects that document again at the same
-- extractor_version. A run replaces every row of the documents it attempted (stage table +
-- EXCHANGE TABLES); ReplacingMergeTree(extracted_at) only guards the unlikely overlap. The
-- website rows of corpscout.esef_document_contact_candidates stay until a later slice moves
-- the contact extraction; company_serving and the backoffice read domains from here now.
CREATE TABLE IF NOT EXISTS corpscout.esef_domains
(
    domain_id FixedString(64),
    source_document_id String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(toString(package_sha256)))))),
    package_sha256 String,
    lei String,
    period_end Date32,
    fiscal_year UInt16,
    extraction_status LowCardinality(String),
    registrable_domain String,
    hosts_json String,
    normalized_urls_json String,
    roles_json String,
    evidence_json String,
    evidence_count UInt32,
    corroborated UInt8,
    error_message String,
    extractor_version LowCardinality(String),
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, source_document_id, registrable_domain);

-- Sweden's slice, rendered by dagster_v3.defs.esef_filings.country_views for the
-- SeEsefView("esef_domains") entry of tables.SE_ESEF_VIEWS -- NOT HAND-WRITTEN, pinned by
-- tests/test_esef_domains_tables.py.
<paste the rendered CREATE OR REPLACE VIEW statement here, ending with ;>
```

`000405_corpscout_esef_domains.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000405. The extractor's asset (esef_domains_clickhouse) and sensor must be undeployed
-- with it, or the next sensor tick fails on the missing table.
DROP VIEW IF EXISTS corpscout.se_esef_domains;
DROP TABLE IF EXISTS corpscout.esef_domains;
```

- [ ] **Step 5: Update the existing pins**

`tests/test_clickhouse_migrations.py`: add `"000405_corpscout_esef_domains",` after the 000403 entry of `EXPECTED_MIGRATIONS`.

`tests/test_esef_country_views.py`: rename `test_eight_views_one_per_swedish_consumer` to `test_nine_views_one_per_swedish_consumer` and append `"se_esef_domains"` to the expected list. Replace `test_migration_000395_embeds_every_rendered_view` with:

```python
MIGRATION_000405 = MIGRATION.parent / "000405_corpscout_esef_domains.up.sql"
# Views added after the country-agnostic cutover live in their own migration.
VIEW_MIGRATIONS = {"se_esef_domains": MIGRATION_000405}


def test_every_rendered_view_is_embedded_in_its_migration() -> None:
    for view in tables.SE_ESEF_VIEWS:
        migration = VIEW_MIGRATIONS.get(view.view, MIGRATION)
        up = _normalized(migration.read_text(encoding="utf-8"))
        assert _normalized(build_se_esef_view_sql(view)) in up, view.view


def test_migration_000395_adds_the_link_status_column() -> None:
    up = _normalized(MIGRATION.read_text(encoding="utf-8"))
    assert "ALTER TABLE corpscout.esef_entity_registry_map ADD COLUMN IF NOT EXISTS link_status LowCardinality(String) DEFAULT 'gleif' AFTER match_source" in up
```

(Keep any other assertions the original test carried.)

- [ ] **Step 6: Run the pins**

Run: `uv run pytest tests/test_esef_domains_tables.py tests/test_esef_country_views.py tests/test_clickhouse_migrations.py -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000405_corpscout_esef_domains.up.sql \
        corpscout/clickhouse/migrations/000405_corpscout_esef_domains.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/tables.py \
        corpscout/services/dagster_v3/tests/test_esef_domains_tables.py \
        corpscout/services/dagster_v3/tests/test_esef_country_views.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): esef_domains table and se_esef_domains view (000405)"
```

---

### Task 5: The extractor asset

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/document_rows.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_extractor.py`
- Test: `corpscout/services/dagster_v3/tests/test_esef_domains_extractor.py`

**Interfaces:**
- Consumes: Task 3 (`domains_extraction.*`), Task 4 (`tables.ESEF_DOMAINS_*`), `segment_assets.ESEF_DOCUMENT_BUCKET` / `report_package_object_key`, `website_candidates.WEBSITE_FACT_CONCEPTS`, `clickhouse.resolved.assert_clickhouse_tables_exist`, `common.resources.ObjectStoreResource.read_bytes(key, bucket=)`.
- Produces: `replace_document_rows(clickhouse, *, table, columns, source_document_ids, rows)`; `EsefDomainsConfig`; `stale_documents_sql(*, all_available: bool, only_listed: bool) -> str`; `stale_document_count_sql() -> str`; `tagged_website_facts_sql()`, `email_domains_sql()`; `select_documents(clickhouse, config) -> list[DomainsDocument]`; `load_tagged_website_facts(clickhouse, ids) -> dict[str, tuple[tuple[str, str], ...]]`; `load_email_domains(clickhouse, ids) -> dict[str, tuple[str, ...]]`; `run_esef_domains_extraction(*, clickhouse, object_store, config, source_run_id, log, extract=extract_package_domains_bounded) -> dict[str, object]`; asset `esef_domains_clickhouse`; job `esef_domains_job` (name `"esef_domains_job"`); `ESEF_DOMAINS_POOL = "esef_domains_clickhouse"`.

- [ ] **Step 1: Write the failing tests**

`tests/test_esef_domains_extractor.py`:

```python
"""The esef_domains extractor asset: selection SQL, the loaders, the replace
writer and one end-to-end run over the fake object store + fake ClickHouse
(fakes from tests/test_esef_llm_enrichment.py)."""

import logging
import zipfile
from datetime import date
from hashlib import sha256
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_FAILED,
    STATUS_OK,
    DomainsDocument,
)
from dagster_v3.defs.esef_filings.domains_extractor import (
    ESEF_DOMAINS_POOL,
    EsefDomainsConfig,
    email_domains_sql,
    esef_domains_clickhouse,
    esef_domains_job,
    load_email_domains,
    load_tagged_website_facts,
    run_esef_domains_extraction,
    select_documents,
    stale_document_count_sql,
    stale_documents_sql,
    tagged_website_facts_sql,
)
from dagster_v3.defs.esef_filings.document_rows import replace_document_rows
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    report_package_object_key,
)
from dagster_v3.defs.esef_filings.website_candidates import WEBSITE_FACT_CONCEPTS
from tests.test_esef_llm_enrichment import _FakeClickHouse, _FakeObjectStore

REPORT = """<html xmlns="http://www.w3.org/1999/xhtml"
 xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>
<p>Mer information finns på www.example.com.</p>
<p>Delårsrapporter publiceras på www.example.com/ir.</p>
</body></html>"""

TABLES_EXIST = [("esef_domains",)]


def _package_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "p.zip"
    with zipfile.ZipFile(path, "w") as package:
        package.writestr("pkg/reports/report.xhtml", REPORT)
    return path.read_bytes()


# --- SQL text -----------------------------------------------------------------


def test_stale_documents_sql_selects_available_documents_at_another_version() -> None:
    sql = stale_documents_sql(all_available=False, only_listed=False)
    assert "FROM corpscout.esef_filings AS filings FINAL" in sql
    assert "SELECT DISTINCT fxo_id FROM corpscout.esef_facts" in sql
    assert "FROM corpscout.esef_domains GROUP BY source_document_id" in sql
    assert "filings.package_sha256 != ''" in sql
    assert "filings.period_end <= today()" in sql
    assert "extracted.extractor_version != %(extractor_version)s" in sql
    assert "ORDER BY filings.period_end DESC, filings.fxo_id" in sql
    assert "%(source_document_ids)s" not in sql


def test_stale_documents_sql_variants() -> None:
    everything = stale_documents_sql(all_available=True, only_listed=False)
    assert "%(extractor_version)s" not in everything
    listed = stale_documents_sql(all_available=True, only_listed=True)
    assert "filings.fxo_id IN %(source_document_ids)s" in listed
    assert stale_document_count_sql().startswith("SELECT count() FROM (")
    assert "%(extractor_version)s" in stale_document_count_sql()


def test_input_sql_reads_facts_and_email_candidates() -> None:
    assert "FROM corpscout.esef_facts" in tagged_website_facts_sql()
    assert "concept_local_name IN %(concepts)s" in tagged_website_facts_sql()
    assert "FROM corpscout.esef_document_contact_candidates" in email_domains_sql()
    assert "candidate_kind = 'email'" in email_domains_sql()


# --- selection and loaders ------------------------------------------------------


def test_select_documents_maps_rows_and_applies_max_documents() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                ("doc-2", "b" * 64, "LEI2", date(2024, 12, 31)),
                ("doc-1", "a" * 64, "LEI1", "2023-12-31"),
            ]
        ]
    )

    documents = select_documents(
        clickhouse, EsefDomainsConfig(max_documents=1, workers=1)
    )

    assert documents == [
        DomainsDocument("doc-2", "b" * 64, "LEI2", date(2024, 12, 31), 2024)
    ]
    sql, parameters = clickhouse.client.calls[0]
    assert "extracted.extractor_version != %(extractor_version)s" in sql
    assert parameters == {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION}


def test_select_documents_listed_ids_are_re_extracted_stale_or_not() -> None:
    clickhouse = _FakeClickHouse([[]])

    select_documents(
        clickhouse, EsefDomainsConfig(source_document_ids=["doc-9", "doc-1"], workers=1)
    )

    sql, parameters = clickhouse.client.calls[0]
    assert "%(extractor_version)s" not in sql
    assert "filings.fxo_id IN %(source_document_ids)s" in sql
    assert parameters["source_document_ids"] == ("doc-1", "doc-9")


def test_loaders_group_by_document_and_dedupe() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                ("doc-1", "WebsitesOfLegalEntity", "https://www.a.example"),
                ("doc-1", "WebsitesOfLegalEntity", "https://www.a.example"),
                ("doc-2", "WebsiteOfTheAuditEntity", "https://audit.example"),
            ],
            [("doc-1", "info@mail.example.se"), ("doc-1", "ir@example.com"), ("doc-3", "x")],
        ]
    )

    tagged = load_tagged_website_facts(clickhouse, ["doc-1", "doc-2"])
    emails = load_email_domains(clickhouse, ["doc-1", "doc-2", "doc-3"])

    assert tagged == {
        "doc-1": (("WebsitesOfLegalEntity", "https://www.a.example"),),
        "doc-2": (("WebsiteOfTheAuditEntity", "https://audit.example"),),
    }
    assert emails == {"doc-1": ("example.com", "mail.example.se")}
    _, parameters = clickhouse.client.calls[0]
    assert parameters["concepts"] == tuple(sorted(WEBSITE_FACT_CONCEPTS))
    assert load_tagged_website_facts(clickhouse, []) == {}


# --- writer ---------------------------------------------------------------------


def test_replace_document_rows_stages_exchanges_and_drops() -> None:
    clickhouse = _FakeClickHouse([])
    rows = [
        {column: f"{column}-value" for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS},
    ]

    replace_document_rows(
        clickhouse,
        table=tables.ESEF_DOMAINS_TABLE,
        columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
        source_document_ids=["doc-2", "doc-1", "doc-2"],
        rows=rows,
    )

    statements = [sql for sql, _ in clickhouse.client.calls]
    assert statements[0].startswith("CREATE TABLE `corpscout`.`_tmp_esef_domains_")
    assert statements[0].endswith("AS `corpscout`.`esef_domains`")
    assert "WHERE source_document_id NOT IN %(source_document_ids)s" in statements[1]
    assert clickhouse.client.calls[1][1] == {"source_document_ids": ("doc-1", "doc-2")}
    assert statements[2].endswith(f"({', '.join(tables.ESEF_DOMAINS_EXPORT_COLUMNS)}) VALUES")
    assert clickhouse.client.calls[2][1] == [
        tuple(f"{column}-value" for column in tables.ESEF_DOMAINS_EXPORT_COLUMNS)
    ]
    assert statements[3].startswith("EXCHANGE TABLES `corpscout`.`_tmp_esef_domains_")
    assert statements[4].startswith("DROP TABLE IF EXISTS `corpscout`.`_tmp_esef_domains_")


def test_replace_document_rows_without_rows_still_clears_the_documents() -> None:
    clickhouse = _FakeClickHouse([])

    replace_document_rows(
        clickhouse,
        table=tables.ESEF_DOMAINS_TABLE,
        columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
        source_document_ids=["doc-1"],
        rows=[],
    )

    statements = [sql for sql, _ in clickhouse.client.calls]
    assert len(statements) == 4
    assert not any(statement.endswith("VALUES") for statement in statements)


# --- the run ----------------------------------------------------------------------


def test_run_extracts_writes_batches_and_marks_a_missing_package(tmp_path: Path) -> None:
    body = _package_bytes(tmp_path)
    digest = sha256(body).hexdigest()
    object_store = _FakeObjectStore(
        {(ESEF_DOCUMENT_BUCKET, report_package_object_key(digest)): body}
    )
    clickhouse = _FakeClickHouse(
        [
            TABLES_EXIST,
            [
                ("doc-ok", digest, "LEI1", date(2024, 12, 31)),
                ("doc-missing", "c" * 64, "LEI2", date(2023, 12, 31)),
            ],
            [("doc-ok", "WebsitesOfLegalEntity", "https://www.example.com")],
            [],
        ]
    )

    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        config=EsefDomainsConfig(workers=1, batch_size=1, parse_timeout_seconds=120),
        source_run_id="run-1",
        log=logging.getLogger("test"),
    )

    assert summary["candidate_document_count"] == 2
    assert summary["attempted_document_count"] == 2
    assert summary["processed_document_count"] == 1
    assert summary["failed_document_count"] == 1
    assert summary["timed_out_document_count"] == 0
    assert summary["documents_without_domains"] == 0
    assert summary["row_count"] == 2
    assert summary["extractor_version"] == ESEF_DOMAINS_EXTRACTOR_VERSION
    assert summary["table"] == "corpscout.esef_domains"

    inserts = [
        (sql, parameters)
        for sql, parameters in clickhouse.client.calls
        if sql.endswith("VALUES")
    ]
    assert len(inserts) == 2  # batch_size=1 -> one replace per document
    columns = tables.ESEF_DOMAINS_EXPORT_COLUMNS
    ok_row = dict(zip(columns, inserts[0][1][0], strict=True))
    assert ok_row["source_document_id"] == "doc-ok"
    assert ok_row["extraction_status"] == STATUS_OK
    assert ok_row["registrable_domain"] == "example.com"
    assert ok_row["corroborated"] == 1  # tagged fact
    assert '"company_website"' in ok_row["roles_json"]
    assert ok_row["fiscal_year"] == 2024
    assert ok_row["source_run_id"] == "run-1"
    missing_row = dict(zip(columns, inserts[1][1][0], strict=True))
    assert missing_row["source_document_id"] == "doc-missing"
    assert missing_row["extraction_status"] == STATUS_FAILED
    assert missing_row["registrable_domain"] == ""
    assert "package download failed" in missing_row["error_message"]
    # The run's temp dir is gone.
    assert not [p for p in Path(tmp_path).iterdir() if p.name.startswith("esef-domains-")]


def test_run_with_nothing_stale_writes_nothing() -> None:
    clickhouse = _FakeClickHouse([TABLES_EXIST, [], [], []])

    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=_FakeObjectStore({}),
        config=EsefDomainsConfig(workers=1),
        source_run_id="run-1",
        log=logging.getLogger("test"),
    )

    assert summary["candidate_document_count"] == 0
    assert summary["row_count"] == 0
    assert not any(sql.startswith("CREATE TABLE") for sql, _ in clickhouse.client.calls)


# --- definitions ------------------------------------------------------------------


def test_asset_and_job_definitions() -> None:
    spec = esef_domains_clickhouse.get_asset_spec()
    assert spec.key.to_user_string() == "esef_domains_clickhouse"
    assert spec.group_name == "esef"
    assert {dep.asset_key.to_user_string() for dep in spec.deps} == {
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_document_contact_candidates_clickhouse",
    }
    assert esef_domains_clickhouse.op.pool == ESEF_DOMAINS_POOL
    assert esef_domains_job.name == "esef_domains_job"
```

If a `_FakeObjectStore`/`_FakeClickHouse` detail differs from what these tests assume (their constructors and `client.calls`/`objects` attributes are shown in `tests/test_esef_llm_enrichment.py` lines 1345-1399), adapt the test to the real fake, not the other way round. `esef_domains_clickhouse.op.pool` — if the Dagster build exposes the pool elsewhere (`esef_domains_clickhouse.op.tags` / `node_def.pool`), assert through the attribute that exists and note it.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_esef_domains_extractor.py -q`
Expected: FAIL with `ModuleNotFoundError` for `document_rows` / `domains_extractor`.

- [ ] **Step 3: Write `document_rows.py`**

```python
"""Replace an extractor table's rows for a set of documents atomically.

The stage + EXCHANGE TABLES recipe of llm_enrichment_assets' writer with a
document-only predicate, shared by the deterministic extractors (esef_domains
first). The LLM writer keeps its provider/model/prompt predicate for now.
"""

import uuid
from collections.abc import Mapping, Sequence

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings import tables


def replace_document_rows(
    clickhouse: ClickhouseResource,
    *,
    table: str,
    columns: Sequence[str],
    source_document_ids: Sequence[str],
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Copy every row of `table` for documents NOT in `source_document_ids`
    into a fresh stage table, append `rows`, EXCHANGE the stage with the
    target, drop the old one. The documents' previous rows vanish even when
    `rows` holds nothing for them. No-op without `source_document_ids`.
    """
    if not source_document_ids:
        return
    target = f"`{tables.ESEF_DATABASE}`.`{table}`"
    stage_name = f"_tmp_{table}_{uuid.uuid4().hex}"
    stage = f"`{tables.ESEF_DATABASE}`.`{stage_name}`"
    parameters = {"source_document_ids": tuple(sorted(set(source_document_ids)))}
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} SELECT * FROM {target} "
                "WHERE source_document_id NOT IN %(source_document_ids)s",
                parameters,
            )
            if rows:
                client.execute(
                    f"INSERT INTO {stage} ({', '.join(columns)}) VALUES",
                    [tuple(row[column] for column in columns) for row in rows],
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
```

- [ ] **Step 4: Write `domains_extractor.py`**

```python
"""The esef_domains extractor: registrable domains per archived ESEF filing.

First extractor of the documents-plus-independent-extractors architecture
(spec 2026-09-13): one asset, one table (corpscout.esef_domains, migration
000405), one version constant. It selects documents whose esef_domains rows
are missing or carry another extractor version, streams each archived package
from the object store into a process pool whose workers run the deterministic
website extraction in a killable child, and replaces the table's rows batch
by batch -- no partitions, no shared artifact, no schema version. A version
bump re-runs this extractor over every document and nothing else; the sensor
in domains_sensor.py launches the runs.

No ``from __future__ import annotations``: Dagster inspects the asset's
``context``/resource annotations directly.
"""

import tempfile
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.document_rows import replace_document_rows
from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_TIMED_OUT,
    DocumentDomains,
    DomainsDocument,
    domain_rows,
    extract_package_domains_bounded,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    report_package_object_key,
)
from dagster_v3.defs.esef_filings.website_candidates import WEBSITE_FACT_CONCEPTS

GROUP_NAME = "esef"
ESEF_DOMAINS_POOL = "esef_domains_clickhouse"

# Pool workers are recycled after this many documents (lxml memory growth).
_EXTRACTIONS_PER_POOL_CHILD = 64
# Packages downloaded ahead of the extraction, per worker: bounds the temp dir.
_DOWNLOAD_AHEAD_PER_WORKER = 2
# fxo_ids per IN-list: the native driver inlines parameters into the query
# text, and ClickHouse's max_query_size defaults to 256 KiB.
_ID_CHUNK = 500

ExtractFn = Callable[
    [str, tuple[tuple[str, str], ...], tuple[str, ...], int, str], DocumentDomains
]


class EsefDomainsConfig(dg.Config):
    max_documents: int | None = Field(
        default=None,
        ge=1,
        le=100_000,
        description="Upper bound on documents processed this run (newest period_end first).",
    )
    source_document_ids: list[str] = Field(
        default_factory=list,
        description="Re-extract exactly these documents (fxo_id), stale or not.",
    )
    refresh_existing: bool = Field(
        default=False,
        description="Re-extract every available document, not only the stale ones.",
    )
    workers: int = Field(default=4, ge=1, le=8)
    batch_size: int = Field(
        default=250,
        ge=1,
        le=5000,
        description="Documents per ClickHouse replace; progress survives a failure after each batch.",
    )
    parse_timeout_seconds: int = Field(default=300, ge=60, le=3600)


# --- SQL ------------------------------------------------------------------------


def stale_documents_sql(*, all_available: bool, only_listed: bool) -> str:
    """Available documents -- a package archived by the weekly parse and
    facts in esef_facts -- whose esef_domains rows are missing or carry
    another extractor version (a newer one counts as stale too, like the
    artifact reuse rule). Newest period_end first; `period_end <= today()`
    drops the future-dated index rows (owner ruling 2026-09-11).

    Parameters: extractor_version (unless all_available),
    source_document_ids (when only_listed).
    """
    version_predicate = (
        ""
        if all_available
        else (
            " AND (extracted.source_document_id = '' "
            "OR extracted.extractor_version != %(extractor_version)s)"
        )
    )
    listed_predicate = (
        " AND filings.fxo_id IN %(source_document_ids)s" if only_listed else ""
    )
    return (
        "SELECT filings.fxo_id, filings.package_sha256, filings.lei, filings.period_end "
        f"FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE} AS filings FINAL "
        f"INNER JOIN (SELECT DISTINCT fxo_id FROM {tables.QUALIFIED_ESEF_FACTS_TABLE}) "
        "AS parsed ON parsed.fxo_id = filings.fxo_id "
        "LEFT JOIN (SELECT source_document_id, max(extractor_version) AS extractor_version "
        f"FROM {tables.QUALIFIED_ESEF_DOMAINS_TABLE} GROUP BY source_document_id) "
        "AS extracted ON extracted.source_document_id = filings.fxo_id "
        "WHERE filings.package_sha256 != '' AND filings.period_end <= today()"
        f"{version_predicate}{listed_predicate} "
        "ORDER BY filings.period_end DESC, filings.fxo_id"
    )


def stale_document_count_sql() -> str:
    return (
        "SELECT count() FROM ("
        f"{stale_documents_sql(all_available=False, only_listed=False)})"
    )


def tagged_website_facts_sql() -> str:
    return (
        "SELECT fxo_id, concept_local_name, raw_value "
        f"FROM {tables.QUALIFIED_ESEF_FACTS_TABLE} "
        "WHERE fxo_id IN %(source_document_ids)s "
        "AND concept_local_name IN %(concepts)s AND raw_value != ''"
    )


def email_domains_sql() -> str:
    return (
        "SELECT source_document_id, normalized_value "
        f"FROM {tables.QUALIFIED_ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE} "
        "WHERE source_document_id IN %(source_document_ids)s "
        "AND candidate_kind = 'email'"
    )


# --- selection and inputs ---------------------------------------------------------


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _chunks(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def select_documents(
    clickhouse: ClickhouseResource, config: EsefDomainsConfig
) -> list[DomainsDocument]:
    listed = tuple(sorted(set(config.source_document_ids)))
    sql = stale_documents_sql(
        all_available=config.refresh_existing or bool(listed),
        only_listed=bool(listed),
    )
    parameters: dict[str, object] = {}
    if not (config.refresh_existing or listed):
        parameters["extractor_version"] = ESEF_DOMAINS_EXTRACTOR_VERSION
    if listed:
        parameters["source_document_ids"] = listed
    with clickhouse.get_connection() as client:
        rows = client.execute(sql, parameters)
    documents = []
    for row in rows:
        period_end = _as_date(row[3])
        documents.append(
            DomainsDocument(
                source_document_id=str(row[0]),
                package_sha256=str(row[1]),
                lei=str(row[2]),
                period_end=period_end,
                fiscal_year=period_end.year,
            )
        )
    if config.max_documents is not None:
        documents = documents[: config.max_documents]
    return documents


def load_tagged_website_facts(
    clickhouse: ClickhouseResource, source_document_ids: Sequence[str]
) -> dict[str, tuple[tuple[str, str], ...]]:
    """(concept_local_name, raw_value) per document, deduplicated and sorted."""
    grouped: dict[str, set[tuple[str, str]]] = {}
    concepts = tuple(sorted(WEBSITE_FACT_CONCEPTS))
    with clickhouse.get_connection() as client:
        for chunk in _chunks(list(source_document_ids), _ID_CHUNK):
            rows = client.execute(
                tagged_website_facts_sql(),
                {"source_document_ids": tuple(chunk), "concepts": concepts},
            )
            for document_id, concept, value in rows:
                grouped.setdefault(str(document_id), set()).add(
                    (str(concept), str(value))
                )
    return {
        document_id: tuple(sorted(values)) for document_id, values in grouped.items()
    }


def load_email_domains(
    clickhouse: ClickhouseResource, source_document_ids: Sequence[str]
) -> dict[str, tuple[str, ...]]:
    """The part after '@' of each e-mail candidate, per document."""
    grouped: dict[str, set[str]] = {}
    with clickhouse.get_connection() as client:
        for chunk in _chunks(list(source_document_ids), _ID_CHUNK):
            rows = client.execute(
                email_domains_sql(), {"source_document_ids": tuple(chunk)}
            )
            for document_id, value in rows:
                domain = str(value).rpartition("@")[2].strip().lower()
                if domain:
                    grouped.setdefault(str(document_id), set()).add(domain)
    return {
        document_id: tuple(sorted(domains)) for document_id, domains in grouped.items()
    }


# --- the run ----------------------------------------------------------------------


@dataclass(frozen=True)
class _Outcome:
    document: DomainsDocument
    result: DocumentDomains


def _extract_batch(
    batch: Sequence[DomainsDocument],
    *,
    object_store: ObjectStoreResource,
    executor: ProcessPoolExecutor,
    temp_root: Path,
    tagged: Mapping[str, tuple[tuple[str, str], ...]],
    emails: Mapping[str, tuple[str, ...]],
    timeout_seconds: int,
    max_ahead: int,
    extract: ExtractFn,
) -> list[_Outcome]:
    """Download each package, hand it to the pool, keep at most `max_ahead`
    packages in flight; a download failure is that document's failure."""
    outcomes: list[_Outcome] = []
    pending: deque[tuple[DomainsDocument, Path, Future[DocumentDomains]]] = deque()

    def settle_oldest() -> None:
        document, package_path, future = pending.popleft()
        try:
            result = future.result()
        finally:
            package_path.unlink(missing_ok=True)
        outcomes.append(_Outcome(document, result))

    for document in batch:
        package_path = temp_root / f"{uuid.uuid4().hex}.zip"
        started = perf_counter()
        try:
            body = object_store.read_bytes(
                report_package_object_key(document.package_sha256),
                bucket=ESEF_DOCUMENT_BUCKET,
            )
            digest = sha256(body).hexdigest()
            if digest != document.package_sha256:
                raise ValueError(
                    "package SHA-256 mismatch: "
                    f"expected={document.package_sha256} actual={digest}"
                )
            package_path.write_bytes(body)
        except Exception as exc:  # noqa: BLE001 - a bad or missing package is this document's failure, not the run's
            outcomes.append(
                _Outcome(
                    document,
                    DocumentDomains(
                        STATUS_FAILED,
                        (),
                        frozenset(),
                        f"package download failed: {type(exc).__name__}: {exc}",
                        perf_counter() - started,
                    ),
                )
            )
            continue
        pending.append(
            (
                document,
                package_path,
                executor.submit(
                    extract,
                    str(package_path),
                    tagged.get(document.source_document_id, ()),
                    emails.get(document.source_document_id, ()),
                    timeout_seconds,
                    str(temp_root),
                ),
            )
        )
        while len(pending) >= max_ahead:
            settle_oldest()
    while pending:
        settle_oldest()
    return outcomes


def run_esef_domains_extraction(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    config: EsefDomainsConfig,
    source_run_id: str,
    log: Any,
    extract: ExtractFn = extract_package_domains_bounded,
) -> dict[str, object]:
    wall_started = perf_counter()
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(tables.ESEF_DOMAINS_TABLE,),
    )
    documents = select_documents(clickhouse, config)
    document_ids = [document.source_document_id for document in documents]
    tagged = load_tagged_website_facts(clickhouse, document_ids)
    emails = load_email_domains(clickhouse, document_ids)
    log.info(
        "esef_domains: %d documents to extract (%d with tagged website facts, "
        "%d with e-mail domains), version %s, %d workers",
        len(documents),
        len(tagged),
        len(emails),
        ESEF_DOMAINS_EXTRACTOR_VERSION,
        config.workers,
    )

    counters = {
        status: 0
        for status in (STATUS_OK, STATUS_EMPTY, STATUS_FAILED, STATUS_TIMED_OUT)
    }
    attempted = 0
    rows_written = 0
    extract_seconds = 0.0
    if documents:
        with (
            tempfile.TemporaryDirectory(prefix="esef-domains-") as temp_root_name,
            ProcessPoolExecutor(
                max_workers=config.workers,
                max_tasks_per_child=_EXTRACTIONS_PER_POOL_CHILD,
            ) as executor,
        ):
            temp_root = Path(temp_root_name)
            for batch_index, batch in enumerate(
                _chunks(documents, config.batch_size), start=1
            ):
                outcomes = _extract_batch(
                    batch,
                    object_store=object_store,
                    executor=executor,
                    temp_root=temp_root,
                    tagged=tagged,
                    emails=emails,
                    timeout_seconds=config.parse_timeout_seconds,
                    max_ahead=config.workers * _DOWNLOAD_AHEAD_PER_WORKER,
                    extract=extract,
                )
                extracted_at = datetime.now(UTC)
                rows = [
                    row
                    for outcome in outcomes
                    for row in domain_rows(
                        outcome.document,
                        outcome.result,
                        extractor_version=ESEF_DOMAINS_EXTRACTOR_VERSION,
                        source_run_id=source_run_id,
                        extracted_at=extracted_at,
                    )
                ]
                replace_document_rows(
                    clickhouse,
                    table=tables.ESEF_DOMAINS_TABLE,
                    columns=tables.ESEF_DOMAINS_EXPORT_COLUMNS,
                    source_document_ids=[
                        outcome.document.source_document_id for outcome in outcomes
                    ],
                    rows=rows,
                )
                for outcome in outcomes:
                    counters[outcome.result.status] += 1
                    extract_seconds += outcome.result.seconds
                    if outcome.result.status in (STATUS_FAILED, STATUS_TIMED_OUT):
                        log.warning(
                            "esef_domains: %s %s: %s",
                            outcome.document.source_document_id,
                            outcome.result.status,
                            outcome.result.error_message,
                        )
                attempted += len(outcomes)
                rows_written += len(rows)
                log.info(
                    "esef_domains: batch %d written (%d documents, %d rows; "
                    "%d/%d documents attempted so far)",
                    batch_index,
                    len(outcomes),
                    len(rows),
                    attempted,
                    len(documents),
                )

    return {
        "candidate_document_count": len(documents),
        "attempted_document_count": attempted,
        "processed_document_count": counters[STATUS_OK] + counters[STATUS_EMPTY],
        "documents_without_domains": counters[STATUS_EMPTY],
        "failed_document_count": counters[STATUS_FAILED],
        "timed_out_document_count": counters[STATUS_TIMED_OUT],
        "row_count": rows_written,
        "extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION,
        "workers": config.workers,
        "wall_seconds": round(perf_counter() - wall_started, 3),
        "extract_seconds": round(extract_seconds, 3),
        "table": tables.QUALIFIED_ESEF_DOMAINS_TABLE,
    }


# --- definitions ------------------------------------------------------------------


@dg.asset(
    name="esef_domains_clickhouse",
    deps=[
        dg.AssetKey("esef_filings_clickhouse"),
        dg.AssetDep(
            dg.AssetKey("esef_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            dg.AssetKey("esef_document_contact_candidates_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "clickhouse", "xbrl"},
    pool=ESEF_DOMAINS_POOL,
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
    metadata={"table": tables.QUALIFIED_ESEF_DOMAINS_TABLE},
    description=(
        "Extracts registrable domains from every archived ESEF report package "
        "whose corpscout.esef_domains rows are missing or predate "
        f"{ESEF_DOMAINS_EXTRACTOR_VERSION}, and replaces those documents' rows "
        "batch by batch (spec 2026-09-13: one independent extractor per product)."
    ),
)
def esef_domains_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefDomainsConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    summary = run_esef_domains_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        config=config,
        source_run_id=context.run_id,
        log=context.log,
    )
    return dg.MaterializeResult(metadata=summary)


esef_domains_job = dg.define_asset_job(
    name="esef_domains_job",
    selection=dg.AssetSelection.assets(esef_domains_clickhouse),
    description="One esef_domains extractor run (the stale-documents sensor's job).",
)

defs = dg.Definitions(assets=[esef_domains_clickhouse], jobs=[esef_domains_job])
```

- [ ] **Step 5: Run the tests and the definitions check**

Run: `uv run pytest tests/test_esef_domains_extractor.py tests/test_esef_domains_extraction.py -q`
Expected: all pass (the end-to-end run spawns a pool worker and a child: ~5 s).

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs`
Expected: `All component YAML validated successfully` / definitions load without error.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check src/dagster_v3/defs/esef_filings/domains_extractor.py src/dagster_v3/defs/esef_filings/document_rows.py tests/test_esef_domains_extractor.py
uv run ruff format src/dagster_v3/defs/esef_filings/domains_extractor.py src/dagster_v3/defs/esef_filings/document_rows.py tests/test_esef_domains_extractor.py
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/document_rows.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_extractor.py \
        corpscout/services/dagster_v3/tests/test_esef_domains_extractor.py
git commit -m "feat(esef): esef_domains_clickhouse extractor asset"
```

---

### Task 6: The stale-documents sensor

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_sensor.py`
- Test: `corpscout/services/dagster_v3/tests/test_esef_domains_sensor.py`

**Interfaces:**
- Consumes: Task 5 (`esef_domains_job`, `stale_document_count_sql`), Task 3 (`ESEF_DOMAINS_EXTRACTOR_VERSION`).
- Produces: `should_launch(*, stale_count, in_flight_count, recently_failed_count) -> bool`, sensor `esef_domains_stale_sensor`, constants `ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS = 5000`, `ESEF_DOMAINS_SENSOR_WORKERS = 4`, `ESEF_DOMAINS_FAILURE_COOLDOWN_SECONDS = 3600`.

- [ ] **Step 1: Write the failing tests**

`tests/test_esef_domains_sensor.py` (same fakes technique as `tests/test_esef_stale_weeks_sensor.py`):

```python
"""The sensor that launches esef_domains_job while stale documents exist."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.esef_filings.domains_extraction import ESEF_DOMAINS_EXTRACTOR_VERSION
from dagster_v3.defs.esef_filings.domains_sensor import (
    ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
    ESEF_DOMAINS_SENSOR_WORKERS,
    esef_domains_stale_sensor,
    should_launch,
)
from dagster_v3.definitions import defs as load_project_defs


def test_should_launch_only_with_stale_documents_and_nothing_running_or_failed() -> None:
    assert should_launch(stale_count=3, in_flight_count=0, recently_failed_count=0)
    assert not should_launch(stale_count=0, in_flight_count=0, recently_failed_count=0)
    assert not should_launch(stale_count=3, in_flight_count=1, recently_failed_count=0)
    assert not should_launch(stale_count=3, in_flight_count=0, recently_failed_count=1)


class FakeClient:
    def __init__(self, rows: list[tuple]) -> None:
        self.executed: list[tuple[str, object]] = []
        self.rows = rows

    def execute(self, sql: str, parameters: object = None) -> list[tuple]:
        self.executed.append((sql, parameters))
        return self.rows


class FakeClickhouse:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def _context(client: FakeClient) -> dg.SensorEvaluationContext:
    return dg.build_sensor_context(
        instance=dg.DagsterInstance.ephemeral(),
        resources={"clickhouse": FakeClickhouse(client)},
        definitions=load_project_defs(),
    )


def test_sensor_launches_one_run_when_documents_are_stale() -> None:
    client = FakeClient(rows=[(1234,)])

    execution_data = esef_domains_stale_sensor.evaluate_tick(_context(client))

    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 1
    request = execution_data.run_requests[0]
    assert request.run_config == {
        "ops": {
            "esef_domains_clickhouse": {
                "config": {
                    "max_documents": ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
                    "workers": ESEF_DOMAINS_SENSOR_WORKERS,
                }
            }
        }
    }
    assert request.tags["dagster/priority"] == "5"
    assert request.tags["launched_by"] == "esef_domains_stale_sensor"
    executed_sql, executed_params = client.executed[0]
    assert executed_sql.startswith("SELECT count() FROM (")
    assert executed_params == {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION}


def test_sensor_skips_when_nothing_is_stale() -> None:
    client = FakeClient(rows=[(0,)])

    execution_data = esef_domains_stale_sensor.evaluate_tick(_context(client))

    assert not execution_data.run_requests
    assert execution_data.skip_message is not None
    assert "0 stale" in execution_data.skip_message


def test_sensor_is_registered_and_running_by_default() -> None:
    assert esef_domains_stale_sensor.name == "esef_domains_stale_sensor"
    assert esef_domains_stale_sensor.default_status == dg.DefaultSensorStatus.RUNNING
    assert esef_domains_stale_sensor.minimum_interval_seconds == 1800
    assert esef_domains_stale_sensor.job.name == "esef_domains_job"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_esef_domains_sensor.py -q`
Expected: FAIL with `ModuleNotFoundError: ... domains_sensor`.

- [ ] **Step 3: Write `domains_sensor.py`**

```python
"""Launches the esef_domains extractor while stale documents exist.

Owner rule 2026-09-13: orchestration lives on the server. This sensor is the
extractor's re-run mechanism -- after a deploy that bumps
ESEF_DOMAINS_EXTRACTOR_VERSION, or as the weekly parse archives new packages,
it launches esef_domains_job runs of up to ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS
documents, one at a time, until nothing is stale. Same shape as
stale_weeks_sensor.py (the facts parser's drain).

No ``from __future__ import annotations``: Dagster inspects the sensor's
``context``/resource-parameter annotations directly.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
)
from dagster_v3.defs.esef_filings.domains_extractor import (
    esef_domains_job,
    stale_document_count_sql,
)

ESEF_DOMAINS_JOB_NAME = esef_domains_job.name
ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS = 5000
ESEF_DOMAINS_SENSOR_WORKERS = 4
# After a failed run, wait this long before launching again.
ESEF_DOMAINS_FAILURE_COOLDOWN_SECONDS = 3600

_IN_FLIGHT_STATUSES = (
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.CANCELING,
)


def should_launch(
    *, stale_count: int, in_flight_count: int, recently_failed_count: int
) -> bool:
    """One run at a time: stale documents exist, nothing of this job is in
    flight, and nothing of it failed inside the cooldown."""
    return stale_count > 0 and in_flight_count == 0 and recently_failed_count == 0


@dg.sensor(
    name="esef_domains_stale_sensor",
    job=esef_domains_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Launches esef_domains_job while documents lack corpscout.esef_domains "
        f"rows at {ESEF_DOMAINS_EXTRACTOR_VERSION}, "
        f"{ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS} documents per run, one run at a time."
    ),
)
def esef_domains_stale_sensor(
    context: dg.SensorEvaluationContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.RunRequest | dg.SkipReason]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            stale_document_count_sql(),
            {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION},
        )
    stale_count = int(rows[0][0]) if rows else 0

    in_flight = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_DOMAINS_JOB_NAME,
            statuses=list(_IN_FLIGHT_STATUSES),
        )
    )
    cooldown_cutoff = datetime.now(UTC) - timedelta(
        seconds=ESEF_DOMAINS_FAILURE_COOLDOWN_SECONDS
    )
    recently_failed = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_DOMAINS_JOB_NAME,
            statuses=[dg.DagsterRunStatus.FAILURE],
            updated_after=cooldown_cutoff,
        )
    )
    context.log.info(
        "esef_domains sensor: %d stale documents, %d in flight, %d failed recently",
        stale_count,
        len(in_flight),
        len(recently_failed),
    )
    if not should_launch(
        stale_count=stale_count,
        in_flight_count=len(in_flight),
        recently_failed_count=len(recently_failed),
    ):
        yield dg.SkipReason(
            f"esef_domains: {stale_count} stale documents, {len(in_flight)} run(s) "
            f"in flight, {len(recently_failed)} failed in the last hour"
        )
        return

    yield dg.RunRequest(
        run_key=f"esef_domains:{datetime.now(UTC).isoformat()}",
        run_config={
            "ops": {
                "esef_domains_clickhouse": {
                    "config": {
                        "max_documents": ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
                        "workers": ESEF_DOMAINS_SENSOR_WORKERS,
                    }
                }
            }
        },
        tags={
            "dagster/priority": "5",
            "launched_by": "esef_domains_stale_sensor",
        },
    )


defs = dg.Definitions(sensors=[esef_domains_stale_sensor])
```

- [ ] **Step 4: Run the tests and the definitions check**

Run: `uv run pytest tests/test_esef_domains_sensor.py tests/test_esef_stale_weeks_sensor.py tests/test_schedule_cron_contracts.py -q`
Expected: all pass.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/domains_sensor.py \
        corpscout/services/dagster_v3/tests/test_esef_domains_sensor.py
git commit -m "feat(esef): sensor drains documents lacking esef_domains rows"
```

---

### Task 7: company_serving reads `se_esef_domains`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml` (after the `se_esef_document_contact_candidates` entry, line ~29)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_domains_build.sql` (the `esef_sources` CTE, lines 66-110)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_contact_current_build.sql`
- Test: `corpscout/services/dagster_v3/tests/test_company_serving_dbt.py`

- [ ] **Step 1: Write the failing pins**

Append to `tests/test_company_serving_dbt.py`:

```python
MODELS_DIR = DBT_DIR / "models"


def test_company_domains_build_reads_the_esef_domains_view() -> None:
    sql = (MODELS_DIR / "company_domains_build.sql").read_text(encoding="utf-8")
    assert "source('corpscout', 'se_esef_domains')" in sql
    assert "se_esef_document_contact_candidates" not in sql
    assert "domains.extraction_status = 'ok'" in sql
    assert "domains.registrable_domain != ''" in sql
    assert "JSONExtract(domains.roles_json, 'Array(String)') != ['external_reference']" in sql
    assert "has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website')" in sql
    assert "domains.corroborated = 1 OR domains.evidence_count >= 2" in sql
    assert "0.50" in sql and "0.75" not in sql


def test_company_contact_current_build_excludes_website_rows() -> None:
    sql = (MODELS_DIR / "company_contact_current_build.sql").read_text(encoding="utf-8")
    assert "WHERE candidate_kind != 'website'" in sql


def test_sources_declare_the_esef_domains_view_with_its_asset_key() -> None:
    text = (MODELS_DIR / "sources.yml").read_text(encoding="utf-8")
    assert "- name: se_esef_domains" in text
    assert "asset_key: [esef_domains_clickhouse]" in text
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_company_serving_dbt.py -q`
Expected: the three new tests FAIL; `test_company_serving_dbt_project_parses` passes.

- [ ] **Step 3: Edit the dbt project**

`sources.yml`, after the `se_esef_document_contact_candidates` block:

```yaml
      - name: se_esef_domains
        meta:
          dagster:
            asset_key: [esef_domains_clickhouse]
```

`company_domains_build.sql`: replace the whole `esef_sources AS ( ... ),` CTE (lines 66-110) with:

```sql
esef_sources AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        domains.company_id AS company_id,
        domains.registrable_domain AS root_domain,
        concat('https://', domains.registrable_domain) AS website_url,
        domains.registrable_domain AS website_host,
        'esef_filing' AS source_name,
        toFloat32(multiIf(
            has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website'),
            0.95,
            domains.corroborated = 1 OR domains.evidence_count >= 2,
            0.90,
            0.50
        )) AS source_confidence,
        domains.source_document_id AS source_record_id,
        coalesce(
            nullIf(filings.viewer_url, ''),
            nullIf(filings.report_url, ''),
            nullIf(filings.package_url, ''),
            ''
        ) AS source_url,
        multiIf(
            has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website'),
            'explicit_company_website',
            domains.corroborated = 1 OR domains.evidence_count >= 2,
            'repeated_filing_website',
            'filing_website_mention'
        ) AS confidence_basis,
        toUInt8(0) AS source_suggested_primary,
        domains.resolved_at AS observed_at
    FROM {{ source('corpscout', 'se_esef_domains') }} AS domains
    INNER JOIN companies
        ON companies.company_id = domains.company_id
    LEFT ANY JOIN {{ source('corpscout', 'se_esef_filings') }} AS filings
        ON filings.fxo_id = domains.source_document_id
    -- The extractor (dagster_v3.defs.esef_filings.domains_extraction) writes one
    -- marker row per document without domains (registrable_domain '') and tags a
    -- domain it saw only in a third-party referral as external_reference; neither
    -- is a company website. A tagged WebsitesOfLegalEntity fact, a known e-mail
    -- domain or a repeated mention (corroborated = 1) lifts a mention to 0.90.
    WHERE domains.extraction_status = 'ok'
      AND domains.registrable_domain != ''
      AND JSONExtract(domains.roles_json, 'Array(String)') != ['external_reference']
),
```

`company_contact_current_build.sql`: insert `WHERE candidate_kind != 'website'` between the `FROM` line and the `QUALIFY` clause:

```sql
FROM {{ source('corpscout', 'se_esef_document_contact_candidates') }}
-- Websites are domains now (company_domains_build reads se_esef_domains); the
-- artifact parser keeps writing website candidate rows until the contact
-- extractor moves too (spec 2026-09-13, section 8).
WHERE candidate_kind != 'website'
QUALIFY row_number() OVER (
```

- [ ] **Step 4: Run the dbt tests and the definitions check**

Run: `uv run pytest tests/test_company_serving_dbt.py tests/test_company_serving.py -q`
Expected: all pass (`dbt parse` validates the new source reference).

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs`
Expected: success. If the check reports a stale dbt manifest / defs state, run `uv run dg utils refresh-defs-state`, re-run the check, and report in the task report which files (if any tracked) it changed — do not commit generated state that is not already tracked.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_domains_build.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_contact_current_build.sql \
        corpscout/services/dagster_v3/tests/test_company_serving_dbt.py
git commit -m "feat(company_serving): domains come from se_esef_domains, contacts drop website rows"
```

---

### Task 8: Backoffice ESEF tab shows the extracted websites

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-company-esef.server.ts` (queries at lines 73-78; row types near line 135; public types near line 197; `SeCompanyEsefDetail`; `loadSeCompanyEsef` at lines ~250-306)
- Modify: `corpscout/services/backoffice/app/routes/admin-se-company-esef.tsx` (the "Contact candidates" card at lines ~214-235)
- Test: `corpscout/services/backoffice/tests/se-company-esef.server.test.ts`

Run commands from `corpscout/services/backoffice`: `pnpm vitest run tests/se-company-esef.server.test.ts`, `pnpm typecheck`.

- [ ] **Step 1: Write the failing tests**

In `tests/se-company-esef.server.test.ts`: add `ESEF_TAB_DOMAINS_SQL` to the import list; in the "SQL contracts" test add

```ts
    expect(ESEF_TAB_DOMAINS_SQL).toContain("FROM corpscout.se_esef_domains");
    expect(ESEF_TAB_DOMAINS_SQL).toContain("company_id = {companyId:String}");
    expect(ESEF_TAB_DOMAINS_SQL).toContain("extraction_status = 'ok'");
    expect(ESEF_TAB_DOMAINS_SQL).not.toContain("FINAL");
    // Websites are domains now (se_esef_domains); the contacts card shows the rest.
    expect(ESEF_TAB_CONTACTS_SQL).toContain("candidate_kind != 'website'");
```

and in the `loadSeCompanyEsef` mapping test (the chain of `mockResolvedValueOnce` at lines ~69-126, one per query in `Promise.all` order) add one more mocked response for the domains query at the position matching the new `Promise.all` order (append it LAST — the domains query is added as the last element), with

```ts
      .mockResolvedValueOnce([
        {
          fiscal_year: 2024,
          registrable_domain: "handelsbanken.com",
          roles_json: '["company_website","report_disclosure"]',
          evidence_count: 19,
          corroborated: 1,
        },
      ])
```

and assert

```ts
    expect(detail?.domains).toEqual([
      {
        fiscalYear: 2024,
        registrableDomain: "handelsbanken.com",
        roles: ["company_website", "report_disclosure"],
        evidenceCount: 19,
        corroborated: true,
      },
    ]);
```

Read the existing test first: keep the order of the mocked responses aligned with the `Promise.all` order in the loader.

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run tests/se-company-esef.server.test.ts`
Expected: FAIL (`ESEF_TAB_DOMAINS_SQL` undefined).

- [ ] **Step 3: Implement the server module**

Add after `ESEF_TAB_CONTACTS_SQL` (and add `AND candidate_kind != 'website'` to that query's WHERE):

```ts
export const ESEF_TAB_CONTACTS_SQL = `
SELECT
  fiscal_year, candidate_kind, normalized_value, registrable_domain
FROM corpscout.se_esef_document_contact_candidates
WHERE company_id = {companyId:String}
  AND candidate_kind != 'website'
ORDER BY fiscal_year DESC, candidate_kind, normalized_value`;

// The esef_domains extractor's rows (se_esef_domains): one per filing and
// registrable domain. Marker rows (registrable_domain '', a document without
// domains or a failed extraction) are filtered out here.
export const ESEF_TAB_DOMAINS_SQL = `
SELECT
  fiscal_year, registrable_domain, roles_json, evidence_count, corroborated
FROM corpscout.se_esef_domains
WHERE company_id = {companyId:String}
  AND extraction_status = 'ok'
  AND registrable_domain != ''
ORDER BY fiscal_year DESC, registrable_domain`;
```

Row type and public type:

```ts
interface EsefTabDomainQueryRow {
  fiscal_year: number;
  registrable_domain: string;
  roles_json: string;
  evidence_count: number;
  corroborated: number;
}

export interface EsefTabDomain {
  fiscalYear: number;
  registrableDomain: string;
  roles: string[];
  evidenceCount: number;
  corroborated: boolean;
}
```

`SeCompanyEsefDetail` gains `domains: EsefTabDomain[];`. In `loadSeCompanyEsef`, append `chQuery<EsefTabDomainQueryRow>(ESEF_TAB_DOMAINS_SQL, params)` as the LAST element of the `Promise.all` array (destructure as `domains`), and add to the returned object:

```ts
    domains: domains.map((r) => ({
      fiscalYear: Number(r.fiscal_year),
      registrableDomain: r.registrable_domain,
      roles: parseRoles(r.roles_json),
      evidenceCount: Number(r.evidence_count),
      corroborated: Number(r.corroborated) === 1,
    })),
```

with a module-level helper:

```ts
function parseRoles(json: string): string[] {
  try {
    const parsed: unknown = JSON.parse(json);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}
```

- [ ] **Step 4: Implement the route card**

In `admin-se-company-esef.tsx`, directly before the `{detail.contacts.length > 0 ? (` block, add:

```tsx
      {detail.domains.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Websites</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="flex flex-col gap-1">
              {detail.domains.map((domain) => (
                <li
                  key={`${domain.fiscalYear}-${domain.registrableDomain}`}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span>{domain.registrableDomain}</span>
                  {domain.roles.map((role) => (
                    <Badge key={role} variant="outline">
                      {role}
                    </Badge>
                  ))}
                  <Badge variant="outline">fiscal {domain.fiscalYear}</Badge>
                  <Badge variant="outline">
                    {domain.evidenceCount} evidence
                    {domain.corroborated ? ", corroborated" : ""}
                  </Badge>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}
```

- [ ] **Step 5: Run the tests and the type check**

Run: `pnpm vitest run tests/se-company-esef.server.test.ts && pnpm typecheck`
Expected: tests pass; typecheck clean.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-company-esef.server.ts \
        corpscout/services/backoffice/app/routes/admin-se-company-esef.tsx \
        corpscout/services/backoffice/tests/se-company-esef.server.test.ts
git commit -m "feat(backoffice): ESEF tab lists the extracted websites from se_esef_domains"
```

---

### Task 9: Docs and whole-module verification

**Files:**
- Create: `corpscout/services/dagster_v3/docs/esef-extractors.md`
- Modify: `corpscout/services/dagster_v3/docs/esef-ixbrl-segment-parser.md` (a short pointer paragraph at the top)
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-13-esef-extractors-and-domains-design.md` (a "Status" line under the title)

- [ ] **Step 1: Write `docs/esef-extractors.md`**

```markdown
# ESEF extractors

Spec: `docs/superpowers/specs/2026-09-13-esef-extractors-and-domains-design.md`.

ESEF is a document store plus independent extractors. `corpscout.esef_filings` indexes the
filings; the weekly parse (`esef_document_artifacts_s3`) archives every report package in the
`source-esef-filings` bucket under `report_package_object_key(package_sha256)` and produces the
facts (`esef_facts`, versioned by `ARTIFACT_SCHEMA_VERSION`). Every other product is an
extractor: one asset, one table, one version constant, its own sensor.

## The pattern (`esef_domains`)

| Piece | Where |
| --- | --- |
| Version | `domains_extraction.ESEF_DOMAINS_EXTRACTOR_VERSION` |
| Per-document function (light, runs in a spawned child) | `defs/esef_filings/domains_extraction.py` |
| Package opener (light) | `defs/esef_filings/report_package.py` |
| Timeout wrapper (stdlib) | `defs/common/child_timeout.py` |
| Asset + job + selection SQL | `defs/esef_filings/domains_extractor.py` (`esef_domains_clickhouse`, `esef_domains_job`) |
| Writer (stage + EXCHANGE, per document set) | `defs/esef_filings/document_rows.py` |
| Sensor | `defs/esef_filings/domains_sensor.py` (`esef_domains_stale_sensor`, 30 min, one run of ≤ 5,000 documents at a time) |
| Table + view | migration 000405: `corpscout.esef_domains`, `corpscout.se_esef_domains` |

Stale = an available document (package archived, facts present, `period_end <= today()`)
whose rows are missing or carry another `extractor_version`. Every attempted document leaves a
row: real domains, or one marker row (`registrable_domain = ''`, `extraction_status` `empty` /
`failed` / `timed_out` + `error_message`), so a document is attempted once per version.

## Operating it

- **Re-run everything:** bump the version constant, deploy; the sensor drains the stale set.
- **Re-run some documents:** launch `esef_domains_clickhouse` with
  `source_document_ids: [fxo_id, ...]` (stale or not) or `refresh_existing: true`.
- **Failed / timed-out documents** stay at their status until a version bump or an explicit
  re-run (look at `error_message`); a run interrupted mid-way leaves its unwritten documents
  stale and the next run retries them.
- **Throughput:** lxml only, ~1-3 s per document per worker plus the child's import cost;
  4 workers ≈ 2-3 h for the whole corpus. Packages are downloaded at most `2 × workers` ahead.

## Adding an extractor

1. A light module with the pure per-document function and a `<NAME>_EXTRACTOR_VERSION`.
2. A migration with the table (`source_document_id`, `package_sha256`, `lei`, `period_end`,
   `fiscal_year`, `extraction_status`, `extractor_version`, `source_run_id`, `extracted_at`,
   `resolved_at DEFAULT now64(3)` plus the product's columns) and its `se_esef_<table>` view
   (`tables.SE_ESEF_VIEWS` + `country_views.build_se_esef_view_sql`).
3. An asset that selects stale documents (copy `stale_documents_sql`), runs the function through
   `run_in_child_with_timeout` in a pool, and writes with `replace_document_rows`.
4. A sensor (copy `domains_sensor.py`).
5. Consumers read the view, never the artifact.
```

- [ ] **Step 2: Pointer in the parser doc and status in the spec**

At the top of `docs/esef-ixbrl-segment-parser.md` (after the title) add:

```markdown
> Since 2026-09-13 the artifact parser is the *facts* extractor only in spirit: new products
> are independent extractors (see `esef-extractors.md`); domains come from `esef_domains`, and
> the website rows of `esef_document_contact_candidates` are no longer read by consumers.
```

Under the spec's title add: `**Status (2026-09-13):** implemented on branch \`esef-domains-extractor\` (plan \`docs/superpowers/plans/2026-09-13-esef-domains-extractor.md\`); rollout pending the owner's migration + deploy.`

- [ ] **Step 3: Whole-module verification**

Run (from `corpscout/services/dagster_v3`):

```bash
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs
uv run pytest tests -q -m "not integration" -x
uv run ruff check src/dagster_v3/defs/common/child_timeout.py src/dagster_v3/defs/esef_filings/report_package.py src/dagster_v3/defs/esef_filings/domains_extraction.py src/dagster_v3/defs/esef_filings/domains_extractor.py src/dagster_v3/defs/esef_filings/domains_sensor.py src/dagster_v3/defs/esef_filings/document_rows.py src/dagster_v3/defs/esef_filings/website_candidates.py src/dagster_v3/defs/esef_filings/segment_parser.py
```

Expected: definitions load; the suite is green (report the counts and the wall time); ruff clean. Also, from `corpscout/services/backoffice`: `pnpm vitest run tests/se-company-esef.server.test.ts` green.

- [ ] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/docs/esef-extractors.md \
        corpscout/services/dagster_v3/docs/esef-ixbrl-segment-parser.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-13-esef-extractors-and-domains-design.md
git commit -m "docs(esef): the extractor pattern and the esef_domains slice"
```

---

## Rollout (executed by the controller on the owner's instruction "you should do it", 2026-09-13)

1. [x] (2026-09-13: 000404 had gone to the SE financial track on prod; renumbered to 000405, free on main and every branch.) Re-check the migration number against main and the prod ledger (`ls corpscout/clickhouse/migrations | tail -3`; `SELECT version FROM corpscout.schema_migrations`); renumber before merging if 000405 is taken.
2. [x] (applied 20:05Z, `405/u corpscout_esef_domains (5.0s)`; ledger 405 clean; table ReplacingMergeTree with 20 columns, view answers.) `make clickhouse-migrate-up-one` (000405); verify `EXISTS TABLE corpscout.esef_domains` and `EXISTS VIEW corpscout.se_esef_domains`.
3. [x] (merged 68e90a680; deployed 20:12Z from a pristine worktree at the merge commit, ansible ok=35 changed=15 failed=0; host defs-state copies read `se_esef_domains`; sensor RUNNING; asset in group `esef`.) Deploy dagster_v3 from main: on the host `uv run --frozen --no-sync dg utils refresh-defs-state` (dbt source/model change), then `cd corpscout/services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml`; confirm `esef_domains_clickhouse` and `esef_domains_stale_sensor` (RUNNING) appear in the UI. This deploy also ships the website-extraction fix (636f30042) to the weekly parse.
4. [x] (drain DONE 09:39Z 2026-09-14: four sensor runs b2db8dfb, c3444335, 4583e020, ee1171d4 all SUCCESS, ~13.5 h in total, faster once the schema-4 re-parse finished around 04:30Z; one timeout (Gjensidige 2022, NO) recovered by a `retry_failed` run at 900 s; final table: 15,824 documents, 14,071 with domains, 1,753 without, 0 failed, 62,809 domain rows, 34 MiB on disk. First tick 20:12:54Z launched run b2db8dfb; batches 1–2: 500 documents, 0 failed / 0 timed out, largest evidence 107 KB; ~14 min per 250-document batch on the shared host, so the whole drain takes ~15 h, ending ~11:30Z 2026-09-14. No tagged website facts exist anywhere in the corpus — expected, not a bug.) Watch the first sensor run (≤ 5,000 documents, 4 workers): metadata `failed_document_count` / `timed_out_document_count` near zero; `SELECT extraction_status, count() FROM corpscout.esef_domains GROUP BY 1`.
5. [x] (09:40Z: `banken.com` / `bankenfonder.se` = 0 rows in the whole table; Handelsbanken 2022 report clean, with `svanen.se` and `ipcc.ch` tagged `external_reference`. OPEN: the 2021 and 2023 reports still carry them under other roles. 2021 `svanen.se` is `unknown` because two mentions of 'Läs mer på svanen.se/spararen' count as corroboration and block the referral tag. 2021 and 2023 `ipcc.ch` is `report_disclosure` because 'Se fullständig rapport här: ipcc.ch/sr15/' matches the role keyword before the referral check. Across Sweden, 44 company-domain pairs are tagged referral in one filing but served from another and never tagged as the company's own. Fix options (extraction v2 vs a serving cross-filing rule) go to the owner.) After the drain (`stale_document_count_sql()` → 0): verify Handelsbanken (5020077862) in `se_esef_domains` — `handelsbanken.com` with `company_website`, `handelsbanken.se`, `handelsbankenfonder.se`, `svanen.se`/`ipcc.ch` only as `external_reference`; `SELECT count() FROM corpscout.esef_domains WHERE registrable_domain IN ('banken.com', 'bankenfonder.se')` → 0.
6. [x] (run d9376772, launched 09:40Z 2026-09-14, SUCCESS 11:15Z: company_domains 10,894 rows, contacts 4,592, source links 23.67M, presence 12.47M. CORRECTION: `corpscout.company_domain_current` is a stale legacy table last changed 2026-08-10 that the publish does not replace; the publish replaces `company_domains` from `company_domains_build`, so verify there with `is_active = 1`, or in `company_domain_current_build`. Handelsbanken after the publish: `banken.com` and `bankenfonder.se` inactive (both were active at 0.75); active are `handelsbanken.se` (primary, 1.0, ESEF + Wikidata), `handelsbanken.com` 0.95, `handelsbankenfonder.se` 0.95, `svanen.se` 0.90 and `ipcc.ch` 0.50, the last two being the open referral gap. Sweden: active ESEF-sourced rows 2,105 across 394 companies (were 2,589 across 397), 530 ESEF rows now inactive; `ipcc.ch` active for 6 companies, `svanen.se` for 1; `deloitte.com` still served for 4 companies through footnote citations ("1. https://www.deloitte.com/…", "Källa:"), the same referral gap.) company_serving build + publish (the serving track's launch); verify against the current, active domains: `SELECT root_domain FROM corpscout.company_domain_current WHERE country_code = 'SE' AND company_id = '5020077862'` (or `company_domains FINAL` with `is_active = 1`) lists no `banken.com` / `bankenfonder.se`. Not plain `company_domains`: it keeps now-inactive rows with their old `source_names`.
7. [x] (data check 2026-09-14: the card's `ESEF_TAB_DOMAINS_SQL`, run verbatim on prod for 5020077862, returns 16 rows for 2021–2024, one per fiscal year and domain, with roles, evidence counts and the corroborated flag; rendering is covered by the backoffice vitest suite and typecheck. Not opened in a browser.) Backoffice: open the ESEF tab of 5020077862 locally, confirm the Websites card.
