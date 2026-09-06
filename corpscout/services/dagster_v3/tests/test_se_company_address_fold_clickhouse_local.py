"""The address entity's fold batch SQL against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle (spec 2026-09-06 sections 5 and 6, task 5 brief):

1. `normalized_watermarks_sql` with FINAL: a company with an `ok` version then a
   `no_address` version of the same (company_id, source, slot) key reports `publishable =
   0` and the newer `normalized_at` -- ReplacingMergeTree(normalized_at) FINAL collapses to
   the newest version, and `no_address` is not in PUBLISHABLE_STATUSES.
2. `stale_companies_sql` selects a company whose active row sits on another geocode
   policy/reference/normalizer version, and excludes a company whose only such row is
   withdrawn, a foreign row, and a `legacy_adopted_v1` row -- even though all four are
   equally "stale" by policy/reference/normalizer.
3. `main_insert_sql` accepts the exact 31-value tuple `PublishedAddress.as_tuple` builds
   (arrays as lists, FixedString(64) keys, Nullable(Float64), Nullable(DateTime64)), and
   `current_main_rows_sql` reads it back into `main_row_from_row` equal to the original
   minus `fold_version`/`source_run_id`; `history_insert_sql` accepts the same tuple.
4. `hidden_keys_sql` with FINAL: a hide rule followed by its `removed = 1` release leaves
   no key -- ReplacingMergeTree(decided_at) FINAL keeps the release, and the `removed = 0`
   filter then excludes it.

None of these four statements contains a LEFT JOIN, so unlike the normalize/geocode
clickhouse-local suites this file does not need the join_use_nulls parametrization -- there
is no ifNull-guarded join column here for that setting to affect.
"""

import dataclasses
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import batch, tables
from dagster_v3.defs.se_company.address.fold import FOLD_VERSION, PublishedAddress
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY
from dagster_v3.defs.sweden_company.geocode_store import LEGACY_ADOPTED_POLICY_VERSION
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000383_corpscout_se_company_address_normalized.up.sql",
    "000384_corpscout_se_company_address_v2.up.sql",
    "000385_corpscout_se_company_address_history.up.sql",
    "000386_corpscout_se_company_address_rule.up.sql",
    "000387_corpscout_se_company_address_precedence.up.sql",
)

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
STALE_POLICY = "se-address-resolution-policy-v6"
OLD_NORMALIZER = "se-address-normalizer-v0"
CURRENT_REFERENCE = "ref-current"
STALE_REFERENCE = "ref-old"

T0 = datetime(2026, 9, 6, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 6, 9, 0, tzinfo=UTC)
GEOCODED_AT = datetime(2026, 9, 6, 9, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 6, 10, 0, tzinfo=UTC)
T_HIDE0 = datetime(2026, 9, 6, 8, 0, tzinfo=UTC)
T_HIDE1 = datetime(2026, 9, 6, 9, 0, tzinfo=UTC)

NW = "5561000001"
STALE_ACTIVE = "5562000001"
STALE_WITHDRAWN = "5562000002"
STALE_FOREIGN = "5562000003"
STALE_LEGACY = "5562000004"
MAIN_RW = "5563000001"
HIDE_CO = "5564000001"


def _fixed(value: str) -> str:
    """Pads to exactly FixedString(64): the test's own keys never need trimming on the way
    back out, so a round trip through the store never has to worry about null padding."""
    return value.ljust(64, "0")


def _literal(value: Any) -> str:
    """A parameter value rendered the way clickhouse-driver renders it: a Python `list`
    becomes a `[...]` array literal (what an Array(...) column needs) and a `tuple` a
    parenthesized `(...)` one (an INSERT ... VALUES row)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _bind(sql: str, **params: Any) -> str:
    """Render batch.py's %(name)s parameters the way clickhouse-driver does."""
    rendered = sql
    for name, value in params.items():
        rendered = rendered.replace(f"%({name})s", _literal(value))
    assert "%(" not in rendered, rendered
    return rendered


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    return statements


def _run(statements: list[str]) -> list[str]:
    script = ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    """Split _run()'s flat line list on the `SELECT '@@name'` markers in the script."""
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


NORMALIZED_DEFAULTS: dict[str, Any] = dict(
    kind="postal", care_of=None, box=None, street_name="Storgatan", house_number="5", unit=None,
    postal_code="11122", city="Stockholm", country_code="SE",
    normalized_address="Storgatan 5, 111 22 Stockholm", parse_notes="",
)


def _normalized_insert(
    company_id: str, source: str, slot: str, *, normalized_id: str, suggestion_id: str,
    suggested_at: datetime, normalized_at: datetime, parse_status: str, address_key: str,
    normalizer_version: str = NORMALIZER_VERSION, **overrides: Any,
) -> str:
    values = dict(NORMALIZED_DEFAULTS)
    values.update(overrides)
    values.update(
        company_id=company_id, source=source, slot=slot, normalized_id=normalized_id,
        suggestion_id=suggestion_id, suggested_at=suggested_at, address_key=address_key,
        parse_status=parse_status, normalizer_version=normalizer_version, normalized_at=normalized_at,
    )
    ordered = tuple(values[column] for column in tables.NORMALIZED_COLUMNS)
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES {_literal(ordered)}"
    )


def published_address(company_id: str, address_key_value: str, **overrides: Any) -> PublishedAddress:
    values: dict[str, Any] = dict(
        company_id=company_id, address_key=address_key_value, care_of=None, box=None,
        street_name="Storgatan", house_number="5", unit=None, postal_code="11122", city="Stockholm",
        country_code="SE", normalized_address="Storgatan 5, 111 22 Stockholm",
        kinds=("postal",), sources=("scb",), slots=("postal",), normalized_ids=(_fixed("scb-postal"),),
        text_source="scb", active=1, inactive_reason="",
        latitude=59.25, longitude=18.5, geocode_status="matched_exact", geocode_method="raw_full_exact",
        geocode_confidence=0.5, geocode_precision="address", geocode_policy=POLICY,
        geocode_reference=CURRENT_REFERENCE, geocoded_at=GEOCODED_AT, normalizer_version=NORMALIZER_VERSION,
        fold_version=FOLD_VERSION, source_run_id="run-1",
    )
    values.update(overrides)
    return PublishedAddress(**values)


def _main_insert(row: PublishedAddress, folded_at: datetime) -> str:
    return f"{batch.main_insert_sql()} {_literal(row.as_tuple(folded_at))}"


def _history_insert(row: PublishedAddress, folded_at: datetime) -> str:
    return f"{batch.history_insert_sql()} {_literal(row.as_tuple(folded_at))}"


def _rule_insert(
    company_id: str, address_key_value: str, action: str, removed: int, decided_by: str,
    note: str, decided_at: datetime,
) -> str:
    row = (company_id, address_key_value, action, removed, decided_by, note, decided_at)
    return (
        f"INSERT INTO {tables.QUALIFIED_RULE_TABLE} "
        f"({', '.join(tables.RULE_COLUMNS)}) VALUES {_literal(row)}"
    )


def _main_row_from_json(obj: dict[str, Any]) -> PublishedAddress:
    """The JSONEachRow answer to current_main_rows_sql(), converted back into the exact row
    shape main_row_from_row expects: arrays already parse as Python lists, and only the
    DateTime64 column needs converting back to an aware datetime."""
    values = dict(obj)
    if values["geocoded_at"] is not None:
        values["geocoded_at"] = datetime.strptime(values["geocoded_at"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    row = [values[column] for column in batch.MAIN_COMPARE_COLUMNS]
    return batch.main_row_from_row(row)


MAIN_ROW = published_address(MAIN_RW, _fixed("main-row-key"))


def _statements() -> list[str]:
    watermark_query = _bind(batch.normalized_watermarks_sql(), company_ids=[NW])

    stale_active = published_address(
        STALE_ACTIVE, _fixed("stale-active"),
        geocode_policy=STALE_POLICY, geocode_reference=STALE_REFERENCE, normalizer_version=OLD_NORMALIZER,
    )
    stale_withdrawn = published_address(
        STALE_WITHDRAWN, _fixed("stale-withdrawn"), active=0, inactive_reason="withdrawn",
        geocode_policy=STALE_POLICY, geocode_reference=STALE_REFERENCE, normalizer_version=OLD_NORMALIZER,
    )
    stale_foreign = published_address(
        STALE_FOREIGN, _fixed("stale-foreign"), geocode_status="foreign",
        geocode_policy=STALE_POLICY, geocode_reference=STALE_REFERENCE, normalizer_version=OLD_NORMALIZER,
        latitude=None, longitude=None, geocode_method="", geocode_confidence=None, geocode_precision="",
    )
    stale_legacy = published_address(
        STALE_LEGACY, _fixed("stale-legacy"),
        geocode_policy=LEGACY_ADOPTED_POLICY_VERSION, geocode_reference="", normalizer_version=OLD_NORMALIZER,
    )
    stale_query = (
        _bind(
            batch.stale_companies_sql(),
            company_ids=[STALE_ACTIVE, STALE_WITHDRAWN, STALE_FOREIGN, STALE_LEGACY],
            policy=POLICY, reference=CURRENT_REFERENCE, normalizer=NORMALIZER_VERSION,
        )
        + " ORDER BY company_id"
    )

    main_query = _bind(batch.current_main_rows_sql(), company_ids=[MAIN_RW]) + " FORMAT JSONEachRow"

    return [
        *_schema_statements(),
        # Claim 1: normalized_watermarks_sql under FINAL.
        _normalized_insert(
            NW, "scb", "postal", normalized_id=_fixed("nrm-ok"), suggestion_id=_fixed("sug-ok"),
            suggested_at=T0, normalized_at=T0, parse_status="ok", address_key=_fixed("addr-ok"),
        ),
        "SELECT '@@watermark_ok'",
        watermark_query,
        _normalized_insert(
            NW, "scb", "postal", normalized_id=_fixed("nrm-no-address"), suggestion_id=_fixed("sug-no-address"),
            suggested_at=T1, normalized_at=T1, parse_status="no_address", address_key=_fixed("addr-no-address"),
            care_of=None, box=None, street_name=None, house_number=None, unit=None, postal_code=None, city=None,
            normalized_address="",
        ),
        "SELECT '@@watermark_no_address'",
        watermark_query,
        # Claim 2: stale_companies_sql excludes withdrawn, foreign and legacy_adopted_v1.
        _main_insert(stale_active, FOLDED_AT),
        _main_insert(stale_withdrawn, FOLDED_AT),
        _main_insert(stale_foreign, FOLDED_AT),
        _main_insert(stale_legacy, FOLDED_AT),
        "SELECT '@@stale'",
        stale_query,
        # Claim 3: main_insert_sql / history_insert_sql accept the full 31-value tuple, and
        # current_main_rows_sql reads it back unchanged (minus fold_version/source_run_id).
        _main_insert(MAIN_ROW, FOLDED_AT),
        _history_insert(MAIN_ROW, FOLDED_AT),
        "SELECT '@@main_row'",
        main_query,
        "SELECT '@@history_count'",
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE company_id = {_literal(MAIN_RW)}",
        # Claim 4: hidden_keys_sql under FINAL, before and after the rule's release.
        _rule_insert(HIDE_CO, _fixed("hide-key"), "hide", 0, "reviewer", "", T_HIDE0),
        "SELECT '@@hidden_before_release'",
        _bind(batch.hidden_keys_sql(), company_ids=[HIDE_CO]),
        _rule_insert(HIDE_CO, _fixed("hide-key"), "hide", 1, "reviewer", "released", T_HIDE1),
        "SELECT '@@hidden_after_release'",
        _bind(batch.hidden_keys_sql(), company_ids=[HIDE_CO]),
    ]


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    return _sections(_run(_statements()))


def test_normalized_watermarks_final_reports_no_address_as_unpublishable(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["watermark_ok"] == [[NW, "2026-09-06 08:00:00.000", "1"]]
    # The no_address version is newer (normalized_at T1): FINAL now answers with that
    # version alone, and no_address is not in PUBLISHABLE_STATUSES.
    assert sections["watermark_no_address"] == [[NW, "2026-09-06 09:00:00.000", "0"]]


def test_stale_companies_excludes_withdrawn_foreign_and_legacy_adopted(
    sections: dict[str, list[list[str]]],
) -> None:
    # All four rows sit on the same stale policy/reference/normalizer_version; only the
    # plain active one is selected.
    assert sections["stale"] == [[STALE_ACTIVE]]


def test_main_and_history_insert_accept_the_full_tuple_and_round_trip(
    sections: dict[str, list[list[str]]],
) -> None:
    [row_line] = sections["main_row"]
    parsed = _main_row_from_json(json.loads(row_line[0]))
    assert parsed == dataclasses.replace(MAIN_ROW, fold_version="", source_run_id="")
    assert sections["history_count"] == [["1"]]


def test_a_hide_rule_and_its_release_round_trip_through_final(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["hidden_before_release"] == [[HIDE_CO, _fixed("hide-key")]]
    assert sections["hidden_after_release"] == []
