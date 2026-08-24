"""Executes the address artifact SELECTs, the change scan, the geocode lookup and the
final's read-back against the migrations' DDL in a disposable clickhouse-local. Proves the
SQL runs on the deployed ClickHouse version -- substring tests cannot.

Two companies. ALPHA has both sources: a Bolagsverket 'postal' row and an SCB
'visiting_or_postal' row for the SAME street, which is the ordinary Swedish case and the
one that shows why both rows are published (the address types differ, so the keys differ).
Its Bolagsverket observation is linked and geocoded; its SCB observation is linked but NOT
geocoded, which is the LEFT JOIN miss build_geocodes_sql has to gate rather than ifNull.
BETA has only an SCB row and no address identity at all, so it exercises the "no link"
path through the INNER JOIN.

The publish sequence (stage -> validate -> LEFT ANTI JOIN copy -> drop stage) mirrors
publish_with_stage(..., new_versions_only=True); common.py has no separate SQL-string
builder for that anti-join -- it is inlined in the function -- so _publish_pass copies the
shape verbatim, exactly as the info harness does.

Set replacement is proved as a STORAGE round-trip here (the rules' decision is a table test
in tests/test_se_company_address_rules.py): a key published is_current = true, then
republished is_current = false with a newer resolved_at, disappears from
`FINAL ... WHERE is_current`; published true again with a newer stamp still, it comes back.

The whole script runs twice, once under default settings and once with
`SET join_use_nulls = 1` prepended: every LEFT JOIN miss in the scan and in the geocode
lookup is read through ifNull, so both settings must answer identically.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address import (
    INSERT_COLUMNS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_geocodes_sql,
    build_published_rows_sql,
)
from dagster_v3.defs.se_company.bolagsverket import (
    SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
)
from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_ADDRESS_SCB_COLUMNS,
    SE_COMPANY_ADDRESS_SCB_SQL,
)
from tests.se_company_ddl import declared_columns
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# Every migration that creates or alters one of NEEDED_TABLES, in migration order. The
# address history table (se_company_addresses) is deliberately absent: the artifacts read
# the _current snapshot only.
MIGRATIONS = (
    "000256_corpscout_se_company_address_current_snapshot.up.sql",
    "000265_corpscout_se_company_address_normalization.up.sql",
    "000273_corpscout_se_company_canonical_addresses.up.sql",
    "000274_corpscout_se_shared_addresses.up.sql",
    "000275_corpscout_se_address_geocodes_current.up.sql",
    "000277_corpscout_se_address_geocode_spread.up.sql",
    "000278_corpscout_se_address_components.up.sql",
    "000307_corpscout_se_company_address.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_company_addresses_current",
    "se_company_address_members_current",
    "se_company_address_links_current",
    "se_addresses_current",
    "se_address_geocodes_current",
    "se_company_address_bolagsverket",
    "se_company_address_scb",
    "se_company_address",
    "se_company_address_correction",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)
# 000256 builds se_company_addresses_current under a staging name and RENAMEs it -- a
# rename the per-statement filter cannot replay -- so its CREATE is taken and renamed here.
# Nothing is hand-copied: a column added to the deployed table lands here on the next run.
STAGING_NAME = "se_company_addresses_current_snapshot_000256"

RUN_ID = "fixture-run-1"
ALPHA = "5565200028"
BETA = "196408233412"  # sole trader: 12-digit id, admitted by the has_company CHECK
T_SEED = _literal(datetime(2026, 8, 1, tzinfo=UTC))
T_GEOCODE = _literal(datetime(2026, 8, 2, tzinfo=UTC))
# DateTime64(3) has millisecond resolution and consecutive statements can share one, so
# every point where a stamp must be strictly newer than the previous one is separated by a
# real pause. FORMAT Null keeps sleep's own row out of the marked-section stream.
SETTLE = "SELECT sleep(0.05) FORMAT Null;\n"
ALPHA_FP_BOLAGSVERKET, ALPHA_FP_SCB, BETA_FP = "a" * 64, "b" * 64, "c" * 64
CANONICAL_KEY, ADDRESS_ID = "d" * 64, "e" * 64
CANONICAL_KEY_SCB, ADDRESS_ID_SCB = "f" * 64, "1" * 64
ALPHA_KEY = "7" * 64


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order.

    Several of these files also touch tables this pipeline never reads
    (se_company_addresses_canonical_current, se_company_address_geocode_results); applying
    those blindly would work but would grow the script for nothing, and 000277's ALTER on
    the legacy results table would fail since it is never created here.
    """
    statements: list[str] = []
    for name in migrations:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        text = text.replace(STAGING_NAME, "se_company_addresses_current")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


FIXTURE = f"""
INSERT INTO corpscout.se_company_addresses_current
    (company_id, address_type, source, raw_address, street_address, care_of, postal_code,
     post_town, country_code, source_run_id, source_record_id, source_payload_hash,
     source_record_uid, updated_from_raw_at, has_address, address_fingerprint,
     observation_fingerprint, observed_at, has_observation)
VALUES
    ('{ALPHA}', 'postal', 'bolagsverket', 'Storgatan 1, 111 22 Stockholm', 'Storgatan 1', NULL,
     '111 22', 'Stockholm', NULL, 'fixture', 'bv-1', 'hash-bv-1', 'uid-bv-1', {T_SEED}, 1,
     '{ALPHA_FP_BOLAGSVERKET}', '{ALPHA_FP_BOLAGSVERKET}', {T_SEED}, 1),
    ('{ALPHA}', 'visiting_or_postal', 'scb', 'Storgatan 1', 'Storgatan 1', 'c/o Anna',
     '111-22', 'Stockholm', NULL, 'fixture', 'scb-1', 'hash-scb-1', 'uid-scb-1', {T_SEED}, 1,
     '{ALPHA_FP_SCB}', '{ALPHA_FP_SCB}', {T_SEED}, 1),
    ('{BETA}', 'visiting_or_postal', 'scb', 'Lillgatan 2', 'Lillgatan 2', NULL, '22100',
     'Lund', NULL, 'fixture', 'scb-2', 'hash-scb-2', 'uid-scb-2', {T_SEED}, 1,
     '{BETA_FP}', '{BETA_FP}', {T_SEED}, 1),
    ('{BETA}', 'postal', 'bolagsverket', NULL, NULL, NULL, NULL, NULL, NULL, 'fixture',
     'bv-2', 'hash-bv-2', 'uid-bv-2', {T_SEED}, 0, '{"0" * 64}', '{"0" * 64}', {T_SEED}, 1);

INSERT INTO corpscout.se_company_address_members_current
    (company_id, canonical_address_key, address_key, address_type, address_source, raw_address,
     display_address, street_address, care_of, postal_code, post_town, country_code,
     registry_source_record_uid, registry_source_run_id, source_observed_at,
     normalization_run_id, normalized_at)
VALUES
    ('{ALPHA}', '{CANONICAL_KEY}', '{ALPHA_FP_BOLAGSVERKET}', 'postal', 'bolagsverket', '', '',
     'Storgatan 1', '', '11122', 'Stockholm', 'SE', 'uid-bv-1', 'fixture', {T_SEED}, 'norm-1', {T_SEED}),
    ('{ALPHA}', '{CANONICAL_KEY_SCB}', '{ALPHA_FP_SCB}', 'visiting_or_postal', 'scb', '', '',
     'Storgatan 1', 'c/o Anna', '11122', 'Stockholm', 'SE', 'uid-scb-1', 'fixture', {T_SEED}, 'norm-1', {T_SEED});

INSERT INTO corpscout.se_company_address_links_current
    (company_id, address_id, canonical_address_key, address_types, address_sources, evidence_count,
     first_observed_at, last_observed_at, address_identity_run_id, address_identity_built_at)
VALUES
    ('{ALPHA}', '{ADDRESS_ID}', '{CANONICAL_KEY}', ['postal'], ['bolagsverket'], 1, {T_SEED}, {T_SEED}, 'ident-1', {T_SEED}),
    ('{ALPHA}', '{ADDRESS_ID_SCB}', '{CANONICAL_KEY_SCB}', ['visiting_or_postal'], ['scb'], 1, {T_SEED}, {T_SEED}, 'ident-1', {T_SEED});

INSERT INTO corpscout.se_address_geocodes_current
    (address_id, address_identity_run_id, normalized_match_key, match_status, candidate_count,
     candidate_record_ids, candidate_record_urls, match_method, match_confidence, latitude, longitude,
     geocode_provider, geocode_precision, coordinate_supporting_point_count, geocode_run_id, matched_at)
VALUES
    ('{ADDRESS_ID}', 'ident-1', 'storgatan 1|11122|stockholm', 'matched_exact', 1, [], [], 'exact',
     0.99, 59.33, 18.06, 'osm', 'building', 1, 'geo-1', {T_GEOCODE});
"""

ARTIFACTS = (
    ("se_company_address_bolagsverket", SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
     SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL),
    ("se_company_address_scb", SE_COMPANY_ADDRESS_SCB_COLUMNS, SE_COMPANY_ADDRESS_SCB_SQL),
)
COUNTS_SQL = (
    "SELECT 'bolagsverket', count() FROM corpscout.se_company_address_bolagsverket"
    " UNION ALL SELECT 'scb', count() FROM corpscout.se_company_address_scb")


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _publish_pass(table: str, columns: tuple[str, ...], select_sql: str, params: dict[str, object]) -> str:
    """Mirrors publish_with_stage(..., new_versions_only=True) in se_company/common.py:
    stage <- SELECT, then copy into the target only the rows whose (company_id,
    source_record_uid, evidence_hash) is not already there, via a LEFT ANTI JOIN. The
    anti-join text is copied verbatim from that function, which has no SQL-string builder
    to import."""
    col_list = ", ".join(columns)
    stage_cols = ", ".join(f"stage.{column}" for column in columns)
    stage = f"corpscout._tmp_{table}"
    anti_join = (
        f"FROM {stage} AS stage\n"
        f"LEFT ANTI JOIN corpscout.{table} AS existing\n"
        "ON existing.company_id = stage.company_id "
        "AND existing.source_record_uid = stage.source_record_uid "
        "AND existing.evidence_hash = stage.evidence_hash")
    return (
        f"DROP TABLE IF EXISTS {stage};\n"
        f"CREATE TABLE {stage} AS corpscout.{table};\n"
        f"INSERT INTO {stage} ({col_list})\n{_render(select_sql, params)};\n"
        f"INSERT INTO corpscout.{table} ({col_list})\nSELECT {stage_cols} {anti_join};\n"
        f"DROP TABLE {stage};\n")


def _changed_params(*, resolve_all: int = 0, resolve_all_before: str = "2099-12-31 23:59:59") -> dict[str, object]:
    """The scan's parameters. resolve_all_before is ALWAYS bound, resolve_all or not: the
    predicate's parseDateTime64BestEffort is parsed regardless of the flag beside it, so an
    empty string would be a query error -- which is exactly what this harness holds
    address.py to."""
    return {"all_companies": 1, "company_ids": ("",), "resolve_all": resolve_all,
            "resolve_all_before": resolve_all_before, "after_company_id": "", "page_size": 10}


def _final_row_values(company: str, key: str, *, is_current: str, resolved_at: str) -> str:
    """One final row, positionally bound to INSERT_COLUMNS."""
    return (f"('{company}', '{key}', 'postal', NULL, 'Storgatan 1', 'storgatan 1|11122|stockholm', "
            f"'111 22', 'Stockholm', NULL, '{ADDRESS_ID}', 59.33, 18.06, 'matched_exact', {T_GEOCODE}, "
            f"{is_current}, ['bolagsverket'], ['uid-bv-1'], ['{'9' * 64}'], [], '{RUN_ID}', {resolved_at})")


def _script(*, join_use_nulls: int) -> str:
    render_params = {"source_run_id": RUN_ID}
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements(MIGRATIONS)) + ";")
    parts.append(FIXTURE)

    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts", COUNTS_SQL))
    parts.append(SETTLE)
    # Second pass, same evidence: the anti-join must append nothing.
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts_after_rerun", COUNTS_SQL))

    parts.append(_marked("changed_empty_final",
                         _render(build_changed_companies_sql(), _changed_params())))
    parts.append(_marked("artifact_rows",
                         "SELECT source, company_id, source_record_uid, payload_json FROM ("
                         + _render(build_artifact_rows_sql(), {"company_ids": (ALPHA, BETA)})
                         + ") ORDER BY source, company_id"))
    parts.append(_marked("geocodes",
                         "SELECT company_id, address_fingerprint, address_id, has_geocode, latitude,"
                         " geocode_status FROM ("
                         + _render(build_geocodes_sql(), {"company_ids": (ALPHA, BETA)})
                         + ") ORDER BY company_id, address_fingerprint"))

    # Set replacement, as a storage round-trip. ALPHA_KEY is published current, then
    # tombstoned, then published current again -- each with a strictly newer resolved_at.
    columns = ", ".join(INSERT_COLUMNS)
    live_sql = ("SELECT company_id, toString(address_key), is_current FROM corpscout.se_company_address"
                " FINAL WHERE is_current ORDER BY company_id, address_key")
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, ALPHA_KEY, is_current="true",
                                     resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_first_publish", live_sql))
    parts.append(SETTLE)
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, ALPHA_KEY, is_current="false",
                                     resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_tombstone", live_sql))
    parts.append(_marked("published_rows",
                         "SELECT company_id, address_key, is_current FROM ("
                         + _render(build_published_rows_sql(), {"company_ids": (ALPHA,)})
                         + ") ORDER BY address_key"))
    parts.append(SETTLE)
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, ALPHA_KEY, is_current="true",
                                     resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_reappearance", live_sql))
    parts.append(_marked("changed_after_publish",
                         _render(build_changed_companies_sql(), _changed_params())))
    parts.append(_marked("changed_resolve_all",
                         _render(build_changed_companies_sql(),
                                 _changed_params(resolve_all=1))))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(command, input=_script(join_use_nulls=request.param),
                                   capture_output=True, text=True, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def test_artifacts_publish_only_addressed_rows_and_are_idempotent(
    sections: dict[str, list[list[str]]],
) -> None:
    """BETA's Bolagsverket row has has_address = 0 -- one row per (company, type, source)
    exists whether or not the register recorded anything, and a row with no address is not
    an address.

    Both sides are sorted: COUNTS_SQL is a UNION ALL, whose branches ClickHouse is free to
    return in either order, and this script really did hand back the two branches
    transposed between the two reads. The counts are what is under test, never the order
    they arrive in.
    """
    assert sorted(sections["counts"]) == [["bolagsverket", "1"], ["scb", "2"]]
    assert sorted(sections["counts_after_rerun"]) == sorted(sections["counts"])


def test_the_artifact_payload_carries_the_fingerprint_the_geocode_join_needs(
    sections: dict[str, list[list[str]]],
) -> None:
    payloads = {(row[0], row[1]): row[3] for row in sections["artifact_rows"]}
    assert ALPHA_FP_BOLAGSVERKET in payloads[("bolagsverket", ALPHA)]
    assert '"city":"Stockholm"' in payloads[("bolagsverket", ALPHA)]


def test_the_geocode_lookup_gates_the_miss_instead_of_reading_a_type_default(
    sections: dict[str, list[list[str]]],
) -> None:
    rows = {row[1]: row for row in sections["geocodes"]}
    # Linked AND geocoded.
    assert rows[ALPHA_FP_BOLAGSVERKET][2] == ADDRESS_ID
    assert rows[ALPHA_FP_BOLAGSVERKET][3] == "1"
    assert rows[ALPHA_FP_BOLAGSVERKET][5] == "matched_exact"
    # Linked, NOT geocoded: address_id present, status EMPTY -- not the type default.
    assert rows[ALPHA_FP_SCB][2] == ADDRESS_ID_SCB
    assert rows[ALPHA_FP_SCB][3] == "0"
    assert rows[ALPHA_FP_SCB][5] == ""
    assert rows[ALPHA_FP_SCB][4] == ""  # latitude, a genuinely Nullable source column
    # Never linked: absent entirely (the members -> links join is INNER).
    assert BETA_FP not in rows


def test_the_scan_selects_every_company_before_anything_is_published(
    sections: dict[str, list[list[str]]],
) -> None:
    assert [row[0] for row in sections["changed_empty_final"]] == sorted([ALPHA, BETA])
    never_published = {row[0]: row[1] for row in sections["changed_empty_final"]}
    assert set(never_published.values()) == {"1"}


def test_a_tombstone_hides_the_address_and_a_reappearance_brings_it_back(
    sections: dict[str, list[list[str]]],
) -> None:
    """The set-replacement contract at the storage layer: ReplacingMergeTree(resolved_at)
    keeps the newest version, and readers filter FINAL ... WHERE is_current."""
    assert [row[1] for row in sections["live_after_first_publish"]] == [ALPHA_KEY]
    assert sections["live_after_tombstone"] == []
    assert [row[1] for row in sections["live_after_reappearance"]] == [ALPHA_KEY]


def test_the_published_read_back_sees_the_tombstone_not_the_older_live_version(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["published_rows"] == [[ALPHA, ALPHA_KEY, "false"]]


def test_a_published_company_is_quiet_until_resolve_all_asks_for_it(
    sections: dict[str, list[list[str]]],
) -> None:
    """ALPHA's final rows are newer than every artifact and than the geocode snapshot, so
    only the resolve_all disjunct can select it again."""
    assert ALPHA not in [row[0] for row in sections["changed_after_publish"]]
    assert ALPHA in [row[0] for row in sections["changed_resolve_all"]]


def test_the_deployed_final_columns_are_what_the_ddl_replay_says(
    sections: dict[str, list[list[str]]],
) -> None:
    """The INSERT above binds INSERT_COLUMNS positionally; had the migration and the module
    disagreed, the script would have failed before any section was produced."""
    assert list(INSERT_COLUMNS) == [column for column in declared_columns("se_company_address")
                                    if column != "evidence_set_hash"]
