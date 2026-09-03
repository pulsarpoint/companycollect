"""The cutover parity check (spec 12 step 4): SQL pinned as text, the pure result rule,
and the check body against a scripted client. Executed against a real engine in
test_se_company_field_resolve_clickhouse_local.py."""

import dagster as dg

from dagster_v3.defs.se_company.fields.parity import (
    CONDITION_NAMES,
    PARITY_COLUMNS,
    PARITY_SNAPSHOT,
    build_parity_snapshot_sql,
    build_parity_sql,
    build_rows_per_field_source_sql,
    parity_result,
    run_parity_check,
)
from dagster_v3.defs.se_company.fields.resolve import PARITY_CHECK_NAME, RESOLVE_ASSET
from tests.test_se_company_common import FakeClient

HANDELSBANKEN = "5020077862"
PRESENT = "ifNull(rebuilt.company_id, '') != ''"


def _zero_counts() -> dict[str, int]:
    return {"companies_compared": 0, "missing_after_rebuild": 0, **dict.fromkeys(CONDITION_NAMES, 0)}


def _parity_row(**overrides: object) -> tuple:
    """One answer row for build_parity_sql, in PARITY_COLUMNS order."""
    values: dict[str, object] = {**_zero_counts(), **{f"{name}_samples": [] for name in CONDITION_NAMES}}
    values.update(overrides)
    return tuple(values[column] for column in PARITY_COLUMNS)


def test_the_snapshot_copies_the_compared_columns_from_the_old_table() -> None:
    sql = build_parity_snapshot_sql()
    assert sql.startswith("CREATE TABLE IF NOT EXISTS corpscout.se_company_info_parity_snapshot\n"
                          "ENGINE = MergeTree ORDER BY company_id AS\n")
    assert ("SELECT company_id, legal_name, legal_form_code, status, incorporation_date,\n"
            "    description, description_sv, llm_enhanced, description_source_count, suggestion_id, correction_ids,\n"
            "    primary_sni_code, primary_nace_code, resolved_at AS snapshot_resolved_at\n"
            "FROM corpscout.se_company_info FINAL") in sql
    assert PARITY_SNAPSHOT == "se_company_info_parity_snapshot"


def test_parity_sql_compares_every_legal_fact_and_both_description_rules() -> None:
    sql = build_parity_sql()
    assert sql.startswith("WITH observation AS (")
    assert "JSONExtractString(suggestion, 'description') AS description" in sql
    assert "JSONExtractString(suggestion, 'description_sv') AS description_sv" in sql
    assert "FROM corpscout.se_company_info_enrichment_observation" in sql
    assert "FROM corpscout.se_company_info FINAL" in sql
    assert "FROM corpscout.se_company_info_parity_snapshot AS old\n" in sql
    assert "LEFT JOIN rebuilt ON rebuilt.company_id = old.company_id\n" in sql
    assert sql.endswith("LEFT JOIN observation ON observation.suggestion_id = old.suggestion_id")
    assert "count() AS companies_compared" in sql
    assert f"countIf(NOT ({PRESENT})) AS missing_after_rebuild" in sql
    # Legal facts and codes: NULL-safe text comparison of the rebuilt row with the old one.
    for column in ("legal_name", "legal_form_code", "status", "incorporation_date", "primary_sni_code", "primary_nace_code"):
        assert (f"countIf({PRESENT} AND (ifNull(toString(rebuilt.{column}), '') != "
                f"ifNull(toString(old.{column}), ''))) AS {column}") in sql
        assert f"groupArrayIf(20)(old.company_id, {PRESENT} AND (" in sql and f" AS {column}_samples" in sql
    # Copied text (single source, no decision) must match the old row ...
    assert ("(NOT old.llm_enhanced AND old.description_source_count <= 1 AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(rebuilt.description), '') != ifNull(toString(old.description), ''))) AS description_copied") in sql
    assert "AS description_sv_copied" in sql
    # ... a decided company matches the old row whatever wrote it ...
    assert ("(NOT (length(old.correction_ids) = 0) AND (ifNull(toString(rebuilt.description), '') != "
            "ifNull(toString(old.description), '') OR ifNull(toString(rebuilt.description_sv), '') != "
            "ifNull(toString(old.description_sv), '')))) AS description_decided") in sql
    # ... and a modelled one matches the stored observation, not the old row.
    assert ("(old.llm_enhanced AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(observation.suggestion_id), '00000000-0000-0000-0000-000000000000') != "
            "'00000000-0000-0000-0000-000000000000' AND "
            "ifNull(rebuilt.description, '') != ifNull(observation.description, ''))) AS description_llm") in sql
    assert "AS description_sv_llm" in sql
    # Informational: expected to change (model never answered), or the observation is gone.
    assert ("(NOT old.llm_enhanced AND old.description_source_count > 1 AND length(old.correction_ids) = 0 AND "
            "ifNull(toString(rebuilt.description), '') != ifNull(toString(old.description), ''))) "
            "AS description_model_pending_changed") in sql
    assert "AS llm_observation_missing" in sql
    assert PARITY_COLUMNS[:2] == ("companies_compared", "missing_after_rebuild")
    assert PARITY_COLUMNS[2 : 2 + len(CONDITION_NAMES)] == CONDITION_NAMES
    assert PARITY_COLUMNS[2 + len(CONDITION_NAMES) :] == tuple(f"{name}_samples" for name in CONDITION_NAMES)
    assert build_rows_per_field_source_sql() == (
        "SELECT field, source, count() AS rows\n"
        "FROM corpscout.se_company_field FINAL\n"
        "GROUP BY field, source\n"
        "ORDER BY field, source")


def test_parity_result_passes_only_with_zero_mismatches_and_reports_informational_counts() -> None:
    clean = parity_result({**_zero_counts(), "companies_compared": 3_500_000,
                           "description_model_pending_changed": 12_000, "llm_observation_missing": 3},
                          {}, [("legal_name", "bolagsverket", 3_400_000), ("legal_name", "scb", 100_000)])
    assert clean.passed is True and clean.severity == dg.AssetCheckSeverity.ERROR
    assert clean.metadata["companies_compared"] == dg.MetadataValue.int(3_500_000)
    assert clean.metadata["description_model_pending_changed"] == dg.MetadataValue.int(12_000)
    assert clean.metadata["rows_per_field_per_source"] == dg.MetadataValue.json(
        [{"field": "legal_name", "source": "bolagsverket", "rows": 3_400_000},
         {"field": "legal_name", "source": "scb", "rows": 100_000}])
    assert clean.metadata["failing"] == dg.MetadataValue.json({})

    for column in ("missing_after_rebuild", "legal_name", "primary_sni_code", "description_copied",
                   "description_decided", "description_llm", "description_sv_llm"):
        failed = parity_result({**_zero_counts(), "companies_compared": 1, column: 1},
                               {column: [HANDELSBANKEN]}, [])
        assert failed.passed is False, column
        assert failed.metadata["failing"] == dg.MetadataValue.json({column: 1})
        assert failed.metadata["samples"] == dg.MetadataValue.json({column: [HANDELSBANKEN]})


def test_run_parity_check_reads_counts_and_samples_from_the_client() -> None:
    client = FakeClient(answers=[
        [(1,)],  # the snapshot exists
        [_parity_row(companies_compared=2, primary_sni_code=1, primary_sni_code_samples=[HANDELSBANKEN])],
        [("description", "llm", 2), ("legal_name", "bolagsverket", 2)],
    ])
    result = run_parity_check(client)
    assert result.passed is False
    assert result.metadata["primary_sni_code"] == dg.MetadataValue.int(1)
    assert result.metadata["samples"] == dg.MetadataValue.json({"primary_sni_code": [HANDELSBANKEN]})
    assert result.metadata["rows_per_field_per_source"] == dg.MetadataValue.json(
        [{"field": "description", "source": "llm", "rows": 2}, {"field": "legal_name", "source": "bolagsverket", "rows": 2}])
    statements = [sql for sql, _ in client.executed]
    assert statements[0] == ("SELECT count() FROM system.tables WHERE database = 'corpscout' "
                             "AND name = 'se_company_info_parity_snapshot'")
    assert statements[1] == build_parity_sql() and statements[2] == build_rows_per_field_source_sql()


def test_run_parity_check_fails_clearly_without_a_snapshot() -> None:
    client = FakeClient(answers=[[(0,)]])
    result = run_parity_check(client)
    assert result.passed is False
    assert "corpscout.se_company_info_parity_snapshot does not exist" in str(result.metadata["error"].value)
    assert len(client.executed) == 1  # nothing else was asked


def test_the_check_is_registered_on_the_resolve_asset() -> None:
    from dagster_v3.defs.se_company.fields.parity import se_company_field_parity_check

    assert PARITY_CHECK_NAME == "se_company_field_parity_check"
    assert se_company_field_parity_check.check_keys == {dg.AssetCheckKey(dg.AssetKey(RESOLVE_ASSET), PARITY_CHECK_NAME)}
