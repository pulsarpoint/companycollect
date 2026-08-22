import json
from pathlib import Path

import dagster as dg

from dagster_v3.defs.company_people.roles import (
    ROLE_COLUMNS,
    ROLE_DRAFT_COLUMNS,
    _publish_role_assignments_sql,
    _role_assignment_quality_sql,
    build_inactive_canonical_roles_sql,
    build_role_assignments_insert_sql,
    build_role_draft_insert_sql,
    build_stale_role_corrections_sql,
    build_unmapped_source_roles_sql,
    canonical_role_code,
)
from dagster_v3.defs.company_people.roles import _ZERO_UUID as ZERO_UUID
from dagster_v3.defs.esef_filings.roles import (
    ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
)
from dagster_v3.defs.sweden_financial.roles import (
    BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE,
    BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
    BOLAGSVERKET_ROLELESS_ROLE_KINDS,
)
from dagster_v3.defs.wikidata.roles import (
    WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def test_role_tables_replace_obsolete_generic_relation() -> None:
    sql = (
        MIGRATIONS_DIR / "000292_corpscout_se_company_person_roles.up.sql"
    ).read_text(encoding="utf-8")
    down_sql = (
        MIGRATIONS_DIR / "000292_corpscout_se_company_person_roles.down.sql"
    ).read_text(encoding="utf-8")

    assert "DROP TABLE IF EXISTS corpscout.company_person_role" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_role_draft" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_role" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_person_role_type" not in sql
    assert sql.count("ENGINE = ReplacingMergeTree") == 2
    assert "is_current UInt8" in sql

    for column in ROLE_DRAFT_COLUMNS:
        assert f"    {column} " in sql
    assert "fiscal_years Array(UInt16)" in sql

    assert "DROP TABLE IF EXISTS corpscout.se_company_person_role" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.se_company_person_role_draft" in down_sql


def test_current_role_table_has_one_fiscal_year_per_row() -> None:
    sql = (
        MIGRATIONS_DIR / "000293_corpscout_se_company_person_roles_by_year.up.sql"
    ).read_text(encoding="utf-8")
    down_sql = (
        MIGRATIONS_DIR / "000293_corpscout_se_company_person_roles_by_year.down.sql"
    ).read_text(encoding="utf-8")

    assert "DROP TABLE IF EXISTS corpscout.se_company_person_role" in sql
    assert "fiscal_year Nullable(UInt16)" in sql
    assert "fiscal_years Array(UInt16)" not in sql
    corrections_sql = (
        MIGRATIONS_DIR / "000295_corpscout_se_company_person_corrections.up.sql"
    ).read_text(encoding="utf-8")
    for column in ROLE_COLUMNS:
        if column == "correction_ids":
            assert (
                "ADD COLUMN IF NOT EXISTS correction_ids Array(UUID) DEFAULT [] "
                "AFTER person_draft_ids" in corrections_sql
            )
        else:
            assert f"    {column} " in sql

    assert "fiscal_years Array(UInt16)" in down_sql


def test_each_source_owns_its_native_role_mapping() -> None:
    assert BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE == {
        "auditor": "auditor",
        "board_member": "board_member",
        "ceo": "chief_executive_officer",
        "chairman": "board_chair",
        "deputy_board_member": "deputy_board_member",
        "liquidator": "liquidator",
    }
    assert BOLAGSVERKET_ROLELESS_ROLE_KINDS == {"unknown"}
    assert BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE == {
        "Arbetstagarrepresentant": "employee_board_representative",
        "Vice VD": "deputy_chief_executive_officer"
    }
    assert ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE["chief_executive"] == (
        "chief_executive_officer"
    )
    assert WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE == {
        "P112": "founder",
        "P127": "owner",
        "P169": "chief_executive_officer",
        "P3320": "board_member",
        "P488": "board_chair",
    }


def test_roleless_and_unmapped_source_roles_are_not_invented() -> None:
    assert canonical_role_code("bolagsverket", {"role_kind": "unknown"}) is None
    assert canonical_role_code("bolagsverket", {"role_kind": "other"}) is None
    assert canonical_role_code("esef", {"role_category": "other"}) is None
    assert canonical_role_code("wikidata", {"role_property": "P169"}) == (
        "chief_executive_officer"
    )

    sql = build_unmapped_source_roles_sql(["5565200028"])
    assert "mapping.role_code = ''" in sql
    assert "source_role_code IN ('unknown')" in sql
    assert "drafts.company_id IN ('5565200028')" in sql
    assert "'other'" not in sql


def test_role_draft_sql_links_exact_person_draft_and_static_mapping() -> None:
    sql = build_role_draft_insert_sql(
        "`corpscout`.`_tmp_role_draft`",
        ["5565200028"],
    )

    assert "FROM corpscout.se_company_person_draft AS drafts FINAL" in sql
    assert "JSONExtractString(drafts.source_value_json, 'role_kind')" in sql
    assert "JSONExtractString(drafts.source_value_json, 'role_category')" in sql
    assert "JSONExtractString(drafts.source_value_json, 'role_property')" in sql
    assert "INNER JOIN role_mapping AS mapping" in sql
    assert "se-company-person-role-observation-v2" in sql
    assert "person_draft_id" in sql
    assert "ifNull(toString(fiscal_year), 'undated')" in sql
    assert "FROM corpscout.se_company_person_role_draft FINAL" in sql
    assert "unknown" in sql


def test_static_mappings_are_validated_against_active_role_pool() -> None:
    sql = build_inactive_canonical_roles_sql()

    assert "FROM corpscout.company_person_role_type FINAL" in sql
    assert "WHERE is_active = 1" in sql
    assert "mapping.role_code NOT IN" in sql


def test_current_roles_join_normalized_people_to_latest_role_drafts() -> None:
    sql = build_role_assignments_insert_sql(
        "`corpscout`.`_tmp_roles`",
        ["5565200028"],
    )

    assert "FROM corpscout.se_company_person_role_draft AS drafts FINAL" in sql
    assert "toString(drafts.role_draft_id)" in sql
    assert "arrayJoin(people.draft_ids) AS person_draft_id" in sql
    assert "FROM corpscout.se_company_person AS people FINAL" in sql
    assert "roles.person_draft_id = evidence.person_draft_id" in sql
    assert "GROUP BY drafts.person_draft_id, drafts.fiscal_year" in sql
    assert "roles.fiscal_year" in sql
    assert "se-company-person-role-v2" in sql
    assert "ifNull(toString(roles.fiscal_year), 'undated')" in sql
    assert "arraySort(groupUniqArray(roles.role_draft_id))" in sql
    assert "fiscal_years" not in sql


def test_role_assets_and_jobs_follow_person_pipeline() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    role_draft = repository.asset_graph.get(
        dg.AssetKey("se_company_person_role_draft_clickhouse")
    )
    role = repository.asset_graph.get(dg.AssetKey("se_company_person_role_clickhouse"))

    assert role_draft.parent_keys == {dg.AssetKey("se_company_person_draft_clickhouse")}
    assert role.parent_keys == {
        dg.AssetKey("se_company_person_clickhouse"),
        dg.AssetKey("se_company_person_role_draft_clickhouse"),
    }

    role_job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "se_company_person_role_job"
        ).asset_layer.executable_asset_keys
    }
    assert role_job_keys == {
        "se_company_person_role_draft_clickhouse",
        "se_company_person_role_clickhouse",
    }


def test_source_role_json_contract_uses_existing_draft_fields() -> None:
    examples = (
        ("bolagsverket", {"role_kind": "chairman"}, "board_chair"),
        ("esef", {"role_category": "audit_partner"}, "audit_partner"),
        ("wikidata", {"role_property": "P112"}, "founder"),
    )

    for source, source_value, expected in examples:
        serialized = json.loads(json.dumps(source_value))
        assert canonical_role_code(source, serialized) == expected


def test_role_assignments_apply_role_corrections_and_skip_merged_people() -> None:
    sql = build_role_assignments_insert_sql("`corpscout`.`_tmp_roles`", ["5565200028"])

    assert "role_corrections AS (" in sql
    assert "correction_kind IN ('set_role', 'remove_role')" in sql
    assert "arrayJoin(ledger.draft_ids) AS person_draft_id" in sql
    assert "JSONExtractString(ledger.payload, 'role_code')" in sql
    assert "WHERE is_active = 1" in sql
    assert "people.merged_into_person_id IS NULL" in sql
    assert "ifNull(corrections.correction_kind, '') != 'remove_role'" in sql
    assert "arraySort(groupUniqArray(corrections.correction_id))" in sql
    assert "assignments.correction_ids," in sql
    assert "(\n    " + ",\n    ".join(ROLE_COLUMNS) + "\n)" in sql
    assert ROLE_COLUMNS.index("correction_ids") == (
        ROLE_COLUMNS.index("person_draft_ids") + 1
    )


def test_role_corrections_win_per_fiscal_year_not_per_draft() -> None:
    sql = build_role_assignments_insert_sql("`corpscout`.`_tmp_roles`", ["5565200028"])

    assert "applicable_corrections AS (" in sql
    assert "ORDER BY ledger.created_at DESC, ledger.correction_id DESC" in sql
    assert (
        "LIMIT 1 BY company_id, subject_person_id, person_draft_id, fiscal_year"
        in sql
    )
    assert (
        "ifNull(toString(corrections.fiscal_year), 'undated')\n"
        "           = ifNull(toString(roles.fiscal_year), 'undated')" in sql
    )


def test_changed_corrections_republish_the_role_row() -> None:
    publish_sql = _publish_role_assignments_sql(
        "`corpscout`.`_tmp_roles`",
        ["5565200028"],
    )
    quality_sql = _role_assignment_quality_sql(
        "`corpscout`.`_tmp_roles`",
        ["5565200028"],
    )

    assert "    existing.correction_ids,\n" in publish_sql
    assert "    staged.correction_ids,\n" in publish_sql
    assert "OR existing.correction_ids != staged.correction_ids" in publish_sql
    assert "OR existing.correction_ids != staged.correction_ids" in quality_sql
    assert "AND existing.correction_ids = staged.correction_ids" in quality_sql


def test_stale_role_corrections_are_counted_not_applied() -> None:
    sql = build_stale_role_corrections_sql(["5565200028"])

    assert "FROM corpscout.se_company_person_correction AS ledger" in sql
    assert "correction_kind IN ('set_role', 'remove_role')" in sql
    assert "supersedes_correction_id IS NOT NULL" in sql
    assert "people.company_id IN ('5565200028')" in sql
    assert "AS stale_count" in sql
    assert "AS applied_count" in sql
    assert "count() AS live_count" in sql


def test_role_sql_never_assumes_join_use_nulls_is_zero() -> None:
    """A LEFT JOIN miss is '' / the zero UUID here, but only while join_use_nulls=0.

    With join_use_nulls=1 every one of these comparisons would yield NULL: the
    role stage would lose every uncorrected role and the publish step would then
    deactivate the whole role table.
    """
    stage = "`corpscout`.`_tmp_roles`"
    companies = ["5565200028"]
    insert_sql = build_role_assignments_insert_sql(stage, companies)
    quality_sql = _role_assignment_quality_sql(stage, companies)
    publish_sql = _publish_role_assignments_sql(stage, companies)
    stale_sql = build_stale_role_corrections_sql(companies)

    assert "ifNull(corrections.correction_kind, '') != 'remove_role'" in insert_sql
    assert "ifNull(corrections.correction_kind, '') = 'set_role'" in insert_sql
    assert (
        "ifNull(toString(existing.role_id), '{zero}') = '{zero}'".format(
            zero=ZERO_UUID
        )
        in insert_sql
    )
    for sql in (insert_sql, quality_sql, publish_sql, stale_sql):
        for column in ("existing.role_id", "current.role_id", "bound.person_id"):
            bare = f"toString({column})"
            for occurrence in _occurrences(sql, bare):
                assert sql[max(0, occurrence - 7) : occurrence] == "ifNull(", (
                    f"{bare} is compared without an ifNull guard"
                )


def _occurrences(text: str, needle: str) -> list[int]:
    found = []
    start = text.find(needle)
    while start != -1:
        found.append(start)
        start = text.find(needle, start + 1)
    return found
