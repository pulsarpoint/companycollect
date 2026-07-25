from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_identifier import tables
from dagster_v3.defs.company_identifier.assets import (
    build_company_identifier_insert_sql,
    replace_company_identifier_clickhouse,
)
from dagster_v3.defs.company_identifier.rules import COUNTRY_IDENTITY_RULES

_STAGE = "`corpscout`.`_tmp_company_identifier_test`"
_SE = COUNTRY_IDENTITY_RULES["SE"]


def test_company_identifier_column_contract() -> None:
    assert tables.COMPANY_IDENTIFIER_TABLE == "company_identifier"
    assert tables.COMPANY_IDENTIFIER_COLUMNS == (
        "issuer_scheme",
        "issuer_id",
        "country_code",
        "company_id",
        "match_method",
        "match_confidence",
        "registration_authority_id",
        "registered_as_raw",
        "company_id_normalized",
        "entity_status",
        "registration_status",
        "is_current",
        "successor_issuer_id",
        "first_seen_date",
        "last_seen_date",
        "source_run_id",
        "resolved_at",
    )


def test_sweden_rule_declares_scheme_register_and_normalization() -> None:
    assert _SE.country_code == "SE"
    assert _SE.issuer_scheme == "lei"
    assert _SE.register_table == "se_companies"
    assert _SE.identifier_length == 10
    assert _SE.min_expected_rows >= 100


def test_sql_deduplicates_the_replacing_merge_tree_register() -> None:
    """se_companies is a ReplacingMergeTree; a raw join fans out."""
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "register_current AS" in sql
    assert "GROUP BY company_id" in sql
    assert "INNER JOIN corpscout.se_companies AS r" not in sql


def test_sql_requires_the_identifier_to_exist_in_the_register() -> None:
    """The register is ground truth; an unresolvable issuer produces no row."""
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "INNER JOIN register_current AS r" in sql
    assert "r.company_id = g.company_id_normalized" in sql


def test_sql_does_not_tier_confidence_by_registration_authority() -> None:
    """The register lookup is what validates a match, so there is one tier.

    Measured 2026-07-25: adding a registration-authority corroboration tier
    separated 7 rows out of 114,974, and cost a per-country discovery step on
    every country added. The raw authority code is still stored for diagnosis,
    it just does not gate or grade anything.
    """
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "'register_verified' AS match_confidence" in sql
    assert "'gleif_registered_as' AS match_method" in sql
    assert "RA000544" not in sql
    assert "'registration_authority'" not in sql
    assert "'jurisdiction_normalized'" not in sql


def test_sql_still_stores_the_authority_code_for_diagnosis() -> None:
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "g.registered_at_id AS registration_authority_id" in sql


def test_sql_marks_superseded_issuers_as_not_current() -> None:
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "successor_entity_lei" in sql
    assert "AS is_current" in sql


def test_sql_emits_the_rule_issuer_scheme() -> None:
    sql = build_company_identifier_insert_sql(_STAGE, _SE)

    assert "'lei' AS issuer_scheme" in sql
    assert "AS issuer_id" in sql


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            return [(table,) for table in requested]
        if "row_count" in sql:
            return [self.quality_row]
        return []


def _resource(
    monkeypatch: pytest.MonkeyPatch,
    client: _FakeClickHouseClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_replace_publishes_and_reports_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # row_count, issuer_count, company_count, identity_key_count, invalid_rows
    client = _FakeClickHouseClient((800, 800, 800, 800, 0))
    resource = _resource(monkeypatch, client)

    metadata = replace_company_identifier_clickhouse(
        clickhouse=resource,
        rule=_SE,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    )

    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert metadata["row_count"] == 800
    assert metadata["country_code"] == "SE"


def test_replace_refuses_a_degraded_gleif_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial GLEIF load must not empty a populated country."""
    client = _FakeClickHouseClient((12, 12, 12, 12, 0))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="below the expected floor"):
        replace_company_identifier_clickhouse(
            clickhouse=resource,
            rule=_SE,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_refuses_duplicate_identity_grain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient((800, 800, 799, 799, 0))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="grain mismatch"):
        replace_company_identifier_clickhouse(
            clickhouse=resource,
            rule=_SE,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)
