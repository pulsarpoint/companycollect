from datetime import UTC, datetime

import dagster as dg
import duckdb

from dagster_v3.defs.company_domain_suggestions import inputs, scoring, tables
from dagster_v3.defs.company_domain_suggestions.feature_index import (
    _domain_label_select_sql,
    _execute_insert_select_with_progress,
    _format_bytes,
    _identifier_select_sql,
    _jsonld_name_select_sql,
    _progress_percentage,
)
from dagster_v3.defs.company_domain_suggestions.publish import (
    publish_country_suggestions,
)


def test_company_name_features_include_bounded_domain_variants() -> None:
    features = scoring.company_name_features("Nordic Signal Systems AB (publ)")
    values_by_field = {
        (feature.feature_type, feature.source_field): feature.normalized_value
        for feature in features
    }

    assert values_by_field[("organization_name", "legal_name_full")] == (
        "nordicsignalsystemsabpubl"
    )
    assert values_by_field[("organization_name", "legal_name_core")] == (
        "nordicsignalsystems"
    )
    assert values_by_field[("domain_label", "legal_name_domain_core")] == (
        "nordicsignalsystems"
    )
    assert values_by_field[("domain_label", "legal_name_domain_first_token")] == (
        "nordic"
    )
    assert values_by_field[("domain_label", "legal_name_domain_acronym")] == "nss"


def test_web_identity_index_excludes_analytics_identifiers_and_bounds_jsonld() -> None:
    domain_label_sql = _domain_label_select_sql()
    identifier_sql = _identifier_select_sql()
    jsonld_sql = _jsonld_name_select_sql()

    assert "'vat', 'lei'" in identifier_sql
    assert "'organization_number'" in identifier_sql
    assert "'ga'" not in identifier_sql
    assert "'gtm'" not in identifier_sql
    assert "crawl_id = (" in jsonld_sql
    assert "SELECT max(crawl_id)" in jsonld_sql
    assert " FINAL" not in domain_label_sql
    assert " FINAL" not in identifier_sql
    assert " FINAL" not in jsonld_sql
    for source_sql in (domain_label_sql, identifier_sql, jsonld_sql):
        assert "resolved_at AS observed_at" in source_sql
        assert "argMax(raw_value, observed_at)" in source_sql
        assert "max(observed_at) AS source_resolved_at" in source_sql
        assert "argMax(raw_value, source_resolved_at)" not in source_sql


def test_web_identity_index_uses_latest_domain_crawl_for_identifiers() -> None:
    identifier_sql = _identifier_select_sql()

    assert "latest_domain_crawls AS" in identifier_sql
    assert "FROM corpscout.commoncrawl_domains AS domains" in identifier_sql
    assert "max(domains.crawl_id) AS crawl_id" in identifier_sql
    assert "INNER JOIN latest_domain_crawls USING (root_domain, crawl_id)" in (
        identifier_sql
    )


class _FeatureIndexProgress:
    rows = 0
    bytes = 0
    total_rows = 0
    total_bytes = 0
    written_rows = 0
    written_bytes = 0


class _FeatureIndexProgressResult:
    def __init__(self) -> None:
        self.progress_totals = _FeatureIndexProgress()

    def __iter__(self):
        for values in (
            (500, 5_000, 1_000, 10_000, 50, 500),
            (1_000, 10_000, 1_000, 10_000, 100, 1_000),
        ):
            (
                self.progress_totals.rows,
                self.progress_totals.bytes,
                self.progress_totals.total_rows,
                self.progress_totals.total_bytes,
                self.progress_totals.written_rows,
                self.progress_totals.written_bytes,
            ) = values
            yield self.progress_totals.rows, self.progress_totals.total_rows


class _FeatureIndexProgressClickHouse:
    def __init__(self) -> None:
        self.query = ""
        self.params: dict[str, object] = {}
        self.query_id = ""

    def execute_with_progress(
        self,
        query: str,
        params: dict[str, object],
        *,
        query_id: str,
    ) -> _FeatureIndexProgressResult:
        self.query = query
        self.params = params
        self.query_id = query_id
        return _FeatureIndexProgressResult()


def test_feature_index_insert_reports_progress_and_returns_phase_stats() -> None:
    client = _FeatureIndexProgressClickHouse()
    messages: list[str] = []

    stats = _execute_insert_select_with_progress(
        client,
        query="INSERT INTO stage SELECT 1",
        params={"indexed_at": "now"},
        source_name="domain_labels",
        phase_number=1,
        phase_count=3,
        progress_log_interval_seconds=30,
        log=lambda message, *args: messages.append(message % args),
    )

    assert client.query == "INSERT INTO stage SELECT 1"
    assert client.params == {"indexed_at": "now"}
    assert client.query_id.startswith("web-domain-identity-domain_labels-")
    assert stats["read_rows"] == 1_000
    assert stats["read_bytes"] == 10_000
    assert stats["total_rows_to_read"] == 1_000
    assert stats["written_rows"] == 100
    assert stats["written_bytes"] == 1_000
    assert any(
        "phase started: phase=domain_labels position=1/3" in line for line in messages
    )
    assert any("phase progress: phase=domain_labels" in line for line in messages)
    assert any("phase completed: phase=domain_labels" in line for line in messages)


def test_feature_index_progress_formatting_handles_known_and_unknown_totals() -> None:
    assert _progress_percentage(250, 1_000) == "25.0%"
    assert _progress_percentage(250, 0) == "unknown"
    assert _format_bytes(1_572_864) == "1.5MiB"


class _SuggestionInputClickHouse:
    def __init__(self) -> None:
        self.external_feature_values: list[str] = []

    def execute_iter(self, sql: str, settings: object | None = None):
        rows = {
            inputs.COMPANIES_SQL: [("5590000000", "Acme Security AB")],
            inputs.LEIS_SQL: [],
            inputs.OFFICERS_SQL: [("5590000000", "Alice", "Distinctive", "board")],
            inputs.COMPANY_INDUSTRIES_SQL: [("5590000000", "6201")],
        }
        return iter(rows[sql])

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
        external_tables: list[dict[str, object]] | None = None,
    ) -> list[tuple[object, ...]]:
        assert params is not None
        if tables.QUALIFIED_FEATURES_TABLE in sql:
            assert "normalized_value IN %(values)s" not in sql
            assert "values" not in params
            assert external_tables is not None
            external_table = external_tables[0]
            assert external_table["name"] == "company_domain_feature_keys"
            assert external_table["structure"] == [("normalized_value", "String")]
            self.external_feature_values.extend(
                str(row[0]) for row in external_table["data"]
            )
            if (
                params["feature_type"] == "domain_label"
                and "acmesecurity" in self.external_feature_values
            ):
                return [
                    (
                        "domain_label",
                        "acmesecurity",
                        "acmesecurity.se",
                        "acmesecurity",
                        "root_domain_label",
                        "https://acmesecurity.se",
                        "CC-MAIN-2026-30",
                        datetime(2026, 8, 1, tzinfo=UTC),
                    )
                ]
            return []
        if "commoncrawl_page_jsonld" in sql:
            return [
                (
                    "acmesecurity.se",
                    "SE",
                    "CC-MAIN-2026-30",
                    "https://acmesecurity.se",
                )
            ]
        if "commoncrawl_industries" in sql:
            return [
                (
                    "acmesecurity.se",
                    "6201",
                    "CC-MAIN-2026-30",
                    "https://acmesecurity.se",
                )
            ]
        if "commoncrawl_domain_identifiers" in sql:
            assert "'ga'" not in sql
            assert "'gtm'" not in sql
            assert "FROM corpscout.commoncrawl_domains AS domains" in sql
            assert "max(domains.crawl_id)" in sql
            return []
        raise AssertionError(sql)


def test_sweden_input_builder_matches_only_generated_feature_keys() -> None:
    connection = duckdb.connect(":memory:")
    messages: list[str] = []
    clickhouse = _SuggestionInputClickHouse()

    input_counts = inputs.replace_sweden_suggestion_inputs(
        connection,
        clickhouse,
        query_batch_size=2,
        log=lambda message, *args: messages.append(message % args),
    )
    score_counts = scoring.replace_scored_suggestions(
        connection,
        discovery_run_id="run-input",
        suggested_at=datetime(2026, 8, 9, tzinfo=UTC),
        log=lambda message, *args: messages.append(message % args),
    )

    assert input_counts["companies"] == 1
    assert input_counts["matched_domain_features"] == 1
    assert input_counts["candidate_domains"] == 1
    assert "acmesecurity" in clickhouse.external_feature_values
    assert {
        key: score_counts[key]
        for key in (
            "candidate_pairs",
            "disqualified_candidates",
            "suggestions",
            "evidence",
        )
    } == {
        "candidate_pairs": 1,
        "disqualified_candidates": 0,
        "suggestions": 1,
        "evidence": 4,
    }
    assert score_counts["scoring_elapsed_seconds"] >= 0
    assert any("phase started: phase=companies" in line for line in messages)
    assert any(
        "phase completed: phase=domain_feature_matches" in line for line in messages
    )
    assert any(
        "scoring phase completed: phase=suggestions" in line for line in messages
    )


def test_domain_feature_match_values_use_external_table() -> None:
    clickhouse = _SuggestionInputClickHouse()
    normalized_values = [f"companyname{index:05d}" * 4 for index in range(10_000)]

    inputs._query_domain_feature_matches(
        clickhouse,
        feature_type="organization_name",
        normalized_values=normalized_values,
    )

    assert clickhouse.external_feature_values == normalized_values


def test_staging_database_filename_does_not_collide_with_schema(tmp_path) -> None:
    assert tables.DUCKDB_PATH.stem != tables.DUCKDB_SCHEMA

    connection = duckdb.connect(str(tmp_path / tables.DUCKDB_PATH.name))
    scoring.prepare_staging_tables(connection)
    connection.execute(
        f"insert into {tables.DUCKDB_SCHEMA}.companies values (?, ?)",
        ["5590000000", "Acme Security AB"],
    )

    assert connection.execute(
        f"select count(*) from {tables.DUCKDB_SCHEMA}.companies"
    ).fetchone() == (1,)


def test_suggestion_sources_use_current_page_level_jsonld_table() -> None:
    assert "commoncrawl_page_jsonld" in inputs.DOMAIN_SUPPORT_SQL
    assert "commoncrawl_domain_metadata" not in inputs.DOMAIN_SUPPORT_SQL


def test_domain_support_query_qualifies_aggregate_inputs() -> None:
    normalized_sql = " ".join(inputs.DOMAIN_SUPPORT_SQL.split())

    assert "FROM corpscout.commoncrawl_page_jsonld AS jsonld" in normalized_sql
    assert (
        "argMaxIf( jsonld.country, jsonld.resolved_at, jsonld.country != '' ) "
        "AS country"
    ) in normalized_sql
    assert (
        "argMaxIf( jsonld.crawl_id, jsonld.resolved_at, jsonld.country != '' ) "
        "AS crawl_id"
    ) in normalized_sql
    assert (
        "argMaxIf( jsonld.page_url, jsonld.resolved_at, jsonld.country != '' ) "
        "AS source_url"
    ) in normalized_sql


class _SuggestionPublishClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.country_rows = {tables.EVIDENCE_TABLE: 0, tables.SUGGESTIONS_TABLE: 0}

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if sql.startswith("INSERT INTO") and sql.rstrip().endswith("VALUES"):
            rows = params if isinstance(params, list) else []
            if f"_tmp_{tables.EVIDENCE_TABLE}_" in sql:
                self.country_rows[tables.EVIDENCE_TABLE] += len(rows)
            elif f"_tmp_{tables.SUGGESTIONS_TABLE}_" in sql:
                self.country_rows[tables.SUGGESTIONS_TABLE] += len(rows)
        if "SELECT count()" in sql and "WHERE country_iso2" in sql:
            table = (
                tables.EVIDENCE_TABLE
                if f"_tmp_{tables.EVIDENCE_TABLE}_" in sql
                else tables.SUGGESTIONS_TABLE
            )
            return [(self.country_rows[table],)]
        return []


def test_publish_reopens_duckdb_and_swaps_evidence_before_suggestions(tmp_path) -> None:
    database_path = tmp_path / "suggestions.duckdb"
    connection = duckdb.connect(str(database_path))
    inputs.replace_sweden_suggestion_inputs(
        connection,
        _SuggestionInputClickHouse(),
        query_batch_size=2,
    )
    scoring.replace_scored_suggestions(
        connection,
        discovery_run_id="run-publish",
        suggested_at=datetime(2026, 8, 9, tzinfo=UTC),
    )
    connection.close()

    client = _SuggestionPublishClickHouse()
    read_only_connection = duckdb.connect(str(database_path), read_only=True)
    messages: list[str] = []
    counts = publish_country_suggestions(
        read_only_connection,
        client,
        discovery_run_id="run-publish",
        started_at=datetime(2026, 8, 9, tzinfo=UTC),
        completed_at=datetime(2026, 8, 9, 1, tzinfo=UTC),
        log=lambda message, *args: messages.append(message % args),
    )
    read_only_connection.close()

    assert {
        key: counts[key]
        for key in (
            "companies",
            "candidate_pairs",
            "disqualified_candidates",
            "suggestions",
            "evidence",
        )
    } == {
        "companies": 1,
        "candidate_pairs": 1,
        "disqualified_candidates": 0,
        "suggestions": 1,
        "evidence": 4,
    }
    assert counts["publish_elapsed_seconds"] >= 0
    assert any("publication validation completed" in line for line in messages)
    assert any("publication export completed" in line for line in messages)
    assert any("publication completed" in line for line in messages)
    exchanges = [
        statement for statement in client.statements if statement.startswith("EXCHANGE")
    ]
    assert tables.EVIDENCE_TABLE in exchanges[0]
    assert tables.SUGGESTIONS_TABLE in exchanges[1]
    run_insert_index = next(
        index
        for index, statement in enumerate(client.statements)
        if statement.startswith(f"INSERT INTO {tables.QUALIFIED_RUNS_TABLE}")
    )
    assert run_insert_index > client.statements.index(exchanges[1])


def test_supporting_signals_do_not_create_candidates() -> None:
    connection = duckdb.connect(":memory:")
    scoring.prepare_staging_tables(connection)
    connection.execute(
        "insert into company_domain_suggestions.companies values (?, ?)",
        ["5590000000", "Industry Only AB"],
    )
    connection.execute(
        "insert into company_domain_suggestions.company_industries values (?, ?)",
        ["5590000000", "6201"],
    )
    connection.execute(
        "insert into company_domain_suggestions.domain_industries values (?, ?, ?, ?)",
        ["unrelated.se", "6201", "CC-MAIN-2026-30", "https://unrelated.se"],
    )
    connection.execute(
        "insert into company_domain_suggestions.domain_support values (?, ?, ?, ?, ?)",
        ["unrelated.se", True, "SE", "CC-MAIN-2026-30", "https://unrelated.se"],
    )

    counts = scoring.replace_scored_suggestions(
        connection,
        discovery_run_id="run-support-only",
        suggested_at=datetime(2026, 8, 9, tzinfo=UTC),
    )

    assert counts["candidate_pairs"] == 0
    assert counts["suggestions"] == 0
    assert counts["evidence"] == 0


def test_trigger_scores_are_explainable_and_conflicts_disqualify() -> None:
    connection = duckdb.connect(":memory:")
    scoring.prepare_staging_tables(connection)
    connection.executemany(
        "insert into company_domain_suggestions.companies values (?, ?)",
        [
            ("5590000000", "Acme Security AB"),
            ("5590000001", "Other Company AB"),
        ],
    )
    connection.executemany(
        """
        insert into company_domain_suggestions.company_features
        values (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "5590000000",
                "domain_label",
                "acmesecurity",
                "Acme Security AB",
                "legal_name_domain_core",
                35.0,
            ),
            (
                "5590000000",
                "identifier",
                "se559000000001",
                "SE559000000001",
                "vat",
                70.0,
            ),
            (
                "5590000001",
                "domain_label",
                "othercompany",
                "Other Company AB",
                "legal_name_domain_core",
                35.0,
            ),
            (
                "5590000001",
                "identifier",
                "se559000000101",
                "SE559000000101",
                "vat",
                70.0,
            ),
        ],
    )
    connection.executemany(
        """
        insert into company_domain_suggestions.domain_features
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "domain_label",
                "acmesecurity",
                "acmesecurity.se",
                "acmesecurity",
                "root_domain_label",
                "https://acmesecurity.se",
                "CC-MAIN-2026-30",
                datetime(2026, 8, 1, tzinfo=UTC),
            ),
            (
                "identifier",
                "se559000000001",
                "acmesecurity.se",
                "SE559000000001",
                "vat",
                "https://acmesecurity.se/about",
                "CC-MAIN-2026-30",
                datetime(2026, 8, 1, tzinfo=UTC),
            ),
            (
                "domain_label",
                "othercompany",
                "othercompany.se",
                "othercompany",
                "root_domain_label",
                "https://othercompany.se",
                "CC-MAIN-2026-30",
                datetime(2026, 8, 1, tzinfo=UTC),
            ),
        ],
    )
    connection.executemany(
        "insert into company_domain_suggestions.domain_support values (?, ?, ?, ?, ?)",
        [
            (
                "acmesecurity.se",
                True,
                "SE",
                "CC-MAIN-2026-30",
                "https://acmesecurity.se",
            ),
            (
                "othercompany.se",
                True,
                "SE",
                "CC-MAIN-2026-30",
                "https://othercompany.se",
            ),
        ],
    )
    connection.executemany(
        """
        insert into company_domain_suggestions.domain_identifiers
        values (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "acmesecurity.se",
                "se559000000001",
                "SE559000000001",
                "vat",
                "CC-MAIN-2026-30",
                "https://acmesecurity.se/about",
            ),
            (
                "othercompany.se",
                "se556999999901",
                "SE556999999901",
                "vat",
                "CC-MAIN-2026-30",
                "https://othercompany.se/about",
            ),
        ],
    )

    counts = scoring.replace_scored_suggestions(
        connection,
        discovery_run_id="run-scored",
        suggested_at=datetime(2026, 8, 9, tzinfo=UTC),
    )

    assert {
        key: counts[key]
        for key in (
            "candidate_pairs",
            "disqualified_candidates",
            "suggestions",
            "evidence",
        )
    } == {
        "candidate_pairs": 2,
        "disqualified_candidates": 1,
        "suggestions": 1,
        "evidence": 4,
    }
    assert counts["scoring_elapsed_seconds"] >= 0
    suggestion = connection.execute(
        f"select * from {tables.DUCKDB_SCHEMA}.{tables.SUGGESTIONS_TABLE}"
    ).fetchone()
    assert suggestion is not None
    assert suggestion[1:4] == ("5590000000", "acmesecurity.se", 1)
    assert suggestion[5] == ["domain_label", "identifier"]
    assert suggestion[6:14] == (70.0, 0.0, 35.0, 0.0, 0.0, 5.0, 5.0, 0.0)
    assert suggestion[14] == 100.0

    evidence_types = {
        row[0]
        for row in connection.execute(
            f"select signal_type from {tables.DUCKDB_SCHEMA}.{tables.EVIDENCE_TABLE}"
        ).fetchall()
    }
    assert evidence_types == {
        "domain_name",
        "identifier",
        "country",
        "web_presence",
    }


def test_only_web_identity_feature_job_is_registered_from_legacy_pipeline() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    job_names = {job.name for job in repo.get_all_jobs()}

    assert "sweden_company_domain_suggestions_job" not in job_names
    feature_job = repo.get_job("web_domain_identity_features_job")
    assert {key.path[-1] for key in feature_job.asset_layer.executable_asset_keys} == {
        "web_domain_identity_features_clickhouse"
    }
    assert not repo.asset_graph.has(dg.AssetKey("sweden_company_domain_suggestions_duckdb"))
    assert not repo.asset_graph.has(
        dg.AssetKey("sweden_company_domain_suggestions_clickhouse")
    )
