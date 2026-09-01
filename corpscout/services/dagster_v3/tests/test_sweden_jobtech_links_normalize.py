import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.normalize import (
    SnapshotProvenance,
    append_snapshot_jsonl,
    build_normalized_tables,
    initialize_raw_tables,
    replace_snapshot_catalog,
)


def _record(
    *,
    provider: str,
    identifier: str,
    title: str,
    hashsum: str,
) -> dict[str, object]:
    return {
        "id": "jobtech-links-job-42",
        "hashsum": hashsum,
        "firstSeen": "2026-01-31T23:15:00Z",
        "date_to_display_as_publish_date": "2026-02-01T08:00:00Z",
        "application_deadline": "2026-02-28T23:59:59Z",
        "isValid": True,
        "brief_description": f"Brief description for {title}",
        "detected_language": "sv",
        "ssyk_lvl4": "2512",
        "originalJobPosting": {
            "@context": "https://schema.org/",
            "@type": "JobPosting",
            "scraper": provider,
            "identifier": identifier,
            "url": f"https://{provider}/jobs/{identifier}",
            "datePosted": "2026-02-01T08:00:00Z",
            "validThrough": "2026-02-28T23:59:59Z",
            "title": title,
            "employmentType": ["FULL_TIME", "PERMANENT"],
            "experienceRequirements": ["Python", "SQL"],
            "skills": ["DuckDB", "Dagster"],
            "qualifications": "Relevant degree",
            "responsibilities": "Build data products",
            "educationRequirements": ["University degree"],
            "jobBenefits": ["Pension"],
            "workHours": "40 hours per week",
            "jobLocationType": "TELECOMMUTE",
            "totalJobOpenings": "2",
            "hiringOrganization": {
                "@type": "Organization",
                "name": "Example AB",
                "url": "https://example.test",
                "logo": "https://example.test/logo.png",
            },
            "relevantOccupation": {
                "@type": "Occupation",
                "name": "Backendutvecklare",
            },
        },
        "workplace_addresses": [
            {
                "municipality_concept_id": "AvNB_uwa_6n6",
                "municipality": "Stockholm",
                "region_concept_id": "CifL_Rzy_Mku",
                "region": "Stockholms län",
                "country_concept_id": "i46j_HmG_v64",
                "country": "Sverige",
            }
        ],
        "text_enrichments_results": {
            "enrichedbinary_result": {
                "enriched_candidates": {
                    "occupations": [
                        {
                            "concept_label": "Backendutvecklare",
                            "term": "backendutvecklare",
                            "term_misspelled": False,
                        }
                    ],
                    "competencies": [
                        {
                            "concept_label": "Python",
                            "term": "python",
                            "term_misspelled": False,
                        }
                    ],
                    "traits": [
                        {
                            "concept_label": "Analytisk",
                            "term": "analytisk",
                            "term_misspelled": False,
                        }
                    ],
                    "geos": [
                        {
                            "concept_label": "Stockholm",
                            "term": "stockholm",
                            "term_misspelled": False,
                        }
                    ],
                }
            }
        },
    }


def _provenance(snapshot_date: date, marker: str) -> SnapshotProvenance:
    return SnapshotProvenance(
        snapshot_uid=marker * 64,
        snapshot_date=snapshot_date,
        catalog_url=tables.CATALOG_URL,
        source_url=f"https://example.test/{snapshot_date}.tar.gz",
        archive_object_key=f"snapshots/{snapshot_date}.tar.gz",
        archive_sha256=marker * 64,
        archive_etag=f'"etag-{marker}"',
        archive_size_bytes=100,
        raw_member_path="jobtechdev/minio/arkiv/output.json",
        raw_member_size_bytes=1_000,
        source_last_modified_at=datetime(
            snapshot_date.year,
            snapshot_date.month,
            snapshot_date.day,
            3,
            tzinfo=UTC,
        ),
        source_run_id="s3-source-run",
        retrieved_at=datetime(2026, 9, 1, 12, tzinfo=UTC),
    )


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def _load_fixture(connection: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
    initialize_raw_tables(connection)
    first_version = _record(
        provider="studentjob.se",
        identifier="job-42",
        title="Backendutvecklare",
        hashsum="source-hash-v1",
    )
    second_version = _record(
        provider="studentjob.se",
        identifier="job-42",
        title="Senior backendutvecklare",
        hashsum="source-hash-v2",
    )
    platsbanken = _record(
        provider=tables.PLATSBANKEN_PROVIDER,
        identifier="platsbanken-42",
        title="Platsbanken duplicate",
        hashsum="platsbanken-source-hash",
    )
    snapshot_records = (
        [first_version, platsbanken],
        [second_version],
        [second_version],
    )
    loaded = []
    for offset, records in enumerate(snapshot_records, start=1):
        snapshot_date = date(2026, 2, offset)
        jsonl_path = tmp_path / f"{snapshot_date}.jsonl"
        _write_jsonl(jsonl_path, records)
        loaded.append(
            append_snapshot_jsonl(
                connection=connection,
                jsonl_path=jsonl_path,
                provenance=_provenance(snapshot_date, chr(96 + offset)),
            )
        )
    replace_snapshot_catalog(connection, loaded)


def test_raw_loader_keeps_external_rows_and_catalogs_all_source_rows(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    try:
        _load_fixture(connection, tmp_path)

        raw_rows = connection.execute(
            f"""
            select snapshot_date, provider, source_identifier
            from {tables.DUCKDB_SCHEMA}.{tables.RAW_EXTERNAL_TABLE}
            order by snapshot_date
            """
        ).fetchall()
        snapshots = connection.execute(
            f"""
            select
                snapshot_date,
                raw_row_count,
                platsbanken_row_count,
                external_row_count,
                external_provider_count
            from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOTS_TABLE}
            order by snapshot_date
            """
        ).fetchall()
    finally:
        connection.close()

    assert raw_rows == [
        (date(2026, 2, 1), "studentjob.se", "job-42"),
        (date(2026, 2, 2), "studentjob.se", "job-42"),
        (date(2026, 2, 3), "studentjob.se", "job-42"),
    ]
    assert snapshots == [
        (date(2026, 2, 1), 2, 1, 1, 1),
        (date(2026, 2, 2), 1, 0, 1, 1),
        (date(2026, 2, 3), 1, 0, 1, 1),
    ]


def test_normalization_builds_stable_versions_observations_and_child_rows(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    try:
        _load_fixture(connection, tmp_path)

        counts = build_normalized_tables(connection=connection)
        versions = connection.execute(
            f"""
            select
                source_job_ad_uid,
                version_uid,
                provider,
                source_identifier,
                headline_original,
                employment_types,
                number_of_vacancies,
                workplace_type
            from {tables.DUCKDB_SCHEMA}.{tables.VERSIONS_TABLE}
            order by headline_original
            """
        ).fetchall()
        observations = connection.execute(
            f"""
            select snapshot_date, version_uid
            from {tables.DUCKDB_SCHEMA}.{tables.OBSERVATIONS_TABLE}
            order by snapshot_date
            """
        ).fetchall()
        locations = connection.execute(
            f"""
            select distinct municipality_name_original
            from {tables.DUCKDB_SCHEMA}.{tables.LOCATIONS_TABLE}
            """
        ).fetchall()
        enrichment_types = connection.execute(
            f"""
            select enrichment_type, count(*)
            from {tables.DUCKDB_SCHEMA}.{tables.ENRICHMENTS_TABLE}
            group by enrichment_type
            order by enrichment_type
            """
        ).fetchall()
    finally:
        connection.close()

    expected_source_uid = hashlib.sha256(b"studentjob.se\0job-42").hexdigest()
    assert counts == {
        "snapshots": 3,
        "raw_external_rows": 3,
        "external_providers": 1,
        "versions": 2,
        "observations": 3,
        "locations": 2,
        "enrichments": 8,
    }
    assert {row[0] for row in versions} == {expected_source_uid}
    assert len({row[1] for row in versions}) == 2
    assert {row[2] for row in versions} == {"studentjob.se"}
    assert {row[3] for row in versions} == {"job-42"}
    assert {row[4] for row in versions} == {
        "Backendutvecklare",
        "Senior backendutvecklare",
    }
    assert {tuple(row[5]) for row in versions} == {("FULL_TIME", "PERMANENT")}
    assert {row[6] for row in versions} == {2}
    assert {row[7] for row in versions} == {"TELECOMMUTE"}
    assert observations[1][1] == observations[2][1]
    assert observations[0][1] != observations[1][1]
    assert locations == [("Stockholm",)]
    assert enrichment_types == [
        ("competency", 2),
        ("geo", 2),
        ("occupation", 2),
        ("trait", 2),
    ]


def test_jobtech_partition_paths_support_year_month_and_day() -> None:
    paths = {
        partition: tables.partition_duckdb_path(partition)
        for partition in ("2025", "2026-02", "2026-09-01")
    }

    assert len(set(paths.values())) == 3
    for partition, path in paths.items():
        assert f"partition_key={partition}" in str(path)


def test_normalized_tables_match_the_migration_owned_clickhouse_shapes(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    try:
        _load_fixture(connection, tmp_path)
        build_normalized_tables(connection=connection)

        expected_columns = {
            tables.SNAPSHOTS_TABLE: tables.SNAPSHOT_COLUMNS,
            tables.VERSIONS_TABLE: tables.VERSION_COLUMNS,
            tables.OBSERVATIONS_TABLE: tables.OBSERVATION_COLUMNS,
            tables.LOCATIONS_TABLE: tables.LOCATION_COLUMNS,
            tables.ENRICHMENTS_TABLE: tables.ENRICHMENT_COLUMNS,
        }
        actual_columns = {
            table_name: tuple(
                row[0]
                for row in connection.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = ? and table_name = ?
                    order by ordinal_position
                    """,
                    [tables.DUCKDB_SCHEMA, table_name],
                ).fetchall()
            )
            for table_name in expected_columns
        }
    finally:
        connection.close()

    assert actual_columns == expected_columns
