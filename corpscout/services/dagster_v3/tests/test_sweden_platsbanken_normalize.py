import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.sweden_platsbanken import tables
from dagster_v3.defs.sweden_platsbanken.normalize import (
    build_normalized_tables,
    replace_raw_jsonl_table,
)


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def test_historical_record_becomes_version_lifecycle_and_requirements(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "historical.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "id": "historical-content-hash",
                "original_id": "30429400",
                "headline": "Trafikledare",
                "publication_date": "2025-12-31T09:08:37",
                "last_publication_date": "2026-01-25T23:59:59",
                "application_deadline": "2026-01-25T23:59:59",
                "timestamp": 1767168517718,
                "removed": False,
                "number_of_vacancies": 2,
                "detected_language": "sv",
                "description": {"text": "Historisk annonstext"},
                "employer": {
                    "organization_number": "16-556351-9437",
                    "name": "VR Sverige AB",
                    "workplace": "VR Sverige AB",
                },
                "occupation": [
                    {"concept_id": "occupation-id", "label": "Trafikledare"}
                ],
                "occupation_group": [
                    {"concept_id": "group-id", "label": "Transportledare"}
                ],
                "occupation_field": [{"concept_id": "field-id", "label": "Transport"}],
                "must_have": {
                    "skills": [
                        {
                            "concept_id": "skill-id",
                            "label": "Tågtrafik",
                            "weight": 10,
                        }
                    ],
                    "languages": [
                        {
                            "concept_id": "language-id",
                            "label": "Svenska",
                            "weight": 5,
                        }
                    ],
                },
                "nice_to_have": {
                    "work_experiences": [
                        {
                            "concept_id": "experience-id",
                            "label": "Trafikledning",
                            "weight": 3,
                        }
                    ]
                },
            }
        ],
    )
    connection = duckdb.connect(":memory:")
    replace_raw_jsonl_table(
        connection=connection,
        jsonl_path=jsonl_path,
        raw_table=tables.HISTORICAL_RAW_TABLE,
        record_kind="archive_record",
        source_run_id="historical-run",
        source_object_key="historical/2025.zip",
        source_url="https://data.jobtechdev.se/2025.zip",
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
    )

    counts = build_normalized_tables(
        connection=connection,
        raw_table=tables.HISTORICAL_RAW_TABLE,
        versions_table=tables.HISTORICAL_VERSIONS_TABLE,
        events_table=tables.HISTORICAL_EVENTS_TABLE,
        requirements_table=tables.HISTORICAL_REQUIREMENTS_TABLE,
        contacts_table=tables.HISTORICAL_CONTACTS_TABLE,
    )

    assert counts == {"versions": 1, "events": 3, "requirements": 3, "contacts": 0}
    version = connection.execute(
        f"""
        select source_job_ad_id, source_record_id, employer_org_number,
               match_eligibility, number_of_vacancies,
               occupation_concept_id, occupation_group_concept_id,
               occupation_field_concept_id
        from {tables.DUCKDB_SCHEMA}.{tables.HISTORICAL_VERSIONS_TABLE}
        """
    ).fetchone()
    assert version == (
        "30429400",
        "historical-content-hash",
        "5563519437",
        "eligible",
        2,
        "occupation-id",
        "group-id",
        "field-id",
    )

    events = connection.execute(
        f"""
        select event_type, is_active, active_to_basis, is_estimated
        from {tables.DUCKDB_SCHEMA}.{tables.HISTORICAL_EVENTS_TABLE}
        order by effective_at, event_type
        """
    ).fetchall()
    assert events == [
        ("archive_record", 1, "", 0),
        ("published", 1, "", 0),
        ("scheduled_end", 0, "last_publication_date", 1),
    ]

    requirement_types = connection.execute(
        f"""
        select requirement_level, requirement_type, concept_id
        from {tables.DUCKDB_SCHEMA}.{tables.HISTORICAL_REQUIREMENTS_TABLE}
        order by requirement_level, requirement_type
        """
    ).fetchall()
    assert requirement_types == [
        ("must_have", "language", "language-id"),
        ("must_have", "skill", "skill-id"),
        ("nice_to_have", "work_experience", "experience-id"),
    ]


def test_sparse_jobstream_removal_closes_history_without_erasing_content(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "removal.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "id": "31380149",
                "removed": True,
                "removed_date": "2026-08-21T10:11:45",
                "timestamp": 1787307105000,
            }
        ],
    )
    connection = duckdb.connect(":memory:")
    replace_raw_jsonl_table(
        connection=connection,
        jsonl_path=jsonl_path,
        raw_table=tables.JOBSTREAM_EVENTS_RAW_TABLE,
        record_kind="stream_event",
        source_run_id="stream-run",
        source_object_key="jobstream/events.jsonl",
        source_url="https://jobstream.api.jobtechdev.se/v2/stream",
        retrieved_at=datetime(2026, 8, 21, 8, 15, tzinfo=UTC),
    )

    counts = build_normalized_tables(
        connection=connection,
        raw_table=tables.JOBSTREAM_EVENTS_RAW_TABLE,
        versions_table=tables.JOBSTREAM_EVENTS_VERSIONS_TABLE,
        events_table=tables.JOBSTREAM_EVENTS_EVENTS_TABLE,
        requirements_table=tables.JOBSTREAM_EVENTS_REQUIREMENTS_TABLE,
        contacts_table=tables.JOBSTREAM_EVENTS_CONTACTS_TABLE,
    )

    assert counts == {"versions": 0, "events": 1, "requirements": 0, "contacts": 0}
    assert connection.execute(
        f"""
        select source_job_ad_id, event_type, is_active, active_to_basis,
               is_estimated
        from {tables.DUCKDB_SCHEMA}.{tables.JOBSTREAM_EVENTS_EVENTS_TABLE}
        """
    ).fetchone() == ("31380149", "removed", 0, "removed_event", 0)


def test_full_removed_archive_record_still_has_a_publication_event(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "removed-historical.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "id": "historical-hash",
                "original_id": "30000001",
                "headline": "Historiskt jobb",
                "publication_date": "2024-01-01T09:00:00",
                "removed": True,
                "removed_date": "2024-01-10T15:00:00",
                "timestamp": 1704895200000,
                "employer": {"organization_number": "5563519437"},
            }
        ],
    )
    connection = duckdb.connect(":memory:")
    replace_raw_jsonl_table(
        connection=connection,
        jsonl_path=jsonl_path,
        raw_table=tables.HISTORICAL_RAW_TABLE,
        record_kind="archive_record",
        source_run_id="historical-run",
        source_object_key="historical/2024.zip",
        source_url="https://data.jobtechdev.se/2024.zip",
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
    )
    counts = build_normalized_tables(
        connection=connection,
        raw_table=tables.HISTORICAL_RAW_TABLE,
        versions_table=tables.HISTORICAL_VERSIONS_TABLE,
        events_table=tables.HISTORICAL_EVENTS_TABLE,
        requirements_table=tables.HISTORICAL_REQUIREMENTS_TABLE,
        contacts_table=tables.HISTORICAL_CONTACTS_TABLE,
    )

    assert counts["versions"] == 1
    assert connection.execute(
        f"""
        select event_type, is_active
        from {tables.DUCKDB_SCHEMA}.{tables.HISTORICAL_EVENTS_TABLE}
        order by effective_at, event_type
        """
    ).fetchall() == [
        ("published", 1),
        ("removed", 0),
    ]


def test_normalized_duckdb_columns_match_clickhouse_export_contract(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "snapshot.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "id": "31380149",
                "headline": "Maskinoperatör",
                "publication_date": "2026-08-21T09:00:15",
                "timestamp": 1787295615123,
                "removed": False,
                "application_details": {
                    "email": "jobs@example.se",
                    "url": "https://example.se/apply",
                    "other": "Apply online",
                    "reference": "JOB-42",
                    "information": "Applications reviewed continuously",
                    "via_af": False,
                },
                "application_contacts": [
                    {
                        "name": "Recruiter",
                        "description": "Hiring manager",
                        "email": "recruiter@example.se",
                        "telephone": "0101234567",
                        "contact_type": "contact",
                    },
                    {
                        "name": "Union Representative",
                        "description": "Union contact",
                        "email": "union@example.se",
                        "telephone": "0109876543",
                        "contact_type": "union",
                    },
                ],
                "employer": {
                    "organization_number": "5563519437",
                    "email": "company@example.se",
                    "phone_number": "0107654321",
                },
            }
        ],
    )
    connection = duckdb.connect(":memory:")
    replace_raw_jsonl_table(
        connection=connection,
        jsonl_path=jsonl_path,
        raw_table=tables.JOBSTREAM_SNAPSHOT_RAW_TABLE,
        record_kind="snapshot",
        source_run_id="snapshot-run",
        source_object_key="jobstream/snapshot.jsonl",
        source_url="https://jobstream.api.jobtechdev.se/v2/snapshot",
        retrieved_at=datetime(2026, 8, 21, 8, 5, tzinfo=UTC),
    )
    build_normalized_tables(
        connection=connection,
        raw_table=tables.JOBSTREAM_SNAPSHOT_RAW_TABLE,
        versions_table=tables.JOBSTREAM_SNAPSHOT_VERSIONS_TABLE,
        events_table=tables.JOBSTREAM_SNAPSHOT_EVENTS_TABLE,
        requirements_table=tables.JOBSTREAM_SNAPSHOT_REQUIREMENTS_TABLE,
        contacts_table=tables.JOBSTREAM_SNAPSHOT_CONTACTS_TABLE,
    )

    for table_name, expected_columns in (
        (tables.JOBSTREAM_SNAPSHOT_VERSIONS_TABLE, tables.VERSION_COLUMNS),
        (tables.JOBSTREAM_SNAPSHOT_EVENTS_TABLE, tables.EVENT_COLUMNS),
        (
            tables.JOBSTREAM_SNAPSHOT_REQUIREMENTS_TABLE,
            tables.REQUIREMENT_COLUMNS,
        ),
        (tables.JOBSTREAM_SNAPSHOT_CONTACTS_TABLE, tables.CONTACT_COLUMNS),
    ):
        columns = tuple(
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
        assert columns == expected_columns

    version_contact_fields = connection.execute(
        f"""
        select application_email, application_url, application_other,
               application_reference, application_information,
               application_via_af, employer_email, employer_phone
        from {tables.DUCKDB_SCHEMA}.{tables.JOBSTREAM_SNAPSHOT_VERSIONS_TABLE}
        """
    ).fetchone()
    assert version_contact_fields == (
        "jobs@example.se",
        "https://example.se/apply",
        "Apply online",
        "JOB-42",
        "Applications reviewed continuously",
        0,
        "company@example.se",
        "0107654321",
    )
    contacts = connection.execute(
        f"""
        select contact_index, name, description, email, telephone, contact_type
        from {tables.DUCKDB_SCHEMA}.{tables.JOBSTREAM_SNAPSHOT_CONTACTS_TABLE}
        order by contact_index
        """
    ).fetchall()
    assert contacts == [
        (0, "Recruiter", "Hiring manager", "recruiter@example.se", "0101234567", "contact"),
        (1, "Union Representative", "Union contact", "union@example.se", "0109876543", "union"),
    ]
