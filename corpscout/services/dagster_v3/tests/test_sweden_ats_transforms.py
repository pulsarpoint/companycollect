import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.sweden_ashby import tables as ashby_tables
from dagster_v3.defs.sweden_ashby.transform import replace_ashby_snapshot_tables
from dagster_v3.defs.sweden_greenhouse import tables as greenhouse_tables
from dagster_v3.defs.sweden_greenhouse.transform import (
    replace_greenhouse_snapshot_tables,
)
from dagster_v3.defs.sweden_lever import tables as lever_tables
from dagster_v3.defs.sweden_lever.transform import replace_lever_snapshot_tables
from dagster_v3.defs.sweden_smartrecruiters import tables as smartrecruiters_tables
from dagster_v3.defs.sweden_smartrecruiters.transform import (
    replace_smartrecruiters_snapshot_tables,
)


@pytest.mark.parametrize(
    ("provider", "company_id", "payload", "replace", "tables"),
    (
        (
            "greenhouse",
            "5568925506",
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Engineer",
                        "content": "<p>Build things</p>",
                        "language": "en",
                        "company_name": "Mentimeter",
                        "location": {"name": "Stockholm"},
                        "departments": [{"name": "Engineering"}],
                        "absolute_url": "https://example.test/greenhouse/1",
                        "first_published": "2026-08-01T10:00:00Z",
                        "updated_at": "2026-08-02T10:00:00Z",
                        "application_deadline": None,
                    },
                    {
                        "id": 2,
                        "title": "Berlin role",
                        "content": "<p>Not Sweden</p>",
                        "language": "en",
                        "company_name": "Mentimeter",
                        "location": {"name": "Berlin"},
                        "departments": [{"name": "Sales"}],
                        "absolute_url": "https://example.test/greenhouse/2",
                        "first_published": "2026-08-01T10:00:00Z",
                        "updated_at": "2026-08-02T10:00:00Z",
                        "application_deadline": None,
                    },
                ]
            },
            replace_greenhouse_snapshot_tables,
            greenhouse_tables,
        ),
        (
            "lever",
            "5020329081",
            [
                {
                    "id": "lever-1",
                    "text": "Engineer",
                    "country": "SE",
                    "description": "<p>Build things</p>",
                    "descriptionPlain": "Build things",
                    "descriptionBodyPlain": "Build things",
                    "createdAt": 1785680000000,
                    "workplaceType": "hybrid",
                    "categories": {
                        "location": "Stockholm",
                        "department": "Technology",
                        "team": "Platform",
                        "commitment": "Full-time",
                    },
                    "hostedUrl": "https://example.test/lever/1",
                    "applyUrl": "https://example.test/lever/1/apply",
                    "salaryRange": {
                        "currency": "SEK",
                        "interval": "per-month-salary",
                        "min": 50000,
                        "max": 70000,
                    },
                },
                {
                    "id": "lever-2",
                    "text": "Tallinn role",
                    "country": "EE",
                    "description": "",
                    "descriptionPlain": "",
                    "descriptionBodyPlain": "",
                    "createdAt": 1785680000000,
                    "workplaceType": "on-site",
                    "categories": {
                        "location": "Tallinn",
                        "department": "Technology",
                        "team": "Platform",
                        "commitment": "Full-time",
                    },
                    "hostedUrl": "https://example.test/lever/2",
                    "applyUrl": "https://example.test/lever/2/apply",
                    "salaryRange": None,
                },
            ],
            replace_lever_snapshot_tables,
            lever_tables,
        ),
        (
            "ashby",
            "5595061739",
            {
                "jobs": [
                    {
                        "id": "ashby-1",
                        "title": "Engineer",
                        "department": "Engineering",
                        "team": "Platform",
                        "employmentType": "FullTime",
                        "location": "Stockholm",
                        "publishedAt": "2026-08-01T10:00:00Z",
                        "isListed": True,
                        "isRemote": False,
                        "workplaceType": "OnSite",
                        "address": {
                            "postalAddress": {
                                "addressRegion": "Stockholm County",
                                "addressCountry": "Sweden",
                                "addressLocality": "Stockholm",
                                "streetAddress": "Main Street 1",
                                "postalCode": "11111",
                            }
                        },
                        "jobUrl": "https://example.test/ashby/1",
                        "applyUrl": "https://example.test/ashby/1/apply",
                        "descriptionHtml": "<p>Build things</p>",
                        "descriptionPlain": "Build things",
                        "compensation": {
                            "compensationTierSummary": None,
                            "scrapeableCompensationSalarySummary": None,
                            "compensationTiers": [],
                            "summaryComponents": [],
                        },
                    }
                ]
            },
            replace_ashby_snapshot_tables,
            ashby_tables,
        ),
        (
            "smartrecruiters",
            "5560427220",
            {
                "content": [
                    {
                        "id": "smart-1",
                        "name": "Sales Advisor",
                        "releasedDate": "2026-08-01T10:00:00Z",
                        "company": {"name": "H&M Group"},
                        "location": {
                            "city": "Stockholm",
                            "region": "Stockholms län",
                            "country": "se",
                            "address": "Main Street 1",
                            "remote": False,
                            "hybrid": True,
                            "latitude": "59.3",
                            "longitude": "18.0",
                        },
                        "department": {"label": "H&M"},
                        "function": {"label": "Sales"},
                        "typeOfEmployment": {"label": "Full-time"},
                        "language": {"code": "sv"},
                        "applyUrl": "https://example.test/smart/1",
                        "jobAd": {
                            "sections": {
                                "companyDescription": {"text": ""},
                                "jobDescription": {"text": "<p>Help customers</p>"},
                                "qualifications": {"text": "<p>Friendly</p>"},
                                "additionalInformation": {"text": ""},
                            }
                        },
                    }
                ]
            },
            replace_smartrecruiters_snapshot_tables,
            smartrecruiters_tables,
        ),
    ),
)
def test_provider_snapshot_normalization_is_source_owned_and_sweden_only(
    tmp_path: Path,
    provider: str,
    company_id: str,
    payload: object,
    replace: Any,
    tables: Any,
) -> None:
    payload_path = tmp_path / f"{provider}.json"
    payload_path.write_text(json.dumps(payload))
    connection = duckdb.connect()

    counts = replace(
        connection=connection,
        snapshot_files=[_snapshot_file(provider, company_id, payload_path)],
    )

    assert counts[tables.BOARDS_TABLE] == 1
    assert counts[tables.BOARD_COMPANY_LINKS_TABLE] == 1
    assert counts[tables.BOARD_SNAPSHOTS_TABLE] == 1
    assert counts[tables.CURRENT_TABLE] == 1
    assert counts[tables.VERSIONS_TABLE] == 1
    assert counts[tables.LOCATIONS_TABLE] == 1
    assert connection.execute(
        f"select distinct company_id from {tables.DUCKDB_SCHEMA}.{tables.CURRENT_TABLE}"
    ).fetchall() == [(company_id,)]
    first_version_uid = connection.execute(
        f"select version_uid from {tables.DUCKDB_SCHEMA}.{tables.VERSIONS_TABLE}"
    ).fetchone()[0]
    first_snapshot_uid = connection.execute(
        f"select snapshot_uid from {tables.DUCKDB_SCHEMA}.{tables.BOARD_SNAPSHOTS_TABLE}"
    ).fetchone()[0]

    repeated_snapshot = _snapshot_file(provider, company_id, payload_path)
    repeated_snapshot["source_run_id"] = "second-run"
    replace(connection=connection, snapshot_files=[repeated_snapshot])

    assert (
        connection.execute(
            f"select version_uid from {tables.DUCKDB_SCHEMA}.{tables.VERSIONS_TABLE}"
        ).fetchone()[0]
        == first_version_uid
    )
    assert (
        connection.execute(
            f"select snapshot_uid from {tables.DUCKDB_SCHEMA}.{tables.BOARD_SNAPSHOTS_TABLE}"
        ).fetchone()[0]
        != first_snapshot_uid
    )


def _snapshot_file(provider: str, company_id: str, path: Path) -> dict[str, object]:
    return {
        "provider_board_id": f"{provider}:test",
        "board_token": "test",
        "display_name": "Test Company",
        "company_id": company_id,
        "country_code": "SE",
        "board_url": "https://example.test/jobs",
        "evidence_url": "https://example.test/careers",
        "configured_at": "2026-08-31T00:00:00+00:00",
        "enabled": True,
        "source_url": "https://example.test/api",
        "source_object_key": f"raw/{provider}.json",
        "retrieved_at": "2026-08-31T12:00:00+00:00",
        "http_status": 200,
        "job_count": 1,
        "local_path": str(path),
        "source_run_id": "test-run",
    }
