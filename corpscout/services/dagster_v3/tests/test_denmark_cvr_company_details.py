import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DenmarkCvrCompanyDetailDownload,
    DenmarkCvrCompanyDetailHttpFailure,
    DenmarkCvrCompanyDetailKeyError,
    DenmarkCvrCompanyDetailResource,
    clear_company_detail_failure_history,
    company_detail_api_url,
    company_detail_bucket_key,
    company_detail_failure_object_key,
    company_detail_object_key,
    company_detail_page_url,
    company_detail_partition_cvrs,
    company_detail_update_cvrs,
    company_detail_update_object_key,
    denmark_cvr_company_detail_updates_s3,
    denmark_cvr_company_details_s3,
    defs,
    insert_company_detail_failure_record,
    record_company_detail_http_failure,
    translate_company_detail_keys,
    write_company_detail_partition,
    write_company_detail_updates,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_SCHEMA,
)

DENMARK_CVR_BUCKET = "source-denmark-cvr"


class FakePage:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = list(results)
        self.goto_calls: list[tuple[str, str]] = []
        self.evaluate_calls: list[dict[str, str]] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    def evaluate(self, _script: str, argument: dict[str, str]) -> dict[str, Any]:
        self.evaluate_calls.append(argument)
        return self.results.pop(0)


class FakeBrowser:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.closed = False

    def new_page(self) -> FakePage:
        return self.page

    def close(self) -> None:
        self.closed = True


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.list_prefixes: list[str] = []
        self.read_keys: list[str] = []
        self.write_keys: list[str] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == DENMARK_CVR_BUCKET

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket == DENMARK_CVR_BUCKET
        self.list_prefixes.append(prefix)
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == DENMARK_CVR_BUCKET
        self.read_keys.append(key)
        return self.objects[key]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket == DENMARK_CVR_BUCKET
        self.objects[key] = body
        self.write_keys.append(key)


class FakeDetailResource:
    def __init__(
        self,
        downloads: dict[
            str,
            DenmarkCvrCompanyDetailDownload | DenmarkCvrCompanyDetailHttpFailure,
        ],
    ) -> None:
        self.downloads = downloads
        self.requested_cvrs: list[str] = []

    def iter_company_details(self, cvrs: tuple[str, ...], **_: Any):
        for cvr in cvrs:
            self.requested_cvrs.append(cvr)
            yield self.downloads[cvr]


def _download(cvr: str, payload: dict[str, Any]) -> DenmarkCvrCompanyDetailDownload:
    raw_body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return DenmarkCvrCompanyDetailDownload(
        cvr=cvr,
        source_url=company_detail_api_url("https://datacvr.virk.dk", cvr),
        raw_body=raw_body,
        payload=payload,
        status=200,
        response_headers={"content-type": "application/json"},
    )


def _http_failure(
    cvr: str,
    *,
    status: int = 500,
) -> DenmarkCvrCompanyDetailHttpFailure:
    return DenmarkCvrCompanyDetailHttpFailure(
        cvr=cvr,
        source_url=company_detail_api_url("https://datacvr.virk.dk", cvr),
        status=status,
        response_headers={"content-type": "application/json"},
    )


def _duckdb_resource(path: Path) -> DuckDBResource:
    return DuckDBResource(database=str(path))


def test_company_detail_urls_are_exact_https_endpoints() -> None:
    assert company_detail_api_url("https://datacvr.virk.dk", "45448037") == (
        "https://datacvr.virk.dk/gateway/virksomhed/hentVirksomhed"
        "?cvrnummer=45448037&locale=en"
    )
    assert company_detail_page_url("https://datacvr.virk.dk", "45448037").startswith(
        "https://datacvr.virk.dk/enhed/virksomhed/45448037"
    )

    with pytest.raises(ValueError, match="HTTPS"):
        company_detail_api_url("http://datacvr.virk.dk", "45448037")
    with pytest.raises(ValueError, match="eight digits"):
        company_detail_api_url("https://datacvr.virk.dk", "4544")


def test_detail_resource_reuses_one_https_browser_session() -> None:
    first = {"stamdata": {"cvrnummer": "45448037"}}
    second = {"stamdata": {"cvrnummer": "22756214"}}
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {
                    "content-type": "application/json",
                    "set-cookie": "secret",
                },
                "body": json.dumps(first),
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(second),
            },
        ]
    )
    browser = FakeBrowser(page)
    sleeps: list[float] = []

    downloads = list(
        DenmarkCvrCompanyDetailResource(
            min_delay_ms=10, max_delay_ms=10
        ).iter_company_details(
            ("45448037", "22756214"),
            launcher=lambda: browser,
            sleep=sleeps.append,
        )
    )

    assert [download.payload for download in downloads] == [first, second]
    assert page.goto_calls == [
        (
            "https://datacvr.virk.dk/enhed/virksomhed/45448037?locale=en",
            "networkidle",
        )
    ]
    assert [call["url"] for call in page.evaluate_calls] == [
        company_detail_api_url("https://datacvr.virk.dk", "45448037"),
        company_detail_api_url("https://datacvr.virk.dk", "22756214"),
    ]
    assert sleeps == [0.01]
    assert downloads[0].response_headers == {"content-type": "application/json"}
    assert browser.closed is True


def test_detail_resource_retries_transient_503_without_aborting_batch() -> None:
    first = {"stamdata": {"cvrnummer": "45448037"}}
    second = {"stamdata": {"cvrnummer": "22756214"}}
    page = FakePage(
        [
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(first),
            },
            {
                "ok": False,
                "status": 503,
                "headers": {"retry-after": "2"},
                "body": "",
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(second),
            },
        ]
    )
    browser = FakeBrowser(page)
    sleeps: list[float] = []

    downloads = list(
        DenmarkCvrCompanyDetailResource(
            min_delay_ms=10,
            max_delay_ms=10,
            max_attempts=3,
            retry_base_delay_seconds=1,
            retry_max_delay_seconds=10,
        ).iter_company_details(
            ("45448037", "22756214"),
            launcher=lambda: browser,
            sleep=sleeps.append,
        )
    )

    assert [download.payload for download in downloads] == [first, second]
    assert [call["url"] for call in page.evaluate_calls] == [
        company_detail_api_url("https://datacvr.virk.dk", "45448037"),
        company_detail_api_url("https://datacvr.virk.dk", "22756214"),
        company_detail_api_url("https://datacvr.virk.dk", "22756214"),
    ]
    assert sleeps == [0.01, 2.0]
    assert browser.closed is True


def test_detail_resource_returns_exhausted_500_and_continues_batch() -> None:
    failed_cvr = "41387971"
    successful_cvr = "45448037"
    successful_payload = {"stamdata": {"cvrnummer": successful_cvr}}
    page = FakePage(
        [
            {
                "ok": False,
                "status": 500,
                "headers": {"content-type": "application/json"},
                "body": "",
            },
            {
                "ok": False,
                "status": 500,
                "headers": {"content-type": "application/json"},
                "body": "",
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(successful_payload),
            },
        ]
    )
    browser = FakeBrowser(page)

    results = list(
        DenmarkCvrCompanyDetailResource(
            min_delay_ms=0,
            max_delay_ms=0,
            max_attempts=2,
            retry_base_delay_seconds=0,
            retry_max_delay_seconds=0,
        ).iter_company_details(
            (failed_cvr, successful_cvr),
            launcher=lambda: browser,
            sleep=lambda _: None,
        )
    )

    assert results[0] == _http_failure(failed_cvr)
    assert isinstance(results[1], DenmarkCvrCompanyDetailDownload)
    assert results[1].payload == successful_payload
    assert len(page.evaluate_calls) == 3
    assert browser.closed is True


def test_detail_resource_does_not_suppress_exhausted_rate_limit() -> None:
    cvr = "41387971"
    browser = FakeBrowser(
        FakePage(
            [
                {
                    "ok": False,
                    "status": 429,
                    "headers": {"retry-after": "60"},
                    "body": "",
                }
            ]
        )
    )

    with pytest.raises(RuntimeError, match="HTTP 429"):
        list(
            DenmarkCvrCompanyDetailResource(
                min_delay_ms=0,
                max_delay_ms=0,
                max_attempts=1,
                retry_base_delay_seconds=0,
                retry_max_delay_seconds=0,
            ).iter_company_details(
                (cvr,),
                launcher=lambda: browser,
                sleep=lambda _: None,
            )
        )

    assert browser.closed is True


def test_company_detail_failure_is_ignored_only_after_24_hours(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "company-detail-failures.sqlite3"
    failure = _http_failure("41387971")
    first_failed_at = datetime(2026, 7, 22, 8, tzinfo=UTC)

    first = record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=first_failed_at,
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=company_detail_bucket_key(failure.cvr),
        source_run_id="run-1",
        failure_object_key="failure-1.json",
    )
    second = record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=first_failed_at + timedelta(hours=23),
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=company_detail_bucket_key(failure.cvr),
        source_run_id="run-2",
        failure_object_key="failure-2.json",
    )
    third = record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=first_failed_at + timedelta(hours=25),
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=company_detail_bucket_key(failure.cvr),
        source_run_id="run-3",
        failure_object_key="failure-3.json",
    )

    assert first.decision == "fail_partition"
    assert first.failure_count == 1
    assert second.decision == "fail_partition"
    assert second.failure_count == 2
    assert third.decision == "ignore_company"
    assert third.failure_count == 3
    assert third.first_failed_at == first_failed_at

    clear_company_detail_failure_history(database_path, failure.cvr)
    reset = record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=first_failed_at + timedelta(hours=26),
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=company_detail_bucket_key(failure.cvr),
        source_run_id="run-4",
        failure_object_key="failure-4.json",
    )
    assert reset.decision == "fail_partition"
    assert reset.failure_count == 1


def test_partition_writer_records_first_failure_before_failing(
    tmp_path: Path,
) -> None:
    failed_cvr = "41387971"
    partition_key = company_detail_bucket_key(failed_cvr)
    failure = _http_failure(failed_cvr)
    store = FakeObjectStore()
    failure_records = []

    with pytest.raises(
        RuntimeError,
        match="persistent failure attempt 1 was recorded",
    ):
        write_company_detail_partition(
            object_store=store,
            details=FakeDetailResource({failed_cvr: failure}),
            partition_key=partition_key,
            cvrs=(failed_cvr,),
            failure_database_path=(tmp_path / "company-detail-failures.sqlite3"),
            failure_suppression_age=timedelta(hours=24),
            observed_at=datetime(2026, 7, 22, 8, tzinfo=UTC),
            source_run_id="run-1",
            record_failure=failure_records.append,
        )

    assert len(failure_records) == 1
    assert failure_records[0].decision == "fail_partition"
    assert (
        company_detail_failure_object_key(partition_key, failed_cvr)
        not in store.objects
    )


def test_partition_writer_marks_repeated_failure_and_continues(
    tmp_path: Path,
) -> None:
    partition_key = "bucket_039"
    failed_cvr = "10000218"
    successful_cvr = "10000356"
    failure = _http_failure(failed_cvr)
    first_failed_at = datetime(2026, 7, 22, 8, tzinfo=UTC)
    database_path = tmp_path / "company-detail-failures.sqlite3"
    failure_key = company_detail_failure_object_key(partition_key, failed_cvr)
    record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=first_failed_at,
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=partition_key,
        source_run_id="run-1",
        failure_object_key=failure_key,
    )
    store = FakeObjectStore()
    detail_resource = FakeDetailResource(
        {
            failed_cvr: failure,
            successful_cvr: _download(
                successful_cvr,
                {"stamdata": {"cvrnummer": successful_cvr, "navn": "Success"}},
            ),
        }
    )
    failure_records = []

    summary = write_company_detail_partition(
        object_store=store,
        details=detail_resource,
        partition_key=partition_key,
        cvrs=(failed_cvr, successful_cvr),
        failure_database_path=database_path,
        failure_suppression_age=timedelta(hours=24),
        observed_at=first_failed_at + timedelta(hours=25),
        source_run_id="run-2",
        record_failure=failure_records.append,
    )

    assert detail_resource.requested_cvrs == [failed_cvr, successful_cvr]
    assert summary.complete_company_count == 1
    assert summary.ignored_company_count == 1
    assert summary.resolved_company_count == 2
    assert len(failure_records) == 1
    assert failure_records[0].decision == "ignore_company"
    marker = json.loads(store.objects[failure_key])
    assert marker["cvr"] == failed_cvr
    assert marker["http_status"] == 500
    assert marker["failure_count"] == 2
    assert "body" not in marker
    assert (
        company_detail_object_key(
            partition_key,
            successful_cvr,
            english_keys=False,
        )
        in store.objects
    )


def test_clickhouse_failure_record_contains_auditable_safe_fields(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "company-detail-failures.sqlite3"
    failure = _http_failure("41387971")
    record = record_company_detail_http_failure(
        database_path,
        failure=failure,
        failed_at=datetime(2026, 7, 23, 9, tzinfo=UTC),
        suppression_age=timedelta(hours=24),
        source_asset="denmark_cvr_company_details_s3",
        source_partition_key=company_detail_bucket_key(failure.cvr),
        source_run_id="run-1",
        failure_object_key="company_error.json",
    )

    class FakeClickHouseClient:
        def __init__(self) -> None:
            self.executions: list[tuple[str, list[tuple[Any, ...]]]] = []

        def execute(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            self.executions.append((sql, rows))

    client = FakeClickHouseClient()
    insert_company_detail_failure_record(client, record)

    assert len(client.executions) == 1
    sql, rows = client.executions[0]
    assert "corpscout.dk_cvr_company_detail_failures" in sql
    assert rows[0][0] == failure.cvr
    assert rows[0][1] == 500
    assert record.source_url not in str(record.response_headers)
    assert "body" not in str(rows[0]).lower()


def test_key_translation_is_recursive_and_does_not_translate_values() -> None:
    original = {
        "stamdata": {
            "cvrnummer": "45448037",
            "navn": "Dansk virksomhed",
            "reklamebeskyttet": False,
        },
        "produktionsenheder": {
            "aktiveProduktionsenheder": [
                {"stamdata": {"pnummer": "1020000001", "navn": "København"}}
            ]
        },
    }

    translated = translate_company_detail_keys(original)

    assert translated == {
        "masterData": {
            "companyRegistrationNumber": "45448037",
            "name": "Dansk virksomhed",
            "advertisingProtected": False,
        },
        "productionUnits": {
            "activeProductionUnits": [
                {
                    "masterData": {
                        "productionUnitNumber": "1020000001",
                        "name": "København",
                    }
                }
            ]
        },
    }
    assert original["stamdata"]["navn"] == "Dansk virksomhed"


def test_key_translation_accepts_camel_case_association_entity_number() -> None:
    original = {
        "foreningsrepraesentanter": [
            {
                "enhedsNummer": "4000000001",
                "indtraadtDato": "2020-01-02",
                "navn": "Example representative",
            }
        ]
    }

    assert translate_company_detail_keys(original) == {
        "associationRepresentatives": [
            {
                "entityNumber": "4000000001",
                "joinedDate": "2020-01-02",
                "name": "Example representative",
            }
        ]
    }


def test_key_translation_translates_parent_company_structure() -> None:
    original = {
        "hovedselskab": {
            "cvrNummer": "12345678",
            "navn": "Example parent company",
            "hjemsted": "København",
            "registreretMyndighed": "Erhvervsstyrelsen",
            "registreringsnummer": "DK-12345678",
            "tegnetKapital": "500000 DKK",
            "tegningsberettiget": ["Example signatory"],
        }
    }

    assert translate_company_detail_keys(original) == {
        "parentCompany": {
            "companyRegistrationNumber": "12345678",
            "name": "Example parent company",
            "registeredOffice": "København",
            "registrationAuthority": "Erhvervsstyrelsen",
            "registrationNumber": "DK-12345678",
            "subscribedCapital": "500000 DKK",
            "authorizedSignatories": ["Example signatory"],
        }
    }


def test_key_translation_translates_audit_firm_structure() -> None:
    original = {
        "oplysningerOmRevisionsvirksomhed": {
            "kontaktperson": "Example contact",
            "netvaerk": None,
            "samledeStemmeandel": "100%",
            "virksomhedstype": "Audit firm",
            "webadresse": "https://example.com",
        },
        "produktionsenheder": {
            "aktiveProduktionsenheder": [
                {
                    "revisionsvirksomhed": {
                        "tilknyttedeRevisorer": [
                            {
                                "mneNummer": "mne12345",
                                "tilknytning": "Affiliated",
                            }
                        ]
                    }
                }
            ]
        },
    }

    assert translate_company_detail_keys(original) == {
        "auditFirmInformation": {
            "contactPerson": "Example contact",
            "network": None,
            "totalVotingShare": "100%",
            "companyType": "Audit firm",
            "webAddress": "https://example.com",
        },
        "productionUnits": {
            "activeProductionUnits": [
                {
                    "auditFirm": {
                        "affiliatedAuditors": [
                            {
                                "mneNumber": "mne12345",
                                "affiliation": "Affiliated",
                            }
                        ]
                    }
                }
            ]
        },
    }


def test_key_translation_reports_all_unmapped_source_key_paths() -> None:
    with pytest.raises(DenmarkCvrCompanyDetailKeyError) as error:
        translate_company_detail_keys(
            {
                "stamdata": {
                    "foersteUkendteNoegle": "value",
                    "andenUkendteNoegle": {
                        "tredjeUkendteNoegle": "value",
                    },
                }
            }
        )

    message = str(error.value)
    assert "stamdata.foersteUkendteNoegle" in message
    assert "stamdata.andenUkendteNoegle" in message
    assert "stamdata.andenUkendteNoegle.tredjeUkendteNoegle" in message


def test_company_detail_hash_buckets_are_stable_and_bounded() -> None:
    assert company_detail_bucket_key("45448037") == "bucket_103"
    assert company_detail_bucket_key("45448037") == company_detail_bucket_key(
        "45448037"
    )
    assert len(DENMARK_CVR_COMPANY_DETAIL_PARTITIONS.get_partition_keys()) == 128
    assert DENMARK_CVR_COMPANY_DETAIL_PARTITIONS.get_first_partition_key() == (
        "bucket_000"
    )
    assert DENMARK_CVR_COMPANY_DETAIL_PARTITIONS.get_last_partition_key() == (
        "bucket_127"
    )


def test_partition_candidates_are_read_from_the_company_duckdb_table(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "denmark.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"CREATE SCHEMA {DENMARK_CVR_DUCKDB_SCHEMA}")
        connection.execute(
            f"CREATE TABLE {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE} "
            "(cvr VARCHAR)"
        )
        connection.executemany(
            f"INSERT INTO {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE} "
            "VALUES (?)",
            [("45448037",), ("22756214",), ("24256790",)],
        )

    selected_bucket = company_detail_bucket_key("45448037")
    selected = company_detail_partition_cvrs(
        _duckdb_resource(database_path),
        selected_bucket,
    )

    assert "45448037" in selected
    assert all(company_detail_bucket_key(cvr) == selected_bucket for cvr in selected)


def test_daily_candidates_are_selected_from_the_duckdb_source_partition(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "denmark.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"CREATE SCHEMA {DENMARK_CVR_DUCKDB_SCHEMA}")
        connection.execute(
            f"CREATE TABLE {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE} "
            "(cvr VARCHAR, source_capture_type VARCHAR, source_partition_key VARCHAR)"
        )
        connection.executemany(
            f"INSERT INTO {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE} "
            "VALUES (?, ?, ?)",
            [
                ("45448037", "active", "2026-07-17"),
                ("22756214", "active", "2026-07-17"),
                ("24256790", "active", "2026-07-18"),
                ("61126228", "backfill", "2026-07-17"),
            ],
        )

    assert company_detail_update_cvrs(
        _duckdb_resource(database_path), "2026-07-17"
    ) == ("22756214", "45448037")


def test_daily_detail_writer_versions_objects_by_update_date(tmp_path: Path) -> None:
    update_date = "2026-07-17"
    first_cvr = "45448037"
    second_cvr = "22756214"
    complete_original_key = company_detail_update_object_key(
        update_date,
        first_cvr,
        english_keys=False,
    )
    complete_english_key = company_detail_update_object_key(
        update_date,
        first_cvr,
        english_keys=True,
    )
    store = FakeObjectStore(
        {
            complete_original_key: b"{}",
            complete_english_key: b"{}",
        }
    )
    detail_resource = FakeDetailResource(
        {
            second_cvr: _download(
                second_cvr,
                {"stamdata": {"cvrnummer": second_cvr, "navn": "Updated"}},
            )
        }
    )

    summary = write_company_detail_updates(
        object_store=store,
        details=detail_resource,
        update_date=update_date,
        cvrs=(first_cvr, second_cvr),
        failure_database_path=tmp_path / "company-detail-failures.sqlite3",
        failure_suppression_age=timedelta(hours=24),
        observed_at=datetime(2026, 7, 17, tzinfo=UTC),
        source_run_id="test-run",
        record_failure=lambda _: None,
    )

    assert detail_resource.requested_cvrs == [second_cvr]
    assert summary.already_complete_company_count == 1
    assert summary.downloaded_company_count == 1
    assert (
        company_detail_update_object_key(
            update_date,
            second_cvr,
            english_keys=False,
        )
        in store.objects
    )
    assert (
        company_detail_update_object_key(
            update_date,
            second_cvr,
            english_keys=True,
        )
        in store.objects
    )


def test_partition_writer_reuses_original_json_and_checkpoints_each_download(
    tmp_path: Path,
) -> None:
    partition_key = "bucket_039"
    complete_cvr = "10000218"
    original_only_cvr = "10000356"
    download_cvr = "10000446"
    complete_original_key = company_detail_object_key(
        partition_key, complete_cvr, english_keys=False
    )
    complete_english_key = company_detail_object_key(
        partition_key, complete_cvr, english_keys=True
    )
    original_only_key = company_detail_object_key(
        partition_key, original_only_cvr, english_keys=False
    )
    original_only_body = json.dumps(
        {"stamdata": {"cvrnummer": original_only_cvr, "navn": "Dansk navn"}}
    ).encode()
    store = FakeObjectStore(
        {
            complete_original_key: b"{}",
            complete_english_key: b"{}",
            original_only_key: original_only_body,
        }
    )
    detail_resource = FakeDetailResource(
        {
            download_cvr: _download(
                download_cvr,
                {"stamdata": {"cvrnummer": download_cvr, "navn": "Nyt navn"}},
            )
        }
    )

    summary = write_company_detail_partition(
        object_store=store,
        details=detail_resource,
        partition_key=partition_key,
        cvrs=(complete_cvr, original_only_cvr, download_cvr),
        failure_database_path=tmp_path / "company-detail-failures.sqlite3",
        failure_suppression_age=timedelta(hours=24),
        observed_at=datetime(2026, 7, 17, tzinfo=UTC),
        source_run_id="test-run",
        record_failure=lambda _: None,
    )

    assert detail_resource.requested_cvrs == [download_cvr]
    assert summary.selected_company_count == 3
    assert summary.complete_company_count == 3
    assert summary.already_complete_company_count == 1
    assert summary.translated_existing_company_count == 1
    assert summary.downloaded_company_count == 1
    assert summary.written_object_count == 3
    assert store.objects[original_only_key] == original_only_body
    translated_existing = json.loads(
        store.objects[
            company_detail_object_key(
                partition_key, original_only_cvr, english_keys=True
            )
        ]
    )
    assert translated_existing["masterData"]["name"] == "Dansk navn"
    assert (
        store.objects[
            company_detail_object_key(partition_key, download_cvr, english_keys=False)
        ]
        == detail_resource.downloads[download_cvr].raw_body.encode()
    )


def test_company_detail_asset_has_own_group_hash_partitions_and_dependency() -> None:
    spec = denmark_cvr_company_details_s3.get_asset_spec()

    assert spec.group_name == "denmark_cvr_company_details"
    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey("denmark_cvr_companies_duckdb")
    }
    assert (
        denmark_cvr_company_details_s3.partitions_def
        is DENMARK_CVR_COMPANY_DETAIL_PARTITIONS
    )
    assert denmark_cvr_company_details_s3.backfill_policy == (
        dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )
    assert denmark_cvr_company_details_s3.op.pool == "denmark_cvr_company_details"
    assert spec.tags["layer"] == "raw_detail"
    assert len(defs.assets) == 2
    assert set(defs.resources) == {
        "denmark_cvr_company_details",
        "denmark_cvr_duckdb",
    }


def test_company_detail_asset_is_registered_in_workspace_definitions() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("denmark_cvr_company_details_s3"))

    assert node.group_name == "denmark_cvr_company_details"
    assert node.parent_keys == {dg.AssetKey("denmark_cvr_companies_duckdb")}


def test_daily_detail_asset_uses_company_active_partitions() -> None:
    from dagster_v3.defs.denmark_cvr.partitions import DENMARK_CVR_ACTIVE_PARTITIONS

    spec = denmark_cvr_company_detail_updates_s3.get_asset_spec()

    assert spec.group_name == "denmark_cvr_company_details"
    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey("denmark_cvr_companies_duckdb")
    }
    assert (
        denmark_cvr_company_detail_updates_s3.partitions_def
        is DENMARK_CVR_ACTIVE_PARTITIONS
    )
    assert denmark_cvr_company_detail_updates_s3.backfill_policy == (
        dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )
    assert (
        denmark_cvr_company_detail_updates_s3.op.pool == "denmark_cvr_company_details"
    )
