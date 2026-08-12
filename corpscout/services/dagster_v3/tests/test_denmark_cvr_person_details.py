import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.denmark_cvr.company_details import (
    company_detail_bucket_key,
    company_detail_failure_object_key,
    company_detail_object_key,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_SCHEMA,
)
from dagster_v3.defs.denmark_cvr.person_details import (
    DENMARK_CVR_PERSON_DETAIL_PARTITIONS,
    DENMARK_CVR_PERSON_IDS_TABLE,
    DenmarkCvrPersonDetailDownload,
    DenmarkCvrPersonDetailHttpFailure,
    DenmarkCvrPersonDetailIdentity,
    DenmarkCvrPersonDetailResource,
    company_detail_person_identities,
    denmark_cvr_company_detail_person_ids_duckdb,
    denmark_cvr_person_details_s3,
    person_detail_api_url,
    person_detail_bucket_key,
    person_detail_failure_object_key,
    person_detail_object_key,
    person_detail_partition_identities,
    rebuild_company_detail_person_ids,
    translate_person_detail_keys,
    write_person_detail_partition,
)


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})

    def ensure_bucket(self, _bucket: str) -> None:
        return None

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        del bucket
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        del bucket
        return self.objects[key]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        del bucket
        self.objects[key] = body


class FakePersonDetailResource:
    def __init__(
        self,
        downloads: dict[
            str,
            DenmarkCvrPersonDetailDownload | DenmarkCvrPersonDetailHttpFailure,
        ],
    ) -> None:
        self.downloads = downloads
        self.requested: list[DenmarkCvrPersonDetailIdentity] = []

    def iter_person_details(
        self,
        identities: tuple[DenmarkCvrPersonDetailIdentity, ...],
    ) -> tuple[
        DenmarkCvrPersonDetailDownload | DenmarkCvrPersonDetailHttpFailure,
        ...,
    ]:
        self.requested.extend(identities)
        return tuple(self.downloads[identity.person_id] for identity in identities)


class FakePage:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = list(results)
        self.evaluate_calls: list[dict[str, str]] = []

    def goto(self, _url: str, *, wait_until: str) -> None:
        assert wait_until == "networkidle"

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


def test_company_detail_person_identities_include_active_and_ceased_people() -> None:
    payload = {
        "personkreds": {
            "personkredser": [
                {
                    "personRoller": [
                        {
                            "id": "4000000001",
                            "enhedstype": "PERSON",
                            "personType": "deltager",
                        },
                        {
                            "id": "4000000002",
                            "enhedstype": "VIRKSOMHED",
                            "personType": None,
                        },
                    ]
                }
            ],
            "ophoerteFad": [
                {
                    "id": "4000000003",
                    "enhedstype": "person",
                    "personType": "deltager",
                },
                {
                    "id": "4000000001",
                    "enhedstype": "person",
                    "personType": "deltager",
                },
            ],
        }
    }

    assert company_detail_person_identities(payload) == (
        DenmarkCvrPersonDetailIdentity("4000000001", "deltager"),
        DenmarkCvrPersonDetailIdentity("4000000003", "deltager"),
    )


def test_company_detail_person_identities_accept_variable_length_numeric_ids() -> None:
    payload = {
        "personkreds": {
            "personkredser": [
                {
                    "personRoller": [
                        {
                            "id": "4082628",
                            "enhedstype": "person",
                            "personType": "deltager",
                        }
                    ]
                }
            ],
            "ophoerteFad": [],
        }
    }

    assert company_detail_person_identities(payload) == (
        DenmarkCvrPersonDetailIdentity("4082628", "deltager"),
    )


def test_person_detail_hash_partitions_are_stable_and_bounded() -> None:
    assert person_detail_bucket_key("4000000001") == person_detail_bucket_key(
        "4000000001"
    )
    assert len(DENMARK_CVR_PERSON_DETAIL_PARTITIONS.get_partition_keys()) == 128
    assert DENMARK_CVR_PERSON_DETAIL_PARTITIONS.get_first_partition_key() == (
        "bucket_000"
    )
    assert DENMARK_CVR_PERSON_DETAIL_PARTITIONS.get_last_partition_key() == (
        "bucket_127"
    )


def test_person_detail_api_is_https_and_includes_person_type() -> None:
    identity = DenmarkCvrPersonDetailIdentity("4000000001", "deltager")

    assert person_detail_api_url("https://datacvr.virk.dk", identity) == (
        "https://datacvr.virk.dk/gateway/person/hentPerson?"
        "enhedsnummer=4000000001&persontype=deltager&locale=en"
    )
    with pytest.raises(ValueError, match="HTTPS"):
        person_detail_api_url("http://datacvr.virk.dk", identity)


def test_person_detail_resource_retries_rate_limits_in_one_browser() -> None:
    identity = DenmarkCvrPersonDetailIdentity("4000000001", "deltager")
    payload = {
        "stamdata": {},
        "personRelationer": {},
    }
    page = FakePage(
        [
            {
                "ok": False,
                "status": 429,
                "headers": {
                    "content-type": "application/json",
                    "retry-after": "2",
                },
                "body": "{}",
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(payload),
            },
        ]
    )
    browser = FakeBrowser(page)
    sleeps: list[float] = []

    downloads = list(
        DenmarkCvrPersonDetailResource(
            min_delay_ms=0,
            max_delay_ms=0,
            max_attempts=3,
            retry_base_delay_seconds=1,
            retry_max_delay_seconds=10,
        ).iter_person_details(
            (identity,),
            launcher=lambda: browser,
            sleep=sleeps.append,
        )
    )

    assert [download.payload for download in downloads] == [payload]
    assert len(page.evaluate_calls) == 2
    assert sleeps == [2.0]
    assert browser.closed is True


def test_person_detail_resource_returns_404_and_continues_batch() -> None:
    missing = DenmarkCvrPersonDetailIdentity("4010858579", "deltager")
    available = DenmarkCvrPersonDetailIdentity("4000000001", "deltager")
    payload = {"stamdata": {}, "personRelationer": {}}
    page = FakePage(
        [
            {
                "ok": False,
                "status": 404,
                "headers": {"content-type": "application/json"},
                "body": '{"message":"not found"}',
            },
            {
                "ok": True,
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": json.dumps(payload),
            },
        ]
    )
    browser = FakeBrowser(page)
    sleeps: list[float] = []

    results = list(
        DenmarkCvrPersonDetailResource(
            min_delay_ms=0,
            max_delay_ms=0,
        ).iter_person_details(
            (missing, available),
            launcher=lambda: browser,
            sleep=sleeps.append,
        )
    )

    assert results[0] == DenmarkCvrPersonDetailHttpFailure(
        identity=missing,
        source_url=person_detail_api_url("https://datacvr.virk.dk", missing),
        status=404,
        attempt_count=1,
        response_headers={"content-type": "application/json"},
    )
    assert isinstance(results[1], DenmarkCvrPersonDetailDownload)
    assert results[1].payload == payload
    assert len(page.evaluate_calls) == 2
    assert sleeps == [0.0]
    assert browser.closed is True


def test_person_detail_resource_does_not_suppress_server_failure() -> None:
    identity = DenmarkCvrPersonDetailIdentity("4000000001", "deltager")
    browser = FakeBrowser(
        FakePage(
            [
                {
                    "ok": False,
                    "status": 500,
                    "headers": {"content-type": "application/json"},
                    "body": "{}",
                }
            ]
        )
    )

    with pytest.raises(RuntimeError, match="HTTP 500"):
        list(
            DenmarkCvrPersonDetailResource(
                min_delay_ms=0,
                max_delay_ms=0,
                max_attempts=1,
                retry_base_delay_seconds=0,
                retry_max_delay_seconds=0,
            ).iter_person_details(
                (identity,),
                launcher=lambda: browser,
                sleep=lambda _: None,
            )
        )

    assert browser.closed is True


def test_person_detail_translation_changes_keys_without_changing_values() -> None:
    original = {
        "konkurskarantaene": None,
        "liberalUdoeverRegistreringer": None,
        "liberaleErhverv": None,
        "personRelationer": {
            "aktiveRelationer": [],
            "ophoerteRelationer": [],
            "simpleRelationer": [],
        },
        "skjulRelationer": False,
        "stamdata": {
            "adresse": "Example address",
            "adresseHemmelig": False,
            "adresseHemmeligUndtagelse": False,
            "adresseOpdateringOphoert": False,
            "aktiveHvidvaskAktiviteter": [],
            "franchise": None,
            "land": "DK",
            "navn": "Example person",
            "postnummerOgBy": "1000 København",
            "registreretIHvidvask": False,
            "tilknytning": [],
            "udenlandskAdresse": None,
        },
    }

    translated = translate_person_detail_keys(original)

    assert translated == {
        "bankruptcyDisqualification": None,
        "liberalPractitionerRegistrations": None,
        "liberalProfessions": None,
        "personRelations": {
            "activeRelations": [],
            "ceasedRelations": [],
            "simpleRelations": [],
        },
        "hideRelations": False,
        "masterData": {
            "address": "Example address",
            "addressConfidential": False,
            "addressConfidentialException": False,
            "addressUpdateCeased": False,
            "activeAntiMoneyLaunderingActivities": [],
            "franchise": None,
            "country": "DK",
            "name": "Example person",
            "postalCodeAndCity": "1000 København",
            "registeredInAntiMoneyLaunderingRegister": False,
            "affiliation": [],
            "foreignAddress": None,
        },
    }


def test_person_id_catalog_requires_complete_company_detail_objects(
    tmp_path: Path,
) -> None:
    cvr = "45448037"
    database = tmp_path / "denmark.duckdb"
    _create_company_table(database, (cvr,))
    partition = company_detail_bucket_key(cvr)
    original_key = company_detail_object_key(partition, cvr, english_keys=False)
    store = FakeObjectStore({original_key: _company_detail_bytes(cvr)})

    with pytest.raises(RuntimeError, match="not fully materialized"):
        rebuild_company_detail_person_ids(
            object_store=store,
            denmark_cvr_duckdb=_duckdb_resource(database),
            rebuilt_at=datetime(2026, 7, 19, tzinfo=UTC),
        )


def test_person_id_catalog_builds_deduplicated_duckdb_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.denmark_cvr import person_details

    monkeypatch.setattr(
        person_details,
        "DENMARK_CVR_PERSON_ID_INSERT_BATCH_ROWS",
        1,
    )
    first_cvr = "45448037"
    second_cvr = "22756214"
    database = tmp_path / "denmark.duckdb"
    _create_company_table(database, (first_cvr, second_cvr))
    objects: dict[str, bytes] = {}
    for cvr in (first_cvr, second_cvr):
        partition = company_detail_bucket_key(cvr)
        objects[company_detail_object_key(partition, cvr, english_keys=False)] = (
            _company_detail_bytes(cvr)
        )
        objects[company_detail_object_key(partition, cvr, english_keys=True)] = b"{}"

    progress_messages: list[str] = []

    def record_progress(message: str, *args: object) -> None:
        progress_messages.append(message % args)

    summary = rebuild_company_detail_person_ids(
        object_store=FakeObjectStore(objects),
        denmark_cvr_duckdb=_duckdb_resource(database),
        rebuilt_at=datetime(2026, 7, 19, tzinfo=UTC),
        log_info=record_progress,
    )

    assert summary.company_count == 2
    assert summary.person_count == 1
    assert progress_messages[0] == (
        "DataCVR person-ID catalog started: companies_total=2 company_buckets_total=128"
    )
    assert progress_messages[1] == (
        "DataCVR person-ID catalog progress: phase=snapshot_validation "
        "company_buckets=0/128 companies_checked=0/2 companies_ignored=0 "
        "missing_original=0 missing_english=0"
    )
    assert any(
        "phase=snapshot_validation company_buckets=128/128 "
        "companies_checked=2/2" in message
        for message in progress_messages
    )
    assert any(
        "phase=identity_extraction companies_processed=1/2" in message
        for message in progress_messages
    )
    assert any(
        "phase=identity_extraction companies_processed=2/2" in message
        for message in progress_messages
    )
    assert progress_messages[-1] == (
        "DataCVR person-ID catalog completed: companies_processed=2/2 "
        "companies_ignored=0 relations=2 persons=1"
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        row = connection.execute(
            f"select person_id, person_type, source_company_count "
            f"from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSON_IDS_TABLE}"
        ).fetchone()
    assert row == ("4000000001", "deltager", 2)


def test_person_id_catalog_accepts_explicit_ignored_company_marker(
    tmp_path: Path,
) -> None:
    available_cvr = "45448037"
    ignored_cvr = "22756214"
    database = tmp_path / "denmark.duckdb"
    _create_company_table(database, (available_cvr, ignored_cvr))
    available_partition = company_detail_bucket_key(available_cvr)
    ignored_partition = company_detail_bucket_key(ignored_cvr)
    objects = {
        company_detail_object_key(
            available_partition,
            available_cvr,
            english_keys=False,
        ): _company_detail_bytes(available_cvr),
        company_detail_object_key(
            available_partition,
            available_cvr,
            english_keys=True,
        ): b"{}",
        company_detail_failure_object_key(
            ignored_partition,
            ignored_cvr,
        ): b'{"decision":"ignore_company"}',
    }

    summary = rebuild_company_detail_person_ids(
        object_store=FakeObjectStore(objects),
        denmark_cvr_duckdb=_duckdb_resource(database),
        rebuilt_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    assert summary.company_count == 2
    assert summary.source_object_count == 1
    assert summary.ignored_company_count == 1
    assert summary.person_count == 1


def test_person_partition_candidates_are_read_from_duckdb(tmp_path: Path) -> None:
    database = tmp_path / "denmark.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(f"create schema {DENMARK_CVR_DUCKDB_SCHEMA}")
        connection.execute(
            f"create table {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_PERSON_IDS_TABLE} "
            "(person_id varchar, person_type varchar, source_company_count bigint)"
        )
        connection.executemany(
            f"insert into {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_PERSON_IDS_TABLE} values (?, ?, ?)",
            [
                ("4000000001", "deltager", 2),
                ("4000000002", "deltager", 1),
            ],
        )
    partition = person_detail_bucket_key("4000000001")

    identities = person_detail_partition_identities(
        _duckdb_resource(database),
        partition,
    )

    assert DenmarkCvrPersonDetailIdentity("4000000001", "deltager") in identities
    assert all(
        person_detail_bucket_key(item.person_id) == partition for item in identities
    )


def test_person_detail_writer_checkpoints_original_and_english_json() -> None:
    first = DenmarkCvrPersonDetailIdentity("4000000001", "deltager")
    second = DenmarkCvrPersonDetailIdentity("4000000002", "deltager")
    partition = person_detail_bucket_key(first.person_id)
    if person_detail_bucket_key(second.person_id) != partition:
        second = _identity_in_partition(partition, start=3)
    first_original = person_detail_object_key(
        partition, first.person_id, english_keys=False
    )
    first_english = person_detail_object_key(
        partition, first.person_id, english_keys=True
    )
    store = FakeObjectStore({first_original: b"{}", first_english: b"{}"})
    download = DenmarkCvrPersonDetailDownload(
        identity=second,
        source_url=person_detail_api_url("https://datacvr.virk.dk", second),
        raw_body=json.dumps({"stamdata": {"navn": "Example"}}),
        payload={"stamdata": {"navn": "Example"}},
        status=200,
        response_headers={"content-type": "application/json"},
    )
    details = FakePersonDetailResource({second.person_id: download})
    progress_messages: list[str] = []

    def record_progress(message: str, *args: object) -> None:
        progress_messages.append(message % args)

    summary = write_person_detail_partition(
        object_store=store,
        details=details,
        partition_key=partition,
        identities=(first, second),
        observed_at=datetime(2026, 8, 11, tzinfo=UTC),
        source_run_id="run-1",
        log_info=record_progress,
    )

    assert details.requested == [second]
    assert summary.already_complete_person_count == 1
    assert summary.downloaded_person_count == 1
    assert (
        person_detail_object_key(partition, second.person_id, english_keys=False)
        in store.objects
    )
    assert (
        person_detail_object_key(partition, second.person_id, english_keys=True)
        in store.objects
    )
    assert progress_messages == [
        f"DataCVR person-detail started: partition={partition} persons_total=2",
        f"DataCVR person-detail progress: phase=snapshot_scan partition={partition} "
        "persons_checked=2/2 already_complete=1 already_skipped=0 "
        "translated_existing=0 persons_to_download=1",
        f"DataCVR person-detail progress: phase=download partition={partition} "
        "persons_downloaded=1/1 downloaded_bytes=33",
        f"DataCVR person-detail completed: partition={partition} "
        "persons_resolved=2/2 persons_completed=2 already_complete=1 "
        "already_skipped=0 translated_existing=0 downloaded=1 skipped=0",
    ]


def test_person_detail_writer_checkpoints_404_and_reuses_marker() -> None:
    missing = DenmarkCvrPersonDetailIdentity("4010858579", "deltager")
    partition = person_detail_bucket_key(missing.person_id)
    available = _identity_in_partition(partition, start=1)
    failure = DenmarkCvrPersonDetailHttpFailure(
        identity=missing,
        source_url=person_detail_api_url("https://datacvr.virk.dk", missing),
        status=404,
        attempt_count=1,
        response_headers={"content-type": "application/json"},
    )
    payload = {"stamdata": {"navn": "Available"}}
    download = DenmarkCvrPersonDetailDownload(
        identity=available,
        source_url=person_detail_api_url("https://datacvr.virk.dk", available),
        raw_body=json.dumps(payload),
        payload=payload,
        status=200,
        response_headers={"content-type": "application/json"},
    )
    store = FakeObjectStore()
    details = FakePersonDetailResource(
        {
            missing.person_id: failure,
            available.person_id: download,
        }
    )
    warnings: list[str] = []

    summary = write_person_detail_partition(
        object_store=store,
        details=details,
        partition_key=partition,
        identities=(missing, available),
        observed_at=datetime(2026, 8, 11, 9, tzinfo=UTC),
        source_run_id="run-404",
        log_warning=lambda message, *args: warnings.append(message % args),
    )

    marker_key = person_detail_failure_object_key(partition, missing.person_id)
    marker = json.loads(store.objects[marker_key])
    assert details.requested == [missing, available]
    assert summary.complete_person_count == 1
    assert summary.skipped_person_count == 1
    assert summary.already_skipped_person_count == 0
    assert summary.skipped_request_attempt_count == 1
    assert summary.resolved_person_count == 2
    assert marker == {
        "decision": "ignore_person",
        "failed_at": "2026-08-11T09:00:00+00:00",
        "http_status": 404,
        "person_id": missing.person_id,
        "person_type": missing.person_type,
        "request_attempt_count": 1,
        "source_asset": "denmark_cvr_person_details_s3",
        "source_partition_key": partition,
        "source_run_id": "run-404",
        "source_url": failure.source_url,
    }
    assert "not found" not in store.objects[marker_key].decode("utf-8")
    assert warnings == [
        "Skipping DataCVR person detail after terminal response: "
        f"partition={partition} person_id={missing.person_id} http_status=404 "
        f"request_attempts=1 marker={marker_key}"
    ]

    rerun_details = FakePersonDetailResource({})
    rerun = write_person_detail_partition(
        object_store=store,
        details=rerun_details,
        partition_key=partition,
        identities=(missing, available),
        observed_at=datetime(2026, 8, 11, 10, tzinfo=UTC),
        source_run_id="run-rerun",
    )

    assert rerun_details.requested == []
    assert rerun.already_complete_person_count == 1
    assert rerun.already_skipped_person_count == 1
    assert rerun.resolved_person_count == 2


def test_person_detail_assets_have_catalog_dependency_and_id_partitions() -> None:
    catalog_spec = denmark_cvr_company_detail_person_ids_duckdb.get_asset_spec()
    detail_spec = denmark_cvr_person_details_s3.get_asset_spec()

    assert {dependency.asset_key for dependency in catalog_spec.deps} == {
        dg.AssetKey("denmark_cvr_companies_duckdb"),
        dg.AssetKey("denmark_cvr_company_details_s3"),
    }
    assert {dependency.asset_key for dependency in detail_spec.deps} == {
        dg.AssetKey("denmark_cvr_company_detail_person_ids_duckdb")
    }
    assert (
        denmark_cvr_person_details_s3.partitions_def
        is DENMARK_CVR_PERSON_DETAIL_PARTITIONS
    )
    assert denmark_cvr_person_details_s3.backfill_policy == (
        dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    )


def _company_detail_bytes(cvr: str) -> bytes:
    return json.dumps(
        {
            "stamdata": {"cvrnummer": cvr},
            "personkreds": {
                "personkredser": [
                    {
                        "personRoller": [
                            {
                                "id": "4000000001",
                                "enhedstype": "person",
                                "personType": "deltager",
                            }
                        ]
                    }
                ],
                "ophoerteFad": [],
            },
        },
        separators=(",", ":"),
    ).encode()


def _create_company_table(database: Path, cvrs: tuple[str, ...]) -> None:
    with duckdb.connect(str(database)) as connection:
        connection.execute(f"create schema {DENMARK_CVR_DUCKDB_SCHEMA}")
        connection.execute(
            f"create table {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_COMPANIES_TABLE} (cvr varchar)"
        )
        connection.executemany(
            f"insert into {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_COMPANIES_TABLE} values (?)",
            [(cvr,) for cvr in cvrs],
        )


def _duckdb_resource(database: Path) -> DuckDBResource:
    return DuckDBResource(database=str(database))


def _identity_in_partition(
    partition: str,
    *,
    start: int,
) -> DenmarkCvrPersonDetailIdentity:
    for suffix in range(start, 10_000):
        identity = DenmarkCvrPersonDetailIdentity(f"400000{suffix:04d}", "deltager")
        if person_detail_bucket_key(identity.person_id) == partition:
            return identity
    raise AssertionError(f"No test identity found for {partition}")
