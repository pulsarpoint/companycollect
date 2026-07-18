import json
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DenmarkCvrCompanyDetailDownload,
    DenmarkCvrCompanyDetailKeyError,
    DenmarkCvrCompanyDetailResource,
    company_detail_api_url,
    company_detail_bucket_key,
    company_detail_object_key,
    company_detail_page_url,
    company_detail_partition_cvrs,
    denmark_cvr_company_details_s3,
    defs,
    translate_company_detail_keys,
    write_company_detail_partition,
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
    def __init__(self, downloads: dict[str, DenmarkCvrCompanyDetailDownload]) -> None:
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


def test_key_translation_rejects_unmapped_source_keys() -> None:
    with pytest.raises(DenmarkCvrCompanyDetailKeyError, match="ukendtNoegle"):
        translate_company_detail_keys({"stamdata": {"ukendtNoegle": "value"}})


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


def test_partition_writer_reuses_original_json_and_checkpoints_each_download() -> None:
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
    assert len(defs.assets) == 1
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
