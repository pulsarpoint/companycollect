import gzip
from io import BytesIO
import json
from pathlib import Path
from typing import Any

import requests
from botocore.exceptions import ClientError

from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource


EXPECTED_ENTITY_RECORD_KEYS = {
    "org_number",
    "change_type",
    "source_change_type",
    "updated_at",
    "update_id",
    "entity_url",
    "entity",
    "raw_update",
}


class FakeResponse:
    def __init__(
        self, *, status_code: int = 200, payload: Any = None, content: bytes = b""
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self.text = (
            json.dumps(payload)
            if payload is not None
            else content.decode("utf-8", "ignore")
        )
        self.raw = BytesIO(content)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self) -> Any:
        return self._payload

    def iter_content(self, chunk_size: int = 1024 * 1024) -> Any:
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]


class FakeHttpSession:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any] | None, int, bool]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: int = 120,
        stream: bool = False,
    ) -> FakeResponse:
        self.calls.append((url, params, timeout, stream))
        return self.responses[url]


class ParamAwareFakeHttpSession:
    def __init__(
        self, responses: dict[tuple[str, str, str, int], FakeResponse]
    ) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any] | None, int, bool]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: int = 120,
        stream: bool = False,
    ) -> FakeResponse:
        self.calls.append((url, params, timeout, stream))
        params = params or {}
        key = (
            url,
            str(params.get("dato")),
            str(params.get("updatedBefore")),
            int(params.get("page", 0)),
        )
        return self.responses[key]


class FakeDagsterS3Resource:
    def __init__(self, client: "FakeS3Client") -> None:
        self._client = client

    def get_client(self) -> "FakeS3Client":
        return self._client


class FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}
        self.head_calls: list[tuple[str, str]] = []
        self.upload_calls: list[tuple[str, str]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.head_calls.append((Bucket, Key))
        object_key = (Bucket, Key)
        if object_key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
        return {"ContentLength": len(self.objects[object_key])}

    def upload_fileobj(self, Fileobj: Any, Bucket: str, Key: str) -> None:
        self.upload_calls.append((Bucket, Key))
        self.objects[(Bucket, Key)] = Fileobj.read()


def test_resource_does_not_expose_direct_snapshot_download_helpers() -> None:
    assert not hasattr(NorwayBrregApiResource, "download_entities_snapshot")
    assert not hasattr(NorwayBrregApiResource, "iter_all_entities")


def test_entries_snapshot_reuses_existing_s3_object_without_http_download() -> None:
    s3_client = FakeS3Client(
        {
            (
                "source-norway-brreg",
                "norway_brreg/entities/raw/snapshot/entities.json.gz",
            ): b"existing"
        }
    )
    session = FakeHttpSession({})
    resource = NorwayBrregApiResource(session=session)

    metadata = resource.entries_snapshot(
        s3=FakeDagsterS3Resource(s3_client),
        bucket="source-norway-brreg",
        key="norway_brreg/entities/raw/snapshot/entities.json.gz",
    )

    assert metadata == {
        "s3_bucket": "source-norway-brreg",
        "s3_key": "norway_brreg/entities/raw/snapshot/entities.json.gz",
        "downloaded": False,
        "bytes_downloaded": 8,
    }
    assert session.calls == []
    assert s3_client.upload_calls == []


def test_entries_snapshot_streams_missing_snapshot_to_s3() -> None:
    snapshot_body = gzip.compress(b'[{"organisasjonsnummer":"923609016"}]')
    s3_client = FakeS3Client()
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned": FakeResponse(
                content=snapshot_body
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)

    metadata = resource.entries_snapshot(
        s3=FakeDagsterS3Resource(s3_client),
        bucket="source-norway-brreg",
        key="norway_brreg/entities/raw/snapshot/entities.json.gz",
    )

    assert metadata == {
        "s3_bucket": "source-norway-brreg",
        "s3_key": "norway_brreg/entities/raw/snapshot/entities.json.gz",
        "downloaded": True,
        "bytes_downloaded": len(snapshot_body),
    }
    assert session.calls == [
        (
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned",
            None,
            120,
            True,
        )
    ]
    assert s3_client.upload_calls == [
        ("source-norway-brreg", "norway_brreg/entities/raw/snapshot/entities.json.gz")
    ]
    assert (
        s3_client.objects[
            (
                "source-norway-brreg",
                "norway_brreg/entities/raw/snapshot/entities.json.gz",
            )
        ]
        == snapshot_body
    )


def test_entries_snapshot_csv_uses_bulk_csv_endpoint() -> None:
    snapshot_body = gzip.compress(
        b"organisasjonsnummer,epostadresse\n1000,post@example.no\n"
    )
    s3_client = FakeS3Client()
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv": FakeResponse(
                content=snapshot_body
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)

    metadata = resource.entries_snapshot_csv(
        s3=FakeDagsterS3Resource(s3_client),
        bucket="source-norway-brreg",
        key="norway_brreg/entities/raw/snapshot/entities.csv.gz",
    )

    assert metadata["downloaded"] is True
    assert session.calls == [
        (
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv",
            None,
            120,
            True,
        )
    ]
    assert (
        s3_client.objects[
            (
                "source-norway-brreg",
                "norway_brreg/entities/raw/snapshot/entities.csv.gz",
            )
        ]
        == snapshot_body
    )


def test_iter_updated_entities_returns_same_shape_and_hydrates_changed_entities() -> (
    None
):
    entity = _load_entity_fixture()
    update = {
        "oppdateringsid": 24720423,
        "dato": "2026-06-28T00:20:10.625Z",
        "organisasjonsnummer": "923609016",
        "endringstype": "Endring",
        "endringer": [{"op": "replace", "path": "/navn", "value": "EQUINOR ASA"}],
        "_links": {
            "enhet": {
                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
            }
        },
    }
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter": FakeResponse(
                payload={
                    "_embedded": {"oppdaterteEnheter": [update]},
                    "page": {
                        "size": 10000,
                        "totalElements": 1,
                        "totalPages": 1,
                        "number": 0,
                    },
                }
            ),
            "https://data.brreg.no/enhetsregisteret/api/enheter/923609016": FakeResponse(
                payload=entity
            ),
        }
    )
    resource = NorwayBrregApiResource(session=session)

    records = list(
        resource.iter_updated_entities(
            start="2026-06-28T00:00:00.000Z",
            end="2026-06-29T00:00:00.000Z",
            include_changes=True,
        )
    )

    assert len(records) == 1
    assert set(records[0]) == EXPECTED_ENTITY_RECORD_KEYS
    assert records[0]["org_number"] == "923609016"
    assert records[0]["change_type"] == "changed"
    assert records[0]["source_change_type"] == "Endring"
    assert records[0]["updated_at"] == "2026-06-28T00:20:10.625Z"
    assert records[0]["update_id"] == 24720423
    assert (
        records[0]["entity_url"]
        == "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
    )
    assert records[0]["entity"] == entity
    assert records[0]["raw_update"] == update
    assert session.calls[0][1] == {
        "dato": "2026-06-28T00:00:00.000Z",
        "updatedBefore": "2026-06-29T00:00:00.000Z",
        "size": 10000,
        "page": 0,
        "sort": "id,ASC",
        "includeChanges": "true",
    }


def test_iter_updated_entities_emits_page_and_hydration_progress_logs() -> None:
    entity = _load_entity_fixture()
    update = {
        "oppdateringsid": 24720423,
        "dato": "2026-06-28T00:20:10.625Z",
        "organisasjonsnummer": "923609016",
        "endringstype": "Endring",
        "endringer": [{"op": "replace", "path": "/navn", "value": "EQUINOR ASA"}],
        "_links": {
            "enhet": {
                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
            }
        },
    }
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter": FakeResponse(
                payload={
                    "_embedded": {"oppdaterteEnheter": [update]},
                    "page": {
                        "size": 10000,
                        "totalElements": 1,
                        "totalPages": 1,
                        "number": 0,
                    },
                }
            ),
            "https://data.brreg.no/enhetsregisteret/api/enheter/923609016": FakeResponse(
                payload=entity
            ),
        }
    )
    resource = NorwayBrregApiResource(session=session)
    log_calls: list[tuple[str, tuple[Any, ...]]] = []

    records = list(
        resource.iter_updated_entities(
            start="2026-06-28T00:00:00.000Z",
            end="2026-06-28T23:59:59.999Z",
            log=lambda message, *args: log_calls.append((message, args)),
            progress_every_rows=1,
        )
    )

    assert len(records) == 1
    assert len(log_calls) >= 5


def test_iter_updated_entities_splits_large_update_windows_instead_of_requesting_page_past_limit() -> (
    None
):
    base_url = "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter"
    start = "2026-06-11T00:00:00.000Z"
    midpoint = "2026-06-11T00:00:05.000Z"
    end = "2026-06-11T00:00:10.000Z"
    overflow_update = _update("999999999", 1, "2026-06-11T00:00:01.000Z")
    left_update = _update("111111111", 2, "2026-06-11T00:00:02.000Z")
    right_update = _update("222222222", 3, "2026-06-11T00:00:07.000Z")
    session = ParamAwareFakeHttpSession(
        {
            (base_url, start, end, 0): FakeResponse(
                payload=_updates_payload(
                    [overflow_update],
                    total_elements=10001,
                    total_pages=2,
                )
            ),
            (base_url, start, midpoint, 0): FakeResponse(
                payload=_updates_payload([left_update], total_elements=1, total_pages=1)
            ),
            (base_url, midpoint, end, 0): FakeResponse(
                payload=_updates_payload(
                    [right_update], total_elements=1, total_pages=1
                )
            ),
        }
    )
    resource = NorwayBrregApiResource(session=session)

    records = list(resource.iter_updated_entities(start=start, end=end))

    assert [record["org_number"] for record in records] == ["111111111", "222222222"]
    assert [call[1]["page"] for call in session.calls if call[1] is not None] == [
        0,
        0,
        0,
    ]
    assert [call[1]["dato"] for call in session.calls if call[1] is not None] == [
        start,
        start,
        midpoint,
    ]


def test_iter_updated_entities_returns_removed_tombstone_without_entity_fetch() -> None:
    update = {
        "oppdateringsid": 24720424,
        "dato": "2026-06-28T00:40:10.669Z",
        "organisasjonsnummer": "937995334",
        "endringstype": "Fjernet",
    }
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter": FakeResponse(
                payload={
                    "_embedded": {"oppdaterteEnheter": [update]},
                    "page": {
                        "size": 10000,
                        "totalElements": 1,
                        "totalPages": 1,
                        "number": 0,
                    },
                }
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)

    records = list(
        resource.iter_updated_entities(
            start="2026-06-28T00:00:00.000Z",
            end="2026-06-29T00:00:00.000Z",
        )
    )

    assert len(records) == 1
    assert set(records[0]) == EXPECTED_ENTITY_RECORD_KEYS
    assert records[0]["org_number"] == "937995334"
    assert records[0]["change_type"] == "removed"
    assert records[0]["entity"] is None
    assert records[0]["raw_update"] == update
    assert [call[0] for call in session.calls] == [
        "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter"
    ]


def test_iter_updated_entities_treats_410_hydration_as_removed_tombstone() -> None:
    update = {
        "oppdateringsid": 24720425,
        "dato": "2026-06-28T00:45:10.669Z",
        "organisasjonsnummer": "937798849",
        "endringstype": "Endring",
        "_links": {
            "enhet": {
                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/937798849"
            }
        },
    }
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter": FakeResponse(
                payload={
                    "_embedded": {"oppdaterteEnheter": [update]},
                    "page": {
                        "size": 10000,
                        "totalElements": 1,
                        "totalPages": 1,
                        "number": 0,
                    },
                }
            ),
            "https://data.brreg.no/enhetsregisteret/api/enheter/937798849": FakeResponse(
                status_code=410,
                payload={"message": "gone"},
            ),
        }
    )
    resource = NorwayBrregApiResource(session=session)

    records = list(
        resource.iter_updated_entities(
            start="2026-06-28T00:00:00.000Z",
            end="2026-06-29T00:00:00.000Z",
        )
    )

    assert len(records) == 1
    assert records[0]["org_number"] == "937798849"
    assert records[0]["change_type"] == "removed"
    assert records[0]["source_change_type"] == "Endring"
    assert records[0]["entity"] is None
    assert records[0]["raw_update"] == update
    assert records[0]["entity_url"] == (
        "https://data.brreg.no/enhetsregisteret/api/enheter/937798849"
    )
    assert [call[0] for call in session.calls] == [
        "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter",
        "https://data.brreg.no/enhetsregisteret/api/enheter/937798849",
    ]


def test_resource_is_company_api_only() -> None:
    assert not hasattr(NorwayBrregApiResource, "get_financial_accounts")


def _load_entity_fixture() -> dict[str, Any]:
    fixture_path = (
        Path(__file__).parents[4]
        / "companies/analysis/norway/data_model/sources/brregenhet/sample_record.json"
    )
    return json.loads(fixture_path.read_text())


def _update(org_number: str, update_id: int, updated_at: str) -> dict[str, Any]:
    return {
        "oppdateringsid": update_id,
        "dato": updated_at,
        "organisasjonsnummer": org_number,
        "endringstype": "Fjernet",
    }


def _updates_payload(
    updates: list[dict[str, Any]],
    *,
    total_elements: int,
    total_pages: int,
) -> dict[str, Any]:
    return {
        "_embedded": {"oppdaterteEnheter": updates},
        "page": {
            "size": 10000,
            "totalElements": total_elements,
            "totalPages": total_pages,
            "number": 0,
        },
    }
