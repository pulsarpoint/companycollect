import gzip
import json
from pathlib import Path
from typing import Any

import requests

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
    def __init__(self, *, status_code: int = 200, payload: Any = None, content: bytes = b"") -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self.text = json.dumps(payload) if payload is not None else content.decode("utf-8", "ignore")

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


def test_iter_all_entities_returns_snapshot_records_with_real_brreg_shape() -> None:
    entity = _load_entity_fixture()
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned": FakeResponse(
                content=gzip.compress(json.dumps([entity]).encode("utf-8"))
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)

    records = list(resource.iter_all_entities())

    assert len(records) == 1
    assert set(records[0]) == EXPECTED_ENTITY_RECORD_KEYS
    assert records[0]["org_number"] == "923609016"
    assert records[0]["change_type"] == "snapshot"
    assert records[0]["source_change_type"] == "snapshot"
    assert records[0]["updated_at"] is None
    assert records[0]["update_id"] is None
    assert records[0]["entity_url"] == "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
    assert records[0]["entity"] == entity
    assert records[0]["raw_update"] is None


def test_iter_all_entities_emits_download_and_parse_progress_logs() -> None:
    entity = _load_entity_fixture()
    archive = gzip.compress(json.dumps([entity, entity]).encode("utf-8"))
    session = FakeHttpSession(
        {
            "https://data.brreg.no/enhetsregisteret/api/enheter/lastned": FakeResponse(
                content=archive
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)
    log_calls: list[tuple[str, tuple[Any, ...]]] = []

    records = list(
        resource.iter_all_entities(
            log=lambda message, *args: log_calls.append((message, args)),
            progress_every_rows=1,
            download_progress_every_bytes=len(archive),
        )
    )

    assert len(records) == 2
    assert len(log_calls) >= 6


def test_iter_updated_entities_returns_same_shape_and_hydrates_changed_entities() -> None:
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
                    "page": {"size": 10000, "totalElements": 1, "totalPages": 1, "number": 0},
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
    assert records[0]["entity_url"] == "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
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
                    "page": {"size": 10000, "totalElements": 1, "totalPages": 1, "number": 0},
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
                    "page": {"size": 10000, "totalElements": 1, "totalPages": 1, "number": 0},
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
                    "page": {"size": 10000, "totalElements": 1, "totalPages": 1, "number": 0},
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


def test_get_financial_accounts_uses_org_number_and_year_filter() -> None:
    financial_payload = [{"id": 5027443, "regnskapstype": "SELSKAP"}]
    session = FakeHttpSession(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": FakeResponse(
                payload=financial_payload
            )
        }
    )
    resource = NorwayBrregApiResource(session=session)

    payload = resource.get_financial_accounts("923609016", year="2024")

    assert payload == financial_payload
    assert session.calls == [
        (
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
            {"år": "2024"},
            120,
            False,
        )
    ]


def _load_entity_fixture() -> dict[str, Any]:
    fixture_path = (
        Path(__file__).parents[3]
        / "companies/analysis/norway/data_model/sources/brregenhet/sample_record.json"
    )
    return json.loads(fixture_path.read_text())
