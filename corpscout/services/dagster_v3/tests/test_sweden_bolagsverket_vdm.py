import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
import requests

from dagster_v3.defs.sweden_bolagsverket_vdm import normalize, source, tables
from dagster_v3.defs.sweden_bolagsverket_vdm.resources import (
    ApiResponse,
    BolagsverketVdmApiError,
    BolagsverketVdmResource,
)


ORGANISATION_RESPONSE = {
    "organisationer": [
        {
            "namnskyddslopnummer": None,
            "organisationsidentitet": {
                "identitetsbeteckning": "5562434182",
                "typ": {"kod": "ORGNR", "klartext": "Organisationsnummer"},
            },
            "organisationsdatum": {
                "registreringsdatum": "1984-05-03",
                "infortHosScb": "1984-08-19",
                "dataproducent": "Bolagsverket",
                "fel": None,
            },
            "verksamOrganisation": {
                "kod": "JA",
                "dataproducent": "SCB",
                "fel": None,
            },
        }
    ]
}
DOCUMENT_RESPONSE = {
    "dokument": [
        {
            "dokumentId": "document-1_paket",
            "filformat": "application/zip",
            "rapporteringsperiodTom": "2025-06-30",
            "registreringstidpunkt": "2025-12-29",
        }
    ]
}


class FakeResponse:
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        status_code: int = 200,
        body: bytes | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.content = body if body is not None else json.dumps(payload).encode()

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.HTTPError(f"unsafe-response-body-{self.status_code}")
            error.response = self  # type: ignore[assignment]
            raise error


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)

    def close(self) -> None:
        pass


class FakeObjectStore:
    def __init__(self) -> None:
        self.bucket: str | None = None
        self.objects: dict[str, bytes] = {}
        self.write_order: list[str] = []

    def ensure_bucket(self, bucket: str) -> None:
        self.bucket = bucket

    def write_bytes(self, key: str, body: bytes, bucket: str) -> None:
        assert bucket == self.bucket
        self.objects[key] = body
        self.write_order.append(key)

    def write_json(self, key: str, body: str, bucket: str) -> None:
        self.write_bytes(key, body.encode(), bucket)

    def read_bytes(self, key: str, bucket: str) -> bytes:
        assert bucket == self.bucket
        return self.objects[key]


class FakeApi:
    def fetch_organisationer(self, company_id: str) -> Any:
        return ApiResponse(
            content=json.dumps(ORGANISATION_RESPONSE).encode(),
            request_id="organisation-request",
            status_code=200,
        )

    def fetch_dokumentlista(self, company_id: str) -> Any:
        return ApiResponse(
            content=json.dumps(DOCUMENT_RESPONSE).encode(),
            request_id="documents-request",
            status_code=200,
        )


def test_selected_company_ids_are_required_valid_deduplicated_and_bounded() -> None:
    assert source.normalize_selected_company_ids(
        [" 5562434182 ", "5562434182", "198001011234"]
    ) == ("5562434182", "198001011234")

    with pytest.raises(ValueError, match="at least one"):
        source.normalize_selected_company_ids([])
    with pytest.raises(ValueError, match="10, 11, or 12 digits"):
        source.normalize_selected_company_ids(["556-243-4182"])
    with pytest.raises(ValueError, match="at most 100"):
        source.normalize_selected_company_ids(
            [f"{number:010d}" for number in range(101)]
        )


def test_resource_caches_oauth_token_and_sets_unique_request_ids() -> None:
    session = FakeSession(
        [
            FakeResponse({"access_token": "secret-token", "expires_in": 3600}),
            FakeResponse(ORGANISATION_RESPONSE),
            FakeResponse(DOCUMENT_RESPONSE),
        ]
    )
    resource = BolagsverketVdmResource(
        client_id="client-id",
        client_secret="client-secret",
        session=session,
    )

    resource.fetch_organisationer("5562434182")
    resource.fetch_dokumentlista("5562434182")

    assert (
        len([call for call in session.calls if call["url"].endswith("/oauth2/token")])
        == 1
    )
    api_calls = [
        call for call in session.calls if not call["url"].endswith("/oauth2/token")
    ]
    request_ids = [call["headers"]["X-Request-ID"] for call in api_calls]
    assert len(request_ids) == len(set(request_ids)) == 2
    assert all(
        call["headers"]["Authorization"] == "Bearer secret-token" for call in api_calls
    )


def test_resource_errors_do_not_expose_credentials_tokens_or_response_bodies() -> None:
    session = FakeSession(
        [
            FakeResponse(
                {"error": "unsafe-oauth-body"},
                status_code=401,
                body=b"unsafe-oauth-body",
            )
        ]
    )
    resource = BolagsverketVdmResource(
        client_id="unsafe-client-id",
        client_secret="unsafe-client-secret",
        session=session,
    )

    with pytest.raises(BolagsverketVdmApiError) as raised:
        resource.fetch_organisationer("5562434182")

    message = str(raised.value)
    assert "OAuth token request failed" in message
    for unsafe_value in (
        "unsafe-client-id",
        "unsafe-client-secret",
        "unsafe-oauth-body",
    ):
        assert unsafe_value not in message


def test_raw_sync_writes_two_exact_responses_then_manifest() -> None:
    object_store = FakeObjectStore()
    observed_at = datetime(2026, 8, 17, 10, 30, tzinfo=UTC)

    result = source.sync_selected_companies(
        object_store=object_store,
        api=FakeApi(),
        company_ids=["5562434182"],
        run_id="run-1",
        observed_at=observed_at,
        request_delay_seconds=0,
    )

    assert result.requested_company_count == 1
    assert result.raw_response_count == 2
    assert object_store.write_order[-1] == result.manifest_key
    assert all("5562434182" not in key for key in object_store.write_order[:-1])
    manifest = json.loads(object_store.objects[result.manifest_key])
    assert manifest["company_ids"] == ["5562434182"]
    assert manifest["responses"][0]["sha256"]
    assert manifest["responses"][1]["sha256"]


def test_normalization_preserves_requested_api_semantics(tmp_path: Path) -> None:
    object_store = FakeObjectStore()
    observed_at = datetime(2026, 8, 17, 10, 30, tzinfo=UTC)
    sync = source.sync_selected_companies(
        object_store=object_store,
        api=FakeApi(),
        company_ids=["5562434182"],
        run_id="run-1",
        observed_at=observed_at,
        request_delay_seconds=0,
    )
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    counts = normalize.load_observations_from_object_store(
        connection=connection,
        object_store=object_store,
        run_id="run-1",
        manifest_key=sync.manifest_key,
    )

    assert counts == {"company_observations": 1, "document_observations": 1}
    company = connection.execute(
        f"SELECT identity_type_code, active_status_code, is_active, "
        f"introduced_at_scb, digital_report_document_count "
        f"FROM {tables.DUCKDB_SCHEMA}.{tables.COMPANY_OBSERVATIONS_TABLE}"
    ).fetchone()
    assert company == ("ORGNR", "JA", True, datetime(1984, 8, 19).date(), 1)
    document = connection.execute(
        f"SELECT bolagsverket_document_id, filing_registered_on, source_file_format "
        f"FROM {tables.DUCKDB_SCHEMA}.{tables.DOCUMENT_OBSERVATIONS_TABLE}"
    ).fetchone()
    assert document == (
        "document-1_paket",
        datetime(2025, 12, 29).date(),
        "application/zip",
    )


def test_clickhouse_migration_contains_new_source_specific_fields() -> None:
    migration_dir = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    sql = (migration_dir / "000279_corpscout_se_bolagsverket_vdm.up.sql").read_text()

    assert "se_bolagsverket_vdm_company_observations" in sql
    assert "se_bolagsverket_vdm_financial_report_documents" in sql
    for field in (
        "bolagsverket_document_id",
        "filing_registered_on",
        "source_file_format",
        "introduced_at_scb",
        "identity_type_code",
        "active_status_code",
    ):
        assert field in sql
