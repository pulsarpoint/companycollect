import json
from io import BytesIO

from botocore.exceptions import ClientError

from norway_financial_bootstrap.paths import (
    DEFAULT_BUCKET,
    RAW_REPORT_PREFIX,
    done_marker_key,
    failed_marker_key,
    raw_report_key,
    report_year_from_report,
)
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage
from norway_financial_bootstrap.types import CandidateMarkerStatus


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[tuple[str, str, bytes]] = []

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body
        self.puts.append((Bucket, Key, Body))

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, int]:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, BytesIO]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


def test_raw_report_key_matches_finance_storage_contract() -> None:
    assert DEFAULT_BUCKET == "source-norway-brreg"
    assert RAW_REPORT_PREFIX == "norway_brreg/finance/raw_reports/"
    assert raw_report_key("811685852", "2024", "SELSKAP", "6697842") == (
        "norway_brreg/finance/raw_reports/org=811685852/"
        "year=2024/type=SELSKAP/id=6697842.json"
    )


def test_status_marker_keys_are_colocated_under_org_raw_report_prefix() -> None:
    assert done_marker_key("811685852") == (
        "norway_brreg/finance/raw_reports/org=811685852/status/done.json"
    )
    assert failed_marker_key("811685852") == (
        "norway_brreg/finance/raw_reports/org=811685852/status/failed.json"
    )


def test_raw_report_key_encodes_unsafe_path_components() -> None:
    assert raw_report_key("923/609016", "2024", "SEL/SKAP", "id/1") == (
        "norway_brreg/finance/raw_reports/org=923%2F609016/"
        "year=2024/type=SEL%2FSKAP/id=id%2F1.json"
    )


def test_report_year_from_report_uses_regnskapsperiode_til_dato() -> None:
    assert (
        report_year_from_report(
            {"regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"}}
        )
        == "2024"
    )


def test_report_year_from_report_falls_back_to_fra_dato() -> None:
    assert (
        report_year_from_report({"regnskapsperiode": {"fraDato": "2023-01-01"}})
        == "2023"
    )


def test_bootstrap_storage_defaults_to_norway_brreg_bucket() -> None:
    assert NorwayFinancialBootstrapStorage().bucket == "source-norway-brreg"


def test_storage_checks_exact_object_existence_with_head_object() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    assert storage.client_object_exists("missing.json") is False
    client.put_object(Bucket=storage.bucket, Key="exists.json", Body=b"{}")
    assert storage.client_object_exists("exists.json") is True


def test_storage_writes_raw_report_to_response_year_key() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    key = storage.write_raw_report(
        org_number="923609016",
        report={
            "id": 5667197,
            "regnskapstype": "SELSKAP",
            "regnskapsperiode": {"tilDato": "2024-12-31"},
        },
    )

    assert key == (
        "norway_brreg/finance/raw_reports/org=923609016/"
        "year=2024/type=SELSKAP/id=5667197.json"
    )
    body = json.loads(client.objects[(storage.bucket, key)].decode("utf-8"))
    assert body["id"] == 5667197


def test_storage_reads_missing_done_failed_marker_status() -> None:
    storage = NorwayFinancialBootstrapStorage(s3_client=FakeS3Client())

    assert storage.candidate_marker_status("923609016") == CandidateMarkerStatus.MISSING


def test_storage_reads_done_marker_before_failed_marker() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)
    storage.write_done_marker(
        org_number="923609016",
        fetch_status="success",
        report_count=1,
        raw_report_keys=["report.json"],
        completed_at="2026-07-01T18:00:00Z",
    )
    storage.write_failed_marker(
        org_number="923609016",
        fetch_status="server_error",
        error_type="RuntimeError",
        error_message="failed",
        failed_at="2026-07-01T18:00:01Z",
    )

    assert storage.candidate_marker_status("923609016") == CandidateMarkerStatus.DONE


def test_storage_writes_failed_marker_with_error_details() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    key = storage.write_failed_marker(
        org_number="923609016",
        fetch_status="server_error",
        error_type="RuntimeError",
        error_message="failed after retries",
        failed_at="2026-07-01T18:00:00Z",
    )

    assert key == "norway_brreg/finance/raw_reports/org=923609016/status/failed.json"
    body = json.loads(client.objects[(storage.bucket, key)].decode("utf-8"))
    assert body["org_number"] == "923609016"
    assert body["fetch_status"] == "server_error"
    assert body["error_type"] == "RuntimeError"
