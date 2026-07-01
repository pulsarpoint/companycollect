from io import BytesIO

from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.storage import (
    COMPANY_SNAPSHOT_NO_COMPANIES_KEY,
    DEFAULT_BUCKET,
    NorwayFinancialBootstrapStorage,
    RAW_REPORT_PREFIX,
    candidate_batch_key,
    completed_key_from_raw_report_key,
    raw_report_key,
)


class FakeS3Paginator:
    def __init__(self, client: "FakeS3Client") -> None:
        self.client = client

    def paginate(self, *, Bucket: str, Prefix: str):
        keys = [
            {"Key": key}
            for bucket, key in sorted(self.client.objects)
            if bucket == Bucket and key.startswith(Prefix)
        ]
        yield {"Contents": keys}


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, BytesIO]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def get_paginator(self, operation_name: str) -> FakeS3Paginator:
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self)


def test_raw_report_key_matches_finance_split_storage_contract() -> None:
    assert RAW_REPORT_PREFIX == "norway_brreg/finance/raw_reports/"
    assert raw_report_key("811685852", "2024", "SELSKAP", "6697842") == (
        "norway_brreg/finance/raw_reports/org=811685852/"
        "year=2024/type=SELSKAP/id=6697842.json"
    )


def test_company_snapshot_no_companies_key_is_fixed() -> None:
    assert (
        COMPANY_SNAPSHOT_NO_COMPANIES_KEY
        == "norway_brreg/company/normalized/snapshot/no_companies.parquet"
    )


def test_bootstrap_storage_defaults_to_norway_brreg_bucket() -> None:
    assert DEFAULT_BUCKET == "source-norway-brreg"
    assert NorwayFinancialBootstrapStorage().bucket == "source-norway-brreg"


def test_completed_key_from_raw_report_key_parses_existing_storage_path() -> None:
    assert completed_key_from_raw_report_key(
        "norway_brreg/finance/raw_reports/org=811685852/"
        "year=2024/type=SELSKAP/id=6697842.json"
    ) == ("811685852", "2024", "SELSKAP", "6697842")


def test_candidate_batch_key_uses_run_scoped_parquet_path() -> None:
    assert candidate_batch_key("run-1", "attempt-a", 2) == (
        "norway_brreg/finance/bootstrap_runs/"
        "run=run-1/attempt=attempt-a/candidates/batch=000002.parquet"
    )


def test_storage_writes_and_reads_candidate_batch_parquet() -> None:
    storage = NorwayFinancialBootstrapStorage(
        s3_client=FakeS3Client(),
    )
    candidates = [
        FinancialCandidate("100", "A AS", "", "2024"),
        FinancialCandidate("200", "B AS", "https://b.example", "2023"),
    ]

    key = storage.write_candidate_batch("run-1", "attempt-a", 0, candidates)

    assert key == (
        "norway_brreg/finance/bootstrap_runs/"
        "run=run-1/attempt=attempt-a/candidates/batch=000000.parquet"
    )
    assert storage.read_candidate_batch(key) == candidates


def test_storage_writes_and_reads_empty_candidate_batch_parquet() -> None:
    storage = NorwayFinancialBootstrapStorage(
        s3_client=FakeS3Client(),
    )

    key = storage.write_candidate_batch("run-1", "attempt-a", 1, [])

    assert key == (
        "norway_brreg/finance/bootstrap_runs/"
        "run=run-1/attempt=attempt-a/candidates/batch=000001.parquet"
    )
    assert storage.read_candidate_batch(key) == []


def test_candidate_batch_keys_differ_across_attempt_ids() -> None:
    assert candidate_batch_key("run-1", "attempt-a", 0) != candidate_batch_key(
        "run-1", "attempt-b", 0
    )


def test_storage_lists_existing_raw_report_ids_from_fixed_bucket() -> None:
    client = FakeS3Client()
    client.objects[
        (
            DEFAULT_BUCKET,
            "norway_brreg/finance/raw_reports/org=811685852/"
            "year=2024/type=SELSKAP/id=6697842.json",
        )
    ] = b"{}"
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    assert storage.existing_raw_report_ids() == {
        ("811685852", "2024", "SELSKAP", "6697842")
    }


def test_storage_writes_raw_report_json_to_fixed_key_and_bucket() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    key = storage.write_raw_report(
        org_number="811685852",
        accounts_year="2024",
        report_type="SELSKAP",
        report_id="6697842",
        report={"id": 6697842, "regnskapstype": "SELSKAP"},
    )

    assert key == (
        "norway_brreg/finance/raw_reports/org=811685852/"
        "year=2024/type=SELSKAP/id=6697842.json"
    )
    assert client.objects[(DEFAULT_BUCKET, key)] == (
        b'{"id":6697842,"regnskapstype":"SELSKAP"}'
    )
