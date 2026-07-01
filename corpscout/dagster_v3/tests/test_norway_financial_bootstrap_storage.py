from io import BytesIO

from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.storage import (
    NorwayFinancialBootstrapStorage,
    candidate_batch_key,
    completed_key_from_raw_fetch_key,
    raw_fetch_key,
)
from dagster_v3.defs.norway_brreg.financial_storage import (
    financial_raw_fetch_object_key,
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


def test_raw_fetch_key_matches_existing_norway_financial_storage_contract() -> None:
    expected = (
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    )
    assert financial_raw_fetch_object_key("811685852", "2024") == expected
    assert raw_fetch_key("811685852", "2024") == financial_raw_fetch_object_key(
        "811685852", "2024"
    )


def test_completed_key_from_raw_fetch_key_parses_existing_storage_path() -> None:
    assert completed_key_from_raw_fetch_key(
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    ) == ("811685852", "2024")


def test_candidate_batch_key_uses_run_scoped_parquet_path() -> None:
    assert candidate_batch_key("run-1", 2) == (
        "norway_brreg/financial/bootstrap_runs/"
        "run=run-1/candidates/batch=000002.parquet"
    )


def test_storage_writes_and_reads_candidate_batch_parquet() -> None:
    storage = NorwayFinancialBootstrapStorage(
        bucket="test-bucket",
        s3_client=FakeS3Client(),
    )
    candidates = [
        FinancialCandidate("100", "A AS", "", "2024"),
        FinancialCandidate("200", "B AS", "https://b.example", "2023"),
    ]

    key = storage.write_candidate_batch("run-1", 0, candidates)

    assert key == (
        "norway_brreg/financial/bootstrap_runs/"
        "run=run-1/candidates/batch=000000.parquet"
    )
    assert storage.read_candidate_batch(key) == candidates
