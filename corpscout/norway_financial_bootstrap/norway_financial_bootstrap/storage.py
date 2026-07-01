from __future__ import annotations

import json
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any

import boto3
import polars as pl

from norway_financial_bootstrap.candidates import FinancialCandidate

DEFAULT_BUCKET = "source-norway-brreg"
RAW_REPORT_PREFIX = "norway_brreg/finance/raw_reports/"
BOOTSTRAP_RUNS_PREFIX = "norway_brreg/finance/bootstrap_runs/"
CANDIDATE_BATCH_SCHEMA: dict[str, pl.DataType] = {
    "org_number": pl.String,
    "legal_name": pl.String,
    "website": pl.String,
    "last_submitted_accounts_year": pl.String,
}


def raw_report_key(
    org_number: str, accounts_year: str, report_type: str, report_id: str
) -> str:
    return (
        f"{RAW_REPORT_PREFIX}org={org_number}/"
        f"year={accounts_year}/type={report_type}/id={report_id}.json"
    )


def candidate_batch_key(source_run_id: str, attempt_id: str, batch_index: int) -> str:
    return (
        f"{BOOTSTRAP_RUNS_PREFIX}run={source_run_id}/"
        f"attempt={attempt_id}/candidates/batch={batch_index:06d}.parquet"
    )


def completed_key_from_raw_report_key(
    key: str,
) -> tuple[str, str, str, str] | None:
    if not key.startswith(RAW_REPORT_PREFIX):
        return None

    suffix = key.removeprefix(RAW_REPORT_PREFIX)
    parts = suffix.split("/")
    if len(parts) != 4:
        return None

    org_part, year_part, type_part, filename = parts
    if not filename.endswith(".json"):
        return None
    if (
        not org_part.startswith("org=")
        or not year_part.startswith("year=")
        or not type_part.startswith("type=")
        or not filename.startswith("id=")
    ):
        return None

    org_number = org_part.removeprefix("org=")
    accounts_year = year_part.removeprefix("year=")
    report_type = type_part.removeprefix("type=")
    report_id = filename.removeprefix("id=").removesuffix(".json")
    if not org_number or not accounts_year or not report_type or not report_id:
        return None

    return org_number, accounts_year, report_type, report_id


@dataclass
class NorwayFinancialBootstrapStorage:
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    bucket: str = field(init=False, default=DEFAULT_BUCKET)
    region_name: str = "us-east-1"
    s3_client: Any | None = None

    def client(self) -> Any:
        if self.s3_client is None:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name,
            )
        return self.s3_client

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def existing_raw_report_ids(self) -> set[tuple[str, str, str, str]]:
        completed: set[tuple[str, str, str, str]] = set()
        for key in self.list_keys(RAW_REPORT_PREFIX):
            report_id = completed_key_from_raw_report_key(key)
            if report_id is not None:
                completed.add(report_id)
        return completed

    def read_parquet(self, key: str) -> pl.DataFrame:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return pl.read_parquet(BytesIO(response["Body"].read()))

    def read_candidate_batch(self, key: str) -> list[FinancialCandidate]:
        rows = (
            self.read_parquet(key)
            .select(
                pl.col("org_number").cast(pl.String),
                pl.col("legal_name").cast(pl.String),
                pl.col("website").fill_null("").cast(pl.String),
                pl.col("last_submitted_accounts_year").cast(pl.String),
            )
            .to_dicts()
        )
        return [
            FinancialCandidate(
                row["org_number"],
                row["legal_name"],
                row["website"],
                row["last_submitted_accounts_year"],
            )
            for row in rows
        ]

    def write_candidate_batch(
        self,
        source_run_id: str,
        attempt_id: str,
        batch_index: int,
        candidates: list[FinancialCandidate],
    ) -> str:
        key = candidate_batch_key(source_run_id, attempt_id, batch_index)
        frame = pl.DataFrame(
            {
                "org_number": [candidate.org_number for candidate in candidates],
                "legal_name": [candidate.legal_name for candidate in candidates],
                "website": [candidate.website for candidate in candidates],
                "last_submitted_accounts_year": [
                    candidate.last_submitted_accounts_year for candidate in candidates
                ],
            },
            schema=CANDIDATE_BATCH_SCHEMA,
        )
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_parquet_bytes(frame),
        )
        return key

    def raw_report_exists(
        self, org_number: str, accounts_year: str, report_type: str, report_id: str
    ) -> bool:
        return self.client_object_exists(
            raw_report_key(org_number, accounts_year, report_type, report_id)
        )

    def client_object_exists(self, key: str) -> bool:
        return any(item_key == key for item_key in self.list_keys(key))

    def write_raw_report(
        self,
        *,
        org_number: str,
        accounts_year: str,
        report_type: str,
        report_id: str,
        report: dict[str, Any],
    ) -> str:
        key = raw_report_key(org_number, accounts_year, report_type, report_id)
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_json_bytes(report),
        )
        return key


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
