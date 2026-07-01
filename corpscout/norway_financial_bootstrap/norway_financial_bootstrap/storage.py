import json
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from norway_financial_bootstrap.paths import (
    DEFAULT_BUCKET,
    done_marker_key,
    failed_marker_key,
    raw_report_key,
    report_year_from_report,
)
from norway_financial_bootstrap.types import CandidateMarkerStatus


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

    def client_object_exists(self, key: str) -> bool:
        try:
            self.client().head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def candidate_marker_status(self, org_number: str) -> CandidateMarkerStatus:
        if self.client_object_exists(done_marker_key(org_number)):
            return CandidateMarkerStatus.DONE
        if self.client_object_exists(failed_marker_key(org_number)):
            return CandidateMarkerStatus.FAILED
        return CandidateMarkerStatus.MISSING

    def write_raw_report(self, *, org_number: str, report: dict[str, Any]) -> str:
        report_id = _required_report_value(report, "id")
        report_type = _required_report_value(report, "regnskapstype")
        report_year = report_year_from_report(report)
        key = raw_report_key(org_number, report_year, report_type, report_id)
        self.client().put_object(Bucket=self.bucket, Key=key, Body=_json_bytes(report))
        return key

    def write_done_marker(
        self,
        *,
        org_number: str,
        fetch_status: str,
        report_count: int,
        raw_report_keys: list[str],
        completed_at: str,
    ) -> str:
        key = done_marker_key(org_number)
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_json_bytes(
                {
                    "org_number": org_number,
                    "fetch_status": fetch_status,
                    "report_count": report_count,
                    "raw_report_keys": raw_report_keys,
                    "completed_at": completed_at,
                }
            ),
        )
        return key

    def write_failed_marker(
        self,
        *,
        org_number: str,
        error_type: str,
        error_message: str,
        failed_at: str,
    ) -> str:
        key = failed_marker_key(org_number)
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_json_bytes(
                {
                    "org_number": org_number,
                    "error_type": error_type,
                    "error_message": error_message,
                    "failed_at": failed_at,
                }
            ),
        )
        return key


def _required_report_value(report: dict[str, Any], key: str) -> str:
    value = report.get(key)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"BRREG financial report is missing required field {key!r}")
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
