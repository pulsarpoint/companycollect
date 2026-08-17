import json
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_bolagsverket_vdm.resources import (
    BolagsverketVdmResource,
)

RAW_BUCKET = "source-sweden-bolagsverket-vdm"
RAW_PREFIX = "sweden_bolagsverket_vdm/raw"
MAX_COMPANIES_PER_RUN = 100
_COMPANY_ID_PATTERN = re.compile(r"^\d{10,12}$")


@dataclass(frozen=True)
class StoredResponse:
    company_id: str
    endpoint: str
    object_key: str
    sha256: str
    size_bytes: int
    request_id: str
    status_code: int


@dataclass(frozen=True)
class TargetedRefreshResult:
    requested_company_count: int
    raw_response_count: int
    manifest_key: str

    def metadata(self) -> dict[str, object]:
        return {
            "requested_company_count": self.requested_company_count,
            "raw_response_count": self.raw_response_count,
            "manifest_key": self.manifest_key,
            "s3_bucket": RAW_BUCKET,
        }


def normalize_selected_company_ids(company_ids: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(str(company_id).strip() for company_id in company_ids)
    )
    if not normalized or normalized == ("",):
        raise ValueError("company_ids must contain at least one selected company")
    if any(not _COMPANY_ID_PATTERN.fullmatch(company_id) for company_id in normalized):
        raise ValueError("each company_id must contain 10, 11, or 12 digits")
    if len(normalized) > MAX_COMPANIES_PER_RUN:
        raise ValueError(
            f"company_ids must contain at most {MAX_COMPANIES_PER_RUN} selected companies"
        )
    return normalized


def manifest_object_key(run_id: str) -> str:
    return f"{RAW_PREFIX}/run_id={_safe_path_component(run_id)}/manifest.json"


def sync_selected_companies(
    *,
    object_store: ObjectStoreResource,
    api: BolagsverketVdmResource,
    company_ids: Iterable[str],
    run_id: str,
    request_delay_seconds: float,
    observed_at: datetime | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> TargetedRefreshResult:
    """Persist both source responses for a bounded, explicitly selected batch."""
    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds must not be negative")
    selected_ids = normalize_selected_company_ids(company_ids)
    observed_at = observed_at or datetime.now(UTC)
    object_store.ensure_bucket(RAW_BUCKET)
    stored_responses: list[StoredResponse] = []

    request_count = 0
    for company_id in selected_ids:
        for endpoint, fetch in (
            ("organisationer", api.fetch_organisationer),
            ("dokumentlista", api.fetch_dokumentlista),
        ):
            if request_count and request_delay_seconds:
                sleeper(request_delay_seconds)
            response = fetch(company_id)
            request_count += 1
            object_key = (
                f"{RAW_PREFIX}/run_id={_safe_path_component(run_id)}/"
                f"identity_sha256={sha256(company_id.encode()).hexdigest()}/"
                f"{endpoint}.json"
            )
            object_store.write_bytes(object_key, response.content, bucket=RAW_BUCKET)
            stored_responses.append(
                StoredResponse(
                    company_id=company_id,
                    endpoint=endpoint,
                    object_key=object_key,
                    sha256=sha256(response.content).hexdigest(),
                    size_bytes=len(response.content),
                    request_id=response.request_id,
                    status_code=response.status_code,
                )
            )

    manifest_key = manifest_object_key(run_id)
    manifest = {
        "source": "bolagsverket_vardefulla_datamangder_v1",
        "run_id": run_id,
        "observed_at": observed_at.isoformat(),
        "company_ids": list(selected_ids),
        "responses": [asdict(response) for response in stored_responses],
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=RAW_BUCKET,
    )
    return TargetedRefreshResult(
        requested_company_count=len(selected_ids),
        raw_response_count=len(stored_responses),
        manifest_key=manifest_key,
    )


def _safe_path_component(value: str) -> str:
    if not value or not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError("run_id contains characters that are unsafe in an object key")
    return value
