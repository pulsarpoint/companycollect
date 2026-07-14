"""On-demand 'latest accounts per company' via the Companies House API.

Unlike the bulk Accounts Data Product (one day's filings per archive), this fetches
the *latest* accounts iXBRL for a specific company on demand:
  Filing History API  ->  newest accounts filing's document_metadata
  Document API        ->  the iXBRL content
  xbrl_common.parser  ->  metrics
Rate-limited (600 req / 5 min ~= 2 calls/company), so it targets a provided list of
company numbers — not all 5.7M (that's the archive-accumulation path).
"""

import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Any

import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.uk_companies_house import financials, raw_archives, resources

LOGGER = logging.getLogger(__name__)

API_ACCOUNTS_PREFIX = "raw/api_accounts"
API_ACCOUNTS_SOURCE_SLUG = "uk_companies_house_accounts_api"


@dataclass(frozen=True)
class StoredApiAccountsDocument:
    company_number: str
    filing_date: str
    metadata_url: str
    document_url: str
    content_type: str
    object_key: str
    metadata_key: str
    size_bytes: int
    sha256: str
    retrieved_at: str

    @classmethod
    def from_content(
        cls,
        *,
        company_number: str,
        filing_date: str,
        metadata_url: str,
        document_url: str,
        content_type: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> StoredApiAccountsDocument:
        normalized_company_number = _normalized_company_number(company_number)
        validated_filing_date = _validated_filing_date(filing_date)
        digest = sha256(content).hexdigest()
        filename = (
            "accounts.xhtml"
            if content_type == "application/xhtml+xml"
            else "accounts.xml"
        )
        object_key = (
            f"{API_ACCOUNTS_PREFIX}/company_number={normalized_company_number}/"
            f"filing_date={validated_filing_date}/sha256={digest}/{filename}"
        )
        return cls(
            company_number=normalized_company_number,
            filing_date=validated_filing_date,
            metadata_url=metadata_url,
            document_url=document_url,
            content_type=content_type,
            object_key=object_key,
            metadata_key=str(PurePosixPath(object_key).parent / "metadata.json"),
            size_bytes=len(content),
            sha256=digest,
            retrieved_at=retrieved_at.isoformat(),
        )


@dataclass(frozen=True)
class ApiAccountsBatchCatalog:
    run_id: str
    requested_company_numbers: tuple[str, ...]
    documents: tuple[StoredApiAccountsDocument, ...]
    missing_company_numbers: tuple[str, ...]
    created_at: str


@dataclass(frozen=True)
class ApiAccountsSyncResult:
    requested: int
    stored: int
    reused: int
    missing: int
    catalog_key: str

    def metadata(self) -> dict[str, object]:
        return {
            "requested": self.requested,
            "stored": self.stored,
            "reused": self.reused,
            "missing": self.missing,
            "catalog_key": self.catalog_key,
            "s3_bucket": raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        }


def sync_api_accounts_documents(
    *,
    object_store: ObjectStoreResource,
    company_numbers: Iterable[str],
    run_id: str,
    client: resources.CompaniesHouseResource,
    request_delay_seconds: float,
    retrieved_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> ApiAccountsSyncResult:
    """Persist configured companies' latest iXBRL documents and a run catalog."""
    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds must not be negative")
    retrieved_at = retrieved_at or datetime.now(UTC)
    numbers = tuple(
        dict.fromkeys(
            _normalized_company_number(str(number))
            for number in company_numbers
            if str(number).strip()
        )
    )
    if not numbers:
        raise ValueError("company_numbers must contain at least one company number")

    object_store.ensure_bucket(raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET)
    stored_documents: list[StoredApiAccountsDocument] = []
    missing_company_numbers: list[str] = []
    reused = 0
    for index, company_number in enumerate(numbers):
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)
        try:
            document = client.latest_accounts_ixbrl_document(company_number)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CH filing-history failed for %s: %s", company_number, exc)
            missing_company_numbers.append(company_number)
            continue
        if document is None:
            missing_company_numbers.append(company_number)
            continue
        stored_document = StoredApiAccountsDocument.from_content(
            company_number=document.company_number,
            filing_date=document.filing_date,
            metadata_url=document.metadata_url,
            document_url=document.document_url,
            content_type=document.content_type,
            content=document.content,
            retrieved_at=retrieved_at,
        )
        object_exists = object_store.exists(
            stored_document.object_key,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
        if object_exists:
            reused += 1
        else:
            object_store.write_bytes(
                stored_document.object_key,
                document.content,
                bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
            )
        if not object_store.exists(
            stored_document.metadata_key,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        ):
            object_store.write_json(
                stored_document.metadata_key,
                json.dumps(asdict(stored_document), sort_keys=True),
                bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
            )
        stored_documents.append(stored_document)

    catalog_key = write_api_accounts_batch_catalog(
        object_store=object_store,
        run_id=run_id,
        requested_company_numbers=numbers,
        documents=tuple(stored_documents),
        missing_company_numbers=tuple(missing_company_numbers),
        created_at=retrieved_at,
    )
    if log is not None:
        log(
            "Stored UK API accounts documents: requested=%s stored=%s missing=%s reused=%s",
            len(numbers),
            len(stored_documents),
            len(missing_company_numbers),
            reused,
        )
    return ApiAccountsSyncResult(
        requested=len(numbers),
        stored=len(stored_documents),
        reused=reused,
        missing=len(missing_company_numbers),
        catalog_key=catalog_key,
    )


def write_api_accounts_batch_catalog(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    requested_company_numbers: tuple[str, ...],
    documents: tuple[StoredApiAccountsDocument, ...],
    missing_company_numbers: tuple[str, ...],
    created_at: datetime,
) -> str:
    object_store.ensure_bucket(raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET)
    catalog_key = api_accounts_batch_catalog_key(run_id)
    catalog = {
        "run_id": run_id,
        "requested_company_numbers": list(requested_company_numbers),
        "documents": [asdict(document) for document in documents],
        "missing_company_numbers": list(missing_company_numbers),
        "created_at": created_at.isoformat(),
    }
    object_store.write_json(
        catalog_key,
        json.dumps(catalog, sort_keys=True),
        bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
    )
    return catalog_key


def read_api_accounts_batch_catalog(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> ApiAccountsBatchCatalog:
    catalog_key = api_accounts_batch_catalog_key(run_id)
    if not object_store.exists(
        catalog_key,
        bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
    ):
        raise ValueError(
            f"No UK API accounts catalog for run {run_id}; materialize "
            "uk_companies_house_api_accounts_documents_s3 in the same run"
        )
    payload = json.loads(
        object_store.read_bytes(
            catalog_key,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
    )
    return ApiAccountsBatchCatalog(
        run_id=str(payload["run_id"]),
        requested_company_numbers=tuple(payload["requested_company_numbers"]),
        documents=tuple(
            StoredApiAccountsDocument(**document)
            for document in payload["documents"]
        ),
        missing_company_numbers=tuple(payload["missing_company_numbers"]),
        created_at=str(payload["created_at"]),
    )


def load_api_financial_metrics_from_object_store(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: ObjectStoreResource,
    run_id: str,
    source_run_id: str,
) -> dict[str, int]:
    catalog = read_api_accounts_batch_catalog(
        object_store=object_store,
        run_id=run_id,
    )
    rows: list[tuple[Any, ...]] = []
    parse_failed = 0
    for document in catalog.documents:
        content = object_store.read_bytes(
            document.object_key,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
        extracted = financials._extract_metrics(content)
        if extracted is None:
            parse_failed += 1
            continue
        company_number, period_end, metrics = extracted
        rows.append(financials.metrics_row(company_number, period_end, metrics))

    counts = financials.write_metrics_table(
        connection=connection,
        rows=rows,
        source_run_id=source_run_id,
        source_slug=API_ACCOUNTS_SOURCE_SLUG,
        allow_empty=True,
    )
    counts.update(
        {
            "requested": len(catalog.requested_company_numbers),
            "stored_documents": len(catalog.documents),
            "parsed_documents": len(rows),
            "missing": len(catalog.missing_company_numbers),
            "parse_failed": parse_failed,
        }
    )
    return counts


def api_accounts_batch_catalog_key(run_id: str) -> str:
    if not run_id or "/" in run_id:
        raise ValueError(f"invalid run_id for object storage key: {run_id!r}")
    return f"{API_ACCOUNTS_PREFIX}/batches/run_id={run_id}/catalog.json"


def _normalized_company_number(company_number: str) -> str:
    normalized = company_number.strip().upper()
    if not normalized or not normalized.isalnum():
        raise ValueError(f"invalid Companies House company number: {company_number!r}")
    return normalized


def _validated_filing_date(filing_date: str) -> str:
    try:
        return date.fromisoformat(filing_date).isoformat()
    except ValueError as exc:
        raise ValueError(
            f"invalid Companies House filing date: {filing_date!r}"
        ) from exc
