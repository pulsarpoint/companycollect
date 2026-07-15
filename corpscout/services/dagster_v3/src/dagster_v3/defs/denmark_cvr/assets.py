import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.models import (
    CompanySearchResult,
    PersonSearchResult,
    ProductionUnitSearchResult,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DenmarkCvrSearchResource,
    DenmarkCvrValidationError,
)

DENMARK_CVR_BUCKET = "source-denmark-cvr"
DENMARK_CVR_SEARCH_TERMS = tuple("0123456789abcdefghijklmnopqrstuvwxyzæøå")
DENMARK_CVR_SEARCH_PARTITIONS = dg.StaticPartitionsDefinition(
    list(DENMARK_CVR_SEARCH_TERMS)
)


@dataclass(frozen=True)
class DenmarkCvrPartitionSummary:
    manifest_key: str
    object_prefix: str
    search_term: str
    advertised_entity_count: int
    downloaded_entity_count: int
    downloaded_file_count: int
    stored_file_count: int
    company_count: int
    person_count: int
    production_unit_count: int
    downloaded_size_bytes: int
    manifest_size_bytes: int
    stored_size_bytes: int


def page_object_key(search_term: str, run_id: str, page_index: int) -> str:
    _validate_object_scope(search_term, run_id)
    if page_index < 0:
        raise ValueError("DataCVR page index must not be negative")
    return (
        f"denmark_cvr/search/search_term={search_term}/run_id={run_id}/"
        f"page={page_index:06d}.json"
    )


def invalid_page_object_key(
    search_term: str,
    run_id: str,
    page_index: int,
) -> str:
    return (
        page_object_key(search_term, run_id, page_index).removesuffix(".json")
        + ".invalid.json"
    )


def manifest_object_key(search_term: str, run_id: str) -> str:
    _validate_object_scope(search_term, run_id)
    return f"denmark_cvr/search/search_term={search_term}/run_id={run_id}/manifest.json"


def write_denmark_cvr_search_partition(
    *,
    object_store: ObjectStoreResource,
    search: DenmarkCvrSearchResource,
    search_term: str,
    run_id: str,
    retrieved_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrPartitionSummary:
    _validate_object_scope(search_term, run_id)
    if retrieved_at.utcoffset() is None:
        raise ValueError("DataCVR retrieval timestamp must include a timezone")
    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    prefix = manifest_object_key(search_term, run_id).removesuffix("manifest.json")
    if log_info is not None:
        log_info(
            "Starting DataCVR download: search_term=%s bucket=%s prefix=%s",
            search_term,
            DENMARK_CVR_BUCKET,
            prefix,
        )

    page_keys: list[str] = []
    advertised_total = 0
    company_count = 0
    person_count = 0
    production_unit_count = 0
    total_size_bytes = 0
    try:
        for search_page in search.iter_search_pages(search_term):
            key = page_object_key(search_term, run_id, search_page.page_index)
            body = search_page.raw_body.encode("utf-8")
            object_store.write_bytes(key, body, bucket=DENMARK_CVR_BUCKET)
            page_keys.append(key)
            total_size_bytes += len(body)
            if len(page_keys) == 1:
                advertised_total = search_page.response.total

            company_count += sum(
                isinstance(unit, CompanySearchResult)
                for unit in search_page.response.enheder
            )
            person_count += sum(
                isinstance(unit, PersonSearchResult)
                for unit in search_page.response.enheder
            )
            production_unit_count += sum(
                isinstance(unit, ProductionUnitSearchResult)
                for unit in search_page.response.enheder
            )
            if log_info is not None:
                log_info(
                    "DataCVR download progress: search_term=%s page=%s object_key=%s "
                    "downloaded_files=%s advertised_entities=%s downloaded_entities=%s "
                    "companies=%s persons=%s production_units=%s downloaded_bytes=%s",
                    search_term,
                    search_page.page_index,
                    key,
                    len(page_keys),
                    advertised_total,
                    company_count + person_count + production_unit_count,
                    company_count,
                    person_count,
                    production_unit_count,
                    total_size_bytes,
                )
    except DenmarkCvrValidationError as exc:
        key = invalid_page_object_key(search_term, run_id, exc.page_index)
        object_store.write_bytes(
            key,
            exc.raw_body.encode("utf-8"),
            bucket=DENMARK_CVR_BUCKET,
        )
        if log_info is not None:
            log_info(
                "DataCVR download stopped after invalid response: search_term=%s page=%s "
                "invalid_object_key=%s downloaded_files=%s downloaded_entities=%s "
                "downloaded_bytes=%s",
                search_term,
                exc.page_index,
                key,
                len(page_keys),
                company_count + person_count + production_unit_count,
                total_size_bytes,
            )
        raise

    entity_count = company_count + person_count + production_unit_count
    key = manifest_object_key(search_term, run_id)
    manifest_body = json.dumps(
        {
            "advertised_total": advertised_total,
            "bucket": DENMARK_CVR_BUCKET,
            "company_count": company_count,
            "entity_count": entity_count,
            "page_count": len(page_keys),
            "page_keys": page_keys,
            "person_count": person_count,
            "production_unit_count": production_unit_count,
            "retrieved_at": retrieved_at.isoformat(),
            "run_id": run_id,
            "search_term": search_term,
            "source": "denmark_cvr",
            "source_url": search.search_base_url,
            "total_size_bytes": total_size_bytes,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    object_store.write_json(
        key,
        manifest_body,
        bucket=DENMARK_CVR_BUCKET,
    )
    manifest_size_bytes = len(manifest_body.encode("utf-8"))
    summary = DenmarkCvrPartitionSummary(
        manifest_key=key,
        object_prefix=prefix,
        search_term=search_term,
        advertised_entity_count=advertised_total,
        downloaded_entity_count=entity_count,
        downloaded_file_count=len(page_keys),
        stored_file_count=len(page_keys) + 1,
        company_count=company_count,
        person_count=person_count,
        production_unit_count=production_unit_count,
        downloaded_size_bytes=total_size_bytes,
        manifest_size_bytes=manifest_size_bytes,
        stored_size_bytes=total_size_bytes + manifest_size_bytes,
    )
    if log_info is not None:
        log_info(
            "DataCVR download complete: search_term=%s downloaded_files=%s "
            "stored_files=%s advertised_entities=%s downloaded_entities=%s "
            "companies=%s persons=%s production_units=%s downloaded_bytes=%s "
            "stored_bytes=%s manifest_key=%s",
            summary.search_term,
            summary.downloaded_file_count,
            summary.stored_file_count,
            summary.advertised_entity_count,
            summary.downloaded_entity_count,
            summary.company_count,
            summary.person_count,
            summary.production_unit_count,
            summary.downloaded_size_bytes,
            summary.stored_size_bytes,
            summary.manifest_key,
        )
    return summary


@dg.asset(
    group_name="denmark_cvr",
    kinds={"python", "browser", "json", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "layer": "raw",
    },
    partitions_def=DENMARK_CVR_SEARCH_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="denmark_cvr_search",
    description=(
        "Captures validated raw DataCVR search response pages for one search-term "
        "partition. Search-term results may overlap and are normalized and deduplicated "
        "in a later DuckDB phase."
    ),
)
def denmark_cvr_search_results_s3(
    context: dg.AssetExecutionContext,
    denmark_cvr_search: DenmarkCvrSearchResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    summary = write_denmark_cvr_search_partition(
        object_store=object_store,
        search=denmark_cvr_search,
        search_term=context.partition_key,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": DENMARK_CVR_BUCKET,
            "s3_prefix": summary.object_prefix,
            "manifest_key": summary.manifest_key,
            "source_url": denmark_cvr_search.search_base_url,
            "search_term": summary.search_term,
            "advertised_entity_count": summary.advertised_entity_count,
            "downloaded_entity_count": summary.downloaded_entity_count,
            "downloaded_file_count": summary.downloaded_file_count,
            "stored_file_count": summary.stored_file_count,
            "company_count": summary.company_count,
            "person_count": summary.person_count,
            "production_unit_count": summary.production_unit_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
            "manifest_size_bytes": summary.manifest_size_bytes,
            "stored_size_bytes": summary.stored_size_bytes,
        }
    )


defs = dg.Definitions(
    assets=[denmark_cvr_search_results_s3],
    resources={"denmark_cvr_search": DenmarkCvrSearchResource()},
)


def _validate_object_scope(search_term: str, run_id: str) -> None:
    if search_term not in DENMARK_CVR_SEARCH_TERMS:
        raise ValueError(f"Unsupported DataCVR search term: {search_term!r}")
    if run_id == "" or any(
        not (character.isalnum() or character in {"-", "_", "."})
        for character in run_id
    ):
        raise ValueError("DataCVR run ID contains unsafe object-key characters")
