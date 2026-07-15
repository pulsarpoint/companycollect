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
    search_term: str
    page_count: int
    entity_count: int
    company_count: int
    person_count: int
    production_unit_count: int
    total_size_bytes: int


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
                    "Stored DataCVR page: search_term=%s page=%s object_key=%s entities=%s bytes=%s",
                    search_term,
                    search_page.page_index,
                    key,
                    len(search_page.response.enheder),
                    len(body),
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
                "Stored invalid DataCVR response: search_term=%s page=%s object_key=%s",
                search_term,
                exc.page_index,
                key,
            )
        raise

    entity_count = company_count + person_count + production_unit_count
    key = manifest_object_key(search_term, run_id)
    object_store.write_json(
        key,
        json.dumps(
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
        ),
        bucket=DENMARK_CVR_BUCKET,
    )
    return DenmarkCvrPartitionSummary(
        manifest_key=key,
        search_term=search_term,
        page_count=len(page_keys),
        entity_count=entity_count,
        company_count=company_count,
        person_count=person_count,
        production_unit_count=production_unit_count,
        total_size_bytes=total_size_bytes,
    )


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
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": DENMARK_CVR_BUCKET,
            "manifest_key": summary.manifest_key,
            "search_term": summary.search_term,
            "page_count": summary.page_count,
            "entity_count": summary.entity_count,
            "company_count": summary.company_count,
            "person_count": summary.person_count,
            "production_unit_count": summary.production_unit_count,
            "total_size_bytes": summary.total_size_bytes,
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
