import hashlib
import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from random import randint
from typing import Any, Self
from urllib.parse import urlencode, urlparse

import dagster as dg
import duckdb
from cloakbrowser import launch
from dagster_duckdb import DuckDBResource
from pydantic import model_validator

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.assets import DENMARK_CVR_BUCKET
from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_KEY_MAP,
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DENMARK_CVR_COMPANY_DETAIL_PREFIX,
    company_detail_object_key,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_PATH,
    DENMARK_CVR_DUCKDB_POOL,
    DENMARK_CVR_DUCKDB_SCHEMA,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_BASE_URL,
    SAFE_RESPONSE_HEADERS,
)

DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT = 128
DENMARK_CVR_PERSON_DETAIL_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:03d}"
        for bucket_index in range(DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT)
    ]
)
DENMARK_CVR_PERSON_DETAIL_PREFIX = "denmark_cvr/person_details"
DENMARK_CVR_PERSON_DETAIL_GROUP = "denmark_cvr_person_details"
DENMARK_CVR_PERSON_DETAIL_POOL = "denmark_cvr_person_details"
DENMARK_CVR_PERSON_DETAIL_MAPPING_VERSION = 1
DENMARK_CVR_PERSON_IDS_TABLE = "person_ids"

DATACVR_PERSON_DETAIL_SCRIPT = """
async ({ url }) => {
  const response = await fetch(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json, text/plain, */*",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        "X-Requested-With": "XMLHttpRequest",
      },
      credentials: "include",
    },
  );

  return {
    ok: response.ok,
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    body: await response.text(),
  };
}
"""

DENMARK_CVR_PERSON_DETAIL_KEY_MAP: dict[str, str] = {
    **DENMARK_CVR_COMPANY_DETAIL_KEY_MAP,
    "adresseHemmelig": "addressConfidential",
    "adresseHemmeligUndtagelse": "addressConfidentialException",
    "adresseOpdateringOphoert": "addressUpdateCeased",
    "aktiveHvidvaskAktiviteter": "activeAntiMoneyLaunderingActivities",
    "aktiveRelationer": "activeRelations",
    "franchise": "franchise",
    "konkurskarantaene": "bankruptcyDisqualification",
    "land": "country",
    "liberalUdoeverRegistreringer": "liberalPractitionerRegistrations",
    "liberaleErhverv": "liberalProfessions",
    "ophoerteRelationer": "ceasedRelations",
    "personRelationer": "personRelations",
    "registreretIHvidvask": "registeredInAntiMoneyLaunderingRegister",
    "simpleRelationer": "simpleRelations",
    "skjulRelationer": "hideRelations",
}


@dataclass(frozen=True, order=True)
class DenmarkCvrPersonDetailIdentity:
    person_id: str
    person_type: str

    def __post_init__(self) -> None:
        _validate_person_id(self.person_id)
        if self.person_type.strip() == "":
            raise ValueError("DataCVR person type must not be blank")


@dataclass(frozen=True)
class DenmarkCvrPersonDetailDownload:
    identity: DenmarkCvrPersonDetailIdentity
    source_url: str
    raw_body: str
    payload: dict[str, Any]
    status: int
    response_headers: dict[str, str]

    @property
    def downloaded_size_bytes(self) -> int:
        return len(self.raw_body.encode("utf-8"))


@dataclass(frozen=True)
class DenmarkCvrPersonIdCatalogSummary:
    company_count: int
    source_object_count: int
    source_relation_count: int
    person_count: int
    database_size_bytes: int


@dataclass(frozen=True)
class DenmarkCvrPersonDetailSummary:
    partition_key: str
    selected_person_count: int
    complete_person_count: int
    already_complete_person_count: int
    translated_existing_person_count: int
    downloaded_person_count: int
    written_object_count: int
    downloaded_size_bytes: int


class DenmarkCvrPersonDetailRequestError(RuntimeError):
    pass


class DenmarkCvrPersonDetailKeyError(ValueError):
    pass


class DenmarkCvrPersonIdCatalogError(ValueError):
    pass


class DenmarkCvrPersonDetailResource(dg.ConfigurableResource):
    detail_base_url: str = DATACVR_BASE_URL
    locale: str = "en"
    min_delay_ms: int = 100
    max_delay_ms: int = 800
    max_attempts: int = 5
    retry_base_delay_seconds: float = 5.0
    retry_max_delay_seconds: float = 120.0

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        _validate_https_base_url(self.detail_base_url)
        if self.locale != "en":
            raise ValueError("DataCVR person details must use locale='en'")
        if self.min_delay_ms < 0 or self.max_delay_ms < 0:
            raise ValueError("request delays must not be negative")
        if self.min_delay_ms > self.max_delay_ms:
            raise ValueError("min_delay_ms must not exceed max_delay_ms")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero")
        if self.retry_base_delay_seconds < 0 or self.retry_max_delay_seconds < 0:
            raise ValueError("retry delays must not be negative")
        if self.retry_base_delay_seconds > self.retry_max_delay_seconds:
            raise ValueError(
                "retry_base_delay_seconds must not exceed retry_max_delay_seconds"
            )
        return self

    def iter_person_details(
        self,
        identities: Sequence[DenmarkCvrPersonDetailIdentity],
        *,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[DenmarkCvrPersonDetailDownload]:
        selected_identities = tuple(identities)
        if not selected_identities:
            return

        try:
            browser = launcher()
        except Exception:
            raise DenmarkCvrPersonDetailRequestError(
                "DataCVR person-detail browser failed to start; verify Chromium "
                "runtime dependencies"
            ) from None

        try:
            page = browser.new_page()
            page.goto(
                _person_search_page_url(self.detail_base_url),
                wait_until="networkidle",
            )
            for person_index, identity in enumerate(selected_identities):
                if person_index > 0:
                    sleep(self._request_delay_seconds())
                source_url = person_detail_api_url(self.detail_base_url, identity)
                result = self._request_with_retry(
                    page,
                    identity=identity,
                    source_url=source_url,
                    sleep=sleep,
                )
                yield _validated_person_detail_download(
                    identity=identity,
                    source_url=source_url,
                    result=result,
                )
        finally:
            browser.close()

    def _request_delay_seconds(self) -> float:
        return randint(self.min_delay_ms, self.max_delay_ms) / 1_000

    def _request_with_retry(
        self,
        page: Any,
        *,
        identity: DenmarkCvrPersonDetailIdentity,
        source_url: str,
        sleep: Callable[[float], None],
    ) -> Any:
        result: Any = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = page.evaluate(
                    DATACVR_PERSON_DETAIL_SCRIPT,
                    {"url": source_url},
                )
            except Exception:
                raise DenmarkCvrPersonDetailRequestError(
                    "DataCVR person-detail request failed for entity "
                    f"{identity.person_id}"
                ) from None
            if not _is_retryable_person_detail_result(result):
                return result
            if attempt < self.max_attempts:
                sleep(
                    _person_detail_retry_delay_seconds(
                        result,
                        attempt=attempt,
                        base_delay_seconds=self.retry_base_delay_seconds,
                        max_delay_seconds=self.retry_max_delay_seconds,
                    )
                )
        return result


def company_detail_person_identities(
    payload: Mapping[str, Any],
) -> tuple[DenmarkCvrPersonDetailIdentity, ...]:
    person_section = payload.get("personkreds")
    if not isinstance(person_section, Mapping):
        raise DenmarkCvrPersonIdCatalogError(
            "DataCVR company detail personkreds must be an object"
        )
    person_groups = person_section.get("personkredser")
    ceased_people = person_section.get("ophoerteFad")
    if not isinstance(person_groups, list) or not isinstance(ceased_people, list):
        raise DenmarkCvrPersonIdCatalogError(
            "DataCVR company detail person lists must be arrays"
        )

    records: list[Any] = list(ceased_people)
    for group in person_groups:
        if not isinstance(group, Mapping) or not isinstance(
            group.get("personRoller"), list
        ):
            raise DenmarkCvrPersonIdCatalogError(
                "DataCVR company detail person group is invalid"
            )
        records.extend(group["personRoller"])

    identities: dict[str, DenmarkCvrPersonDetailIdentity] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise DenmarkCvrPersonIdCatalogError(
                "DataCVR company detail person record must be an object"
            )
        if str(record.get("enhedstype", "")).lower() != "person":
            continue
        person_id = record.get("id")
        person_type = record.get("personType")
        if not isinstance(person_id, str) or not isinstance(person_type, str):
            raise DenmarkCvrPersonIdCatalogError(
                "DataCVR person record requires text id and personType"
            )
        identity = DenmarkCvrPersonDetailIdentity(person_id, person_type)
        existing = identities.get(identity.person_id)
        if existing is not None and existing.person_type != identity.person_type:
            raise DenmarkCvrPersonIdCatalogError(
                "DataCVR company detail contains conflicting person types for "
                f"entity {identity.person_id}"
            )
        identities[identity.person_id] = identity
    return tuple(sorted(identities.values()))


def rebuild_company_detail_person_ids(
    *,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
    rebuilt_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrPersonIdCatalogSummary:
    if rebuilt_at.utcoffset() is None:
        raise ValueError("Person-ID catalog rebuild timestamp must include a timezone")

    database_path = duckdb_database_path(denmark_cvr_duckdb)
    if str(database_path) != ":memory:":
        database_path.parent.mkdir(parents=True, exist_ok=True)

    with denmark_cvr_duckdb.get_connection() as connection:
        try:
            company_count = int(
                connection.execute(
                    f"select count(*) from {DENMARK_CVR_DUCKDB_SCHEMA}."
                    f"{DENMARK_CVR_COMPANIES_TABLE}"
                ).fetchone()[0]
            )
        except duckdb.CatalogException:
            raise RuntimeError(
                "Denmark CVR person IDs require the "
                "denmark_cvr_companies_duckdb asset; materialize it first"
            ) from None

        _require_complete_company_details(object_store, connection)
        connection.execute(
            "create or replace temporary table denmark_cvr_staged_person_ids "
            "(person_id varchar, person_type varchar, company_cvr varchar)"
        )
        source_object_count = 0
        source_relation_count = 0
        for partition_index, partition_key in enumerate(
            DENMARK_CVR_COMPANY_DETAIL_PARTITIONS.get_partition_keys(),
            start=1,
        ):
            cvrs = _company_partition_cvrs(connection, partition_key)
            rows: list[tuple[str, str, str]] = []
            for cvr in cvrs:
                object_key = company_detail_object_key(
                    partition_key,
                    cvr,
                    english_keys=False,
                )
                payload = _read_company_detail_payload(object_store, object_key, cvr)
                rows.extend(
                    (identity.person_id, identity.person_type, cvr)
                    for identity in company_detail_person_identities(payload)
                )
                source_object_count += 1
            if rows:
                connection.executemany(
                    "insert into denmark_cvr_staged_person_ids values (?, ?, ?)",
                    rows,
                )
            source_relation_count += len(rows)
            if log_info is not None and (
                partition_index == 1
                or partition_index % 8 == 0
                or partition_index == DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT
            ):
                log_info(
                    "DataCVR person-ID catalog progress: company_bucket=%s/%s "
                    "companies=%s relations=%s",
                    partition_index,
                    DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT,
                    source_object_count,
                    source_relation_count,
                )

        conflict = connection.execute(
            """
            select person_id
            from denmark_cvr_staged_person_ids
            group by person_id
            having count(distinct person_type) > 1
            limit 1
            """
        ).fetchone()
        if conflict is not None:
            raise DenmarkCvrPersonIdCatalogError(
                "DataCVR company details contain conflicting person types for "
                f"entity {conflict[0]}"
            )

        connection.execute("begin transaction")
        try:
            connection.execute(
                f"create schema if not exists {DENMARK_CVR_DUCKDB_SCHEMA}"
            )
            connection.execute(
                f"""
                create or replace table
                  {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSON_IDS_TABLE} (
                    person_id varchar primary key,
                    person_type varchar not null,
                    source_company_count bigint not null,
                    source_relation_count bigint not null,
                    rebuilt_at timestamptz not null
                  )
                """
            )
            connection.execute(
                f"""
                insert into {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSON_IDS_TABLE}
                select
                  person_id,
                  min(person_type),
                  count(distinct company_cvr),
                  count(*),
                  ?
                from denmark_cvr_staged_person_ids
                group by person_id
                """,
                [rebuilt_at.astimezone(UTC)],
            )
            person_count = int(
                connection.execute(
                    f"select count(*) from {DENMARK_CVR_DUCKDB_SCHEMA}."
                    f"{DENMARK_CVR_PERSON_IDS_TABLE}"
                ).fetchone()[0]
            )
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise

    database_size_bytes = (
        database_path.stat().st_size
        if str(database_path) != ":memory:" and database_path.exists()
        else 0
    )
    return DenmarkCvrPersonIdCatalogSummary(
        company_count=company_count,
        source_object_count=source_object_count,
        source_relation_count=source_relation_count,
        person_count=person_count,
        database_size_bytes=database_size_bytes,
    )


def person_detail_bucket_key(person_id: str) -> str:
    _validate_person_id(person_id)
    digest = hashlib.md5(person_id.encode("ascii"), usedforsecurity=False).digest()
    duckdb_lower_md5 = int.from_bytes(digest[-8:], byteorder="little")
    bucket_index = duckdb_lower_md5 % DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT
    return f"bucket_{bucket_index:03d}"


def person_detail_partition_identities(
    denmark_cvr_duckdb: DuckDBResource,
    partition_key: str,
) -> tuple[DenmarkCvrPersonDetailIdentity, ...]:
    bucket_index = _person_detail_bucket_index(partition_key)
    try:
        with denmark_cvr_duckdb.get_connection() as connection:
            rows = connection.execute(
                f"""
                select person_id, person_type
                from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSON_IDS_TABLE}
                where md5_number_lower(person_id) % ? = ?
                order by person_id
                """,
                [DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT, bucket_index],
            ).fetchall()
    except duckdb.CatalogException:
        raise RuntimeError(
            "Denmark CVR person details require the "
            "denmark_cvr_company_detail_person_ids_duckdb asset; materialize it first"
        ) from None
    return tuple(
        DenmarkCvrPersonDetailIdentity(str(person_id), str(person_type))
        for person_id, person_type in rows
    )


def person_detail_api_url(
    base_url: str,
    identity: DenmarkCvrPersonDetailIdentity,
) -> str:
    normalized_base_url = _validate_https_base_url(base_url)
    query = urlencode(
        {
            "enhedsnummer": identity.person_id,
            "persontype": identity.person_type,
            "locale": "en",
        }
    )
    return f"{normalized_base_url}/gateway/person/hentPerson?{query}"


def person_detail_object_key(
    partition_key: str,
    person_id: str,
    *,
    english_keys: bool,
) -> str:
    if person_detail_bucket_key(person_id) != partition_key:
        raise ValueError(
            f"Person entity {person_id} does not belong to partition {partition_key}"
        )
    filename = "person_en.json" if english_keys else "person.json"
    return (
        f"{DENMARK_CVR_PERSON_DETAIL_PREFIX}/{partition_key}/"
        f"enhedsnummer={person_id}/{filename}"
    )


def translate_person_detail_keys(payload: Mapping[str, Any]) -> dict[str, Any]:
    unmapped_paths = tuple(_unmapped_person_key_paths(payload, path=()))
    if unmapped_paths:
        raise DenmarkCvrPersonDetailKeyError(
            "Unmapped DataCVR person-detail keys at " + ", ".join(unmapped_paths)
        )
    translated = _translate_person_value(payload, path=())
    if not isinstance(translated, dict):
        raise TypeError("Translated DataCVR person detail must be an object")
    return translated


def write_person_detail_partition(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrPersonDetailResource,
    partition_key: str,
    identities: Sequence[DenmarkCvrPersonDetailIdentity],
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrPersonDetailSummary:
    _person_detail_bucket_index(partition_key)
    selected_identities = tuple(identities)
    object_keys = {
        identity.person_id: (
            person_detail_object_key(
                partition_key,
                identity.person_id,
                english_keys=False,
            ),
            person_detail_object_key(
                partition_key,
                identity.person_id,
                english_keys=True,
            ),
        )
        for identity in selected_identities
    }
    if len(object_keys) != len(selected_identities):
        raise ValueError("DataCVR person-detail identities contain duplicate IDs")

    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    existing_keys = set(
        object_store.list_keys(
            f"{DENMARK_CVR_PERSON_DETAIL_PREFIX}/{partition_key}/",
            bucket=DENMARK_CVR_BUCKET,
        )
    )
    already_complete_count = 0
    translated_existing_count = 0
    written_object_count = 0
    pending: list[DenmarkCvrPersonDetailIdentity] = []
    for identity in selected_identities:
        original_key, english_key = object_keys[identity.person_id]
        if original_key in existing_keys and english_key in existing_keys:
            already_complete_count += 1
            continue
        if original_key in existing_keys:
            payload = _json_object(
                object_store.read_bytes(original_key, bucket=DENMARK_CVR_BUCKET),
                context=f"stored person detail {identity.person_id}",
            )
            object_store.write_bytes(
                english_key,
                _translated_person_json_bytes(payload),
                bucket=DENMARK_CVR_BUCKET,
            )
            translated_existing_count += 1
            written_object_count += 1
            continue
        pending.append(identity)

    downloaded_count = 0
    downloaded_size_bytes = 0
    returned_ids: set[str] = set()
    for download in details.iter_person_details(tuple(pending)):
        person_id = download.identity.person_id
        if person_id not in object_keys or person_id in returned_ids:
            raise DenmarkCvrPersonDetailRequestError(
                "DataCVR person-detail download returned an unexpected entity"
            )
        returned_ids.add(person_id)
        original_key, english_key = object_keys[person_id]
        object_store.write_bytes(
            original_key,
            download.raw_body.encode("utf-8"),
            bucket=DENMARK_CVR_BUCKET,
        )
        object_store.write_bytes(
            english_key,
            _translated_person_json_bytes(download.payload),
            bucket=DENMARK_CVR_BUCKET,
        )
        downloaded_count += 1
        downloaded_size_bytes += download.downloaded_size_bytes
        written_object_count += 2
        if log_info is not None and (
            downloaded_count == 1
            or downloaded_count % 100 == 0
            or downloaded_count == len(pending)
        ):
            log_info(
                "DataCVR person-detail progress: partition=%s downloaded=%s/%s "
                "downloaded_bytes=%s",
                partition_key,
                downloaded_count,
                len(pending),
                downloaded_size_bytes,
            )
    if returned_ids != {identity.person_id for identity in pending}:
        raise DenmarkCvrPersonDetailRequestError(
            "DataCVR person-detail download did not return every selected entity"
        )

    complete_count = (
        already_complete_count + translated_existing_count + downloaded_count
    )
    return DenmarkCvrPersonDetailSummary(
        partition_key=partition_key,
        selected_person_count=len(selected_identities),
        complete_person_count=complete_count,
        already_complete_person_count=already_complete_count,
        translated_existing_person_count=translated_existing_count,
        downloaded_person_count=downloaded_count,
        written_object_count=written_object_count,
        downloaded_size_bytes=downloaded_size_bytes,
    )


@dg.asset(
    deps=[
        dg.AssetKey("denmark_cvr_companies_duckdb"),
        dg.AssetKey("denmark_cvr_company_details_s3"),
    ],
    group_name=DENMARK_CVR_PERSON_DETAIL_GROUP,
    kinds={"python", "s3", "json", "duckdb"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "person",
        "layer": "catalog",
    },
    pool=DENMARK_CVR_DUCKDB_POOL,
    metadata={
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_PERSON_IDS_TABLE,
    },
    description=(
        "Requires every company-detail hash partition to contain original and "
        "English-key JSON, then rebuilds the deduplicated DataCVR person-ID and "
        "person-type catalog used by person-detail downloads."
    ),
)
def denmark_cvr_company_detail_person_ids_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    summary = rebuild_company_detail_person_ids(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        rebuilt_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "company_count": summary.company_count,
            "source_object_count": summary.source_object_count,
            "source_relation_count": summary.source_relation_count,
            "person_count": summary.person_count,
            "database_path": str(DENMARK_CVR_DUCKDB_PATH),
            "database_size_bytes": summary.database_size_bytes,
            "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
            "duckdb_table": DENMARK_CVR_PERSON_IDS_TABLE,
            "required_company_detail_partition_count": (
                DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT
            ),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_company_detail_person_ids_duckdb")],
    group_name=DENMARK_CVR_PERSON_DETAIL_GROUP,
    kinds={"python", "browser", "duckdb", "json", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "person",
        "layer": "raw_detail",
    },
    partitions_def=DENMARK_CVR_PERSON_DETAIL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_PERSON_DETAIL_POOL,
    description=(
        "Reads one stable 128-way person-ID hash bucket from DuckDB, downloads "
        "each HTTPS DataCVR person detail in one browser session, and checkpoints "
        "original and English-key JSON objects."
    ),
)
def denmark_cvr_person_details_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_person_details: DenmarkCvrPersonDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    summary = write_person_detail_partition(
        object_store=object_store,
        details=denmark_cvr_person_details,
        partition_key=partition_key,
        identities=person_detail_partition_identities(
            denmark_cvr_duckdb,
            partition_key,
        ),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_key": summary.partition_key,
            "selected_person_count": summary.selected_person_count,
            "complete_person_count": summary.complete_person_count,
            "already_complete_person_count": (summary.already_complete_person_count),
            "translated_existing_person_count": (
                summary.translated_existing_person_count
            ),
            "downloaded_person_count": summary.downloaded_person_count,
            "written_object_count": summary.written_object_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
            "key_mapping_version": DENMARK_CVR_PERSON_DETAIL_MAPPING_VERSION,
            "hash_bucket_count": DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT,
            "s3_bucket": DENMARK_CVR_BUCKET,
            "s3_prefix": (f"{DENMARK_CVR_PERSON_DETAIL_PREFIX}/{partition_key}/"),
        }
    )


def _require_complete_company_details(
    object_store: ObjectStoreResource,
    connection: duckdb.DuckDBPyConnection,
) -> None:
    missing_original_count = 0
    missing_english_count = 0
    expected_count = 0
    for partition_key in DENMARK_CVR_COMPANY_DETAIL_PARTITIONS.get_partition_keys():
        cvrs = _company_partition_cvrs(connection, partition_key)
        expected_count += len(cvrs)
        existing_keys = set(
            object_store.list_keys(
                f"{DENMARK_CVR_COMPANY_DETAIL_PREFIX}/{partition_key}/",
                bucket=DENMARK_CVR_BUCKET,
            )
        )
        for cvr in cvrs:
            if (
                company_detail_object_key(
                    partition_key,
                    cvr,
                    english_keys=False,
                )
                not in existing_keys
            ):
                missing_original_count += 1
            if (
                company_detail_object_key(
                    partition_key,
                    cvr,
                    english_keys=True,
                )
                not in existing_keys
            ):
                missing_english_count += 1
    if missing_original_count or missing_english_count:
        raise RuntimeError(
            "Denmark CVR company details are not fully materialized: "
            f"expected_companies={expected_count} "
            f"missing_original={missing_original_count} "
            f"missing_english={missing_english_count}"
        )


def _company_partition_cvrs(
    connection: duckdb.DuckDBPyConnection,
    partition_key: str,
) -> tuple[str, ...]:
    bucket_index = _company_detail_bucket_index(partition_key)
    rows = connection.execute(
        f"""
        select cvr
        from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
        where md5_number_lower(cvr) % ? = ?
        order by cvr
        """,
        [DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT, bucket_index],
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _read_company_detail_payload(
    object_store: ObjectStoreResource,
    object_key: str,
    cvr: str,
) -> dict[str, Any]:
    payload = _json_object(
        object_store.read_bytes(object_key, bucket=DENMARK_CVR_BUCKET),
        context=f"company detail {cvr}",
    )
    master_data = payload.get("stamdata")
    response_cvr = (
        master_data.get("cvrnummer") if isinstance(master_data, Mapping) else None
    )
    if response_cvr != cvr:
        raise DenmarkCvrPersonIdCatalogError(
            f"DataCVR company detail CVR mismatch for {cvr}"
        )
    return payload


def _validated_person_detail_download(
    *,
    identity: DenmarkCvrPersonDetailIdentity,
    source_url: str,
    result: Any,
) -> DenmarkCvrPersonDetailDownload:
    if not isinstance(result, Mapping):
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail response is invalid for {identity.person_id}"
        )
    status = result.get("status")
    raw_body = result.get("body")
    headers = result.get("headers")
    if not isinstance(status, int) or not isinstance(raw_body, str):
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail response is invalid for {identity.person_id}"
        )
    if not isinstance(headers, Mapping):
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail headers are invalid for {identity.person_id}"
        )
    safe_headers = {
        str(key).lower(): str(value)
        for key, value in headers.items()
        if str(key).lower() in SAFE_RESPONSE_HEADERS
    }
    if status != 200:
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail request returned HTTP {status} for "
            f"{identity.person_id}"
        )
    content_type = safe_headers.get("content-type", "").lower()
    if "application/json" not in content_type:
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail response is not JSON for {identity.person_id}"
        )
    payload = _json_object(
        raw_body.encode("utf-8"),
        context=f"person detail {identity.person_id}",
    )
    if not isinstance(payload.get("stamdata"), Mapping) or not isinstance(
        payload.get("personRelationer"), Mapping
    ):
        raise DenmarkCvrPersonDetailRequestError(
            f"DataCVR person-detail JSON has an invalid shape for {identity.person_id}"
        )
    return DenmarkCvrPersonDetailDownload(
        identity=identity,
        source_url=source_url,
        raw_body=raw_body,
        payload=payload,
        status=status,
        response_headers=safe_headers,
    )


def _is_retryable_person_detail_result(result: Any) -> bool:
    return isinstance(result, Mapping) and result.get("status") in {
        429,
        500,
        502,
        503,
        504,
    }


def _person_detail_retry_delay_seconds(
    result: Mapping[str, Any],
    *,
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> float:
    headers = result.get("headers")
    if isinstance(headers, Mapping):
        retry_after = next(
            (
                value
                for key, value in headers.items()
                if str(key).lower() == "retry-after"
            ),
            None,
        )
        if isinstance(retry_after, str):
            try:
                parsed_retry_after = float(retry_after)
            except ValueError:
                parsed_retry_after = -1
            if parsed_retry_after >= 0:
                return min(parsed_retry_after, max_delay_seconds)
    return min(base_delay_seconds * (2 ** (attempt - 1)), max_delay_seconds)


def _translated_person_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        translate_person_detail_keys(payload),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _translate_person_value(value: Any, *, path: tuple[str, ...]) -> Any:
    if isinstance(value, Mapping):
        translated: dict[str, Any] = {}
        for source_key, child in value.items():
            if not isinstance(source_key, str):
                raise DenmarkCvrPersonDetailKeyError(
                    f"DataCVR person-detail key at {_display_path(path)} is not text"
                )
            if source_key not in DENMARK_CVR_PERSON_DETAIL_KEY_MAP:
                raise DenmarkCvrPersonDetailKeyError(
                    "Unmapped DataCVR person-detail key "
                    f"{source_key!r} at {_display_path((*path, source_key))}"
                )
            target_key = DENMARK_CVR_PERSON_DETAIL_KEY_MAP[source_key]
            if target_key in translated:
                raise DenmarkCvrPersonDetailKeyError(
                    "DataCVR person-detail key mapping collision at "
                    f"{_display_path(path)} for English key {target_key!r}"
                )
            translated[target_key] = _translate_person_value(
                child,
                path=(*path, source_key),
            )
        return translated
    if isinstance(value, list):
        return [
            _translate_person_value(item, path=(*path, f"[{index}]"))
            for index, item in enumerate(value)
        ]
    return value


def _unmapped_person_key_paths(
    value: Any,
    *,
    path: tuple[str, ...],
) -> Iterator[str]:
    if isinstance(value, Mapping):
        for source_key, child in value.items():
            child_path = (*path, str(source_key))
            if (
                not isinstance(source_key, str)
                or source_key not in DENMARK_CVR_PERSON_DETAIL_KEY_MAP
            ):
                yield _display_path(child_path)
            yield from _unmapped_person_key_paths(child, path=child_path)
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            yield from _unmapped_person_key_paths(
                child,
                path=(*path, f"[{index}]"),
            )


def _json_object(raw_body: bytes, *, context: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_body)
    except UnicodeDecodeError, json.JSONDecodeError:
        raise ValueError(f"DataCVR {context} is not valid JSON") from None
    if not isinstance(payload, dict):
        raise ValueError(f"DataCVR {context} must be a JSON object")
    return payload


def _validate_https_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or parsed.netloc == "":
        raise ValueError("DataCVR person-detail base URL must use HTTPS")
    return normalized


def _person_search_page_url(base_url: str) -> str:
    normalized = _validate_https_base_url(base_url)
    return f"{normalized}/soegeresultater?sideIndex=0&enhedstype=person&size=1"


def _validate_person_id(person_id: str) -> None:
    if len(person_id) != 10 or not person_id.isascii() or not person_id.isdigit():
        raise ValueError("DataCVR person entity number must contain ten digits")


def _person_detail_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid DataCVR person-detail partition: {partition_key}")
    bucket_index = int(suffix)
    if (
        bucket_index < 0
        or bucket_index >= DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT
        or partition_key != f"bucket_{bucket_index:03d}"
    ):
        raise ValueError(f"Invalid DataCVR person-detail partition: {partition_key}")
    return bucket_index


def _company_detail_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid DataCVR company-detail partition: {partition_key}")
    bucket_index = int(suffix)
    if (
        bucket_index < 0
        or bucket_index >= DENMARK_CVR_PERSON_DETAIL_BUCKET_COUNT
        or partition_key != f"bucket_{bucket_index:03d}"
    ):
        raise ValueError(f"Invalid DataCVR company-detail partition: {partition_key}")
    return bucket_index


def _display_path(path: tuple[str, ...]) -> str:
    return ".".join(path) if path else "<root>"


defs = dg.Definitions(
    assets=[
        denmark_cvr_company_detail_person_ids_duckdb,
        denmark_cvr_person_details_s3,
    ],
    resources={
        "denmark_cvr_person_details": DenmarkCvrPersonDetailResource(),
        "denmark_cvr_duckdb": duckdb_resource(DENMARK_CVR_DUCKDB_PATH),
    },
)
