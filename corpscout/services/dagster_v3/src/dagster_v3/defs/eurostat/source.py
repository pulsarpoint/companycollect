from __future__ import annotations

import gzip
import hashlib
import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import dagster as dg
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

EUROSTAT_API_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
EUROSTAT_RAW_BUCKET = "source-eurostat"
EUROSTAT_RAW_PREFIX = "eurostat/dissemination"
EUROSTAT_START_YEAR = 2010
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 2.0
DOWNLOAD_CHUNK_BYTES = 1 << 20
DEFAULT_USER_AGENT = "corpscout-dagster-v3-eurostat/0.1"

_COMMON_NAMESPACE = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
_STRUCTURE_NAMESPACE = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
_XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"
_NAMESPACES = {"c": _COMMON_NAMESPACE, "s": _STRUCTURE_NAMESPACE}


@dataclass(frozen=True)
class EurostatDataset:
    code: str
    expected_dimensions: tuple[str, ...]


@dataclass(frozen=True)
class EurostatDimensionValue:
    code: str
    label: str
    position: int


@dataclass(frozen=True)
class EurostatDimension:
    code: str
    label: str
    position: int
    values: tuple[EurostatDimensionValue, ...]


@dataclass(frozen=True)
class EurostatStructureMetadata:
    dataset_code: str
    title: str
    dsd_version: str
    source_observation_count: int
    source_oldest_period: str
    source_latest_period: str
    data_updated_at: datetime
    structure_updated_at: datetime
    dimensions: tuple[EurostatDimension, ...]


EUROSTAT_DATASETS = (
    EurostatDataset(
        code="nama_10_gdp",
        expected_dimensions=("freq", "unit", "na_item", "geo"),
    ),
    EurostatDataset(
        code="nama_10_pc",
        expected_dimensions=("freq", "unit", "na_item", "geo"),
    ),
    EurostatDataset(
        code="nama_10_a10",
        expected_dimensions=("freq", "unit", "nace_r2", "na_item", "geo"),
    ),
    EurostatDataset(
        code="gov_10dd_edpt1",
        expected_dimensions=("freq", "unit", "sector", "na_item", "geo"),
    ),
    EurostatDataset(
        code="gov_10a_main",
        expected_dimensions=("freq", "unit", "sector", "na_item", "geo"),
    ),
    EurostatDataset(
        code="prc_hicp_aind",
        expected_dimensions=("freq", "unit", "coicop", "geo"),
    ),
    EurostatDataset(
        code="une_rt_a",
        expected_dimensions=("freq", "age", "unit", "sex", "geo"),
    ),
    EurostatDataset(
        code="demo_gind",
        expected_dimensions=("freq", "indic_de", "geo"),
    ),
    EurostatDataset(
        code="bd_size",
        expected_dimensions=(
            "freq",
            "age",
            "sizeclas",
            "indic_sbs",
            "nace_r2",
            "geo",
        ),
    ),
    EurostatDataset(
        code="bd_hg",
        expected_dimensions=("freq", "indic_sbs", "nace_r2", "geo"),
    ),
    EurostatDataset(
        code="sbs_ovw_act",
        expected_dimensions=("freq", "nace_r2", "indic_sbs", "geo"),
    ),
    EurostatDataset(
        code="sbs_sc_ovw",
        expected_dimensions=("freq", "indic_sbs", "nace_r2", "size_emp", "geo"),
    ),
)


def dataset_tsv_url(dataset: EurostatDataset) -> str:
    return (
        f"{EUROSTAT_API_BASE_URL}/data/{dataset.code.upper()}"
        "?format=tsv&compressed=true"
    )


def dataset_structure_url(dataset: EurostatDataset) -> str:
    return (
        f"{EUROSTAT_API_BASE_URL}/dataflow/ESTAT/{dataset.code.upper()}/1.0"
        "?references=descendants&detail=referencepartial"
    )


def snapshot_manifest_key(run_id: str) -> str:
    return f"{EUROSTAT_RAW_PREFIX}/snapshots/run_id={run_id}/manifest.json"


def read_snapshot_manifest(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> dict[str, Any]:
    key = snapshot_manifest_key(run_id)
    if not object_store.exists(key, bucket=EUROSTAT_RAW_BUCKET):
        raise ValueError(
            f"Eurostat snapshot manifest {key} does not exist; materialize "
            "eurostat_snapshot_s3 in the same run"
        )
    payload = json.loads(
        object_store.read_bytes(key, bucket=EUROSTAT_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Eurostat snapshot manifest {key} is not a JSON object")
    return payload


def sync_eurostat_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    datasets: tuple[EurostatDataset, ...],
    session: Any | None,
    timeout_seconds: int,
) -> dg.MaterializeResult:
    if len(datasets) == 0:
        raise ValueError("Eurostat snapshot requires at least one dataset")
    dataset_codes = tuple(dataset.code for dataset in datasets)
    if len(set(dataset_codes)) != len(dataset_codes):
        raise ValueError("Eurostat snapshot dataset codes must be unique")
    object_store.ensure_bucket(EUROSTAT_RAW_BUCKET)
    owns_session = session is None
    http_session = session or eurostat_http_session()
    manifest_datasets: list[dict[str, Any]] = []
    downloaded_count = 0
    reused_count = 0
    total_bytes = 0

    try:
        with tempfile.TemporaryDirectory(prefix="eurostat_snapshot_") as temp_dir:
            temp_path = Path(temp_dir)
            for dataset in datasets:
                data_path = temp_path / f"{dataset.code}.tsv.gz"
                structure_path = temp_path / f"{dataset.code}.structure.xml"
                data_url = dataset_tsv_url(dataset)
                structure_url = dataset_structure_url(dataset)

                data_size, data_hash, data_content_type = _download_validated_file(
                    source_url=data_url,
                    target_path=data_path,
                    timeout_seconds=timeout_seconds,
                    session=http_session,
                    validator=lambda path, selected=dataset: validate_tsv_gzip(
                        path,
                        dataset=selected,
                    ),
                )
                structure_size, structure_hash, structure_content_type = (
                    _download_validated_file(
                        source_url=structure_url,
                        target_path=structure_path,
                        timeout_seconds=timeout_seconds,
                        session=http_session,
                        validator=lambda path, selected=dataset: (
                            parse_structure_metadata(
                                path.read_bytes(),
                                dataset=selected,
                            )
                        ),
                    )
                )
                metadata = parse_structure_metadata(
                    structure_path.read_bytes(),
                    dataset=dataset,
                )

                data_key = (
                    f"{EUROSTAT_RAW_PREFIX}/raw/dataset={dataset.code}/"
                    f"data/sha256={data_hash}/{dataset.code}.tsv.gz"
                )
                structure_key = (
                    f"{EUROSTAT_RAW_PREFIX}/raw/dataset={dataset.code}/"
                    f"structure/sha256={structure_hash}/"
                    f"{dataset.code}.structure.xml"
                )
                data_downloaded = not object_store.exists(
                    data_key,
                    bucket=EUROSTAT_RAW_BUCKET,
                )
                structure_downloaded = not object_store.exists(
                    structure_key,
                    bucket=EUROSTAT_RAW_BUCKET,
                )
                if data_downloaded:
                    object_store.upload_file(
                        data_key,
                        data_path,
                        bucket=EUROSTAT_RAW_BUCKET,
                    )
                if structure_downloaded:
                    object_store.upload_file(
                        structure_key,
                        structure_path,
                        bucket=EUROSTAT_RAW_BUCKET,
                    )

                downloaded_count += int(data_downloaded) + int(structure_downloaded)
                reused_count += int(not data_downloaded) + int(not structure_downloaded)
                total_bytes += data_size + structure_size
                manifest_datasets.append(
                    {
                        "dataset_code": dataset.code,
                        "title": metadata.title,
                        "dsd_version": metadata.dsd_version,
                        "dimensions": list(dataset.expected_dimensions),
                        "source_observation_count": metadata.source_observation_count,
                        "source_oldest_period": metadata.source_oldest_period,
                        "source_latest_period": metadata.source_latest_period,
                        "data_updated_at": metadata.data_updated_at.isoformat(),
                        "structure_updated_at": (
                            metadata.structure_updated_at.isoformat()
                        ),
                        "data": {
                            "source_url": data_url,
                            "object_key": data_key,
                            "sha256": data_hash,
                            "size_bytes": data_size,
                            "content_type": data_content_type,
                            "downloaded": data_downloaded,
                        },
                        "structure": {
                            "source_url": structure_url,
                            "object_key": structure_key,
                            "sha256": structure_hash,
                            "size_bytes": structure_size,
                            "content_type": structure_content_type,
                            "downloaded": structure_downloaded,
                        },
                    }
                )
    finally:
        if owns_session:
            http_session.close()

    manifest = {
        "source": "eurostat",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "start_year": EUROSTAT_START_YEAR,
        "datasets": manifest_datasets,
    }
    manifest_key = snapshot_manifest_key(run_id)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=EUROSTAT_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": EUROSTAT_RAW_BUCKET,
            "manifest_key": manifest_key,
            "dataset_count": len(manifest_datasets),
            "object_count": len(manifest_datasets) * 2,
            "downloaded_object_count": downloaded_count,
            "reused_object_count": reused_count,
            "size_bytes": total_bytes,
        }
    )


def eurostat_http_session() -> dlt_requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
    )
    client.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return client.session


def validate_tsv_gzip(path: Path, *, dataset: EurostatDataset) -> None:
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as file_handle:
        header = file_handle.readline().rstrip("\r\n")
        first_data_row = file_handle.readline()
    if first_data_row == "":
        raise ValueError(f"Eurostat dataset {dataset.code} contains no data rows")
    first_column, separator, _ = header.partition("\t")
    if separator == "":
        raise ValueError(f"Eurostat dataset {dataset.code} has no time columns")
    dimensions_text, marker, _ = first_column.partition("\\TIME_PERIOD")
    if marker == "":
        raise ValueError(
            f"Eurostat dataset {dataset.code} has no TIME_PERIOD header marker"
        )
    dimensions = tuple(part.strip() for part in dimensions_text.split(","))
    if dimensions != dataset.expected_dimensions:
        raise ValueError(
            f"Eurostat dataset {dataset.code} dimensions changed: "
            f"expected {dataset.expected_dimensions}, got {dimensions}"
        )


def parse_structure_metadata(
    payload: bytes,
    *,
    dataset: EurostatDataset,
) -> EurostatStructureMetadata:
    root = ElementTree.fromstring(payload)
    dataflow = _element_by_id(
        root.findall(".//s:Dataflow", _NAMESPACES),
        dataset.code,
        element_name="dataflow",
    )
    data_structure = _element_by_id(
        root.findall(".//s:DataStructure", _NAMESPACES),
        dataset.code,
        element_name="data structure",
    )
    annotations = {
        str(
            annotation.findtext("c:AnnotationType", default="", namespaces=_NAMESPACES)
        ): str(
            annotation.findtext("c:AnnotationTitle", default="", namespaces=_NAMESPACES)
        )
        for annotation in dataflow.findall(
            "c:Annotations/c:Annotation",
            _NAMESPACES,
        )
    }
    concepts = {
        str(concept.attrib.get("id", "")).casefold(): _english_name(concept)
        for concept in root.findall(".//s:ConceptScheme/s:Concept", _NAMESPACES)
    }
    codelists = {
        str(codelist.attrib.get("id", "")).casefold(): codelist
        for codelist in root.findall(".//s:Codelist", _NAMESPACES)
    }

    dimensions: list[EurostatDimension] = []
    for dimension in data_structure.findall(
        ".//s:DimensionList/s:Dimension",
        _NAMESPACES,
    ):
        dimension_code = str(dimension.attrib.get("id", "")).casefold()
        position = int(dimension.attrib.get("position", "0"))
        codelist_ref = dimension.find(
            "s:LocalRepresentation/s:Enumeration/*",
            _NAMESPACES,
        )
        if codelist_ref is None:
            raise ValueError(
                f"Eurostat dataset {dataset.code} dimension {dimension_code} "
                "has no codelist"
            )
        codelist_code = str(codelist_ref.attrib.get("id", "")).casefold()
        codelist = codelists.get(codelist_code)
        if codelist is None:
            raise ValueError(
                f"Eurostat dataset {dataset.code} has no {codelist_code} codelist"
            )
        values = tuple(
            EurostatDimensionValue(
                code=str(code.attrib.get("id", "")),
                label=_english_name(code),
                position=value_position,
            )
            for value_position, code in enumerate(
                codelist.findall("s:Code", _NAMESPACES),
                start=1,
            )
        )
        if len(values) == 0:
            raise ValueError(
                f"Eurostat dataset {dataset.code} codelist {codelist_code} is empty"
            )
        dimensions.append(
            EurostatDimension(
                code=dimension_code,
                label=concepts.get(dimension_code, dimension_code),
                position=position,
                values=values,
            )
        )

    dimensions.sort(key=lambda item: item.position)
    actual_dimensions = tuple(dimension.code for dimension in dimensions)
    if actual_dimensions != dataset.expected_dimensions:
        raise ValueError(
            f"Eurostat dataset {dataset.code} structure dimensions changed: "
            f"expected {dataset.expected_dimensions}, got {actual_dimensions}"
        )

    observation_count_text = _required_annotation(
        annotations,
        "OBS_COUNT",
        dataset.code,
    )
    try:
        observation_count = int(observation_count_text)
    except ValueError as exc:
        raise ValueError(
            f"Eurostat dataset {dataset.code} has invalid OBS_COUNT "
            f"{observation_count_text!r}"
        ) from exc
    if observation_count <= 0:
        raise ValueError(f"Eurostat dataset {dataset.code} has no source observations")

    return EurostatStructureMetadata(
        dataset_code=dataset.code,
        title=_english_name(dataflow),
        dsd_version=str(data_structure.attrib.get("version", "")),
        source_observation_count=observation_count,
        source_oldest_period=_required_annotation(
            annotations,
            "OBS_PERIOD_OVERALL_OLDEST",
            dataset.code,
        ),
        source_latest_period=_required_annotation(
            annotations,
            "OBS_PERIOD_OVERALL_LATEST",
            dataset.code,
        ),
        data_updated_at=_parse_timestamp(
            _required_annotation(annotations, "UPDATE_DATA", dataset.code),
            dataset_code=dataset.code,
            field="UPDATE_DATA",
        ),
        structure_updated_at=_parse_timestamp(
            _required_annotation(annotations, "UPDATE_STRUCTURE", dataset.code),
            dataset_code=dataset.code,
            field="UPDATE_STRUCTURE",
        ),
        dimensions=tuple(dimensions),
    )


def _download_validated_file(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: Any,
    validator: Callable[[Path], object],
) -> tuple[int, str, str]:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
        try:
            result = _stream_download(
                source_url=source_url,
                target_path=target_path,
                timeout_seconds=timeout_seconds,
                session=session,
            )
            validator(target_path)
            return result
        except (
            dlt_requests.RequestException,
            ElementTree.ParseError,
            OSError,
            ValueError,
        ) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < DEFAULT_DOWNLOAD_ATTEMPTS:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise RuntimeError(
        f"Eurostat download failed after {DEFAULT_DOWNLOAD_ATTEMPTS} attempts: "
        f"{source_url}"
    ) from last_error


def _stream_download(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: Any,
) -> tuple[int, str, str]:
    response = session.get(source_url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    digest = hashlib.sha256()
    size_bytes = 0
    with target_path.open("wb") as file_handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            file_handle.write(chunk)

    expected_length = response.headers.get("Content-Length")
    content_encoding = response.headers.get("Content-Encoding", "").casefold()
    if (
        expected_length is not None
        and expected_length.isdigit()
        and content_encoding in {"", "identity"}
        and size_bytes != int(expected_length)
    ):
        raise dlt_requests.ChunkedEncodingError(
            f"incomplete Eurostat download: {size_bytes}/{expected_length} bytes "
            f"from {source_url}"
        )
    if size_bytes == 0:
        raise ValueError(f"Eurostat returned an empty response from {source_url}")
    return size_bytes, digest.hexdigest(), response.headers.get("Content-Type", "")


def _element_by_id(
    elements: list[ElementTree.Element],
    expected_id: str,
    *,
    element_name: str,
) -> ElementTree.Element:
    matches = [
        element
        for element in elements
        if str(element.attrib.get("id", "")).casefold() == expected_id.casefold()
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Eurostat structure contains {len(matches)} {element_name} entries "
            f"for {expected_id}; expected exactly one"
        )
    return matches[0]


def _english_name(element: ElementTree.Element) -> str:
    names = element.findall("c:Name", _NAMESPACES)
    for name in names:
        if name.attrib.get(f"{{{_XML_NAMESPACE}}}lang") == "en":
            return (name.text or "").strip()
    if names:
        return (names[0].text or "").strip()
    return str(element.attrib.get("id", ""))


def _required_annotation(
    annotations: dict[str, str],
    annotation_type: str,
    dataset_code: str,
) -> str:
    value = annotations.get(annotation_type, "").strip()
    if value == "":
        raise ValueError(
            f"Eurostat dataset {dataset_code} has no {annotation_type} annotation"
        )
    return value


def _parse_timestamp(
    value: str,
    *,
    dataset_code: str,
    field: str,
) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"Eurostat dataset {dataset_code} has invalid {field} timestamp {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError(
            f"Eurostat dataset {dataset_code} {field} timestamp has no timezone"
        )
    return parsed.astimezone(UTC)
