from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any, Protocol

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers.requests import Client as DltRequestsClient

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
NACE_DUCKDB_PIPELINE_NAME = "nace_raw"
NACE_DUCKDB_DATASET_NAME = "nace_stage"
NACE_RAW_DLT_TABLE = "raw_sparql_payloads"
NACE_TIMEOUT_SECONDS = 120
NACE_REQUEST_MAX_ATTEMPTS = 5
NACE_RETRY_INITIAL_DELAY_SECONDS = 10.0
NACE_RETRY_MAX_DELAY_SECONDS = 120.0


@dataclass(frozen=True)
class NaceScheme:
    classification_version: str
    scheme_uri: str
    valid_from: str
    valid_to: str | None
    is_current: int


NACE_SCHEMES = (
    NaceScheme(
        "NACE_REV_2",
        "http://data.europa.eu/ux2/nace2/nace2",
        "2008-01-01",
        "2024-12-31",
        0,
    ),
    NaceScheme(
        "NACE_REV_2_1",
        "http://data.europa.eu/ux2/nace2.1/nace2.1",
        "2025-01-01",
        None,
        1,
    ),
)


class HttpResponse(Protocol):
    text: str

    def raise_for_status(self) -> None:
        ...


class HttpClient(Protocol):
    headers: dict[str, str]

    def get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        timeout: int = NACE_TIMEOUT_SECONDS,
    ) -> HttpResponse:
        ...


def nace_sparql_query(scheme_uri: str) -> str:
    return f"""prefix skos: <http://www.w3.org/2004/02/skos/core#>
select ?concept ?notation ?label ?broader where {{
  ?concept skos:inScheme <{scheme_uri}> ;
           skos:notation ?notation ;
           skos:prefLabel ?label .
  filter(lang(?label) = "en")
  optional {{ ?concept skos:broader ?broader . }}
}}
order by ?notation"""


def parse_sparql_csv(body: str) -> list[dict[str, str]]:
    return [dict(row) for row in csv.DictReader(StringIO(body))]


def _nace_http_client(
    *,
    timeout_seconds: int,
    user_agent: str,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        request_max_attempts=max_retries,
        request_backoff_factor=retry_initial_delay_seconds,
        request_max_retry_delay=retry_max_delay_seconds,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def fetch_nace_scheme_rows(
    *,
    scheme: NaceScheme,
    endpoint_url: str = SPARQL_ENDPOINT,
    timeout_seconds: int = NACE_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    max_retries: int = NACE_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = NACE_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = NACE_RETRY_MAX_DELAY_SECONDS,
    session: HttpClient | None = None,
) -> tuple[list[dict[str, str]], str, str]:
    payload = fetch_nace_scheme_payload(
        scheme=scheme,
        endpoint_url=endpoint_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        session=session,
    )
    return (
        parse_sparql_csv(str(payload["response_csv"])),
        str(payload["source_url"]),
        str(payload["source_payload_hash"]),
    )


def fetch_nace_scheme_payload(
    *,
    scheme: NaceScheme,
    endpoint_url: str = SPARQL_ENDPOINT,
    timeout_seconds: int = NACE_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    max_retries: int = NACE_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = NACE_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = NACE_RETRY_MAX_DELAY_SECONDS,
    session: HttpClient | None = None,
) -> dict[str, str | int | None]:
    http_client = session or _nace_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
    )
    headers = getattr(http_client, "headers", None)
    if isinstance(headers, dict):
        headers["User-Agent"] = user_agent

    response = http_client.get(
        endpoint_url,
        params={"query": nace_sparql_query(scheme.scheme_uri), "format": "text/csv"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    body = response.text
    return {
        "classification_version": scheme.classification_version,
        "source_scheme_uri": scheme.scheme_uri,
        "source_url": endpoint_url,
        "request_query": nace_sparql_query(scheme.scheme_uri),
        "response_csv": body,
        "source_payload_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "valid_from": scheme.valid_from,
        "valid_to": scheme.valid_to,
        "is_current": scheme.is_current,
    }


@dlt.source(name="nace_raw")
def nace_raw_source(
    *,
    schemes: tuple[NaceScheme, ...] = NACE_SCHEMES,
    endpoint_url: str = SPARQL_ENDPOINT,
    timeout_seconds: int = NACE_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    max_retries: int = NACE_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = NACE_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = NACE_RETRY_MAX_DELAY_SECONDS,
    source_run_id: str = "",
    pulled_at: str | None = None,
    session: HttpClient | None = None,
) -> DltResource:
    return _nace_raw_payloads_resource(
        schemes=schemes,
        endpoint_url=endpoint_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        source_run_id=source_run_id,
        pulled_at=pulled_at,
        session=session,
    )


@dlt.resource(
    name=NACE_RAW_DLT_TABLE,
    write_disposition="replace",
    primary_key=("classification_version", "source_payload_hash"),
)
def _nace_raw_payloads_resource(
    *,
    schemes: tuple[NaceScheme, ...],
    endpoint_url: str,
    timeout_seconds: int,
    user_agent: str,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
    source_run_id: str,
    pulled_at: str | None,
    session: HttpClient | None,
) -> Iterator[dict[str, Any]]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    for scheme in schemes:
        payload = fetch_nace_scheme_payload(
            scheme=scheme,
            endpoint_url=endpoint_url,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            max_retries=max_retries,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            session=session,
        )
        yield {
            **payload,
            "source_run_id": source_run_id,
            "pulled_at": effective_pulled_at,
        }


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_nace_code(code: str) -> str:
    stripped = code.strip()
    return stripped if stripped.isalpha() else "".join(char for char in stripped if char.isalnum())


def nace_level(code: str) -> str:
    normalized = normalize_nace_code(code)
    if normalized.isalpha():
        return "section"
    if len(normalized) == 2:
        return "division"
    if len(normalized) == 3:
        return "group"
    if len(normalized) == 4:
        return "class"
    raise ValueError(f"Unsupported NACE code level: {code}")


def concept_code(concept_uri: str) -> str | None:
    value = concept_uri.rstrip("/").rsplit("/", maxsplit=1)[-1]
    if not value:
        return None
    if value.isalpha() or len(value) == 2:
        return value
    if len(value) in {3, 4}:
        return f"{value[:2]}.{value[2:]}"
    return value


def _section_code_for(
    normalized_code: str,
    parent_by_normalized_code: dict[str, str | None],
    section_by_normalized_code: dict[str, str],
) -> str | None:
    if normalized_code in section_by_normalized_code:
        return section_by_normalized_code[normalized_code]

    parent_normalized_code = parent_by_normalized_code.get(normalized_code)
    while parent_normalized_code:
        if parent_normalized_code in section_by_normalized_code:
            return section_by_normalized_code[parent_normalized_code]
        parent_normalized_code = parent_by_normalized_code.get(parent_normalized_code)
    return None


def build_nace_rows(
    *,
    scheme: NaceScheme,
    source_rows: list[dict[str, str]],
    source_url: str,
    source_payload_hash: str,
    source_run_id: str,
    pulled_at: str,
) -> list[dict[str, Any]]:
    if not source_rows:
        raise ValueError(f"{scheme.classification_version} returned no NACE rows")

    parent_by_normalized_code: dict[str, str | None] = {}
    section_by_normalized_code: dict[str, str] = {}
    for source_row in source_rows:
        code = source_row["notation"].strip()
        normalized_code = normalize_nace_code(code)
        parent_concept_uri = source_row.get("broader", "").strip() or None
        parent_code = concept_code(parent_concept_uri) if parent_concept_uri else None
        parent_by_normalized_code[normalized_code] = (
            normalize_nace_code(parent_code) if parent_code else None
        )
        if nace_level(code) == "section":
            section_by_normalized_code[normalized_code] = code

    rows: list[dict[str, Any]] = []
    for source_row in source_rows:
        code = source_row["notation"].strip()
        normalized_code = normalize_nace_code(code)
        level = nace_level(code)
        parent_concept_uri = source_row.get("broader", "").strip() or None
        parent_code = concept_code(parent_concept_uri) if parent_concept_uri else None
        rows.append(
            {
                "classification_version": scheme.classification_version,
                "code": code,
                "normalized_code": normalized_code,
                "parent_code": parent_code,
                "level": level,
                "section_code": _section_code_for(
                    normalized_code,
                    parent_by_normalized_code,
                    section_by_normalized_code,
                ),
                "description_en": source_row["label"].strip(),
                "concept_uri": source_row["concept"].strip(),
                "parent_concept_uri": parent_concept_uri,
                "source_scheme_uri": scheme.scheme_uri,
                "source_url": source_url,
                "source_payload_hash": source_payload_hash,
                "valid_from": scheme.valid_from,
                "valid_to": scheme.valid_to,
                "is_current": scheme.is_current,
                "source_run_id": source_run_id,
                "pulled_at": pulled_at,
            }
        )
    return rows
