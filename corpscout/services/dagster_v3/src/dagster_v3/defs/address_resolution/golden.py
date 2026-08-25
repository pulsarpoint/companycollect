import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.address_resolution.model import (
    AddressResolutionPolicy,
    GoldenAddressDocument,
    GoldenAddressResolutionCase,
    GoldenAddressResolutionEvaluation,
    GoldenAddressResolutionFailure,
)
from dagster_v3.defs.address_resolution.resolution import (
    replace_address_resolution_candidates,
    replace_address_resolution_results,
)
from dagster_v3.defs.address_resolution.search_documents import (
    SEARCH_DOCUMENT_INPUT_COLUMNS,
    replace_address_search_document_input_table,
    replace_address_search_documents,
    replace_address_street_variants,
)


def load_golden_address_resolution_corpus(
    corpus_path: Path,
) -> tuple[str, tuple[GoldenAddressResolutionCase, ...]]:
    payload = json.loads(corpus_path.read_text(encoding="utf-8"))
    corpus_version = str(payload["version"])
    cases = tuple(_load_case(case) for case in payload["cases"])
    if not cases:
        raise ValueError("Address-resolution golden corpus must contain cases")
    return corpus_version, cases


def evaluate_golden_address_resolution_corpus(
    *,
    corpus_path: Path,
    policy: AddressResolutionPolicy,
    street_variant_languages_by_country: Mapping[str, Sequence[str]],
    street_suffix_expansions_by_country: Mapping[str, Mapping[str, str]],
    exact_suffix_expansions_by_country: Mapping[str, Mapping[str, str]] | None = None,
    separate_definite_by_country: Mapping[str, Mapping[str, str]] | None = None,
) -> GoldenAddressResolutionEvaluation:
    corpus_version, cases = load_golden_address_resolution_corpus(corpus_path)
    with duckdb.connect(":memory:") as connection:
        _insert_corpus_documents(connection, cases)
        replace_address_search_documents(
            connection,
            source_sql="select * from _golden_query_input",
            table_name="_golden_query_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="_golden_query_documents",
            variant_table="_golden_query_street_variants",
            languages_by_country=street_variant_languages_by_country,
            suffix_expansions_by_country=street_suffix_expansions_by_country,
            exact_suffix_expansions_by_country=exact_suffix_expansions_by_country,
            separate_definite_by_country=separate_definite_by_country,
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from _golden_reference_input",
            table_name="_golden_reference_documents",
        )
        replace_address_resolution_candidates(
            connection,
            query_table="_golden_query_documents",
            query_street_variant_table="_golden_query_street_variants",
            reference_table="_golden_reference_documents",
            candidate_table="_golden_candidates",
            policy=policy,
        )
        replace_address_resolution_results(
            connection,
            query_table="_golden_query_documents",
            candidate_table="_golden_candidates",
            result_table="_golden_results",
            policy=policy,
        )
        result_rows = connection.execute(
            """
            select
                query_document_id,
                resolution_status,
                geocode_precision,
                match_strategy
            from _golden_results
            """
        ).fetchall()

    results = {
        str(query_document_id): (
            str(status),
            str(precision),
            str(strategy),
        )
        for query_document_id, status, precision, strategy in result_rows
    }
    failures = tuple(
        GoldenAddressResolutionFailure(
            case_id=case.case_id,
            expected_status=case.expected_status,
            actual_status=results[case.query.document_id][0],
            expected_precision=case.expected_precision,
            actual_precision=results[case.query.document_id][1],
            expected_strategy=case.expected_strategy,
            actual_strategy=results[case.query.document_id][2],
        )
        for case in cases
        if results[case.query.document_id]
        != (
            case.expected_status,
            case.expected_precision,
            case.expected_strategy,
        )
    )
    return GoldenAddressResolutionEvaluation(
        corpus_version=corpus_version,
        policy_version=policy.version,
        case_count=len(cases),
        passed_count=len(cases) - len(failures),
        failures=failures,
    )


def _insert_corpus_documents(
    connection: Any,
    cases: tuple[GoldenAddressResolutionCase, ...],
) -> None:
    replace_address_search_document_input_table(
        connection,
        table_name="_golden_query_input",
    )
    replace_address_search_document_input_table(
        connection,
        table_name="_golden_reference_input",
    )
    placeholders = ", ".join("?" for _ in SEARCH_DOCUMENT_INPUT_COLUMNS)
    connection.executemany(
        f"insert into _golden_query_input values ({placeholders})",
        [_document_row(case.case_id, case.query) for case in cases],
    )
    reference_rows = [
        _document_row(case.case_id, reference)
        for case in cases
        for reference in case.references
    ]
    if reference_rows:
        connection.executemany(
            f"insert into _golden_reference_input values ({placeholders})",
            reference_rows,
        )


def _document_row(
    index_scope: str,
    document: GoldenAddressDocument,
) -> tuple[object, ...]:
    return (
        index_scope,
        document.document_id,
        document.country_code,
        document.raw_address,
        document.search_text,
        document.street_name,
        document.house_number,
        document.unit,
        document.postal_code,
        document.locality,
        document.address_kind,
        document.reference_precision,
        document.latitude,
        document.longitude,
        document.coordinate_spread_meters,
        document.supporting_record_count,
        document.source_record_id,
        document.source_record_url,
    )


def _load_case(payload: dict[str, object]) -> GoldenAddressResolutionCase:
    expected = _mapping(payload["expected"])
    return GoldenAddressResolutionCase(
        case_id=str(payload["case_id"]),
        description=str(payload["description"]),
        query=_load_document(_mapping(payload["query"])),
        references=tuple(
            _load_document(_mapping(reference))
            for reference in _sequence(payload["references"])
        ),
        expected_status=str(expected["status"]),
        expected_precision=str(expected["precision"]),
        expected_strategy=str(expected["strategy"]),
    )


def _load_document(payload: dict[str, object]) -> GoldenAddressDocument:
    return GoldenAddressDocument(
        document_id=str(payload["document_id"]),
        country_code=str(payload["country_code"]),
        raw_address=str(payload["raw_address"]),
        search_text=str(payload["search_text"]),
        street_name=str(payload["street_name"]),
        house_number=str(payload["house_number"]),
        unit=str(payload["unit"]),
        postal_code=str(payload["postal_code"]),
        locality=str(payload["locality"]),
        address_kind=str(payload["address_kind"]),
        reference_precision=str(payload["reference_precision"]),
        latitude=_optional_float(payload["latitude"]),
        longitude=_optional_float(payload["longitude"]),
        coordinate_spread_meters=_optional_float(payload["coordinate_spread_meters"]),
        supporting_record_count=int(payload["supporting_record_count"]),
        source_record_id=str(payload["source_record_id"]),
        source_record_url=str(payload["source_record_url"]),
    )


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("Golden corpus object must be a mapping")
    return value


def _sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("Golden corpus references must be a list")
    return value


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)
