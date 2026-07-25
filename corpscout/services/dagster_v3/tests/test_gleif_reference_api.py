import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.gleif import reference_api


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(
        self,
        responses: dict[str | tuple[str, int], dict[str, Any]],
    ) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, int] | None, int]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, int] | None = None,
        timeout: int,
    ) -> _FakeResponse:
        self.calls.append((url, params, timeout))
        page_number = (params or {}).get("page[number]", 1)
        payload = self.responses.get((url, page_number), self.responses.get(url))
        if payload is None:
            raise AssertionError(f"Unexpected request: {url}, page {page_number}")
        return _FakeResponse(payload)


def test_reference_api_source_declares_explicit_raw_table_schemas() -> None:
    source = reference_api.gleif_reference_api_source(
        source_run_id="test-run",
        retrieved_at="2026-07-25T10:00:00Z",
        session=_FakeSession({}),
    )

    issuer_schema = source.resources[
        reference_api.GLEIF_RAW_LEI_ISSUERS_API_TABLE
    ].compute_table_schema()
    code_list_schema = source.resources[
        reference_api.GLEIF_RAW_CODE_LIST_ENTRIES_API_TABLE
    ].compute_table_schema()

    assert set(issuer_schema["columns"]) == set(reference_api.GLEIF_RAW_LEI_ISSUERS_COLUMNS)
    assert set(code_list_schema["columns"]) == set(
        reference_api.GLEIF_RAW_CODE_LIST_ENTRIES_COLUMNS
    )
    assert issuer_schema["columns"]["lei"]["nullable"] is False
    assert code_list_schema["columns"]["code"]["nullable"] is False


def test_lei_issuer_rows_include_accredited_jurisdictions() -> None:
    issuer_url = f"{reference_api.GLEIF_API_BASE_URL}/lei-issuers"
    jurisdiction_url = f"{issuer_url}/029200067A7K6CH0H586/jurisdictions"
    fund_jurisdiction_url = f"{issuer_url}/029200067A7K6CH0H586/fundJurisdictions"
    session = _FakeSession(
        {
            issuer_url: _collection(
                [
                    {
                        "id": "029200067A7K6CH0H586",
                        "attributes": {
                            "lei": "029200067A7K6CH0H586",
                            "name": "Central Securities Clearing System PLC",
                            "marketingName": "CSCS Nigeria",
                            "website": "https://lei.cscs.ng/",
                            "accreditationDate": "2018-01-30T00:00:00+00:00",
                        },
                    }
                ]
            ),
            jurisdiction_url: _collection(
                [
                    {"attributes": {"countryCode": "NG"}},
                    {"attributes": {"countryCode": "GH"}},
                ]
            ),
            fund_jurisdiction_url: _collection(
                [{"attributes": {"countryCode": "NG"}}]
            ),
        }
    )

    rows = list(
        reference_api.iter_lei_issuer_rows(
            source_run_id="run-1",
            retrieved_at="2026-07-25T10:00:00Z",
            timeout_seconds=30,
            session=session,
        )
    )

    assert len(rows) == 1
    assert rows[0]["lei"] == "029200067A7K6CH0H586"
    assert json.loads(rows[0]["jurisdictions_json"]) == ["GH", "NG"]
    assert json.loads(rows[0]["fund_jurisdictions_json"]) == ["NG"]
    assert rows[0]["source_run_id"] == "run-1"


def test_pagination_does_not_follow_gleif_next_link_with_invalid_sort() -> None:
    source_url = (
        f"{reference_api.GLEIF_API_BASE_URL}/lei-issuers/"
        "213800WAVVOPS85N2205/jurisdictions"
    )
    session = _FakeSession(
        {
            (source_url, 1): _collection(
                [{"attributes": {"countryCode": "AD"}}],
                current_page=1,
                last_page=2,
                next_link=(
                    f"{source_url}?sort=countryCode&page[number]=2"
                    f"&page[size]={reference_api.GLEIF_API_PAGE_SIZE}"
                ),
            ),
            (source_url, 2): _collection(
                [{"attributes": {"countryCode": "AE"}}],
                current_page=2,
                last_page=2,
            ),
        }
    )

    rows = list(
        reference_api._iter_collection_items(
            source_url,
            timeout_seconds=30,
            session=session,
        )
    )

    assert [row["attributes"]["countryCode"] for row in rows] == ["AD", "AE"]
    assert [call[0] for call in session.calls] == [source_url, source_url]
    assert session.calls[1][1] == {
        "page[number]": 2,
        "page[size]": reference_api.GLEIF_API_PAGE_SIZE,
    }


def test_code_list_rows_cover_all_official_gleif_collections() -> None:
    session = _reference_api_session()

    rows = list(
        reference_api.iter_code_list_entry_rows(
            source_run_id="run-1",
            retrieved_at="2026-07-25T10:00:00Z",
            timeout_seconds=30,
            session=session,
        )
    )
    rows_by_key = {(row["code_list"], row["code"]): row for row in rows}

    assert {row["code_list"] for row in rows} == {
        "ACCEPTED_LEGAL_JURISDICTION",
        "ENTITY_LEGAL_FORM",
        "OFFICIAL_ORGANIZATIONAL_ROLE",
        "REGISTRATION_AUTHORITY",
    }
    assert rows_by_key[("REGISTRATION_AUTHORITY", "RA000001")][
        "country_iso2"
    ] == "AF"
    assert rows_by_key[("ENTITY_LEGAL_FORM", "09K3")][
        "label"
    ] == "Samstarvsfelag vid abyrgd"
    assert rows_by_key[("ENTITY_LEGAL_FORM", "09K3")]["valid_from"] == "2026-02-19"
    assert rows_by_key[("OFFICIAL_ORGANIZATIONAL_ROLE", "01D0O4")]["code"] == "01D0O4"
    assert rows_by_key[("ACCEPTED_LEGAL_JURISDICTION", "US-CA")][
        "label"
    ] == "California"
    assert rows_by_key[("ACCEPTED_LEGAL_JURISDICTION", "AD-02")]["label"] == "AD-02"


def test_reference_api_rejects_an_empty_official_code_list() -> None:
    session = _reference_api_session()
    session.responses[
        f"{reference_api.GLEIF_API_BASE_URL}/entity-legal-forms"
    ] = _collection([])

    with pytest.raises(ValueError, match="entity-legal-forms returned no rows"):
        list(
            reference_api.iter_code_list_entry_rows(
                source_run_id="run-1",
                retrieved_at="2026-07-25T10:00:00Z",
                timeout_seconds=30,
                session=session,
            )
        )


def test_reference_api_dlt_pipeline_loads_both_raw_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"

    reference_api.load_gleif_reference_api_raw_tables(
        database_path=database_path,
        source_run_id="run-1",
        retrieved_at="2026-07-25T10:00:00Z",
        timeout_seconds=30,
        session=_reference_api_session(include_issuer=True),
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            f"""
            select count(*)
            from {reference_api.GLEIF_REFERENCE_API_DLT_DATASET_NAME}.
                 {reference_api.GLEIF_RAW_LEI_ISSUERS_API_TABLE}
            """
        ).fetchone() == (1,)
        assert connection.execute(
            f"""
            select count(*)
            from {reference_api.GLEIF_REFERENCE_API_DLT_DATASET_NAME}.
                 {reference_api.GLEIF_RAW_CODE_LIST_ENTRIES_API_TABLE}
            """
        ).fetchone() == (5,)


def _reference_api_session(*, include_issuer: bool = False) -> _FakeSession:
    base_url = reference_api.GLEIF_API_BASE_URL
    responses = {
        f"{base_url}/registration-authorities": _collection(
            [
                {
                    "attributes": {
                        "code": "RA000001",
                        "internationalName": "Afghanistan Central Business Registry",
                        "localName": None,
                        "internationalOrganizationName": "Ministry of Commerce",
                        "localOrganizationName": None,
                        "website": "https://example.test/ra",
                        "jurisdictions": [{"countryCode": "AF"}],
                    }
                }
            ]
        ),
        f"{base_url}/entity-legal-forms": _collection(
            [
                {
                    "attributes": {
                        "code": "09K3",
                        "countryCode": "FO",
                        "dateCreated": "2026-02-19",
                        "status": "ACTV",
                        "names": [
                            {
                                "localName": "Samstarvsfelag við ábyrgd",
                                "transliteratedName": "Samstarvsfelag vid abyrgd",
                            }
                        ],
                    }
                }
            ]
        ),
        f"{base_url}/official-organizational-roles": _collection(
            [
                {
                    "attributes": {
                        "code": "01D0O4",
                        "countryCode": "QA",
                        "dateCreated": "2023-06-28",
                        "status": "ACTV",
                        "names": [
                            {
                                "name": "موظف",
                                "transliteratedName": "muwadaf",
                            }
                        ],
                    }
                }
            ]
        ),
        f"{base_url}/jurisdictions": _collection(
            [
                {"attributes": {"code": "US-CA", "name": "California"}},
                {"attributes": {"code": "AD-02", "name": None}},
            ]
        ),
    }
    if include_issuer:
        issuer_url = f"{base_url}/lei-issuers"
        responses[issuer_url] = _collection(
            [
                {
                    "attributes": {
                        "lei": "029200067A7K6CH0H586",
                        "name": "CSCS PLC",
                        "marketingName": "CSCS Nigeria",
                        "website": "https://lei.cscs.ng/",
                        "accreditationDate": "2018-01-30T00:00:00+00:00",
                    }
                }
            ]
        )
        responses[f"{issuer_url}/029200067A7K6CH0H586/jurisdictions"] = _collection(
            [{"attributes": {"countryCode": "NG"}}]
        )
        responses[f"{issuer_url}/029200067A7K6CH0H586/fundJurisdictions"] = _collection(
            [{"attributes": {"countryCode": "NG"}}]
        )
    return _FakeSession(responses)


def _collection(
    rows: list[dict[str, Any]],
    *,
    current_page: int = 1,
    last_page: int = 1,
    next_link: str | None = None,
) -> dict[str, Any]:
    return {
        "data": rows,
        "links": {
            "first": "https://example.test/first",
            "last": "https://example.test/last",
            "next": next_link,
        },
        "meta": {
            "pagination": {
                "currentPage": current_page,
                "lastPage": last_page,
                "total": len(rows),
            }
        },
    }
