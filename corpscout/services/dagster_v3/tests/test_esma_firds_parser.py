from io import BytesIO
from pathlib import Path

from dagster_v3.defs.esma_firds.parser import FirdsFileContext, iter_firds_records

FIXTURES = Path(__file__).parent / "fixtures" / "esma_firds"


def _context(file_type: str, file_name: str) -> FirdsFileContext:
    return FirdsFileContext(
        source_file_id=f"id-{file_type}",
        source_file_name=file_name,
        source_file_type=file_type,
        source_file_checksum="a" * 32,
        source_publication_date="2026-07-20",
        source_download_url=f"https://example.test/{file_name}.zip",
        source_object_key=f"raw/{file_name}.zip",
        source_run_id="run-1",
        source_retrieved_at="2026-07-20T10:00:00+00:00",
    )


def test_full_parser_is_namespace_agnostic_and_preserves_multiple_countries() -> None:
    records = list(
        iter_firds_records(
            BytesIO((FIXTURES / "fulins_sample.xml").read_bytes()),
            context=_context("FULINS", "FULINS_E_20260718_01of01.zip"),
        )
    )

    assert len(records) == 4
    sweden = records[0]
    assert sweden.event_type == "BASELINE"
    assert sweden.isin == "SE0000000001"
    assert sweden.mic == "XSTO"
    assert sweden.issuer_lei == "5493001KJTIIGC8Y1R12"
    assert sweden.cfi_code == "ESVUFR"
    assert sweden.competent_authority_country == "SE"
    assert sweden.valid_from == "2024-01-02"
    assert records[1].competent_authority_country == "FR"
    assert (records[2].isin, records[2].mic, records[2].relevant_venue_mic) == (
        "SE0000000001",
        "XNGM",
        "XSTO",
    )
    assert records[3].competent_authority_country == "DE"
    assert records[3].cfi_code.startswith("D")


def test_delta_parser_preserves_all_event_types_and_full_record_fields() -> None:
    records = list(
        iter_firds_records(
            BytesIO((FIXTURES / "dltins_sample.xml").read_bytes()),
            context=_context("DLTINS", "DLTINS_20260720_01of01.zip"),
        )
    )

    assert [record.event_type for record in records] == [
        "MODIFIED",
        "NEW",
        "TERMINATED",
        "CANCELLED",
    ]
    assert records[0].full_name == "Sweden Example AB class A"
    assert records[1].mic == "FNSE"
    assert records[2].termination_at == "2026-07-19T23:59:59Z"
    assert records[3].mic == "XSAT"
    assert len({record.source_record_id for record in records}) == 4


def test_consolidated_cancellation_parser_keeps_exact_isin_mic_key() -> None:
    records = list(
        iter_firds_records(
            BytesIO((FIXTURES / "fulcan_sample.xml").read_bytes()),
            context=_context("FULCAN", "FULCAN_20260720_01of01.zip"),
        )
    )

    assert len(records) == 1
    assert records[0].event_type == "CONSOLIDATED_CANCELLED"
    assert records[0].isin == "SE0000000003"
    assert records[0].mic == "XSAT"
