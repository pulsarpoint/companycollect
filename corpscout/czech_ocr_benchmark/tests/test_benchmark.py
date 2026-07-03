from pathlib import Path

from czech_ocr_benchmark.benchmark import (
    discover_pdf_files,
    metadata_from_pdf_path,
    score_ocr_text,
)


def test_metadata_from_probe_pdf_path() -> None:
    path = Path(
        "ico_prefix=27/ico=27074358/year=2024/document=85645222.pdf"
    )

    metadata = metadata_from_pdf_path(path)

    assert metadata == {
        "ico": "27074358",
        "ico_prefix": "27",
        "year": "2024",
        "document_id": "85645222",
    }


def test_score_ocr_text_counts_financial_terms_and_numbers() -> None:
    score = score_ocr_text(
        "Aktiva celkem 100 000\n"
        "Vlastní kapitál 25 000\n"
        "Výsledek hospodaření 5 000\n"
    )

    assert score["char_count"] > 0
    assert score["numeric_token_count"] == 3
    assert score["financial_term_hits"]["aktiva"] == 1
    assert score["financial_term_hits"]["vlastní kapitál"] == 1
    assert score["financial_term_hits"]["výsledek hospodaření"] == 1


def test_discover_pdf_files_finds_nested_pdfs(tmp_path: Path) -> None:
    pdf = tmp_path / "ico_prefix=27" / "ico=27074358" / "year=2024" / "document=1.pdf"
    pdf.parent.mkdir(parents=True)
    pdf.write_bytes(b"%PDF")

    assert discover_pdf_files(tmp_path) == [pdf]
