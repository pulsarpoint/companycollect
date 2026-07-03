from pathlib import Path
import urllib.error

import pytest

from czech_ocr_benchmark import benchmark
from czech_ocr_benchmark.benchmark import discover_pdf_files, metadata_from_pdf_path, score_ocr_text


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


def test_selected_page_indices_supports_one_based_start_page() -> None:
    assert benchmark.selected_page_indices(page_count=10, page_start=1, max_pages=3) == [0, 1, 2]
    assert benchmark.selected_page_indices(page_count=10, page_start=8, max_pages=5) == [7, 8, 9]
    assert benchmark.selected_page_indices(page_count=10, page_start=8, max_pages=0) == [7, 8, 9]


def test_run_benchmark_dispatches_glm_engine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "ico_prefix=27" / "ico=27074358" / "year=2024" / "document=1.pdf"
    pdf_path.parent.mkdir(parents=True)
    pdf_path.write_bytes(b"%PDF")
    output_dir = tmp_path / "results"
    calls = []

    def fake_discover_pdf_files(pdf_dir: Path) -> list[Path]:
        assert pdf_dir == tmp_path
        return [pdf_path]

    def fake_run_glmocr(
        *,
        pdf_path: Path,
        output_dir: Path,
        max_pages: int,
        page_start: int,
        layout_device: str,
        config_path: Path | None,
        api_url: str | None,
    ) -> benchmark.OcrRunResult:
        calls.append((pdf_path, output_dir, max_pages, page_start, layout_device, config_path, api_url))
        text_path = output_dir / "glm" / "result.txt"
        text_path.parent.mkdir(parents=True)
        text_path.write_text("Aktiva celkem 100", encoding="utf-8")
        return benchmark.OcrRunResult(
            engine="glm",
            pdf_path=pdf_path,
            text_path=text_path,
            status="success",
            runtime_seconds=1.2,
            pages_processed=2,
            error_message="",
            score=benchmark.score_ocr_text(text_path.read_text(encoding="utf-8")),
        )

    monkeypatch.setattr(benchmark, "discover_pdf_files", fake_discover_pdf_files)
    monkeypatch.setattr(benchmark, "run_glmocr", fake_run_glmocr, raising=False)

    report = benchmark.run_benchmark(
        pdf_dir=tmp_path,
        output_dir=output_dir,
        engines=["glm"],
        max_pages=2,
        page_start=29,
    )

    assert calls == [(pdf_path, output_dir, 2, 29, "cpu", None, None)]
    assert report["results"][0]["engine"] == "glm"
    assert report["results"][0]["pages_processed"] == 2
    assert report["summary"]["glm"]["success"] == 1


def test_run_benchmark_passes_glm_connection_options(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "ico_prefix=27" / "ico=27074358" / "year=2024" / "document=1.pdf"
    pdf_path.parent.mkdir(parents=True)
    pdf_path.write_bytes(b"%PDF")
    output_dir = tmp_path / "results"
    config_path = tmp_path / "glm.yaml"
    calls = []

    def fake_run_glmocr(
        *,
        pdf_path: Path,
        output_dir: Path,
        max_pages: int,
        page_start: int,
        layout_device: str,
        config_path: Path | None,
        api_url: str | None,
    ) -> benchmark.OcrRunResult:
        calls.append((config_path, api_url, layout_device))
        return benchmark.OcrRunResult(
            engine="glm",
            pdf_path=pdf_path,
            text_path=output_dir / "glm" / "result.txt",
            status="success",
            runtime_seconds=0,
            pages_processed=1,
            error_message="",
            score=benchmark.score_ocr_text(""),
        )

    monkeypatch.setattr(benchmark, "discover_pdf_files", lambda _: [pdf_path])
    monkeypatch.setattr(benchmark, "run_glmocr", fake_run_glmocr, raising=False)

    benchmark.run_benchmark(
        pdf_dir=tmp_path,
        output_dir=output_dir,
        engines=["glm"],
        max_pages=1,
        glm_layout_device="cuda:0",
        glm_config_path=config_path,
        glm_api_url="http://localhost:5002/glmocr/parse",
    )

    assert calls == [(config_path, "http://localhost:5002/glmocr/parse", "cuda:0")]


def test_run_benchmark_dispatches_direct_glm_vllm_engine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "ico_prefix=27" / "ico=27074358" / "year=2024" / "document=1.pdf"
    pdf_path.parent.mkdir(parents=True)
    pdf_path.write_bytes(b"%PDF")
    output_dir = tmp_path / "results"
    calls = []

    def fake_run_glm_vllm(
        *,
        pdf_path: Path,
        output_dir: Path,
        max_pages: int,
        page_start: int,
        api_url: str,
        model: str,
        max_tokens: int,
        prompt: str,
    ) -> benchmark.OcrRunResult:
        calls.append((api_url, model, max_tokens, prompt, max_pages, page_start))
        return benchmark.OcrRunResult(
            engine="glm-vllm",
            pdf_path=pdf_path,
            text_path=output_dir / "glm-vllm" / "result.txt",
            status="success",
            runtime_seconds=0,
            pages_processed=1,
            error_message="",
            score=benchmark.score_ocr_text("Aktiva celkem 100"),
        )

    monkeypatch.setattr(benchmark, "discover_pdf_files", lambda _: [pdf_path])
    monkeypatch.setattr(benchmark, "run_glm_vllm", fake_run_glm_vllm, raising=False)

    report = benchmark.run_benchmark(
        pdf_dir=tmp_path,
        output_dir=output_dir,
        engines=["glm-vllm"],
        max_pages=1,
        page_start=29,
        glm_vllm_api_url="http://localhost:8000/v1/chat/completions",
        glm_vllm_model="glm-ocr",
        glm_vllm_max_tokens=4096,
        glm_vllm_prompt="Table Recognition:",
    )

    assert calls == [
        (
            "http://localhost:8000/v1/chat/completions",
            "glm-ocr",
            4096,
            "Table Recognition:",
            1,
            29,
        )
    ]
    assert report["results"][0]["engine"] == "glm-vllm"
    assert report["summary"]["glm-vllm"]["success"] == 1


def test_glm_vllm_payload_uses_openai_compatible_image_request(tmp_path: Path) -> None:
    image_path = tmp_path / "page.png"
    image_path.write_bytes(b"fake-png")

    payload = benchmark._glm_vllm_payload(
        image_path=image_path,
        model="glm-ocr",
        max_tokens=4096,
        prompt="Table Recognition:",
    )

    assert payload["model"] == "glm-ocr"
    assert payload["max_tokens"] == 4096
    assert payload["messages"][0]["content"][0] == {
        "type": "text",
        "text": "Table Recognition:",
    }
    image_content = payload["messages"][0]["content"][1]
    assert image_content["type"] == "image_url"
    assert image_content["image_url"]["url"].startswith("data:image/png;base64,")


def test_glm_vllm_response_text_extracts_openai_message_content() -> None:
    text = benchmark._glm_vllm_response_text(
        {
            "choices": [
                {
                    "message": {
                        "content": "Aktiva celkem 100\nVlastní kapitál 25",
                    }
                }
            ]
        }
    )

    assert text == "Aktiva celkem 100\nVlastní kapitál 25"


def test_post_glm_vllm_payload_includes_http_error_body(monkeypatch) -> None:
    class ErrorBody:
        def read(self) -> bytes:
            return b'{"error":"prompt plus max tokens exceeds context"}'

        def close(self) -> None:
            pass

    def fake_urlopen(*args, **kwargs):
        raise urllib.error.HTTPError(
            url="http://localhost:8000/v1/chat/completions",
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=ErrorBody(),
        )

    monkeypatch.setattr(benchmark.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="prompt plus max tokens exceeds context"):
        benchmark._post_glm_vllm_payload(
            api_url="http://localhost:8000/v1/chat/completions",
            payload={"model": "glm-ocr"},
        )


def test_glmocr_result_text_extracts_json_result_content() -> None:
    class FakeGlmResult:
        json_result = {
            "pages": [
                {"markdown": "Aktiva celkem 100"},
                {"markdown": "Vlastní kapitál 25"},
            ]
        }

    text = benchmark._glmocr_result_text(FakeGlmResult())

    assert "Aktiva celkem 100" in text
    assert "Vlastní kapitál 25" in text
