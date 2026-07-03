# Czech OCR Benchmark

Benchmarks OCR engines on Czech Justice scanned financial PDFs.

The benchmark is intentionally separate from Dagster. It reads PDFs already
downloaded by `dagster_v3/scripts/czech_justice_pdf_probe.py`, runs OCR engines,
and writes JSON reports with runtime and rough text-quality signals.

## System Dependencies

For OCRmyPDF/Tesseract:

```bash
brew install qpdf tesseract-lang
```

The local machine already has `tesseract` and Ghostscript. `tesseract-lang` adds
the Czech `ces` language pack.

## Python Environment

PaddleOCR currently has better compatibility on Python 3.12 than Python 3.14, so
this package pins Python to `>=3.12,<3.13`.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/czech_ocr_benchmark
uv sync --python 3.12 --extra cpu
```

To include the GLM-OCR SDK client:

```bash
uv sync --python 3.12 --extra cpu --extra glm
```

On a GPU host, install the CUDA PaddlePaddle build recommended for that host
instead of the `cpu` extra. Keep `paddleocr` from this package, but do not install
the CPU `paddlepaddle` wheel into the GPU environment.

## SparkDGX / GPU Host

Copy this package and the probe PDFs to the GPU host. On the host:

```bash
cd czech_ocr_benchmark
uv sync --python 3.12
```

For PaddleOCR, install the PaddlePaddle GPU wheel that matches the host CUDA
version. PaddleGPU is currently not a good SparkDGX/GB10 path unless a compatible
Paddle wheel exists. Check:

```bash
nvidia-smi
```

After the GPU Paddle runtime is installed, verify:

```bash
uv run python - <<'PY'
import paddle
print("paddle", paddle.__version__)
print("cuda", paddle.device.is_compiled_with_cuda())
PY
```

Run the benchmark:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir data/czech_justice_pdf_probe \
  --output-dir data/results-sparkdgx \
  --engines ocrmypdf,paddle \
  --max-pages 5
```

For a full-document Paddle run:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir data/czech_justice_pdf_probe \
  --output-dir data/results-sparkdgx-full \
  --engines paddle \
  --max-pages 0
```

For GLM-OCR on SparkDGX/GB10, prefer Docker vLLM and the direct `glm-vllm`
engine. Start the model server:

```bash
docker run -d --name glm-ocr-vllm --gpus all --ipc=host -p 8000:8000 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  -e HF_HOME=/root/.cache/huggingface \
  vllm/vllm-openai:cu130-nightly \
  zai-org/GLM-OCR \
  --host 0.0.0.0 \
  --port 8000 \
  --served-model-name glm-ocr \
  --max-model-len 8192 \
  --gpu-memory-utilization 0.80 \
  --limit-mm-per-prompt '{"image":1}'
```

Verify the OpenAI-compatible endpoint:

```bash
curl http://localhost:8000/v1/models
```

Run a targeted benchmark against vLLM:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir data/czech_justice_pdf_probe \
  --output-dir data/results-glm-vllm \
  --engines glm-vllm \
  --page-start 29 \
  --max-pages 5 \
  --glm-vllm-api-url http://localhost:8000/v1/chat/completions \
  --glm-vllm-model glm-ocr \
  --glm-vllm-max-tokens 8192 \
  --glm-vllm-prompt "Table Recognition:"
```

The `glm` engine uses the GLM-OCR Python SDK. Install the light client with this
package:

```bash
uv sync --python 3.12 --extra glm
```

If the host will run the full GLM-OCR SDK pipeline locally, install its
self-hosted dependencies in the GPU environment after confirming the CUDA/PyTorch
stack works on that host:

```bash
uv pip install "glmocr[selfhosted]>=0.1.5"
```

Then run a targeted page-window benchmark. For the sample Asseco PDF, financial
tables start much later than the first five pages, so `--page-start` is important:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir data/czech_justice_pdf_probe \
  --output-dir data/results-sparkdgx-glm \
  --engines glm \
  --page-start 29 \
  --max-pages 5 \
  --glm-config config.yaml \
  --glm-layout-device cpu
```

If GLM-OCR is exposed through the SDK server or MaaS-compatible endpoint, pass
the endpoint directly to the SDK engine:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir data/czech_justice_pdf_probe \
  --output-dir data/results-glm-server \
  --engines glm \
  --page-start 29 \
  --max-pages 5 \
  --glm-api-url http://localhost:5002/glmocr/parse
```

## Run

Use the 8 PDFs produced by the probe:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir ../dagster_v3/data/czech_justice_pdf_probe \
  --output-dir data/results \
  --engines ocrmypdf,paddle \
  --max-pages 5
```

Run GLM-OCR on pages around the financial statements:

```bash
uv run czech-ocr-benchmark \
  --pdf-dir ../dagster_v3/data/czech_justice_pdf_probe \
  --output-dir data/results-glm \
  --engines glm \
  --page-start 29 \
  --max-pages 5
```

The SDK `glm` run needs one of these configured: `ZHIPU_API_KEY`,
`--glm-api-url`, or `--glm-config` pointing at a self-hosted setup. The direct
`glm-vllm` run only needs the OpenAI-compatible vLLM endpoint.

Use `--max-pages 0` to process all pages for engines that support page limiting.
OCRmyPDF always processes the full PDF. `--page-start` is one-based and applies
to image-based engines (`paddle`, `glm`).

The benchmark writes:

- `data/results/report.json` - aggregate report
- `data/results/ocrmypdf/*.txt` - OCRmyPDF sidecar text
- `data/results/paddle/*.txt` - PaddleOCR text
- `data/results/glm/*.txt` - GLM-OCR SDK extracted text or markdown
- `data/results/glm/*.json` - GLM-OCR SDK JSON sidecar when exposed by the SDK
- `data/results/glm-vllm/*.txt` - direct vLLM extracted page text
- `data/results/glm-vllm/*.json` - raw OpenAI-compatible vLLM responses

## Quality Signals

The report counts occurrences of Czech financial labels such as:

- `aktiva`
- `vlastní kapitál`
- `tržby`
- `výnosy`
- `výsledek hospodaření`
- `závazky`

This is not final metric extraction. It is only enough to compare OCR engine
speed and whether the extracted text is useful for later JSON extraction.
