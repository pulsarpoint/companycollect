# Czech Justice PDF Metadata S3 Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a partitioned Dagster asset that discovers Czech Justice financial PDF metadata for Czech companies and stores the PDF download URLs on S3 without downloading PDFs.

**Architecture:** Keep the implementation source-specific and direct. The asset reads `corpscout.cz_companies` from ClickHouse for one two-digit IČO prefix, scrapes `or.justice.cz` pages to discover financial statement/annual-report PDF download URLs, and writes one JSONL metadata file plus a manifest and `_SUCCESS.json` marker per partition. No new generic service layer or interface is needed.

**Tech Stack:** Dagster assets, Dagster static partitions, ClickHouse resource, existing `ObjectStoreResource`, `requests`, `lxml.html`, S3/RustFS JSONL objects.

---

## File Structure

- Create: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
  - Constants for Czech Justice URLs, S3 bucket/key layout, partitions.
  - Small parsing functions for `subjektId`, financial document rows, year, PDF download URL.
  - HTTP GET with finite retry/backoff.
  - ClickHouse company query by two-digit IČO prefix.
  - Partition materializer and the Dagster asset.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/assets.py`
  - Import/register the new asset.
  - Add `ObjectStoreResource` to this defs module resources.
  - Add manual job `czech_justice_pdf_metadata_job`.
- Modify: `corpscout/dagster_v3/tests/test_czech_ares.py`
  - Unit tests for key helpers, parsing, materialization behavior, and job selection.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/docs/czech_financials-research.md`
  - Add the final S3 metadata layout and marker behavior after implementation.

Do not create a `CzechJusticeResource` class for this first asset. The production boundary is just HTTP, ClickHouse, and S3; concrete functions plus `requests.Session` are easier to read and test here.

## S3 Contract

Bucket:

```text
source-czech-justice
```

Keys:

```text
financials/pdf_metadata/
  ico_prefix=<00-99>/
    metadata.jsonl
    manifest.json
    _SUCCESS.json
```

Each `metadata.jsonl` row is one financial PDF document:

```json
{
  "ico": "27074358",
  "company_name": "Asseco Central Europe, a.s.",
  "subjekt_id": "157589",
  "document_id": "85645222",
  "statement_year": "2024",
  "document_label": "účetní závěrka [2024], výroční zpráva [2024]",
  "detail_url": "https://or.justice.cz/ias/ui/vypis-sl-detail?dokument=85645222&subjektId=157589&spis=...",
  "pdf_download_url": "https://or.justice.cz/ias/content/download?id=...",
  "company_detail_url": "https://or.justice.cz/ias/ui/rejstrik-$firma?ico=27074358",
  "document_list_url": "https://or.justice.cz/ias/ui/vypis-sl-firma?subjektId=157589",
  "ico_prefix": "27",
  "discovered_at": "2026-07-03T12:00:00+00:00"
}
```

`_SUCCESS.json` is written only after every company in the partition was processed without persistent HTTP/parser failure. If `_SUCCESS.json` already exists, the asset skips the whole partition.

## Task 1: Add Partition And S3 Key Helpers

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing tests for partition keys and S3 key helpers**

Add to `test_czech_ares.py`:

```python
def test_czech_justice_pdf_metadata_partitions_cover_all_ico_prefixes():
    from dagster_v3.defs.czech_ares import financial_metadata

    keys = financial_metadata.CZECH_JUSTICE_PDF_METADATA_PARTITIONS.get_partition_keys()

    assert keys[0] == "00"
    assert keys[-1] == "99"
    assert len(keys) == 100


def test_czech_justice_pdf_metadata_s3_keys():
    from dagster_v3.defs.czech_ares import financial_metadata

    assert financial_metadata.pdf_metadata_partition_prefix("27") == (
        "financials/pdf_metadata/ico_prefix=27"
    )
    assert financial_metadata.pdf_metadata_rows_key("27") == (
        "financials/pdf_metadata/ico_prefix=27/metadata.jsonl"
    )
    assert financial_metadata.pdf_metadata_manifest_key("27") == (
        "financials/pdf_metadata/ico_prefix=27/manifest.json"
    )
    assert financial_metadata.pdf_metadata_success_key("27") == (
        "financials/pdf_metadata/ico_prefix=27/_SUCCESS.json"
    )
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_pdf_metadata_partitions_cover_all_ico_prefixes tests/test_czech_ares.py::test_czech_justice_pdf_metadata_s3_keys -q
```

Expected: import or attribute failure because `financial_metadata.py` does not exist.

- [ ] **Step 3: Implement minimal constants and helper functions**

Create `financial_metadata.py`:

```python
from __future__ import annotations

import dagster as dg

CZECH_JUSTICE_BUCKET = "source-czech-justice"
CZECH_JUSTICE_BASE_URL = "https://or.justice.cz"
CZECH_JUSTICE_UI_BASE_URL = f"{CZECH_JUSTICE_BASE_URL}/ias/ui/"
CZECH_JUSTICE_PDF_METADATA_PREFIX = "financials/pdf_metadata"
CZECH_JUSTICE_HTTP_POOL = "czech_justice_http"

CZECH_JUSTICE_PDF_METADATA_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"{value:02d}" for value in range(100)]
)


def pdf_metadata_partition_prefix(ico_prefix: str) -> str:
    return f"{CZECH_JUSTICE_PDF_METADATA_PREFIX}/ico_prefix={ico_prefix}"


def pdf_metadata_rows_key(ico_prefix: str) -> str:
    return f"{pdf_metadata_partition_prefix(ico_prefix)}/metadata.jsonl"


def pdf_metadata_manifest_key(ico_prefix: str) -> str:
    return f"{pdf_metadata_partition_prefix(ico_prefix)}/manifest.json"


def pdf_metadata_success_key(ico_prefix: str) -> str:
    return f"{pdf_metadata_partition_prefix(ico_prefix)}/_SUCCESS.json"
```

- [ ] **Step 4: Run tests and verify they pass**

Run the same command from Step 2. Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: add Czech Justice metadata partitions"
```

## Task 2: Add Czech Justice HTML Parsing Functions

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing parser tests**

Add:

```python
def test_czech_justice_extracts_subjekt_id():
    from dagster_v3.defs.czech_ares import financial_metadata

    html = '<a href="/ias/ui/vypis?subjektId=157589">Sbírka listin</a>'

    assert financial_metadata.extract_subjekt_id(html) == "157589"


def test_czech_justice_extracts_financial_documents_from_listing_html():
    from dagster_v3.defs.czech_ares import financial_metadata

    html = """
    <table>
      <tr>
        <td>účetní závěrka [2024], výroční zpráva [2024]</td>
        <td><a href="/ias/ui/vypis-sl-detail?dokument=85645222&subjektId=157589">Detail</a></td>
      </tr>
      <tr>
        <td>notářský zápis [2024]</td>
        <td><a href="/ias/ui/vypis-sl-detail?dokument=111&subjektId=157589">Detail</a></td>
      </tr>
    </table>
    """

    documents = financial_metadata.extract_financial_documents(
        html,
        base_url="https://or.justice.cz/ias/ui/vypis-sl-firma?subjektId=157589",
    )

    assert len(documents) == 1
    assert documents[0]["document_id"] == "85645222"
    assert documents[0]["statement_year"] == "2024"
    assert documents[0]["detail_url"].startswith("https://or.justice.cz/ias/ui/vypis-sl-detail")


def test_czech_justice_extracts_pdf_download_url():
    from dagster_v3.defs.czech_ares import financial_metadata

    html = '<a href="/ias/content/download?id=abc123">Stáhnout PDF</a>'

    assert financial_metadata.extract_pdf_download_url(
        html,
        base_url="https://or.justice.cz/ias/ui/vypis-sl-detail?dokument=85645222",
    ) == "https://or.justice.cz/ias/content/download?id=abc123"
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_extracts_subjekt_id tests/test_czech_ares.py::test_czech_justice_extracts_financial_documents_from_listing_html tests/test_czech_ares.py::test_czech_justice_extracts_pdf_download_url -q
```

Expected: missing function failures.

- [ ] **Step 3: Implement parsing functions using `lxml.html`**

Add:

```python
import re
import unicodedata
from urllib.parse import parse_qs, urljoin, urlparse

from lxml import html as lxml_html

FINANCIAL_LABEL_TERMS = (
    "ucetni zaverka",
    "vyrocni zprava",
)


def extract_subjekt_id(html: str) -> str | None:
    match = re.search(r"subjektId=(\d+)", html)
    return match.group(1) if match is not None else None


def extract_financial_documents(html: str, *, base_url: str) -> list[dict[str, str]]:
    tree = lxml_html.fromstring(html)
    documents: list[dict[str, str]] = []
    seen_document_ids: set[str] = set()
    for link in tree.xpath("//a[@href]"):
        href = str(link.get("href") or "")
        if "vypis-sl-detail" not in href or "dokument=" not in href:
            continue
        detail_url = urljoin(base_url, href)
        document_id = _query_param(detail_url, "dokument")
        if document_id is None or document_id in seen_document_ids:
            continue
        label = " ".join(_nearest_row_text(link).split())
        if not is_financial_document_label(label):
            continue
        seen_document_ids.add(document_id)
        documents.append(
            {
                "document_id": document_id,
                "statement_year": year_from_text(label),
                "detail_url": detail_url,
                "document_label": label,
            }
        )
    return sorted(documents, key=lambda row: (row["statement_year"], row["document_id"]))


def extract_pdf_download_url(html: str, *, base_url: str) -> str | None:
    tree = lxml_html.fromstring(html)
    for link in tree.xpath("//a[@href]"):
        href = str(link.get("href") or "")
        if "/ias/content/download" in href or "content/download?id=" in href:
            return urljoin(base_url, href)
    return None


def is_financial_document_label(value: str) -> bool:
    normalized = _ascii_lower(value)
    return any(term in normalized for term in FINANCIAL_LABEL_TERMS)


def year_from_text(value: str) -> str:
    bracket_match = re.search(r"\[(19\d{2}|20\d{2})\]", value)
    if bracket_match is not None:
        return bracket_match.group(1)
    match = re.search(r"\b(19\d{2}|20\d{2})\b", value)
    return match.group(1) if match is not None else "unknown"


def _nearest_row_text(link: lxml_html.HtmlElement) -> str:
    rows = link.xpath("ancestor::tr[1]")
    if rows:
        return rows[0].text_content()
    return link.text_content()


def _query_param(url: str, name: str) -> str | None:
    values = parse_qs(urlparse(url).query).get(name)
    return values[0] if values else None


def _ascii_lower(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower()
```

- [ ] **Step 4: Run parser tests and verify they pass**

Run the command from Step 2. Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: parse Czech Justice financial document metadata"
```

## Task 3: Add HTTP Fetch With Finite Retry

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing retry tests**

Add:

```python
def test_czech_justice_get_text_retries_timeout(monkeypatch):
    from dagster_v3.defs.czech_ares import financial_metadata

    class Response:
        encoding = "utf-8"
        text = "ok"

        def raise_for_status(self):
            return None

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, url, *, timeout):
            self.calls += 1
            if self.calls == 1:
                raise financial_metadata.requests.exceptions.Timeout("timeout")
            return Response()

    slept = []
    session = Session()

    result = financial_metadata.get_text_with_retries(
        session=session,
        url="https://example.test",
        timeout_seconds=10,
        max_attempts=2,
        retry_base_seconds=3,
        sleep=slept.append,
    )

    assert result == "ok"
    assert session.calls == 2
    assert slept == [3]


def test_czech_justice_get_text_raises_after_retry_budget(monkeypatch):
    from dagster_v3.defs.czech_ares import financial_metadata

    class Session:
        def get(self, url, *, timeout):
            raise financial_metadata.requests.exceptions.Timeout("timeout")

    with pytest.raises(financial_metadata.requests.exceptions.Timeout):
        financial_metadata.get_text_with_retries(
            session=Session(),
            url="https://example.test",
            timeout_seconds=10,
            max_attempts=2,
            retry_base_seconds=0,
            sleep=lambda _: None,
        )
```

Ensure `pytest` is imported in `test_czech_ares.py` if not already imported.

- [ ] **Step 2: Run tests and verify they fail**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_get_text_retries_timeout tests/test_czech_ares.py::test_czech_justice_get_text_raises_after_retry_budget -q
```

Expected: missing function failure.

- [ ] **Step 3: Implement HTTP helper**

Add:

```python
import time
from collections.abc import Callable

import requests

DEFAULT_JUSTICE_TIMEOUT_SECONDS = 120
DEFAULT_JUSTICE_MAX_ATTEMPTS = 5
DEFAULT_JUSTICE_RETRY_BASE_SECONDS = 2.0
DEFAULT_JUSTICE_REQUEST_DELAY_SECONDS = 0.5
DEFAULT_JUSTICE_USER_AGENT = "corpscout-czech-justice-metadata/0.1"

RETRYABLE_JUSTICE_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def get_text_with_retries(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    max_attempts: int,
    retry_base_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            if response.encoding is None:
                response.encoding = "utf-8"
            return response.text
        except RETRYABLE_JUSTICE_ERRORS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep(retry_base_seconds * attempt)
    assert last_error is not None
    raise last_error
```

Do not catch `HTTPError` as retryable by default. If Justice returns a persistent 4xx/5xx, fail the partition so missing source data is visible.

- [ ] **Step 4: Run retry tests and verify they pass**

Run the command from Step 2. Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: add Czech Justice HTTP retry helper"
```

## Task 4: Add Company Query And Metadata Discovery

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing tests for ClickHouse query shape**

Add a focused fake ClickHouse test:

```python
def test_czech_justice_fetches_company_rows_by_ico_prefix():
    from dagster_v3.defs.czech_ares import financial_metadata

    class Client:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))
            return [("27074358", "Asseco Central Europe, a.s.")]

    class Connection:
        def __enter__(self):
            return client

        def __exit__(self, exc_type, exc, tb):
            return None

    class ClickHouse:
        def get_connection(self):
            return Connection()

    client = Client()

    rows = financial_metadata.fetch_company_rows_for_ico_prefix(
        clickhouse=ClickHouse(),
        ico_prefix="27",
    )

    assert rows == [{"ico": "27074358", "company_name": "Asseco Central Europe, a.s."}]
    assert client.calls[0][1] == {"ico_prefix": "27"}
    assert "startsWith(ico, %(ico_prefix)s)" in client.calls[0][0]
```

- [ ] **Step 2: Run test and verify it fails**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_fetches_company_rows_by_ico_prefix -q
```

Expected: missing function failure.

- [ ] **Step 3: Implement company query**

Add:

```python
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.czech_ares import tables


def fetch_company_rows_for_ico_prefix(
    *,
    clickhouse: ClickhouseResource,
    ico_prefix: str,
) -> list[dict[str, str]]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT
                ico,
                name
            FROM {tables.QUALIFIED_COMPANIES_TABLE}
            WHERE length(ico) = 8
              AND match(ico, '^[0-9]{{8}}$')
              AND startsWith(ico, %(ico_prefix)s)
            ORDER BY ico
            """,
            {"ico_prefix": ico_prefix},
        )
    return [{"ico": str(row[0]), "company_name": str(row[1])} for row in rows]
```

- [ ] **Step 4: Run test and verify it passes**

Run the command from Step 2. Expected: `1 passed`.

- [ ] **Step 5: Write failing test for one-company metadata discovery**

Add:

```python
def test_czech_justice_discovers_pdf_metadata_for_company():
    from dagster_v3.defs.czech_ares import financial_metadata

    detail_url = "https://or.justice.cz/ias/ui/rejstrik-$firma?ico=27074358"
    list_url = "https://or.justice.cz/ias/ui/vypis-sl-firma?subjektId=157589"
    doc_url = "https://or.justice.cz/ias/ui/vypis-sl-detail?dokument=85645222&subjektId=157589"

    pages = {
        detail_url: '<a href="/ias/ui/vypis?subjektId=157589">Sbírka</a>',
        list_url: (
            '<table><tr><td>účetní závěrka [2024]</td>'
            '<td><a href="/ias/ui/vypis-sl-detail?dokument=85645222&subjektId=157589">Detail</a></td>'
            '</tr></table>'
        ),
        doc_url: '<a href="/ias/content/download?id=abc123">PDF</a>',
    }

    class Session:
        headers = {}

        def get(self, url, *, timeout):
            class Response:
                encoding = "utf-8"
                text = pages[url]

                def raise_for_status(self):
                    return None

            return Response()

    rows = financial_metadata.discover_pdf_metadata_for_company(
        session=Session(),
        company={"ico": "27074358", "company_name": "Asseco Central Europe, a.s."},
        timeout_seconds=10,
        max_attempts=1,
        retry_base_seconds=0,
        sleep=lambda _: None,
    )

    assert rows == [
        {
            "ico": "27074358",
            "company_name": "Asseco Central Europe, a.s.",
            "subjekt_id": "157589",
            "document_id": "85645222",
            "statement_year": "2024",
            "document_label": "účetní závěrka [2024]",
            "detail_url": doc_url,
            "pdf_download_url": "https://or.justice.cz/ias/content/download?id=abc123",
            "company_detail_url": detail_url,
            "document_list_url": list_url,
        }
    ]
```

- [ ] **Step 6: Implement discovery function**

Add:

```python
def discover_pdf_metadata_for_company(
    *,
    session: requests.Session,
    company: dict[str, str],
    timeout_seconds: int,
    max_attempts: int,
    retry_base_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, str]]:
    ico = company["ico"]
    company_detail_url = f"{CZECH_JUSTICE_UI_BASE_URL}rejstrik-$firma?ico={ico}"
    detail_html = get_text_with_retries(
        session=session,
        url=company_detail_url,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        retry_base_seconds=retry_base_seconds,
        sleep=sleep,
    )
    subjekt_id = extract_subjekt_id(detail_html)
    if subjekt_id is None:
        return []

    document_list_url = f"{CZECH_JUSTICE_UI_BASE_URL}vypis-sl-firma?subjektId={subjekt_id}"
    list_html = get_text_with_retries(
        session=session,
        url=document_list_url,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        retry_base_seconds=retry_base_seconds,
        sleep=sleep,
    )
    documents = extract_financial_documents(list_html, base_url=document_list_url)

    rows: list[dict[str, str]] = []
    for document in documents:
        detail_html = get_text_with_retries(
            session=session,
            url=document["detail_url"],
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            retry_base_seconds=retry_base_seconds,
            sleep=sleep,
        )
        pdf_url = extract_pdf_download_url(detail_html, base_url=document["detail_url"])
        if pdf_url is None:
            continue
        rows.append(
            {
                "ico": ico,
                "company_name": company["company_name"],
                "subjekt_id": subjekt_id,
                "document_id": document["document_id"],
                "statement_year": document["statement_year"],
                "document_label": document["document_label"],
                "detail_url": document["detail_url"],
                "pdf_download_url": pdf_url,
                "company_detail_url": company_detail_url,
                "document_list_url": document_list_url,
            }
        )
    return rows
```

- [ ] **Step 7: Run tests and verify they pass**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_fetches_company_rows_by_ico_prefix tests/test_czech_ares.py::test_czech_justice_discovers_pdf_metadata_for_company -q
```

Expected: `2 passed`.

- [ ] **Step 8: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: discover Czech Justice PDF metadata"
```

## Task 5: Add Partition Materializer That Writes JSONL To S3

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing skip test**

Add:

```python
def test_czech_justice_metadata_partition_skips_when_success_exists(monkeypatch):
    from dagster_v3.defs.czech_ares import financial_metadata

    class Store:
        def exists(self, key, *, bucket=None):
            return key.endswith("_SUCCESS.json")

    def fail_fetch(*args, **kwargs):
        raise AssertionError("ClickHouse should not be queried")

    monkeypatch.setattr(financial_metadata, "fetch_company_rows_for_ico_prefix", fail_fetch)

    result = financial_metadata.materialize_czech_justice_pdf_metadata_partition(
        ico_prefix="27",
        clickhouse=object(),
        object_store=Store(),
        log_info=lambda *args: None,
    )

    assert result.metadata["skipped_existing_partition"].value is True
```

- [ ] **Step 2: Write failing materialization test**

Add:

```python
def test_czech_justice_metadata_partition_writes_jsonl_manifest_and_success(monkeypatch):
    from dagster_v3.defs.czech_ares import financial_metadata

    writes = {}

    class Store:
        def ensure_bucket(self, bucket=None):
            writes["bucket"] = bucket

        def exists(self, key, *, bucket=None):
            return False

        def write_bytes(self, key, body, *, bucket=None):
            writes[key] = body

    monkeypatch.setattr(
        financial_metadata,
        "fetch_company_rows_for_ico_prefix",
        lambda *, clickhouse, ico_prefix: [
            {"ico": "27074358", "company_name": "Asseco Central Europe, a.s."}
        ],
    )
    monkeypatch.setattr(
        financial_metadata,
        "discover_pdf_metadata_for_company",
        lambda **kwargs: [
            {
                "ico": "27074358",
                "company_name": "Asseco Central Europe, a.s.",
                "subjekt_id": "157589",
                "document_id": "85645222",
                "statement_year": "2024",
                "document_label": "účetní závěrka [2024]",
                "detail_url": "https://or.justice.cz/detail",
                "pdf_download_url": "https://or.justice.cz/ias/content/download?id=abc",
                "company_detail_url": "https://or.justice.cz/company",
                "document_list_url": "https://or.justice.cz/list",
            }
        ],
    )

    result = financial_metadata.materialize_czech_justice_pdf_metadata_partition(
        ico_prefix="27",
        clickhouse=object(),
        object_store=Store(),
        log_info=lambda *args: None,
        sleep=lambda _: None,
    )

    rows_key = financial_metadata.pdf_metadata_rows_key("27")
    manifest_key = financial_metadata.pdf_metadata_manifest_key("27")
    success_key = financial_metadata.pdf_metadata_success_key("27")

    assert writes["bucket"] == financial_metadata.CZECH_JUSTICE_BUCKET
    assert b'"document_id": "85645222"' in writes[rows_key]
    assert b'"companies_count": 1' in writes[manifest_key]
    assert b'"documents_count": 1' in writes[success_key]
    assert result.metadata["documents_count"].value == 1
```

- [ ] **Step 3: Run tests and verify they fail**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_metadata_partition_skips_when_success_exists tests/test_czech_ares.py::test_czech_justice_metadata_partition_writes_jsonl_manifest_and_success -q
```

Expected: missing function failure.

- [ ] **Step 4: Implement partition materializer**

Add:

```python
import json
from datetime import UTC, datetime

from dagster_v3.defs.common.resources import ObjectStoreResource


def materialize_czech_justice_pdf_metadata_partition(
    *,
    ico_prefix: str,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    timeout_seconds: int = DEFAULT_JUSTICE_TIMEOUT_SECONDS,
    max_attempts: int = DEFAULT_JUSTICE_MAX_ATTEMPTS,
    retry_base_seconds: float = DEFAULT_JUSTICE_RETRY_BASE_SECONDS,
    request_delay_seconds: float = DEFAULT_JUSTICE_REQUEST_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[..., None] | None = None,
) -> dg.MaterializeResult:
    rows_key = pdf_metadata_rows_key(ico_prefix)
    manifest_key = pdf_metadata_manifest_key(ico_prefix)
    success_key = pdf_metadata_success_key(ico_prefix)
    prefix = pdf_metadata_partition_prefix(ico_prefix)

    if object_store.exists(success_key, bucket=CZECH_JUSTICE_BUCKET):
        _log(log_info, "Czech Justice PDF metadata partition already complete: ico_prefix=%s", ico_prefix)
        return dg.MaterializeResult(
            metadata={
                "ico_prefix": ico_prefix,
                "s3_bucket": CZECH_JUSTICE_BUCKET,
                "s3_prefix": prefix,
                "rows_key": rows_key,
                "manifest_key": manifest_key,
                "success_key": success_key,
                "skipped_existing_partition": True,
            }
        )

    object_store.ensure_bucket(CZECH_JUSTICE_BUCKET)
    companies = fetch_company_rows_for_ico_prefix(clickhouse=clickhouse, ico_prefix=ico_prefix)
    session = requests.Session()
    session.headers["User-Agent"] = DEFAULT_JUSTICE_USER_AGENT

    discovered_at = datetime.now(UTC).isoformat()
    metadata_rows: list[dict[str, str]] = []
    companies_with_documents = 0
    for index, company in enumerate(companies, start=1):
        rows = discover_pdf_metadata_for_company(
            session=session,
            company=company,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            retry_base_seconds=retry_base_seconds,
            sleep=sleep,
        )
        if rows:
            companies_with_documents += 1
        for row in rows:
            metadata_rows.append(
                {
                    **row,
                    "ico_prefix": ico_prefix,
                    "discovered_at": discovered_at,
                }
            )
        if _should_log_progress(index, len(companies)):
            _log(
                log_info,
                "Czech Justice PDF metadata progress: ico_prefix=%s companies=%s/%s documents=%s",
                ico_prefix,
                index,
                len(companies),
                len(metadata_rows),
            )
        if request_delay_seconds > 0 and index < len(companies):
            sleep(request_delay_seconds)

    rows_body = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in metadata_rows
    ).encode("utf-8")
    manifest = {
        "ico_prefix": ico_prefix,
        "companies_count": len(companies),
        "companies_with_documents_count": companies_with_documents,
        "documents_count": len(metadata_rows),
        "rows_key": rows_key,
        "manifest_key": manifest_key,
        "success_key": success_key,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    manifest_body = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")

    object_store.write_bytes(rows_key, rows_body, bucket=CZECH_JUSTICE_BUCKET)
    object_store.write_bytes(manifest_key, manifest_body, bucket=CZECH_JUSTICE_BUCKET)
    object_store.write_bytes(success_key, manifest_body, bucket=CZECH_JUSTICE_BUCKET)

    return dg.MaterializeResult(
        metadata={
            **manifest,
            "s3_bucket": CZECH_JUSTICE_BUCKET,
            "s3_prefix": prefix,
            "skipped_existing_partition": False,
        }
    )


def _should_log_progress(index: int, total: int) -> bool:
    return index == 1 or index == total or index % 100 == 0


def _log(log_info: Callable[..., None] | None, message: str, *args: object) -> None:
    if log_info is not None:
        log_info(message, *args)
```

Do not catch exceptions around `discover_pdf_metadata_for_company` here. Persistent failures must fail the partition and leave `_SUCCESS.json` absent.

- [ ] **Step 5: Run tests and verify they pass**

Run the command from Step 3. Expected: `2 passed`.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: write Czech Justice PDF metadata to S3"
```

## Task 6: Add Dagster Asset And Job Wiring

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/assets.py`
- Test: `corpscout/dagster_v3/tests/test_czech_ares.py`

- [ ] **Step 1: Write failing asset/job tests**

Add:

```python
def test_czech_justice_pdf_metadata_job_selects_only_metadata_asset():
    from dagster_v3.defs.czech_ares import assets

    asset_keys = {
        key.to_user_string()
        for key in assets.czech_justice_pdf_metadata_job.selection.resolve(
            assets.defs.get_asset_graph()
        )
    }

    assert asset_keys == {"czech_justice_pdf_metadata_s3"}
```

- [ ] **Step 2: Run test and verify it fails**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_pdf_metadata_job_selects_only_metadata_asset -q
```

Expected: missing job failure.

- [ ] **Step 3: Add asset function**

In `financial_metadata.py`, add:

```python
class CzechJusticePdfMetadataConfig(dg.Config):
    timeout_seconds: int = DEFAULT_JUSTICE_TIMEOUT_SECONDS
    max_attempts: int = DEFAULT_JUSTICE_MAX_ATTEMPTS
    retry_base_seconds: float = DEFAULT_JUSTICE_RETRY_BASE_SECONDS
    request_delay_seconds: float = DEFAULT_JUSTICE_REQUEST_DELAY_SECONDS


@dg.asset(
    name="czech_justice_pdf_metadata_s3",
    group_name="czech_financials",
    deps=[dg.AssetKey("czech_ares_clickhouse_companies")],
    partitions_def=CZECH_JUSTICE_PDF_METADATA_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=CZECH_JUSTICE_HTTP_POOL,
    kinds={"python", "s3", "metadata", "justice"},
    description=(
        "Discovers Czech Justice Sbírka listin financial PDF download URLs and "
        "stores metadata JSONL on S3 by two-digit IČO prefix. Does not download PDFs."
    ),
)
def czech_justice_pdf_metadata_s3(
    context: dg.AssetExecutionContext,
    config: CzechJusticePdfMetadataConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return materialize_czech_justice_pdf_metadata_partition(
        ico_prefix=context.partition_key,
        clickhouse=clickhouse,
        object_store=object_store,
        timeout_seconds=config.timeout_seconds,
        max_attempts=config.max_attempts,
        retry_base_seconds=config.retry_base_seconds,
        request_delay_seconds=config.request_delay_seconds,
        log_info=context.log.info,
    )
```

- [ ] **Step 4: Wire asset, job, and resource in `assets.py`**

Add imports:

```python
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.czech_ares.financial_metadata import czech_justice_pdf_metadata_s3
```

Add job:

```python
czech_justice_pdf_metadata_job = dg.define_asset_job(
    "czech_justice_pdf_metadata_job",
    selection=dg.AssetSelection.assets("czech_justice_pdf_metadata_s3"),
)
```

Add asset to `defs.assets`:

```python
czech_justice_pdf_metadata_s3,
```

Add job to `defs.jobs`:

```python
czech_justice_pdf_metadata_job,
```

Add resource to `defs.resources`:

```python
"object_store": ObjectStoreResource(),
```

Do not add a schedule. This is a manually-triggered/backfilled metadata crawl.

- [ ] **Step 5: Run asset/job test and `dg check`**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py::test_czech_justice_pdf_metadata_job_selects_only_metadata_asset -q
uv run dg check defs
```

Expected: test passes; `dg check defs` exits 0.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/financial_metadata.py corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/assets.py corpscout/dagster_v3/tests/test_czech_ares.py
git commit -m "feat: add Czech Justice PDF metadata asset"
```

## Task 7: Document Operational Behavior

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/docs/czech_financials-research.md`

- [ ] **Step 1: Add S3 metadata asset section**

Add under the financials section:

```markdown
### Planned metadata crawl asset

Asset: `czech_justice_pdf_metadata_s3`

This asset discovers financial PDF metadata only. It does not download PDFs.
It is partitioned by the first two digits of Czech IČO (`00` through `99`).
Each partition reads matching companies from `corpscout.cz_companies`, scrapes
`or.justice.cz`, filters document labels for `účetní závěrka` and `výroční zpráva`,
resolves the final `/ias/content/download?id=...` URL, and writes metadata to:

```text
source-czech-justice/
  financials/pdf_metadata/
    ico_prefix=<00-99>/
      metadata.jsonl
      manifest.json
      _SUCCESS.json
```

`_SUCCESS.json` is the completion marker. If it exists, the partition is skipped.
If HTTP or parsing fails persistently, the partition fails and does not write
`_SUCCESS.json`.
```

- [ ] **Step 2: Run markdown diff check**

```bash
cd corpscout
git diff --check -- dagster_v3/src/dagster_v3/defs/czech_ares/docs/czech_financials-research.md
```

Expected: no whitespace errors.

- [ ] **Step 3: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/docs/czech_financials-research.md
git commit -m "docs: document Czech Justice PDF metadata crawl"
```

## Task 8: Final Verification

**Files:**
- All touched files.

- [ ] **Step 1: Run focused tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_czech_ares.py -q
```

Expected: all Czech ARES tests pass.

- [ ] **Step 2: Run lint/checks for definitions**

```bash
cd corpscout/dagster_v3
uv run ruff check src/dagster_v3/defs/czech_ares tests/test_czech_ares.py
uv run dg check defs
```

Expected: both commands exit 0.

- [ ] **Step 3: Run diff whitespace check**

```bash
cd corpscout
git diff --check
```

Expected: no whitespace errors.

- [ ] **Step 4: Optional smoke materialization for one partition**

Run only when S3, ClickHouse, and network access to `or.justice.cz` are available:

```bash
cd corpscout/dagster_v3
uv run dg launch --assets "czech_justice_pdf_metadata_s3" --partition "27"
```

Expected in Dagster logs:

```text
Czech Justice PDF metadata progress: ico_prefix=27 ...
```

Expected S3 objects:

```text
source-czech-justice/financials/pdf_metadata/ico_prefix=27/metadata.jsonl
source-czech-justice/financials/pdf_metadata/ico_prefix=27/manifest.json
source-czech-justice/financials/pdf_metadata/ico_prefix=27/_SUCCESS.json
```

## Design Notes

- This first asset intentionally stores metadata only. PDF download, OCR page discovery, GLM-OCR table extraction, and ClickHouse financial metrics are separate downstream tasks.
- The asset should fail on persistent HTTP/source errors. Silent partial metadata would be worse than a failed partition.
- `czech_justice_http` should be configured as a low-concurrency Dagster pool on the deployment. Start with pool size `1`; increase only after observing Justice source behavior.
- Do not add a schedule. This crawl is large and should be operated as explicit partition backfills.
- Do not add a generic “document scraper” abstraction. The Justice site flow is source-specific and easier to maintain when the URLs and parsing decisions are visible in one file.
