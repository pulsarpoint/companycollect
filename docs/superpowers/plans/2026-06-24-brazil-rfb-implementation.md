# Brazil RFB Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Brazil RFB CNPJ registry source in `dagster_v3`, landing official bulk CSVs in DuckDB, transforming companies/establishments/contacts/domains/industries, and exporting normalized tables to ClickHouse.

**Architecture:** Use a dlt asset as the extraction boundary for the monthly snapshot file manifest and download/extract metadata, then use DuckDB's C++ `read_csv` reader for the actual large CSV rows. Keep RFB raw staging in `data/brazil_rfb_source.duckdb`; build set-based DuckDB transforms; export migration-owned ClickHouse tables atomically.

**Tech Stack:** Dagster assets, `dagster_dlt`, dlt DuckDB destination, DuckDB SQL, ClickHouse migrations, `dagster_clickhouse`, existing `dagster_v3.domains` UDFs, pytest.

---

## Scope

This plan implements the source described in `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/docs/brazil_rfb-design.md`.

In scope:
- RFB CNPJ bulk snapshot file discovery/download metadata.
- ZIP extraction and raw CSV loading into DuckDB.
- `br_companies`, `br_establishments`, `br_company_contacts`, `br_company_domains`, and `br_industries`.
- CNAE to NACE mapping through the already-created `corpscout.br_cnae_to_nace`.
- ClickHouse migrations and exports.
- Jobs/schedules and validation.

Out of scope:
- `Socios` partner data. That needs a separate restricted partner-enrichment design.
- CVM DFP/ITR financials. That needs a separate financial-statement design.

## File Structure

Create these package files:

- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/__init__.py`
  Loads source definitions from `assets.py`.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
  Dataset/table names, raw RFB column layouts, ClickHouse export columns.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/source.py`
  dlt snapshot file manifest source, URL discovery, ZIP download/extract helpers.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py`
  DuckDB `read_csv` staging from the dlt manifest table into raw family tables.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py`
  Legal entity and establishment transforms.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py`
  Contact unpivot and email-domain derivation.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/company_domains.py`
  Deduped company-domain feeder table.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/industries.py`
  CNAE splitting, dedupe, and CNAE-to-NACE mapping.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
  Export helpers for migration-owned ClickHouse tables.
- `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
  Dagster assets, jobs, and schedules.

Create tests:

- `corpscout/dagster_v3/tests/test_brazil_rfb_source.py`
- `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`
- `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`
- `corpscout/dagster_v3/tests/test_brazil_rfb_contacts.py`
- `corpscout/dagster_v3/tests/test_brazil_rfb_industries.py`
- `corpscout/dagster_v3/tests/test_brazil_rfb_assets.py`

Create migrations:

- `corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.up.sql`
- `corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.down.sql`

Modify:

- `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
  Register and assert the new migration.
- `corpscout/dagster_v3/src/dagster_v3/defs/domains/assets.py`
  Add `br_company_domains` to the shared domain graph.
- `corpscout/dagster_v3/src/dagster_v3/defs/domains/tables.py`
  Add Brazil branch/export constants if the existing domain export helper needs source-table constants.

---

## Section 1: Pull CSV Snapshot -> dlt Manifest -> DuckDB Raw Staging

This section is the first implementation milestone. Stop and verify it before starting transforms. The output is a materializable `brazil_rfb_snapshot_files_duckdb` dlt asset plus `brazil_rfb_raw_files_duckdb` with raw DuckDB tables for Empresas, Estabelecimentos, Simples, and reference CSVs.

### Task 1: Package Skeleton And RFB Raw Schemas

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`

- [ ] **Step 1: Write failing schema tests**

Add this to `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`:

```python
from dagster_v3.defs.brazil_rfb import tables


def test_rfb_raw_column_layouts_match_published_file_families() -> None:
    assert tables.RAW_TABLE_BY_FAMILY == {
        "empresas": "empresas_raw",
        "estabelecimentos": "estabelecimentos_raw",
        "simples": "simples_raw",
        "cnaes": "cnaes_raw",
        "naturezas": "naturezas_raw",
        "municipios": "municipios_raw",
        "paises": "paises_raw",
        "qualificacoes": "qualificacoes_raw",
        "motivos": "motivos_raw",
    }
    assert tables.RAW_COLUMNS_BY_FAMILY["empresas"] == (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    )
    assert tables.RAW_COLUMNS_BY_FAMILY["estabelecimentos"] == (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    )
    assert tables.RAW_COLUMNS_BY_FAMILY["simples"] == (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    )


def test_reference_file_families_are_two_column_code_lists() -> None:
    for family in (
        "cnaes",
        "naturezas",
        "municipios",
        "paises",
        "qualificacoes",
        "motivos",
    ):
        assert tables.RAW_COLUMNS_BY_FAMILY[family] == ("code", "description_pt")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py::test_rfb_raw_column_layouts_match_published_file_families -q
```

Expected: FAIL because `dagster_v3.defs.brazil_rfb` does not exist.

- [ ] **Step 3: Create package and table constants**

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/__init__.py`:

```python
from dagster_v3.defs.brazil_rfb.assets import defs

__all__ = ["defs"]
```

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`:

```python
DLT_DATASET_NAME = "brazil_rfb"
SNAPSHOT_FILES_TABLE = "snapshot_files"

BRAZIL_RFB_DATABASE = "corpscout"

RAW_TABLE_BY_FAMILY = {
    "empresas": "empresas_raw",
    "estabelecimentos": "estabelecimentos_raw",
    "simples": "simples_raw",
    "cnaes": "cnaes_raw",
    "naturezas": "naturezas_raw",
    "municipios": "municipios_raw",
    "paises": "paises_raw",
    "qualificacoes": "qualificacoes_raw",
    "motivos": "motivos_raw",
}

RAW_COLUMNS_BY_FAMILY = {
    "empresas": (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    ),
    "estabelecimentos": (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ),
    "simples": (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ),
    "cnaes": ("code", "description_pt"),
    "naturezas": ("code", "description_pt"),
    "municipios": ("code", "description_pt"),
    "paises": ("code", "description_pt"),
    "qualificacoes": ("code", "description_pt"),
    "motivos": ("code", "description_pt"),
}

RAW_PROVENANCE_COLUMNS = (
    "source_file_family",
    "source_archive_url",
    "source_csv_member",
    "source_run_id",
    "loaded_at",
)
```

Create a temporary minimal `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py` so package import succeeds:

```python
import dagster as dg

defs = dg.Definitions()
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py -q
```

Expected: PASS for the two schema tests.

- [ ] **Step 5: Commit Section 1 schema skeleton**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/__init__.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_staging.py
git commit -m "Add Brazil RFB source skeleton"
```

### Task 2: dlt Snapshot File Manifest Source

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/source.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_source.py`

- [ ] **Step 1: Write failing dlt schema and discovery tests**

Create `corpscout/dagster_v3/tests/test_brazil_rfb_source.py`:

```python
import io
import zipfile
from pathlib import Path

from dagster_v3.defs.brazil_rfb import source


def _zip_bytes(member_name: str, body: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, body)
    return output.getvalue()


class FakeResponse:
    def __init__(self, body: bytes, *, json_payload: dict | None = None) -> None:
        self.content = body
        self.headers = {"Content-Length": str(len(body))}
        self._json_payload = json_payload

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0):
        yield self.content

    def json(self) -> dict:
        if self._json_payload is None:
            raise AssertionError("json() called on non-json fake response")
        return self._json_payload


class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, bool]] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.calls.append((url, stream))
        return self.responses[url]


def test_family_from_archive_name_matches_rfb_patterns() -> None:
    assert source.family_from_archive_name("K3241.K03200Y0.D30612.EMPRECSV.zip") == "empresas"
    assert source.family_from_archive_name("K3241.K03200Y0.D30612.ESTABELE.zip") == "estabelecimentos"
    assert source.family_from_archive_name("K3241.K03200Y0.D30612.SIMPLES.CSV.zip") == "simples"
    assert source.family_from_archive_name("F.K03200$Z.D30612.CNAECSV.zip") == "cnaes"
    assert source.family_from_archive_name("F.K03200$Z.D30612.NATJUCSV.zip") == "naturezas"
    assert source.family_from_archive_name("F.K03200$Z.D30612.MUNICCSV.zip") == "municipios"
    assert source.family_from_archive_name("F.K03200$Z.D30612.PAISCSV.zip") == "paises"
    assert source.family_from_archive_name("F.K03200$Z.D30612.QUALSCSV.zip") == "qualificacoes"
    assert source.family_from_archive_name("F.K03200$Z.D30612.MOTICSV.zip") == "motivos"
    assert source.family_from_archive_name("SOCIOCSV.zip") == ""


def test_discover_snapshot_zip_urls_from_directory_html() -> None:
    html = '''
    <html><body>
      <a href="K3241.K03200Y0.D30612.EMPRECSV.zip">empresas</a>
      <a href="K3241.K03200Y0.D30612.ESTABELE.zip">estab</a>
      <a href="ignore.txt">ignore</a>
    </body></html>
    '''

    files = source.discover_snapshot_zip_urls(
        html,
        base_url="https://example.test/dados_abertos_cnpj/2026-06/",
        families=("empresas", "estabelecimentos"),
    )

    assert [(item.family, item.url) for item in files] == [
        (
            "empresas",
            "https://example.test/dados_abertos_cnpj/2026-06/K3241.K03200Y0.D30612.EMPRECSV.zip",
        ),
        (
            "estabelecimentos",
            "https://example.test/dados_abertos_cnpj/2026-06/K3241.K03200Y0.D30612.ESTABELE.zip",
        ),
    ]


def test_download_extract_and_build_manifest_rows(tmp_path: Path) -> None:
    archive_url = "https://example.test/K3241.K03200Y0.D30612.EMPRECSV.zip"
    session = FakeSession(
        {
            archive_url: FakeResponse(
                _zip_bytes(
                    "K3241.K03200Y0.D30612.EMPRECSV",
                    b"12345678;ACME LTDA;2062;49;1000,00;01;\\n",
                )
            )
        }
    )

    rows = source.download_extract_snapshot_files(
        remote_files=[
            source.BrazilRfbRemoteFile(
                family="empresas",
                url=archive_url,
                archive_name="K3241.K03200Y0.D30612.EMPRECSV.zip",
            )
        ],
        download_dir=tmp_path,
        source_run_id="run-1",
        session=session,
    )

    assert len(rows) == 1
    assert rows[0]["family"] == "empresas"
    assert rows[0]["archive_url"] == archive_url
    assert rows[0]["archive_sha256"]
    assert rows[0]["csv_member_name"] == "K3241.K03200Y0.D30612.EMPRECSV"
    assert Path(rows[0]["csv_path"]).exists()
    assert rows[0]["source_run_id"] == "run-1"
    assert session.calls == [(archive_url, True)]


def test_snapshot_files_resource_declares_explicit_schema(tmp_path: Path) -> None:
    row = source.build_snapshot_file_row(
        family="empresas",
        archive_url="https://example.test/empresas.zip",
        archive_name="empresas.zip",
        archive_sha256="a" * 64,
        csv_member_name="empresas.csv",
        csv_path=tmp_path / "empresas.csv",
        source_run_id="run-1",
    )
    dlt_source = source.brazil_rfb_source(
        manifest_rows=[row],
        source_run_id="run-1",
    )
    schema = dlt_source.resources["snapshot_files"].compute_table_schema()

    assert set(schema["columns"]) == set(row)
    assert schema["columns"]["family"]["data_type"] == "text"
    assert schema["columns"]["csv_path"]["data_type"] == "text"
    assert schema["columns"]["retrieved_at"]["data_type"] == "timestamp"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_source.py -q
```

Expected: FAIL because `source.py` and functions do not exist.

- [ ] **Step 3: Add dlt snapshot source implementation**

Append to `tables.py`:

```python
SNAPSHOT_FILE_COLUMNS = {
    "family": {"data_type": "text", "nullable": False},
    "archive_url": {"data_type": "text", "nullable": False},
    "archive_name": {"data_type": "text", "nullable": False},
    "archive_sha256": {"data_type": "text", "nullable": False},
    "csv_member_name": {"data_type": "text", "nullable": False},
    "csv_path": {"data_type": "text", "nullable": False},
    "source_run_id": {"data_type": "text", "nullable": False},
    "retrieved_at": {"data_type": "timestamp", "nullable": False},
}
```

Create `source.py`:

```python
from __future__ import annotations

import hashlib
import re
import tempfile
import zipfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.brazil_rfb import tables

DEFAULT_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_FAMILIES = tuple(tables.RAW_TABLE_BY_FAMILY)
DOWNLOAD_CHUNK_BYTES = 1 << 20


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


@dataclass(frozen=True)
class BrazilRfbRemoteFile:
    family: str
    url: str
    archive_name: str


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


_FAMILY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("empresas", re.compile(r"EMPRECSV\\.zip$", re.IGNORECASE)),
    ("estabelecimentos", re.compile(r"ESTABELE\\.zip$", re.IGNORECASE)),
    ("simples", re.compile(r"SIMPLES\\.CSV\\.zip$", re.IGNORECASE)),
    ("cnaes", re.compile(r"CNAECSV\\.zip$", re.IGNORECASE)),
    ("naturezas", re.compile(r"NATJUCSV\\.zip$", re.IGNORECASE)),
    ("municipios", re.compile(r"MUNICCSV\\.zip$", re.IGNORECASE)),
    ("paises", re.compile(r"PAISCSV\\.zip$", re.IGNORECASE)),
    ("qualificacoes", re.compile(r"QUALSCSV\\.zip$", re.IGNORECASE)),
    ("motivos", re.compile(r"MOTICSV\\.zip$", re.IGNORECASE)),
)


def family_from_archive_name(archive_name: str) -> str:
    normalized = archive_name.strip()
    for family, pattern in _FAMILY_PATTERNS:
        if pattern.search(normalized):
            return family
    return ""


def discover_snapshot_zip_urls(
    html: str,
    *,
    base_url: str,
    families: Sequence[str] = DEFAULT_FAMILIES,
) -> list[BrazilRfbRemoteFile]:
    wanted = set(families)
    parser = _HrefParser()
    parser.feed(html)
    files: list[BrazilRfbRemoteFile] = []
    for href in parser.hrefs:
        archive_name = href.rstrip("/").split("/")[-1]
        family = family_from_archive_name(archive_name)
        if family not in wanted:
            continue
        files.append(
            BrazilRfbRemoteFile(
                family=family,
                url=urljoin(base_url, href),
                archive_name=archive_name,
            )
        )
    return sorted(files, key=lambda item: (item.family, item.archive_name))


def build_month_base_url(*, snapshot_month: str, base_url: str = DEFAULT_BASE_URL) -> str:
    clean_month = snapshot_month.strip()
    if not re.fullmatch(r"\\d{4}-\\d{2}", clean_month):
        raise ValueError("snapshot_month must use YYYY-MM format")
    return urljoin(base_url.rstrip("/") + "/", clean_month + "/")


def fetch_snapshot_remote_files(
    *,
    snapshot_month: str,
    base_url: str = DEFAULT_BASE_URL,
    families: Sequence[str] = DEFAULT_FAMILIES,
    session: HttpSession | None = None,
    timeout_seconds: int = 60,
) -> list[BrazilRfbRemoteFile]:
    month_url = build_month_base_url(snapshot_month=snapshot_month, base_url=base_url)
    http_session = session or dlt_requests.Session()
    response = http_session.get(month_url, timeout=timeout_seconds)
    response.raise_for_status()
    files = discover_snapshot_zip_urls(
        response.content.decode("utf-8", errors="replace"),
        base_url=month_url,
        families=families,
    )
    missing = sorted(set(families) - {item.family for item in files})
    if missing:
        raise LookupError(f"missing Brazil RFB snapshot file families: {', '.join(missing)}")
    return files


def build_snapshot_file_row(
    *,
    family: str,
    archive_url: str,
    archive_name: str,
    archive_sha256: str,
    csv_member_name: str,
    csv_path: str | Path,
    source_run_id: str,
    retrieved_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "family": family,
        "archive_url": archive_url,
        "archive_name": archive_name,
        "archive_sha256": archive_sha256,
        "csv_member_name": csv_member_name,
        "csv_path": str(csv_path),
        "source_run_id": source_run_id,
        "retrieved_at": retrieved_at or datetime.now(timezone.utc),
    }


def _download(url: str, *, dest: Path, session: HttpSession | None, timeout_seconds: int) -> bytes:
    http_session = session or dlt_requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    sha = hashlib.sha256()
    with dest.open("wb") as output:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            output.write(chunk)
            sha.update(chunk)
    return sha.digest()


def _extract_single_csv(zip_path: Path, dest_dir: Path) -> tuple[str, Path]:
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one CSV member in {zip_path}, found {members}")
        member = members[0]
        archive.extract(member, dest_dir)
        return member, dest_dir / member


def download_extract_snapshot_files(
    *,
    remote_files: Sequence[BrazilRfbRemoteFile],
    download_dir: str | Path,
    source_run_id: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, object]]:
    root = Path(download_dir)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for remote_file in remote_files:
        family_dir = root / remote_file.family
        family_dir.mkdir(parents=True, exist_ok=True)
        archive_path = family_dir / remote_file.archive_name
        digest = _download(
            remote_file.url,
            dest=archive_path,
            session=session,
            timeout_seconds=timeout_seconds,
        ).hex()
        csv_member_name, csv_path = _extract_single_csv(archive_path, family_dir)
        rows.append(
            build_snapshot_file_row(
                family=remote_file.family,
                archive_url=remote_file.url,
                archive_name=remote_file.archive_name,
                archive_sha256=digest,
                csv_member_name=csv_member_name,
                csv_path=csv_path,
                source_run_id=source_run_id,
            )
        )
    return rows


@dlt.resource(
    name=tables.SNAPSHOT_FILES_TABLE,
    write_disposition="replace",
    columns=tables.SNAPSHOT_FILE_COLUMNS,
)
def snapshot_files_resource(rows: Sequence[dict[str, object]]) -> Iterator[dict[str, object]]:
    yield from rows


@dlt.source(name="brazil_rfb")
def brazil_rfb_source(
    *,
    source_run_id: str,
    manifest_rows: Sequence[dict[str, object]] | None = None,
    snapshot_month: str | None = None,
    snapshot_base_url: str = DEFAULT_BASE_URL,
    download_dir: str | Path | None = None,
    families: Sequence[str] = DEFAULT_FAMILIES,
    session: HttpSession | None = None,
) -> DltResource:
    if manifest_rows is not None:
        return snapshot_files_resource(manifest_rows)
    if snapshot_month is None:
        raise ValueError("snapshot_month is required when manifest_rows is not provided")
    resolved_download_dir = Path(download_dir) if download_dir is not None else Path(
        tempfile.gettempdir()
    ) / "brazil_rfb"
    remote_files = fetch_snapshot_remote_files(
        snapshot_month=snapshot_month,
        base_url=snapshot_base_url,
        families=families,
        session=session,
    )
    rows = download_extract_snapshot_files(
        remote_files=remote_files,
        download_dir=resolved_download_dir,
        source_run_id=source_run_id,
        session=session,
    )
    return snapshot_files_resource(rows)


def brazil_rfb_pipeline(database_path: str | Path) -> Pipeline:
    return dlt.pipeline(
        pipeline_name="brazil_rfb_snapshot_files",
        destination=dlt.destinations.duckdb(str(database_path)),
        dataset_name=tables.DLT_DATASET_NAME,
        dev_mode=False,
    )
```

- [ ] **Step 4: Run source tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_source.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit dlt snapshot manifest source**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/source.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_source.py
git commit -m "Add Brazil RFB snapshot manifest source"
```

### Task 3: DuckDB Raw CSV Staging From dlt Manifest

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`

- [ ] **Step 1: Add failing staging tests**

Append to `test_brazil_rfb_staging.py`:

```python
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_rfb import staging, tables


def _write_manifest(connection: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} (
            family varchar,
            archive_url varchar,
            archive_name varchar,
            archive_sha256 varchar,
            csv_member_name varchar,
            csv_path varchar,
            source_run_id varchar,
            retrieved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        insert into {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE}
        values ('empresas', 'https://example.test/emp.zip', 'emp.zip', 'hash',
                'emp.csv', ?, 'run-1', now())
        """,
        [str(csv_path)],
    )


def test_load_raw_family_uses_latin1_no_header_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empresas.csv"
    csv_path.write_bytes("12345678;CAF\\xc9 LTDA;2062;49;1000,00;01;\\n".encode("latin-1"))
    database_path = tmp_path / "br.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _write_manifest(connection, csv_path)

    count = staging.load_raw_family_from_manifest(
        database_path=database_path,
        family="empresas",
        source_run_id="run-1",
    )

    assert count == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        row = connection.execute(
            f"""
            select cnpj_basico, razao_social, source_file_family, source_archive_url,
                   source_csv_member, source_run_id
            from {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE_BY_FAMILY["empresas"]}
            """
        ).fetchone()

    assert row == (
        "12345678",
        "CAFÉ LTDA",
        "empresas",
        "https://example.test/emp.zip",
        "emp.csv",
        "run-1",
    )


def test_load_raw_family_refuses_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty_empresas.csv"
    csv_path.write_text("")
    database_path = tmp_path / "br.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _write_manifest(connection, csv_path)

    with pytest.raises(ValueError, match="produced no rows"):
        staging.load_raw_family_from_manifest(
            database_path=database_path,
            family="empresas",
            source_run_id="run-1",
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py::test_load_raw_family_uses_latin1_no_header_csv -q
```

Expected: FAIL because `staging.py` does not exist.

- [ ] **Step 3: Implement DuckDB raw staging**

Create `staging.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _list_literal(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_sql_literal(value) for value in values) + "]"


def load_raw_family_from_manifest(
    *,
    database_path: str | Path,
    family: str,
    source_run_id: str,
) -> int:
    if family not in tables.RAW_TABLE_BY_FAMILY:
        raise ValueError(f"unknown Brazil RFB file family: {family}")
    table_name = tables.RAW_TABLE_BY_FAMILY[family]
    column_names = tables.RAW_COLUMNS_BY_FAMILY[family]
    dataset = tables.DLT_DATASET_NAME
    manifest = f"{dataset}.{tables.SNAPSHOT_FILES_TABLE}"
    qualified = f"{dataset}.{table_name}"

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {dataset}")
        manifest_rows = connection.execute(
            f"""
            select csv_path, archive_url, csv_member_name
            from {manifest}
            where family = ? and source_run_id = ?
            order by archive_name, csv_member_name
            """,
            [family, source_run_id],
        ).fetchall()
        if not manifest_rows:
            raise ValueError(f"No Brazil RFB manifest rows found for family {family}")

        csv_paths = tuple(str(row[0]) for row in manifest_rows)
        read_csv_paths = _list_literal(csv_paths)
        read_csv_columns = _list_literal(column_names)
        manifest_values = ", ".join(
            "("
            + ", ".join(
                (
                    _sql_literal(str(row[0])),
                    _sql_literal(family),
                    _sql_literal(str(row[1])),
                    _sql_literal(str(row[2])),
                )
            )
            + ")"
            for row in manifest_rows
        )
        connection.execute(
            f"""
            create or replace table {qualified} as
            with file_manifest(csv_path, source_file_family, source_archive_url, source_csv_member) as (
                values {manifest_values}
            ),
            loaded as (
                select *
                from read_csv(
                    {read_csv_paths},
                    names = {read_csv_columns},
                    header = false,
                    all_varchar = true,
                    delim = ';',
                    quote = '"',
                    escape = '"',
                    encoding = 'latin-1',
                    filename = true
                )
            )
            select
                {", ".join(column_names)},
                fm.source_file_family,
                fm.source_archive_url,
                fm.source_csv_member,
                {_sql_literal(source_run_id)} as source_run_id,
                now() as loaded_at
            from loaded
            join file_manifest fm on loaded.filename = fm.csv_path
            """
        )
        count = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])

    if count == 0:
        raise ValueError(f"Brazil RFB family {family} produced no rows")
    return count


def load_all_raw_families_from_manifest(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
    return {
        family: load_raw_family_from_manifest(
            database_path=database_path,
            family=family,
            source_run_id=source_run_id,
        )
        for family in tables.RAW_TABLE_BY_FAMILY
    }
```

- [ ] **Step 4: Run staging tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit raw staging implementation**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_staging.py
git commit -m "Load Brazil RFB raw CSV families into DuckDB"
```

### Task 4: Dagster Assets For dlt Manifest And Raw Staging

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_assets.py`

- [ ] **Step 1: Write failing asset graph tests**

Create `test_brazil_rfb_assets.py`:

```python
import dagster as dg


def test_brazil_rfb_raw_assets_are_registered_with_single_writer_pool() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_rfb_snapshot_files_duckdb" in keys
    assert "brazil_rfb_raw_files_duckdb" in keys

    raw_asset = repo.assets_defs_by_key[dg.AssetKey("brazil_rfb_raw_files_duckdb")]
    assert raw_asset.op.pool == "brazil_rfb_duckdb"


def test_brazil_rfb_raw_asset_depends_on_snapshot_manifest() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    graph = repo.asset_graph
    parents = {
        parent.path[-1]
        for parent in graph.get_parents(dg.AssetKey("brazil_rfb_raw_files_duckdb"))
    }

    assert parents == {"brazil_rfb_snapshot_files_duckdb"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_assets.py -q
```

Expected: FAIL because assets are not defined.

- [ ] **Step 3: Implement assets**

Replace the temporary `assets.py` content with:

```python
from __future__ import annotations

from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets

from dagster_v3.defs.brazil_rfb import source, staging, tables

GROUP_NAME = "brazil_rfb"
BRAZIL_RFB_DUCKDB_POOL = "brazil_rfb_duckdb"
BRAZIL_RFB_DUCKDB_PATH = Path("data/brazil_rfb_source.duckdb")
BRAZIL_RFB_DOWNLOAD_DIR = Path("data/brazil_rfb_downloads")


class BrazilRfbConfig(dg.Config):
    snapshot_month: str
    snapshot_base_url: str = source.DEFAULT_BASE_URL


class BrazilRfbDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: object) -> dg.AssetSpec:
        return dg.AssetSpec(
            "brazil_rfb_snapshot_files_duckdb",
            group_name=GROUP_NAME,
            kinds={"dlt", "duckdb"},
            description=(
                "Brazil RFB CNPJ monthly snapshot ZIP files downloaded, extracted, "
                "and recorded as a dlt manifest table in DuckDB."
            ),
        )


@dlt_assets(
    dlt_source=source.brazil_rfb_source(
        source_run_id="definition",
        manifest_rows=[],
    ),
    dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DUCKDB_PATH),
    name="brazil_rfb_snapshot_files_duckdb",
    group_name=GROUP_NAME,
    dagster_dlt_translator=BrazilRfbDltTranslator(),
)
def brazil_rfb_snapshot_files_duckdb(
    context: AssetExecutionContext,
    config: BrazilRfbConfig,
    dlt: DagsterDltResource,
):
    yield from dlt.run(
        context=context,
        dlt_source=source.brazil_rfb_source(
            source_run_id=context.run_id,
            snapshot_month=config.snapshot_month,
            snapshot_base_url=config.snapshot_base_url,
            download_dir=BRAZIL_RFB_DOWNLOAD_DIR / config.snapshot_month,
        ),
        dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DUCKDB_PATH),
    )


@dg.asset(
    name="brazil_rfb_raw_files_duckdb",
    deps=[dg.AssetKey("brazil_rfb_snapshot_files_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description="Brazil RFB CNPJ raw CSV file families loaded into DuckDB with read_csv.",
)
def brazil_rfb_raw_files_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = staging.load_all_raw_families_from_manifest(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    context.log.info("Loaded Brazil RFB raw CSV families: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)


defs = dg.Definitions(
    assets=[brazil_rfb_snapshot_files_duckdb, brazil_rfb_raw_files_duckdb],
)
```

- [ ] **Step 4: Run asset tests and definition check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_assets.py -q
uv run dg check defs
```

Expected: pytest PASS and `dg check defs` succeeds.

- [ ] **Step 5: Commit Section 1 assets**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "Add Brazil RFB raw staging assets"
```

---

## Section 2: Normalize Legal Entities And Establishments

### Task 5: Build `br_companies` And `br_establishments` In DuckDB

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`

- [ ] **Step 1: Write failing transform tests**

Create `test_brazil_rfb_transforms.py` with a minimal raw DuckDB fixture:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables, transforms


def _create_raw_tables(database_path: Path) -> None:
    dataset = tables.DLT_DATASET_NAME
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {dataset}")
        connection.execute(
            f"""
            create table {dataset}.empresas_raw as
            select * from (values
                ('12345678', 'ACME LTDA', '2062', '49', '1000,50', '01', ''),
                ('99999999', 'BRANCH ONLY SA', '2054', '10', '2500,00', '05', '')
            ) as t(cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
                   capital_social, porte, ente_federativo_responsavel)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.estabelecimentos_raw as
            select * from (values
                ('12345678', '0001', '90', '1', 'ACME', '02', '20200101', '', '', '',
                 '20200101', '6201501', '6311900,6202300', 'RUA', 'A', '10', '', 'CENTRO',
                 '01001000', 'SP', '7107', '11', '11111111', '', '', '', '', 'info@acme.com.br', '', ''),
                ('99999999', '0002', '91', '2', 'BRANCH', '02', '20200101', '', '', '',
                 '20200202', '6311900', '', 'AV', 'B', '20', '', 'CENTRO',
                 '20000000', 'RJ', '6001', '', '', '', '', '', '', '', '', '')
            ) as t(
                cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
                situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
                nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal,
                cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento,
                bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2,
                ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial
            )
            """
        )
        connection.execute(
            f"""
            create table {dataset}.naturezas_raw as
            select * from (values ('2062', 'Sociedade Empresária Limitada'), ('2054', 'Sociedade Anônima Fechada'))
            as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.municipios_raw as
            select * from (values ('7107', 'SAO PAULO'), ('6001', 'RIO DE JANEIRO'))
            as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.motivos_raw as
            select * from (values ('', ''))
            as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.simples_raw as
            select * from (values ('12345678', 'S', '20200101', '', 'N', '', ''))
            as t(cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
                 opcao_mei, data_opcao_mei, data_exclusao_mei)
            """
        )


def test_build_companies_selects_hq_then_fallback_establishment(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)

    counts = transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )

    assert counts == {"companies": 2, "establishments": 2, "active_companies": 2}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        companies = connection.execute(
            f"""
            select cnpj_basico, headquarters_cnpj, legal_name, trade_name,
                   share_capital_amount_original, company_size_en, status_en,
                   municipality_name
            from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            order by cnpj_basico
            """
        ).fetchall()
        establishments = connection.execute(
            f"""
            select cnpj, cnpj_basico, is_headquarters, primary_cnae_code
            from {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            order by cnpj
            """
        ).fetchall()

    assert companies == [
        ("12345678", "12345678000190", "ACME LTDA", "ACME", 1000.50, "Micro", "Active", "SAO PAULO"),
        ("99999999", "99999999000291", "BRANCH ONLY SA", "BRANCH", 2500.00, "Other", "Active", "RIO DE JANEIRO"),
    ]
    assert establishments == [
        ("12345678000190", "12345678", 1, "6201501"),
        ("99999999000291", "99999999", 0, "6311900"),
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_transforms.py::test_build_companies_selects_hq_then_fallback_establishment -q
```

Expected: FAIL because transforms/tables are missing.

- [ ] **Step 3: Add table constants and transform implementation**

Append to `tables.py`:

```python
COMPANIES_TABLE = "companies"
ESTABLISHMENTS_TABLE = "establishments"

BR_COMPANIES_TABLE_CH = "br_companies"
BR_ESTABLISHMENTS_TABLE_CH = "br_establishments"
QUALIFIED_BR_COMPANIES_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_COMPANIES_TABLE_CH}"
QUALIFIED_BR_ESTABLISHMENTS_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_ESTABLISHMENTS_TABLE_CH}"
```

Create `transforms.py` with helper maps and SQL. Keep the SQL set-based:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables

STATUS_EN_BY_CODE = {"01": "Null", "02": "Active", "03": "Suspended", "04": "Unfit", "08": "Closed"}
COMPANY_SIZE_EN_BY_CODE = {"00": "Not informed", "01": "Micro", "03": "Small", "05": "Other"}


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _case_map(column: str, mapping: dict[str, str]) -> str:
    cases = " ".join(
        f"when {_sql_literal(code)} then {_sql_literal(label)}"
        for code, label in mapping.items()
    )
    return f"case {column} {cases} else '' end"


def build_brazil_rfb_companies_and_establishments(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    status_en = _case_map("e.situacao_cadastral", STATUS_EN_BY_CODE)
    size_en = _case_map("emp.porte", COMPANY_SIZE_EN_BY_CODE)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"""
            create or replace table {dataset}.{tables.ESTABLISHMENTS_TABLE} as
            select
                concat(cnpj_basico, cnpj_ordem, cnpj_dv) as cnpj,
                cnpj_basico,
                cnpj_ordem,
                cnpj_dv,
                case when identificador_matriz_filial = '1' then 1 else 0 end as is_headquarters,
                coalesce(trim(nome_fantasia), '') as trade_name,
                coalesce(situacao_cadastral, '') as status_code,
                {status_en} as status_en,
                try_strptime(nullif(data_situacao_cadastral, ''), '%Y%m%d')::date as status_date,
                coalesce(motivo_situacao_cadastral, '') as status_reason_code,
                try_strptime(nullif(data_inicio_atividade, ''), '%Y%m%d')::date as activity_start_date,
                coalesce(cnae_fiscal_principal, '') as primary_cnae_code,
                coalesce(cnae_fiscal_secundaria, '') as secondary_cnae_codes,
                coalesce(tipo_logradouro, '') as street_type,
                coalesce(logradouro, '') as street_name,
                coalesce(numero, '') as street_number,
                coalesce(complemento, '') as address_complement,
                coalesce(bairro, '') as district,
                coalesce(cep, '') as postal_code,
                coalesce(uf, '') as state,
                coalesce(municipio, '') as municipality_code,
                coalesce(m.description_pt, '') as municipality_name,
                {_sql_literal(source_run_id)} as source_run_id,
                now() as resolved_at
            from {dataset}.{tables.RAW_TABLE_BY_FAMILY["estabelecimentos"]} e
            left join {dataset}.{tables.RAW_TABLE_BY_FAMILY["municipios"]} m
                on m.code = e.municipio
            """
        )
        connection.execute(
            f"""
            create or replace table {dataset}.{tables.COMPANIES_TABLE} as
            with ranked_establishments as (
                select
                    *,
                    row_number() over (
                        partition by cnpj_basico
                        order by is_headquarters desc, (status_code = '02') desc, cnpj_ordem, cnpj
                    ) as rn
                from {dataset}.{tables.ESTABLISHMENTS_TABLE}
            ),
            picked as (
                select * from ranked_establishments where rn = 1
            ),
            simples_current as (
                select cnpj_basico, opcao_simples, opcao_mei
                from {dataset}.{tables.RAW_TABLE_BY_FAMILY["simples"]}
            )
            select
                emp.cnpj_basico,
                p.cnpj as headquarters_cnpj,
                coalesce(trim(emp.razao_social), '') as legal_name,
                coalesce(p.trade_name, '') as trade_name,
                coalesce(emp.natureza_juridica, '') as legal_nature_code,
                coalesce(n.description_pt, '') as legal_nature_description_pt,
                coalesce(emp.porte, '') as company_size_code,
                {size_en} as company_size_en,
                try_cast(replace(replace(emp.capital_social, '.', ''), ',', '.') as decimal(18, 2)) as share_capital_amount_original,
                p.status_code,
                p.status_en,
                case when p.status_code = '02' then 1 else 0 end as is_active,
                p.status_date,
                p.activity_start_date,
                p.street_type,
                p.street_name,
                p.street_number,
                p.address_complement,
                p.district,
                p.postal_code,
                p.state,
                p.municipality_code,
                p.municipality_name,
                case when s.opcao_simples = 'S' then 1 else 0 end as is_simples,
                case when s.opcao_mei = 'S' then 1 else 0 end as is_mei,
                {_sql_literal(source_run_id)} as source_run_id,
                now() as resolved_at
            from {dataset}.{tables.RAW_TABLE_BY_FAMILY["empresas"]} emp
            left join picked p on p.cnpj_basico = emp.cnpj_basico
            left join {dataset}.{tables.RAW_TABLE_BY_FAMILY["naturezas"]} n
                on n.code = emp.natureza_juridica
            left join simples_current s on s.cnpj_basico = emp.cnpj_basico
            """
        )
        companies = int(connection.execute(f"select count(*) from {dataset}.{tables.COMPANIES_TABLE}").fetchone()[0])
        establishments = int(connection.execute(f"select count(*) from {dataset}.{tables.ESTABLISHMENTS_TABLE}").fetchone()[0])
        active_companies = int(connection.execute(
            f"select count(*) from {dataset}.{tables.COMPANIES_TABLE} where is_active = 1"
        ).fetchone()[0])
    if companies == 0:
        raise ValueError("Brazil RFB company transform produced no rows")
    return {"companies": companies, "establishments": establishments, "active_companies": active_companies}
```

- [ ] **Step 4: Add asset wrappers**

Append to `assets.py`:

```python
from dagster_v3.defs.brazil_rfb import transforms


@dg.asset(
    name="brazil_rfb_companies_duckdb",
    deps=[dg.AssetKey("brazil_rfb_raw_files_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    description="Brazil RFB legal entities and establishments normalized in DuckDB.",
)
def brazil_rfb_companies_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = transforms.build_brazil_rfb_companies_and_establishments(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    return dg.MaterializeResult(metadata=counts)
```

Add `brazil_rfb_companies_duckdb` to the `assets` list in the existing
`dg.Definitions(...)` call in `assets.py`.

- [ ] **Step 5: Run transform tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_transforms.py -q
uv run pytest tests/test_brazil_rfb_assets.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit transform milestone**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "Normalize Brazil RFB companies and establishments"
```

---

## Section 3: Contacts And Company Domains

### Task 6: Build Contacts And Email-Derived Domains

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/company_domains.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_contacts.py`

- [ ] **Step 1: Write failing contact/domain tests**

Create `test_brazil_rfb_contacts.py` with a small `establishments` DuckDB table:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import company_domains, contacts, tables


def _create_establishments(database_path: Path) -> None:
    dataset = tables.DLT_DATASET_NAME
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {dataset}")
        connection.execute(
            f"""
            create table {dataset}.{tables.ESTABLISHMENTS_TABLE} as
            select * from (values
                ('12345678000190', '12345678', '0001', '90', 1, 'ACME', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '', 'RUA', 'A',
                 '10', '', 'CENTRO', '01001000', 'SP', '7107', 'SAO PAULO', '11',
                 '11111111', '', '', '', '', 'info@acme.com.br', '', '', 'run-1', now()),
                ('22222222000190', '22222222', '0001', '90', 1, 'BETA', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '', 'RUA', 'B',
                 '10', '', 'CENTRO', '01001000', 'SP', '7107', 'SAO PAULO', '11',
                 '22222222', '', '', '', '', 'owner@gmail.com', '', '', 'run-1', now()),
                ('33333333000190', '33333333', '0001', '90', 1, 'GAMMA', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '', 'RUA', 'C',
                 '10', '', 'CENTRO', '01001000', 'SP', '7107', 'SAO PAULO', '',
                 '', '', '', '', '', 'finance@shared.com.br', '', '', 'run-1', now()),
                ('44444444000190', '44444444', '0001', '90', 1, 'DELTA', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '', 'RUA', 'D',
                 '10', '', 'CENTRO', '01001000', 'SP', '7107', 'SAO PAULO', '',
                 '', '', '', '', '', 'billing@shared.com.br', '', '', 'run-1', now())
            ) as t(
                cnpj, cnpj_basico, cnpj_ordem, cnpj_dv, is_headquarters, trade_name,
                status_code, status_en, status_date, status_reason_code, activity_start_date,
                primary_cnae_code, secondary_cnae_codes, street_type, street_name,
                street_number, address_complement, district, postal_code, state,
                municipality_code, municipality_name, ddd_1, telefone_1, ddd_2, telefone_2,
                ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
                source_run_id, resolved_at
            )
            """
        )


def test_contacts_extract_phone_email_and_unique_email_domain(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_establishments(database_path)

    counts = contacts.build_brazil_rfb_contacts(database_path=database_path, source_run_id="run-2")

    assert counts["contacts"] == 6
    assert counts["email_domains"] == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select cnpj_basico, contact_type, contact_value, domain, domain_source
            from {tables.DLT_DATASET_NAME}.{tables.CONTACTS_TABLE}
            order by cnpj_basico, contact_type
            """
        ).fetchall()

    assert ("12345678", "email", "info@acme.com.br", "acme.com.br", "email") in rows
    assert ("22222222", "email", "owner@gmail.com", "", "") in rows
    assert ("33333333", "email", "finance@shared.com.br", "", "") in rows
    assert ("44444444", "email", "billing@shared.com.br", "", "") in rows
    assert ("12345678", "phone", "+55 11 11111111", "", "") in rows


def test_company_domains_dedupes_domains(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_establishments(database_path)
    contacts.build_brazil_rfb_contacts(database_path=database_path, source_run_id="run-2")

    counts = company_domains.build_brazil_rfb_company_domains(
        database_path=database_path,
        source_run_id="run-2",
    )

    assert counts == {"domains": 1, "email_domains": 1, "companies": 1}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_contacts.py -q
```

Expected: FAIL because contacts/company_domains do not exist.

- [ ] **Step 3: Implement contacts and domains**

Append to `tables.py`:

```python
CONTACTS_TABLE = "company_contacts"
COMPANY_DOMAINS_TABLE = "company_domains"
```

Create `contacts.py` following the Estonia email-domain uniqueness rule:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables

EMAIL_PROVIDER_DENYLIST = frozenset({"gmail.com", "hotmail.com", "outlook.com", "yahoo.com"})
EMAIL_DOMAIN_MAX_COMPANIES = 1


def _denylist_sql() -> str:
    return ", ".join("'" + item.replace("'", "''") + "'" for item in sorted(EMAIL_PROVIDER_DENYLIST))


def build_brazil_rfb_contacts(*, database_path: str | Path, source_run_id: str) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    source = f"{dataset}.{tables.ESTABLISHMENTS_TABLE}"
    target = f"{dataset}.{tables.CONTACTS_TABLE}"
    denylist = _denylist_sql()
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"""
            create or replace table {target} as
            with unpivoted as (
                select cnpj_basico, cnpj, 'email' as contact_type, 'Email' as contact_type_en,
                       lower(trim(correio_eletronico)) as contact_value
                from {source}
                where coalesce(trim(correio_eletronico), '') <> ''
                union all
                select cnpj_basico, cnpj, 'phone' as contact_type, 'Phone' as contact_type_en,
                       trim(concat('+55 ', ddd_1, ' ', telefone_1)) as contact_value
                from {source}
                where coalesce(trim(telefone_1), '') <> ''
                union all
                select cnpj_basico, cnpj, 'phone' as contact_type, 'Phone' as contact_type_en,
                       trim(concat('+55 ', ddd_2, ' ', telefone_2)) as contact_value
                from {source}
                where coalesce(trim(telefone_2), '') <> ''
                union all
                select cnpj_basico, cnpj, 'fax' as contact_type, 'Fax' as contact_type_en,
                       trim(concat('+55 ', ddd_fax, ' ', fax)) as contact_value
                from {source}
                where coalesce(trim(fax), '') <> ''
            ),
            email_enriched as (
                select *,
                       case when contact_type = 'email' and contains(contact_value, '@')
                            then nullif(lower(trim(regexp_extract(contact_value, '[^@]+$'))), '')
                            else '' end as email_domain
                from unpivoted
            ),
            email_counts as (
                select email_domain, count(distinct cnpj_basico) as company_count
                from email_enriched
                where email_domain <> ''
                group by email_domain
            )
            select
                c.cnpj_basico,
                c.cnpj,
                c.contact_type,
                c.contact_type_en,
                c.contact_value,
                case when c.email_domain <> ''
                          and c.email_domain not in ({denylist})
                          and ec.company_count <= {EMAIL_DOMAIN_MAX_COMPANIES}
                     then c.email_domain else '' end as domain,
                case when c.email_domain <> ''
                          and c.email_domain not in ({denylist})
                          and ec.company_count <= {EMAIL_DOMAIN_MAX_COMPANIES}
                     then 'email' else '' end as domain_source,
                1 as is_current,
                {_sql_literal(source_run_id)} as source_run_id,
                now() as resolved_at
            from email_enriched c
            left join email_counts ec on ec.email_domain = c.email_domain
            """
        )
        contacts = int(connection.execute(f"select count(*) from {target}").fetchone()[0])
        email_domains = int(connection.execute(
            f"select count(*) from {target} where domain_source = 'email'"
        ).fetchone()[0])
    if contacts == 0:
        raise ValueError("Brazil RFB contacts transform produced no rows")
    return {"contacts": contacts, "email_domains": email_domains}


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
```

Create `company_domains.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_brazil_rfb_company_domains(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    source = f"{dataset}.{tables.CONTACTS_TABLE}"
    target = f"{dataset}.{tables.COMPANY_DOMAINS_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"""
            create or replace table {target} as
            with picked as (
                select *,
                       row_number() over (
                           partition by cnpj_basico, domain
                           order by is_current desc, contact_value
                       ) as rn
                from {source}
                where domain <> ''
            )
            select
                cnpj_basico,
                domain,
                domain_source,
                '' as website_url,
                '' as website_normalized_url,
                '' as website_host,
                is_current,
                1 as is_primary,
                {_sql_literal(source_run_id)} as source_run_id,
                now() as resolved_at
            from picked
            where rn = 1
            """
        )
        domains = int(connection.execute(f"select count(*) from {target}").fetchone()[0])
        email_domains = int(connection.execute(
            f"select count(*) from {target} where domain_source = 'email'"
        ).fetchone()[0])
        companies = int(connection.execute(
            f"select count(distinct cnpj_basico) from {target}"
        ).fetchone()[0])
    return {"domains": domains, "email_domains": email_domains, "companies": companies}
```

- [ ] **Step 4: Add contact/domain assets**

Append to `assets.py`:

```python
from dagster_v3.defs.brazil_rfb import company_domains, contacts


@dg.asset(
    name="brazil_rfb_contacts_duckdb",
    deps=[dg.AssetKey("brazil_rfb_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
)
def brazil_rfb_contacts_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = contacts.build_brazil_rfb_contacts(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="brazil_rfb_company_domains_duckdb",
    deps=[dg.AssetKey("brazil_rfb_contacts_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
)
def brazil_rfb_company_domains_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = company_domains.build_brazil_rfb_company_domains(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    return dg.MaterializeResult(metadata=counts)
```

Add `brazil_rfb_contacts_duckdb` and `brazil_rfb_company_domains_duckdb` to the
`assets` list in the existing `dg.Definitions(...)` call in `assets.py`.

- [ ] **Step 5: Run tests and commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_contacts.py tests/test_brazil_rfb_assets.py -q

cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/company_domains.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_contacts.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "Build Brazil RFB contacts and company domains"
```

---

## Section 4: CNAE To NACE Industries

### Task 7: Build `br_industries` From Establishment CNAEs

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/industries.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_industries.py`

- [ ] **Step 1: Write failing industry mapping test**

Create `test_brazil_rfb_industries.py`:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import industries, tables


def test_industries_keep_many_to_many_cnae_to_nace_edges(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    dataset = tables.DLT_DATASET_NAME
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {dataset}")
        connection.execute(
            f"""
            create table {dataset}.{tables.ESTABLISHMENTS_TABLE} as
            select * from (values
                ('12345678000190', '12345678', '0001', '90', 1, 'ACME', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '6311900', 'RUA', 'A',
                 '10', '', 'CENTRO', '01001000', 'SP', '7107', 'SAO PAULO', '11',
                 '11111111', '', '', '', '', 'info@acme.com.br', '', '', 'run-1', now())
            ) as t(
                cnpj, cnpj_basico, cnpj_ordem, cnpj_dv, is_headquarters, trade_name,
                status_code, status_en, status_date, status_reason_code, activity_start_date,
                primary_cnae_code, secondary_cnae_codes, street_type, street_name,
                street_number, address_complement, district, postal_code, state,
                municipality_code, municipality_name, ddd_1, telefone_1, ddd_2, telefone_2,
                ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
                source_run_id, resolved_at
            )
            """
        )
        connection.execute(
            f"""
            create table {dataset}.{tables.RAW_TABLE_BY_FAMILY["cnaes"]} as
            select * from (values
                ('6201501', 'Desenvolvimento de programas de computador sob encomenda'),
                ('6311900', 'Tratamento de dados')
            ) as t(code, description_pt)
            """
        )
        connection.execute(
            """
            create table br_cnae_to_nace as
            select * from (values
                ('CNAE_2_0', '6201-5/01', '6201501', 'Software', 'Software',
                 'NACE_REV_2', '62.01', '6201', 'Computer programming activities',
                 'fixture', 'https://example.test', 'hash', 'run', now()),
                ('CNAE_2_0', '6201-5/01', '6201501', 'Software', 'Software',
                 'NACE_REV_2', '62.02', '6202', 'Computer consultancy activities',
                 'fixture', 'https://example.test', 'hash', 'run', now())
            ) as t(cnae_version, cnae_code, cnae_normalized_code, cnae_description_pt,
                   cnae_description_en, nace_revision, nace_code, nace_normalized_code,
                   nace_description_en, mapping_source, source_url, source_payload_hash,
                   source_run_id, pulled_at)
            """
        )

    counts = industries.build_brazil_rfb_industries(
        database_path=database_path,
        source_run_id="run-2",
        cnae_to_nace_table="br_cnae_to_nace",
    )

    assert counts == {"industry_rows": 3, "cnae_codes": 2, "mapped_rows": 2, "unmapped_rows": 1}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select source_industry_code, nace_code, nace_mapping_status, is_primary
            from {dataset}.{tables.INDUSTRIES_TABLE}
            order by source_industry_code, nace_code
            """
        ).fetchall()

    assert rows == [
        ("6201501", "62.01", "mapped", 1),
        ("6201501", "62.02", "mapped", 1),
        ("6311900", "", "unmapped", 0),
    ]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_industries.py -q
```

Expected: FAIL because `industries.py` does not exist.

- [ ] **Step 3: Implement industries**

Append to `tables.py`:

```python
INDUSTRIES_TABLE = "industries"
```

Create `industries.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import tables


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_brazil_rfb_industries(
    *,
    database_path: str | Path,
    source_run_id: str,
    cnae_to_nace_table: str = "corpscout.br_cnae_to_nace",
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    establishments = f"{dataset}.{tables.ESTABLISHMENTS_TABLE}"
    cnaes = f"{dataset}.{tables.RAW_TABLE_BY_FAMILY['cnaes']}"
    target = f"{dataset}.{tables.INDUSTRIES_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"""
            create or replace table {target} as
            with source_codes as (
                select cnpj_basico, primary_cnae_code as source_industry_code, 1 as is_primary
                from {establishments}
                where coalesce(primary_cnae_code, '') <> ''
                union all
                select
                    cnpj_basico,
                    trim(code.value) as source_industry_code,
                    0 as is_primary
                from {establishments},
                     unnest(str_split(coalesce(secondary_cnae_codes, ''), ',')) as code(value)
                where trim(code.value) <> ''
            ),
            deduped as (
                select
                    cnpj_basico,
                    source_industry_code,
                    max(is_primary) as is_primary,
                    count(*) as establishment_count
                from source_codes
                group by cnpj_basico, source_industry_code
            )
            select
                d.cnpj_basico,
                d.source_industry_code,
                'CNAE_2_0' as source_industry_code_set,
                coalesce(c.description_pt, '') as description_original,
                'pt' as description_language,
                coalesce(m.cnae_description_en, '') as description_en,
                cast(null as timestamp) as description_translated_at,
                '' as description_translation_provider,
                '' as description_translation_model,
                coalesce(m.nace_revision, '') as nace_revision,
                coalesce(m.nace_code, '') as nace_code,
                coalesce(m.nace_normalized_code, '') as nace_normalized_code,
                case when m.nace_code is null then '' else 'br_cnae_to_nace_fixture' end as nace_mapping_method,
                case when m.nace_code is null then 'unmapped' else 'mapped' end as nace_mapping_status,
                d.is_primary,
                d.establishment_count,
                'brazil_rfb' as source_system,
                {_sql_literal(source_run_id)} as source_run_id,
                concat(d.cnpj_basico, ':', d.source_industry_code) as source_record_id,
                now() as resolved_at
            from deduped d
            left join {cnaes} c on c.code = d.source_industry_code
            left join {cnae_to_nace_table} m
                on m.cnae_normalized_code = d.source_industry_code
            """
        )
        industry_rows = int(connection.execute(f"select count(*) from {target}").fetchone()[0])
        cnae_codes = int(connection.execute(f"select count(distinct source_industry_code) from {target}").fetchone()[0])
        mapped_rows = int(connection.execute(f"select count(*) from {target} where nace_mapping_status = 'mapped'").fetchone()[0])
        unmapped_rows = int(connection.execute(f"select count(*) from {target} where nace_mapping_status = 'unmapped'").fetchone()[0])
    return {
        "industry_rows": industry_rows,
        "cnae_codes": cnae_codes,
        "mapped_rows": mapped_rows,
        "unmapped_rows": unmapped_rows,
    }
```

- [ ] **Step 4: Add industry asset**

Append to `assets.py`:

```python
from dagster_v3.defs.brazil_rfb import industries


@dg.asset(
    name="brazil_rfb_industries_duckdb",
    deps=[
        dg.AssetKey("brazil_rfb_companies_duckdb"),
        dg.AssetKey("brazil_cnae_to_nace_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
)
def brazil_rfb_industries_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    counts = industries.build_brazil_rfb_industries(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        source_run_id=context.run_id,
    )
    return dg.MaterializeResult(metadata=counts)
```

Add `brazil_rfb_industries_duckdb` to the `assets` list in the existing
`dg.Definitions(...)` call in `assets.py`.

- [ ] **Step 5: Run tests and commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_industries.py tests/test_brazil_rfb_assets.py -q

cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/industries.py \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_industries.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "Map Brazil RFB CNAE industries to NACE"
```

---

## Section 5: ClickHouse Tables And Exports

### Task 8: Add Migration And Export Helpers

**Files:**
- Create: `corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.up.sql`
- Create: `corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`

- [ ] **Step 1: Add failing migration registration test**

Modify `EXPECTED_MIGRATIONS` in `test_clickhouse_migrations.py` to include:

```python
    "000053_corpscout_br_rfb_registry",
```

Add a focused test:

```python
def test_brazil_rfb_registry_migration_covers_exported_tables() -> None:
    from dagster_v3.defs.brazil_rfb import tables as br_tables

    sql = _migration_sql("000053_corpscout_br_rfb_registry.up.sql")
    down_sql = _migration_sql("000053_corpscout_br_rfb_registry.down.sql")

    for table_name in (
        br_tables.QUALIFIED_BR_COMPANIES_TABLE,
        br_tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE,
        br_tables.QUALIFIED_BR_COMPANY_CONTACTS_TABLE,
        br_tables.QUALIFIED_BR_COMPANY_DOMAINS_TABLE,
        br_tables.QUALIFIED_BR_INDUSTRIES_TABLE,
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        assert f"DROP TABLE IF EXISTS {table_name}" in down_sql
```

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py::test_brazil_rfb_registry_migration_covers_exported_tables -q
```

Expected: FAIL because migration/table constants are missing.

- [ ] **Step 2: Add ClickHouse table constants**

Append to `tables.py`:

```python
BR_COMPANY_CONTACTS_TABLE_CH = "br_company_contacts"
BR_COMPANY_DOMAINS_TABLE_CH = "br_company_domains"
BR_INDUSTRIES_TABLE_CH = "br_industries"

QUALIFIED_BR_COMPANY_CONTACTS_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_COMPANY_CONTACTS_TABLE_CH}"
QUALIFIED_BR_COMPANY_DOMAINS_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_COMPANY_DOMAINS_TABLE_CH}"
QUALIFIED_BR_INDUSTRIES_TABLE = f"{BRAZIL_RFB_DATABASE}.{BR_INDUSTRIES_TABLE_CH}"
```

- [ ] **Step 3: Create migration**

Create `000053_corpscout_br_rfb_registry.up.sql` with all five tables. Use `ReplacingMergeTree(resolved_at)` and non-nullable sort keys:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_companies
(
    cnpj_basico String,
    headquarters_cnpj String,
    legal_name String,
    trade_name String,
    legal_nature_code String,
    legal_nature_description_pt String,
    company_size_code String,
    company_size_en String,
    share_capital_amount_original Nullable(Decimal(18, 2)),
    status_code String,
    status_en String,
    is_active UInt8,
    status_date Nullable(Date),
    activity_start_date Nullable(Date),
    street_type String,
    street_name String,
    street_number String,
    address_complement String,
    district String,
    postal_code String,
    state String,
    municipality_code String,
    municipality_name String,
    is_simples UInt8,
    is_mei UInt8,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico);

CREATE TABLE IF NOT EXISTS corpscout.br_establishments
(
    cnpj String,
    cnpj_basico String,
    cnpj_ordem String,
    cnpj_dv String,
    is_headquarters UInt8,
    trade_name String,
    status_code String,
    status_en String,
    status_date Nullable(Date),
    status_reason_code String,
    activity_start_date Nullable(Date),
    primary_cnae_code String,
    secondary_cnae_codes String,
    street_type String,
    street_name String,
    street_number String,
    address_complement String,
    district String,
    postal_code String,
    state String,
    municipality_code String,
    municipality_name String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, cnpj);

CREATE TABLE IF NOT EXISTS corpscout.br_company_contacts
(
    cnpj_basico String,
    cnpj String,
    contact_type LowCardinality(String),
    contact_type_en String,
    contact_value String,
    domain String,
    domain_source LowCardinality(String),
    is_current UInt8,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, contact_type, contact_value, cnpj);

CREATE TABLE IF NOT EXISTS corpscout.br_company_domains
(
    cnpj_basico String,
    domain String,
    domain_source LowCardinality(String),
    website_url String,
    website_normalized_url String,
    website_host String,
    is_current UInt8,
    is_primary UInt8,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, domain);

CREATE TABLE IF NOT EXISTS corpscout.br_industries
(
    cnpj_basico String,
    source_industry_code String,
    source_industry_code_set LowCardinality(String),
    description_original String,
    description_language LowCardinality(String),
    description_en String,
    description_translated_at Nullable(DateTime64(3, 'UTC')),
    description_translation_provider String,
    description_translation_model String,
    nace_revision LowCardinality(String),
    nace_code String,
    nace_normalized_code String,
    nace_mapping_method LowCardinality(String),
    nace_mapping_status LowCardinality(String),
    is_primary UInt8,
    establishment_count UInt64,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, source_industry_code, nace_revision, nace_normalized_code);
```

Create down migration:

```sql
DROP TABLE IF EXISTS corpscout.br_industries;
DROP TABLE IF EXISTS corpscout.br_company_domains;
DROP TABLE IF EXISTS corpscout.br_company_contacts;
DROP TABLE IF EXISTS corpscout.br_establishments;
DROP TABLE IF EXISTS corpscout.br_companies;
```

- [ ] **Step 4: Add export helpers**

Create `clickhouse.py`:

```python
from __future__ import annotations

from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_rfb import tables
from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse


def export_brazil_rfb_table(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
) -> int:
    return export_duckdb_table_to_clickhouse(
        database_path=database_path,
        duckdb_table=f"{tables.DLT_DATASET_NAME}.{duckdb_table}",
        clickhouse=clickhouse,
        clickhouse_table=clickhouse_table,
        columns=columns,
    )
```

Add export column tuples in `tables.py` for each transformed table and use them in asset wrappers.

- [ ] **Step 5: Add ClickHouse assets**

Add assets after each DuckDB asset:

```python
@dg.asset(
    deps=[dg.AssetKey("brazil_rfb_companies_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=BRAZIL_RFB_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_BR_COMPANIES_TABLE},
)
def brazil_rfb_clickhouse_companies(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = clickhouse_exports.export_brazil_rfb_table(
        database_path=BRAZIL_RFB_DUCKDB_PATH,
        clickhouse=clickhouse,
        duckdb_table=tables.COMPANIES_TABLE,
        clickhouse_table=tables.QUALIFIED_BR_COMPANIES_TABLE,
        columns=tables.BR_COMPANIES_EXPORT_COLUMNS,
    )
    return dg.MaterializeResult(metadata={"rows": rows, "table": tables.QUALIFIED_BR_COMPANIES_TABLE})
```

Repeat the same pattern for establishments, contacts, company domains, and industries. Import the module as:

```python
from dagster_v3.defs.brazil_rfb import clickhouse as clickhouse_exports
from dagster_clickhouse import ClickhouseResource
```

- [ ] **Step 6: Run tests and migrate**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py tests/test_brazil_rfb_*.py -q
uv run dg check defs

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: tests pass; migrate applies `53/u corpscout_br_rfb_registry`.

- [ ] **Step 7: Commit ClickHouse export milestone**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.up.sql \
  corpscout/clickhouse/migrations/000053_corpscout_br_rfb_registry.down.sql \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb \
  corpscout/dagster_v3/tests/test_brazil_rfb_*.py \
  corpscout/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "Export Brazil RFB registry tables to ClickHouse"
```

---

## Section 6: Shared Domain Graph, Jobs, Schedules, And Validation

### Task 9: Wire Brazil Domains Into Shared Domain Assets

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/domains/assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/domains/tables.py`
- Test: existing domain tests under `corpscout/dagster_v3/tests/`.

- [ ] **Step 1: Write failing domain branch test**

Find the current domain test:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
rg -n "company_website_domains|domain_source|estonia" tests
```

Add an assertion beside existing source branches:

```python
assert "br_company_domains" in sql
assert "'BR'" in sql or "'brazil_rfb'" in sql
```

- [ ] **Step 2: Add Brazil branch to the domain union**

Follow the Estonia branch shape. The selected columns should map:

```sql
select
    'BR' as country_iso2,
    'brazil_rfb' as source_slug,
    cnpj_basico as company_id,
    domain,
    domain_source,
    website_url,
    website_normalized_url,
    website_host,
    is_primary,
    source_run_id,
    resolved_at
from corpscout.br_company_domains
```

- [ ] **Step 3: Run domain tests and commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_domains*.py -q
uv run dg check defs

cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/domains \
  corpscout/dagster_v3/tests
git commit -m "Add Brazil RFB domains to shared domain graph"
```

### Task 10: Add Jobs And Schedules

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Test: `corpscout/dagster_v3/tests/test_brazil_rfb_assets.py`

- [ ] **Step 1: Add failing job/schedule tests**

Append:

```python
def test_brazil_rfb_jobs_and_schedule_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("brazil_rfb_register_schedule")
    assert schedule.cron_schedule == "0 8 12 * *"
    assert schedule.job.name == "brazil_rfb_register_job"

    keys = {
        key.path[-1]
        for key in repo.get_job("brazil_rfb_full_refresh_job").asset_layer.executable_asset_keys
    }
    assert "brazil_rfb_raw_files_duckdb" in keys
    assert "brazil_rfb_clickhouse_companies" in keys
    assert "brazil_rfb_clickhouse_industries" in keys
```

- [ ] **Step 2: Implement jobs/schedule**

Append to `assets.py`:

```python
brazil_rfb_register_job = dg.define_asset_job(
    "brazil_rfb_register_job",
    selection=dg.AssetSelection.assets(
        "brazil_rfb_clickhouse_companies",
        "brazil_rfb_clickhouse_establishments",
        "brazil_rfb_clickhouse_company_contacts",
        "brazil_rfb_clickhouse_company_domains",
        "brazil_rfb_clickhouse_industries",
    ).upstream(),
)

brazil_rfb_register_schedule = dg.ScheduleDefinition(
    name="brazil_rfb_register_schedule",
    job=brazil_rfb_register_job,
    cron_schedule="0 8 12 * *",
    execution_timezone="Europe/Belgrade",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

brazil_rfb_full_refresh_job = dg.define_asset_job(
    "brazil_rfb_full_refresh_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
)

defs = dg.Definitions(
    assets=[
        brazil_rfb_snapshot_files_duckdb,
        brazil_rfb_raw_files_duckdb,
        brazil_rfb_companies_duckdb,
        brazil_rfb_contacts_duckdb,
        brazil_rfb_company_domains_duckdb,
        brazil_rfb_industries_duckdb,
        brazil_rfb_clickhouse_companies,
        brazil_rfb_clickhouse_establishments,
        brazil_rfb_clickhouse_company_contacts,
        brazil_rfb_clickhouse_company_domains,
        brazil_rfb_clickhouse_industries,
    ],
    jobs=[brazil_rfb_register_job, brazil_rfb_full_refresh_job],
    schedules=[brazil_rfb_register_schedule],
)
```

- [ ] **Step 3: Run checks and commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_assets.py -q
uv run dg check defs

cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "Add Brazil RFB jobs and schedule"
```

### Task 11: End-To-End Verification

**Files:**
- Modify docs only if verification uncovers design deviations:
  `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/docs/brazil_rfb-design.md`

- [ ] **Step 1: Run focused unit tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_source.py \
  tests/test_brazil_rfb_staging.py \
  tests/test_brazil_rfb_transforms.py \
  tests/test_brazil_rfb_contacts.py \
  tests/test_brazil_rfb_industries.py \
  tests/test_brazil_rfb_assets.py \
  tests/test_clickhouse_migrations.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run Dagster definition check**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.

- [ ] **Step 3: Run ClickHouse migrations**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: `no change` if already applied, or `53/u corpscout_br_rfb_registry`.

- [ ] **Step 4: Run a bounded local materialization**

Use a small fixture snapshot URL or local HTTP server that exposes a directory containing one ZIP per family. Then run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg launch --assets brazil_rfb_raw_files_duckdb
```

Expected: raw tables land in `data/brazil_rfb_source.duckdb`, with nonzero row counts in materialization metadata.

- [ ] **Step 5: Run full source materialization after raw validation**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg launch --assets brazil_rfb_clickhouse_companies,brazil_rfb_clickhouse_establishments,brazil_rfb_clickhouse_company_contacts,brazil_rfb_clickhouse_company_domains,brazil_rfb_clickhouse_industries
```

Expected: all five ClickHouse assets materialize and report nonzero rows for a real snapshot. `br_industries` reports mapped/unmapped counts.

- [ ] **Step 6: Spot-check ClickHouse**

Run with the local ClickHouse connection helper already used elsewhere in the repo:

```sql
SELECT count() FROM corpscout.br_companies;
SELECT count() FROM corpscout.br_establishments;
SELECT count() FROM corpscout.br_company_contacts;
SELECT count() FROM corpscout.br_company_domains;
SELECT nace_mapping_status, count() FROM corpscout.br_industries GROUP BY nace_mapping_status;
```

Expected: counts are nonzero for company/establishment/contact tables; domain count depends on email uniqueness; industry count is nonzero and mapping coverage is visible.

- [ ] **Step 7: Final commit if verification required doc updates**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
git diff --check
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/docs/brazil_rfb-design.md
git commit -m "Document Brazil RFB implementation findings"
```

Skip this commit when there are no doc changes.

---

## Self-Review

Spec coverage:
- Source overview and module layout: Tasks 1, 4.
- CSV pull, dlt boundary, DuckDB staging: Tasks 2, 3, 4.
- Legal entity and establishment tables: Task 5.
- Contacts and domains: Task 6 and Task 9.
- CNAE to NACE industries: Task 7.
- ClickHouse migration/export: Task 8.
- Scheduling and verification: Tasks 10 and 11.
- `Socios` and CVM exclusions: Scope section keeps both out of this plan.

Placeholder scan:
- The plan avoids placeholder markers and unspecified function names.
- The only conditional instruction is migration numbering through the explicit current next migration, `000053`.

Type consistency:
- `DLT_DATASET_NAME`, raw table names, and transform table names are introduced in `tables.py` before use.
- Asset names used in dependencies match the asset definitions in the plan.
- Test function names match the commands shown under each task.
