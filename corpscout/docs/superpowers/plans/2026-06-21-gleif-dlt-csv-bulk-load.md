# GLEIF dlt CSV Bulk Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the slow Python JSON-to-DuckDB GLEIF bootstrap path with CSV ZIP raw files loaded into DuckDB through dlt, then normalized with DuckDB SQL.

**Architecture:** GLEIF raw assets download `latest.csv.zip` files to object storage. The DuckDB state asset extracts each CSV ZIP to a temp directory, uses dlt filesystem CSV resources to load raw tables into the same DuckDB file, and builds normalized `gleif.*` tables through set-based SQL. The final ClickHouse schema and asset graph stay unchanged.

**Tech Stack:** Dagster assets, dlt filesystem CSV source, dlt DuckDB destination, pandas for dlt CSV reading, DuckDB SQL, ClickHouse publication through existing helper, pytest.

---

## File Structure

- Modify: `corpscout/dagster_v3/pyproject.toml`
  - Add `pandas>=3.0.0`, required by `dlt.sources.filesystem.read_csv`.
- Modify: `corpscout/dagster_v3/uv.lock`
  - Regenerate with `uv lock`.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
  - Change default GLEIF raw format to CSV.
  - Add `file_format` to each manifest file entry.
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`
  - Extract single CSV members from ZIP files.
  - Load extracted CSVs into DuckDB raw tables through dlt.
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py`
  - Build normalized `gleif_staging.gleif_*` tables from dlt raw tables.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
  - Route manifest processing through the CSV+dlt path.
  - Reject non-CSV manifests.
  - Keep old row-list helpers available for existing unit tests.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
  - Add post-publication per-table logs for ClickHouse row counts.
- Modify: `corpscout/dagster_v3/tests/test_gleif_source.py`
  - Update raw format expectations and manifest tests.
- Create: `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`
  - Test ZIP CSV extraction and dlt DuckDB loading.
- Create: `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py`
  - Test normalized SQL transforms from dlt-normalized raw tables.
- Modify: `corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`
  - Replace active manifest processing tests with CSV ZIP fixtures.

## Task 1: Source Defaults And Manifest Format

**Files:**
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Modify: `corpscout/dagster_v3/uv.lock`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_source.py`

- [ ] **Step 1: Write failing tests for CSV default and manifest file format**

Add these tests to `corpscout/dagster_v3/tests/test_gleif_source.py`:

```python
def test_raw_download_config_defaults_to_csv() -> None:
    assert source.GleifRawDownloadConfig().file_format == "csv"


def test_manifest_payload_includes_file_format_per_file() -> None:
    manifest = source.build_manifest(
        load_mode="full",
        delta=None,
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
        pulled_at=datetime(2026, 6, 20, 17, 0, tzinfo=UTC),
        files=[
            source.DownloadedFile(
                file_kind="lei_records",
                file_format="csv",
                source_url=(
                    "https://goldencopy.gleif.org/api/v2/"
                    "golden-copies/publishes/lei2/latest.csv"
                ),
                s3_key=(
                    "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
                    "run_id=run-1/file_kind=lei_records/source.csv.zip"
                ),
                size_bytes=123,
                sha256="a" * 64,
                etag='"etag"',
                last_modified="Sat, 20 Jun 2026 17:29:16 GMT",
            )
        ],
    )

    assert manifest["files"][0]["file_format"] == "csv"
```

Update `test_golden_copy_url_supports_full_and_delta` so the first assertion covers CSV:

```python
def test_golden_copy_url_supports_full_and_delta() -> None:
    assert source.golden_copy_url(file_kind="lei2", file_format="csv", delta=None) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"
    )
    assert source.golden_copy_url(
        file_kind="repex",
        file_format="csv",
        delta="LastDay",
    ) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/"
        "repex/latest.csv?delta=LastDay"
    )
```

Update raw object key tests to use `extension="csv.zip"` and assert `source.csv.zip`.

- [ ] **Step 2: Run source tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: failure because `GleifRawDownloadConfig.file_format` still defaults to `json`, `DownloadedFile` does not accept `file_format`, and manifest entries do not include `file_format`.

- [ ] **Step 3: Add pandas dependency**

Modify `corpscout/dagster_v3/pyproject.toml` dependencies:

```toml
"pandas>=3.0.0",
```

Place it near the existing data-processing dependencies:

```toml
"openai>=2.41.1",
"pandas>=3.0.0",
"polars[rtcompat]>=1.41.2",
```

Regenerate the lockfile:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv lock
```

- [ ] **Step 4: Implement CSV default and manifest file format**

In `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`, change:

```python
class GleifRawDownloadConfig(dg.Config):
    file_format: str = "json"
```

to:

```python
class GleifRawDownloadConfig(dg.Config):
    file_format: str = "csv"
```

Change `DownloadedFile` to:

```python
@dataclass(frozen=True)
class DownloadedFile:
    file_kind: str
    file_format: str
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None
```

In `build_manifest`, add `file_format`:

```python
"files": [
    {
        "file_kind": item.file_kind,
        "file_format": item.file_format,
        "source_url": item.source_url,
        "s3_key": item.s3_key,
        "size_bytes": item.size_bytes,
        "sha256": item.sha256,
        "etag": item.etag,
        "last_modified": item.last_modified,
    }
    for item in files
],
```

In `_download_one_file`, set `file_format` on the returned dataclass:

```python
downloaded_file = DownloadedFile(
    file_kind=file_kind,
    file_format=file_format,
    source_url=source_url,
    s3_key=s3_key,
    size_bytes=size_bytes,
    sha256=digest.hexdigest(),
    etag=response.headers.get("etag"),
    last_modified=response.headers.get("last-modified"),
)
```

- [ ] **Step 5: Update source test fakes to expect CSV URLs**

In `tests/test_gleif_source.py`, change `_FakeSession` to accept the expected URL:

```python
class _FakeSession:
    def __init__(
        self,
        response: _FakeResponse,
        expected_source_url: str = "https://example.test/latest.csv",
    ) -> None:
        self.response = response
        self.expected_source_url = expected_source_url

    def get(self, source_url: str, *, timeout: int, stream: bool) -> _FakeResponse:
        assert source_url == self.expected_source_url
        assert timeout == 30
        assert stream is True
        return self.response
```

Update `_download_one_file` tests so `source_url="https://example.test/latest.csv"` and `file_format="csv"`.

- [ ] **Step 6: Run source tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: all `test_gleif_source.py` tests pass.

- [ ] **Step 7: Commit Task 1**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/pyproject.toml \
  corpscout/dagster_v3/uv.lock \
  corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py \
  corpscout/dagster_v3/tests/test_gleif_source.py
git commit -m "feat: default GLEIF raw downloads to CSV"
```

## Task 2: dlt CSV Extraction And Raw DuckDB Loading

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`
- Create: `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`

- [ ] **Step 1: Write failing tests for CSV ZIP extraction and dlt loading**

Create `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`:

```python
import zipfile
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif import dlt_csv


def test_extract_single_csv_member_writes_csv_file(tmp_path: Path) -> None:
    zip_path = tmp_path / "source.csv.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("gleif.csv", "LEI,Entity.LegalName\nLEI1,Alpha Inc\n")

    extracted = dlt_csv.extract_single_csv_member(
        zip_path=zip_path,
        output_dir=tmp_path / "extracted",
        file_kind="lei_records",
    )

    assert extracted.file_kind == "lei_records"
    assert extracted.table_name == "source_lei_records"
    assert extracted.path.read_text() == "LEI,Entity.LegalName\nLEI1,Alpha Inc\n"


def test_extract_single_csv_member_rejects_multiple_csv_files(tmp_path: Path) -> None:
    zip_path = tmp_path / "source.csv.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("one.csv", "id\n1\n")
        archive.writestr("two.csv", "id\n2\n")

    try:
        dlt_csv.extract_single_csv_member(
            zip_path=zip_path,
            output_dir=tmp_path / "extracted",
            file_kind="lei_records",
        )
    except ValueError as exc:
        assert "expected exactly one CSV member" in str(exc)
    else:
        raise AssertionError("multiple CSV members should fail")


def test_load_extracted_csvs_to_duckdb_uses_dlt_raw_schema(tmp_path: Path) -> None:
    csv_path = tmp_path / "lei.csv"
    csv_path.write_text(
        "LEI,Entity.LegalName,Entity.LegalName.xmllang\n"
        "LEI1,Alpha Inc,en\n"
    )
    extracted = dlt_csv.ExtractedGleifCsv(
        file_kind="lei_records",
        table_name="source_lei_records",
        path=csv_path,
        size_bytes=csv_path.stat().st_size,
    )
    db_path = tmp_path / "gleif.duckdb"

    row_counts = dlt_csv.load_extracted_csvs_to_duckdb(
        database_path=db_path,
        extracted_files=[extracted],
        raw_schema_name="gleif_raw",
        pipelines_dir=tmp_path / ".dlt",
    )

    assert row_counts == {"source_lei_records": 1}
    with duckdb.connect(str(db_path)) as connection:
        columns = {
            row[0]
            for row in connection.execute(
                'describe "gleif"."gleif_raw"."source_lei_records"'
            ).fetchall()
        }
        assert {"lei", "entity_legal_name", "entity_legal_name_xmllang"} <= columns
        rows = connection.execute(
            'select lei, entity_legal_name from "gleif"."gleif_raw"."source_lei_records"'
        ).fetchall()

    assert rows == [("LEI1", "Alpha Inc")]
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_dlt_csv.py -q
```

Expected: failure because `dagster_v3.defs.gleif.dlt_csv` does not exist.

- [ ] **Step 3: Implement dlt CSV helper module**

Create `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`:

```python
from __future__ import annotations

import shutil
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import dlt
import duckdb
from dlt.destinations import duckdb as duckdb_destination
from dlt.sources.filesystem import filesystem, read_csv


RAW_SCHEMA_NAME = "gleif_raw"
DLT_PIPELINE_NAME = "gleif_csv_raw_loader"
DLT_TABLE_BY_FILE_KIND = {
    "lei_records": "source_lei_records",
    "relationships": "source_relationships",
    "reporting_exceptions": "source_reporting_exceptions",
}


@dataclass(frozen=True)
class ExtractedGleifCsv:
    file_kind: str
    table_name: str
    path: Path
    size_bytes: int


def table_name_for_file_kind(file_kind: str) -> str:
    try:
        return DLT_TABLE_BY_FILE_KIND[file_kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported GLEIF file_kind for CSV load: {file_kind}") from exc


def extract_single_csv_member(
    *,
    zip_path: Path,
    output_dir: Path,
    file_kind: str,
) -> ExtractedGleifCsv:
    output_dir.mkdir(parents=True, exist_ok=True)
    table_name = table_name_for_file_kind(file_kind)
    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(
                f"expected exactly one CSV member in {zip_path}, found {len(csv_names)}"
            )
        output_path = output_dir / f"{table_name}.csv"
        with archive.open(csv_names[0]) as source, output_path.open("wb") as target:
            shutil.copyfileobj(source, target)
    return ExtractedGleifCsv(
        file_kind=file_kind,
        table_name=table_name,
        path=output_path,
        size_bytes=output_path.stat().st_size,
    )


def load_extracted_csvs_to_duckdb(
    *,
    database_path: str | Path,
    extracted_files: Sequence[ExtractedGleifCsv],
    raw_schema_name: str = RAW_SCHEMA_NAME,
    pipelines_dir: str | Path | None = None,
    chunksize: int = 100_000,
) -> dict[str, int]:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    pipeline_dir = Path(pipelines_dir or database_file.parent / ".dlt" / "gleif_csv")
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    resources = []
    for extracted in extracted_files:
        resource = filesystem(
            bucket_url=str(extracted.path.parent),
            file_glob=extracted.path.name,
        ) | read_csv(
            chunksize=chunksize,
            dtype=str,
            keep_default_na=False,
        )
        resources.append(
            resource.with_name(extracted.table_name).apply_hints(
                write_disposition="replace"
            )
        )

    pipeline = dlt.pipeline(
        pipeline_name=DLT_PIPELINE_NAME,
        pipelines_dir=str(pipeline_dir),
        destination=duckdb_destination(credentials=str(database_file)),
        dataset_name=raw_schema_name,
        refresh="drop_data",
    )
    pipeline.run(resources)
    return _raw_table_counts(database_file, raw_schema_name, extracted_files)


def _raw_table_counts(
    database_path: Path,
    raw_schema_name: str,
    extracted_files: Sequence[ExtractedGleifCsv],
) -> dict[str, int]:
    catalog_name = database_path.stem
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return {
            extracted.table_name: int(
                connection.execute(
                    f'select count(*) from "{catalog_name}"."{raw_schema_name}"."{extracted.table_name}"'
                ).fetchone()[0]
            )
            for extracted in extracted_files
        }
```

- [ ] **Step 4: Run dlt CSV tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_dlt_csv.py -q
```

Expected: all `test_gleif_dlt_csv.py` tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py \
  corpscout/dagster_v3/tests/test_gleif_dlt_csv.py
git commit -m "feat: load GLEIF CSV files into DuckDB with dlt"
```

## Task 3: DuckDB SQL Normalization From dlt Raw Tables

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py`
- Create: `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py`

- [ ] **Step 1: Write failing tests for normalized SQL transforms**

Create `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py`:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif import csv_transforms


def test_build_normalized_tables_from_dlt_raw_csv_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute('create schema "gleif"."gleif_raw"')
        connection.execute(
            """
            create table "gleif"."gleif_raw"."source_lei_records" (
                lei varchar,
                entity_legal_name varchar,
                entity_legal_name_xmllang varchar,
                entity_entity_status varchar,
                entity_legal_jurisdiction varchar,
                entity_entity_category varchar,
                entity_entity_sub_category varchar,
                entity_legal_form_entity_legal_form_code varchar,
                entity_legal_form_other_legal_form varchar,
                entity_registration_authority_registration_authority_id varchar,
                entity_registration_authority_other_registration_authority_id varchar,
                entity_registration_authority_registration_authority_entity_id varchar,
                entity_entity_creation_date varchar,
                entity_entity_expiration_date varchar,
                entity_entity_expiration_reason varchar,
                entity_legal_address_xmllang varchar,
                entity_legal_address_first_address_line varchar,
                entity_legal_address_address_number varchar,
                entity_legal_address_address_number_within_building varchar,
                entity_legal_address_mail_routing varchar,
                entity_legal_address_additional_address_line_1 varchar,
                entity_legal_address_additional_address_line_2 varchar,
                entity_legal_address_additional_address_line_3 varchar,
                entity_legal_address_city varchar,
                entity_legal_address_region varchar,
                entity_legal_address_country varchar,
                entity_legal_address_postal_code varchar,
                entity_headquarters_address_xmllang varchar,
                entity_headquarters_address_first_address_line varchar,
                entity_headquarters_address_address_number varchar,
                entity_headquarters_address_address_number_within_building varchar,
                entity_headquarters_address_mail_routing varchar,
                entity_headquarters_address_additional_address_line_1 varchar,
                entity_headquarters_address_additional_address_line_2 varchar,
                entity_headquarters_address_additional_address_line_3 varchar,
                entity_headquarters_address_city varchar,
                entity_headquarters_address_region varchar,
                entity_headquarters_address_country varchar,
                entity_headquarters_address_postal_code varchar,
                entity_other_entity_names_other_entity_name_1 varchar,
                entity_other_entity_names_other_entity_name_1_xmllang varchar,
                entity_other_entity_names_other_entity_name_1_type varchar,
                entity_transliterated_other_entity_names_transliterated_other_entity_name_1 varchar,
                entity_transliterated_other_entity_names_transliterated_other_entity_name_1_xmllang varchar,
                entity_transliterated_other_entity_names_transliterated_other_entity_name_1_type varchar,
                registration_initial_registration_date varchar,
                registration_last_update_date varchar,
                registration_registration_status varchar,
                registration_next_renewal_date varchar,
                registration_managing_lou varchar,
                registration_validation_sources varchar,
                registration_validation_authority_validation_authority_id varchar,
                registration_validation_authority_other_validation_authority_id varchar,
                registration_validation_authority_validation_authority_entity_id varchar,
                conformity_flag varchar
            )
            """
        )
        connection.execute(
            """
            insert into "gleif"."gleif_raw"."source_lei_records" values (
                'LEI1', 'Alpha Inc', 'en', 'ACTIVE', 'US-DE', 'GENERAL', '',
                'XTIQ', '', 'RA000001', '', '12345',
                '2020-01-01T00:00:00+00:00', '', '',
                'en', '1 Main Street', '', '', '', 'Suite 1', '', '', 'Wilmington', 'US-DE', 'US', '19801',
                'en', '2 HQ Street', '', '', '', '', '', '', 'New York', 'US-NY', 'US', '10001',
                'Alpha Old', 'en', 'PREVIOUS_LEGAL_NAME',
                'Alpha ASCII', 'en', 'PREFERRED_ASCII_TRANSLITERATED_LEGAL_NAME',
                '2020-01-02T00:00:00+00:00', '2026-06-20T00:00:00+00:00',
                'ISSUED', '2027-06-20T00:00:00+00:00', 'LOU1',
                'FULLY_CORROBORATED', 'RA000001', '', '12345', 'CONFORMING'
            )
            """
        )
        connection.execute(
            """
            create table "gleif"."gleif_raw"."source_relationships" (
                relationship_start_node_node_id varchar,
                relationship_start_node_node_id_type varchar,
                relationship_end_node_node_id varchar,
                relationship_end_node_node_id_type varchar,
                relationship_relationship_type varchar,
                relationship_relationship_status varchar,
                relationship_period_1_start_date varchar,
                relationship_period_1_end_date varchar,
                relationship_period_1_period_type varchar,
                registration_initial_registration_date varchar,
                registration_last_update_date varchar,
                registration_registration_status varchar,
                registration_next_renewal_date varchar,
                registration_managing_lou varchar,
                registration_validation_sources varchar,
                registration_validation_documents varchar,
                registration_validation_reference varchar,
                deleted_at varchar
            )
            """
        )
        connection.execute(
            """
            insert into "gleif"."gleif_raw"."source_relationships" values (
                'LEI1', 'LEI', 'LEI_PARENT', 'LEI', 'IS_DIRECTLY_CONSOLIDATED_BY',
                'ACTIVE', '2025-01-01T00:00:00+00:00', '2025-12-31T00:00:00+00:00',
                'ACCOUNTING_PERIOD', '2020-01-01T00:00:00+00:00',
                '2026-06-20T00:00:00+00:00', 'PUBLISHED', '2027-06-20T00:00:00+00:00',
                'LOU1', 'FULLY_CORROBORATED', 'SUPPORTING_DOCUMENTS', 'Annual report', ''
            )
            """
        )
        connection.execute(
            """
            create table "gleif"."gleif_raw"."source_reporting_exceptions" (
                lei varchar,
                exception_category varchar,
                exception_reason_1 varchar,
                exception_reason_2 varchar,
                exception_reference_1 varchar,
                exception_reference_2 varchar,
                deleted_at varchar
            )
            """
        )
        connection.execute(
            """
            insert into "gleif"."gleif_raw"."source_reporting_exceptions" values (
                'LEI1', 'NO_KNOWN_PERSON', '', 'NO_LEI', '', 'Regulatory filing', ''
            )
            """
        )

        counts = csv_transforms.replace_normalized_staging_from_raw(
            connection,
            catalog_name="gleif",
            raw_schema_name="gleif_raw",
            staging_schema_name="gleif_staging",
            source_run_id="run-1",
            retrieved_at="2026-06-21T00:00:00+00:00",
            resolved_at="2026-06-21T00:00:00+00:00",
            golden_copy_publish_date="2026-06-21T00:00:00+00:00",
        )

        assert counts["gleif_lei_records"] == 1
        assert counts["gleif_lei_names"] == 3
        assert counts["gleif_lei_addresses"] == 2
        assert counts["gleif_lei_relationships"] == 1
        assert counts["gleif_lei_relationship_periods"] == 1
        assert counts["gleif_lei_reporting_exceptions"] == 1
        assert counts["gleif_lei_identifiers"] == 0
        assert counts["gleif_lei_issuers"] == 0
        assert counts["gleif_code_list_entries"] == 0

        records = connection.execute(
            'select lei, legal_name, primary_country_iso2 from "gleif"."gleif_staging"."gleif_lei_records"'
        ).fetchall()
        names = connection.execute(
            'select name_type, name from "gleif"."gleif_staging"."gleif_lei_names" order by sequence'
        ).fetchall()
        addresses = connection.execute(
            'select address_role, city, country from "gleif"."gleif_staging"."gleif_lei_addresses" order by address_role'
        ).fetchall()
        periods = connection.execute(
            'select period_type, start_date, end_date from "gleif"."gleif_staging"."gleif_lei_relationship_periods"'
        ).fetchall()
        exceptions = connection.execute(
            'select exception_category, exception_reason, exception_reference from "gleif"."gleif_staging"."gleif_lei_reporting_exceptions"'
        ).fetchall()

    assert records == [("LEI1", "Alpha Inc", "US")]
    assert names == [
        ("legal_name", "Alpha Inc"),
        ("other_name", "Alpha Old"),
        ("transliterated_other_name", "Alpha ASCII"),
    ]
    assert addresses == [
        ("headquarters", "New York", "US"),
        ("legal", "Wilmington", "US"),
    ]
    assert periods == [("ACCOUNTING_PERIOD", "2025-01-01", "2025-12-31")]
    assert exceptions == [("NO_KNOWN_PERSON", "NO_LEI", "Regulatory filing")]
```

- [ ] **Step 2: Run transform tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_csv_transforms.py -q
```

Expected: failure because `csv_transforms.py` does not exist.

- [ ] **Step 3: Implement SQL transform module**

Create `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py` with these public functions and helpers:

```python
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import duckdb

from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.duckdb_state import DUCKDB_COLUMN_TYPES


SOURCE_LEI_RECORDS_TABLE = "source_lei_records"
SOURCE_RELATIONSHIPS_TABLE = "source_relationships"
SOURCE_REPORTING_EXCEPTIONS_TABLE = "source_reporting_exceptions"


def replace_normalized_staging_from_raw(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
    golden_copy_publish_date: str | None,
) -> dict[str, int]:
    connection.execute(f"create schema if not exists {_schema(catalog_name, staging_schema_name)}")
    _require_source_columns(connection, catalog_name, raw_schema_name)
    _replace_lei_records(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
        golden_copy_publish_date=golden_copy_publish_date,
    )
    _replace_lei_names(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
    )
    _replace_lei_addresses(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
    )
    _replace_relationships(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
    )
    _replace_relationship_periods(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
    )
    _replace_reporting_exceptions(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=raw_schema_name,
        staging_schema_name=staging_schema_name,
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        resolved_at=resolved_at,
    )
    _replace_empty_table(connection, catalog_name, staging_schema_name, tables.GLEIF_LEI_IDENTIFIERS_TABLE)
    _replace_empty_table(connection, catalog_name, staging_schema_name, tables.GLEIF_LEI_ISSUERS_TABLE)
    _replace_empty_table(connection, catalog_name, staging_schema_name, tables.GLEIF_CODE_LIST_ENTRIES_TABLE)
    return _row_counts(connection, catalog_name, staging_schema_name)
```

Use these SQL helper patterns in the same file:

```python
def _replace_lei_records(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
    golden_copy_publish_date: str | None,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_LEI_RECORDS_TABLE)
    target = _table(catalog_name, staging_schema_name, tables.GLEIF_LEI_RECORDS_TABLE)
    connection.execute(
        f"""
        create or replace table {target} as
        select
          {_text('lei')} as lei,
          coalesce({_text('entity_legal_name')}, '') as legal_name,
          {_text('entity_legal_name_xmllang')} as legal_name_language,
          coalesce({_text('entity_entity_status')}, '') as entity_status,
          coalesce({_text('registration_registration_status')}, '') as registration_status,
          {_text('entity_legal_jurisdiction')} as jurisdiction,
          {_text('entity_entity_category')} as category,
          {_text('entity_entity_sub_category')} as subcategory,
          {_text('entity_legal_form_entity_legal_form_code')} as legal_form_id,
          {_text('entity_legal_form_other_legal_form')} as legal_form_other,
          {_text('entity_registration_authority_registration_authority_id')} as registered_at_id,
          {_text('entity_registration_authority_other_registration_authority_id')} as registered_at_other,
          {_text('entity_registration_authority_registration_authority_entity_id')} as registered_as,
          cast(null as varchar) as associated_entity_lei,
          cast(null as varchar) as associated_entity_name,
          cast(null as varchar) as successor_entity_lei,
          cast(null as varchar) as successor_entity_name,
          {_timestamp('entity_entity_creation_date')} as creation_date,
          {_timestamp('entity_entity_expiration_date')} as expiration_date,
          {_text('entity_entity_expiration_reason')} as expiration_reason,
          {_timestamp('registration_initial_registration_date')} as initial_registration_date,
          {_timestamp('registration_last_update_date')} as last_update_date,
          {_timestamp('registration_next_renewal_date')} as next_renewal_date,
          {_text('registration_managing_lou')} as managing_lou,
          {_text('registration_validation_sources')} as corroboration_level,
          {_text('registration_validation_authority_validation_authority_id')} as validated_at_id,
          {_text('registration_validation_authority_other_validation_authority_id')} as validated_at_other,
          {_text('registration_validation_authority_validation_authority_entity_id')} as validated_as,
          {_text('conformity_flag')} as conformity_flag,
          {_text('entity_legal_address_country')} as legal_address_country,
          {_text('entity_headquarters_address_country')} as headquarters_address_country,
          coalesce({_text('entity_legal_address_country')}, {_text('entity_headquarters_address_country')}) as primary_country_iso2,
          try_cast({repr(golden_copy_publish_date)} as timestamp) as golden_copy_publish_date,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text('lei')} is not null
        """
    )
```

Add name, address, relationship, period, and exception builders using generated `UNION ALL` fragments:

```python
def _replace_lei_names(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_LEI_RECORDS_TABLE)
    target = _table(catalog_name, staging_schema_name, tables.GLEIF_LEI_NAMES_TABLE)
    selects = [
        _name_select(
            source,
            name_column="entity_legal_name",
            language_column="entity_legal_name_xmllang",
            cdf_type_column=None,
            name_type="legal_name",
            sequence=1,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )
    ]
    for index in range(1, 6):
        selects.append(
            _name_select(
                source,
                name_column=f"entity_other_entity_names_other_entity_name_{index}",
                language_column=f"entity_other_entity_names_other_entity_name_{index}_xmllang",
                cdf_type_column=f"entity_other_entity_names_other_entity_name_{index}_type",
                name_type="other_name",
                sequence=1 + index,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            )
        )
        selects.append(
            _name_select(
                source,
                name_column=(
                    "entity_transliterated_other_entity_names_"
                    f"transliterated_other_entity_name_{index}"
                ),
                language_column=(
                    "entity_transliterated_other_entity_names_"
                    f"transliterated_other_entity_name_{index}_xmllang"
                ),
                cdf_type_column=(
                    "entity_transliterated_other_entity_names_"
                    f"transliterated_other_entity_name_{index}_type"
                ),
                name_type="transliterated_other_name",
                sequence=6 + index,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            )
        )
    connection.execute(f"create or replace table {target} as {' union all '.join(selects)}")
```

Add `_replace_empty_table` so unavailable Golden Copy CSV tables are still created:

```python
def _replace_empty_table(
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> None:
    columns = ", ".join(
        f"cast(null as {_duckdb_type(column)}) as {_quote(column)}"
        for column in tables.GLEIF_TABLE_COLUMNS[table_name]
    )
    connection.execute(
        f"create or replace table {_table(catalog_name, schema_name, table_name)} "
        f"as select {columns} where false"
    )
```

Add shared helpers:

```python
def _text(column: str) -> str:
    return f"nullif(trim({_quote(column)}), '')"


def _timestamp(column: str) -> str:
    return f"try_cast({_text(column)} as timestamp)"


def _date(column: str) -> str:
    return f"cast(try_cast({_text(column)} as timestamp) as date)"


def _address_lines(prefix: str) -> str:
    values = ", ".join(
        _text(f"{prefix}_{suffix}")
        for suffix in (
            "first_address_line",
            "additional_address_line_1",
            "additional_address_line_2",
            "additional_address_line_3",
        )
    )
    return f"list_filter([{values}], x -> x is not null and x <> '')"


def _duckdb_type(column: str) -> str:
    return DUCKDB_COLUMN_TYPES.get(column, "varchar")


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


def _schema(catalog_name: str, schema_name: str) -> str:
    return f"{_quote(catalog_name)}.{_quote(schema_name)}"


def _table(catalog_name: str, schema_name: str, table_name: str) -> str:
    return f"{_schema(catalog_name, schema_name)}.{_quote(table_name)}"
```

Add the helper used by `_replace_lei_names`:

```python
def _name_select(
    source: str,
    *,
    name_column: str,
    language_column: str,
    cdf_type_column: str | None,
    name_type: str,
    sequence: int,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> str:
    cdf_type = _text(cdf_type_column) if cdf_type_column is not None else "cast(null as varchar)"
    return f"""
        select
          {_text('lei')} as lei,
          {repr(name_type)} as name_type,
          {_text(name_column)} as name,
          lower(trim({_text(name_column)})) as name_normalized,
          {_text(language_column)} as language,
          {cdf_type} as cdf_type,
          {sequence} as sequence,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text(name_column)} is not null
    """
```

Add `_replace_lei_addresses`:

```python
def _replace_lei_addresses(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_LEI_RECORDS_TABLE)
    target = _table(catalog_name, staging_schema_name, tables.GLEIF_LEI_ADDRESSES_TABLE)
    selects = [
        _address_select(
            source,
            role="legal",
            prefix="entity_legal_address",
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        ),
        _address_select(
            source,
            role="headquarters",
            prefix="entity_headquarters_address",
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        ),
    ]
    connection.execute(f"create or replace table {target} as {' union all '.join(selects)}")


def _address_select(
    source: str,
    *,
    role: str,
    prefix: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> str:
    return f"""
        select
          {_text('lei')} as lei,
          {repr(role)} as address_role,
          {_text(f'{prefix}_xmllang')} as language,
          {_address_lines(prefix)} as address_lines,
          {_text(f'{prefix}_address_number')} as address_number,
          {_text(f'{prefix}_address_number_within_building')} as address_number_within_building,
          {_text(f'{prefix}_mail_routing')} as mail_routing,
          {_text(f'{prefix}_city')} as city,
          {_text(f'{prefix}_region')} as region,
          {_text(f'{prefix}_country')} as country,
          {_text(f'{prefix}_postal_code')} as postal_code,
          cast(null as varchar) as normalized_address,
          cast(null as double) as latitude,
          cast(null as double) as longitude,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text('lei')} is not null
          and (
            {_text(f'{prefix}_first_address_line')} is not null
            or {_text(f'{prefix}_city')} is not null
            or {_text(f'{prefix}_country')} is not null
          )
    """
```

Add `_replace_relationships` and `_replace_relationship_periods`:

```python
def _replace_relationships(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_RELATIONSHIPS_TABLE)
    target = _table(catalog_name, staging_schema_name, tables.GLEIF_LEI_RELATIONSHIPS_TABLE)
    relationship_id = (
        "concat_ws(':', "
        f"{_text('relationship_start_node_node_id')}, "
        f"{_text('relationship_relationship_type')}, "
        f"{_text('relationship_end_node_node_id')})"
    )
    connection.execute(
        f"""
        create or replace table {target} as
        select
          {relationship_id} as relationship_record_id,
          coalesce({_text('relationship_start_node_node_id')}, '') as start_node_lei,
          {_text('relationship_start_node_node_id_type')} as start_node_type,
          coalesce({_text('relationship_end_node_node_id')}, '') as end_node_lei,
          {_text('relationship_end_node_node_id_type')} as end_node_type,
          coalesce({_text('relationship_relationship_type')}, '') as relationship_type,
          coalesce({_text('relationship_relationship_status')}, '') as relationship_status,
          cast(null as timestamp) as valid_from,
          cast(null as timestamp) as valid_to,
          {_timestamp('registration_initial_registration_date')} as initial_registration_date,
          {_timestamp('registration_last_update_date')} as last_update_date,
          {_text('registration_registration_status')} as registration_status,
          {_timestamp('registration_next_renewal_date')} as next_renewal_date,
          {_text('registration_managing_lou')} as managing_lou,
          {_text('registration_validation_sources')} as corroboration_level,
          {_text('registration_validation_documents')} as corroboration_documents,
          {_text('registration_validation_reference')} as corroboration_reference,
          {_timestamp('deleted_at')} as deleted_at,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text('relationship_start_node_node_id')} is not null
          and {_text('relationship_end_node_node_id')} is not null
          and {_text('relationship_relationship_type')} is not null
        """
    )


def _replace_relationship_periods(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_RELATIONSHIPS_TABLE)
    target = _table(
        catalog_name,
        staging_schema_name,
        tables.GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE,
    )
    selects = [
        _period_select(
            source,
            index=index,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )
        for index in range(1, 6)
    ]
    connection.execute(f"create or replace table {target} as {' union all '.join(selects)}")


def _period_select(
    source: str,
    *,
    index: int,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> str:
    relationship_id = (
        "concat_ws(':', "
        f"{_text('relationship_start_node_node_id')}, "
        f"{_text('relationship_relationship_type')}, "
        f"{_text('relationship_end_node_node_id')})"
    )
    return f"""
        select
          {relationship_id} as relationship_record_id,
          coalesce({_text(f'relationship_period_{index}_period_type')}, '') as period_type,
          {_date(f'relationship_period_{index}_start_date')} as start_date,
          {_date(f'relationship_period_{index}_end_date')} as end_date,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text(f'relationship_period_{index}_period_type')} is not null
    """
```

Add `_replace_reporting_exceptions`:

```python
def _replace_reporting_exceptions(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    raw_schema_name: str,
    staging_schema_name: str,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> None:
    source = _table(catalog_name, raw_schema_name, SOURCE_REPORTING_EXCEPTIONS_TABLE)
    target = _table(
        catalog_name,
        staging_schema_name,
        tables.GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE,
    )
    reason = _coalesce_text(*(f"exception_reason_{index}" for index in range(1, 6)))
    reference = _coalesce_text(*(f"exception_reference_{index}" for index in range(1, 6)))
    exception_id = f"sha256(concat_ws(':', {_text('lei')}, {_text('exception_category')}))"
    connection.execute(
        f"""
        create or replace table {target} as
        select
          {exception_id} as exception_record_id,
          coalesce({_text('lei')}, '') as lei,
          'IS_DIRECTLY_CONSOLIDATED_BY' as parent_relationship_type,
          coalesce({_text('exception_category')}, '') as exception_category,
          {reason} as exception_reason,
          {reference} as exception_reference,
          cast(null as timestamp) as initial_registration_date,
          cast(null as timestamp) as last_update_date,
          cast(null as varchar) as registration_status,
          cast(null as timestamp) as next_renewal_date,
          cast(null as varchar) as managing_lou,
          'gleif' as source_system,
          {repr(source_run_id)} as source_run_id,
          try_cast({repr(retrieved_at)} as timestamp) as retrieved_at,
          try_cast({repr(resolved_at)} as timestamp) as resolved_at
        from {source}
        where {_text('lei')} is not null
          and {_text('exception_category')} is not null
        """
    )


def _coalesce_text(*columns: str) -> str:
    return "coalesce(" + ", ".join(_text(column) for column in columns) + ")"
```

Add source-column validation and row counts:

```python
def _require_source_columns(
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    raw_schema_name: str,
) -> None:
    required = {
        SOURCE_LEI_RECORDS_TABLE: {"lei", "entity_legal_name"},
        SOURCE_RELATIONSHIPS_TABLE: {
            "relationship_start_node_node_id",
            "relationship_end_node_node_id",
            "relationship_relationship_type",
        },
        SOURCE_REPORTING_EXCEPTIONS_TABLE: {"lei", "exception_category"},
    }
    for table_name, required_columns in required.items():
        existing_columns = {
            row[1]
            for row in connection.execute(
                """
                select table_name, column_name
                from information_schema.columns
                where table_catalog = ?
                  and table_schema = ?
                  and table_name = ?
                """,
                [catalog_name, raw_schema_name, table_name],
            ).fetchall()
        }
        missing_columns = sorted(required_columns - existing_columns)
        if missing_columns:
            raise ValueError(
                f"Missing GLEIF raw CSV columns for {table_name}: "
                + ", ".join(missing_columns)
            )


def _row_counts(
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    schema_name: str,
) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(
                f"select count(*) from {_table(catalog_name, schema_name, table_name)}"
            ).fetchone()[0]
        )
        for table_name in tables.GLEIF_TABLES
    }
```

- [ ] **Step 4: Run transform tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_csv_transforms.py -q
```

Expected: all transform tests pass.

- [ ] **Step 5: Commit Task 3**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py \
  corpscout/dagster_v3/tests/test_gleif_csv_transforms.py
git commit -m "feat: normalize GLEIF dlt raw tables in DuckDB"
```

## Task 4: Wire CSV+dlt Processing Into DuckDB State Asset

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`

- [ ] **Step 1: Write failing integration tests for CSV manifest processing**

In `corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`, replace the active manifest test with CSV ZIP fixtures:

```python
def test_refresh_duckdb_state_loads_csv_zip_manifest_through_dlt(tmp_path: Path) -> None:
    source_key = (
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
        "run_id=run-full/file_kind=lei_records/source.csv.zip"
    )
    manifest_key = (
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
        "run_id=run-full/manifest.json"
    )
    object_store = _FakeObjectStore(
        {
            manifest_key: {
                "load_mode": "full",
                "publish_date": "2026-06-20T16:00:00+00:00",
                "pulled_at": "2026-06-20T17:00:00+00:00",
                "run_id": "run-full",
                "files": [
                    {
                        "file_kind": "lei_records",
                        "file_format": "csv",
                        "s3_key": source_key,
                    },
                    {
                        "file_kind": "relationships",
                        "file_format": "csv",
                        "s3_key": "relationships.csv.zip",
                    },
                    {
                        "file_kind": "reporting_exceptions",
                        "file_format": "csv",
                        "s3_key": "reporting_exceptions.csv.zip",
                    },
                ],
            }
        },
        blobs={
            source_key: _zip_text(
                "lei2.csv",
                (
                    "LEI,Entity.LegalName,Entity.LegalName.xmllang,"
                    "Entity.EntityStatus,Registration.RegistrationStatus,"
                    "Entity.LegalAddress.Country,Entity.HeadquartersAddress.Country\n"
                    "LEI1,Alpha Inc,en,ACTIVE,ISSUED,US,US\n"
                ),
            ),
            "relationships.csv.zip": _zip_text(
                "rr.csv",
                (
                    "Relationship.StartNode.NodeID,Relationship.StartNode.NodeIDType,"
                    "Relationship.EndNode.NodeID,Relationship.EndNode.NodeIDType,"
                    "Relationship.RelationshipType,Relationship.RelationshipStatus\n"
                    "LEI1,LEI,LEI_PARENT,LEI,IS_DIRECTLY_CONSOLIDATED_BY,ACTIVE\n"
                ),
            ),
            "reporting_exceptions.csv.zip": _zip_text(
                "repex.csv",
                "LEI,Exception.Category,Exception.Reason.1,Exception.Reference.1\n"
                "LEI1,NO_KNOWN_PERSON,NO_LEI,Reference\n",
            ),
        },
    )

    result = duckdb_state.refresh_gleif_duckdb_state(
        context=_FakeContext("run-full"),
        object_store=object_store,
        database_path=tmp_path / "gleif.duckdb",
    )

    assert result.metadata["gleif_lei_records_row_count"] == 1
    assert source_key not in object_store.read_bytes_keys
    assert object_store.state["last_full_publish_date"] == "2026-06-20T16:00:00+00:00"
```

Add a non-CSV rejection test:

```python
def test_refresh_duckdb_state_rejects_legacy_json_manifest(tmp_path: Path) -> None:
    manifest_key = (
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
        "run_id=run-full/manifest.json"
    )
    object_store = _FakeObjectStore(
        {
            manifest_key: {
                "load_mode": "full",
                "publish_date": "2026-06-20T16:00:00+00:00",
                "pulled_at": "2026-06-20T17:00:00+00:00",
                "run_id": "run-full",
                "files": [
                    {
                        "file_kind": "lei_records",
                        "file_format": "json",
                        "s3_key": "source.json.zip",
                    }
                ],
            }
        }
    )

    try:
        duckdb_state.refresh_gleif_duckdb_state(
            context=_FakeContext("run-full"),
            object_store=object_store,
            database_path=tmp_path / "gleif.duckdb",
        )
    except ValueError as exc:
        assert "Only CSV GLEIF raw manifests are supported" in str(exc)
    else:
        raise AssertionError("legacy JSON manifests should fail")
```

Add this helper in the test file:

```python
def _zip_text(member_name: str, body: str) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, body)
    return buffer.getvalue()
```

- [ ] **Step 2: Run DuckDB state tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_duckdb_state.py -q
```

Expected: failure because `refresh_gleif_duckdb_state` still routes manifest processing through JSON row iterators.

- [ ] **Step 3: Implement CSV manifest routing in duckdb_state.py**

In `corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`, import:

```python
from dagster_v3.defs.gleif import csv_transforms
from dagster_v3.defs.gleif import dlt_csv
```

Add:

```python
def _ensure_csv_manifest(manifest: dict[str, Any]) -> None:
    bad_files = [
        str(item.get("s3_key"))
        for item in manifest.get("files", [])
        if item.get("file_format") != "csv"
    ]
    if bad_files:
        raise ValueError(
            "Only CSV GLEIF raw manifests are supported by the DuckDB state asset. "
            "Unsupported files: "
            + ", ".join(bad_files)
        )
```

Replace `_load_manifest_into_schema` usage in `replace_current_state_from_manifest` and `apply_delta_state_from_manifest` with a new CSV function:

```python
def _load_csv_manifest_into_staging(
    connection: duckdb.DuckDBPyConnection,
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    database_path: Path,
    catalog_name: str,
    staging_schema_name: str,
) -> dict[str, int]:
    _ensure_csv_manifest(manifest)
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        extracted_files: list[dlt_csv.ExtractedGleifCsv] = []
        for item in manifest.get("files", []):
            file_kind = str(item["file_kind"])
            s3_key = str(item["s3_key"])
            zip_path = temp_dir / f"{file_kind}.csv.zip"
            with zip_path.open("wb") as target:
                _copy_s3_object_to_file(object_store, s3_key, target)
            extracted_files.append(
                dlt_csv.extract_single_csv_member(
                    zip_path=zip_path,
                    output_dir=temp_dir / "csv",
                    file_kind=file_kind,
                )
            )
        dlt_csv.load_extracted_csvs_to_duckdb(
            database_path=database_path,
            extracted_files=extracted_files,
            raw_schema_name=dlt_csv.RAW_SCHEMA_NAME,
            pipelines_dir=database_path.parent / ".dlt" / "gleif_csv",
        )
    return csv_transforms.replace_normalized_staging_from_raw(
        connection,
        catalog_name=catalog_name,
        raw_schema_name=dlt_csv.RAW_SCHEMA_NAME,
        staging_schema_name=staging_schema_name,
        source_run_id=str(manifest["run_id"]),
        retrieved_at=str(manifest["pulled_at"]),
        resolved_at=str(manifest["pulled_at"]),
        golden_copy_publish_date=str(manifest["publish_date"]),
    )
```

Change `_copy_s3_object_to_file` to accept any binary target file object:

```python
def _copy_s3_object_to_file(
    object_store: ObjectStoreResource,
    s3_key: str,
    target_file: Any,
) -> None:
    response = object_store.client().get_object(Bucket=GLEIF_RAW_BUCKET, Key=s3_key)
    body = response["Body"]
    shutil.copyfileobj(body, target_file)
```

In `replace_current_state_from_manifest`, replace the old manifest-loading call with:

```python
_load_csv_manifest_into_staging(
    connection,
    object_store=object_store,
    manifest=manifest,
    database_path=database_file,
    catalog_name=catalog_name,
    staging_schema_name=DUCKDB_STAGING_SCHEMA,
)
```

In `apply_delta_state_from_manifest`, assign staged counts from the same function:

```python
staged_counts = _load_csv_manifest_into_staging(
    connection,
    object_store=object_store,
    manifest=manifest,
    database_path=database_file,
    catalog_name=catalog_name,
    staging_schema_name=DUCKDB_STAGING_SCHEMA,
)
```

Leave the old JSON `_iter_manifest_row_groups` path in the file for now only if existing tests still use the small row-list APIs. Do not call it from `refresh_gleif_duckdb_state`.

- [ ] **Step 4: Add minimal progress logs**

Inside `_load_csv_manifest_into_staging`, if a Dagster context is passed directly or a logger is threaded into the function, log:

```python
context.log.info(
    "copying_gleif_raw_file_from_s3",
    extra={"file_kind": file_kind, "s3_key": s3_key},
)
context.log.info(
    "extracted_gleif_csv",
    extra={
        "file_kind": extracted.file_kind,
        "table_name": extracted.table_name,
        "size_bytes": extracted.size_bytes,
    },
)
```

If threading `context` into the helper makes the signature clearer, use:

```python
def _load_csv_manifest_into_staging(
    context: dg.AssetExecutionContext,
    connection: duckdb.DuckDBPyConnection,
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    database_path: Path,
    catalog_name: str,
    staging_schema_name: str,
) -> dict[str, int]:
```

Then log normalized staging counts after `csv_transforms.replace_normalized_staging_from_raw`.

- [ ] **Step 5: Run DuckDB state tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_duckdb_state.py tests/test_gleif_dlt_csv.py tests/test_gleif_csv_transforms.py -q
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit Task 4**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py \
  corpscout/dagster_v3/tests/test_gleif_duckdb_state.py
git commit -m "feat: build GLEIF DuckDB state from dlt CSV loads"
```

## Task 5: ClickHouse Publication Logs And Asset Verification

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_assets.py`

- [ ] **Step 1: Add an asset-level logging test**

In `corpscout/dagster_v3/tests/test_gleif_assets.py`, keep existing registration tests and add this constant test:

```python
def test_gleif_clickhouse_logs_are_named_per_table() -> None:
    from dagster_v3.defs.gleif import assets

    assert assets.GLEIF_CLICKHOUSE_PUBLISH_LOG_EVENT == "published_gleif_clickhouse_table"
```

- [ ] **Step 2: Implement a stable log event constant and logs**

In `assets.py`, add:

```python
GLEIF_CLICKHOUSE_PUBLISH_LOG_EVENT = "published_gleif_clickhouse_table"
```

Change the `gleif_reference_clickhouse` signature to accept context:

```python
def gleif_reference_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
```

In `gleif_reference_clickhouse`, after the `replace_duckdb_tables_in_clickhouse` call assigns `row_counts`, add:

```python
for table_name, row_count in row_counts.items():
    context.log.info(
        GLEIF_CLICKHOUSE_PUBLISH_LOG_EVENT,
        extra={"table": table_name, "row_count": row_count},
    )
```

- [ ] **Step 3: Run asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py -q
```

Expected: all asset tests pass.

- [ ] **Step 4: Commit Task 5**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py \
  corpscout/dagster_v3/tests/test_gleif_assets.py
git commit -m "chore: log GLEIF ClickHouse publish counts"
```

## Task 6: Full Verification And Server Rollout Notes

**Files:**
- Modify: no production files unless verification finds a defect.

- [ ] **Step 1: Run focused GLEIF tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py \
  tests/test_gleif_dlt_csv.py \
  tests/test_gleif_csv_transforms.py \
  tests/test_gleif_duckdb_state.py \
  tests/test_gleif_assets.py \
  tests/test_gleif_tables.py \
  tests/test_clickhouse_migrations.py \
  -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run full test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest -q
```

Expected: full suite passes.

- [ ] **Step 3: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 4: Handle verification failures**

If Step 1, Step 2, or Step 3 fails, return to the task that introduced the failing behavior, add a focused regression test there, and commit through that task's commit step. If all verification commands pass, do not create an empty commit.

- [ ] **Step 5: Server rollout commands**

After the branch is pushed and pulled on `companycollect`, stop the old long-running bootstrap if it is still executing:

```bash
pgrep -af "gleif_reference_bootstrap_job|dagster|dg"
```

Terminate only the stale GLEIF launch command after confirming it is the old run:

```bash
pkill -f "uv run dg launch --job gleif_reference_bootstrap_job"
```

Then update dependencies and verify:

```bash
cd ~/companycollect/corpscout/dagster_v3
uv sync
uv run python - <<'PY'
import pandas
import dlt
print("pandas", pandas.__version__)
print("dlt", dlt.__version__)
PY
uv run dg check defs
```

Launch the new bootstrap:

```bash
uv run dg launch --job gleif_reference_bootstrap_job
```

Expected early log sequence:

```text
downloaded_gleif_golden_copy_files
copying_gleif_raw_file_from_s3
extracted_gleif_csv
loaded_gleif_source_csv_with_dlt
built_gleif_table
published_gleif_clickhouse_table
```
