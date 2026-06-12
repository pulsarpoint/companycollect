# Dagster ClickHouse Import Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first Dagster-native import slice after `finland_prhytj/raw_snapshot`: parse the RustFS snapshot and code-list artifacts, normalize them in Python, and write the existing ClickHouse source tables in `corpscout_sources`.

**Architecture:** Dagster remains the orchestration and transformation surface for this slice. RustFS stores durable raw run artifacts and manifests. A small ClickHouse resource owns client construction and inserts. Source-specific Python modules own parsing, normalization, table metadata, and batching. ClickHouse table schema continues to be owned by the existing ClickHouse migrations, not by Dagster.

**Tech Stack:** Python 3.12, Dagster, boto3/RustFS, clickhouse-connect, pytest, moto, ClickHouse.

---

## Scope

In scope:

- Add ClickHouse runtime configuration to the Dagster image, daemon/webserver/code location, and run containers.
- Add a Dagster `ClickHouseResource`.
- Add RustFS read helpers for manifests and artifact streams.
- Add Python parser/normalizer/importer for the PRH YTJ company snapshot.
- Add Python parser/importer for the seven PRH YTJ code-list TSV artifacts.
- Add `finland_prhytj/normalized_tables` and `finland_prhytj/code_lists` assets.
- Insert into the existing real ClickHouse tables:
  - `fi_prhytj_identifiers`
  - `fi_prhytj_statuses`
  - `fi_prhytj_names`
  - `fi_prhytj_business_lines`
  - `fi_prhytj_business_line_descriptions`
  - `fi_prhytj_websites`
  - `fi_prhytj_company_forms`
  - `fi_prhytj_company_form_descriptions`
  - `fi_prhytj_company_situations`
  - `fi_prhytj_company_situation_descriptions`
  - `fi_prhytj_registered_entries`
  - `fi_prhytj_registered_entry_descriptions`
  - `fi_prhytj_addresses`
  - `fi_prhytj_address_post_offices`
  - `fi_prhytj_code_lists`

Out of scope for this slice:

- NACE mapping/cache/import.
- XBRL assets.
- dbt.
- Old Go importer parity jobs or bakeoff mode.
- Dagster-managed ClickHouse schema migrations.
- Multi-source generic import framework.

## Current Inputs

Raw snapshot asset writes:

- Bucket: `source-finland-prhytj`
- Snapshot object: `runs/{run_id}/source.ndjson`
- Code-list objects: `runs/{run_id}/codelists/{CODE}.{lang}.tsv`
- Manifest object: `runs/{run_id}/manifest.json`

Manifest shape:

```json
{
  "run_id": "20260611T211500Z-aa6a74aa",
  "source": "finland_prhytj",
  "workflow_id": "dagster-run-aa6a74aa-20ce-4447-b776-8b00e4bc8112",
  "artifacts": [
    {
      "key": "source",
      "object_key": "runs/20260611T211500Z-aa6a74aa/source.ndjson",
      "content_sha256": "...",
      "content_length_bytes": 123,
      "records_written": 456
    },
    {
      "key": "codelist_REK_en",
      "object_key": "runs/20260611T211500Z-aa6a74aa/codelists/REK.en.tsv",
      "content_sha256": "...",
      "content_length_bytes": 123,
      "records_written": 0
    }
  ]
}
```

For the first import slice, downstream assets select the latest completed manifest by lexicographic object key under `runs/*/manifest.json`. This works because `run_id` starts with UTC timestamp and the manifest is written only after all raw artifacts are uploaded.

## Target Asset Graph

```text
finland_prhytj/raw_snapshot
  -> finland_prhytj/normalized_tables
  -> finland_prhytj/code_lists
```

`normalized_tables` and `code_lists` both read from the same completed manifest. They are separate assets because they have different parsing rules, target tables, and row counts.

## Implementation Steps

- [x] 1. Add ClickHouse dependency and runtime configuration

  Files:

  - `corpscout/dagster/pyproject.toml`
  - `corpscout/dagster/.env.example`
  - `corpscout/dagster/dagster.yaml`

  Update dependencies:

  ```toml
  dependencies = [
      "dagster>=1.10,<2",
      "dagster-postgres>=0.26",
      "dagster-docker>=0.26",
      "boto3>=1.34",
      "requests>=2.32",
      "clickhouse-connect>=0.8",
  ]
  ```

  Add example environment:

  ```dotenv
  # ClickHouse source database.
  CLICKHOUSE_HOST=companycollect
  CLICKHOUSE_PORT=8123
  CLICKHOUSE_USER=default
  CLICKHOUSE_PASSWORD=CHANGE_ME
  CLICKHOUSE_DATABASE=corpscout_sources
  CLICKHOUSE_SECURE=false
  ```

  Add these variables to `dagster.yaml` DockerRunLauncher `env_vars` so materialization run containers can connect to ClickHouse:

  ```yaml
      env_vars:
        - DAGSTER_PG_URL
        - CORPSCOUT_S3_ENDPOINT
        - CORPSCOUT_S3_ACCESS_KEY
        - CORPSCOUT_S3_SECRET_KEY
        - CLICKHOUSE_HOST
        - CLICKHOUSE_PORT
        - CLICKHOUSE_USER
        - CLICKHOUSE_PASSWORD
        - CLICKHOUSE_DATABASE
        - CLICKHOUSE_SECURE
  ```

  Verification:

  ```bash
  cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
  docker compose build
  docker compose run --rm dagster-code python -c "import clickhouse_connect; print(clickhouse_connect.__version__)"
  ```

- [x] 2. Add RustFS read helpers

  Files:

  - `corpscout/dagster/dagster_corpscout/resources/rustfs.py`
  - `corpscout/dagster/tests/test_rustfs.py`

  Add tests first:

  ```python
  def test_get_json_reads_manifest(rustfs, s3_client):
      s3_client.create_bucket(Bucket="source-finland-prhytj")
      s3_client.put_object(
          Bucket="source-finland-prhytj",
          Key="runs/20260611T100000Z-abc12345/manifest.json",
          Body=b'{"run_id":"20260611T100000Z-abc12345","artifacts":[]}',
      )

      manifest = rustfs.get_json(
          "source-finland-prhytj",
          "runs/20260611T100000Z-abc12345/manifest.json",
      )

      assert manifest["run_id"] == "20260611T100000Z-abc12345"
  ```

  ```python
  def test_latest_manifest_uses_timestamp_sorted_completed_manifest(rustfs, s3_client):
      s3_client.create_bucket(Bucket="source-finland-prhytj")
      s3_client.put_object(
          Bucket="source-finland-prhytj",
          Key="runs/20260611T100000Z-aaaaaaaa/manifest.json",
          Body=b'{"run_id":"old","artifacts":[]}',
      )
      s3_client.put_object(
          Bucket="source-finland-prhytj",
          Key="runs/20260611T110000Z-bbbbbbbb/manifest.json",
          Body=b'{"run_id":"new","artifacts":[]}',
      )

      manifest = rustfs.latest_manifest("source-finland-prhytj")

      assert manifest["run_id"] == "new"
  ```

  Implement:

  ```python
  def get_json(self, bucket: str, key: str) -> dict:
      response = self.client().get_object(Bucket=bucket, Key=key)
      with response["Body"] as body:
          return json.loads(body.read().decode("utf-8"))

  def open_object(self, bucket: str, key: str):
      return self.client().get_object(Bucket=bucket, Key=key)["Body"]

  def latest_manifest(self, bucket: str, prefix: str = "runs/") -> dict:
      paginator = self.client().get_paginator("list_objects_v2")
      keys: list[str] = []
      for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
          for item in page.get("Contents", []):
              key = item["Key"]
              if key.endswith("/manifest.json"):
                  keys.append(key)
      if not keys:
          raise FileNotFoundError(f"no manifests found in s3://{bucket}/{prefix}")
      return self.get_json(bucket, sorted(keys)[-1])
  ```

  Keep `open_object` streaming. Do not download the NDJSON snapshot to local disk.

- [x] 3. Add ClickHouse resource

  Files:

  - `corpscout/dagster/dagster_corpscout/resources/clickhouse.py`
  - `corpscout/dagster/tests/test_clickhouse_resource.py`

  Test first with a monkeypatched `clickhouse_connect.get_client`:

  ```python
  def test_clickhouse_resource_builds_client(monkeypatch):
      calls = []

      def fake_get_client(**kwargs):
          calls.append(kwargs)
          return object()

      monkeypatch.setattr("clickhouse_connect.get_client", fake_get_client)

      resource = ClickHouseResource(
          host="companycollect",
          port=8123,
          username="default",
          password="secret",
          database="corpscout_sources",
          secure=False,
      )

      resource.client()

      assert calls == [
          {
              "host": "companycollect",
              "port": 8123,
              "username": "default",
              "password": "secret",
              "database": "corpscout_sources",
              "secure": False,
          }
      ]
  ```

  Implement:

  ```python
  from collections.abc import Sequence

  import clickhouse_connect
  from dagster import ConfigurableResource


  class ClickHouseResource(ConfigurableResource):
      host: str
      port: int | str = 8123
      username: str = "default"
      password: str
      database: str = "corpscout_sources"
      secure: bool | str = False

      def client(self):
          secure = self.secure
          if isinstance(secure, str):
              secure = secure.lower() == "true"
          return clickhouse_connect.get_client(
              host=self.host,
              port=int(self.port),
              username=self.username,
              password=self.password,
              database=self.database,
              secure=secure,
          )

      def truncate_tables(self, client, tables: Sequence[str]) -> None:
          for table in tables:
              client.command(f"TRUNCATE TABLE IF EXISTS {table}")

      def insert_rows(self, client, table: str, columns: Sequence[str], rows: Sequence[dict]) -> None:
          if not rows:
              return
          data = [[row.get(column) for column in columns] for row in rows]
          client.insert(table, data, column_names=list(columns))
  ```

  Table names and column names are controlled by source constants, not user input.
  Importers should create one client at the start of the asset run and reuse it for all truncates/inserts in that asset run.

- [x] 4. Add PRH YTJ table metadata

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/tables.py`
  - `corpscout/dagster/tests/test_finland_prhytj_tables.py`

  Implement source-specific table metadata by porting the existing ClickHouse column order from:

  - `corpscout/scheduler/internal/companysources/finland/prhytj/rows.go`
  - `corpscout/scheduler/internal/companysources/finland/prhytj/code_lists.go`

  Required public constants:

  ```python
  NORMALIZED_TABLE_COLUMNS: dict[str, list[str]]
  NORMALIZED_TABLES: list[str]
  CODE_LIST_TABLE = "fi_prhytj_code_lists"
  CODE_LIST_COLUMNS: list[str]
  ```

  Tests:

  ```python
  def test_normalized_tables_match_existing_clickhouse_tables():
      assert NORMALIZED_TABLES == [
          "fi_prhytj_identifiers",
          "fi_prhytj_statuses",
          "fi_prhytj_names",
          "fi_prhytj_business_lines",
          "fi_prhytj_business_line_descriptions",
          "fi_prhytj_websites",
          "fi_prhytj_company_forms",
          "fi_prhytj_company_form_descriptions",
          "fi_prhytj_company_situations",
          "fi_prhytj_company_situation_descriptions",
          "fi_prhytj_registered_entries",
          "fi_prhytj_registered_entry_descriptions",
          "fi_prhytj_addresses",
          "fi_prhytj_address_post_offices",
      ]
  ```

  ```python
  def test_code_list_columns_match_clickhouse_table():
      assert CODE_LIST_COLUMNS == [
          "country_iso2",
          "source_slug",
          "source_run_id",
          "file_run_id",
          "file_key",
          "code_list",
          "language_code",
          "code",
          "description",
          "source_line_number",
          "source_payload_hash",
          "ingested_at",
          "source_export_id",
      ]
  ```

  Keep these constants boring and explicit. Do not introspect ClickHouse at import time.

- [x] 5. Add streaming snapshot parser

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/parser.py`
  - `corpscout/dagster/tests/test_finland_prhytj_parser.py`

  Match the current parser behavior:

  - Iterate line by line from a binary stream.
  - Keep 1-based physical source line numbers.
  - Skip blank/whitespace-only lines.
  - Decode trimmed JSON.
  - Compute `source_payload_hash` from the raw line bytes with trailing newline removed by line iteration, not from the trimmed JSON bytes.
  - Raise a clear `ValueError` with source line number on malformed JSON.

  Test first:

  ```python
  def test_parse_snapshot_records_line_number_and_raw_line_hash():
      body = io.BytesIO(
          b'  {"businessId":{"value":"1234567-8"},"names":[]}  \n'
          b"\n"
          b'{"businessId":{"value":"8765432-1"},"names":[]}\n'
      )

      records = list(parse_snapshot(body))

      assert [record.line_number for record in records] == [1, 3]
      assert records[0].payload_hash == hashlib.sha256(
          b'  {"businessId":{"value":"1234567-8"},"names":[]}  '
      ).hexdigest()
      assert records[0].payload["businessId"]["value"] == "1234567-8"
  ```

  Implementation shape:

  ```python
  @dataclass(frozen=True)
  class ParsedRecord:
      line_number: int
      payload_hash: str
      payload: dict[str, Any]


  def parse_snapshot(stream: BinaryIO) -> Iterator[ParsedRecord]:
      for line_number, raw_line in enumerate(stream, start=1):
          raw_line = raw_line.rstrip(b"\n")
          if raw_line.endswith(b"\r"):
              raw_line = raw_line[:-1]
          if not raw_line.strip():
              continue
          payload_hash = hashlib.sha256(raw_line).hexdigest()
          try:
              payload = json.loads(raw_line.strip())
          except json.JSONDecodeError as exc:
              raise ValueError(f"malformed PRH YTJ snapshot JSON on line {line_number}") from exc
          yield ParsedRecord(line_number=line_number, payload_hash=payload_hash, payload=payload)
  ```

- [x] 6. Add Python normalizer for company records

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/normalizer.py`
  - `corpscout/dagster/tests/test_finland_prhytj_normalizer.py`

  Port the transformation rules directly from:

  - `corpscout/scheduler/internal/companysources/finland/prhytj/normalize.go`
  - `corpscout/scheduler/internal/companysources/finland/prhytj/rows.go`

  Data model:

  ```python
  @dataclass(frozen=True)
  class ImportRun:
      run_id: str
      source_export_id: uuid.UUID
      ingested_at: datetime
  ```

  Public API:

  ```python
  def normalize_record(run: ImportRun, parsed: ParsedRecord) -> dict[str, list[dict]]:
      ...
  ```

  Shared helpers:

  ```python
  def source_item_hash(*parts: object) -> str:
      return hashlib.sha256("\x00".join("" if part is None else str(part) for part in parts).encode("utf-8")).hexdigest()


  def is_current(end_date: str | None) -> bool:
      return not (end_date or "").strip()


  def normalize_website(raw: str | None) -> str:
      value = (raw or "").strip()
      if not value:
          return ""
      parsed = urllib.parse.urlparse(value)
      if parsed.scheme:
          return value
      return f"https://{value}"
  ```

  Minimum behavioral tests:

  ```python
  def test_normalize_record_emits_lineage_status_identifier_and_name():
      run = ImportRun(
          run_id="20260611T100000Z-abc12345",
          source_export_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
          ingested_at=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
      )
      parsed = ParsedRecord(
          line_number=7,
          payload_hash="payload-hash",
          payload={
              "businessId": {"type": "businessId", "value": "1234567-8", "registrationDate": "2020-01-01"},
              "names": [{"name": "Example Oy", "type": "1", "version": 1, "registrationDate": "2020-01-01"}],
              "tradeRegisterStatus": "1",
              "status": "2",
              "registrationDate": "2020-01-01",
              "lastModified": "2026-06-01T00:00:00Z",
          },
      )

      rows = normalize_record(run, parsed)

      assert rows["fi_prhytj_identifiers"][0]["business_id"] == "1234567-8"
      assert rows["fi_prhytj_identifiers"][0]["identifier_scope"] == "business_id"
      assert rows["fi_prhytj_statuses"][0]["source_line_number"] == 7
      assert rows["fi_prhytj_statuses"][0]["source_payload_hash"] == "payload-hash"
      assert rows["fi_prhytj_statuses"][0]["lifecycle_status"] == "active"
      assert rows["fi_prhytj_names"][0]["is_primary"] is True
  ```

  ```python
  def test_normalize_record_marks_ceased_when_end_date_or_trade_register_status_3():
      ...
      assert rows["fi_prhytj_statuses"][0]["lifecycle_status"] == "ceased"
      assert rows["fi_prhytj_statuses"][0]["is_active"] is False
  ```

  ```python
  def test_normalize_record_normalizes_website_host_and_path():
      ...
      assert website["normalized_url"] == "https://example.fi/path"
      assert website["host"] == "example.fi"
      assert website["path"] == "/path"
  ```

  Required row rules:

  - `country_iso2 = "FI"`.
  - `source_slug = "prhytj"`.
  - `source_run_id = run.run_id`.
  - `source_record_id = business_id`.
  - `business_id = payload["businessId"]["value"]`.
  - `source_line_number = parsed.line_number`.
  - `source_payload_hash = parsed.payload_hash`.
  - `ingested_at = run.ingested_at`.
  - `source_export_id = run.source_export_id`.
  - Position fields are 1-based.
  - Missing nested objects/lists are treated as empty values/lists.
  - `lifecycle_status = "ceased"` when `endDate` is non-empty or `tradeRegisterStatus == "3"`, otherwise `"active"`.
  - `is_active = lifecycle_status == "active"`.
  - Main business line emits one `fi_prhytj_business_lines` row when the type is present or descriptions exist.
  - Website emits one row only when `website.url` is non-empty after trimming.
  - Description child rows reference their parent item hash with the correct `*_item_hash` column.

- [x] 7. Add normalized table batch importer

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/importer.py`
  - `corpscout/dagster/tests/test_finland_prhytj_importer.py`

  Public API:

  ```python
  def import_normalized_snapshot(
      *,
      clickhouse: ClickHouseResource,
      stream: BinaryIO,
      run_id: str,
      truncate: bool = True,
      batch_size: int = 1000,
  ) -> dict[str, int]:
      ...
  ```

  Test first with a fake ClickHouse object:

  ```python
  class FakeClickHouse:
      def __init__(self):
          self.truncated = []
          self.inserts = []
          self.client_object = object()

      def client(self):
          return self.client_object

      def truncate_tables(self, client, tables):
          assert client is self.client_object
          self.truncated.extend(tables)

      def insert_rows(self, client, table, columns, rows):
          assert client is self.client_object
          self.inserts.append((table, list(columns), list(rows)))
  ```

  ```python
  def test_import_normalized_snapshot_truncates_and_batches_rows():
      fake = FakeClickHouse()
      stream = io.BytesIO(
          b'{"businessId":{"value":"1234567-8"},"names":[{"name":"Example Oy","type":"1","version":1}]}\n'
      )

      counts = import_normalized_snapshot(
          clickhouse=fake,
          stream=stream,
          run_id="20260611T100000Z-abc12345",
          batch_size=1,
      )

      assert fake.truncated == NORMALIZED_TABLES
      assert counts["fi_prhytj_identifiers"] == 1
      assert any(table == "fi_prhytj_names" for table, _, _ in fake.inserts)
  ```

  Implementation rules:

  - Create one ClickHouse client with `client = clickhouse.client()` and reuse it for every truncate and insert in this asset run.
  - Generate one `source_export_id = uuid.uuid4()` per import asset run.
  - Generate one `ingested_at = datetime.now(timezone.utc)` per import asset run.
  - Parse the stream once.
  - Normalize each parsed record and append rows into per-table buffers.
  - Flush when a table buffer reaches `batch_size`.
  - Flush all tables at EOF.
  - Return a count per table.
  - Default `truncate=True` for this first full-snapshot slice.
  - Keep import errors fatal so Dagster retries the asset.

- [x] 8. Add code-list parser and importer

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/code_lists.py`
  - `corpscout/dagster/tests/test_finland_prhytj_code_lists.py`

  Match the existing behavior from `code_lists.go`:

  - TSV row format is `code<TAB>description`.
  - Keep 1-based physical line numbers.
  - Skip blank lines.
  - Trim trailing `\r`.
  - Trim whitespace around code and description.
  - Raise on malformed rows without a tab.
  - `source_payload_hash = sha256(file_key + "\x00" + line)`.
  - `country_iso2 = "FI"`.
  - `source_slug = "prhytj"`.
  - `file_run_id = run_id`.
  - `source_run_id = run_id`.
  - `source_export_id` and `ingested_at` are generated once per code-list import asset run.

  Test first:

  ```python
  def test_parse_code_list_rows_sets_metadata_and_hash():
      run = CodeListRun(
          run_id="20260611T100000Z-abc12345",
          source_export_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
          ingested_at=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
      )
      rows = list(
          parse_code_list_rows(
              stream=io.BytesIO(b"1\\tActive\\r\\n\\n"),
              run=run,
              file_key="codelist_STATUS3_en",
              code_list="STATUS3",
              language_code="en",
          )
      )

      assert rows[0]["code"] == "1"
      assert rows[0]["description"] == "Active"
      assert rows[0]["source_line_number"] == 1
      assert rows[0]["source_payload_hash"] == source_item_hash("codelist_STATUS3_en", "1\tActive")
  ```

  Public importer:

  ```python
  def import_code_lists(
      *,
      clickhouse: ClickHouseResource,
      objects: Iterable[CodeListObject],
      run_id: str,
      truncate: bool = True,
      batch_size: int = 1000,
  ) -> int:
      ...
  ```

  `CodeListObject` should carry `file_key`, `code_list`, `language_code`, and a binary stream factory or stream. A stream factory is safer because boto3 response bodies are one-shot.

- [x] 9. Add Dagster assets and resource wiring

  Files:

  - `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/assets.py`
  - `corpscout/dagster/dagster_corpscout/definitions.py`
  - `corpscout/dagster/tests/test_definitions.py`

  Add assets:

  ```python
  @dg.asset(
      key_prefix=[spec.SOURCE_NAME],
      name="normalized_tables",
      group_name=spec.SOURCE_NAME,
      deps=[raw_snapshot],
      retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
      op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
  )
  def normalized_tables(
      context: dg.AssetExecutionContext,
      rustfs: RustFSResource,
      clickhouse: ClickHouseResource,
  ) -> dg.MaterializeResult:
      manifest = rustfs.latest_manifest(spec.BUCKET)
      source = next(artifact for artifact in manifest["artifacts"] if artifact["key"] == "source")
      with rustfs.open_object(spec.BUCKET, source["object_key"]) as stream:
          counts = import_normalized_snapshot(
              clickhouse=clickhouse,
              stream=stream,
              run_id=manifest["run_id"],
          )
      return dg.MaterializeResult(
          metadata={
              "run_id": manifest["run_id"],
              "tables": len(counts),
              "rows": sum(counts.values()),
              **{f"rows_{table}": count for table, count in counts.items()},
          }
      )
  ```

  ```python
  @dg.asset(
      key_prefix=[spec.SOURCE_NAME],
      name="code_lists",
      group_name=spec.SOURCE_NAME,
      deps=[raw_snapshot],
      retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
      op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
  )
  def code_lists(
      context: dg.AssetExecutionContext,
      rustfs: RustFSResource,
      clickhouse: ClickHouseResource,
  ) -> dg.MaterializeResult:
      manifest = rustfs.latest_manifest(spec.BUCKET)
      objects = code_list_objects_from_manifest(manifest, rustfs, spec.BUCKET)
      imported = import_code_lists(
          clickhouse=clickhouse,
          objects=objects,
          run_id=manifest["run_id"],
      )
      return dg.MaterializeResult(metadata={"run_id": manifest["run_id"], "rows": imported})
  ```

  Wire resources:

  ```python
  from dagster_corpscout.resources.clickhouse import ClickHouseResource
  from dagster_corpscout.sources.finland_prhytj.assets import code_lists, normalized_tables, raw_snapshot

  defs = dg.Definitions(
      assets=[raw_snapshot, normalized_tables, code_lists],
      jobs=[pull_job],
      schedules=[pull_schedule],
      resources={
          "rustfs": RustFSResource(...),
          "clickhouse": ClickHouseResource(
              host=dg.EnvVar("CLICKHOUSE_HOST"),
              port=dg.EnvVar("CLICKHOUSE_PORT"),
              username=dg.EnvVar("CLICKHOUSE_USER"),
              password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
              database=dg.EnvVar("CLICKHOUSE_DATABASE"),
              secure=dg.EnvVar("CLICKHOUSE_SECURE"),
          ),
      },
  )
  ```

  Test:

  ```python
  def test_definitions_include_finland_prhytj_import_assets():
      keys = {asset.key for asset in defs.assets}
      assert dg.AssetKey(["finland_prhytj", "raw_snapshot"]) in keys
      assert dg.AssetKey(["finland_prhytj", "normalized_tables"]) in keys
      assert dg.AssetKey(["finland_prhytj", "code_lists"]) in keys
  ```

- [x] 10. Add local and live validation commands

  Run unit tests:

  ```bash
  cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
  pytest
  ```

  Rebuild Dagster image:

  ```bash
  cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
  docker compose up -d --build --force-recreate
  ```

  Confirm Dagster webserver:

  ```bash
  curl -fsS http://localhost:3500 >/dev/null
  ```

  Confirm run container environment has ClickHouse variables:

  ```bash
  docker compose run --rm dagster-code python - <<'PY'
  import os
  required = [
      "CLICKHOUSE_HOST",
      "CLICKHOUSE_PORT",
      "CLICKHOUSE_USER",
      "CLICKHOUSE_PASSWORD",
      "CLICKHOUSE_DATABASE",
      "CLICKHOUSE_SECURE",
  ]
  missing = [name for name in required if not os.environ.get(name)]
  if missing:
      raise SystemExit(f"missing env vars: {missing}")
  print("clickhouse env ok")
  PY
  ```

  Confirm ClickHouse tables exist before materialization:

  ```bash
  docker compose run --rm dagster-code python - <<'PY'
  import os
  import clickhouse_connect

  client = clickhouse_connect.get_client(
      host=os.environ["CLICKHOUSE_HOST"],
      port=int(os.environ["CLICKHOUSE_PORT"]),
      username=os.environ["CLICKHOUSE_USER"],
      password=os.environ["CLICKHOUSE_PASSWORD"],
      database=os.environ["CLICKHOUSE_DATABASE"],
      secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true",
  )
  rows = client.query("SHOW TABLES LIKE 'fi_prhytj_%'").result_rows
  print(len(rows), "fi_prhytj tables")
  if len(rows) < 15:
      raise SystemExit("expected at least 15 PRH YTJ tables")
  PY
  ```

  Materialize in Dagster UI:

  - `finland_prhytj/raw_snapshot`
  - `finland_prhytj/normalized_tables`
  - `finland_prhytj/code_lists`

  Confirm row counts:

  ```bash
  docker compose run --rm dagster-code python - <<'PY'
  import os
  import clickhouse_connect

  client = clickhouse_connect.get_client(
      host=os.environ["CLICKHOUSE_HOST"],
      port=int(os.environ["CLICKHOUSE_PORT"]),
      username=os.environ["CLICKHOUSE_USER"],
      password=os.environ["CLICKHOUSE_PASSWORD"],
      database=os.environ["CLICKHOUSE_DATABASE"],
      secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true",
  )
  for table in [
      "fi_prhytj_identifiers",
      "fi_prhytj_statuses",
      "fi_prhytj_names",
      "fi_prhytj_code_lists",
  ]:
      count = client.query(f"SELECT count() FROM {table}").result_rows[0][0]
      print(table, count)
      if count == 0:
          raise SystemExit(f"{table} is empty")
  PY
  ```

## Failure and Retry Behavior

- `raw_snapshot` already has a per-source concurrency key.
- `normalized_tables` and `code_lists` share `finland_prhytj:clickhouse` concurrency key so they do not truncate/insert concurrently.
- Both import assets are full-refresh for this slice: truncate first, then insert.
- If an import fails after truncate, the asset run fails and should be retried from the same RustFS manifest.
- This is acceptable for the first slice because the existing system is not serving production reads. A later slice can replace truncate/insert with partitioned run-versioned tables or `ReplacingMergeTree` promotion.

## Notes for Implementation

- Keep PRH YTJ logic source-specific. Do not introduce generic source-import abstractions yet.
- Keep table/column constants explicit. The existing migrations are the schema source of truth.
- Do not write temporary snapshot files unless ClickHouse driver or parser behavior forces it.
- Do not log credentials or full payloads.
- Prefer small tests with representative records over copying a huge fixture.
- The normalizer is intentionally Python code now; do not call the old Go importer from Dagster.

## Self-Review Checklist

- [x] Plan uses existing ClickHouse tables rather than generated staging tables.
- [x] Plan keeps Dagster as the transformation/orchestration surface.
- [x] Plan avoids dbt, NACE, XBRL, and Go importer parity in this first slice.
- [x] Plan includes run-container env wiring, not only webserver/code-location env wiring.
- [x] Plan uses streaming reads from RustFS.
- [x] Plan defines deterministic lineage fields and item hash behavior.
- [x] Plan includes tests before implementation for parser, normalizer, code-list importer, ClickHouse resource, and asset wiring.
- [x] Plan includes live validation commands against `companycollect`.
