# Dagster v2 Project — Finland Reference Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `corpscout/dagster_v2/` — a separate Dagster project implementing the source-pipeline design (`docs/superpowers/specs/2026-06-12-dagster-source-pipeline-design.md`) — with Finland as the complete reference example: `prh_ytj` (snapshot archetype, ported) and `prh_xbrl` (window archetype, new, financial statements → RustFS raw XML → ClickHouse parsed tables → derived metrics).

**Architecture:** `dagster_v2/` is a sibling **project** to `dagster/` with its own `pyproject.toml`, venv, Docker image, and tests — v2 work can never break the running v1 stack, and cutover is "delete `dagster/`, rename `dagster_v2/`". The Python package keeps the name `dagster_corpscout` (separate venv/image, so no conflict; ports need no import rewriting and cutover needs no renames). v1 is being **removed after this**, so `dagster_v2/` carries the **full Dagster stack** — `docker-compose.yml` copied from v1 (code server + webserver + daemon), own `workspace.yaml`/`dagster.yaml` — reusing the same Dagster Postgres, network name, and UI port; deployment switch = stop v1's compose, start v2's. v1 files are never modified. Pipeline design: everything is a partitioned asset graph — PRH XBRL pulls partitioned by registration month (the PRH discovery API is natively windowed by registration date), layers connected via `AutomationCondition.eager()`, idempotency in deterministic RustFS keys and `ReplacingMergeTree` tables. No Temporal, no dlt, no custom sensors, no manifest-driven processing state for the XBRL source.

**Tech Stack:** Python 3.12, Dagster ≥1.10, boto3/RustFS, requests, lxml, clickhouse-connect, golang-migrate for ClickHouse migrations.

**Non-goals (explicitly out of scope):**
- Taxonomy code map loader / Finnish label enrichment (`*_label_fi` columns stay NULL/empty; `fi_prh_xbrl_taxonomy_code_map` stays empty).
- Inline (HTML) XBRL support — parser handles XML instance documents only.
- v1 decommission/cutover (separate task after v2 runs in parallel).
- The "entity" scaffold archetype template (only `snapshot` and `window` now).

---

## File Structure

```
corpscout/
├── dagster/                                     # v1 — fully untouched; removed after cutover
├── dagster_v2/                                  # NEW project
│   ├── pyproject.toml
│   ├── .gitignore
│   ├── .env.example                             # copied from v1, DAGSTER_RUN_IMAGE → v2 image
│   ├── dagster.yaml                             # copied from v1 (storage, run launcher, queue)
│   ├── workspace.yaml                           # single grpc location: dagster-code:4266
│   ├── Dockerfile
│   ├── docker-compose.yml                       # full stack copied from v1: code + webserver + daemon
│   ├── README.md
│   ├── dagster_corpscout/                       # same package name as v1, by design
│   │   ├── __init__.py
│   │   ├── definitions.py                       # Definitions + automation sensor
│   │   ├── registry.py                          # aggregates SourceBundles
│   │   ├── source_bundle.py                     # copied from v1
│   │   ├── source_scaffold.py                   # v1 + --archetype snapshot|window
│   │   ├── lib/                                 # copied from v1 (streaming, manifest)
│   │   ├── resources/
│   │   │   ├── clickhouse.py                    # copied from v1
│   │   │   └── rustfs.py                        # v1 + ensure_bucket/list_keys/get_bytes
│   │   └── sources/finland/
│   │       ├── prh_ytj/                         # ported from v1 + eager automation
│   │       └── prh_xbrl/                        # NEW — window archetype
│   │           ├── __init__.py                  # SourceBundle
│   │           ├── spec.py                      # constants + object key functions
│   │           ├── partitions.py                # MonthlyPartitionsDefinition
│   │           ├── client.py                    # PRH XBRL API client
│   │           ├── parser.py                    # XML → raw-first rows (pure)
│   │           ├── tables.py                    # ClickHouse table/column contract
│   │           ├── importer.py                  # rows → ClickHouse
│   │           ├── metrics.py                   # metric mapping + derive (pure)
│   │           ├── checks.py                    # asset checks
│   │           ├── jobs.py                      # window job + on-demand company job
│   │           ├── schedules.py                 # monthly schedule (STOPPED)
│   │           └── assets/
│   │               ├── __init__.py
│   │               ├── external.py              # source_system AssetSpec
│   │               ├── raw.py                   # API → RustFS
│   │               ├── parsed.py                # RustFS → ClickHouse
│   │               └── derived.py               # facts → financial_metrics
│   └── tests/                                   # normal tests/ tree (has __init__.py)
└── clickhouse/migrations/
    └── 000011_create_finland_prh_xbrl_financial_tables.up.sql   # modify: Replacing engines
```

---

### Task 0: Commit Pending Work and Remove the Aborted Go Attempt

**Files:**
- Commit (already modified): `corpscout/dagster/README.md`, `corpscout/dagster/dagster_corpscout/source_scaffold.py`, `corpscout/dagster/dagster_corpscout/sources/finland/prh_ytj/spec.py`, `corpscout/dagster/tests/test_definitions.py`, `corpscout/dagster/tests/test_source_conventions.py`
- Delete: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go`
- Delete: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go`
- Delete: `corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go`
- Modify: revert PRH XBRL company-pull hunks in `corpscout/scheduler/internal/app/temporal.go`, `corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go`, `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`, `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`, `corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go`

- [ ] **Step 1: Commit the pending "group by source" rename**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/README.md \
  corpscout/dagster/dagster_corpscout/source_scaffold.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_ytj/spec.py \
  corpscout/dagster/tests/test_definitions.py \
  corpscout/dagster/tests/test_source_conventions.py
git commit -m "Group Dagster assets by source instead of country"
```

Expected: clean commit; `git status` no longer lists those five files as modified.

- [ ] **Step 2: Confirm the unwanted Go symbols exist**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n 'FinlandPRHXBRLCompanyPull|PullCompanyStatementsToS3|buildCompanyFinancialsURL' corpscout/scheduler/internal
```

Expected: matches only from the interrupted implementation. If no matches, skip to Step 5.

- [ ] **Step 3: Delete the untracked Go files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rm -f corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go \
  corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go \
  corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go
```

- [ ] **Step 4: Remove the interrupted hunks from the five modified Go files**

Remove only these additions (use `git diff <file>` to see them):
- `FinlandPRHXBRLCompanyPull` workflow registration and `FinlandPRHXBRLCompanyPullActivity` activity registration in `internal/app/temporal.go`
- `buildCompanyFinancialsURL` / `downloadCompanyFinancialsPage` in `internal/companysources/finland/prhxbrl/client.go`
- `s3 *s3client.Client` field and `FinlandPRHXBRLCompanyPullActivity` method in `internal/temporal/actions/companysources/actions.go`
- The `/api/v1/workflows/finland/prh-xbrl/company-pull` test in `internal/httpapi/workflow_triggers_test.go`
- `TestFinlandPRHXBRLCompanyPullRunsActivityWithWorkflowIdentity` in `internal/temporal/workflow/companysources/workflow_test.go`

If a file has no PRH-XBRL-company-pull diff, run `git checkout -- <file>` instead.

- [ ] **Step 5: Verify and test**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n 'FinlandPRHXBRLCompanyPull|PullCompanyStatementsToS3|buildCompanyFinancialsURL' corpscout/scheduler/internal
cd corpscout/scheduler
GOWORK=off go test ./internal/temporal/... ./internal/app ./internal/httpapi ./internal/companysources/... -count=1
```

Expected: rg prints nothing; tests PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add -A corpscout/scheduler/internal
git commit -m "Remove aborted Go PRH XBRL workflow attempt"
```

---

### Task 1: New Project Skeleton and Platform Core

**Files:**
- Create: `corpscout/dagster_v2/pyproject.toml`, `.gitignore`, `.env.example`, `.env` (local only), `dagster.yaml`
- Create: `corpscout/dagster_v2/dagster_corpscout/__init__.py`
- Create: `corpscout/dagster_v2/dagster_corpscout/source_bundle.py`, `lib/`, `resources/` (copied from v1)
- Create: `corpscout/dagster_v2/dagster_corpscout/registry.py`, `definitions.py`
- Test: `corpscout/dagster_v2/tests/__init__.py`, `tests/test_definitions.py`, `tests/test_rustfs_verbs.py`, `tests/test_source_conventions.py`

- [ ] **Step 1: Create the project skeleton**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
mkdir -p dagster_v2/dagster_corpscout dagster_v2/tests
cd dagster_v2
touch dagster_corpscout/__init__.py tests/__init__.py
cp ../dagster/.env.example .env.example
cp ../dagster/.env .env            # local only, gitignored
cp ../dagster/dagster.yaml dagster.yaml
```

Create `corpscout/dagster_v2/.gitignore`:

```gitignore
.venv/
.env
__pycache__/
*.egg-info/
```

Create `corpscout/dagster_v2/pyproject.toml`:

```toml
[project]
name = "dagster-corpscout-v2"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dagster>=1.10,<2",
    "dagster-postgres>=0.26",
    "dagster-docker>=0.26",
    "boto3>=1.34",
    "requests>=2.32",
    "clickhouse-connect>=0.8",
    "lxml>=5.2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "moto[s3]>=5",
    "responses>=0.25",
    "dagster-webserver>=1.10,<2",
]

[project.scripts]
dagster-corpscout-scaffold-source = "dagster_corpscout.source_scaffold:main"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["dagster_corpscout*"]

[tool.dagster]
module_name = "dagster_corpscout.definitions"
```

- [ ] **Step 2: Copy the proven v1 core modules (same package name — no import rewriting)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
cp ../dagster/dagster_corpscout/source_bundle.py dagster_corpscout/source_bundle.py
cp -R ../dagster/dagster_corpscout/lib dagster_corpscout/lib
cp -R ../dagster/dagster_corpscout/resources dagster_corpscout/resources
/usr/bin/find dagster_corpscout -name __pycache__ -type d -exec rm -rf {} +
```

- [ ] **Step 3: Create the venv**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
python3.12 -m venv .venv || python3 -m venv .venv
./.venv/bin/python -m pip install -e '.[dev]'
```

Expected: install succeeds.

- [ ] **Step 4: Write failing tests for the new RustFS verbs and definitions**

Create `tests/test_rustfs_verbs.py`:

```python
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource


@mock_aws
def test_ensure_bucket_is_idempotent_and_list_get_roundtrip():
    resource = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    resource.ensure_bucket("source-test")
    resource.ensure_bucket("source-test")
    resource.put_bytes("source-test", "companies/0176460-0/2024-09-30.xml", b"<xbrl />")

    assert resource.list_keys("source-test", "companies/") == [
        "companies/0176460-0/2024-09-30.xml"
    ]
    assert resource.get_bytes("source-test", "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
```

Create `tests/test_definitions.py`:

```python
import dagster as dg


def test_definitions_load_with_automation_sensor():
    from dagster_corpscout.definitions import defs

    assert defs.resolve_asset_graph() is not None
    sensor_names = {sensor.name for sensor in defs.sensors}
    assert "automation_condition_sensor" in sensor_names


def test_automation_sensor_defaults_to_stopped():
    from dagster_corpscout.definitions import defs

    sensor = next(s for s in defs.sensors if s.name == "automation_condition_sensor")
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
```

Create `tests/test_source_conventions.py`:

```python
import importlib
from pathlib import Path

import dagster as dg

from dagster_corpscout.registry import source_bundles, source_modules
from dagster_corpscout.source_bundle import SourceBundle

LAYER_VOCABULARY = {"external", "raw", "parsed", "reference", "normalized", "mapping", "serving"}


def test_registered_source_packages_follow_layout_convention():
    seen_source_names = set()
    seen_asset_prefixes = set()

    for module_name in source_modules:
        module = importlib.import_module(module_name)
        source_bundle = module.source_bundle

        assert isinstance(source_bundle, SourceBundle)
        assert source_bundle in source_bundles
        assert source_bundle.source_name not in seen_source_names
        assert source_bundle.asset_key_prefix not in seen_asset_prefixes
        assert len(source_bundle.asset_key_prefix) == 3
        assert source_bundle.asset_key_prefix[0] == "sources"

        seen_source_names.add(source_bundle.source_name)
        seen_asset_prefixes.add(source_bundle.asset_key_prefix)

        country, source = source_bundle.asset_key_prefix[1:]
        assert module_name.endswith(f".{country}.{source}")
        assert module.spec.GROUP_NAME == f"source_{country}_{source}"
        assert module.spec.TAGS == {
            "country": country,
            "source": source,
            "source_name": source_bundle.source_name,
        }

        package_dir = Path(module.__file__).parent
        for relative_path in [
            "spec.py",
            "jobs.py",
            "schedules.py",
            "assets/__init__.py",
            "assets/external.py",
            "assets/raw.py",
        ]:
            assert (package_dir / relative_path).is_file(), f"{module_name} missing {relative_path}"


def test_all_assets_declare_a_layer_from_the_vocabulary():
    from dagster_corpscout.definitions import defs

    graph = defs.resolve_asset_graph()
    for node in graph.asset_nodes:
        assert node.tags.get("layer") in LAYER_VOCABULARY, node.key


def test_all_schedules_default_to_stopped():
    from dagster_corpscout.definitions import defs

    for schedule in defs.schedules:
        assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED, schedule.name
```

- [ ] **Step 5: Run the failing tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
./.venv/bin/python -m pytest tests -v
```

Expected: FAIL — `ensure_bucket` missing, `dagster_corpscout.registry` / `definitions` missing.

- [ ] **Step 6: Add the RustFS verbs**

Append to the `RustFSResource` class in `dagster_corpscout/resources/rustfs.py` (and add `from botocore.exceptions import ClientError` to the imports):

```python
    def ensure_bucket(self, bucket: str) -> None:
        try:
            self.client().create_bucket(Bucket=bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def get_bytes(self, bucket: str, key: str) -> bytes:
        response = self.client().get_object(Bucket=bucket, Key=key)
        with response["Body"] as body:
            return body.read()
```

- [ ] **Step 7: Create the registry and definitions**

Create `dagster_corpscout/registry.py`:

```python
"""Aggregates source bundles into the lists consumed by definitions.py."""

source_modules: tuple[str, ...] = ()
source_bundles: list = []

all_assets = [asset for bundle in source_bundles for asset in bundle.assets]
all_asset_checks = [check for bundle in source_bundles for check in bundle.asset_checks]
all_jobs = [job for bundle in source_bundles for job in bundle.jobs]
all_schedules = [schedule for bundle in source_bundles for schedule in bundle.schedules]
```

Create `dagster_corpscout/definitions.py`:

```python
import dagster as dg

from dagster_corpscout.registry import all_asset_checks, all_assets, all_jobs, all_schedules
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource

# Explicit (instead of Dagster's implicit default) so its existence and default
# status are part of the tested platform contract. It evaluates every asset
# with an automation_condition; enable it in the UI to activate eager cascades.
automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.STOPPED,
)

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=[automation_sensor],
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        ),
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

- [ ] **Step 8: Run tests**

```bash
./.venv/bin/python -m pytest tests -v
```

Expected: PASS (conventions tests pass trivially on the empty registry).

- [ ] **Step 9: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2
git commit -m "Add dagster_v2 project with platform core"
```

---

### Task 2: Scaffold with Archetypes

**Files:**
- Create: `corpscout/dagster_v2/dagster_corpscout/source_scaffold.py`
- Test: `corpscout/dagster_v2/tests/test_source_scaffold.py`

- [ ] **Step 1: Write failing scaffold tests**

Create `tests/test_source_scaffold.py`:

```python
import pytest

from dagster_corpscout.source_scaffold import scaffold_source


def test_snapshot_archetype_creates_v1_compatible_layout(tmp_path):
    package_dir = scaffold_source(tmp_path / "sources", country="serbia", source="apr")

    for relative_path in [
        "__init__.py",
        "spec.py",
        "jobs.py",
        "schedules.py",
        "assets/__init__.py",
        "assets/external.py",
        "assets/raw.py",
    ]:
        assert (package_dir / relative_path).is_file()
    assert not (package_dir / "partitions.py").exists()
    assert 'GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"' in (package_dir / "spec.py").read_text()


def test_window_archetype_adds_partitions_and_parsed_asset(tmp_path):
    package_dir = scaffold_source(
        tmp_path / "sources", country="france", source="inpi", archetype="window"
    )

    assert (package_dir / "partitions.py").is_file()
    assert (package_dir / "assets/parsed.py").is_file()
    partitions_text = (package_dir / "partitions.py").read_text()
    assert "MonthlyPartitionsDefinition" in partitions_text
    raw_text = (package_dir / "assets/raw.py").read_text()
    assert "partitions_def" in raw_text
    parsed_text = (package_dir / "assets/parsed.py").read_text()
    assert "AutomationCondition.eager()" in parsed_text


def test_unknown_archetype_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        scaffold_source(tmp_path / "sources", country="x", source="y", archetype="entity")
```

- [ ] **Step 2: Run failing tests**

```bash
./.venv/bin/python -m pytest tests/test_source_scaffold.py -v
```

Expected: FAIL — `source_scaffold` does not exist in the v2 project.

- [ ] **Step 3: Copy the v1 scaffold and extend with archetypes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
cp ../dagster/dagster_corpscout/source_scaffold.py dagster_corpscout/source_scaffold.py
```

(The package name is unchanged, so the copied templates' `dagster_corpscout.sources.…` imports are already correct.)

Then apply these modifications to `dagster_corpscout/source_scaffold.py`:

Replace the `scaffold_source` function with:

```python
ARCHETYPES = ("snapshot", "window")


def scaffold_source(
    sources_root: Path, *, country: str, source: str, archetype: str = "snapshot"
) -> Path:
    """Create a source-owned Dagster package skeleton under sources_root."""
    _validate_identifier("country", country)
    _validate_identifier("source", source)
    if archetype not in ARCHETYPES:
        raise ValueError(f"archetype must be one of {ARCHETYPES}: {archetype}")

    package_dir = sources_root / country / source
    if package_dir.exists() and any(package_dir.iterdir()):
        raise FileExistsError(f"source package already exists and is not empty: {package_dir}")

    assets_dir = package_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    files = {
        package_dir / "__init__.py": _init_template(country, source),
        package_dir / "spec.py": _spec_template(country, source),
        package_dir / "jobs.py": _jobs_template(country, source),
        package_dir / "schedules.py": _schedules_template(),
        assets_dir / "__init__.py": _assets_init_template(country, source),
        assets_dir / "external.py": _external_asset_template(country, source),
        assets_dir / "raw.py": _raw_asset_template(country, source),
    }
    if archetype == "window":
        files[package_dir / "partitions.py"] = _window_partitions_template(country, source)
        files[assets_dir / "raw.py"] = _window_raw_asset_template(country, source)
        files[assets_dir / "parsed.py"] = _window_parsed_asset_template(country, source)
    for path, content in files.items():
        path.write_text(content)

    country_init = sources_root / country / "__init__.py"
    country_init.touch(exist_ok=True)
    return package_dir
```

Add an `--archetype` argument in `main()` after the `--sources-root` argument:

```python
    parser.add_argument(
        "--archetype",
        choices=ARCHETYPES,
        default="snapshot",
        help="snapshot = run-keyed full dump; window = time-partitioned incremental pull",
    )
```

and change the `scaffold_source` call in `main()` to:

```python
    package_dir = scaffold_source(
        args.sources_root, country=args.country, source=args.source, archetype=args.archetype
    )
```

Add `PARTITION_START_DATE = "2024-01-01"` to the body generated by `_spec_template` (after the `TAGS` block) so window scaffolds import cleanly.

Append the three new template functions at the end of the file (before `if __name__`):

```python
def _window_partitions_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module} import spec

window_partitions = dg.MonthlyPartitionsDefinition(start_date=spec.PARTITION_START_DATE)
'''


def _window_raw_asset_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module} import spec
from {module}.assets.external import source_system
from {module}.partitions import window_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_documents",
    partitions_def=window_partitions,
    group_name=spec.GROUP_NAME,
    tags={{**spec.TAGS, "layer": "raw"}},
    deps=[source_system],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={{"dagster/concurrency_key": spec.SOURCE_NAME}},
)
def raw_documents(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    raise NotImplementedError("Download the partition window into RustFS before registering.")
'''


def _window_parsed_asset_template(country: str, source: str) -> str:
    module = f"dagster_corpscout.sources.{country}.{source}"
    return f'''import dagster as dg

from {module} import spec
from {module}.assets.raw import raw_documents
from {module}.partitions import window_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="parsed_tables",
    partitions_def=window_partitions,
    group_name=spec.GROUP_NAME,
    tags={{**spec.TAGS, "layer": "parsed"}},
    deps=[raw_documents],
    automation_condition=dg.AutomationCondition.eager(),
    op_tags={{"dagster/concurrency_key": spec.SOURCE_NAME}},
)
def parsed_tables(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    raise NotImplementedError("Parse raw objects into ClickHouse before registering.")
'''
```

- [ ] **Step 4: Run tests**

```bash
./.venv/bin/python -m pytest tests/test_source_scaffold.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/source_scaffold.py corpscout/dagster_v2/tests/test_source_scaffold.py
git commit -m "Add v2 source scaffold with snapshot and window archetypes"
```

---

### Task 3: Port prh_ytj (Snapshot Archetype + Eager Automation)

**Files:**
- Create: `dagster_v2/dagster_corpscout/sources/finland/prh_ytj/` (plain copy — same package name)
- Modify: `…/prh_ytj/assets/{normalized,code_lists,industry_mapping,explorer_cache}.py` (eager)
- Modify: `…/prh_ytj/schedules.py` (raw-only schedule)
- Modify: `dagster_corpscout/registry.py`
- Test: ported `tests/` files + `tests/test_finland_prhytj_definitions.py`

- [ ] **Step 1: Copy the source package and its tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
mkdir -p dagster_corpscout/sources/finland
touch dagster_corpscout/sources/__init__.py dagster_corpscout/sources/finland/__init__.py
cp -R ../dagster/dagster_corpscout/sources/finland/prh_ytj dagster_corpscout/sources/finland/prh_ytj
/usr/bin/find dagster_corpscout/sources -name __pycache__ -type d -exec rm -rf {} +

for f in test_finland_prhytj_checks test_finland_prhytj_code_lists test_finland_prhytj_explorer_cache \
         test_finland_prhytj_importer test_finland_prhytj_industry_mapping test_finland_prhytj_normalizer \
         test_finland_prhytj_parser test_finland_prhytj_tables test_client test_spec test_raw_snapshot \
         test_streaming test_manifest test_rustfs test_clickhouse_resource; do
  cp ../dagster/tests/$f.py tests/$f.py
done
cp ../dagster/tests/test_definitions.py tests/test_finland_prhytj_definitions.py
```

(No import rewriting anywhere — the package name is identical.)

- [ ] **Step 2: Register prh_ytj in the registry**

Replace the top of `dagster_corpscout/registry.py`:

```python
"""Aggregates source bundles into the lists consumed by definitions.py."""

from dagster_corpscout.sources.finland.prh_ytj import source_bundle as finland_prh_ytj

source_modules: tuple[str, ...] = ("dagster_corpscout.sources.finland.prh_ytj",)
source_bundles: list = [finland_prh_ytj]
```

(Keep the four comprehensions below unchanged.)

- [ ] **Step 3: Add eager automation to the four downstream assets**

In each file, add one line to the `@dg.asset(...)` decorator, directly after the `deps=[...]` line:

`assets/normalized.py`:
```python
    deps=[raw_snapshot],
    automation_condition=dg.AutomationCondition.eager(),
```

`assets/code_lists.py`:
```python
    deps=[raw_snapshot],
    automation_condition=dg.AutomationCondition.eager(),
```

`assets/industry_mapping.py`:
```python
    deps=[normalized_tables],
    automation_condition=dg.AutomationCondition.eager(),
```

`assets/explorer_cache.py`:
```python
    deps=[normalized_tables, code_lists, industry_nace_mappings],
    automation_condition=dg.AutomationCondition.eager(),
```

- [ ] **Step 4: Point the schedule at the raw pull only**

Replace `dagster_corpscout/sources/finland/prh_ytj/schedules.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj.jobs import pull_job

# Cron enters the graph at the raw layer only; the eager automation conditions
# on normalized/code_lists/mapping/serving cascade the rest.
pull_schedule = dg.ScheduleDefinition(
    name="finland_prhytj_pull_schedule",
    job=pull_job,
    cron_schedule="0 3 * * 1",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

- [ ] **Step 5: Update the ported definitions test**

In `tests/test_finland_prhytj_definitions.py`, replace `test_pull_schedule_exists_and_is_stopped` with:

```python
def test_pull_schedule_targets_raw_only_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prhytj_pull_schedule")
    assert schedule.cron_schedule == "0 3 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "finland_prhytj_pull"


def test_downstream_prhytj_assets_have_eager_automation():
    from dagster_corpscout.definitions import defs

    graph = defs.resolve_asset_graph()
    for asset_name in [
        "normalized_tables",
        "code_lists",
        "industry_nace_mappings",
        "company_explorer_cache",
    ]:
        node = graph.get(source_key(asset_name))
        assert node.automation_condition is not None, asset_name
```

- [ ] **Step 6: Run the full suite**

```bash
./.venv/bin/python -m pytest tests -v
```

Expected: PASS (the layer-vocabulary and stopped-schedule convention tests now cover six real assets).

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2
git commit -m "Port Finland PRH YTJ into dagster_v2 with eager automation"
```

---

### Task 4: ClickHouse Migration — Idempotent Engines for XBRL Tables

**Files:**
- Modify: `corpscout/clickhouse/migrations/000011_create_finland_prh_xbrl_financial_tables.up.sql` (file is still untracked, so editing in place is safe)

- [ ] **Step 1: Replace the contexts table definition**

The design requires every parsed table to be re-insert-safe (`ReplacingMergeTree` + version column). Replace the `fi_prh_xbrl_contexts` statement with (adds `parsed_at`, switches engine):

```sql
CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_contexts` (
  `statement_key` String,
  `context_id` String,
  `entity_identifier` String,
  `entity_scheme` String,
  `period_type` LowCardinality(String),
  `instant_date` Nullable(Date),
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `dimensions` Array(Tuple(
    dimension_code String,
    member_code String,
    member_label_fi String
  )),
  `mcy_member_code` Nullable(String),
  `mcy_member_label_fi` Nullable(String),
  `ref_member_code` Nullable(String),
  `ref_member_label_fi` Nullable(String),
  `is_comparative` UInt8,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
ORDER BY (`statement_key`, `context_id`);
```

- [ ] **Step 2: Replace the units table definition**

```sql
CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_units` (
  `statement_key` String,
  `unit_id` String,
  `measures` Array(String),
  `is_divide` UInt8,
  `raw_xml` String,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
ORDER BY (`statement_key`, `unit_id`);
```

- [ ] **Step 3: Switch the facts table engine**

In the `fi_prh_xbrl_facts_raw` statement, change only the engine line:

```sql
ENGINE = ReplacingMergeTree(`parsed_at`)
```

(Columns, `PARTITION BY`, `ORDER BY`, and `SETTINGS allow_nullable_key = 1` stay as they are; `parsed_at` already exists on this table. `fi_prh_xbrl_statement_documents` and `fi_prh_xbrl_metrics_long_v1` are already `ReplacingMergeTree` — leave them.)

- [ ] **Step 4: Re-apply locally**

If migration 11 was already applied to the dev ClickHouse, roll it back first (dev-only tables, no data to preserve):

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-down   # only if 000011 is currently applied
make clickhouse-migrate-up
```

Verify:

```bash
docker exec -i companyindex-dev-clickhouse-1 clickhouse-client --query \
  "SELECT name, engine FROM system.tables WHERE database='corpscout_sources' AND name LIKE 'fi_prh_xbrl%'"
```

Expected: contexts, units, facts_raw, statement_documents, metrics_long_v1, taxonomy_code_map all exist; the first three show `ReplacingMergeTree`.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000011_create_finland_prh_xbrl_financial_tables.up.sql \
  corpscout/clickhouse/migrations/000011_create_finland_prh_xbrl_financial_tables.down.sql
git commit -m "Create Finland PRH XBRL ClickHouse tables with replacing engines"
```

---

### Task 5: prh_xbrl Package — Spec, Partitions, Registration

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/` via scaffold (window archetype)
- Create/replace: `spec.py`, `partitions.py`
- Modify: `dagster_corpscout/registry.py`
- Test: `tests/test_finland_prh_xbrl_spec.py`, extend `tests/test_definitions.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_finland_prh_xbrl_spec.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


def test_object_keys_are_deterministic_and_readable():
    assert spec.document_object_key("0176460-0", "2024-09-30") == "companies/0176460-0/2024-09-30.xml"
    assert spec.window_listing_object_key("2025-01-01") == "windows/2025-01-01/listing.json"


def test_partitions_are_monthly_registration_windows():
    assert registration_month_partitions.start.strftime("%Y-%m-%d") == "2025-01-01"
```

Append to `tests/test_definitions.py`:

```python
def prh_xbrl_key(name: str) -> dg.AssetKey:
    return dg.AssetKey(["sources", "finland", "prh_xbrl", name])


def test_definitions_include_finland_prh_xbrl_assets():
    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(prh_xbrl_key("raw_xml_documents")) is not None
```

- [ ] **Step 2: Run failing tests**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_spec.py tests/test_definitions.py -v
```

Expected: FAIL — package does not exist.

- [ ] **Step 3: Scaffold the package**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
./.venv/bin/python -m dagster_corpscout.source_scaffold finland prh_xbrl \
  --sources-root dagster_corpscout/sources --archetype window
```

Expected: prints the package path.

- [ ] **Step 4: Replace spec.py**

Replace `dagster_corpscout/sources/finland/prh_xbrl/spec.py`:

```python
"""Declarative source config for Finland PRH XBRL financial statements."""

SOURCE_NAME = "finland_prh_xbrl"
COUNTRY = "finland"
SOURCE_SLUG = "prh_xbrl"
DISPLAY_NAME = "Finland PRH XBRL"
ASSET_KEY_PREFIX = ["sources", COUNTRY, SOURCE_SLUG]
GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"
TAGS = {
    "country": COUNTRY,
    "source": SOURCE_SLUG,
    "source_name": SOURCE_NAME,
}

BUCKET = "source-finland-prh-xbrl"
BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-dagster/0.1"
PARSER_VERSION = "1.0.0"
# PRH publishes digitally filed statements registered from 2025 onward.
PARTITION_START_DATE = "2025-01-01"


def document_object_key(business_id: str, financial_date: str) -> str:
    """Deterministic, company-keyed, readable. Re-downloads overwrite in place."""
    return f"companies/{business_id}/{financial_date}.xml"


def window_listing_object_key(partition_key: str) -> str:
    """Raw discovery payload for one registration-month window."""
    return f"windows/{partition_key}/listing.json"
```

- [ ] **Step 5: Replace partitions.py and rename scaffold symbols**

Replace `dagster_corpscout/sources/finland/prh_xbrl/partitions.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec

# The PRH discovery API is natively windowed by registration date
# (all_financial_statements?registeredDateStart&registeredDateEnd), so the
# registration month is the pull's unit of work, retry, and backfill.
registration_month_partitions = dg.MonthlyPartitionsDefinition(
    start_date=spec.PARTITION_START_DATE,
)
```

Rename the scaffold's symbols to match: in `assets/raw.py` change `window_partitions` → `registration_month_partitions` and asset name `raw_documents` → `raw_xml_documents`; in `assets/parsed.py` change `window_partitions` → `registration_month_partitions`, `raw_documents` → `raw_xml_documents`, and asset name `parsed_tables` → `statement_tables` (the stub bodies still raise `NotImplementedError`; Tasks 8–9 replace them). Update `assets/__init__.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents

__all__ = ["raw_xml_documents", "source_system", "statement_tables"]
```

and `__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import (
    raw_xml_documents,
    source_system,
    statement_tables,
)

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents, statement_tables),
)

__all__ = ["source_bundle"]
```

Replace the scaffold's `jobs.py` (it references the snapshot template's `raw_snapshot`) with a placeholder until Task 11:

```python
# Jobs are added in the on-demand pull task.
```

and `schedules.py`:

```python
# The monthly window schedule is added with the jobs.
```

- [ ] **Step 6: Register the bundle**

In `dagster_corpscout/registry.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl import source_bundle as finland_prh_xbrl
from dagster_corpscout.sources.finland.prh_ytj import source_bundle as finland_prh_ytj

source_modules: tuple[str, ...] = (
    "dagster_corpscout.sources.finland.prh_ytj",
    "dagster_corpscout.sources.finland.prh_xbrl",
)
source_bundles: list = [finland_prh_ytj, finland_prh_xbrl]
```

- [ ] **Step 7: Run tests**

```bash
./.venv/bin/python -m pytest tests -v
```

Expected: PASS — conventions tests now cover both sources.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2
git commit -m "Add Finland PRH XBRL v2 source package skeleton"
```

---

### Task 6: PRH XBRL API Client

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/client.py`
- Test: `tests/test_finland_prh_xbrl_client.py`

- [ ] **Step 1: Write failing client tests**

Create `tests/test_finland_prh_xbrl_client.py`:

```python
import responses

from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient

BASE = "https://example.test/opendata-xbrl-api/v3"


def _client() -> PRHXBRLClient:
    return PRHXBRLClient(base_url=BASE, user_agent="corpscout-test/1.0")


def test_iter_registration_window_paginates_until_total_results():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{BASE}/all_financial_statements",
            json={
                "totalResults": 2,
                "financials": [
                    {"businessId": "0176460-0", "financialDate": "2024-09-30", "registrationDate": "2025-01-23"}
                ],
            },
        )
        rsps.add(
            responses.GET,
            f"{BASE}/all_financial_statements",
            json={
                "totalResults": 2,
                "financials": [
                    {"businessId": "0200510-4", "financialDate": "2024-12-31", "registrationDate": "2025-01-30"}
                ],
            },
        )

        statements = list(
            _client().iter_registration_window(
                registered_date_start="2025-01-01", registered_date_end="2025-01-31"
            )
        )

        assert "registeredDateStart=2025-01-01" in rsps.calls[0].request.url
        assert "registeredDateEnd=2025-01-31" in rsps.calls[0].request.url
        assert "page=2" in rsps.calls[1].request.url
        assert rsps.calls[0].request.headers["User-Agent"] == "corpscout-test/1.0"

    assert [s.business_id for s in statements] == ["0176460-0", "0200510-4"]


def test_iter_company_financials_uses_business_id():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{BASE}/financials",
            json={
                "totalResults": 1,
                "financials": [
                    {"businessId": "0176460-0", "financialDate": "2023-09-30", "registrationDate": "2025-01-23"}
                ],
            },
        )

        statements = list(_client().iter_company_financials("0176460-0"))

        assert "businessId=0176460-0" in rsps.calls[0].request.url

    assert statements[0].financial_date == "2023-09-30"


def test_download_financial_xml_returns_bytes_and_url():
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/financial", body=b"<xbrl />")

        body, source_url = _client().download_financial_xml("0176460-0", "2023-09-30")

    assert body == b"<xbrl />"
    assert "businessId=0176460-0" in source_url
    assert "financialDate=2023-09-30" in source_url
```

- [ ] **Step 2: Run failing tests**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_client.py -v
```

Expected: FAIL — `client.py` does not exist.

- [ ] **Step 3: Implement the client**

Create `dagster_corpscout/sources/finland/prh_xbrl/client.py`:

```python
"""HTTP client for the PRH open data XBRL API v3. No Dagster imports."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class DiscoveredStatement:
    business_id: str
    financial_date: str
    registration_date: str | None = None


@dataclass(frozen=True)
class DiscoveryPage:
    total_results: int
    statements: list[DiscoveredStatement]


class PRHXBRLClient:
    def __init__(self, *, base_url: str, user_agent: str, timeout_seconds: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent

    def list_registration_window(
        self, *, registered_date_start: str, registered_date_end: str, page: int = 1
    ) -> DiscoveryPage:
        response = self.session.get(
            f"{self.base_url}/all_financial_statements",
            params={
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return _discovery_page(response.json())

    def iter_registration_window(
        self, *, registered_date_start: str, registered_date_end: str
    ) -> Iterator[DiscoveredStatement]:
        yield from _paginate(
            lambda page: self.list_registration_window(
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                page=page,
            )
        )

    def list_company_financials(self, business_id: str, *, page: int = 1) -> DiscoveryPage:
        response = self.session.get(
            f"{self.base_url}/financials",
            params={"businessId": business_id, "page": page},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return _discovery_page(response.json())

    def iter_company_financials(self, business_id: str) -> Iterator[DiscoveredStatement]:
        yield from _paginate(lambda page: self.list_company_financials(business_id, page=page))

    def download_financial_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self.session.get(
            f"{self.base_url}/financial",
            params={"businessId": business_id, "financialDate": financial_date},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content, response.url


def _paginate(fetch_page) -> Iterator[DiscoveredStatement]:
    page_number = 1
    seen = 0
    while True:
        page = fetch_page(page_number)
        if not page.statements:
            return
        yield from page.statements
        seen += len(page.statements)
        if page.total_results and seen >= page.total_results:
            return
        page_number += 1


def _discovery_page(payload: dict) -> DiscoveryPage:
    return DiscoveryPage(
        total_results=int(payload.get("totalResults") or 0),
        statements=[
            DiscoveredStatement(
                business_id=str(item.get("businessId") or "").strip(),
                financial_date=str(item.get("financialDate") or "").strip(),
                registration_date=(
                    str(item["registrationDate"]).strip() if item.get("registrationDate") else None
                ),
            )
            for item in payload.get("financials", [])
        ],
    )
```

- [ ] **Step 4: Run tests**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_client.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/client.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_client.py
git commit -m "Add PRH XBRL API client"
```

---

### Task 7: Table Contract and XBRL Parser

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/tables.py`
- Create: `dagster_corpscout/sources/finland/prh_xbrl/parser.py`
- Test: `tests/test_finland_prh_xbrl_parser.py`

- [ ] **Step 1: Create the table contract**

Create `dagster_corpscout/sources/finland/prh_xbrl/tables.py`:

```python
"""ClickHouse table contract for Finland PRH XBRL.

Mirrors corpscout/clickhouse/migrations/000011_create_finland_prh_xbrl_financial_tables.up.sql.
Column order here is the insert order.
"""

STATEMENT_DOCUMENTS_TABLE = "fi_prh_xbrl_statement_documents"
CONTEXTS_TABLE = "fi_prh_xbrl_contexts"
UNITS_TABLE = "fi_prh_xbrl_units"
FACTS_TABLE = "fi_prh_xbrl_facts_raw"
METRICS_TABLE = "fi_prh_xbrl_metrics_long_v1"

TABLE_COLUMNS = {
    STATEMENT_DOCUMENTS_TABLE: [
        "statement_key", "source_run_id", "business_id", "financial_date",
        "registration_date", "source_url", "xml_object_key", "xml_sha256",
        "xml_size_bytes", "root_name", "schema_refs", "taxonomy_entrypoint",
        "reported_business_id", "reported_company_name",
        "reported_period_start", "reported_period_end",
        "contexts_count", "units_count", "facts_count",
        "validation_warnings", "parser_version", "parsed_at",
    ],
    CONTEXTS_TABLE: [
        "statement_key", "context_id", "entity_identifier", "entity_scheme",
        "period_type", "instant_date", "period_start", "period_end",
        "dimensions", "mcy_member_code", "mcy_member_label_fi",
        "ref_member_code", "ref_member_label_fi", "is_comparative", "parsed_at",
    ],
    UNITS_TABLE: [
        "statement_key", "unit_id", "measures", "is_divide", "raw_xml", "parsed_at",
    ],
    FACTS_TABLE: [
        "statement_key", "business_id", "financial_date", "fact_ordinal",
        "concept_qname", "concept_namespace", "concept_local_name",
        "context_id", "unit_id", "decimals", "precision",
        "value_kind", "raw_value", "numeric_value", "date_value", "text_value",
        "mcy_member_code", "mcy_member_label_fi",
        "ref_member_code", "ref_member_label_fi",
        "is_comparative", "dimensions", "parser_version", "parsed_at",
    ],
    METRICS_TABLE: [
        "statement_key", "business_id", "financial_date",
        "period_start", "period_end",
        "metric_key", "metric_label", "period_reference", "value", "currency",
        "source_concept_qname", "source_mcy_member_code", "source_ref_member_code",
        "source_fact_ordinal", "mapping_version", "derived_at",
    ],
}
```

- [ ] **Step 2: Write failing parser tests**

Create `tests/test_finland_prh_xbrl_parser.py` (fixture modeled on the real spike samples in `companies/analysis/finland/prh_xbrl_schema_spike/samples/`):

```python
from datetime import date, datetime, timezone
from decimal import Decimal

from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml

SAMPLE_XML = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
    xmlns:link="http://www.xbrl.org/2003/linkbase"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
    xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
    xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
    xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC"
    xmlns:fi_RF="http://www.suomi.fi/xbrl/crr/dict/dom/RF"
    xmlns:iso4217="http://www.xbrl.org/2003/iso4217">
  <link:schemaRef xlink:href="http://www.valtiokonttori.fi/fi/fr/xbrl/crr/fws/oytp/kpl-2016-12/2019-03-28/mod/oytp_gaap_ind.xsd" xlink:type="simple"/>
  <context id="ctx_base">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2023-09-30</instant></period>
  </context>
  <context id="ctx_mcy">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2023-09-30</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </scenario>
  </context>
  <context id="ctx_prev">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2022-09-30</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
      <xbrldi:explicitMember dimension="fi_dim:REF">fi_RF:x4</xbrldi:explicitMember>
    </scenario>
  </context>
  <unit id="EUR"><measure>iso4217:EUR</measure></unit>
  <fi_met:si289 contextRef="ctx_base">0176460-0</fi_met:si289>
  <fi_met:si168 contextRef="ctx_base">Testi Oy</fi_met:si168>
  <fi_met:di120 contextRef="ctx_base">2022-10-01</fi_met:di120>
  <fi_met:di121 contextRef="ctx_base">2023-09-30</fi_met:di121>
  <fi_met:md103 contextRef="ctx_mcy" unitRef="EUR" decimals="0">125000</fi_met:md103>
  <fi_met:md103 contextRef="ctx_prev" unitRef="EUR" decimals="0">110000</fi_met:md103>
</xbrl>"""

PARSED_AT = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)


def _parse(business_id: str = "0176460-0"):
    return parse_statement_xml(
        business_id=business_id,
        financial_date="2023-09-30",
        registration_date="2025-01-23",
        source_url="https://example.test/financial?businessId=0176460-0&financialDate=2023-09-30",
        xml_object_key="companies/0176460-0/2023-09-30.xml",
        source_run_id="dagster-run-1",
        body=SAMPLE_XML,
        parsed_at=PARSED_AT,
    )


def test_document_row_has_identity_counts_and_reported_metadata():
    parsed = _parse()
    [document] = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]

    assert len(parsed.statement_key) == 64
    assert document["statement_key"] == parsed.statement_key
    assert document["business_id"] == "0176460-0"
    assert document["financial_date"] == date(2023, 9, 30)
    assert document["registration_date"] == date(2025, 1, 23)
    assert document["root_name"] == "xbrl"
    assert document["taxonomy_entrypoint"].endswith("oytp_gaap_ind.xsd")
    assert document["reported_business_id"] == "0176460-0"
    assert document["reported_company_name"] == "Testi Oy"
    assert document["reported_period_start"] == date(2022, 10, 1)
    assert document["reported_period_end"] == date(2023, 9, 30)
    assert document["contexts_count"] == 3
    assert document["units_count"] == 1
    assert document["facts_count"] == 6
    assert document["validation_warnings"] == []
    assert document["parser_version"] == "1.0.0"


def test_contexts_capture_dimensions_and_comparative_flag():
    parsed = _parse()
    contexts = {row["context_id"]: row for row in parsed.rows_by_table[tables.CONTEXTS_TABLE]}

    assert contexts["ctx_base"]["period_type"] == "instant"
    assert contexts["ctx_base"]["instant_date"] == date(2023, 9, 30)
    assert contexts["ctx_base"]["entity_scheme"] == "http://ytj.fi"
    assert contexts["ctx_mcy"]["mcy_member_code"] == "fi_MC:x673"
    assert contexts["ctx_mcy"]["is_comparative"] == 0
    assert contexts["ctx_prev"]["ref_member_code"] == "fi_RF:x4"
    assert contexts["ctx_prev"]["is_comparative"] == 1
    assert ("fi_dim:MCY", "fi_MC:x673", "") in contexts["ctx_prev"]["dimensions"]


def test_facts_are_typed_and_denormalized_with_context_members():
    parsed = _parse()
    facts = parsed.rows_by_table[tables.FACTS_TABLE]
    by_concept = {}
    for fact in facts:
        by_concept.setdefault(fact["concept_qname"], []).append(fact)

    revenue_current = next(
        f for f in by_concept["fi_met:md103"] if f["context_id"] == "ctx_mcy"
    )
    assert revenue_current["value_kind"] == "numeric"
    assert revenue_current["numeric_value"] == Decimal("125000")
    assert revenue_current["mcy_member_code"] == "fi_MC:x673"
    assert revenue_current["unit_id"] == "EUR"
    assert revenue_current["decimals"] == "0"

    business_id_fact = by_concept["fi_met:si289"][0]
    assert business_id_fact["value_kind"] == "text"
    assert business_id_fact["text_value"] == "0176460-0"

    period_start_fact = by_concept["fi_met:di120"][0]
    assert period_start_fact["value_kind"] == "date"
    assert period_start_fact["date_value"] == date(2022, 10, 1)

    ordinals = [fact["fact_ordinal"] for fact in facts]
    assert ordinals == sorted(ordinals) and len(set(ordinals)) == len(ordinals)


def test_reported_business_id_mismatch_produces_warning_not_failure():
    parsed = _parse(business_id="9999999-9")
    [document] = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]

    assert any("reported business id" in warning for warning in parsed.warnings)
    assert document["facts_count"] == 6
```

- [ ] **Step 3: Run failing tests**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_parser.py -v
```

Expected: FAIL — `parser.py` does not exist.

- [ ] **Step 4: Implement the parser**

Create `dagster_corpscout/sources/finland/prh_xbrl/parser.py`:

```python
"""Parse PRH XBRL statement XML into raw-first ClickHouse rows.

Pure module: bytes in, typed rows out. No I/O, no Dagster imports.
Parser rules follow companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md:
keep all dimensions, never filter facts, denormalize MCY/REF, store reported
identity facts (si289/si168/di120/di121) on the document row, warn instead of drop.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from lxml import etree

from dagster_corpscout.sources.finland.prh_xbrl import spec, tables

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"

_REPORTED_CONCEPTS = {
    "si289": "reported_business_id",
    "si168": "reported_company_name",
    "di120": "reported_period_start",
    "di121": "reported_period_end",
}


@dataclass(frozen=True)
class ParsedStatement:
    statement_key: str
    rows_by_table: dict[str, list[dict]]
    warnings: list[str]


def statement_key_for(business_id: str, financial_date: str, xml_sha256: str) -> str:
    return hashlib.sha256(
        f"{business_id}:{financial_date}:{xml_sha256}".encode("utf-8")
    ).hexdigest()


def parse_statement_xml(
    *,
    business_id: str,
    financial_date: str,
    registration_date: str | None,
    source_url: str,
    xml_object_key: str,
    source_run_id: str,
    body: bytes,
    parsed_at: datetime,
) -> ParsedStatement:
    root = etree.fromstring(body)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(business_id, financial_date, xml_sha256)
    financial_date_value = date.fromisoformat(financial_date)
    warnings: list[str] = []

    context_rows = [
        _context_row(statement_key, element, parsed_at)
        for element in root.findall(f"{{{XBRLI_NS}}}context")
    ]
    contexts_by_id = {row["context_id"]: row for row in context_rows}

    unit_rows = [
        _unit_row(statement_key, element, parsed_at)
        for element in root.findall(f"{{{XBRLI_NS}}}unit")
    ]

    prefix_by_namespace = {
        namespace: prefix for prefix, namespace in (root.nsmap or {}).items() if prefix
    }

    fact_rows: list[dict] = []
    reported: dict[str, str] = {}
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue  # comments / processing instructions
        if element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace == XBRLI_NS:
            continue
        row = _fact_row(
            statement_key=statement_key,
            business_id=business_id,
            financial_date_value=financial_date_value,
            ordinal=len(fact_rows) + 1,
            element=element,
            qname=qname,
            prefix_by_namespace=prefix_by_namespace,
            contexts_by_id=contexts_by_id,
            warnings=warnings,
            parsed_at=parsed_at,
        )
        fact_rows.append(row)
        if qname.localname in _REPORTED_CONCEPTS:
            reported[_REPORTED_CONCEPTS[qname.localname]] = row["raw_value"]

    reported_business_id = reported.get("reported_business_id")
    if reported_business_id and reported_business_id != business_id:
        warnings.append(
            f"reported business id {reported_business_id!r} does not match requested {business_id!r}"
        )
    if not fact_rows:
        warnings.append("statement contains no facts")

    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.findall(f"{{{LINK_NS}}}schemaRef")
    ]

    document = {
        "statement_key": statement_key,
        "source_run_id": source_run_id,
        "business_id": business_id,
        "financial_date": financial_date_value,
        "registration_date": _date_or_none(registration_date or ""),
        "source_url": source_url,
        "xml_object_key": xml_object_key,
        "xml_sha256": xml_sha256,
        "xml_size_bytes": len(body),
        "root_name": etree.QName(root).localname,
        "schema_refs": schema_refs,
        "taxonomy_entrypoint": schema_refs[0] if schema_refs else "",
        "reported_business_id": reported_business_id,
        "reported_company_name": reported.get("reported_company_name"),
        "reported_period_start": _date_or_none(reported.get("reported_period_start", "")),
        "reported_period_end": _date_or_none(reported.get("reported_period_end", "")),
        "contexts_count": len(context_rows),
        "units_count": len(unit_rows),
        "facts_count": len(fact_rows),
        "validation_warnings": list(warnings),
        "parser_version": spec.PARSER_VERSION,
        "parsed_at": parsed_at,
    }

    return ParsedStatement(
        statement_key=statement_key,
        rows_by_table={
            tables.STATEMENT_DOCUMENTS_TABLE: [document],
            tables.CONTEXTS_TABLE: context_rows,
            tables.UNITS_TABLE: unit_rows,
            tables.FACTS_TABLE: fact_rows,
        },
        warnings=warnings,
    )


def _date_or_none(value: str) -> date | None:
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _member_for(dimensions: list[tuple[str, str, str]], suffix: str) -> str | None:
    for dimension_code, member_code, _label in dimensions:
        if dimension_code == suffix or dimension_code.endswith(f":{suffix}"):
            return member_code
    return None


def _context_row(statement_key: str, element, parsed_at: datetime) -> dict:
    entity = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
    period = element.find(f"{{{XBRLI_NS}}}period")
    instant = period.findtext(f"{{{XBRLI_NS}}}instant") if period is not None else None
    period_start = period.findtext(f"{{{XBRLI_NS}}}startDate") if period is not None else None
    period_end = period.findtext(f"{{{XBRLI_NS}}}endDate") if period is not None else None
    if instant:
        period_type = "instant"
    elif period_start or period_end:
        period_type = "duration"
    else:
        period_type = "none"

    dimensions = [
        (member.get("dimension", ""), (member.text or "").strip(), "")
        for member in element.findall(f".//{{{XBRLDI_NS}}}explicitMember")
    ]
    ref_member = _member_for(dimensions, "REF")

    return {
        "statement_key": statement_key,
        "context_id": element.get("id", ""),
        "entity_identifier": (entity.text or "").strip() if entity is not None else "",
        "entity_scheme": entity.get("scheme", "") if entity is not None else "",
        "period_type": period_type,
        "instant_date": _date_or_none(instant) if instant else None,
        "period_start": _date_or_none(period_start) if period_start else None,
        "period_end": _date_or_none(period_end) if period_end else None,
        "dimensions": dimensions,
        "mcy_member_code": _member_for(dimensions, "MCY"),
        "mcy_member_label_fi": None,
        "ref_member_code": ref_member,
        "ref_member_label_fi": None,
        "is_comparative": 1 if ref_member is not None else 0,
        "parsed_at": parsed_at,
    }


def _unit_row(statement_key: str, element, parsed_at: datetime) -> dict:
    measures = [
        (measure.text or "").strip()
        for measure in element.findall(f".//{{{XBRLI_NS}}}measure")
    ]
    return {
        "statement_key": statement_key,
        "unit_id": element.get("id", ""),
        "measures": measures,
        "is_divide": 1 if element.find(f"{{{XBRLI_NS}}}divide") is not None else 0,
        "raw_xml": etree.tostring(element, encoding="unicode"),
        "parsed_at": parsed_at,
    }


def _fact_row(
    *,
    statement_key: str,
    business_id: str,
    financial_date_value: date,
    ordinal: int,
    element,
    qname,
    prefix_by_namespace: dict[str, str],
    contexts_by_id: dict[str, dict],
    warnings: list[str],
    parsed_at: datetime,
) -> dict:
    raw_value = (element.text or "").strip()
    unit_id = element.get("unitRef")
    numeric_value = None
    date_value = None
    text_value = None
    if not raw_value:
        value_kind = "empty"
    elif unit_id is not None:
        try:
            numeric_value = Decimal(raw_value)
            value_kind = "numeric"
        except InvalidOperation:
            text_value = raw_value
            value_kind = "text"
    else:
        date_value = _date_or_none(raw_value)
        if date_value is not None:
            value_kind = "date"
        else:
            text_value = raw_value
            value_kind = "text"

    prefix = prefix_by_namespace.get(qname.namespace)
    concept_qname = f"{prefix}:{qname.localname}" if prefix else qname.localname

    context_ref = element.get("contextRef", "")
    context = contexts_by_id.get(context_ref)
    if context is None:
        warnings.append(f"fact {concept_qname} references unknown context {context_ref!r}")
        context = {
            "dimensions": [],
            "mcy_member_code": None,
            "ref_member_code": None,
            "is_comparative": 0,
        }

    return {
        "statement_key": statement_key,
        "business_id": business_id,
        "financial_date": financial_date_value,
        "fact_ordinal": ordinal,
        "concept_qname": concept_qname,
        "concept_namespace": qname.namespace or "",
        "concept_local_name": qname.localname,
        "context_id": context_ref,
        "unit_id": unit_id,
        "decimals": element.get("decimals"),
        "precision": element.get("precision"),
        "value_kind": value_kind,
        "raw_value": raw_value,
        "numeric_value": numeric_value,
        "date_value": date_value,
        "text_value": text_value,
        "mcy_member_code": context["mcy_member_code"],
        "mcy_member_label_fi": None,
        "ref_member_code": context["ref_member_code"],
        "ref_member_label_fi": None,
        "is_comparative": context["is_comparative"],
        "dimensions": context["dimensions"],
        "parser_version": spec.PARSER_VERSION,
        "parsed_at": parsed_at,
    }
```

- [ ] **Step 5: Run tests**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_parser.py -v
```

Expected: PASS.

- [ ] **Step 6: Sanity-check against a real spike sample**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
./.venv/bin/python - <<'EOF'
from datetime import datetime, timezone
from pathlib import Path
from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml

body = Path("../../companies/analysis/finland/prh_xbrl_schema_spike/samples/06__0176460-0__2023-09-30.xml").read_bytes()
parsed = parse_statement_xml(
    business_id="0176460-0", financial_date="2023-09-30", registration_date="2025-01-23",
    source_url="sample", xml_object_key="sample", source_run_id="sample",
    body=body, parsed_at=datetime.now(timezone.utc),
)
doc = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0]
print("facts:", doc["facts_count"], "contexts:", doc["contexts_count"], "warnings:", parsed.warnings)
EOF
```

Expected: facts/contexts counts in the dozens, no warnings (or only explainable ones).

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/tables.py \
  corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/parser.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_parser.py
git commit -m "Add PRH XBRL raw-first parser and table contract"
```

---

### Task 8: Importer and Raw Asset

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/importer.py`
- Replace: `dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`
- Test: `tests/test_finland_prh_xbrl_importer.py`, `tests/test_finland_prh_xbrl_raw_asset.py`

- [ ] **Step 1: Write failing importer test**

Create `tests/test_finland_prh_xbrl_importer.py`:

```python
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows


class _RecordingClient:
    def __init__(self):
        self.inserts = []

    def insert(self, table, data, column_names):
        self.inserts.append((table, len(data), tuple(column_names)))


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _recorder


_recorder = _RecordingClient()


def test_load_rows_inserts_per_table_in_contract_column_order():
    _recorder.inserts.clear()
    resource = FakeClickHouseResource(host="test", password="test")
    unit_row = {column: None for column in tables.TABLE_COLUMNS[tables.UNITS_TABLE]}

    counts = load_rows(resource, {tables.UNITS_TABLE: [unit_row], tables.FACTS_TABLE: []})

    assert counts == {tables.UNITS_TABLE: 1, tables.FACTS_TABLE: 0}
    assert _recorder.inserts == [
        (tables.UNITS_TABLE, 1, tuple(tables.TABLE_COLUMNS[tables.UNITS_TABLE]))
    ]
```

- [ ] **Step 2: Run failing test, then implement the importer**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_importer.py -v
```

Expected: FAIL. Then create `dagster_corpscout/sources/finland/prh_xbrl/importer.py`:

```python
"""Load parsed PRH XBRL rows into ClickHouse via the platform resource."""

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl.tables import TABLE_COLUMNS


def load_rows(clickhouse: ClickHouseResource, rows_by_table: dict[str, list[dict]]) -> dict[str, int]:
    client = clickhouse.client()
    counts: dict[str, int] = {}
    for table, rows in rows_by_table.items():
        clickhouse.insert_rows(client, table, TABLE_COLUMNS[table], rows)
        counts[table] = len(rows)
    return counts
```

Re-run; expected: PASS.

- [ ] **Step 3: Write failing raw asset test**

Create `tests/test_finland_prh_xbrl_raw_asset.py`:

```python
import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents


@mock_aws
def test_raw_xml_documents_downloads_window_and_writes_listing():
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json={
                "totalResults": 1,
                "financials": [
                    {
                        "businessId": "0176460-0",
                        "financialDate": "2024-09-30",
                        "registrationDate": "2025-01-23",
                    }
                ],
            },
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=b"<xbrl />")

        result = dg.materialize(
            [source_system, raw_xml_documents],
            selection=[raw_xml_documents],
            partition_key="2025-01-01",
            resources={"rustfs": rustfs},
        )

        assert "registeredDateStart=2025-01-01" in rsps.calls[0].request.url
        assert "registeredDateEnd=2025-01-31" in rsps.calls[0].request.url

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    assert listing["registered_date_start"] == "2025-01-01"
    assert listing["registered_date_end"] == "2025-01-31"
    [entry] = listing["documents"]
    assert entry["business_id"] == "0176460-0"
    assert entry["object_key"] == "companies/0176460-0/2024-09-30.xml"
    assert entry["registration_date"] == "2025-01-23"
```

- [ ] **Step 4: Run failing test, then implement the raw asset**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_raw_asset.py -v
```

Expected: FAIL (stub raises `NotImplementedError`). Replace `dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`:

```python
"""Raw layer: download one registration month of PRH XBRL statements into RustFS."""

from datetime import timedelta

import dagster as dg

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_xml_documents",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
    deps=[source_system],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_xml_documents(
    context: dg.AssetExecutionContext, rustfs: RustFSResource
) -> dg.MaterializeResult:
    """Statements registered in the partition month: XML to company-keyed objects,
    plus the discovery listing (the only place registration_date exists) as raw data."""
    window = context.partition_time_window
    registered_date_start = window.start.date().isoformat()
    registered_date_end = (window.end.date() - timedelta(days=1)).isoformat()

    client = PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT)
    rustfs.ensure_bucket(spec.BUCKET)

    documents: list[dict] = []
    bytes_downloaded = 0
    for statement in client.iter_registration_window(
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
    ):
        body, source_url = client.download_financial_xml(
            statement.business_id, statement.financial_date
        )
        object_key = spec.document_object_key(statement.business_id, statement.financial_date)
        xml_sha256 = rustfs.put_bytes(spec.BUCKET, object_key, body)
        bytes_downloaded += len(body)
        documents.append(
            {
                "business_id": statement.business_id,
                "financial_date": statement.financial_date,
                "registration_date": statement.registration_date,
                "object_key": object_key,
                "source_url": source_url,
                "xml_sha256": xml_sha256,
                "xml_size_bytes": len(body),
            }
        )
        context.log.info(
            "downloaded statement business_id=%s financial_date=%s bytes=%d",
            statement.business_id,
            statement.financial_date,
            len(body),
        )

    listing_key = spec.window_listing_object_key(context.partition_key)
    rustfs.put_json(
        spec.BUCKET,
        listing_key,
        {
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "documents": documents,
        },
    )

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(documents),
            "bytes_downloaded": bytes_downloaded,
            "listing_object_key": listing_key,
        }
    )
```

Re-run; expected: PASS.

- [ ] **Step 5: Run the whole suite and commit**

```bash
./.venv/bin/python -m pytest tests -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_importer.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_raw_asset.py
git commit -m "Add PRH XBRL raw window asset and ClickHouse importer"
```

---

### Task 9: Parsed Asset and Asset Check

**Files:**
- Replace: `dagster_corpscout/sources/finland/prh_xbrl/assets/parsed.py`
- Create: `dagster_corpscout/sources/finland/prh_xbrl/checks.py`
- Modify: `dagster_corpscout/sources/finland/prh_xbrl/__init__.py`
- Test: `tests/test_finland_prh_xbrl_parsed_asset.py`

- [ ] **Step 1: Write failing parsed asset test**

Create `tests/test_finland_prh_xbrl_parsed_asset.py`:

```python
import dagster as dg
from moto import mock_aws

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from tests.test_finland_prh_xbrl_parser import SAMPLE_XML


class _RecordingClient:
    def __init__(self):
        self.inserts = []

    def insert(self, table, data, column_names):
        self.inserts.append((table, len(data)))


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _recorder


_recorder = _RecordingClient()


@mock_aws
def test_statement_tables_parses_listing_documents_into_clickhouse():
    _recorder.inserts.clear()
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    rustfs.ensure_bucket(spec.BUCKET)
    rustfs.put_bytes(spec.BUCKET, "companies/0176460-0/2023-09-30.xml", SAMPLE_XML)
    rustfs.put_json(
        spec.BUCKET,
        spec.window_listing_object_key("2025-01-01"),
        {
            "registered_date_start": "2025-01-01",
            "registered_date_end": "2025-01-31",
            "documents": [
                {
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "registration_date": "2025-01-23",
                    "object_key": "companies/0176460-0/2023-09-30.xml",
                    "source_url": "https://example.test/financial",
                    "xml_sha256": "ignored",
                    "xml_size_bytes": len(SAMPLE_XML),
                }
            ],
        },
    )

    result = dg.materialize(
        [source_system, raw_xml_documents, statement_tables],
        selection=[statement_tables],
        partition_key="2025-01-01",
        resources={
            "rustfs": rustfs,
            "clickhouse": FakeClickHouseResource(host="test", password="test"),
        },
    )

    assert result.success
    inserted = dict(_recorder.inserts)
    assert inserted[tables.STATEMENT_DOCUMENTS_TABLE] == 1
    assert inserted[tables.CONTEXTS_TABLE] == 3
    assert inserted[tables.UNITS_TABLE] == 1
    assert inserted[tables.FACTS_TABLE] == 6
```

- [ ] **Step 2: Run failing test, then implement the parsed asset**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_parsed_asset.py -v
```

Expected: FAIL. Replace `dagster_corpscout/sources/finland/prh_xbrl/assets/parsed.py`:

```python
"""Parsed layer: re-parse the partition's raw XML from RustFS into ClickHouse.

Rebuildable from object storage forever — never touches the PRH API.
Re-runs are safe: all target tables are ReplacingMergeTree keyed on statement
identity, versioned by parsed_at.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="statement_tables",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "parsed"},
    deps=[raw_xml_documents],
    automation_condition=dg.AutomationCondition.eager(),
    retry_policy=dg.RetryPolicy(max_retries=2, delay=120, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def statement_tables(
    context: dg.AssetExecutionContext,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key(context.partition_key))
    parsed_at = datetime.now(timezone.utc)

    rows_by_table: dict[str, list[dict]] = {
        tables.STATEMENT_DOCUMENTS_TABLE: [],
        tables.CONTEXTS_TABLE: [],
        tables.UNITS_TABLE: [],
        tables.FACTS_TABLE: [],
    }
    warnings: list[str] = []
    for entry in listing["documents"]:
        body = rustfs.get_bytes(spec.BUCKET, entry["object_key"])
        parsed = parse_statement_xml(
            business_id=entry["business_id"],
            financial_date=entry["financial_date"],
            registration_date=entry.get("registration_date"),
            source_url=entry["source_url"],
            xml_object_key=entry["object_key"],
            source_run_id=context.run.run_id,
            body=body,
            parsed_at=parsed_at,
        )
        for table, rows in parsed.rows_by_table.items():
            rows_by_table[table].extend(rows)
        warnings.extend(parsed.warnings)

    counts = load_rows(clickhouse, rows_by_table)
    for warning in warnings:
        context.log.warning(warning)

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(listing["documents"]),
            "warnings_count": len(warnings),
            **{f"rows_{table}": count for table, count in counts.items()},
        }
    )
```

Re-run; expected: PASS.

- [ ] **Step 3: Add the asset check**

Create `dagster_corpscout/sources/finland/prh_xbrl/checks.py`:

```python
"""Quality gates for the parsed XBRL tables."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables


@dg.asset_check(asset=statement_tables, name="statement_documents_have_facts")
def statement_documents_have_facts(
    context: dg.AssetCheckExecutionContext, clickhouse: ClickHouseResource
) -> dg.AssetCheckResult:
    """A document with zero facts is a parse problem worth surfacing, not hiding."""
    client = clickhouse.client()
    result = client.query(
        f"SELECT countIf(facts_count = 0), count() FROM {tables.STATEMENT_DOCUMENTS_TABLE} FINAL"
    )
    empty_documents, total_documents = result.result_rows[0]
    return dg.AssetCheckResult(
        passed=int(empty_documents) == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "empty_documents": int(empty_documents),
            "total_documents": int(total_documents),
        },
    )
```

Update the bundle in `dagster_corpscout/sources/finland/prh_xbrl/__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import (
    raw_xml_documents,
    source_system,
    statement_tables,
)
from dagster_corpscout.sources.finland.prh_xbrl.checks import statement_documents_have_facts

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents, statement_tables),
    asset_checks=(statement_documents_have_facts,),
)

__all__ = ["source_bundle"]
```

- [ ] **Step 4: Run the suite and commit**

```bash
./.venv/bin/python -m pytest tests -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_parsed_asset.py
git commit -m "Add PRH XBRL parsed statement tables asset with quality check"
```

---

### Task 10: Derived Financial Metrics Asset

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/metrics.py`
- Create: `dagster_corpscout/sources/finland/prh_xbrl/assets/derived.py`
- Modify: `assets/__init__.py`, `__init__.py`
- Test: `tests/test_finland_prh_xbrl_metrics.py`

- [ ] **Step 1: Write failing metrics tests**

Create `tests/test_finland_prh_xbrl_metrics.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

from dagster_corpscout.sources.finland.prh_xbrl.metrics import (
    METRIC_MAPPINGS,
    derive_metric_rows,
)

DERIVED_AT = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)


def _fact(concept, mcy, ref, value):
    return {
        "statement_key": "k1",
        "business_id": "0176460-0",
        "financial_date": date(2023, 9, 30),
        "concept_qname": concept,
        "mcy_member_code": mcy,
        "ref_member_code": ref,
        "fact_ordinal": 1,
        "numeric_value": value,
        "reported_period_start": date(2022, 10, 1),
        "reported_period_end": date(2023, 9, 30),
    }


def test_mapped_facts_become_metric_rows_with_period_reference():
    facts = [
        _fact("fi_met:md103", "fi_MC:x673", None, Decimal("125000")),
        _fact("fi_met:md103", "fi_MC:x673", "fi_RF:x53", Decimal("110000")),
        _fact("fi_met:md103", "fi_MC:x999999", None, Decimal("1")),  # unmapped
    ]

    rows = derive_metric_rows(facts, derived_at=DERIVED_AT)

    assert len(rows) == 2
    current = next(row for row in rows if row["period_reference"] == "current")
    previous = next(row for row in rows if row["period_reference"] == "previous_period")
    assert current["metric_key"] == "revenue"
    assert current["value"] == Decimal("125000")
    assert current["currency"] == "EUR"
    assert current["mapping_version"]
    assert previous["value"] == Decimal("110000")


def test_metric_mappings_cover_core_financials():
    metric_keys = {mapping[0] for mapping in METRIC_MAPPINGS}
    assert {"revenue", "profit_loss", "total_assets", "equity", "liabilities"} <= metric_keys
```

- [ ] **Step 2: Run failing tests, then implement metrics.py**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_metrics.py -v
```

Expected: FAIL. Create `dagster_corpscout/sources/finland/prh_xbrl/metrics.py`:

```python
"""Derive curated long-format metrics from raw facts.

Pure module. The mapping comes from the schema spike's line-item analysis
(companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md): the real
line item lives in the fi_dim:MCY context member, not the concept name. Only
explicit (concept, MCY member) pairs become metrics — never inferred ones.
"""

from __future__ import annotations

from datetime import datetime

METRIC_MAPPING_VERSION = "2026-06-12"

# (metric_key, metric_label_fi, concept_qname, mcy_member_code)
METRIC_MAPPINGS = (
    ("revenue", "Liikevaihto", "fi_met:md103", "fi_MC:x673"),
    ("operating_profit_loss", "Liikevoitto (-tappio)", "fi_met:md103", "fi_MC:x689"),
    ("profit_loss", "Tilikauden voitto (tappio)", "fi_met:md103", "fi_MC:x740"),
    ("personnel_expenses", "Henkilöstökulut", "fi_met:md103", "fi_MC:x5"),
    ("wages_and_salaries", "Palkat ja palkkiot", "fi_met:md103", "fi_MC:x6"),
    ("total_assets", "Vastaavaa", "fi_met:mi53", "fi_MC:x360"),
    ("equity", "Oma pääoma", "fi_met:mi53", "fi_MC:x376"),
    ("liabilities", "Vieras pääoma", "fi_met:mi53", "fi_MC:x424"),
    ("cash_and_bank", "Rahat ja pankkisaamiset", "fi_met:mi53", "fi_MC:x399"),
    ("current_assets", "Vaihtuvat vastaavat", "fi_met:mi53", "fi_MC:x435"),
    ("current_receivables", "Lyhytaikaiset saamiset", "fi_met:mi53", "fi_MC:x1768"),
    ("current_liabilities", "Lyhytaikainen vieras pääoma", "fi_met:mi53", "fi_MC:x1811"),
)

_MAPPING_BY_PAIR = {
    (concept, mcy): (metric_key, metric_label)
    for metric_key, metric_label, concept, mcy in METRIC_MAPPINGS
}

# fi_dim:REF distinguishes current from comparative values (spike, "Parser Rules").
_PERIOD_REFERENCES = {
    None: "current",
    "fi_RF:x4": "previous_balance_date",
    "fi_RF:x53": "previous_period",
}


def derive_metric_rows(facts: list[dict], *, derived_at: datetime) -> list[dict]:
    """facts: numeric fact rows joined with the document's reported period dates."""
    rows: list[dict] = []
    for fact in facts:
        mapping = _MAPPING_BY_PAIR.get((fact["concept_qname"], fact["mcy_member_code"]))
        if mapping is None or fact["numeric_value"] is None:
            continue
        metric_key, metric_label = mapping
        rows.append(
            {
                "statement_key": fact["statement_key"],
                "business_id": fact["business_id"],
                "financial_date": fact["financial_date"],
                "period_start": fact.get("reported_period_start"),
                "period_end": fact.get("reported_period_end"),
                "metric_key": metric_key,
                "metric_label": metric_label,
                "period_reference": _PERIOD_REFERENCES.get(fact["ref_member_code"], "other"),
                "value": fact["numeric_value"],
                # All observed PRH units are iso4217:EUR (spike sample set).
                "currency": "EUR",
                "source_concept_qname": fact["concept_qname"],
                "source_mcy_member_code": fact["mcy_member_code"],
                "source_ref_member_code": fact["ref_member_code"],
                "source_fact_ordinal": fact["fact_ordinal"],
                "mapping_version": METRIC_MAPPING_VERSION,
                "derived_at": derived_at,
            }
        )
    return rows
```

Re-run; expected: PASS.

- [ ] **Step 3: Add the derived asset**

Create `dagster_corpscout/sources/finland/prh_xbrl/assets/derived.py`:

```python
"""Normalized layer: curated long-format financial metrics from raw facts.

Rebuildable from ClickHouse alone (never re-downloads, never re-parses XML).
Unpartitioned: it derives over all facts each run; the metrics table is
ReplacingMergeTree keyed on (business_id, financial_date, metric_key,
period_reference), so re-derivation supersedes in place. Move to
INSERT...SELECT inside ClickHouse if full-registry volume makes the
Python round-trip slow.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.metrics import (
    METRIC_MAPPINGS,
    derive_metric_rows,
)

_FACTS_QUERY = f"""
SELECT
    f.statement_key,
    f.business_id,
    f.financial_date,
    f.concept_qname,
    f.mcy_member_code,
    f.ref_member_code,
    f.fact_ordinal,
    f.numeric_value,
    d.reported_period_start,
    d.reported_period_end
FROM {tables.FACTS_TABLE} AS f FINAL
LEFT JOIN {tables.STATEMENT_DOCUMENTS_TABLE} AS d FINAL USING (statement_key)
WHERE f.value_kind = 'numeric' AND f.numeric_value IS NOT NULL
  AND (f.concept_qname, f.mcy_member_code) IN ({{pairs}})
"""


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="financial_metrics",
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "normalized"},
    deps=[statement_tables],
    automation_condition=dg.AutomationCondition.eager(),
    op_tags={"dagster/concurrency_key": f"{spec.SOURCE_NAME}:clickhouse"},
)
def financial_metrics(
    context: dg.AssetExecutionContext, clickhouse: ClickHouseResource
) -> dg.MaterializeResult:
    client = clickhouse.client()
    # Mapping pairs are static module constants — safe to inline.
    pairs = ", ".join(
        f"('{concept}', '{mcy}')" for _key, _label, concept, mcy in METRIC_MAPPINGS
    )
    result = client.query(_FACTS_QUERY.format(pairs=pairs))
    facts = [dict(zip(result.column_names, row)) for row in result.result_rows]

    rows = derive_metric_rows(facts, derived_at=datetime.now(timezone.utc))
    clickhouse.insert_rows(
        client, tables.METRICS_TABLE, tables.TABLE_COLUMNS[tables.METRICS_TABLE], rows
    )
    return dg.MaterializeResult(
        metadata={"facts_considered": len(facts), "metric_rows": len(rows)}
    )
```

Update `assets/__init__.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.assets.derived import financial_metrics
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.parsed import statement_tables
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents

__all__ = ["financial_metrics", "raw_xml_documents", "source_system", "statement_tables"]
```

and in `__init__.py` extend the bundle assets (with the matching import added):

```python
    assets=(source_system, raw_xml_documents, statement_tables, financial_metrics),
```

- [ ] **Step 4: Run the suite and commit**

```bash
./.venv/bin/python -m pytest tests -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_metrics.py
git commit -m "Add PRH XBRL derived financial metrics asset"
```

---

### Task 11: Jobs, On-Demand Company Pull, and Schedule

**Files:**
- Replace: `dagster_corpscout/sources/finland/prh_xbrl/jobs.py`
- Replace: `dagster_corpscout/sources/finland/prh_xbrl/schedules.py`
- Modify: `dagster_corpscout/sources/finland/prh_xbrl/__init__.py`
- Test: `tests/test_finland_prh_xbrl_jobs.py`

- [ ] **Step 1: Write failing job tests**

Create `tests/test_finland_prh_xbrl_jobs.py`:

```python
import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_company_job
from tests.test_finland_prh_xbrl_parsed_asset import FakeClickHouseResource, _recorder
from tests.test_finland_prh_xbrl_parser import SAMPLE_XML


def test_window_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prh_xbrl_pull_window_schedule")
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "finland_prh_xbrl_pull_window"


@mock_aws
def test_pull_company_job_downloads_parses_and_loads_one_company():
    _recorder.inserts.clear()
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/financials",
            json={
                "totalResults": 1,
                "financials": [
                    {
                        "businessId": "0176460-0",
                        "financialDate": "2023-09-30",
                        "registrationDate": "2025-01-23",
                    }
                ],
            },
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=SAMPLE_XML)

        result = pull_company_job.execute_in_process(
            run_config={
                "ops": {"pull_company_statements": {"config": {"business_id": "0176460-0"}}}
            },
            resources={
                "rustfs": rustfs,
                "clickhouse": FakeClickHouseResource(host="test", password="test"),
            },
        )

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2023-09-30.xml") == SAMPLE_XML
    inserted = dict(_recorder.inserts)
    assert inserted[tables.STATEMENT_DOCUMENTS_TABLE] == 1
    assert inserted[tables.FACTS_TABLE] == 6
```

- [ ] **Step 2: Run failing tests, then implement jobs**

```bash
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_jobs.py -v
```

Expected: FAIL. Replace `dagster_corpscout/sources/finland/prh_xbrl/jobs.py`:

```python
"""Jobs: the partitioned window pull and the on-demand company pull.

The on-demand pull is the secondary path from the design: it writes the same
deterministic object keys and the same ReplacingMergeTree tables as the window
pull, so the two paths converge idempotently — it never matters which path
fetched a statement.
"""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions

pull_window_job = dg.define_asset_job(
    name="finland_prh_xbrl_pull_window",
    selection=[raw_xml_documents],
    partitions_def=registration_month_partitions,
)


class CompanyPullConfig(dg.Config):
    business_id: str


@dg.op
def pull_company_statements(
    context: dg.OpExecutionContext,
    config: CompanyPullConfig,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> None:
    client = PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT)
    rustfs.ensure_bucket(spec.BUCKET)
    parsed_at = datetime.now(timezone.utc)

    rows_by_table: dict[str, list[dict]] = {
        tables.STATEMENT_DOCUMENTS_TABLE: [],
        tables.CONTEXTS_TABLE: [],
        tables.UNITS_TABLE: [],
        tables.FACTS_TABLE: [],
    }
    downloaded = 0
    for statement in client.iter_company_financials(config.business_id):
        body, source_url = client.download_financial_xml(
            statement.business_id, statement.financial_date
        )
        object_key = spec.document_object_key(statement.business_id, statement.financial_date)
        rustfs.put_bytes(spec.BUCKET, object_key, body)
        downloaded += 1
        parsed = parse_statement_xml(
            business_id=statement.business_id,
            financial_date=statement.financial_date,
            registration_date=statement.registration_date,
            source_url=source_url,
            xml_object_key=object_key,
            source_run_id=context.run.run_id,
            body=body,
            parsed_at=parsed_at,
        )
        for table, rows in parsed.rows_by_table.items():
            rows_by_table[table].extend(rows)
        for warning in parsed.warnings:
            context.log.warning(warning)

    counts = load_rows(clickhouse, rows_by_table)
    context.log.info(
        "company pull complete business_id=%s statements=%d facts=%d",
        config.business_id,
        downloaded,
        counts[tables.FACTS_TABLE],
    )


@dg.job(name="finland_prh_xbrl_pull_company")
def pull_company_job():
    pull_company_statements()
```

Replace `dagster_corpscout/sources/finland/prh_xbrl/schedules.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_window_job

# Materializes the just-closed registration month; eager automation cascades
# statement_tables and financial_metrics. STOPPED until production-ready.
pull_window_schedule = dg.build_schedule_from_partitioned_job(
    pull_window_job,
    name="finland_prh_xbrl_pull_window_schedule",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

Update the bundle in `dagster_corpscout/sources/finland/prh_xbrl/__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import (
    financial_metrics,
    raw_xml_documents,
    source_system,
    statement_tables,
)
from dagster_corpscout.sources.finland.prh_xbrl.checks import statement_documents_have_facts
from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_company_job, pull_window_job
from dagster_corpscout.sources.finland.prh_xbrl.schedules import pull_window_schedule

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents, statement_tables, financial_metrics),
    asset_checks=(statement_documents_have_facts,),
    jobs=(pull_window_job, pull_company_job),
    schedules=(pull_window_schedule,),
)

__all__ = ["source_bundle"]
```

- [ ] **Step 3: Run the full suite and commit**

```bash
./.venv/bin/python -m pytest tests -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_jobs.py
git commit -m "Add PRH XBRL window job, on-demand company pull, and schedule"
```

---

### Task 12: Deployment Wiring (Full Stack Replacement)

v1 is removed after cutover, so `dagster_v2/` carries the complete deployment: compose copied from v1, same Dagster Postgres (run history preserved), same network name (v1's `dagster.yaml` hardcodes `dagster-corpscout` for run containers), same UI port. The two stacks share state, so they must not run simultaneously — stop v1 before starting v2. No v1 file is modified.

**Files:**
- Create: `corpscout/dagster_v2/Dockerfile`
- Create: `corpscout/dagster_v2/workspace.yaml`
- Create: `corpscout/dagster_v2/docker-compose.yml` (copied from v1, image renamed)
- Modify: `corpscout/dagster_v2/.env.example` and local `.env` (`DAGSTER_RUN_IMAGE`)

- [ ] **Step 1: Create the v2 Dockerfile** (mirrors v1's)

Create `corpscout/dagster_v2/Dockerfile`:

```dockerfile
FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app

COPY pyproject.toml ./
COPY dagster_corpscout ./dagster_corpscout
RUN pip install --no-cache-dir . dagster-webserver

COPY dagster.yaml workspace.yaml /opt/dagster/home/
```

- [ ] **Step 2: Create the v2 workspace**

Create `corpscout/dagster_v2/workspace.yaml`:

```yaml
load_from:
  - grpc_server:
      host: dagster-code
      port: 4266
      location_name: dagster_corpscout
```

(Same location name as v1 — the module and asset keys are identical, so existing run/materialization history stays attached.)

- [ ] **Step 3: Copy and adapt the compose file**

```bash
cp corpscout/dagster/docker-compose.yml corpscout/dagster_v2/docker-compose.yml
```

Then in `corpscout/dagster_v2/docker-compose.yml` change every `image: dagster-corpscout:latest` to `image: dagster-corpscout-v2:latest` (three services: `dagster-code`, `dagster-webserver`, `dagster-daemon`). Everything else — service names, commands, ports (3500), network `dagster-corpscout`, extra_hosts — stays identical to v1.

- [ ] **Step 4: Point the run launcher at the v2 image**

In both `corpscout/dagster_v2/.env.example` and the local `corpscout/dagster_v2/.env`, change:

```dotenv
DAGSTER_RUN_IMAGE=dagster-corpscout-v2:latest
```

(`dagster.yaml`'s DockerRunLauncher reads this env var; without it, runs would execute in the v1 image.)

- [ ] **Step 5: Switch stacks and verify**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
docker compose down
cd ../dagster_v2
docker compose build && docker compose up -d
docker compose logs dagster-code --tail=20
```

Expected: the gRPC server starts without import errors; the Dagster UI at `http://localhost:3500` shows one code location `dagster_corpscout` containing groups `source_finland_prh_ytj` and `source_finland_prh_xbrl`, plus prior v1 run history (shared Dagster Postgres).

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/Dockerfile corpscout/dagster_v2/workspace.yaml \
  corpscout/dagster_v2/docker-compose.yml corpscout/dagster_v2/.env.example
git commit -m "Add full Dagster stack deployment for dagster_v2"
```

---

### Task 13: End-to-End Smoke Test and README

**Files:**
- Create: `corpscout/dagster_v2/README.md`

- [ ] **Step 1: Enable the v2 automation sensor**

In the Dagster UI (`http://localhost:3500`, now serving the v2 stack) → code location `dagster_corpscout` → Sensors → enable `automation_condition_sensor`.

- [ ] **Step 2: Materialize one registration month**

UI → Assets → `sources/finland/prh_xbrl/raw_xml_documents` → Materialize → partition `2025-01-01`.

Expected: run succeeds in the v2 image; metadata shows `documents_count > 0`; XML objects appear in RustFS under `source-finland-prh-xbrl/companies/<business_id>/<financial_date>.xml` and `windows/2025-01-01/listing.json` exists.

- [ ] **Step 3: Watch the eager cascade**

Expected within a sensor tick: `statement_tables` partition `2025-01-01` materializes automatically, then `financial_metrics`. The `statement_documents_have_facts` check reports on the `statement_tables` asset page.

- [ ] **Step 4: Verify ClickHouse**

```bash
docker exec -i companyindex-dev-clickhouse-1 clickhouse-client --query "
SELECT count() FROM corpscout_sources.fi_prh_xbrl_statement_documents FINAL;
SELECT count() FROM corpscout_sources.fi_prh_xbrl_facts_raw FINAL;
SELECT metric_key, count() FROM corpscout_sources.fi_prh_xbrl_metrics_long_v1 FINAL GROUP BY metric_key ORDER BY metric_key;
"
```

Expected: documents and facts counts > 0; metric rows for revenue/total_assets/etc.

- [ ] **Step 5: Verify idempotency by re-materializing**

Re-materialize `statement_tables` partition `2025-01-01`, then re-run the document count query with and without `FINAL`. Expected: the `FINAL` count is unchanged (replacing engine superseded the old rows).

- [ ] **Step 6: Run the on-demand company pull**

UI → Jobs → `finland_prh_xbrl_pull_company` → Launchpad:

```yaml
ops:
  pull_company_statements:
    config:
      business_id: "0176460-0"
```

Expected: run succeeds; the company's statements appear in RustFS and ClickHouse regardless of registration month.

- [ ] **Step 7: Write the README**

Create `corpscout/dagster_v2/README.md`:

```markdown
# dagster_v2

Second-generation Corpscout Dagster project (design:
`../docs/superpowers/specs/2026-06-12-dagster-source-pipeline-design.md`).
Separate project from `../dagster` carrying the full Dagster stack — own venv,
own image (`dagster-corpscout-v2:latest`), own compose (code server,
webserver on :3500, daemon) — sharing v1's Dagster Postgres, network name, and
code-location name, so run history carries over. The two stacks must not run
at the same time. The Python package keeps the name `dagster_corpscout`;
`../dagster` is deleted at cutover and this directory replaces it with no
renames.

## Development

```bash
python3.12 -m venv .venv
./.venv/bin/python -m pip install -e '.[dev]'
./.venv/bin/python -m pytest tests -v
```

Deploy: stop the v1 stack first (`cd ../dagster && docker compose down`), then
`docker compose build && docker compose up -d` here. The UI stays at :3500.

## Finland PRH XBRL (window archetype reference)

- `raw_xml_documents` — partitioned by registration month. Backfill months from
  the asset page; the monthly schedule (`finland_prh_xbrl_pull_window_schedule`,
  default STOPPED) keeps it current. Raw XML lands at
  `source-finland-prh-xbrl/companies/<business_id>/<financial_date>.xml`;
  the discovery listing at `windows/<partition>/listing.json`.
- `statement_tables` — re-parses the partition's XML from RustFS into the
  `fi_prh_xbrl_*` ClickHouse tables. Rebuildable any time without touching the
  PRH API (re-materialize after parser changes). All tables are
  ReplacingMergeTree — re-runs supersede, never duplicate. Query with FINAL.
- `financial_metrics` — curated metrics from explicit (concept, MCY) mappings
  in `metrics.py`.
- Layer cascade is automatic via `automation_condition_sensor` (enable it per
  location). On-demand single-company pull: job `finland_prh_xbrl_pull_company`.

## Finland PRH YTJ (snapshot archetype reference)

Ported from v1; the weekly schedule now triggers only `raw_snapshot`, and
normalized/code_lists/mapping/serving cascade via eager automation.

## Adding a source

```bash
./.venv/bin/python -m dagster_corpscout.source_scaffold <country> <source> \
  --sources-root dagster_corpscout/sources --archetype snapshot|window
```

Register the bundle in `dagster_corpscout/registry.py`; the conventions suite
(`tests/test_source_conventions.py`) enforces layout, layer vocabulary, and
stopped-by-default schedules.
```

- [ ] **Step 8: Full test run and final commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
./.venv/bin/python -m pytest tests -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/README.md
git commit -m "Document dagster_v2 Finland runbook"
```

---

## Self-Review

- **Spec coverage:** Design §2.1–2.8 are all implemented: assets-only pipeline (Tasks 8–10), layer vocabulary enforced by conventions test (Task 1), monthly window partitions matching the PRH discovery API (Task 5), eager automation with one explicit sensor (Tasks 1, 3, 9, 10), storage-level idempotency via deterministic keys + ReplacingMergeTree (Tasks 4, 5, 8), resources own I/O with pure client/parser/metrics modules (Tasks 6, 7, 10), Dagster-native retries/concurrency (Tasks 8, 9), scaffold archetypes + conventions-as-code (Tasks 1, 2). Both archetypes have a Finland example: prh_ytj (snapshot, Task 3) and prh_xbrl (window, Tasks 5–11). Financial data lands in ClickHouse raw-first tables and curated metrics. Project isolation: v2 has its own pyproject/venv/image and carries the full compose stack (Task 12); no v1 file is touched, and v1 is deleted after cutover.
- **Placeholder scan:** the scaffold templates and Task 5's temporary stubs intentionally raise `NotImplementedError` (scaffold convention inherited from v1); Tasks 8–9 replace the prh_xbrl stubs with full implementations. No other TBDs.
- **Type consistency:** `parse_statement_xml(...)` signature matches all three call sites (parsed asset, company-pull op, parser tests); `TABLE_COLUMNS` keys match parser output dict keys and migration columns (`parsed_at` added to contexts/units in both Task 4 SQL and Task 7 parser); `DiscoveredStatement.business_id/financial_date/registration_date` used consistently across client, raw asset, and jobs; `registration_month_partitions` shared by raw asset, parsed asset, and window job; `FakeClickHouseResource`/`_recorder` imported by the jobs test from the parsed-asset test module (`tests/` is a package — `tests/__init__.py` exists from Task 1).
- **Known risk to verify during execution:** `dg.materialize` / `build_schedule_from_partitioned_job` / `AutomationConditionSensorDefinition` argument names are per Dagster 1.10 — if the pinned version differs, adjust at the failing test, not by skipping it. `DAGSTER_RUN_IMAGE=dagster-corpscout-v2:latest` in v2's `.env` is what makes DockerRunLauncher execute runs in the v2 image — verify on the first UI-launched run in Task 13.
