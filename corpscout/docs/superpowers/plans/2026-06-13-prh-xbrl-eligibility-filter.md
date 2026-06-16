# PRH XBRL Eligibility Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Download XBRL statements only for companies that are alive and have a website (the technology-correlation target set), driven by the PRH YTJ company explorer cache in ClickHouse.

**Architecture:** The monthly registration-window spine stays unchanged. The raw asset gains an eligibility filter: one ClickHouse query loads the eligible business-id set (`fi_prhytj_company_explorer_cache` where `lifecycle_status = 'active' AND website != ''`, ~119k ids), discovered statements from ineligible companies are recorded as `skipped` in the window listing instead of downloaded, and already-present RustFS objects are reused instead of re-downloaded — which makes "company became eligible later" catch-up a plain re-materialization of old window partitions that only fetches the newly eligible statements. The asset graph gains an explicit cross-source dependency edge: `raw_xml_documents` ← `sources/finland/prh_ytj/company_explorer_cache`. Measured impact: ~22% of statements are eligible today (550 of 2,445 companies in Jan–Feb 2025), so downloads/storage/parse drop ~78%.

**Tech Stack:** Existing dagster_v2 project (Python 3.12, Dagster 1.13, boto3/RustFS, requests, clickhouse-connect). No new dependencies.

**Non-goals:**
- No change to `pull_company_job` (the on-demand single-company pull stays unrestricted — explicit requests bypass eligibility).
- No reconciliation tables or new sensors — catch-up is window re-materialization.
- No deletion of already-downloaded ineligible XML from RustFS (raw archive is cheap and already paid for; only ClickHouse is reset to the eligible-only dataset).

---

## File Structure

```
corpscout/dagster_v2/
├── dagster_corpscout/
│   ├── resources/rustfs.py                          # modify: add object_exists verb
│   └── sources/finland/prh_xbrl/
│       ├── client.py                                # modify: rename download_financial_xml →
│       │                                            #   download_statement_xml; add statement_xml_url
│       ├── jobs.py                                  # modify: rename at call site
│       ├── eligibility.py                           # NEW: eligibility query + fetch helper
│       └── assets/raw.py                            # modify: filter, reuse, skipped listing,
│                                                    #   cross-source dep, RawPullConfig
├── tests/
│   ├── test_rustfs_verbs.py                         # modify: object_exists test
│   ├── test_finland_prh_xbrl_client.py              # modify: rename refs + statement_xml_url test
│   ├── test_finland_prh_xbrl_eligibility.py         # NEW
│   ├── test_finland_prh_xbrl_raw_asset.py           # rewrite: filter/reuse/empty-set tests
│   └── test_definitions.py                          # modify: dep-edge test
└── README.md                                        # modify: eligibility section
```

All commands run with the project venv: `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2` unless stated otherwise. Commits happen from the repo root `/Users/graovic/pulsarpoint/ppoint/companycollect` on `main` (repo convention).

---

### Task 1: Platform Verbs and Client Statement-Method Naming

This task adds `object_exists` (RustFS), renames the client's `download_financial_xml` → `download_statement_xml`, and adds a paired `statement_xml_url` URL-builder. The downloader returns the URL built by `statement_xml_url`, so a reused object's recorded `source_url` is byte-identical to a freshly downloaded one.

**Files:**
- Modify: `dagster_corpscout/resources/rustfs.py`
- Modify: `dagster_corpscout/sources/finland/prh_xbrl/client.py`
- Modify: `dagster_corpscout/sources/finland/prh_xbrl/jobs.py` (call-site rename)
- Test: `tests/test_rustfs_verbs.py`, `tests/test_finland_prh_xbrl_client.py`

- [ ] **Step 1: Write/adjust the failing tests**

Append to `tests/test_rustfs_verbs.py`:

```python
@mock_aws
def test_object_exists_distinguishes_present_and_absent_keys():
    resource = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    resource.ensure_bucket("source-test")
    resource.put_bytes("source-test", "companies/0176460-0/2024-09-30.xml", b"<xbrl />")

    assert resource.object_exists("source-test", "companies/0176460-0/2024-09-30.xml") is True
    assert resource.object_exists("source-test", "companies/9999999-9/2024-09-30.xml") is False
```

In `tests/test_finland_prh_xbrl_client.py`, rename every reference `download_financial_xml` → `download_statement_xml` (the test function `test_download_financial_xml_returns_bytes_and_url` → `test_download_statement_xml_returns_bytes_and_url` and the 5 call sites at the previously-listed lines). Then append a new test for the URL builder:

```python
def test_statement_xml_url_builds_download_url_without_requesting():
    url = _client().statement_xml_url("0176460-0", "2023-09-30")

    assert url == f"{BASE}/financial?businessId=0176460-0&financialDate=2023-09-30"
```

Also confirm `test_download_statement_xml_returns_bytes_and_url` still asserts the returned URL equals `{BASE}/financial?businessId=0176460-0&financialDate=2023-09-30` (it does after the rename; the builder produces exactly this).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./.venv/bin/python -m pytest tests/test_rustfs_verbs.py tests/test_finland_prh_xbrl_client.py -v`
Expected: FAILs — `object_exists`/`statement_xml_url` don't exist, and the renamed `download_statement_xml` references resolve to a missing method.

- [ ] **Step 3: Implement `object_exists`**

Append to the `RustFSResource` class in `dagster_corpscout/resources/rustfs.py` (the `ClientError` import already exists):

```python
    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.client().head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        return True
```

- [ ] **Step 4: Rename the downloader and add the URL builder**

In `dagster_corpscout/sources/finland/prh_xbrl/client.py`, add to the imports:

```python
from urllib.parse import urlencode
```

Replace the existing `download_financial_xml` method:

```python
    def download_financial_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self._get(
            "/financial",
            {"businessId": business_id, "financialDate": financial_date},
        )
        return response.content, response.url
```

with the renamed downloader plus the paired builder (the downloader sources its returned URL from the builder, so download and reuse record the identical `source_url`):

```python
    def statement_xml_url(self, business_id: str, financial_date: str) -> str:
        """The canonical download URL for one statement, without making a request.
        Used when an already-downloaded object is reused and the listing still
        needs source_url — and by download_statement_xml so both paths agree."""
        query = urlencode({"businessId": business_id, "financialDate": financial_date})
        return f"{self.base_url}/financial?{query}"

    def download_statement_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self._get(
            "/financial",
            {"businessId": business_id, "financialDate": financial_date},
        )
        return response.content, self.statement_xml_url(business_id, financial_date)
```

- [ ] **Step 5: Update the jobs.py call site**

In `dagster_corpscout/sources/finland/prh_xbrl/jobs.py`, change the call (around line 50):

```python
            body, source_url = client.download_statement_xml(
                statement.business_id, statement.financial_date
            )
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./.venv/bin/python -m pytest tests/test_rustfs_verbs.py tests/test_finland_prh_xbrl_client.py tests/test_finland_prh_xbrl_jobs.py -v`
Expected: ALL PASS (4 rustfs + 8 client + 2 jobs tests).

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/resources/rustfs.py \
  corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/client.py \
  corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/jobs.py \
  corpscout/dagster_v2/tests/test_rustfs_verbs.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_client.py
git commit -m "Add object existence check and statement URL/download method pair"
```

---

### Task 2: Eligibility Module

**Files:**
- Create: `dagster_corpscout/sources/finland/prh_xbrl/eligibility.py`
- Test: `tests/test_finland_prh_xbrl_eligibility.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_finland_prh_xbrl_eligibility.py`:

```python
from types import SimpleNamespace

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import (
    COMPANY_CACHE_ASSET_KEY,
    fetch_eligible_business_ids,
)


class _QueryRecordingClient:
    def __init__(self):
        self.rows = []
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return SimpleNamespace(result_rows=list(self.rows))


_query_client = _QueryRecordingClient()


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _query_client


def test_fetch_eligible_business_ids_returns_id_set_from_active_website_query():
    _query_client.rows = [("0176460-0",), ("0100037-9",)]
    _query_client.queries.clear()

    ids = fetch_eligible_business_ids(FakeClickHouseResource(host="x", password="x"))

    assert ids == {"0176460-0", "0100037-9"}
    [sql] = _query_client.queries
    assert "fi_prhytj_company_explorer_cache" in sql
    assert "lifecycle_status = 'active'" in sql
    assert "website != ''" in sql


def test_company_cache_asset_key_points_at_prh_ytj_serving_asset():
    assert COMPANY_CACHE_ASSET_KEY == ["sources", "finland", "prh_ytj", "company_explorer_cache"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_eligibility.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_corpscout.sources.finland.prh_xbrl.eligibility'`.

- [ ] **Step 3: Implement the module**

Create `dagster_corpscout/sources/finland/prh_xbrl/eligibility.py`:

```python
"""Which companies' statements are worth downloading.

The pipeline exists to correlate technology signals with financials, so only
companies that are alive and have a web presence are pulled:
lifecycle_status = 'active' AND website != '' in the PRH YTJ explorer cache
(~119k of ~459k active companies as of 2026-06).

Statements of ineligible companies are recorded as skipped in the window
listing — never silently dropped — and re-materializing a window picks up
companies that became eligible since the window was last pulled.
"""

from dagster_corpscout.resources.clickhouse import ClickHouseResource

COMPANY_CACHE_TABLE = "fi_prhytj_company_explorer_cache"
COMPANY_CACHE_ASSET_KEY = ["sources", "finland", "prh_ytj", "company_explorer_cache"]

ELIGIBLE_BUSINESS_IDS_QUERY = f"""
SELECT business_id
FROM {COMPANY_CACHE_TABLE}
WHERE lifecycle_status = 'active' AND website != ''
"""


def fetch_eligible_business_ids(clickhouse: ClickHouseResource) -> set[str]:
    result = clickhouse.client().query(ELIGIBLE_BUSINESS_IDS_QUERY)
    return {row[0] for row in result.result_rows}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_eligibility.py -v`
Expected: 2 PASSED.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/eligibility.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_eligibility.py
git commit -m "Add PRH XBRL company eligibility query"
```

---

### Task 3: Eligibility-Filtered Raw Asset

**Files:**
- Modify: `dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py` (full replacement below)
- Test: `tests/test_finland_prh_xbrl_raw_asset.py` (full rewrite below)
- Test: `tests/test_definitions.py` (append dep-edge test)

- [ ] **Step 1: Rewrite the raw asset test file (failing first)**

Replace `tests/test_finland_prh_xbrl_raw_asset.py` entirely with:

```python
from types import SimpleNamespace

import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import COMPANY_CACHE_ASSET_KEY

# Stand-in for the prh_ytj serving asset so the dep edge resolves without
# importing the whole prh_ytj asset chain into this test.
company_cache_stub = dg.AssetSpec(key=dg.AssetKey(COMPANY_CACHE_ASSET_KEY))


class _EligibilityClient:
    def __init__(self):
        self.rows = []

    def query(self, _sql):
        return SimpleNamespace(result_rows=list(self.rows))


_eligibility_client = _EligibilityClient()


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _eligibility_client


def _materialize(rustfs, raise_on_error=True):
    return dg.materialize(
        [source_system, company_cache_stub, raw_xml_documents],
        selection=[raw_xml_documents],
        partition_key="2025-01-01",
        resources={
            "rustfs": rustfs,
            "clickhouse": FakeClickHouseResource(host="test", password="test"),
        },
        raise_on_error=raise_on_error,
    )


def _discovery_response(statements):
    return {
        "totalResults": len(statements),
        "financials": [
            {
                "businessId": business_id,
                "financialDate": financial_date,
                "registrationDate": "2025-01-23",
            }
            for business_id, financial_date in statements
        ],
    }


@mock_aws
def test_only_eligible_companies_are_downloaded_and_skips_are_recorded():
    _eligibility_client.rows = [("0176460-0",)]
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json=_discovery_response(
                [("0176460-0", "2024-09-30"), ("9999999-9", "2024-12-31")]
            ),
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=b"<xbrl />")

        result = _materialize(rustfs)

        # Exactly one /financial download happened (for the eligible company).
        financial_calls = [c for c in rsps.calls if "/financial?" in c.request.url]
        assert len(financial_calls) == 1
        assert "businessId=0176460-0" in financial_calls[0].request.url

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<xbrl />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    [document] = listing["documents"]
    assert document["business_id"] == "0176460-0"
    [skipped] = listing["skipped"]
    assert skipped["business_id"] == "9999999-9"
    assert skipped["financial_date"] == "2024-12-31"
    assert skipped["reason"] == "not_eligible"


@mock_aws
def test_existing_objects_are_reused_without_downloading():
    _eligibility_client.rows = [("0176460-0",)]
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")
    rustfs.ensure_bucket(spec.BUCKET)
    rustfs.put_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml", b"<existing />")

    with responses.RequestsMock() as rsps:
        # Only discovery is registered; any /financial download attempt would
        # raise ConnectionError and fail the run.
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/all_financial_statements",
            json=_discovery_response([("0176460-0", "2024-09-30")]),
        )

        result = _materialize(rustfs)

    assert result.success
    # Object untouched, but still listed for the parsed layer.
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2024-09-30.xml") == b"<existing />"
    listing = rustfs.get_json(spec.BUCKET, spec.window_listing_object_key("2025-01-01"))
    [document] = listing["documents"]
    assert document["object_key"] == "companies/0176460-0/2024-09-30.xml"
    assert document["source_url"].endswith(
        "/financial?businessId=0176460-0&financialDate=2024-09-30"
    )
    assert listing["skipped"] == []


@mock_aws
def test_empty_eligibility_set_fails_the_run_instead_of_skipping_everything():
    _eligibility_client.rows = []
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    result = _materialize(rustfs, raise_on_error=False)

    assert not result.success
```

Append to `tests/test_definitions.py`:

```python
def test_raw_xml_documents_depends_on_company_explorer_cache():
    from dagster_corpscout.definitions import defs

    graph = defs.resolve_asset_graph()
    node = next(n for n in graph.asset_nodes if n.key == prh_xbrl_key("raw_xml_documents"))
    parent_keys = {parent.key for parent in graph.get_parents(node)}

    assert dg.AssetKey(["sources", "finland", "prh_ytj", "company_explorer_cache"]) in parent_keys
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_raw_asset.py tests/test_definitions.py -v`
Expected: the three raw-asset tests FAIL (asset takes no `clickhouse` resource, no filtering, no `skipped` in listing); the dep-edge test FAILS (no such parent).

- [ ] **Step 3: Replace the raw asset**

Replace `dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py` entirely with:

```python
"""Raw layer: download one registration month of PRH XBRL statements into RustFS.

Only statements from eligible companies (active + website, from the PRH YTJ
explorer cache) are downloaded; the rest are recorded as `skipped` in the
window listing. Already-present objects are reused, so re-materializing an old
window only fetches statements of companies that became eligible since — that
re-run IS the catch-up mechanism.
"""

from datetime import timedelta

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import (
    COMPANY_CACHE_ASSET_KEY,
    fetch_eligible_business_ids,
)
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


class RawPullConfig(dg.Config):
    """Set refresh_existing to re-download objects already in RustFS
    (e.g. after a suspected upstream correction)."""

    refresh_existing: bool = False


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_xml_documents",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
    deps=[source_system, dg.AssetKey(COMPANY_CACHE_ASSET_KEY)],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_xml_documents(
    context: dg.AssetExecutionContext,
    config: RawPullConfig,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Statements registered in the partition month, filtered to eligible companies.
    The discovery listing (the only place registration_date and skip decisions
    exist) is stored as raw data alongside the XML objects."""
    window = context.partition_time_window
    registered_date_start = window.start.date().isoformat()
    registered_date_end = (window.end.date() - timedelta(days=1)).isoformat()

    eligible = fetch_eligible_business_ids(clickhouse)
    if not eligible:
        raise dg.Failure(
            "eligibility query returned no companies — the company cache is empty or "
            "broken; refusing to record the whole window as skipped"
        )

    rustfs.ensure_bucket(spec.BUCKET)

    documents: list[dict] = []
    skipped: list[dict] = []
    downloaded_count = 0
    reused_count = 0
    bytes_downloaded = 0
    with PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT) as client:
        for statement in client.iter_registration_window(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
        ):
            if statement.business_id not in eligible:
                skipped.append(
                    {
                        "business_id": statement.business_id,
                        "financial_date": statement.financial_date,
                        "registration_date": statement.registration_date,
                        "reason": "not_eligible",
                    }
                )
                continue

            object_key = spec.document_object_key(statement.business_id, statement.financial_date)
            if not config.refresh_existing and rustfs.object_exists(spec.BUCKET, object_key):
                reused_count += 1
                documents.append(
                    {
                        "business_id": statement.business_id,
                        "financial_date": statement.financial_date,
                        "registration_date": statement.registration_date,
                        "object_key": object_key,
                        "source_url": client.statement_xml_url(
                            statement.business_id, statement.financial_date
                        ),
                        # Parser recomputes hash and size from the stored body.
                        "xml_sha256": "",
                        "xml_size_bytes": 0,
                    }
                )
                continue

            body, source_url = client.download_statement_xml(
                statement.business_id, statement.financial_date
            )
            xml_sha256 = rustfs.put_bytes(spec.BUCKET, object_key, body)
            downloaded_count += 1
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
            "skipped": skipped,
        },
    )
    context.log.info(
        "window complete: %d eligible documents (%d downloaded, %d reused), %d skipped as ineligible",
        len(documents),
        downloaded_count,
        reused_count,
        len(skipped),
    )

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(documents),
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "skipped_ineligible_count": len(skipped),
            "eligible_companies_count": len(eligible),
            "bytes_downloaded": bytes_downloaded,
            "listing_object_key": listing_key,
        }
    )
```

(The parsed asset needs no change: it reads only `listing["documents"]`, and the entry fields it uses — business_id, financial_date, registration_date, source_url, object_key — are present for both downloaded and reused entries.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_raw_asset.py tests/test_definitions.py tests/test_finland_prh_xbrl_jobs.py -v`
Expected: ALL PASS. (`test_finland_prh_xbrl_jobs.py` is included because the window job selects this asset — it must still resolve.)

- [ ] **Step 5: Run the full suite**

Run: `./.venv/bin/python -m pytest tests -q`
Expected: 102 passed (95 before this plan + 2 Task 1 + 2 Task 2 + 2 net new raw tests + 1 dep-edge test).

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py \
  corpscout/dagster_v2/tests/test_finland_prh_xbrl_raw_asset.py \
  corpscout/dagster_v2/tests/test_definitions.py
git commit -m "Filter PRH XBRL downloads to active companies with websites"
```

---

### Task 4: README

**Files:**
- Modify: `corpscout/dagster_v2/README.md`

- [ ] **Step 1: Document the eligibility behavior**

In `README.md`, inside the "Finland PRH XBRL (window archetype reference)" section, replace the `raw_xml_documents` bullet with:

```markdown
- `raw_xml_documents` — partitioned by registration month. Downloads only
  statements of **eligible companies** (active + website, queried from
  `fi_prhytj_company_explorer_cache`; the asset depends on
  `sources/finland/prh_ytj/company_explorer_cache`). Ineligible statements are
  recorded under `skipped` in `windows/<partition>/listing.json` — re-materialize
  a window to catch up companies that became eligible later (already-downloaded
  objects are reused, only new ones are fetched; set `refresh_existing: true`
  in run config to force re-downloads). Raw XML lands at
  `source-finland-prh-xbrl/companies/<business_id>/<financial_date>.xml`.
```

- [ ] **Step 2: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v2/README.md
git commit -m "Document PRH XBRL eligibility filtering"
```

---

### Task 5: Deploy, Reset ClickHouse to the Eligible Dataset, Re-run Windows

The Jan+Feb 2025 windows were pulled unfiltered, so ClickHouse currently holds all 2,473 companies' data. RustFS keeps everything (raw archive), but ClickHouse is rebuilt to eligible-only.

- [ ] **Step 1: Rebuild and redeploy the stack**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
docker compose build && docker compose up -d
```

Expected: three services up. Confirm the automation sensor is still RUNNING (UI → Sensors, or GraphQL); restart it if the redeploy reset it:

```bash
docker compose exec -T dagster-daemon dagster sensor start automation_condition_sensor -w /opt/dagster/home/workspace.yaml
```

- [ ] **Step 2: Truncate the XBRL tables (remote ClickHouse)**

```bash
ssh graovic@100.85.212.113 "docker exec -i clickhouse-clickhouse-1 clickhouse-client --query \"
TRUNCATE TABLE corpscout_sources.fi_prh_xbrl_statement_documents;
TRUNCATE TABLE corpscout_sources.fi_prh_xbrl_contexts;
TRUNCATE TABLE corpscout_sources.fi_prh_xbrl_units;
TRUNCATE TABLE corpscout_sources.fi_prh_xbrl_facts_raw;
TRUNCATE TABLE corpscout_sources.fi_prh_xbrl_metrics_long_v1;
\""
```

Expected: no output, no error.

- [ ] **Step 3: Re-materialize the January window**

Launch via GraphQL (or UI: asset `raw_xml_documents`, partition `2025-01-01`):

```bash
curl -s -X POST http://localhost:3500/graphql -H 'Content-Type: application/json' -d '{"query":"mutation { launchRun(executionParams: { selector: { repositoryLocationName: \"dagster_corpscout\", repositoryName: \"__repository__\", jobName: \"finland_prh_xbrl_pull_window\" }, executionMetadata: { tags: [{ key: \"dagster/partition\", value: \"2025-01-01\" }] } }) { __typename ... on LaunchRunSuccess { run { id } } ... on PythonError { message } } }"}'
```

Expected: run succeeds in a few minutes (12 discovery requests; ~0 downloads because all eligible objects already exist — metadata shows `reused_count` ≈ documents_count, `downloaded_count` ≈ 0, `skipped_ineligible_count` ≈ 850). The eager cascade then re-parses only the eligible documents into the truncated tables and re-derives metrics.

- [ ] **Step 4: Re-materialize the February window**

Same launch with partition `2025-02-01`. Expected behavior identical.

- [ ] **Step 5: Verify the eligible-only dataset**

```bash
ssh graovic@100.85.212.113 "docker exec -i clickhouse-clickhouse-1 clickhouse-client --query \"
SELECT 'documents', count() FROM corpscout_sources.fi_prh_xbrl_statement_documents FINAL;
SELECT 'companies', uniqExact(business_id) FROM corpscout_sources.fi_prh_xbrl_statement_documents FINAL;
SELECT 'ineligible_leaked', count() FROM corpscout_sources.fi_prh_xbrl_statement_documents AS d FINAL LEFT JOIN corpscout_sources.fi_prhytj_company_explorer_cache AS c ON d.business_id = c.business_id WHERE c.lifecycle_status != 'active' OR c.website = '';
SELECT 'metrics', count() FROM corpscout_sources.fi_prh_xbrl_metrics_long_v1 FINAL;
\""
```

Expected: companies ≈ 550 (the measured eligible overlap), `ineligible_leaked` = 0, metrics > 0.

- [ ] **Step 6: Live validation on a fresh month (March 2025)**

Launch partition `2025-03-01` the same way. Expected: the run downloads ~20–25% of the discovered statements (watch `downloaded_count` vs `skipped_ineligible_count` in the materialization metadata), the cascade fires automatically, and the ineligible-leak query still returns 0.

- [ ] **Step 7: Run the full test suite one last time**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v2
./.venv/bin/python -m pytest tests -q
```

Expected: 102 passed.

---

## Self-Review

- **Spec coverage:** eligibility from the company cache (Task 2), filter applied at the window spine with skips recorded (Task 3), cross-source lineage edge (Task 3), catch-up via window re-materialization with object reuse + `refresh_existing` escape hatch (Tasks 1, 3, 5), dataset reset to eligible-only and live verification including a fresh month (Task 5). The user's per-company flow is preserved where it belongs: `pull_company_job` untouched (explicit non-goal).
- **Placeholder scan:** none — every code step carries complete code; expected counts are stated.
- **Type consistency:** `fetch_eligible_business_ids(clickhouse) -> set[str]` matches its single call site; `COMPANY_CACHE_ASSET_KEY` (list) wrapped in `dg.AssetKey(...)` at the dep site and asserted as a list in the eligibility test; listing schema gains `skipped` while `documents` keeps every field the parsed asset reads (business_id, financial_date, registration_date, source_url, object_key); reused entries' empty `xml_sha256`/zero `xml_size_bytes` are safe because the parser recomputes both from the body.
- **Known risks to verify during execution:** `graph.get_parents(node)` accessor name on the installed Dagster (mirrors the existing prh_ytj test, so it should hold); `raise_on_error=False` + `result.success` semantics for the failure test (adjust to `pytest.raises(dg.Failure)` with default `raise_on_error` if needed — keep the intent: empty eligibility set must fail the run, not skip the window).
