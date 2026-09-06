import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from requests import Response
from requests.adapters import BaseAdapter

from dagster_v3.defs.wikipedia.source import (
    ARTICLE_COLUMNS,
    WikipediaClient,
    WikipediaSnapshotConfig,
    article_row,
    extract_article_text,
    wikipedia_sitelinks,
)
from dagster_v3.defs.wikipedia.wikipedia_articles_component import (
    WikipediaArticlesComponent,
)
from dagster_v3.defs.wikipedia.snapshot import (
    build_article_snapshot,
    freeze_company_inventory,
    load_manifest,
    publish_articles,
    snapshot_prefix,
)


class WikimediaTransport(BaseAdapter):
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def send(self, request, **kwargs):
        self.requests.append(request)
        status, payload, headers = next(self.responses)
        response = Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        response.headers.update({"Content-Type": "application/json", **headers})
        response.request = request
        response.url = request.url
        return response

    def close(self):
        pass


def http_client(monkeypatch, responses):
    monkeypatch.setattr("dagster_v3.defs.wikipedia.source.time.sleep", lambda _: None)
    client = WikipediaClient(WikipediaSnapshotConfig(source_run_id="2026-08-31"))
    transport = WikimediaTransport(responses)
    client.http.session.mount("https://", transport)
    return client, transport


def test_http_retries_rate_limit_and_retains_request_identity(monkeypatch):
    client, transport = http_client(
        monkeypatch,
        [
            (429, {}, {"Retry-After": "0"}),
            (200, page(), {}),
        ],
    )
    assert client.article(sitelink())["status"] == "ok"
    assert len(transport.requests) == 2
    assert all(
        "CorpscoutWikipedia" in req.headers["User-Agent"] for req in transport.requests
    )


def test_wikidata_maxlag_is_retried_and_ids_are_batched(monkeypatch):
    client, transport = http_client(
        monkeypatch,
        [
            (200, {"error": {"code": "maxlag"}}, {"Retry-After": "0"}),
            (200, {"entities": {"Q1": {}, "Q2": {}}}, {}),
        ],
    )
    assert set(client.entities(["Q1", "Q2"])["entities"]) == {"Q1", "Q2"}
    assert "ids=Q1%7CQ2" in transport.requests[0].url
    assert "props=sitelinks%2Furls" in transport.requests[0].url


def test_page_redirect_resolves_title_and_rejects_external_host(monkeypatch):
    client, transport = http_client(
        monkeypatch,
        [
            (301, {}, {"Location": "/w/rest.php/v1/page/Handelsbanken/with_html"}),
            (200, page(), {}),
        ],
    )
    assert (
        client.article(sitelink(title="Old name"))["page"]["title"] == "Handelsbanken"
    )
    assert "Old%20name" in transport.requests[0].url
    client, _ = http_client(
        monkeypatch, [(302, {}, {"Location": "https://evil.test/page"})]
    )
    with pytest.raises(ValueError, match="Wikipedia URL"):
        client.article(sitelink())


def test_missing_page_is_terminal_but_generic_404_and_malformed_success_fail(
    monkeypatch,
):
    client, _ = http_client(
        monkeypatch, [(404, {"errorKey": "rest-nonexistent-title"}, {})]
    )
    assert client.article(sitelink())["status"] == "missing"
    client, _ = http_client(monkeypatch, [(404, {"errorKey": "rest-no-match"}, {})])
    with pytest.raises(ValueError, match="confirmed missing"):
        client.article(sitelink())
    client, _ = http_client(monkeypatch, [(200, {"error": "broken"}, {})])
    with pytest.raises(KeyError):
        client.article(sitelink())


def test_inventory_freeze_rejects_different_source_date_without_writing():
    store = MemoryStore()

    class ChangedSnapshot:
        def execute(self, sql):
            return [("Q1", "2026-09-07")]

    with pytest.raises(ValueError, match="completed snapshot"):
        freeze_company_inventory(store, ChangedSnapshot(), "2026-08-31")
    assert not store.objects


def test_successful_checkpoints_survive_failure_in_next_batch():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, [f"Q{i}" for i in range(1, 28)])
    original = client.article

    def interrupted(target):
        if client.article_calls == 25:
            raise RuntimeError("interrupted after first checkpoint")
        return original(target)

    client.article = interrupted
    with pytest.raises(RuntimeError):
        build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    client.article = original
    assert (
        build_article_snapshot(store, client, "2026-08-31", lambda *args: None)[
            "row_count"
        ]
        == 27
    )
    assert client.article_calls == 27


def test_missing_pages_are_audited_without_partial_publication():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1", "Q2"])
    original = client.article
    client.article = lambda target: (
        {"target": target, "status": "missing", "http_status": 404}
        if target["wikidata_id"] == "Q2"
        else original(target)
    )
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    assert manifest["row_count"] == 1 and manifest["missing_count"] == 1
    ch = FakeClickhouse()
    assert (
        publish_articles(ch, store, manifest, "missing_test", lambda *args: None) == 1
    )


def test_duplicate_raw_objects_and_staging_mismatch_never_exchange():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1"])
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    manifest["objects"] *= 2
    ch = FakeClickhouse()
    with pytest.raises(ValueError, match="Duplicate"):
        publish_articles(ch, store, manifest, "duplicate_test", lambda *args: None)
    assert not any(query.startswith("EXCHANGE") for query in ch.queries)
    assert ch.queries[-1].startswith("DROP TABLE")


def test_sensor_hands_same_snapshot_to_both_steps():
    from dagster_v3.defs.wikipedia.wikipedia_articles_component import (
        wikidata_wikipedia_articles_sensor,
    )
    from types import SimpleNamespace

    event = SimpleNamespace(
        asset_materialization=dg.AssetMaterialization(
            "wikidata_snapshot_complete",
            metadata={"source_run_id": "2026-08-31"},
        )
    )
    request = next(
        wikidata_wikipedia_articles_sensor._asset_materialization_fn(None, event)
    )
    assert request.run_key == "wikidata-wikipedia:2026-08-31"
    assert {
        op["config"]["source_run_id"] for op in request.run_config["ops"].values()
    } == {"2026-08-31"}


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def exists(self, key):
        return key in self.objects

    def write_bytes(self, key, body):
        self.objects[key] = body

    def read_bytes(self, key):
        return self.objects[key]


class WikimediaFixture:
    def __init__(self):
        self.entity_calls = 0
        self.article_calls = 0
        self.fail = False

    def entities(self, qids):
        self.entity_calls += 1
        return {
            "entities": {
                qid: {
                    "sitelinks": {
                        "enwiki": {
                            "title": "Handelsbanken",
                            "url": "https://en.wikipedia.org/wiki/Handelsbanken",
                        }
                    }
                }
                for qid in qids
            }
        }

    def article(self, target):
        self.article_calls += 1
        if self.fail:
            raise RuntimeError("transient upstream failure")
        return {
            "target": target,
            "status": "ok",
            "page": page(),
            "retrieved_at": "2026-09-05T12:00:00+00:00",
        }


def inventory(store, qids):
    store.write_bytes(
        snapshot_prefix("2026-08-31") + "companies.json",
        json.dumps(
            {
                "version": 1,
                "source_run_id": "2026-08-31",
                "company_qids": sorted(qids),
            }
        ).encode(),
    )


def test_s3_snapshot_resume_reuses_complete_batches_and_manifest():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, [f"Q{i}" for i in range(1, 28)])
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    assert manifest["row_count"] == 27
    assert manifest["company_count"] == 27
    assert len(manifest["objects"]) == 2
    assert client.article_calls == 27
    assert all(
        key.endswith(".jsonl.gz") for key in (obj["key"] for obj in manifest["objects"])
    )
    assert (
        build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
        == manifest
    )
    assert client.article_calls == 27 and client.entity_calls == 1


def test_failed_download_cannot_write_a_complete_manifest_and_can_resume():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1421630"])
    client.fail = True
    with pytest.raises(RuntimeError):
        build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    assert not store.exists(snapshot_prefix("2026-08-31") + "manifest.json")
    client.fail = False
    assert (
        build_article_snapshot(store, client, "2026-08-31", lambda *args: None)[
            "row_count"
        ]
        == 1
    )
    assert client.entity_calls == 1


def test_corrupted_raw_object_is_rejected_before_publication():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1421630"])
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    store.objects[manifest["objects"][0]["key"]] = gzip.compress(b"{}\n")
    with pytest.raises(ValueError, match="checksum"):
        publish_articles(
            FakeClickhouse(), store, manifest, "test_run", lambda *args: None
        )


class FakeClickhouse:
    def __init__(self, latest=""):
        self.latest = latest
        self.queries = []
        self.rows = []

    def execute(self, sql, params=None):
        self.queries.append(sql)
        if "max(source_run_id)" in sql:
            return [(self.latest,)]
        if sql.startswith("INSERT"):
            self.rows.extend(params)
        if "uniqExact" in sql:
            return [(len(self.rows), len({(row[0], row[1]) for row in self.rows}))]
        return []


def test_publish_uses_block_insert_and_atomic_exchange_only_after_validation():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1", "Q2"])
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    ch = FakeClickhouse()
    assert publish_articles(ch, store, manifest, "test_run", lambda *args: None) == 2
    assert len([query for query in ch.queries if query.startswith("INSERT")]) == 1
    assert any(query.startswith("EXCHANGE TABLES") for query in ch.queries)
    assert not any("TRUNCATE" in query for query in ch.queries)


def test_old_snapshot_never_replaces_newer_serving_data():
    store, client = MemoryStore(), WikimediaFixture()
    inventory(store, ["Q1"])
    manifest = build_article_snapshot(store, client, "2026-08-31", lambda *args: None)
    ch = FakeClickhouse(latest="2026-09-07")
    with pytest.raises(ValueError, match="newer"):
        publish_articles(ch, store, manifest, "test_run", lambda *args: None)
    assert not any(query.startswith("EXCHANGE") for query in ch.queries)


def test_incomplete_manifest_is_rejected():
    store = MemoryStore()
    store.write_bytes(
        snapshot_prefix("2026-08-31") + "manifest.json", b'{"complete":false}'
    )
    with pytest.raises(ValueError):
        load_manifest(store, "2026-08-31")


def sitelink(site="enwiki", language="en", title="Handelsbanken"):
    return {
        "wikidata_id": "Q1421630",
        "site_id": site,
        "language_code": language,
        "article_title": title,
        "article_url": f"https://{language}.wikipedia.org/wiki/{title}",
    }


def page():
    return {
        "id": 493229,
        "key": "Handelsbanken",
        "title": "Handelsbanken",
        "latest": {"id": 1372087742, "timestamp": "2026-08-30T06:14:41Z"},
        "license": {
            "title": "Creative Commons Attribution-Share Alike 4.0",
            "url": "https://creativecommons.org/licenses/by-sa/4.0/deed.en",
        },
        "html": '<html><body><section data-mw-section-id="0"><p>A Swedish bank.</p></section>'
        '<section data-mw-section-id="1"><h2>History</h2><p>Founded in 1871.</p>'
        "</section></body></html>",
    }


def test_all_wikipedia_languages_are_preserved_without_special_columns():
    links = wikipedia_sitelinks(
        "Q1421630",
        {
            "sitelinks": {
                "enwiki": {
                    "title": "Handelsbanken",
                    "url": "https://en.wikipedia.org/wiki/Handelsbanken",
                },
                "svwiki": {
                    "title": "Svenska Handelsbanken",
                    "url": "https://sv.wikipedia.org/wiki/Svenska_Handelsbanken",
                },
                "be_x_oldwiki": {
                    "title": "Банк",
                    "url": "https://be-tarask.wikipedia.org/wiki/Банк",
                },
                "commonswiki": {
                    "title": "Category:Handelsbanken",
                    "url": "https://commons.wikimedia.org/wiki/Category:Handelsbanken",
                },
            }
        },
    )
    assert {row["language_code"] for row in links} == {"en", "sv", "be-tarask"}
    assert len(links) == 3
    assert "wikipedia_sv_url" not in ARTICLE_COLUMNS


@pytest.mark.parametrize(
    "url",
    [
        "http://en.wikipedia.org/wiki/X",
        "https://en.wikipedia.org.evil.test/wiki/X",
        "https://user@en.wikipedia.org/wiki/X",
        "https://en.wikipedia.org:443/wiki/X",
    ],
)
def test_invalid_wikipedia_sitelink_is_rejected(url):
    with pytest.raises(ValueError):
        wikipedia_sitelinks("Q1", {"sitelinks": {"enwiki": {"title": "X", "url": url}}})


def test_text_preserves_prose_headings_lists_and_tables_but_removes_chrome():
    lead, full = extract_article_text("""<html><body><section data-mw-section-id="0">
      <table class="infobox"><tr><td>Bank</td><td>Sweden</td></tr></table>
      <p>A <b>Swedish</b> bank.<sup class="reference">[1]</sup></p></section>
      <section data-mw-section-id="1"><h2>History<span class="mw-editsection">edit</span></h2>
      <p>Founded in 1871.</p><ul><li>First office</li><li>Second office</li></ul>
      <div class="navbox">Navigation junk</div><ol class="references"><li>citation junk</li></ol>
      <script>alert(1)</script></section></body></html>""")
    assert lead == "A Swedish bank."
    assert "History\n\nFounded in 1871." in full
    assert "First office" in full and "Bank | Sweden" in full
    assert all(
        junk not in full
        for junk in ("[1]", "edit", "Navigation junk", "citation junk", "alert")
    )


def test_normalized_row_matches_migration_and_keeps_revision_and_provenance():
    row = article_row(
        {
            "target": sitelink(),
            "page": page(),
            "retrieved_at": "2026-09-05T12:00:00+00:00",
        },
        source_run_id="2026-08-31",
        resolved_at=datetime(2026, 9, 5, tzinfo=UTC),
    )
    assert tuple(row) == ARTICLE_COLUMNS
    assert row["wikipedia_page_id"] == 493229
    assert row["source_record_id"] == "Q1421630:enwiki"
    assert (
        row["article_revision_url"]
        == "https://en.wikipedia.org/w/index.php?oldid=1372087742"
    )
    assert row["article_lead_text"] == "A Swedish bank."
    assert row["source_run_id"] == "2026-08-31"
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse/migrations/000380_corpscout_wikidata_company_wikipedia_articles.up.sql"
    )
    ddl = migration.read_text(encoding="utf-8")
    assert all(f"    {column} " in ddl for column in ARTICLE_COLUMNS)
    assert "source_payload_hash" not in ddl and "raw_html" not in ddl


def test_config_rejects_non_snapshot_identifiers():
    with pytest.raises(ValueError):
        WikipediaSnapshotConfig(source_run_id="../latest")


def test_component_adds_only_two_assets_outside_core_completion():
    defs = WikipediaArticlesComponent().build_defs(None)
    by_key = {next(iter(asset.keys)).to_user_string(): asset for asset in defs.assets}
    assert set(by_key) == {
        "wikidata_company_wikipedia_articles_s3",
        "wikidata_company_wikipedia_articles",
    }
    raw = by_key["wikidata_company_wikipedia_articles_s3"]
    assert raw.dependency_keys == {dg.AssetKey("wikidata_snapshot_complete")}
    assert defs.sensors[0].default_status == dg.DefaultSensorStatus.STOPPED
