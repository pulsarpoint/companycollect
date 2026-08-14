import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import duckdb


class _FakeResponse:
    def __init__(
        self,
        *,
        body: bytes,
        url: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._body = body
        self.url = url
        self.headers = headers or {}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8")

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeSession:
    def __init__(self, pbf_body: bytes) -> None:
        self.pbf_body = pbf_body
        self.calls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> _FakeResponse:
        self.calls.append(url)
        if url.endswith(".md5"):
            md5 = hashlib.md5(self.pbf_body).hexdigest()
            return _FakeResponse(
                body=f"{md5}  sweden-latest.osm.pbf\n".encode(),
                url=url,
                headers={"Content-Type": "text/plain"},
            )
        return _FakeResponse(
            body=self.pbf_body,
            url="https://download.geofabrik.de/europe/sweden-260811.osm.pbf",
            headers={
                "Content-Length": str(len(self.pbf_body)),
                "Content-Type": "application/octet-stream",
                "ETag": '"fixture-etag"',
                "Last-Modified": "Tue, 11 Aug 2026 23:11:37 GMT",
            },
        )

    def close(self) -> None:
        return None


class _FakeObjectStore:
    def __init__(self, *, failed_uploads: int = 0) -> None:
        self.bucket: str | None = None
        self.objects: dict[str, bytes] = {}
        self.failed_uploads = failed_uploads
        self.upload_attempts = 0

    def ensure_bucket(self, bucket: str) -> None:
        self.bucket = bucket

    def exists(self, key: str, bucket: str) -> bool:
        assert bucket == self.bucket
        return key in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str,
        *,
        transfer_config: object | None = None,
    ) -> None:
        assert bucket == self.bucket
        assert transfer_config is not None
        self.upload_attempts += 1
        if self.upload_attempts <= self.failed_uploads:
            raise RuntimeError("transient object-store failure")
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(self, key: str, body: str, bucket: str) -> None:
        assert bucket == self.bucket
        self.objects[key] = body.encode("utf-8")

    def read_bytes(self, key: str, bucket: str) -> bytes:
        assert bucket == self.bucket
        return self.objects[key]

    def list_keys(self, prefix: str, bucket: str) -> list[str]:
        assert bucket == self.bucket
        return sorted(key for key in self.objects if key.startswith(prefix))


def test_sweden_osm_snapshot_is_content_addressed_and_reused() -> None:
    from dagster_v3.defs.sweden_address_osm import tables
    from dagster_v3.defs.sweden_address_osm.resources import sync_osm_snapshot

    body = b"osm-pbf-fixture"
    session = _FakeSession(body)
    object_store = _FakeObjectStore()
    retrieved_at = datetime(2026, 8, 12, 19, 0, tzinfo=UTC)

    first = sync_osm_snapshot(
        object_store=object_store,
        run_id="first-run",
        retrieved_at=retrieved_at,
        session=session,
        minimum_size_bytes=len(body),
        download_attempts=1,
    )

    assert first.downloaded is True
    assert first.source_md5 == hashlib.md5(body).hexdigest()
    assert first.object_key.endswith("/sweden-latest.osm.pbf")
    assert object_store.objects[first.object_key] == body
    manifest = json.loads(object_store.objects[first.manifest_key])
    assert manifest["source_slug"] == "sweden_address_osm"
    assert manifest["source_url"] == tables.SOURCE_URL
    assert manifest["resolved_url"].endswith("sweden-260811.osm.pbf")

    second = sync_osm_snapshot(
        object_store=object_store,
        run_id="second-run",
        retrieved_at=retrieved_at,
        session=session,
        minimum_size_bytes=len(body),
        download_attempts=1,
    )

    assert second.downloaded is False
    assert session.calls.count(tables.SOURCE_URL) == 1
    assert session.calls.count(tables.SOURCE_MD5_URL) == 2


def test_sweden_osm_snapshot_retries_upload_without_redownloading(monkeypatch) -> None:
    from dagster_v3.defs.sweden_address_osm import tables
    from dagster_v3.defs.sweden_address_osm.resources import sync_osm_snapshot

    monkeypatch.setattr("dagster_v3.defs.sweden_address_osm.resources.time.sleep", lambda _: None)
    body = b"osm-pbf-fixture"
    session = _FakeSession(body)
    object_store = _FakeObjectStore(failed_uploads=2)

    snapshot = sync_osm_snapshot(
        object_store=object_store,
        run_id="retry-run",
        retrieved_at=datetime(2026, 8, 12, 19, 0, tzinfo=UTC),
        session=session,
        minimum_size_bytes=len(body),
        download_attempts=1,
        upload_attempts=3,
    )

    assert snapshot.downloaded is True
    assert object_store.upload_attempts == 3
    assert session.calls.count(tables.SOURCE_URL) == 1


def test_sweden_osm_address_index_extracts_nodes_and_way_points() -> None:
    from dagster_v3.defs.sweden_address_osm.normalize import (
        replace_osm_address_points_from_relation,
    )

    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    connection.execute(
        """
        create table osm_fixture (
            kind varchar,
            id bigint,
            tags map(varchar, varchar),
            refs bigint[],
            lat double,
            lon double,
            ref_roles varchar[],
            ref_types varchar[]
        )
        """
    )
    rows = [
        ("node", 1, None, None, 59.3300, 18.0600, None, None),
        ("node", 2, None, None, 59.3300, 18.0610, None, None),
        ("node", 3, None, None, 59.3310, 18.0610, None, None),
        (
            "way",
            100,
            {
                "addr:street": "Storgatan",
                "addr:housenumber": "10 A",
                "addr:postcode": "111 22",
                "addr:city": "Stockholm",
                "building": "yes",
            },
            [1, 2, 3, 1],
            None,
            None,
            None,
            None,
        ),
        (
            "node",
            200,
            {
                "addr:street": "Drottninggatan",
                "addr:housenumber": "5",
                "addr:postcode": "111 51",
                "addr:city": "Stockholm",
            },
            None,
            59.3320,
            18.0630,
            None,
            None,
        ),
        (
            "relation",
            300,
            {"addr:street": "Ignored relation", "addr:housenumber": "1"},
            [100],
            None,
            None,
            ["outer"],
            ["way"],
        ),
    ]
    for row in rows:
        connection.execute(
            "insert into osm_fixture values (?, ?, ?, ?, ?, ?, ?, ?)", row
        )

    counts = replace_osm_address_points_from_relation(
        connection=connection,
        source_relation_sql="select * from osm_fixture",
        source_relation_parameters=(),
        source_url="https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
        source_object_key="raw/md5=fixture/sweden-latest.osm.pbf",
        source_md5="fixture",
        source_snapshot_at=datetime(2026, 8, 11, 23, 11, 37, tzinfo=UTC),
        source_retrieved_at=datetime(2026, 8, 12, 19, 0, tzinfo=UTC),
    )

    assert counts == {
        "raw_address_objects": 3,
        "node_address_points": 1,
        "way_address_points": 1,
        "relation_address_objects_omitted": 1,
        "incomplete_way_address_objects_omitted": 0,
        "address_points": 2,
    }
    records = connection.execute(
        """
        select
            source_record_id,
            normalized_street,
            normalized_house_number,
            normalized_postcode,
            address_match_key,
            coordinate_method,
            longitude,
            latitude
        from sweden_address_osm.address_points
        order by source_record_id
        """
    ).fetchall()
    assert records[0][:6] == (
        "node/200",
        "drottninggatan",
        "5",
        "11151",
        "11151|drottninggatan|5",
        "osm_node",
    )
    assert records[0][6:] == (18.063, 59.332)
    assert records[1][:6] == (
        "way/100",
        "storgatan",
        "10a",
        "11122",
        "11122|storgatan|10a",
        "osm_way_point_on_surface",
    )
    assert 18.06 <= records[1][6] <= 18.061
    assert 59.33 <= records[1][7] <= 59.331


def test_sweden_address_osm_assets_are_registered_as_their_own_group() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    raw = repo.asset_graph.get(dg.AssetKey("sweden_osm_pbf_s3"))
    addresses = repo.asset_graph.get(dg.AssetKey("sweden_osm_addresses_duckdb"))
    job = repo.get_job("sweden_address_osm_job")

    assert raw.group_name == "sweden_address_osm"
    assert addresses.group_name == "sweden_address_osm"
    assert raw.partitions_def is None
    assert addresses.partitions_def is None
    assert dg.AssetKey("sweden_osm_pbf_s3") in addresses.parent_keys
    assert job.name == "sweden_address_osm_job"


def test_sweden_address_osm_design_documents_source_and_overlay_strategy() -> None:
    design = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "sweden_address_osm"
        / "docs"
        / "sweden_address_osm-design.md"
    ).read_text(encoding="utf-8")

    assert "Geofabrik" in design
    assert "ODbL" in design
    assert "ST_ReadOSM" in design
    assert "Lantmäteriet" in design
    assert "relation" in design.lower()
