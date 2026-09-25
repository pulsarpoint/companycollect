"""Small application catalog transactions; bulk graph rows remain in ClickHouse."""

from contextlib import contextmanager
from datetime import datetime
from dataclasses import asdict
from uuid import uuid4

import dagster as dg
import psycopg2
from psycopg2.extras import Json, RealDictCursor

from dagster_v3.defs.commoncrawl_domain_graph.catalog import GraphRelease
from dagster_v3.defs.commoncrawl_domain_graph.download import (
    ArtifactSource,
    CachedArtifact,
)


class GraphStore:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def transaction(self):
        with (
            self.connection,
            self.connection.cursor(cursor_factory=RealDictCursor) as cursor,
        ):
            yield cursor

    def pin_run(
        self, release: str, run_id: str, selection: str, request_id: str | None = None
    ) -> dict:
        """Pin a source manifest once, including manually launched Dagster runs."""
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT singleton FROM commoncrawl_graph_state WHERE singleton FOR UPDATE"
            )
            if request_id is not None:
                cursor.execute(
                    """UPDATE commoncrawl_graph_import_requests SET dagster_run_id=%s,status='running',updated_at=now()
                    WHERE request_id=%s AND graph_release=%s AND selection=%s AND status IN ('launching','running')
                    AND (dagster_run_id IS NULL OR dagster_run_id=%s)""",
                    (run_id, request_id, release, selection, run_id),
                )
                if cursor.rowcount != 1:
                    raise ValueError("Import request is not launchable by this run")
            cursor.execute(
                "SELECT graph_release,source_manifest FROM commoncrawl_graph_import_requests WHERE dagster_run_id=%s FOR UPDATE",
                (run_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if (
                    existing["graph_release"] != release
                    or not existing["source_manifest"]
                ):
                    raise ValueError(
                        "Import request does not match the requested release/manifest"
                    )
                return existing["source_manifest"]
            required = (
                ("ranks",) if selection == "ranks" else ("nodes", "edges", "ranks")
            )
            cursor.execute(
                """SELECT artifact_kind,source_url,source_etag,source_bytes,expected_rows
                FROM commoncrawl_graph_release_files WHERE graph_release=%s AND availability='available'
                AND artifact_kind=ANY(%s)""",
                (release, list(required)),
            )
            files = {
                row["artifact_kind"]: asdict(
                    ArtifactSource(graph_release=release, **row)
                )
                for row in cursor.fetchall()
            }
            if any(kind not in files for kind in required):
                raise ValueError(
                    "Discover the release and required files before importing it"
                )
            cursor.execute(
                """INSERT INTO commoncrawl_graph_import_requests
                (request_id,graph_release,selection,origin,requested_by,status,dagster_run_id,source_manifest)
                VALUES (%s,%s,%s,'manual','dagster','running',%s,%s)""",
                (str(uuid4()), release, selection, run_id, Json(files)),
            )
            return files

    def assert_rank_source(self, client, source: ArtifactSource) -> None:
        """An existing serving partition must have the same pinned source identity."""
        rows = client.execute(
            """SELECT groupUniqArray(2)(source_run_id)
            FROM corpscout.commoncrawl_domain_graph_ranks WHERE graph_release=%(release)s""",
            {"release": source.graph_release},
        )[0][0]
        if not rows:
            return
        if len(rows) != 1:
            raise ValueError("Rank partition contains mixed import runs")
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT source_manifest FROM commoncrawl_graph_import_requests WHERE dagster_run_id::text=%s",
                (rows[0],),
            )
            record = cursor.fetchone()
        if record is None or record["source_manifest"].get("ranks") != asdict(source):
            raise ValueError(
                "Existing ranking release has a different or unknown source identity"
            )

    def latest_ranking_release(self, client) -> str | None:
        loaded = [
            row[0]
            for row in client.execute(
                "SELECT DISTINCT partition FROM system.parts WHERE database='corpscout' AND table='commoncrawl_domain_graph_ranks' AND active"
            )
        ]
        if not loaded:
            return None
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT graph_release FROM commoncrawl_graph_releases WHERE graph_release=ANY(%s) AND coverage_end IS NOT NULL ORDER BY coverage_end DESC,graph_release LIMIT 1",
                (loaded,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(
                "Loaded rankings have no known coverage dates; cannot choose latest"
            )
        return row["graph_release"]

    def record_cache(self, artifact: CachedArtifact) -> None:
        source = artifact.source
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE commoncrawl_graph_release_files SET
                cache_bucket=%s,cache_key=%s,cached_source_etag=%s,cached_bytes=%s,
                cached_sha256=%s,cached_at=now(),schema_version=%s
                WHERE graph_release=%s AND artifact_kind=%s AND source_etag=%s""",
                (
                    artifact.bucket,
                    artifact.key,
                    source.source_etag,
                    source.source_bytes,
                    artifact.sha256,
                    artifact.schema_version,
                    source.graph_release,
                    source.artifact_kind,
                    source.source_etag,
                ),
            )

    def record_discovery_attempt(self, started_at: datetime) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE commoncrawl_graph_state
                SET discovery_attempted_at=%s, discovery_error=NULL, updated_at=now()
                WHERE singleton""",
                (started_at,),
            )

    def record_discovery_failure(self, error: str) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                "UPDATE commoncrawl_graph_state SET discovery_error=%s, updated_at=now() WHERE singleton",
                (error,),
            )

    def save_catalog(self, releases: list[GraphRelease], checked_at: datetime) -> None:
        if not releases:
            raise ValueError("Cannot publish an empty release catalog")
        with self.transaction() as cursor:
            for release in releases:
                cursor.execute(
                    """INSERT INTO commoncrawl_graph_releases
                    (graph_release,source_index_url,crawl_ids,coverage_start,coverage_end,discovered_at,last_checked_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (graph_release) DO UPDATE SET
                        source_index_url=excluded.source_index_url, crawl_ids=excluded.crawl_ids,
                        coverage_start=excluded.coverage_start, coverage_end=excluded.coverage_end,
                        last_checked_at=excluded.last_checked_at""",
                    (
                        release.graph_release,
                        release.source_index_url,
                        release.crawl_ids,
                        release.coverage_start,
                        release.coverage_end,
                        checked_at,
                        checked_at,
                    ),
                )
                for file in release.files:
                    cursor.execute(
                        """INSERT INTO commoncrawl_graph_release_files
                        (graph_release,artifact_kind,source_url,source_etag,source_bytes,expected_rows,availability,last_checked_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (graph_release,graph_level,artifact_kind) DO UPDATE SET
                            source_url=excluded.source_url, source_etag=excluded.source_etag,
                            source_bytes=excluded.source_bytes, expected_rows=excluded.expected_rows,
                            availability=excluded.availability, last_checked_at=excluded.last_checked_at""",
                        (
                            release.graph_release,
                            file.artifact_kind,
                            file.source_url,
                            file.source_etag,
                            file.source_bytes,
                            file.expected_rows,
                            file.availability,
                            checked_at,
                        ),
                    )
            cursor.execute(
                """UPDATE commoncrawl_graph_state
                SET discovery_succeeded_at=%s, discovery_error=NULL,
                    discovery_baseline_at=coalesce(discovery_baseline_at,%s), updated_at=now()
                WHERE singleton""",
                (checked_at, checked_at),
            )


class GraphCatalogResource(dg.ConfigurableResource):
    postgres_url: str

    @contextmanager
    def get_store(self):
        connection = psycopg2.connect(
            self.postgres_url, connect_timeout=10, application_name="commoncrawl_graph"
        )
        try:
            yield GraphStore(connection)
        finally:
            connection.close()
