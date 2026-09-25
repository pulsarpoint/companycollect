"""The application request ledger owns intent; Dagster owns run execution."""

from dataclasses import asdict
from uuid import uuid4

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from psycopg2.extras import Json

from dagster_v3.defs.commoncrawl_domain_graph.assets import (
    PARTITIONS,
    commoncrawl_domain_graph_job,
)
from dagster_v3.defs.commoncrawl_domain_graph.rank_assets import (
    commoncrawl_domain_ranks_job,
)
from dagster_v3.defs.commoncrawl_domain_graph.download import ArtifactSource
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource

REQUEST_TAG = "commoncrawl/request_id"


def reconcile_requests(store, instance) -> None:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT request_id,dagster_run_id FROM commoncrawl_graph_import_requests WHERE status IN ('launching','running')"
        )
        requests = cursor.fetchall()
    for request in requests:
        if request["dagster_run_id"] is not None:
            run = instance.get_run_by_id(str(request["dagster_run_id"]))
        else:
            runs = instance.get_runs(
                filters=dg.RunsFilter(tags={REQUEST_TAG: str(request["request_id"])}),
                limit=1,
            )
            run = runs[0] if runs else None
        if run is None:
            continue  # A launching request is returned again with the same deduplication key.
        status = {
            dg.DagsterRunStatus.SUCCESS: "succeeded",
            dg.DagsterRunStatus.FAILURE: "failed",
            dg.DagsterRunStatus.CANCELED: "canceled",
        }.get(run.status, "running")
        with store.transaction() as cursor:
            cursor.execute(
                """UPDATE commoncrawl_graph_import_requests SET status=%s,dagster_run_id=%s,
                finished_at=CASE WHEN %s THEN now() ELSE NULL END,updated_at=now(),last_error=%s
                WHERE request_id=%s AND status IN ('launching','running')""",
                (
                    status,
                    run.run_id,
                    run.is_finished,
                    "See Dagster run logs" if status == "failed" else None,
                    str(request["request_id"]),
                ),
            )


def queue_automatic_requests(store, loaded_ranks: set[str]) -> None:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT * FROM commoncrawl_graph_state WHERE singleton FOR UPDATE"
        )
        state = cursor.fetchone()
        if (
            not state["automatic_imports_enabled"]
            or state["discovery_baseline_at"] is None
        ):
            return
        cursor.execute(
            """SELECT r.graph_release,r.coverage_end,
            count(*) FILTER (WHERE f.availability='available' AND f.source_etag IS NOT NULL AND f.source_bytes>0 AND f.expected_rows>0) AS available,
            bool_or(f.artifact_kind='ranks' AND f.availability='available' AND f.source_etag IS NOT NULL AND f.source_bytes>0 AND f.expected_rows>0) AS ranks
            FROM commoncrawl_graph_releases r JOIN commoncrawl_graph_release_files f USING(graph_release)
            WHERE r.discovered_at>%s GROUP BY r.graph_release ORDER BY r.coverage_end DESC NULLS LAST,r.graph_release""",
            (state["discovery_baseline_at"],),
        )
        releases = cursor.fetchall()
        cursor.execute(
            "SELECT coverage_end FROM commoncrawl_graph_releases WHERE graph_release=%s",
            (state["active_graph_release"],),
        )
        current = cursor.fetchone()
        full_candidate = next(
            (
                r["graph_release"]
                for r in releases
                if r["available"] == 3
                and r["coverage_end"] is not None
                and (
                    current is None
                    or (
                        current["coverage_end"] is not None
                        and r["coverage_end"] > current["coverage_end"]
                    )
                )
            ),
            None,
        )
        for release in releases:
            key = release["graph_release"]
            selection = "full" if key == full_candidate else "ranks"
            if not release["ranks"] or (selection == "ranks" and key in loaded_ranks):
                continue
            cursor.execute(
                "SELECT * FROM commoncrawl_graph_import_requests WHERE graph_release=%s ORDER BY created_at DESC LIMIT 1",
                (key,),
            )
            latest = cursor.fetchone()
            retry_of, manifest = None, None
            if latest is not None:
                if latest["status"] != "failed" or latest["origin"] != "automatic":
                    continue
                cursor.execute(
                    """SELECT count(*) AS attempts, max(finished_at) < now()-interval '1 hour' AS ready
                    FROM commoncrawl_graph_import_requests WHERE graph_release=%s AND origin='automatic'""",
                    (key,),
                )
                attempts = cursor.fetchone()
                if attempts["attempts"] >= 3 or not attempts["ready"]:
                    continue
                retry_of, manifest = latest["request_id"], latest["source_manifest"]
                selection = latest["selection"]
            cursor.execute(
                """INSERT INTO commoncrawl_graph_import_requests
                (request_id,graph_release,selection,origin,requested_by,retry_of,source_manifest)
                VALUES (%s,%s,%s,'automatic','dagster',%s,%s) ON CONFLICT DO NOTHING""",
                (
                    str(uuid4()),
                    key,
                    selection,
                    retry_of,
                    Json(manifest) if manifest else None,
                ),
            )


def prepare_request(store) -> dict | None:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT automatic_imports_enabled FROM commoncrawl_graph_state WHERE singleton FOR UPDATE"
        )
        enabled = cursor.fetchone()["automatic_imports_enabled"]
        cursor.execute(
            "SELECT * FROM commoncrawl_graph_import_requests WHERE status IN ('launching','running') ORDER BY created_at LIMIT 1"
        )
        active = cursor.fetchone()
        if active is not None:
            return active if active["status"] == "launching" else None
        cursor.execute(
            """SELECT * FROM commoncrawl_graph_import_requests WHERE status='queued'
            AND (origin<>'automatic' OR %s) ORDER BY created_at LIMIT 1 FOR UPDATE""",
            (enabled,),
        )
        request = cursor.fetchone()
        if request is None:
            return None
        manifest = request["source_manifest"]
        if not manifest:
            required = (
                ["ranks"]
                if request["selection"] == "ranks"
                else ["nodes", "edges", "ranks"]
            )
            cursor.execute(
                """SELECT artifact_kind,source_url,source_etag,source_bytes,expected_rows
                FROM commoncrawl_graph_release_files WHERE graph_release=%s AND availability='available'
                AND artifact_kind=ANY(%s)""",
                (request["graph_release"], required),
            )
            try:
                manifest = {
                    row["artifact_kind"]: asdict(
                        ArtifactSource(graph_release=request["graph_release"], **row)
                    )
                    for row in cursor.fetchall()
                }
                if any(kind not in manifest for kind in required):
                    raise ValueError("Required files are unavailable")
            except ValueError:
                cursor.execute(
                    "UPDATE commoncrawl_graph_import_requests SET status='failed',finished_at=now(),updated_at=now(),last_error='Required files are unavailable or lack validators; refresh catalog' WHERE request_id=%s",
                    (request["request_id"],),
                )
                return None
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET status='launching',source_manifest=%s,updated_at=now() WHERE request_id=%s RETURNING *",
            (Json(manifest), request["request_id"]),
        )
        return cursor.fetchone()


@dg.sensor(
    jobs=[commoncrawl_domain_graph_job, commoncrawl_domain_ranks_job],
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def commoncrawl_graph_import_sensor(
    context: dg.SensorEvaluationContext,
    graph_catalog: GraphCatalogResource,
    clickhouse: ClickhouseResource,
):
    with graph_catalog.get_store() as store:
        reconcile_requests(store, context.instance)
        with clickhouse.get_connection() as client:
            loaded = {
                row[0]
                for row in client.execute(
                    "SELECT DISTINCT partition FROM system.parts WHERE database='corpscout' AND table='commoncrawl_domain_graph_ranks' AND active"
                )
            }
        queue_automatic_requests(store, loaded)
        request = prepare_request(store)
    if request is None:
        return dg.SkipReason("No import ready; existing imports are bounded to one run")
    release = request["graph_release"]
    context.instance.add_dynamic_partitions(PARTITIONS.name, [release])
    return dg.RunRequest(
        run_key=str(request["request_id"]),
        partition_key=release,
        job_name="commoncrawl_domain_graph_job"
        if request["selection"] == "full"
        else "commoncrawl_domain_ranks_job",
        tags={
            REQUEST_TAG: str(request["request_id"]),
            "commoncrawl/selection": request["selection"],
        },
    )
