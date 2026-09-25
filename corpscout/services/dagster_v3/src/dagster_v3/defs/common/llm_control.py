"""Admission fences and durable ownership for saved-model work."""

import os
from contextlib import contextmanager
from datetime import UTC, datetime

import dagster as dg
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor


class LlmDisabledError(RuntimeError):
    pass


@contextmanager
def control_transaction():
    url = os.environ.get("LLM_CONTROL_PG_URL")
    if not url:
        raise LlmDisabledError("LLM control database is not configured; saved-model work is blocked")
    connection = psycopg2.connect(url, connect_timeout=5, application_name="llm_control",
                                  options="-c statement_timeout=10000")
    try:
        with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor
    finally:
        connection.close()


def current_request_id() -> str:
    context = dg.AssetExecutionContext.get()
    request_id = context.run.tags.get("llm/request_id")
    if not request_id:
        raise LlmDisabledError("Start this saved-model execution from Backoffice to register its dependencies")
    with control_transaction() as cursor:
        cursor.execute("""UPDATE processing.run_requests SET dagster_run_id=%s,updated_at=now()
            WHERE request_id=%s AND (dagster_run_id IS NULL OR dagster_run_id=%s) RETURNING request_id""",
            (context.run.run_id, request_id, context.run.run_id))
        if cursor.fetchone() is None:
            raise LlmDisabledError("This is a different run; start or resume it from Backoffice")
    return request_id


def check_admission(llm: dict, request_id: str | None = None, *, service: str | None = None,
                    external_request_id: str | None = None) -> None:
    if not llm.get("profile_id"):
        return  # Legacy environment-driven runs have no saved-profile dependency.
    with control_transaction() as cursor:
        cursor.execute("""SELECT p.state,r.invalidated_at FROM processing.llm_profiles p
            JOIN processing.llm_profile_revisions r ON r.profile_id=p.profile_id
            WHERE p.profile_id=%s AND r.revision=%s FOR UPDATE OF p""",
            (llm["profile_id"], llm["profile_revision"]))
        profile = cursor.fetchone()
        if profile is None or profile["state"] != "enabled" or profile["invalidated_at"] is not None:
            raise LlmDisabledError("The selected LLM is disabled, removed, or its revision failed validation")
        if request_id is not None:
            cursor.execute("""SELECT r.request_id FROM processing.run_requests r
                JOIN processing.run_llm_dependencies d USING (request_id)
                WHERE r.request_id=%s AND r.stop_requested_at IS NULL AND r.finished_at IS NULL
                AND d.profile_id=%s AND d.revision=%s FOR UPDATE OF r""",
                (request_id,llm["profile_id"],llm["profile_revision"]))
            if cursor.fetchone() is None:
                raise LlmDisabledError("This LLM execution was stopped or has no matching dependency")
        if service is not None:
            if request_id is None or not external_request_id:
                raise LlmDisabledError("External LLM work requires a registered execution")
            cursor.execute("""INSERT INTO processing.llm_external_requests (service,external_request_id,request_id)
                VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""", (service,external_request_id,request_id))


def invalidate_revision(llm: dict, target: str, started_at: datetime, result: dict) -> None:
    """Only explicit provider credential/model failures stop other work; never parse text."""
    if not llm.get("profile_id") or result.get("ok") is True or result.get("failure_kind") != "configuration":
        return
    reason = "Model provider rejected the saved configuration. Test or replace it in LLM settings."
    with control_transaction() as cursor:
        cursor.execute("SELECT state,current_revision FROM processing.llm_profiles WHERE profile_id=%s FOR UPDATE", (llm["profile_id"],))
        profile = cursor.fetchone()
        if profile is None or profile["state"] == "archived":
            return
        cursor.execute("""INSERT INTO processing.llm_checks (profile_id,revision,target,started_at,ok,failure_kind,message)
            VALUES (%s,%s,%s,%s,false,'configuration',%s) ON CONFLICT (profile_id,revision,target) DO UPDATE SET
            started_at=excluded.started_at,finished_at=now(),ok=false,failure_kind='configuration',message=excluded.message
            WHERE processing.llm_checks.started_at < excluded.started_at RETURNING profile_id""",
            (llm["profile_id"],llm["profile_revision"],target,started_at,reason))
        if cursor.fetchone() is None:
            return
        cursor.execute("""SELECT 1 FROM processing.llm_checks WHERE profile_id=%s AND revision=%s
            AND started_at > %s AND (ok OR failure_kind='configuration') LIMIT 1""",
            (llm["profile_id"],llm["profile_revision"],started_at))
        if cursor.fetchone() is not None:
            return
        cursor.execute("""UPDATE processing.llm_profile_revisions SET invalidated_at=now(),invalid_reason=%s
            WHERE profile_id=%s AND revision=%s""", (reason,llm["profile_id"],llm["profile_revision"]))
        if profile["current_revision"] == llm["profile_revision"]:
            cursor.execute("""UPDATE processing.llm_profiles SET state='disabled',is_default=false,
                disabled_reason=%s,updated_at=now() WHERE profile_id=%s""", (reason,llm["profile_id"]))
        cursor.execute("""UPDATE processing.run_requests r SET stop_requested_at=coalesce(stop_requested_at,now()),
            stop_reason=%s,updated_at=now() WHERE finished_at IS NULL AND EXISTS
            (SELECT 1 FROM processing.run_llm_dependencies d WHERE d.request_id=r.request_id
            AND d.profile_id=%s AND d.revision=%s)""", (reason,llm["profile_id"],llm["profile_revision"]))


def guarded_http_client(llm: dict | None, timeout: float) -> httpx.Client | None:
    if llm is None or not llm.get("profile_id"):
        return None
    request_id = current_request_id()
    check_admission(llm, request_id)

    def before_request(request: httpx.Request):
        check_admission(llm, request_id)
        request.extensions["llm_started_at"] = datetime.now(UTC)

    def after_response(response: httpx.Response):
        if response.status_code in {401,403,404}:
            invalidate_revision(llm, "crawler", response.request.extensions["llm_started_at"],
                                {"ok":False,"failure_kind":"configuration"})

    return httpx.Client(timeout=timeout, event_hooks={"request":[before_request], "response":[after_response]})


def finish_external_request(service: str, external_request_id: str, state: str = "completed") -> None:
    with control_transaction() as cursor:
        cursor.execute("""UPDATE processing.llm_external_requests SET state=%s,updated_at=now(),last_error=NULL
            WHERE service=%s AND external_request_id=%s AND state='submitted'""", ({"cancelled":"canceled"}.get(state,state),service,external_request_id))
