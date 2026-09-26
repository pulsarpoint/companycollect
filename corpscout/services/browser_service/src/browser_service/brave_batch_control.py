"""Enforce Backoffice LLM admission inside the service that consumes the queue."""

import psycopg

from browser_service.brave_models import BraveBatchRequest


class BraveAdmissionError(RuntimeError):
    pass


class BraveBatchControl:
    def __init__(self, postgres_url: str):
        self.postgres_url = postgres_url

    async def transfer(self, old_owner: str, batch: BraveBatchRequest) -> None:
        await self.admit(batch)
        async with await psycopg.AsyncConnection.connect(
            self.postgres_url,
            connect_timeout=5,
            application_name="brave_batch",
            options="-c statement_timeout=10000",
        ) as connection:
            cursor = await connection.execute(
                """SELECT 1 FROM processing.run_requests
                WHERE request_id=%s AND stop_requested_at IS NULL AND finished_at IS NULL""",
                (old_owner,),
            )
            if await cursor.fetchone() is not None:
                raise BraveAdmissionError(
                    "The previous LLM execution still owns this batch"
                )

    async def admit(
        self, batch: BraveBatchRequest, request_id: str | None = None
    ) -> None:
        profile = batch.options.llm
        if profile is None or profile.profile_id is None:
            raise BraveAdmissionError("A saved LLM profile is required")
        async with await psycopg.AsyncConnection.connect(
            self.postgres_url,
            connect_timeout=5,
            application_name="brave_batch",
            options="-c statement_timeout=10000",
        ) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """SELECT 1 FROM processing.llm_profiles p
                    JOIN processing.llm_profile_revisions v ON v.profile_id=p.profile_id
                    WHERE p.profile_id=%s AND v.revision=%s AND p.state='enabled'
                    AND v.invalidated_at IS NULL FOR UPDATE OF p""",
                    (profile.profile_id, profile.profile_revision),
                )
                if await cursor.fetchone() is None:
                    raise BraveAdmissionError(
                        "The selected LLM is disabled, removed, or invalidated"
                    )
                await cursor.execute(
                    """SELECT 1 FROM processing.run_requests r
                    JOIN processing.run_llm_dependencies d USING(request_id)
                    WHERE r.request_id=%s AND r.task_id=%s AND r.stop_requested_at IS NULL
                    AND r.finished_at IS NULL AND d.profile_id=%s AND d.revision=%s
                    FOR UPDATE OF r""",
                    (
                        batch.owner_request_id,
                        batch.task_id,
                        profile.profile_id,
                        profile.profile_revision,
                    ),
                )
                if await cursor.fetchone() is None:
                    raise BraveAdmissionError(
                        "The owning task is stopped or has no matching LLM dependency"
                    )
                if request_id is not None:
                    await cursor.execute(
                        """INSERT INTO processing.llm_external_requests
                        (service,external_request_id,request_id) VALUES ('brave',%s,%s)
                        ON CONFLICT DO NOTHING""",
                        (request_id, batch.owner_request_id),
                    )

    async def finish(
        self, batch: BraveBatchRequest, request_id: str, state: str
    ) -> None:
        async with await psycopg.AsyncConnection.connect(
            self.postgres_url,
            connect_timeout=5,
            application_name="brave_batch",
            options="-c statement_timeout=10000",
        ) as connection:
            await connection.execute(
                """UPDATE processing.llm_external_requests
                SET state=%s,updated_at=now(),last_error=NULL
                WHERE service='brave' AND external_request_id=%s AND request_id=%s AND state='submitted'""",
                (state, request_id, batch.owner_request_id),
            )
