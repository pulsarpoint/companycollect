"""Real SQLite queue, PostgreSQL admission and ClickHouse publication; fake browser answers."""

import json
import socket
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest


@pytest.fixture
def brave_batch_service(request, tmp_path):
    # Run with: uv run --with-editable ../browser_service pytest ...
    pytest.importorskip(
        "browser_service",
        reason="Run with --with-editable ../browser_service for the service integration",
    )
    import uvicorn
    from browser_service.brave_batch_control import BraveBatchControl
    from browser_service.brave_batch_results import (
        BraveBatchPublisher,
        BraveClickHouseSettings,
    )
    from browser_service.brave_batches import BraveBatchQueue, batch_router
    from fastapi import FastAPI, HTTPException

    def start(resource, dsn, task_id):
        profile_id, owner = str(uuid4()), str(uuid4())
        llm = {
            "profile_id": profile_id,
            "profile_revision": 1,
            "provider": "fixture",
            "base_url": "http://llm.test/v1",
            "model": "fixture",
            "api_key_encrypted": "v1." + "a" * 16 + "." + "b" * 32,
        }
        with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
            migrations = Path(__file__).resolve().parents[3] / "database/migrations"
            cursor.execute((migrations / "000127_llm_lifecycle.up.sql").read_text())
            cursor.execute(
                "INSERT INTO processing.llm_profiles(profile_id,name,current_revision) VALUES (%s,'fixture',1)",
                (profile_id,),
            )
            cursor.execute(
                """INSERT INTO processing.llm_profile_revisions(profile_id,revision,provider,base_url,model)
                VALUES (%s,1,'fixture','http://llm.test/v1','fixture')""",
                (profile_id,),
            )
            cursor.execute(
                """INSERT INTO processing.run_requests(request_id,task_id,job_name)
                VALUES (%s,%s,'__ephemeral_asset_job__')""",
                (owner, task_id),
            )
            cursor.execute(
                "INSERT INTO processing.run_llm_dependencies VALUES (%s,%s,1,'browser')",
                (owner, profile_id),
            )
        containers = subprocess.check_output(
            ["docker", "ps", "-q", "--filter", "name=ip-input-test-"], text=True
        ).split()
        ports = [
            json.loads(
                subprocess.check_output(
                    [
                        "docker",
                        "inspect",
                        "--format",
                        "{{json .NetworkSettings.Ports}}",
                        container,
                    ],
                    text=True,
                )
            )
            for container in containers
        ]
        mapping = next(
            mapping
            for mapping in ports
            if any(
                int(binding["HostPort"]) == resource.port
                for binding in mapping.get("9000/tcp", []) or []
            )
        )
        port = mapping["8123/tcp"][0]["HostPort"]
        runtime = SimpleNamespace(root=tmp_path, accepting=True, active={})
        fixture = SimpleNamespace(
            queries=[],
            llm=llm,
            owner=owner,
            batches=[],
            cancelled=[],
            responder=lambda _: {},
            answers={},
        )

        async def ask(payload):
            fixture.queries.append(payload.query)
            result = {
                "request_id": payload.request_id,
                "query": payload.query,
                "route": payload.route,
                "status": "success",
                "answer": "https://example.se",
                "source_url": "https://search.brave.com/ask",
                "fetched_at": datetime.now(UTC).isoformat(),
                "error_type": "",
                "error_stage": "",
                "elapsed_ms": 10,
                "challenge_runs": [],
            }
            result.update(fixture.responder(payload.model_dump()))
            fixture.answers[payload.request_id] = result
            return result

        async def status(request_id):
            if request_id not in fixture.answers:
                raise HTTPException(404, "Unknown")
            return fixture.answers[request_id]

        queue = BraveBatchQueue(
            runtime,
            ask=ask,
            status=status,
            publisher=BraveBatchPublisher(
                BraveClickHouseSettings(
                    url=f"http://127.0.0.1:{port}", username="test", password="test"
                )
            ),
            control=BraveBatchControl(dsn),
        )
        fixture.queue = queue

        @asynccontextmanager
        async def lifespan(_):
            queue.start()
            try:
                yield
            finally:
                await queue.close()

        app = FastAPI(lifespan=lifespan)
        app.include_router(batch_router(queue), prefix="/v1/brave")
        app.add_api_route("/v1/brave/requests/{request_id}",status,methods=["GET"])

        @app.middleware("http")
        async def record(request, call_next):
            if request.url.path == "/v1/brave/batches" and request.method == "POST":
                fixture.batches.append(await request.json())
            if request.url.path.endswith("/cancel"):
                fixture.cancelled.append(request.url.path)
            return await call_next(request)

        @app.post("/v1/brave/llm/verify")
        async def verify():
            return {"ok": True}

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        config = uvicorn.Config(app, log_level="error")
        server = uvicorn.Server(config)
        thread = threading.Thread(
            target=server.run, kwargs={"sockets": [sock]}, daemon=True
        )
        thread.start()
        deadline = time.monotonic() + 10
        while not server.started:
            assert time.monotonic() < deadline
            time.sleep(0.02)
        fixture.config = {
            "api_url": f"http://127.0.0.1:{sock.getsockname()[1]}",
            "api_token": "fixture-secret",
        }

        def stop():
            server.should_exit = True
            thread.join(10)
            sock.close()
            assert not thread.is_alive()

        request.addfinalizer(stop)
        return fixture

    return start
