"""Durable crawl input with optional S3 delivery and a JetStream completion outbox."""

import asyncio
import hashlib
import json
import logging
from pathlib import Path

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.errors import Error as NatsError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StorageType
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from company_research.service import (
    CrawlRequest,
    CrawlService,
    RequestConflict,
    ServiceUnavailable,
)
from company_research.service_results import ResultDeliveryError, S3Results
from company_research.storage import utc_now, write_json

LOGGER = logging.getLogger(__name__)


class JetStreamSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    servers: list[str] = Field(
        default_factory=lambda: ["nats://127.0.0.1:4222"], min_length=1
    )
    stream: str = Field(default="COMPANY_CRAWL", pattern=r"^[A-Za-z0-9_-]+$")
    subject: str = Field(
        default="company.crawl.requests",
        pattern=r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$",
    )
    durable: str = Field(default="company-crawl-local", pattern=r"^[A-Za-z0-9_-]+$")
    ack_wait: float = Field(default=60, ge=1)
    create_stream: bool = False
    credentials: Path | None = Field(default=None, exclude=True)
    result_stream: str = Field(
        default="COMPANY_CRAWL_RESULTS", pattern=r"^[A-Za-z0-9_-]+$"
    )
    result_subject: str = Field(
        default="company.crawl.results",
        pattern=r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$",
    )
    result_max_age: float = Field(default=7 * 24 * 3600, gt=0)

    @model_validator(mode="after")
    def distinct_streams(self) -> "JetStreamSettings":
        if self.result_stream == self.stream or self.result_subject == self.subject:
            raise ValueError(
                "Crawl requests and results must use separate streams and subjects"
            )
        return self


class JetStreamInput:
    def __init__(
        self,
        service: CrawlService,
        settings: JetStreamSettings,
        results: S3Results | None = None,
    ):
        self.service = service
        self.settings = settings
        self.client: Client | None = None
        self.subscription: JetStreamContext.PullSubscription | None = None
        self.task: asyncio.Task | None = None
        self.results = results

    async def start(self) -> None:
        async def connection_error(error: Exception) -> None:
            LOGGER.error("NATS connection error (%s)", type(error).__name__)

        self.client = await nats.connect(
            servers=self.settings.servers,
            user_credentials=self.settings.credentials,
            error_cb=connection_error,
            name="company-crawl-service",
            max_reconnect_attempts=10,
        )
        try:
            js = self.client.jetstream()
            if self.results is not None:
                try:
                    result_info = await js.stream_info(self.settings.result_stream)
                except NotFoundError:
                    if not self.settings.create_stream:
                        raise ValueError(
                            "NATS result stream is missing; provision it or use --create-stream"
                        ) from None
                    result_info = await js.add_stream(
                        name=self.settings.result_stream,
                        subjects=[self.settings.result_subject],
                        retention=RetentionPolicy.LIMITS,
                        storage=StorageType.FILE,
                        max_age=self.settings.result_max_age,
                    )
                if (
                    result_info.config.retention != RetentionPolicy.LIMITS
                    or result_info.config.storage != StorageType.FILE
                    or self.settings.result_subject
                    not in (result_info.config.subjects or [])
                ):
                    raise ValueError(
                        "Result stream must use file storage, limits retention and the result subject"
                    )
            try:
                await js.stream_info(self.settings.stream)
            except NotFoundError:
                if not self.settings.create_stream:
                    raise ValueError(
                        "NATS stream is missing; provision it or use --create-stream"
                    ) from None
                await js.add_stream(
                    name=self.settings.stream,
                    subjects=[self.settings.subject],
                    retention=RetentionPolicy.WORK_QUEUE,
                    storage=StorageType.FILE,
                )
            try:
                info = await js.consumer_info(
                    self.settings.stream, self.settings.durable
                )
            except NotFoundError:
                info = await js.add_consumer(
                    self.settings.stream,
                    ConsumerConfig(
                        durable_name=self.settings.durable,
                        filter_subject=self.settings.subject,
                        ack_policy=AckPolicy.EXPLICIT,
                        ack_wait=self.settings.ack_wait,
                        max_ack_pending=1,
                        max_deliver=-1,
                    ),
                )
            if (
                info.config.ack_policy != AckPolicy.EXPLICIT
                or info.config.filter_subject != self.settings.subject
                or info.config.deliver_subject is not None
                or info.config.ack_wait != self.settings.ack_wait
                or info.config.max_deliver not in {None, -1}
            ):
                raise ValueError(
                    "Existing NATS consumer configuration does not match this worker"
                )
            self.subscription = await js.pull_subscribe_bind(
                stream=self.settings.stream, durable=self.settings.durable
            )
            self.task = asyncio.create_task(self.consume())
        except BaseException:
            await self.close()
            raise

    def healthy(self) -> bool:
        return (
            self.client is not None
            and self.client.is_connected
            and self.task is not None
            and not self.task.done()
        )

    async def consume(self) -> None:
        assert self.subscription is not None
        while True:
            try:
                messages = await self.subscription.fetch(batch=1, timeout=1)
                for message in messages:
                    try:
                        await self.process(message)
                    except (OSError, ServiceUnavailable, ResultDeliveryError) as error:
                        LOGGER.error("NATS job deferred (%s)", type(error).__name__)
                        await message.nak(delay=5)
            except NatsTimeoutError:
                continue
            except NatsError as error:
                LOGGER.error("NATS input retry (%s)", type(error).__name__)
                await asyncio.sleep(1)

    async def process(self, message: Msg) -> None:
        metadata = message.metadata
        delivery_id = hashlib.sha256(
            f"{metadata.stream}:{metadata.sequence.stream}:{metadata.timestamp.isoformat()}".encode()
        ).hexdigest()
        try:
            payload = json.loads(message.data)
            if not isinstance(payload, dict):
                raise ValueError("Expected a JSON object")
            payload.setdefault("request_id", f"nats-{delivery_id}")
            request = CrawlRequest.model_validate(payload)
            job = self.service.submit(request, source="jetstream")
        except (ValueError, ValidationError, RequestConflict) as error:
            # Never persist rejected raw input: it may contain accidental credentials.
            rejection_file = self.service.root / "rejected" / f"{delivery_id}.json"
            if rejection_file.exists():
                rejection = json.loads(rejection_file.read_text(encoding="utf-8"))
            else:
                rejection = {
                    "rejected_at": utc_now(),
                    "stream": metadata.stream,
                    "sequence": metadata.sequence.stream,
                    "payload_sha256": hashlib.sha256(message.data).hexdigest(),
                    "reason": "request_id_conflict"
                    if isinstance(error, RequestConflict)
                    else "invalid_request",
                }
                write_json(rejection_file, rejection)
            if self.results is not None:
                event = {
                    "schema_version": "company-crawl-event/1.0",
                    "event_id": f"rejected-{delivery_id}",
                    "request_id": None,
                    "state": "rejected",
                    "error": rejection["reason"],
                    "request_stream": metadata.stream,
                    "request_sequence": metadata.sequence.stream,
                    "finished_at": rejection["rejected_at"],
                    "result": None,
                }
                await self.publish_event(event)
            await message.term()
            return
        waiter = asyncio.create_task(self.deliver(job.request_id))
        try:
            while True:
                done, _ = await asyncio.wait(
                    {waiter}, timeout=self.settings.ack_wait / 3
                )
                if not done:
                    if not self.service.healthy():
                        raise ServiceUnavailable("Crawl workers stopped") from None
                    await message.in_progress()
                    continue
                # Publication timeouts must propagate, not become progress heartbeats.
                await waiter
                await message.ack_sync()
                return
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

    async def publish_event(self, event: dict):
        assert self.client is not None
        return await self.client.jetstream().publish(
            self.settings.result_subject,
            json.dumps(event, ensure_ascii=False).encode(),
            stream=self.settings.result_stream,
            headers={"Nats-Msg-Id": event["event_id"]},
        )

    async def deliver(self, request_id: str) -> None:
        completed = await self.service.wait(request_id)
        assert completed.result_file is not None
        if self.results is None:
            # Local-only mode retains its existing acknowledgement contract.
            try:
                json.loads(
                    (self.service.root / completed.result_file).read_text(
                        encoding="utf-8"
                    )
                )
            except ValueError as error:
                raise ResultDeliveryError("Saved result is not valid JSON") from error
            return
        outbox_file = self.service.root / "jobs" / request_id / "delivery.json"
        destination = {
            "s3": self.results.settings.model_dump(),
            "stream": self.settings.result_stream,
            "subject": self.settings.result_subject,
        }
        outbox: dict
        if outbox_file.exists():
            outbox = json.loads(outbox_file.read_text(encoding="utf-8"))
            if outbox["destination"] != destination:
                raise ResultDeliveryError(
                    "Saved delivery belongs to a different destination"
                )
        else:
            try:
                event = await asyncio.to_thread(
                    self.results.prepare_event, self.service.root, completed
                )
            except ValueError as error:
                raise ResultDeliveryError(
                    "Saved result or request is invalid"
                ) from error
            outbox = {
                "schema_version": "company-crawl-delivery/1.0",
                "destination": destination,
                "event": event,
                "published": None,
            }
            write_json(outbox_file, outbox)
        if outbox["published"] is None:
            receipt = await self.publish_event(outbox["event"])
            outbox["published"] = {
                "stream": receipt.stream,
                "sequence": receipt.seq,
                "published_at": utc_now(),
            }
            write_json(outbox_file, outbox)

    async def close(self) -> None:
        if self.task is not None:
            self.task.cancel()
            outcomes = await asyncio.gather(self.task, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, Exception):
                    LOGGER.error("NATS input stopped (%s)", type(outcome).__name__)
            self.task = None
        if self.client is not None:
            await self.client.close()
            self.client = None
