"""A headed browser desktop shared by crawl assistance and saved sessions."""

import asyncio
import os
import signal
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import httpx

from browser_service.capture import utc_now


@dataclass(frozen=True)
class VirtualDesktop:
    cdp_port: int
    vnc_port: int
    kind: str = "interactive"
    owner: str | None = None
    id: str = field(default_factory=lambda: uuid4().hex)
    started_at: str = field(default_factory=utc_now)


ACTIVE_DESKTOPS: dict[str, VirtualDesktop] = {}


@asynccontextmanager
async def open_virtual_browser(
    profile: Path | None = None,
    *,
    kind: str = "interactive",
    owner: str | None = None,
) -> AsyncIterator[VirtualDesktop]:
    with socket.socket() as cdp, socket.socket() as vnc:
        cdp.bind(("127.0.0.1", 0))
        vnc.bind(("127.0.0.1", 0))
        cdp_port, vnc_port = cdp.getsockname()[1], vnc.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        "xvfb-run",
        "-a",
        "-s",
        "-screen 0 1440x1000x24 -nolisten tcp",
        sys.executable,
        "-m",
        "browser_service.browser_process",
        "--cdp-port",
        str(cdp_port),
        "--vnc-port",
        str(vnc_port),
        *(["--profile", str(profile)] if profile is not None else []),
        start_new_session=True,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    desktop = VirtualDesktop(cdp_port, vnc_port, kind, owner)
    try:
        async with httpx.AsyncClient(timeout=1) as client:
            async with asyncio.timeout(30):
                while True:
                    if process.returncode is not None:
                        raise RuntimeError("The virtual browser could not start")
                    try:
                        response = await client.get(
                            f"http://127.0.0.1:{cdp_port}/json/version"
                        )
                        if response.status_code == 200:
                            _, writer = await asyncio.open_connection(
                                "127.0.0.1", vnc_port
                            )
                            writer.close()
                            await writer.wait_closed()
                            break
                    except (httpx.TransportError, OSError):
                        pass
                    await asyncio.sleep(0.1)
        ACTIVE_DESKTOPS[desktop.id] = desktop
        yield desktop
    finally:
        ACTIVE_DESKTOPS.pop(desktop.id, None)
        try:
            os.killpg(process.pid, signal.SIGTERM)
            async with asyncio.timeout(5):
                await process.wait()
        except TimeoutError:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        except ProcessLookupError:
            await process.wait()
