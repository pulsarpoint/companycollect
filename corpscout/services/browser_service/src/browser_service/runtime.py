"""Single process ownership of the configured browser profiles and SQLite database."""

import fcntl
from pathlib import Path
from typing import TextIO

from browser_service.browser_multiplexer import BrowserMultiplexer
from browser_service.browser_sessions import BrowserSessions
from browser_service.session_store import SessionStore


class BrowserService:
    def __init__(
        self, root: Path, *, count: int, max_pending: int, idle_timeout: float
    ):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.lock: TextIO | None = None
        self.pool = BrowserSessions(root / "profiles", count)
        self.store = SessionStore(root / "sessions.sqlite3")
        self.multiplexer = BrowserMultiplexer(
            self.pool, self.store, max_pending=max_pending, idle_timeout=idle_timeout
        )

    async def start(self) -> None:
        self.lock = (self.root / "service.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise RuntimeError(
                "Another browser service owns this state directory"
            ) from None
        try:
            await self.pool.start(restore_tabs=False)
            await self.multiplexer.start()
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        self.pool.closing = True
        await self.multiplexer.close()
        await self.pool.close()
        self.store.close()
        if self.lock is not None:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            self.lock = None
