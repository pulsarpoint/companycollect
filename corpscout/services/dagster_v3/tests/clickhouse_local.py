"""Running SQL against a real ClickHouse engine from a test.

`clickhouse-local` when the machine has a binary, otherwise the pinned server image under
Docker, otherwise the caller skips. Lived in tests/test_se_company_person_clickhouse_local.py
until the people chain that file covered was retired (person slice 0, 2026-09-09); eight
other integration tests imported it from there, which is why it is its own module now.
"""

import functools
import shutil
import subprocess
from datetime import datetime
from typing import Any

import pytest

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"


@functools.cache
def clickhouse_local_command() -> list[str]:
    """A `clickhouse-local` invocation, or skip when the machine has none."""
    direct = shutil.which("clickhouse-local")
    if direct:
        return [direct, "--multiquery"]
    binary = shutil.which("clickhouse")
    if binary:
        return [binary, "local", "--multiquery"]
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("no clickhouse-local binary and no docker to run one")
    probe = subprocess.run(
        [docker, "info"], capture_output=True, text=True, timeout=60, check=False
    )
    if probe.returncode != 0:
        pytest.skip("docker is installed but not running")
    return [docker, "run", "--rm", "-i", CLICKHOUSE_IMAGE, "clickhouse-local", "--multiquery"]


def literal(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, tuple | list):
        return "(" + ", ".join(literal(item) for item in value) + ")"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def render(sql: str, parameters: dict[str, Any]) -> str:
    """Inline clickhouse-driver's `%(name)s` placeholders for the CLI."""
    for name, value in parameters.items():
        sql = sql.replace(f"%({name})s", literal(value))
    assert "%(" not in sql, sql
    return sql
