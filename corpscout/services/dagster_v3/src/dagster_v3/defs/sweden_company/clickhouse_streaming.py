"""Connection hardening shared by the Sweden geocoding weekly's two ClickHouse loads.

Both the demand current-outcome load (``geocode_demand``) and the canonical-address load
(``address_canonicalization``) walk a multi-million-row read in keyset pages instead of one
unbounded ``execute_iter`` over a single long-lived connection -- that pattern RESET (Errno
104) on one run and HUNG 150 minutes on another, holding the DuckDB pool and blocking the
instance. These are the knobs that make a stalled or dropped connection ERROR promptly rather
than hang; keeping them in one place stops the twin loads drifting apart.
"""

from typing import Any

# The ClickHouse server aborts a page that runs past this instead of the client blocking on a
# dead socket forever. Each page is a few seconds of work; five minutes is pure headroom.
MAX_PAGE_EXECUTION_SECONDS = 300
# clickhouse-driver socket timeouts, set on the connection before its first query (they are
# applied when the socket connects). send_receive_timeout bounds a stalled recv; tcp_keepalive
# (idle_seconds, interval_seconds, probes) makes a silently dropped peer -- the Errno 104 the
# incident also saw -- surface in ~2 minutes rather than never.
SOCKET_SEND_RECEIVE_TIMEOUT_SECONDS = 300
TCP_KEEPALIVE = (60, 15, 4)


def harden_clickhouse_socket(clickhouse_client: Any) -> None:
    """Give the driver connection a bounded socket timeout and TCP keepalive.

    Set before the first query, so it lands when clickhouse-driver connects the socket. A
    fake client (or any object without a ``connection``) is left untouched.
    """
    connection = getattr(clickhouse_client, "connection", None)
    if connection is None:
        return
    if hasattr(connection, "send_receive_timeout"):
        connection.send_receive_timeout = SOCKET_SEND_RECEIVE_TIMEOUT_SECONDS
    if hasattr(connection, "tcp_keepalive"):
        connection.tcp_keepalive = TCP_KEEPALIVE
