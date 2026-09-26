"""APNIC whois over port 43 with -r: the holder object without personal data.

APNIC's RDAP answers embed contact entities; its whois service with the -r flag returns
the most specific inetnum/inet6num with contact handles only (no person, role or irt
objects), followed by matching route objects, which are ignored. The HTTP gateway
(wq.apnic.net) does not honour -r (verified 2026-09-26: it returned an irt object with an
address), so this client speaks the port-43 protocol directly: one TCP connection per
query, `-r <ip>`, read to EOF. The object is reshaped into the RDAP document
normalize_rdap_network reads. Holder name: APNIC objects rarely carry org: (2.4% of the
inetnum objects in the 2026-09-25 dump), so the FIRST descr line is the holder name;
further descr lines (addresses) and every contact handle are dropped. NIR-managed space:
when the answer is the NIR's own allocation object (netname or first descr naming JPNIC,
KRNIC, TWNIC, IDNIC, CNNIC, IRINN or VNNIC) the end holder lives in the NIR's database and
the caller falls back to RDAP, which the IANA bootstrap routes to the NIR server. mnt-by
alone decides nothing: FPT's 103.35.64.0/22 is maintained by MAINT-VN-VNNIC and IS the
holder's allocation.
"""

import re
import socket
import time
from collections.abc import Mapping
from ipaddress import ip_network

from dagster_v3.defs.commoncrawl_rdap.client import RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

WHOIS_HOST = "whois.apnic.net"
WHOIS_PORT = 43
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 30.0
# The whole exchange, however slowly the server trickles bytes.
TOTAL_TIMEOUT = 60.0
# Every complete answer (objects or %ERROR) ends with this footer; an answer without it
# was cut off and is retried, never read as "no object".
COMPLETE_MARKER = "% This query was served by"
NETWORK_TYPES = ("inetnum", "inet6num")
OBJECT_URL = "https://rdap.apnic.net/ip/{start}"
# The NIRs' own allocation objects are recognised by their first descr line naming the NIR.
# Netnames are not used: NIR members' own objects carry NIR-style netnames too (IDNIC
# assigns "IDNIC-<HOLDER>-ID" to its members), and APNIC whois -r answers every NIR except
# JPNIC with the holder's object. VNNIC spells its name both ways in APNIC's database.
NIR_DESCR_NAMES = {
    "JAPAN NETWORK INFORMATION CENTER": "jpnic",
    "KOREA NETWORK INFORMATION CENTER": "krnic",
    "KOREA INTERNET & SECURITY AGENCY": "krnic",
    "TAIWAN NETWORK INFORMATION CENTER": "twnic",
    "INDONESIA NETWORK INFORMATION CENTER": "idnic",
    "CHINA INTERNET NETWORK INFORMATION CENTER": "cnnic",
    "INDIAN REGISTRY FOR INTERNET NAMES AND NUMBERS": "irinn",
    "VIETNAM INTERNET NETWORK INFORMATION CENTRE": "vnnic",
    "VIETNAM INTERNET NETWORK INFORMATION CENTER": "vnnic",
}
_ERROR = re.compile(r"^%\s*ERROR:\s*(\d+)", re.MULTILINE)


def parse_answer(text: str) -> list[list[tuple[str, str]]]:
    """RPSL objects of a whois answer as lists of (attribute, value); % lines are comments."""
    objects: list[list[tuple[str, str]]] = []
    block: list[tuple[str, str]] = []
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        if not line.strip():
            if block:
                objects.append(block)
                block = []
            continue
        if line.startswith("%"):
            continue
        if line[0] in " \t+" and block:
            # A continuation line extends the previous attribute's value.
            block[-1] = (
                block[-1][0],
                (block[-1][1] + " " + line.strip(" \t+")).strip(),
            )
        else:
            key, _, value = line.partition(":")
            block.append((key.strip().lower(), value.strip()))
    if block:
        objects.append(block)
    return objects


def network_object(
    objects: list[list[tuple[str, str]]],
) -> list[tuple[str, str]] | None:
    """The first inetnum/inet6num object: whois answers the most specific one first."""
    return next((obj for obj in objects if obj and obj[0][0] in NETWORK_TYPES), None)


def nir_of(netname: str, descr: str) -> str:
    """The NIR whose own allocation object this is, or '' for a holder's object.

    Only the first descr line decides: it must begin with the NIR's name. ``netname`` is
    kept for the call site and ignored (see NIR_DESCR_NAMES).
    """
    first = " ".join(descr.upper().split())
    for name, nir in NIR_DESCR_NAMES.items():
        if first.startswith(name):
            return nir
    return ""


def rdap_shape(obj: list[tuple[str, str]]) -> dict:
    """An RDAP 'ip network' document built from one whois object."""
    kind, key = obj[0]
    if kind not in NETWORK_TYPES or not key:
        raise ValueError(f"not a network object: {kind!r}")
    values: dict[str, str] = {}
    descr: list[str] = []
    for name, value in obj:
        if name == "descr":
            descr.append(value)
        else:
            values.setdefault(name, value)
    if kind == "inetnum":
        start, end = (part.strip() for part in key.split("-", 1))
        version = "v4"
    else:
        network = ip_network(key, strict=False)
        start, end, version = str(network[0]), str(network[-1]), "v6"
    holder = descr[0] if descr else values.get("org")
    status = " ".join(values.get("status", "").upper().split())
    entities = []
    if holder:
        entities.append(
            {
                "objectClassName": "entity",
                "handle": values.get("org"),
                "roles": ["registrant"],
                "vcardArray": [
                    "vcard",
                    [
                        ["version", {}, "text", "4.0"],
                        ["kind", {}, "text", "org"],
                        ["fn", {}, "text", holder],
                    ],
                ],
            }
        )
    events = [
        {"eventAction": action, "eventDate": values[attribute]}
        for attribute, action in (
            ("created", "registration"),
            ("last-modified", "last changed"),
        )
        if values.get(attribute)
    ]
    return {
        "objectClassName": "ip network",
        "handle": key,
        "startAddress": start,
        "endAddress": end,
        "ipVersion": version,
        "name": values.get("netname"),
        "type": status or None,
        "country": values.get("country"),
        "status": ["active"],
        "entities": entities,
        "links": [{"rel": "self", "href": OBJECT_URL.format(start=start)}],
        "events": events,
        "port43": WHOIS_HOST,
        "corpscout": {
            "source": "apnic-whois",
            "flags": "-r",
            "nir": nir_of(values.get("netname", ""), descr[0] if descr else ""),
            "mnt_by": [value for name, value in obj if name == "mnt-by"],
            "mnt_irt": [value for name, value in obj if name == "mnt-irt"],
        },
    }


def is_nir_object(raw: Mapping) -> bool:
    """True when a reshaped whois answer is an NIR's own allocation object."""
    return bool(raw.get("corpscout", {}).get("nir"))


class ApnicWhoisClient:
    """One TCP connection per query to whois.apnic.net:43 with -r; errors use RdapClient's codes."""

    def __init__(
        self,
        *,
        host: str = WHOIS_HOST,
        port: int = WHOIS_PORT,
        connect=socket.create_connection,
        clock=time.monotonic,
    ) -> None:
        self._host, self._port, self._connect = host, port, connect
        self._clock = clock

    def close(self) -> None:
        return None  # nothing is kept open between queries

    def query(self, ip: str) -> str:
        """The raw port-43 answer to `-r <ip>`, read to EOF."""
        deadline = self._clock() + TOTAL_TIMEOUT
        try:
            with self._connect(
                (self._host, self._port), timeout=CONNECT_TIMEOUT
            ) as sock:
                sock.sendall(f"-r {ip}\r\n".encode("ascii"))
                chunks = []
                while True:
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        raise TimeoutError(
                            f"APNIC whois answer took over {TOTAL_TIMEOUT:.0f} s"
                        )
                    sock.settimeout(min(READ_TIMEOUT, remaining))
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
        except (OSError, ValueError) as error:
            raise RdapClientError(
                str(error), code="transport_error", retryable=True
            ) from error
        return b"".join(chunks).decode("utf-8", errors="replace")

    def lookup_ip(self, ip: str) -> RdapLookupResponse:
        text = self.query(ip)
        error = _ERROR.search(text)
        if error is not None:
            number = int(error.group(1))
            if number == 101:
                raise RdapClientError(
                    "no entries found", code="not_found", retryable=False
                )
            if 200 <= number < 300:
                # 2xx: access denied / connection or query limits (201 when the daily
                # query limit is hit): a rate limit, which pauses APNIC.
                raise RdapClientError(
                    f"APNIC whois access error {number}",
                    code="rate_limited",
                    retryable=True,
                )
            raise RdapClientError(
                f"APNIC whois error {number}",
                code="query_error",
                retryable=number >= 300,
            )
        if COMPLETE_MARKER not in text:
            raise RdapClientError(
                f"APNIC whois answer is empty or cut off ({len(text)} characters)",
                code="transport_error",
                retryable=True,
            )
        obj = network_object(parse_answer(text))
        if obj is None:
            raise RdapClientError(
                "no network object in the APNIC answer",
                code="not_found",
                retryable=False,
            )
        try:
            return RdapLookupResponse(rir="apnic", raw_response=rdap_shape(obj))
        except ValueError as error:
            raise RdapClientError(
                str(error), code="invalid_response", retryable=False
            ) from error
