"""RIPE Database REST lookups without personal data.

RIPE's acceptable use policy limits the personal data sets (person and role objects) one
source address may receive to 1,000 per 24 hours, while queries themselves are unlimited
within reasonable use. RIPE's RDAP `ip` answers embed the contacts as person objects, so
every one of them counts. The REST search with flags=no-referenced (whois -r) returns the
most specific inetnum/inet6num alone: netname, country, status, org, mnt-by, dates and
contact handles, but no person or role object, so nothing counts. The object is reshaped
into the RDAP document normalize_rdap_network expects, with the same handle RIPE's RDAP
uses (the range text), so network keys, segments, classes and results are unchanged.
descr, admin-c, tech-c and remarks are dropped on purpose: no personal data, not even a
name written into descr.
"""

from ipaddress import ip_network
from urllib.parse import quote

import requests

from dagster_v3.defs.commoncrawl_rdap.client import RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import RdapLookupResponse

SEARCH_URL = "https://rest.db.ripe.net/search.json"
OBJECT_URL = "https://rest.db.ripe.net/ripe/{kind}/{key}"
NETWORK_TYPES = ("inetnum", "inet6num")
QUERY_FLAGS = (
    ("flags", "no-referenced"),
    ("source", "ripe"),
    ("type-filter", "inetnum"),
    ("type-filter", "inet6num"),
)


def rdap_shape(obj: dict) -> dict:
    """An RDAP 'ip network' document built from one REST search object."""
    kind = obj.get("type")
    attributes = [
        (attribute.get("name"), attribute.get("value", ""))
        for attribute in obj.get("attributes", {}).get("attribute", [])
    ]
    values: dict[str, str] = {}
    for name, value in attributes:
        values.setdefault(name, value)
    key = values.get(kind or "")
    if kind not in NETWORK_TYPES or not key:
        raise ValueError(f"not a network object: {kind!r}")
    if kind == "inetnum":
        start, end = (part.strip() for part in key.split("-", 1))
        version = "v4"
    else:
        network = ip_network(key, strict=False)
        start, end, version = str(network[0]), str(network[-1]), "v6"
    events = [
        {"eventAction": action, "eventDate": values[attribute]}
        for attribute, action in (
            ("created", "registration"),
            ("last-modified", "last changed"),
        )
        if values.get(attribute)
    ]
    entities = (
        [
            {
                "objectClassName": "entity",
                "handle": values["org"],
                "roles": ["registrant"],
            }
        ]
        if values.get("org")
        else []
    )
    return {
        "objectClassName": "ip network",
        "handle": key,
        "startAddress": start,
        "endAddress": end,
        "ipVersion": version,
        "name": values.get("netname"),
        "type": values.get("status"),
        "country": values.get("country"),
        "status": ["active"],
        "entities": entities,
        "links": [
            {
                "rel": "self",
                "href": OBJECT_URL.format(kind=kind, key=quote(key, safe="")),
            }
        ],
        "events": events,
        "port43": "whois.ripe.net",
        "corpscout": {
            "source": "ripe-rest",
            "flags": "no-referenced",
            "mnt_by": [value for name, value in attributes if name == "mnt-by"],
        },
    }


class RipeRestClient:
    """One session; errors use RdapClient's codes."""

    def __init__(
        self, *, user_agent: str, session: requests.Session | None = None
    ) -> None:
        if user_agent.strip() == "":
            raise ValueError("user_agent must not be empty")
        self._owns_session = session is None
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = user_agent.strip()

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def lookup_ip(self, ip: str) -> RdapLookupResponse:
        try:
            response = self._session.get(
                SEARCH_URL,
                params=[("query-string", ip), *QUERY_FLAGS],
                headers={"Accept": "application/json"},
                timeout=(10, 30),
            )
        except requests.RequestException as error:
            raise RdapClientError(
                str(error), code="transport_error", retryable=True
            ) from error
        status = response.status_code
        if status == 404:
            raise RdapClientError(
                "no RIPE object", code="not_found", retryable=False, status_code=404
            )
        if status == 429:
            raise RdapClientError(
                "RIPE REST rate limit",
                code="rate_limited",
                retryable=True,
                status_code=429,
            )
        if status >= 500:
            raise RdapClientError(
                f"RIPE REST {status}",
                code="remote_server",
                retryable=True,
                status_code=status,
            )
        if status == 403:
            raise RdapClientError(
                "RIPE REST access denied",
                code="access_denied",
                retryable=False,
                status_code=403,
            )
        if status != 200:
            raise RdapClientError(
                f"RIPE REST {status}",
                code="query_error",
                retryable=False,
                status_code=status,
            )
        try:
            objects = response.json().get("objects", {}).get("object", [])
        except ValueError as error:
            raise RdapClientError(
                "RIPE REST answer is not JSON", code="invalid_response", retryable=False
            ) from error
        network = next(
            (item for item in objects if item.get("type") in NETWORK_TYPES), None
        )
        if network is None:
            raise RdapClientError(
                "no network object in the RIPE answer",
                code="not_found",
                retryable=False,
                status_code=200,
            )
        try:
            return RdapLookupResponse(rir="ripe", raw_response=rdap_shape(network))
        except ValueError as error:
            raise RdapClientError(
                str(error), code="invalid_response", retryable=False
            ) from error
