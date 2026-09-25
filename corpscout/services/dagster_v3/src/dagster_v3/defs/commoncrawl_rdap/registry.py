"""Data-driven classification of RDAP registrations against the IP registry reference data.

An RDAP answer is reusable coverage only when it is a holder's registration: not a range that
covers at least one entire IANA block designated to an RIR (registry level), and not a range
whose first address lies in space an RIR lists as available or reserved, or in an IANA block
that is reserved or not assigned at all (unallocated). The rule is written twice on purpose:
registry_class() for the enrichers and REGISTRY_CLASS_SQL for the view
rdap_network_registry_class_derived that migration 000449 embeds verbatim;
tests/test_ip_registry.py proves they agree on the same fixtures.
"""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapNetwork
from dagster_v3.defs.ip_registry.source import address_int

UNALLOCATED_STATUSES = ("available", "reserved")
REGISTRY_CLASSES = ("reusable", "registry_level", "unallocated", "unknown")


@dataclass(frozen=True)
class IanaCoverage:
    """The IANA block holding a registration's first address."""

    designation: str
    rir: str
    status: str


@dataclass(frozen=True)
class SpecialCoverage:
    """The available/reserved RIR segment holding a registration's first address."""

    registry: str
    status: str
    first: int
    last: int


@dataclass(frozen=True)
class RegistryContext:
    ready: bool
    covered_rir_blocks: int
    iana: IanaCoverage | None
    special: SpecialCoverage | None


# One round trip per RDAP miss: readiness, how many RIR-designated IANA blocks the
# registration covers entirely, the IANA block and the special segment holding its first
# address. Parameters are IPv6 texts in the shared key space (mapped_address).
REGISTRY_CONTEXT_SQL = """SELECT (SELECT ready FROM corpscout.ip_registry_ready) AS ready,
    (SELECT count() FROM corpscout.ip_registry_iana_blocks_current
     WHERE rir != '' AND toUInt128(first_ip) >= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) <= toUInt128(toIPv6(%(last)s))) AS covered_rir_blocks,
    (SELECT (any(designation), any(rir), any(status)) FROM corpscout.ip_registry_iana_blocks_current
     WHERE toUInt128(first_ip) <= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) >= toUInt128(toIPv6(%(first)s))) AS iana,
    dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(%(first)s)), ('', '', toUInt128(0), toUInt128(0))) AS special"""

# The SQL twin of registry_class() over the aliases the derived view defines: ready,
# covered_rir_blocks, iana (designation, rir, status), special (registry, status, first, last).
# Migration 000449 embeds this text verbatim.
REGISTRY_CLASS_SQL = """multiIf(
        NOT ready, 'unknown',
        covered_rir_blocks > 0, 'registry_level',
        special.2 IN ('available', 'reserved') OR iana.3 IN ('', 'RESERVED'), 'unallocated',
        'reusable') AS registry_class"""

REGISTRY_CLASS_COLUMNS = (
    "network_key",
    "registry_class",
    "network_first",
    "network_last",
    "covered_rir_blocks",
    "iana_designation",
    "iana_rir",
    "iana_status",
    "special_registry",
    "special_status",
    "special_first",
    "special_last",
    "classified_at",
)
REGISTRY_CLASS_INSERT_SQL = (
    "INSERT INTO corpscout.rdap_network_registry_class ("
    + ", ".join(REGISTRY_CLASS_COLUMNS)
    + ") VALUES"
)
# Bulk reclassification of every cached registration from the current reference snapshots.
REGISTRY_CLASS_REFRESH_SQL = (
    "INSERT INTO corpscout.rdap_network_registry_class ("
    + ", ".join(REGISTRY_CLASS_COLUMNS)
    + ")\nSELECT "
    + ", ".join(REGISTRY_CLASS_COLUMNS[:-1])
    + ", now64(3, 'UTC')\nFROM corpscout.rdap_network_registry_class_derived\n"
    "WHERE registry_class != 'unknown'"
)


def registry_class(context: RegistryContext) -> str:
    """'reusable', 'registry_level', 'unallocated', or 'unknown' while reference data is incomplete."""
    if not context.ready:
        return "unknown"
    if context.covered_rir_blocks > 0:
        return "registry_level"
    special, iana = context.special, context.iana
    if (
        (special is not None and special.status in UNALLOCATED_STATUSES)
        or iana is None
        or iana.status == "RESERVED"
    ):
        return "unallocated"
    return "reusable"


def mapped_address(address: str) -> str:
    """The IPv6 text of an address in the shared key space (IPv4 as its ::ffff: mapping)."""
    return str(IPv6Address(address_int(address)))


def fetch_registry_context(
    client, first_address: str, last_address: str
) -> RegistryContext:
    """The context of a registration; an empty answer means not ready."""
    rows = client.execute(
        REGISTRY_CONTEXT_SQL,
        {"first": mapped_address(first_address), "last": mapped_address(last_address)},
    )
    if not rows:
        return RegistryContext(
            ready=False, covered_rir_blocks=0, iana=None, special=None
        )
    ready, covered, iana, special = rows[0]
    iana = iana or ("", "", "")
    special = special or ("", "", 0, 0)
    return RegistryContext(
        ready=bool(ready),
        covered_rir_blocks=int(covered or 0),
        iana=IanaCoverage(iana[0], iana[1], iana[2]) if iana[2] else None,
        special=(
            SpecialCoverage(special[0], special[1], int(special[2]), int(special[3]))
            if special[1]
            else None
        ),
    )


@dataclass(frozen=True)
class RegistryClassification:
    registry_class: str
    context: RegistryContext
    first: int
    last: int

    @property
    def reusable(self) -> bool:
        """Unknown counts as reusable: without reference data the caches behave as before."""
        return self.registry_class in ("reusable", "unknown")

    def clickhouse_values(self, network_key: str, classified_at: datetime) -> tuple:
        iana, special = self.context.iana, self.context.special
        return (
            network_key,
            self.registry_class,
            IPv6Address(self.first),
            IPv6Address(self.last),
            self.context.covered_rir_blocks,
            iana.designation if iana else "",
            iana.rir if iana else "",
            iana.status if iana else "",
            special.registry if special else "",
            special.status if special else "",
            IPv6Address(special.first if special else 0),
            IPv6Address(special.last if special else 0),
            classified_at,
        )


def classify_registration(client, network: RdapNetwork) -> RegistryClassification:
    """Classify a normalized registration (the daily asset does the same in SQL)."""
    first, last = address_int(network.start_address), address_int(network.end_address)
    context = fetch_registry_context(client, network.start_address, network.end_address)
    return RegistryClassification(
        registry_class=registry_class(context), context=context, first=first, last=last
    )
