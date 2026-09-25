"""Data-driven classification of RDAP registrations against the IP registry reference data.

An RDAP answer is reusable coverage only when it is a holder's registration: not a range that
covers at least one entire IANA block designated to an RIR which no holder block covers
entirely (registry level: APNIC-AP 103.0.0.0/8, but not Comcast's 73.0.0.0/8, which ARIN
lists as one allocated record), and not a range whose first address lies in space an RIR
lists as available or reserved, or in an IANA block that is reserved or not assigned at all
(unallocated).

Two layers, each with a defined home:
- The context (readiness, the count of such covered blocks, the IANA block and the special
  segment holding the first address) is computed in SQL only: REGISTRY_CONTEXT_SQL per RDAP
  miss here, and the bulk view rdap_network_registry_class_derived that migration 000449
  defines over the same objects. Both count covered blocks from the view
  ip_registry_iana_blocks_rule_current, the one home of the holder exclusion.
  registry_context() is its pure-Python reference over parsed reference rows, used by tests.
- The final classification from a context is twinned: registry_class() in Python and
  REGISTRY_CLASS_SQL, which the derived view embeds verbatim; tests/test_ip_registry.py
  proves they agree on the same fixtures.
"""

from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address

from dagster_v3.defs.commoncrawl_rdap.rdap import RdapNetwork
from dagster_v3.defs.ip_registry.source import (
    HolderBlock,
    IanaBlock,
    SpecialSegment,
    address_int,
)

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
    """What the rule needs about a registration N = [first, last].

    covered_rir_blocks counts the RIR-designated IANA blocks N covers entirely that no holder
    block (an allocated/assigned RIR record) also covers entirely.
    """

    ready: bool
    covered_rir_blocks: int
    iana: IanaCoverage | None
    special: SpecialCoverage | None


# One round trip per RDAP miss: readiness, how many RIR-designated IANA blocks the
# registration covers entirely without a holder block covering them entirely, the IANA block
# and the special segment holding its first address. Parameters are IPv6 texts in the shared
# key space (mapped_address). The holder exclusion lives once, in the view
# ip_registry_iana_blocks_rule_current (unheld_rir_block, migration 000449), which the bulk
# view rdap_network_registry_class_derived reads too: an empty holder table excludes nothing.
# The scalar subqueries are wrapped in ifNull: a scalar subquery is typed Nullable, and
# clickhouse_driver cannot read a Nullable(Tuple) column (verified on 26.5).
# Objects it reads (migration 000449): ip_registry_ready (ready UInt8),
# ip_registry_iana_blocks_rule_current, ip_registry_iana_blocks_current (first_ip, last_ip
# IPv6, designation, rir, status) and the special trie.
REGISTRY_CONTEXT_SQL = """SELECT ifNull((SELECT ready FROM corpscout.ip_registry_ready), 0) AS ready,
    ifNull((SELECT count() FROM corpscout.ip_registry_iana_blocks_rule_current
     WHERE unheld_rir_block = 1 AND toUInt128(first_ip) >= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) <= toUInt128(toIPv6(%(last)s))), 0) AS covered_rir_blocks,
    ifNull((SELECT (any(designation), any(rir), any(status)) FROM corpscout.ip_registry_iana_blocks_current
     WHERE toUInt128(first_ip) <= toUInt128(toIPv6(%(first)s)) AND toUInt128(last_ip) >= toUInt128(toIPv6(%(first)s))), ('', '', '')) AS iana,
    dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(%(first)s)), ('', '', toUInt128(0), toUInt128(0))) AS special"""

# The SQL twin of registry_class() over the aliases the derived view defines: ready,
# covered_rir_blocks, iana (designation, rir, status), special (registry, status, first, last).
# NULL ready/count/tuple elements read as not ready / 0 / '' so SQL agrees with Python on a
# missing readiness row. Migration 000449 embeds this text verbatim.
REGISTRY_CLASS_SQL = """multiIf(
        NOT ifNull(ready, 0), 'unknown',
        ifNull(covered_rir_blocks, 0) > 0, 'registry_level',
        ifNull(special.2, '') IN ('available', 'reserved') OR ifNull(iana.3, '') IN ('', 'RESERVED'), 'unallocated',
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


def registry_context(
    first: int,
    last: int,
    iana_blocks: list[IanaBlock],
    holder_blocks: list[HolderBlock],
    special_segments: list[SpecialSegment],
    *,
    ready: bool = True,
) -> RegistryContext:
    """Pure-Python reference of REGISTRY_CONTEXT_SQL over parsed reference rows."""
    held = [
        block
        for block in iana_blocks
        if any(h.first <= block.first and h.last >= block.last for h in holder_blocks)
    ]
    covered = sum(
        1
        for block in iana_blocks
        if block.rir
        and first <= block.first
        and block.last <= last
        and block not in held
    )
    iana = next((b for b in iana_blocks if b.first <= first <= b.last), None)
    special = next((s for s in special_segments if s.first <= first <= s.last), None)
    return RegistryContext(
        ready=ready,
        covered_rir_blocks=covered,
        iana=IanaCoverage(iana.designation, iana.rir, iana.status) if iana else None,
        special=(
            SpecialCoverage(
                special.registry, special.status, special.first, special.last
            )
            if special
            else None
        ),
    )


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
