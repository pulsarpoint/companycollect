"""Parsers for the IANA address-space CSVs and the RIR delegated-extended files (no I/O).

Formats verified on 2026-09-25 against the published files; see the fixtures in
tests/fixtures/ip_registry for real excerpts. Of a delegated file only two kinds of ipv4/ipv6
records are returned: the available and reserved ones (the special segments) and the
allocated/assigned ones wide enough to cover an entire IANA block (the holder blocks, e.g.
Comcast's 73.0.0.0/8). Every record line is still counted so the file's own bookkeeping
(records, per-type summaries) can be checked.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import date
from ipaddress import (
    IPv4Address,
    IPv6Address,
    ip_address,
    ip_network,
    summarize_address_range,
)

# ::ffff:0.0.0.0 — IPv4 addresses live in the IPv4-mapped range so that both families share
# one integer key space, exactly as ClickHouse's toUInt128(toIPv6(...)) maps them.
IPV4_MAPPED_OFFSET = 0xFFFF00000000
RIR_BY_DESIGNATION = {
    "AFRINIC": "afrinic",
    "APNIC": "apnic",
    "ARIN": "arin",
    "LACNIC": "lacnic",
    "RIPE NCC": "ripencc",
}
IANA_STATUSES = frozenset({"ALLOCATED", "LEGACY", "RESERVED"})
DELEGATION_STATUSES = frozenset({"allocated", "assigned", "available", "reserved"})
# The statuses whose records are kept: space no holder has.
SPECIAL_STATUSES = frozenset({"available", "reserved"})
RECORD_TYPES = ("asn", "ipv4", "ipv6")
# A record can cover an entire IANA block only if one of its CIDRs is at most as long as the
# longest prefix of an RIR-designated IANA block. Derived from the IANA files of 2026-09-25
# (every IPv4 block is a /8, the IPv6 blocks run from /12 to /23; holder_prefix_limits()
# recomputes it from any IANA snapshot). The kept records are a superset of the real holder
# blocks: whether a record covers an entire block is decided in SQL against the current
# IANA snapshot (REGISTRY_CONTEXT_SQL).
HOLDER_MAX_PREFIX = {4: 8, 6: 23}
IANA_HEADER = ["Prefix", "Designation", "Date", "WHOIS", "RDAP", "Status", "Note"]
_MD5 = re.compile(r"\b([0-9a-f]{32})\b")


def address_int(value: str | IPv4Address | IPv6Address) -> int:
    """The IPv6-mapped integer of an address (IPv4 as ::ffff:a.b.c.d)."""
    address = ip_address(value) if isinstance(value, str) else value
    if isinstance(address, IPv4Address):
        return IPV4_MAPPED_OFFSET + int(address)
    return int(address)


def designation_rir(designation: str) -> str:
    """The RIR an IANA designation names ('APNIC', 'Administered by ARIN'), else ''."""
    return RIR_BY_DESIGNATION.get(
        designation.removeprefix("Administered by ").strip(), ""
    )


@dataclass(frozen=True)
class IanaBlock:
    source: str
    prefix: str
    ip_version: int
    first: int
    last: int
    designation: str
    rir: str
    status: str
    assigned_on: str
    whois: str
    rdap: str
    note: str


def parse_iana_csv(text: str, source: str) -> list[IanaBlock]:
    """Rows of ipv4-address-space.csv (source iana_ipv4) or ipv6-unicast-address-assignments.csv.

    The IPv4 file writes 'Status [1]' (a footnote marker) and zero-padded prefixes ('008/8');
    notes may span lines inside quotes; the RDAP column can glue two URLs together and is
    kept as published. The files are published with CRLF row endings and LF line breaks
    inside notes; CRLF is normalized so both spellings parse to the same notes.
    """
    reader = csv.reader(io.StringIO(text.replace("\r\n", "\n")))
    header = [re.sub(r"\s*\[\d+\]$", "", column).strip() for column in next(reader, [])]
    if header != IANA_HEADER:
        raise ValueError(f"{source}: unexpected columns {header}")
    blocks: list[IanaBlock] = []
    for row in reader:
        if not row or not row[0].strip():
            continue
        if len(row) != 7:
            raise ValueError(f"{source}: row {row!r} does not have 7 columns")
        prefix, designation, assigned_on, whois, rdap, status, note = (
            cell.strip() for cell in row
        )
        if source == "iana_ipv4":
            octet, length = prefix.split("/")
            network = ip_network(f"{int(octet)}.0.0.0/{length}")
        else:
            network = ip_network(prefix)
        if status not in IANA_STATUSES:
            raise ValueError(f"{source}: unknown status {status!r} for {prefix}")
        blocks.append(
            IanaBlock(
                source=source,
                prefix=str(network),
                ip_version=network.version,
                first=address_int(network[0]),
                last=address_int(network[-1]),
                designation=designation,
                rir=designation_rir(designation),
                status=status,
                assigned_on=assigned_on,
                whois=whois,
                rdap=rdap,
                note=note,
            )
        )
    return blocks


def holder_prefix_limits(blocks: list[IanaBlock]) -> dict[int, int]:
    """The longest prefix of an RIR-designated IANA block per IP version (see HOLDER_MAX_PREFIX)."""
    limits: dict[int, int] = {}
    for block in blocks:
        if block.rir:
            length = int(block.prefix.split("/")[1])
            limits[block.ip_version] = max(limits.get(block.ip_version, 0), length)
    return limits


def parse_md5(text: str) -> str:
    """The digest of a .md5 file: BSD 'MD5 (name) = hex' or GNU 'hex  name'."""
    match = _MD5.search(text.lower())
    if match is None:
        raise ValueError(f"no MD5 digest in {text!r}")
    return match.group(1)


@dataclass(frozen=True)
class DelegatedHeader:
    version: str
    registry: str
    serial: str
    records: int
    start_date: str
    end_date: date
    utc_offset: str


@dataclass(frozen=True)
class SpecialSegment:
    """An available or reserved ipv4/ipv6 record of a delegated-extended file."""

    registry: str
    cc: str
    ip_version: int
    status: str
    start_address: str
    value: int
    first: int
    last: int
    cidrs: tuple[str, ...]


@dataclass(frozen=True)
class HolderBlock:
    """An allocated or assigned ipv4/ipv6 record wide enough to cover an entire IANA block.

    A registration that covers an IANA block which such a record also covers entirely is the
    holder's own (Comcast 73.0.0.0/8, JPNIC 133.0.0.0/8), not a registry-level object.
    """

    registry: str
    cc: str
    ip_version: int
    status: str
    start_address: str
    value: int
    first: int
    last: int
    cidrs: tuple[str, ...]


@dataclass(frozen=True)
class DelegatedFile:
    header: DelegatedHeader
    summaries: dict[str, int]
    special: tuple[SpecialSegment, ...]
    holders: tuple[HolderBlock, ...]
    record_lines: int

    def special_count(self, ip_version: int) -> int:
        return sum(1 for segment in self.special if segment.ip_version == ip_version)

    def holder_count(self, ip_version: int) -> int:
        return sum(1 for block in self.holders if block.ip_version == ip_version)


def _delegation_date(value: str) -> date | None:
    if value in ("", "00000000"):
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def _record_range(
    registry: str, line_number: int, kind: str, start: str, value: str
) -> tuple[int, int, tuple[str, ...]]:
    """(first, last, cidrs) of an ipv4 record (value = address count) or ipv6 record (prefix)."""
    try:
        size = int(value)
    except ValueError:
        raise ValueError(
            f"{registry}: line {line_number} has a non-numeric value {value!r}"
        ) from None
    if kind == "ipv4":
        first_address = IPv4Address(start)
        if size < 1 or int(first_address) + size - 1 > int(
            IPv4Address("255.255.255.255")
        ):
            raise ValueError(
                f"{registry}: line {line_number}: {start} + {size} addresses is not an IPv4 range"
            )
        last_address = first_address + (size - 1)
        cidrs = tuple(
            str(cidr) for cidr in summarize_address_range(first_address, last_address)
        )
        return address_int(first_address), address_int(last_address), cidrs
    if not 0 <= size <= 128:
        raise ValueError(
            f"{registry}: line {line_number}: /{size} is not an IPv6 prefix"
        )
    network = ip_network(f"{start}/{size}")
    return address_int(network[0]), address_int(network[-1]), (str(network),)


def _may_be_holder_block(
    kind: str, value: str, holder_max_prefix: dict[int, int]
) -> bool:
    """Cheap pre-filter: can this record contain a CIDR of an IANA block's size?"""
    if kind == "ipv4":
        return value.isdigit() and int(value) >= 2 ** (32 - holder_max_prefix[4])
    return value.isdigit() and int(value) <= holder_max_prefix[6]


def parse_delegated(
    text: str,
    registry: str,
    holder_max_prefix: dict[int, int] = HOLDER_MAX_PREFIX,
) -> DelegatedFile:
    """A delegated-<registry>-extended file, keeping its special segments and holder blocks.

    Lines: '#' comments (APNIC's banner), one version line
    'version|registry|serial|records|startdate|enddate|UTCoffset', summary lines
    'registry|*|type|*|count|summary' (one each for asn, ipv4 and ipv6) and records
    'registry|cc|type|start|value|date|status|opaque-id' — seven fields for RIPE NCC's and
    LACNIC's available/reserved records. IPv4 value is an address count (not always a power
    of two), IPv6 value a prefix length. 'records' must equal the number of record lines and
    each summary its type's count over ALL statuses. Kept: available/reserved ipv4/ipv6
    records (special) and allocated/assigned ipv4/ipv6 records with a CIDR no longer than
    holder_max_prefix for their version (holders).
    """
    header: DelegatedHeader | None = None
    summaries: dict[str, int] = {}
    special: list[SpecialSegment] = []
    holders: list[HolderBlock] = []
    counted = dict.fromkeys(RECORD_TYPES, 0)
    record_lines = 0
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("|")
        if header is None:
            if len(fields) != 7 or fields[0] not in ("2", "2.3"):
                raise ValueError(
                    f"{registry}: line {line_number} is not a version line: {line!r}"
                )
            if fields[1] != registry:
                raise ValueError(f"{registry}: file belongs to {fields[1]!r}")
            end_date = _delegation_date(fields[5])
            if end_date is None:
                raise ValueError(f"{registry}: version line has no end date: {line!r}")
            header = DelegatedHeader(
                version=fields[0],
                registry=fields[1],
                serial=fields[2],
                records=int(fields[3]),
                start_date=fields[4],
                end_date=end_date,
                utc_offset=fields[6],
            )
            continue
        if len(fields) == 6 and fields[1] == "*" and fields[5] == "summary":
            if fields[0] != registry:
                raise ValueError(
                    f"{registry}: summary line {line_number} belongs to {fields[0]!r}"
                )
            if fields[2] not in RECORD_TYPES:
                raise ValueError(
                    f"{registry}: summary line {line_number} has unknown type {fields[2]!r}"
                )
            if fields[2] in summaries:
                raise ValueError(
                    f"{registry}: duplicate {fields[2]} summary on line {line_number}"
                )
            summaries[fields[2]] = int(fields[4])
            continue
        if len(fields) < 7:
            raise ValueError(
                f"{registry}: line {line_number} has {len(fields)} fields: {line!r}"
            )
        if fields[0] != registry:
            raise ValueError(f"{registry}: line {line_number} belongs to {fields[0]!r}")
        record_lines += 1
        _, cc, kind, start, value, _delegated, status = fields[:7]
        if status not in DELEGATION_STATUSES:
            raise ValueError(
                f"{registry}: unknown status {status!r} on line {line_number}"
            )
        if kind not in counted:
            raise ValueError(f"{registry}: unknown type {kind!r} on line {line_number}")
        counted[kind] += 1
        if kind == "asn":
            continue
        ip_version = 4 if kind == "ipv4" else 6
        if status in SPECIAL_STATUSES:
            first, last, cidrs = _record_range(
                registry, line_number, kind, start, value
            )
            special.append(
                SpecialSegment(
                    registry=registry,
                    cc=cc,
                    ip_version=ip_version,
                    status=status,
                    start_address=start,
                    value=int(value),
                    first=first,
                    last=last,
                    cidrs=cidrs,
                )
            )
        elif _may_be_holder_block(kind, value, holder_max_prefix):
            first, last, cidrs = _record_range(
                registry, line_number, kind, start, value
            )
            limit = holder_max_prefix[ip_version]
            if any(int(cidr.split("/")[1]) <= limit for cidr in cidrs):
                holders.append(
                    HolderBlock(
                        registry=registry,
                        cc=cc,
                        ip_version=ip_version,
                        status=status,
                        start_address=start,
                        value=int(value),
                        first=first,
                        last=last,
                        cidrs=cidrs,
                    )
                )
    if header is None:
        raise ValueError(f"{registry}: no version line")
    if record_lines != header.records:
        raise ValueError(
            f"{registry}: header announces {header.records} records, file has {record_lines}"
        )
    for kind in RECORD_TYPES:
        if kind not in summaries:
            raise ValueError(f"{registry}: no {kind} summary line")
        if summaries[kind] != counted[kind]:
            raise ValueError(
                f"{registry}: {kind} summary {summaries[kind]} != {counted[kind]} records"
            )
    return DelegatedFile(
        header=header,
        summaries=summaries,
        special=tuple(special),
        holders=tuple(holders),
        record_lines=record_lines,
    )
