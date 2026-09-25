"""Parsers for the IANA address-space CSVs and the RIR delegated-extended files (no I/O).

Formats verified on 2026-09-25 against the published files; see the fixtures in
tests/fixtures/ip_registry for real excerpts. Of a delegated file only the available and
reserved ipv4/ipv6 records (the special segments) are returned; every record line is still
counted so the file's own bookkeeping (records, per-type summaries) can be checked.
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
    kept as published.
    """
    reader = csv.reader(io.StringIO(text))
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
class DelegatedFile:
    header: DelegatedHeader
    summaries: dict[str, int]
    special: tuple[SpecialSegment, ...]
    record_lines: int

    def special_count(self, ip_version: int) -> int:
        return sum(1 for segment in self.special if segment.ip_version == ip_version)


def _delegation_date(value: str) -> date | None:
    if value in ("", "00000000"):
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def parse_delegated(text: str, registry: str) -> DelegatedFile:
    """A delegated-<registry>-extended file, keeping only its available/reserved records.

    Lines: '#' comments (APNIC's banner), one version line
    'version|registry|serial|records|startdate|enddate|UTCoffset', summary lines
    'registry|*|type|*|count|summary' and records
    'registry|cc|type|start|value|date|status|opaque-id' — seven fields for RIPE NCC's and
    LACNIC's available/reserved records. IPv4 value is an address count (not always a power
    of two), IPv6 value a prefix length. 'records' must equal the number of record lines and
    each ipv4/ipv6 summary its type's count over ALL statuses; asn records are counted only.
    """
    header: DelegatedHeader | None = None
    summaries: dict[str, int] = {}
    special: list[SpecialSegment] = []
    counted = {"ipv4": 0, "ipv6": 0}
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
        if kind == "asn":
            continue
        if kind not in counted:
            raise ValueError(f"{registry}: unknown type {kind!r} on line {line_number}")
        counted[kind] += 1
        if status not in SPECIAL_STATUSES:
            continue
        if kind == "ipv4":
            first_address = IPv4Address(start)
            last_address = first_address + (int(value) - 1)
            first, last = address_int(first_address), address_int(last_address)
            cidrs = tuple(
                str(cidr)
                for cidr in summarize_address_range(first_address, last_address)
            )
        else:
            network = ip_network(f"{start}/{value}")
            first, last = address_int(network[0]), address_int(network[-1])
            cidrs = (str(network),)
        special.append(
            SpecialSegment(
                registry=registry,
                cc=cc,
                ip_version=4 if kind == "ipv4" else 6,
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
    for kind in ("ipv4", "ipv6"):
        if summaries.get(kind, 0) != counted[kind]:
            raise ValueError(
                f"{registry}: {kind} summary {summaries.get(kind, 0)} != {counted[kind]} records"
            )
    return DelegatedFile(
        header=header,
        summaries=summaries,
        special=tuple(special),
        record_lines=record_lines,
    )
