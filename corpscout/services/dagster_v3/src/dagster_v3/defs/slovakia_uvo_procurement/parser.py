import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Any
from urllib.parse import urljoin

from lxml import html

from dagster_v3.defs.slovakia_uvo_procurement import tables

_DETAIL_ID = re.compile(r"/detail/(\d+)")
_BULLETIN_CODE = re.compile(r"\d+\s*-\s*([A-Z]+)")
_LOT_REFERENCE = re.compile(r"(LOT-[A-Z0-9-]+)(?:\s*\((.*)\))?")
_NAME_REFERENCE = re.compile(r"\(([^()]*)\)\s*$")
_ICO = re.compile(r"^\d{8}$")


@dataclass(frozen=True)
class BulletinNotice:
    uvo_notice_id: str
    bulletin_number: str
    bulletin_code: str
    title: str
    detail_url: str
    publication_date: date


@dataclass
class _OrganizationIdentifier:
    raw: str = ""
    ico: str = ""
    country: str = ""


def parse_bulletin_issue(
    issue_html: bytes, *, publication_date: date
) -> list[BulletinNotice]:
    document = html.fromstring(issue_html)
    page_text = _clean(document.text_content())
    bulletin_match = re.search(r"vestnik\s+(?:cislo\s+)?(\d+/\d{4})", _fold(page_text))
    bulletin_number = (
        bulletin_match.group(1)
        if bulletin_match is not None
        else f"unknown/{publication_date.year}"
    )
    published_match = re.search(
        r"vestnik\s+(?:cislo\s+)?\d+/\d{4}\s*-\s*(\d{2}\.\d{2}\.\d{4})",
        _fold(page_text),
    )
    bulletin_date = (
        _date(published_match.group(1))
        if published_match is not None
        else publication_date
    ) or publication_date
    result_root_ids = {
        control
        for element in document.xpath("//*[@aria-controls]")
        if (
            (control := str(element.get("aria-controls") or ""))
            in {"vestnik-0-V", "vestnik-0-IP"}
        )
    }
    notices: list[BulletinNotice] = []
    seen_ids: set[str] = set()
    for root_id in sorted(result_root_ids):
        roots = document.xpath(f"//*[@id='{root_id}']")
        if not roots:
            continue
        for anchor in roots[0].xpath(".//a[contains(@class, 'ul-link')]"):
            href = str(anchor.get("href") or "")
            id_match = _DETAIL_ID.search(href)
            if id_match is None or id_match.group(1) in seen_ids:
                continue
            anchor_text = _clean(anchor.text_content())
            code_match = _BULLETIN_CODE.search(anchor_text)
            title_nodes = anchor.xpath(".//span")
            title = (
                _clean(title_nodes[-1].text_content()) if title_nodes else anchor_text
            )
            notice_id = id_match.group(1)
            seen_ids.add(notice_id)
            notices.append(
                BulletinNotice(
                    uvo_notice_id=notice_id,
                    bulletin_number=bulletin_number,
                    bulletin_code=(
                        code_match.group(1) if code_match is not None else ""
                    ),
                    title=title,
                    detail_url=urljoin(tables.BULLETIN_URL, href),
                    publication_date=bulletin_date,
                )
            )
    return notices


def parse_result_notice(
    detail_html: bytes,
    *,
    uvo_notice_id: str,
    bulletin_number: str,
    bulletin_code: str,
    publication_date: date,
    source_run_id: str,
    source_object_key: str,
    source_retrieved_at: datetime | None = None,
    resolved_at: datetime | None = None,
) -> list[dict[str, Any]]:
    document = html.fromstring(detail_html)
    containers = document.xpath("//*[@id='output-container']")
    root = containers[0] if containers else document
    sections = _sections(root)
    all_lines = [line for lines in sections.values() for line in lines]
    basic = sections.get("1", all_lines)
    organizations = sections.get("2", all_lines)
    procedure = sections.get("4", all_lines)
    result = sections.get("6", [])

    organization_identifiers = _organization_identifiers(organizations)
    buyer_name = _strip_reference(_field(basic, "organizacia"))
    buyer_identifier = organization_identifiers.get(
        _fold(buyer_name), _OrganizationIdentifier()
    )
    buyer_ico = buyer_identifier.ico
    procedure_id = _field(basic, "identifikator postupu")
    notice_version_id = _field(basic, "identifikator verzie oznamenia")
    legal_basis = _field(procedure, "pravny zaklad postupu")
    directive_governed = (
        "yes"
        if "smernica" in _fold(legal_basis) or "directive" in _fold(legal_basis)
        else ("no" if legal_basis else "")
    )
    title = _field(procedure, "nazov")
    cpv_code = _first_cpv(procedure)
    global_conclusion_date = _date(_field(result, "datum uzavretia zmluvy"))
    lowest_tender = _amount(_field_containing(result, "bt-710", "(hodnota)"))
    highest_tender = _amount(_field_containing(result, "bt-711", "(hodnota)"))
    notice_value = _amount(_field_containing(result, "bt-161", "(hodnota)"))
    tender_blocks = _tender_blocks(result)
    retrieved_at = source_retrieved_at or datetime.combine(
        publication_date, datetime.min.time(), tzinfo=UTC
    )
    resolved = resolved_at or retrieved_at
    rows: list[dict[str, Any]] = []
    for winner_ordinal, block in enumerate(tender_blocks, 1):
        rank = _integer(_field(block, "poradie ponuky"))
        if rank is not None and rank != 1:
            continue
        winner_reference = _field(block, "id uchadzaca")
        winner_name = _parenthesized_name(winner_reference)
        winner_identifier = organization_identifiers.get(
            _fold(winner_name), _OrganizationIdentifier()
        )
        winner_ico = winner_identifier.ico
        winner_id_raw = winner_identifier.raw
        lot_reference = _field(block, "identifikator casti alebo skupiny casti")
        lot_match = _LOT_REFERENCE.search(lot_reference)
        lot_id = lot_match.group(1) if lot_match is not None else ""
        lot_title = _clean(lot_match.group(2) or "") if lot_match is not None else ""
        amount = _amount(_field_containing(block, "bt-720", "(hodnota)"))
        currency = _currency(_field_containing(block, "bt-720", "(mena)"))
        source_record_id = sha256(
            f"{uvo_notice_id}|{lot_id}|{winner_ordinal}|{winner_id_raw}".encode()
        ).hexdigest()
        rows.append(
            {
                "country_code": tables.COUNTRY_CODE,
                "source_slug": tables.SOURCE_SLUG,
                "source_run_id": source_run_id,
                "source_record_id": source_record_id,
                "uvo_notice_id": uvo_notice_id,
                "bulletin_number": bulletin_number,
                "bulletin_code": bulletin_code,
                "publication_date": publication_date,
                "procedure_id": procedure_id,
                "notice_version_id": notice_version_id,
                "buyer_name": buyer_name,
                "buyer_ico": buyer_ico,
                "title": title,
                "cpv_code": cpv_code,
                "lot_id": lot_id,
                "lot_title": lot_title,
                "winner_ordinal": winner_ordinal,
                "winner_name": winner_name,
                "winner_id_raw": winner_id_raw,
                "winner_ico": winner_ico if _ICO.fullmatch(winner_ico) else "",
                "winner_country": winner_identifier.country,
                "contract_conclusion_date": (
                    _date(_field(block, "datum uzavretia zmluvy"))
                    or global_conclusion_date
                ),
                "awarded_amount_eur": amount,
                "awarded_amount_usd": None,
                "awarded_currency": currency,
                "lowest_tender_amount_eur": lowest_tender,
                "highest_tender_amount_eur": highest_tender,
                "notice_value_amount_eur": notice_value,
                "received_tenders": None,
                "directive_governed": directive_governed,
                "source_url": tables.DETAIL_URL_TEMPLATE.format(
                    notice_id=uvo_notice_id
                ),
                "source_object_key": source_object_key,
                "source_retrieved_at": retrieved_at,
                "resolved_at": resolved,
                "partition_key": publication_date.replace(day=1).isoformat(),
                "match_eligibility": _match_eligibility(
                    raw=winner_id_raw,
                    ico=winner_ico,
                    country=winner_identifier.country,
                ),
            }
        )
    return rows


def _sections(root: Any) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    for title in root.xpath(".//div[contains(@class, 'list-title')]"):
        title_text = _clean(title.text_content())
        number_match = re.match(r"(\d+)\.", title_text)
        key = number_match.group(1) if number_match is not None else title_text
        sibling = title.getnext()
        while sibling is not None and str(sibling.tag).lower() != "ul":
            sibling = sibling.getnext()
        if sibling is None:
            continue
        sections.setdefault(key, []).extend(
            _clean(item.text_content())
            for item in sibling.xpath(".//li")
            if _clean(item.text_content())
        )
    return sections


def _organization_identifiers(
    lines: list[str],
) -> dict[str, _OrganizationIdentifier]:
    organizations: dict[str, _OrganizationIdentifier] = {}
    current_name = ""
    for line in lines:
        folded = _fold(line)
        if folded.startswith("nazov organizacie:"):
            current_name = _value(line)
            organizations.setdefault(_fold(current_name), _OrganizationIdentifier())
        elif folded.startswith("ico:") and current_name:
            raw = _value(line)
            ico = re.sub(r"\D+", "", raw)
            identifier = organizations[_fold(current_name)]
            identifier.raw = raw
            if _ICO.fullmatch(ico):
                identifier.ico = ico
        elif (
            folded.startswith(("ic dph:", "vat:"))
            and current_name
            and organizations[_fold(current_name)].raw == ""
        ):
            organizations[_fold(current_name)].raw = _value(line)
        elif folded.startswith("krajina:") and current_name:
            organizations[_fold(current_name)].country = _country_code(_value(line))
    return organizations


def _tender_blocks(lines: list[str]) -> list[list[str]]:
    starts = [
        index
        for index, line in enumerate(lines)
        if _fold(line).startswith("zakladne informacie o ponuke")
    ]
    if not starts:
        return []
    return [
        lines[start : starts[index + 1] if index + 1 < len(starts) else len(lines)]
        for index, start in enumerate(starts)
    ]


def _field(lines: list[str], label: str) -> str:
    wanted = _fold(label)
    for line in lines:
        folded = _fold(line)
        if folded.startswith(f"{wanted}:"):
            return _value(line)
    return ""


def _field_containing(lines: list[str], *needles: str) -> str:
    folded_needles = tuple(_fold(needle) for needle in needles)
    for line in lines:
        folded = _fold(line)
        if all(needle in folded for needle in folded_needles):
            return _value(line)
    return ""


def _first_cpv(lines: list[str]) -> str:
    for line in lines:
        if "cpv" not in _fold(line):
            continue
        match = re.search(r"\b(\d{8})(?:-\d)?\b", line)
        if match is not None:
            return match.group(1)
    return ""


def _strip_reference(value: str) -> str:
    return re.sub(r"\s*\(ID:\s*\d+\)\s*$", "", value, flags=re.IGNORECASE).strip()


def _parenthesized_name(value: str) -> str:
    match = _NAME_REFERENCE.search(value)
    if match is not None:
        return _clean(match.group(1))
    return _clean(value)


def _value(line: str) -> str:
    return _clean(line.split(":", 1)[1]) if ":" in line else ""


def _fold(value: str) -> str:
    return _clean(
        "".join(
            char
            for char in unicodedata.normalize("NFKD", value)
            if not unicodedata.combining(char)
        )
    ).lower()


def _clean(value: str) -> str:
    return " ".join(value.split())


def _date(value: str) -> date | None:
    for format_string in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[:10], format_string).date()
        except ValueError:
            continue
    return None


def _amount(value: str) -> str | None:
    normalized = value.replace("\xa0", "").replace(" ", "").replace(",", ".")
    if normalized == "":
        return None
    try:
        return f"{float(normalized):.2f}"
    except ValueError:
        return None


def _integer(value: str) -> int | None:
    digits = re.sub(r"\D+", "", value)
    return int(digits) if digits else None


def _currency(value: str) -> str:
    folded = _fold(value)
    if folded in {"euro", "eur"}:
        return "EUR"
    return value.upper()


def _country_code(value: str) -> str:
    folded = _fold(value)
    if folded in {"slovensko", "slovakia", "slovenska republika", "sk"}:
        return "SK"
    return value.upper()


def _match_eligibility(*, raw: str, ico: str, country: str) -> str:
    if country not in {"", "SK"}:
        return "foreign_winner"
    if _ICO.fullmatch(ico):
        return "eligible"
    if raw == "":
        return "missing_winner_identifier"
    return "missing_winner_ico"
