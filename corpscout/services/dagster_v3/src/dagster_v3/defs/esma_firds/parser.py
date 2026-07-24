from dataclasses import astuple, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from collections.abc import Iterator
from typing import BinaryIO

from lxml import etree

from dagster_v3.defs.esma_firds import tables

_DELTA_EVENT_TYPES = {
    "NewRcrd": "NEW",
    "ModfdRcrd": "MODIFIED",
    "TermntdRcrd": "TERMINATED",
    "CancRcrd": "CANCELLED",
}


@dataclass(frozen=True)
class FirdsFileContext:
    source_file_id: str
    source_file_name: str
    source_file_type: str
    source_file_checksum: str
    source_publication_date: str
    source_download_url: str
    source_object_key: str
    source_run_id: str
    source_retrieved_at: str


@dataclass(frozen=True)
class FirdsRecord:
    source_record_id: str
    source_file_id: str
    source_file_name: str
    source_file_type: str
    source_file_checksum: str
    source_publication_date: str
    source_row_number: int
    event_type: str
    isin: str
    mic: str
    issuer_lei: str
    full_name: str
    short_name: str
    cfi_code: str
    notional_currency: str
    commodity_derivative: bool | None
    issuer_request: bool | None
    admission_approval_at: str
    request_admission_at: str
    first_trade_at: str
    termination_at: str
    competent_authority_country: str
    relevant_venue_mic: str
    valid_from: str
    source_download_url: str
    source_object_key: str
    source_run_id: str
    source_retrieved_at: str
    source_payload_hash: str
    resolved_at: str

    def as_tuple(self) -> tuple[object, ...]:
        values = astuple(self)
        if len(values) != len(tables.RAW_RECORD_COLUMNS):
            raise AssertionError("FIRDS record and raw table contracts diverged")
        return values


def iter_firds_records(
    xml_stream: BinaryIO,
    *,
    context: FirdsFileContext,
) -> Iterator[FirdsRecord]:
    file_type = context.source_file_type.upper()
    if file_type == "FULINS":
        target_tag = "{*}RefData"
    elif file_type == "DLTINS":
        target_tag = "{*}FinInstrm"
    elif file_type == "FULCAN":
        target_tag = "{*}CxlData"
    else:
        raise ValueError(f"Unsupported FIRDS XML file type: {file_type}")

    resolved_at = datetime.now(UTC).isoformat()
    row_number = 0
    for _, element in etree.iterparse(
        xml_stream,
        events=("end",),
        tag=target_tag,
        huge_tree=True,
        recover=False,
    ):
        row_number += 1
        if file_type == "FULINS":
            record_element = element
            event_type = "BASELINE"
        elif file_type == "DLTINS":
            record_element, event_type = _delta_record(element)
        else:
            record_element = element
            event_type = "CONSOLIDATED_CANCELLED"

        raw_xml_bytes = etree.tostring(record_element, encoding="utf-8")
        yield _record_from_element(
            record_element,
            context=context,
            row_number=row_number,
            event_type=event_type,
            raw_xml_bytes=raw_xml_bytes,
            resolved_at=resolved_at,
        )
        _clear_element(element)


def _delta_record(element: etree._Element) -> tuple[etree._Element, str]:
    for child in element:
        event_type = _DELTA_EVENT_TYPES.get(_local_name(child))
        if event_type is not None:
            return child, event_type
    raise ValueError("FIRDS DLTINS FinInstrm row has no supported event element")


def _record_from_element(
    element: etree._Element,
    *,
    context: FirdsFileContext,
    row_number: int,
    event_type: str,
    raw_xml_bytes: bytes,
    resolved_at: str,
) -> FirdsRecord:
    general = _child(element, "FinInstrmGnlAttrbts")
    venue = _child(element, "TradgVnRltdAttrbts")
    technical = _child(element, "TechAttrbts")
    isin = _text(_child(general, "Id"))
    mic = _text(_child(venue, "Id"))
    if isin == "" or mic == "":
        raise ValueError(
            f"FIRDS {context.source_file_name} row {row_number} has empty ISIN/MIC"
        )
    valid_from = _text(
        _child(_child(technical, "PblctnPrd"), "FrDt")
    ) or context.source_publication_date
    source_record_id = f"{context.source_file_id}:{row_number}"
    return FirdsRecord(
        source_record_id=source_record_id,
        source_file_id=context.source_file_id,
        source_file_name=context.source_file_name,
        source_file_type=context.source_file_type,
        source_file_checksum=context.source_file_checksum,
        source_publication_date=context.source_publication_date,
        source_row_number=row_number,
        event_type=event_type,
        isin=isin,
        mic=mic,
        issuer_lei=_text(_child(element, "Issr")),
        full_name=_text(_child(general, "FullNm")),
        short_name=_text(_child(general, "ShrtNm")),
        cfi_code=_text(_child(general, "ClssfctnTp")),
        notional_currency=_text(_child(general, "NtnlCcy")),
        commodity_derivative=_bool_text(_child(general, "CmmdtyDerivInd")),
        issuer_request=_bool_text(_child(venue, "IssrReq")),
        admission_approval_at=_text(_child(venue, "AdmssnApprvlDtByIssr")),
        request_admission_at=_text(_child(venue, "ReqForAdmssnDt")),
        first_trade_at=_text(_child(venue, "FrstTradDt")),
        termination_at=_text(_child(venue, "TermntnDt")),
        competent_authority_country=_text(
            _child(technical, "RlvntCmptntAuthrty")
        ),
        relevant_venue_mic=_text(_child(technical, "RlvntTradgVn")),
        valid_from=valid_from,
        source_download_url=context.source_download_url,
        source_object_key=context.source_object_key,
        source_run_id=context.source_run_id,
        source_retrieved_at=context.source_retrieved_at,
        source_payload_hash=sha256(raw_xml_bytes).hexdigest(),
        resolved_at=resolved_at,
    )


def _child(
    element: etree._Element | None,
    local_name: str,
) -> etree._Element | None:
    if element is None:
        return None
    for child in element:
        if _local_name(child) == local_name:
            return child
    return None


def _local_name(element: etree._Element) -> str:
    return etree.QName(element).localname


def _text(element: etree._Element | None) -> str:
    if element is None or element.text is None:
        return ""
    return element.text.strip()


def _bool_text(element: etree._Element | None) -> bool | None:
    value = _text(element).lower()
    if value == "":
        return None
    if value in {"true", "1"}:
        return True
    if value in {"false", "0"}:
        return False
    raise ValueError(f"Invalid FIRDS boolean value: {value!r}")


def _clear_element(element: etree._Element) -> None:
    parent = element.getparent()
    element.clear()
    if parent is None:
        return
    while element.getprevious() is not None:
        del parent[0]
