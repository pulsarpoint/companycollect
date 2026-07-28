from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from io import BytesIO
from typing import Any, Mapping

from lxml import etree

from dagster_v3.defs.estonia_rhr_procurement import tables
from dagster_v3.defs.ted_procurement.parser import parse_award_notice_xml

_NS = {
    "can": "urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2",
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
}
_AWARD_NOTICE_TAG = f"{{{_NS['can']}}}ContractAwardNotice"
_WHITESPACE = re.compile(r"\s+")
_AWARD_NOTICE_TYPES = {"can-standard", "can-social", "can-desg", "can-modif"}


@dataclass(frozen=True)
class ParsedRhrMonth:
    notices: list[dict[str, Any]]
    lots: list[dict[str, Any]]
    winners: list[dict[str, Any]]


def normalize_estonia_reg_code(value: Any) -> str:
    """Return an eight-digit registry code, never a guessed VAT conversion."""
    compact = _WHITESPACE.sub("", _text(value))
    return compact if re.fullmatch(r"\d{8}", compact) is not None else ""


def parse_monthly_awards(
    xml_bytes: bytes,
    *,
    ted_index: Mapping[str, Mapping[str, str]],
    partition_key: str,
    source_run_id: str,
    source_object_key: str,
    source_retrieved_at: datetime,
    resolved_at: datetime,
) -> ParsedRhrMonth:
    notices: list[dict[str, Any]] = []
    lots: list[dict[str, Any]] = []
    winners: list[dict[str, Any]] = []
    source_url = _source_url(partition_key)

    for _, root in etree.iterparse(
        BytesIO(xml_bytes), events=("end",), tag=_AWARD_NOTICE_TAG
    ):
        notice_type = _find_text(root, "cbc:NoticeTypeCode")
        if notice_type not in _AWARD_NOTICE_TYPES:
            root.clear()
            continue

        notice_id = _find_text(root, "cbc:ID")
        version_id = _find_text(root, "cbc:VersionID")
        if notice_id == "":
            root.clear()
            continue
        notice_version_id = (
            f"{notice_id}-{version_id}" if version_id != "" else notice_id
        )
        publication_date = _date(_find_text(root, "cbc:IssueDate"))
        if publication_date is None:
            root.clear()
            continue

        parsed = parse_award_notice_xml(etree.tostring(root))
        organizations = {org.org_ref: org for org in parsed.organizations}
        buyer = organizations.get(parsed.buyer_org_ref)
        buyer_id_raw = buyer.national_id_raw if buyer is not None else ""
        ted = ted_index.get(notice_id, {})
        notice_values = parsed.notice_values
        notice_common = {
            "country_code": tables.COUNTRY_CODE,
            "source_slug": tables.SOURCE_SLUG,
            "source_run_id": source_run_id,
            "notice_version_id": notice_version_id,
            "publication_date": publication_date,
            "source_object_key": source_object_key,
            "resolved_at": resolved_at,
            "partition_key": partition_key,
        }
        notices.append(
            {
                **notice_common,
                "notice_id": notice_id,
                "version_id": version_id,
                "changed_notice_version_id": _find_text(
                    root, ".//efbc:ChangedNoticeIdentifier"
                ),
                "procedure_id": _find_text(root, "cbc:ContractFolderID"),
                "notice_type": notice_type,
                "notice_subtype": _find_text(
                    root, ".//efac:NoticeSubType/cbc:SubTypeCode"
                ),
                "buyer_name": buyer.name if buyer is not None else "",
                "buyer_id_raw": buyer_id_raw,
                "buyer_reg_code": normalize_estonia_reg_code(buyer_id_raw),
                "title": _find_text(root, "cac:ProcurementProject/cbc:Name"),
                "cpv_code": _find_text(
                    root,
                    "cac:ProcurementProject/"
                    "cac:MainCommodityClassification/cbc:ItemClassificationCode",
                ),
                "ted_publication_number": _text(ted.get("publication_number")),
                "ted_publication_date": _date(ted.get("publication_date")),
                "directive_governed": "yes" if ted else "no",
                **_notice_money(root, notice_values),
                "source_url": source_url,
                "source_retrieved_at": source_retrieved_at,
            }
        )

        contracts_by_lot = _settled_contracts_by_lot(root)
        for lot in parsed.lots:
            settled = contracts_by_lot.get(lot.lot_id, [])
            lots.append(
                {
                    **notice_common,
                    "lot_id": lot.lot_id,
                    "lot_title": lot.lot_title,
                    **_lot_money(lot),
                    "settled_contract_count": len(settled),
                    "settled_contracts_json": json.dumps(
                        settled, ensure_ascii=False, sort_keys=True
                    ),
                }
            )

        tender_party_counts: dict[tuple[str, str], int] = {}
        for winner in parsed.winners:
            key = (winner.lot_id, winner.tender_id)
            tender_party_counts[key] = tender_party_counts.get(key, 0) + 1
        for winner in parsed.winners:
            organization = organizations.get(winner.org_ref)
            winner_id_raw = (
                organization.national_id_raw if organization is not None else ""
            )
            winner_country = (
                organization.country.upper() if organization is not None else ""
            )
            winner_reg_code = normalize_estonia_reg_code(winner_id_raw)
            winners.append(
                {
                    "country_code": tables.COUNTRY_CODE,
                    "source_slug": tables.SOURCE_SLUG,
                    "source_run_id": source_run_id,
                    "source_record_id": sha256(
                        (
                            f"{notice_version_id}|{winner.lot_id}|"
                            f"{winner.tender_id}|{winner.winner_ordinal}|"
                            f"{winner.org_ref}"
                        ).encode()
                    ).hexdigest(),
                    "notice_version_id": notice_version_id,
                    "notice_id": notice_id,
                    "procedure_id": _find_text(root, "cbc:ContractFolderID"),
                    "lot_id": winner.lot_id,
                    "tender_id": winner.tender_id,
                    "winner_ordinal": winner.winner_ordinal,
                    "winner_name": organization.name if organization is not None else "",
                    "winner_id_raw": winner_id_raw,
                    "winner_reg_code": winner_reg_code,
                    "winner_country": winner_country,
                    "awarded_amount_original": _decimal_text(winner.awarded_amount),
                    "awarded_amount_eur": _eur_amount(
                        winner.awarded_amount, winner.awarded_currency
                    ),
                    "awarded_amount_usd": None,
                    "awarded_currency": winner.awarded_currency,
                    "subcontracting_amount_original": _decimal_text(
                        winner.subcontracting_amount
                    ),
                    "subcontracting_amount_eur": _eur_amount(
                        winner.subcontracting_amount,
                        winner.subcontracting_currency,
                    ),
                    "subcontracting_amount_usd": None,
                    "subcontracting_currency": winner.subcontracting_currency,
                    "awarded_value_attributable": int(
                        tender_party_counts[(winner.lot_id, winner.tender_id)] == 1
                    ),
                    "publication_date": publication_date,
                    "source_object_key": source_object_key,
                    "resolved_at": resolved_at,
                    "partition_key": partition_key,
                    "match_eligibility": _match_eligibility(
                        winner_country=winner_country,
                        winner_reg_code=winner_reg_code,
                    ),
                }
            )
        root.clear()

    if not notices:
        raise ValueError("RHR month produced zero contract-award notices")
    return ParsedRhrMonth(
        notices=_deduplicate(notices, key_columns=("notice_version_id",)),
        lots=_deduplicate(
            lots, key_columns=("notice_version_id", "lot_id")
        ),
        winners=_deduplicate(winners, key_columns=("source_record_id",)),
    )


def _notice_money(root: etree._Element, values: Any) -> dict[str, Any]:
    total = root.find(".//efac:NoticeResult/cbc:TotalAmount", _NS)
    return {
        "total_value_amount_original": _decimal_text(_element_text(total)),
        "total_value_currency": _currency(total),
        "estimated_value_amount_original": _decimal_text(
            values.estimated_value_amount
        ),
        "estimated_value_currency": values.estimated_value_currency,
        "framework_maximum_amount_original": _decimal_text(
            values.framework_maximum_amount
        ),
        "framework_maximum_currency": values.framework_maximum_currency,
        "framework_total_maximum_amount_original": _decimal_text(
            values.framework_total_maximum_amount
        ),
        "framework_total_maximum_currency": (
            values.framework_total_maximum_currency
        ),
        "framework_total_approximate_amount_original": _decimal_text(
            values.framework_total_approximate_amount
        ),
        "framework_total_approximate_currency": (
            values.framework_total_approximate_currency
        ),
    }


def _lot_money(lot: Any) -> dict[str, Any]:
    return {
        f"{metric}_amount_original": _decimal_text(getattr(lot, f"{metric}_amount"))
        for metric in (
            "estimated_value",
            "framework_maximum",
            "framework_value_maximum",
            "framework_value_reestimated",
            "lower_tender",
            "higher_tender",
        )
    } | {
        f"{metric}_currency": getattr(lot, f"{metric}_currency")
        for metric in (
            "estimated_value",
            "framework_maximum",
            "framework_value_maximum",
            "framework_value_reestimated",
            "lower_tender",
            "higher_tender",
        )
    }


def _settled_contracts_by_lot(
    root: etree._Element,
) -> dict[str, list[dict[str, str]]]:
    contracts: dict[str, dict[str, str]] = {}
    for contract in root.findall(".//efac:NoticeResult/efac:SettledContract", _NS):
        contract_id = _child_text(contract, "cbc:ID")
        contracts[contract_id] = {
            "id": contract_id,
            "reference": _child_text(contract, "efac:ContractReference/cbc:ID"),
            "issue_date": _date_text(_child_text(contract, "cbc:IssueDate")),
            "title": _child_text(contract, "cbc:Title"),
        }
    by_lot: dict[str, list[dict[str, str]]] = {}
    for result in root.findall(".//efac:NoticeResult/efac:LotResult", _NS):
        lot_id = _child_text(result, "efac:TenderLot/cbc:ID")
        by_lot[lot_id] = [
            contracts[contract_id]
            for contract_id in (
                _element_text(node)
                for node in result.findall("efac:SettledContract/cbc:ID", _NS)
            )
            if contract_id in contracts
        ]
    return by_lot


def _match_eligibility(*, winner_country: str, winner_reg_code: str) -> str:
    if winner_country not in {"EE", "EST"}:
        return "foreign_winner"
    if winner_reg_code == "":
        return "invalid_identifier"
    return "eligible"


def _deduplicate(
    rows: list[dict[str, Any]], *, key_columns: tuple[str, ...]
) -> list[dict[str, Any]]:
    """Keep the last copy of keys repeated verbatim in an RHR monthly bundle."""
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        by_key[tuple(row[column] for column in key_columns)] = row
    return list(by_key.values())


def _source_url(partition_key: str) -> str:
    month = date.fromisoformat(partition_key)
    return (
        f"{tables.SOURCE_API_ROOT}/notice_award/"
        f"{month.year}/month/{month.month}/xml"
    )


def _find_text(root: etree._Element, path: str) -> str:
    return _element_text(root.find(path, _NS))


def _child_text(root: etree._Element, path: str) -> str:
    return _element_text(root.find(path, _NS))


def _element_text(node: etree._Element | None) -> str:
    return "" if node is None or node.text is None else node.text.strip()


def _currency(node: etree._Element | None) -> str:
    return "" if node is None else (node.get("currencyID") or "").upper()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _date(value: Any) -> date | None:
    text = _text(value)
    if text == "":
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _date_text(value: Any) -> str:
    parsed = _date(value)
    return parsed.isoformat() if parsed is not None else ""


def _decimal_text(value: Any) -> str | None:
    text = _text(value)
    return text if text != "" else None


def _eur_amount(value: Any, currency: str) -> str | None:
    return _decimal_text(value) if currency.upper() == "EUR" else None
