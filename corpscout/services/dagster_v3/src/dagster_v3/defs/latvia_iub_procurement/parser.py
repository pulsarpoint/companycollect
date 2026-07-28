import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from typing import Any

from dagster_v3.defs.latvia_iub_procurement import tables

_NON_DIGITS = re.compile(r"\D+")


@dataclass(frozen=True)
class ParsedIubDaily:
    notices: list[dict[str, Any]]
    lots: list[dict[str, Any]]
    winners: list[dict[str, Any]]
    executions: list[dict[str, Any]]


def normalize_latvia_regcode(value: Any) -> str:
    text = _text(value)
    digits = _NON_DIGITS.sub("", text)
    if text.upper().startswith("LV") and len(digits) == 11:
        return digits
    if len(digits) == 11:
        return digits
    return ""


def parse_daily_payload(
    payload: list[dict[str, Any]] | bytes,
    *,
    publication_date: date,
    source_run_id: str,
    source_object_key: str,
    source_retrieved_at: datetime,
    resolved_at: datetime,
) -> ParsedIubDaily:
    records = json.loads(payload) if isinstance(payload, bytes) else payload
    notices: list[dict[str, Any]] = []
    lots: list[dict[str, Any]] = []
    winners: list[dict[str, Any]] = []
    executions: list[dict[str, Any]] = []
    partition_key = publication_date.replace(day=1).isoformat()
    source_url = f"{tables.SOURCE_BASE_URL}/{publication_date:%Y/%m/%d-%m-%Y}.json"
    for notice in records:
        notice_id = _text(notice.get("identifier"))
        if notice_id == "":
            continue
        buyer = _mapping(notice.get("organizationData"))
        buyer_name = _text(buyer.get("name"))
        buyer_regcode = normalize_latvia_regcode(buyer.get("identifier"))
        legal_basis = _text(notice.get("procedureLegalBasis"))
        directive_governed = _directive_governed(legal_basis)
        common = {
            "country_code": tables.COUNTRY_CODE,
            "source_slug": tables.SOURCE_SLUG,
            "source_run_id": source_run_id,
            "notice_id": notice_id,
            "procedure_id": _text(notice.get("procurementProcedureIdentifier")),
            "publication_date": publication_date,
            "buyer_name": buyer_name,
            "buyer_regcode": buyer_regcode,
            "notice_title": _text(notice.get("name")),
            "source_url": source_url,
            "source_object_key": source_object_key,
            "source_retrieved_at": source_retrieved_at,
            "resolved_at": resolved_at,
            "partition_key": partition_key,
        }
        notices.append(
            {
                "country_code": tables.COUNTRY_CODE,
                "source_slug": tables.SOURCE_SLUG,
                "source_run_id": source_run_id,
                "notice_id": notice_id,
                "cloned_from": _identifier(notice.get("clonedFrom")),
                "previous_identifier": _identifier(notice.get("previousIdentifier")),
                "procedure_id": common["procedure_id"],
                "form_type": _text(notice.get("formType")),
                "notice_type": _text(notice.get("noticeType")),
                "publication_date": publication_date,
                "buyer_name": buyer_name,
                "buyer_regcode": buyer_regcode,
                "title": common["notice_title"],
                "cpv_code": _text(notice.get("cpvType")),
                "legal_basis": legal_basis,
                "directive_governed": directive_governed,
                "source_url": source_url,
                "source_object_key": source_object_key,
                "source_retrieved_at": source_retrieved_at,
                "resolved_at": resolved_at,
                "partition_key": partition_key,
            }
        )
        notice_lots = _items(notice.get("lots"))
        for lot_index, lot in enumerate(notice_lots, 1):
            lot_id = _text(lot.get("id")) or str(lot_index)
            result = _mapping(lot.get("result"))
            tendering = _mapping(lot.get("tenderingProcess"))
            statistics = _mapping(tendering.get("receivedSubmissionsStatistics"))
            additional = _mapping(lot.get("additionalInformation"))
            lots.append(
                {
                    "country_code": tables.COUNTRY_CODE,
                    "source_slug": tables.SOURCE_SLUG,
                    "source_run_id": source_run_id,
                    "notice_id": notice_id,
                    "lot_id": lot_id,
                    "lot_sequence": _integer(lot.get("sequenceNumber")) or lot_index,
                    "lot_title": _text(lot.get("name")),
                    "decision_date": _date(result.get("decisionDate")),
                    "winner_selection_status": _text(
                        result.get("winnerSelectionStatus")
                    ),
                    "estimated_value_amount_eur": _amount(
                        additional.get("estimatedValue")
                    ),
                    "lowest_tender_amount_eur": _amount(
                        tendering.get("tenderValueLowest")
                    ),
                    "highest_tender_amount_eur": _amount(
                        tendering.get("tenderValueHighest")
                    ),
                    "received_tenders": _integer(
                        statistics.get("receivedNumberOfOffers")
                    ),
                    "publication_date": publication_date,
                    "source_object_key": source_object_key,
                    "resolved_at": resolved_at,
                    "partition_key": partition_key,
                }
            )
            if _text(notice.get("formType")) == "result":
                winners.extend(
                    _award_winner_rows(
                        notice=notice,
                        lot=lot,
                        lot_id=lot_id,
                        common=common,
                        legal_basis=legal_basis,
                        directive_governed=directive_governed,
                    )
                )
        if _text(notice.get("formType")) in {"execution", "cont-modif"}:
            executions.extend(_execution_rows(notice=notice, common=common))
    return ParsedIubDaily(
        notices=notices,
        lots=lots,
        winners=winners,
        executions=executions,
    )


def _award_winner_rows(
    *,
    notice: dict[str, Any],
    lot: dict[str, Any],
    lot_id: str,
    common: dict[str, Any],
    legal_basis: str,
    directive_governed: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cpv_code = _text(notice.get("cpvType"))
    for contract_index, contract in enumerate(_items(lot.get("contracts")), 1):
        contract_id = _text(contract.get("identifier")) or _text(contract.get("id"))
        for winner_index, winner in enumerate(_items(contract.get("winners")), 1):
            parties = _items(winner.get("winnerBusinessParties"))
            tender_value_attributable = len(parties) == 1
            for party_index, party in enumerate(parties, 1):
                winner_id_raw = _text(party.get("companyId"))
                winner_country = _text(party.get("countryCode")).upper()
                is_natural_person = bool(party.get("isNaturalPerson"))
                winner_regcode = normalize_latvia_regcode(winner_id_raw)
                eligibility = _match_eligibility(
                    winner_id_raw=winner_id_raw,
                    winner_regcode=winner_regcode,
                    winner_country=winner_country,
                    is_natural_person=is_natural_person,
                )
                source_record_id = sha256(
                    (
                        f"{common['notice_id']}|{lot_id}|{contract_id}|"
                        f"{winner_index}|{party_index}|{winner_id_raw}"
                    ).encode()
                ).hexdigest()
                rows.append(
                    {
                        "country_code": common["country_code"],
                        "source_slug": common["source_slug"],
                        "source_run_id": common["source_run_id"],
                        "source_record_id": source_record_id,
                        "notice_id": common["notice_id"],
                        "procedure_id": common["procedure_id"],
                        "lot_id": lot_id,
                        "contract_id": contract_id or str(contract_index),
                        "winner_ordinal": winner_index,
                        "party_ordinal": party_index,
                        "winner_name": _text(party.get("name")),
                        "winner_id_raw": winner_id_raw,
                        "winner_regcode": winner_regcode,
                        "winner_country": winner_country,
                        "is_natural_person": int(is_natural_person),
                        "tender_value_amount_eur": _amount(winner.get("tenderValue")),
                        "tender_value_amount_usd": None,
                        "tender_value_attributable": int(tender_value_attributable),
                        "contract_conclusion_date": _date(
                            contract.get("conclusionDate")
                        ),
                        "contract_title": _text(contract.get("title")),
                        "contract_url": _text(contract.get("url")),
                        "publication_date": common["publication_date"],
                        "buyer_name": common["buyer_name"],
                        "buyer_regcode": common["buyer_regcode"],
                        "notice_title": common["notice_title"],
                        "cpv_code": cpv_code,
                        "legal_basis": legal_basis,
                        "directive_governed": directive_governed,
                        "source_url": common["source_url"],
                        "source_object_key": common["source_object_key"],
                        "source_retrieved_at": common["source_retrieved_at"],
                        "resolved_at": common["resolved_at"],
                        "partition_key": common["partition_key"],
                        "match_eligibility": eligibility,
                    }
                )
    return rows


def _execution_rows(
    *,
    notice: dict[str, Any],
    common: dict[str, Any],
) -> list[dict[str, Any]]:
    contract = _mapping(notice.get("draftContract"))
    contract_id = (
        _text(contract.get("contractIdentifier"))
        or _text(contract.get("uuid"))
        or _text(contract.get("id"))
    )
    rows: list[dict[str, Any]] = []
    winners = _items(contract.get("winners"))
    if not winners:
        winners = [{}]
    for winner_index, winner in enumerate(winners, 1):
        parties = _items(
            winner.get("businessParty")
            if "businessParty" in winner
            else winner.get("winnerBusinessParties")
        )
        if not parties:
            parties = [{}]
        for party_index, party in enumerate(parties, 1):
            winner_id_raw = _text(party.get("companyId"))
            source_record_id = sha256(
                (
                    f"{common['notice_id']}|{contract_id}|{winner_index}|"
                    f"{party_index}|{winner_id_raw}"
                ).encode()
            ).hexdigest()
            rows.append(
                {
                    "country_code": common["country_code"],
                    "source_slug": common["source_slug"],
                    "source_run_id": common["source_run_id"],
                    "source_record_id": source_record_id,
                    "notice_id": common["notice_id"],
                    "procedure_id": common["procedure_id"],
                    "contract_id": contract_id,
                    "winner_ordinal": winner_index,
                    "party_ordinal": party_index,
                    "winner_name": _text(party.get("name")),
                    "winner_id_raw": winner_id_raw,
                    "winner_regcode": normalize_latvia_regcode(winner_id_raw),
                    "winner_country": _text(party.get("countryCode")).upper(),
                    "is_natural_person": int(bool(party.get("isNaturalPerson"))),
                    "tender_value_amount_eur": _amount(winner.get("tenderValue")),
                    "contract_conclusion_date": _date(
                        contract.get("contractConclusionDate")
                    ),
                    "actual_end_date": _date(contract.get("actualDurationEndDate")),
                    "contract_title": _text(contract.get("contractTitle")),
                    "publication_date": common["publication_date"],
                    "buyer_name": common["buyer_name"],
                    "buyer_regcode": common["buyer_regcode"],
                    "notice_title": common["notice_title"],
                    "source_url": common["source_url"],
                    "source_object_key": common["source_object_key"],
                    "source_retrieved_at": common["source_retrieved_at"],
                    "resolved_at": common["resolved_at"],
                    "partition_key": common["partition_key"],
                }
            )
    return rows


def _directive_governed(legal_basis: str) -> str:
    normalized = legal_basis.lower()
    if "over" in normalized or "directive" in normalized:
        return "yes"
    if "under" in normalized or normalized in {"law-9", "iv", "mk_104", "adjil-under"}:
        return "no"
    return ""


def _match_eligibility(
    *,
    winner_id_raw: str,
    winner_regcode: str,
    winner_country: str,
    is_natural_person: bool,
) -> str:
    if is_natural_person:
        return "natural_person"
    if winner_country not in {"", "LV", "LVA"}:
        return "foreign_winner"
    if winner_id_raw == "":
        return "missing_winner_identifier"
    if winner_regcode == "":
        return "invalid_winner_identifier"
    return "eligible"


def _items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict) and value:
        return [value]
    return []


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _identifier(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("identifier") or value.get("id"))
    return _text(value)


def _integer(value: Any) -> int | None:
    text = _text(value)
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _amount(value: Any) -> str | None:
    text = _text(value)
    if text == "":
        return None
    normalized = text.replace(" ", "").replace(",", ".")
    try:
        float(normalized)
    except ValueError:
        return None
    return normalized


def _date(value: Any) -> date | None:
    text = _text(value)
    if text == "":
        return None
    for format_string in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], format_string).date()
        except ValueError:
            continue
    return None
