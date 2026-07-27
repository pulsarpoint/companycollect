"""eForms ContractAwardNotice parser — country-agnostic.

Extracts organizations, lots and resolved winner rows from one notice XML. The
winner linkage chain (verified against real FIN and SWE notices) is:

    efac:LotResult -> efac:LotTender(ref) -> efac:TenderingParty(ref)
        -> efac:Tenderer org refs (consortium = several refs)

Organizations carry the national registration number in
cac:PartyLegalEntity/cbc:CompanyID (schemeID 002) and the display name in
efac:Company/cac:PartyName/cbc:Name.

**Every monetary element the notice publishes is read, at its own grain.**
Measured across 100 live SE/FI/NO notices, the XML carries ten distinct
amounts and this parser originally read two. The one treated as *the* contract
value (BT-720, per winner) appeared on 21 of them, while BT-27's estimated
contract amount — discarded — appeared on 57. Storing a subset is the loss the
guidelines forbid, so the grains are kept apart rather than flattened:

    notice      cac:ProcurementProject/...          BT-27, BT-709
                efac:NoticeResult/...               BT-161, BT-118, BT-1118
    lot         cac:ProcurementProjectLot/...       BT-27, BT-709   (per lot)
                efac:LotResult/...                  BT-271, BT-660, BT-710, BT-711
    winner      efac:LotTender/...                  BT-720, BT-553

A framework ceiling is not money anyone spent, so BT-271/BT-709/BT-118/BT-1118
get their own columns and are never folded into a realized value.

LotResult is 1:1 with ProcurementProjectLot — verified over 75 notices, 116
lots, 116 results, no duplicates and no dangling reference — so the two lot
sources share one row.
"""

from __future__ import annotations

from dataclasses import dataclass

from lxml import etree

_NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}

# eForms buries extension elements under a fixed five-element wrapper. Spelling
# it out at each use site made the paths unreadable and easy to get subtly
# wrong, which is how FrameworkMaximumAmount was missed the first time.
_EXT = (
    "ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/efext:EformsExtension"
)
_NS["efext"] = "http://data.europa.eu/p27/eforms-ubl-extensions/1"


@dataclass(frozen=True)
class ParsedOrganization:
    org_ref: str
    name: str
    national_id_raw: str
    country: str


@dataclass(frozen=True)
class ParsedWinner:
    lot_id: str
    tender_id: str
    winner_ordinal: int
    org_ref: str
    awarded_amount: str
    awarded_currency: str
    # BT-553: the share of the tender to be subcontracted. Rare (1 notice in
    # 100) but published, and cheap to carry.
    subcontracting_amount: str = ""
    subcontracting_currency: str = ""


@dataclass(frozen=True)
class ParsedNoticeValues:
    """Notice-grain amounts other than BT-161, which arrives via the listing API."""

    # BT-27, the estimated value of the whole procedure. The single most
    # commonly published amount: 57 of 100 sampled notices.
    estimated_value_amount: str = ""
    estimated_value_currency: str = ""
    # BT-709, the maximum a framework agreement may reach.
    framework_maximum_amount: str = ""
    framework_maximum_currency: str = ""
    # BT-118, the maximum value of all contracts under the framework.
    framework_total_maximum_amount: str = ""
    framework_total_maximum_currency: str = ""
    # BT-1118, the approximate value of all contracts under the framework.
    framework_total_approximate_amount: str = ""
    framework_total_approximate_currency: str = ""


@dataclass(frozen=True)
class ParsedLot:
    """One lot, carrying both its project-side and result-side amounts.

    Safe to merge because LotResult is 1:1 with ProcurementProjectLot (verified
    over 116 lots). A lot with no result keeps the result columns empty rather
    than being dropped.
    """

    lot_id: str
    lot_title: str = ""
    estimated_value_amount: str = ""
    estimated_value_currency: str = ""
    framework_maximum_amount: str = ""
    framework_maximum_currency: str = ""
    # BT-271 / BT-660: the framework's ceiling and its revised estimate, as
    # stated on the award. Ceilings, not spend -- never merged into a value.
    framework_value_maximum_amount: str = ""
    framework_value_maximum_currency: str = ""
    framework_value_reestimated_amount: str = ""
    framework_value_reestimated_currency: str = ""
    # BT-710 / BT-711: the range of admissible tenders received for this lot.
    lower_tender_amount: str = ""
    lower_tender_currency: str = ""
    higher_tender_amount: str = ""
    higher_tender_currency: str = ""


@dataclass(frozen=True)
class ParsedAwardNotice:
    buyer_org_ref: str
    organizations: tuple[ParsedOrganization, ...]
    winners: tuple[ParsedWinner, ...]
    notice_values: ParsedNoticeValues = ParsedNoticeValues()
    lots: tuple[ParsedLot, ...] = ()


def _text(node: etree._Element | None) -> str:
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def _money(parent: etree._Element | None, path: str) -> tuple[str, str]:
    """An amount and its own currency.

    Currency comes from the element's ``currencyID`` rather than a per-notice
    default: a notice may quote a Danish framework alongside a Swedish award,
    and one shared currency column would silently mislabel one of them.
    """
    if parent is None:
        return "", ""
    node = parent.find(path, _NS)
    if node is None:
        return "", ""
    return _text(node), (node.get("currencyID") or "").upper()


def parse_award_notice_xml(xml_bytes: bytes) -> ParsedAwardNotice:
    root = etree.fromstring(xml_bytes)

    organizations = []
    for org in root.findall(".//efac:Organizations/efac:Organization", _NS):
        company = org.find("efac:Company", _NS)
        if company is None:
            continue
        organizations.append(
            ParsedOrganization(
                org_ref=_text(company.find("cac:PartyIdentification/cbc:ID", _NS)),
                name=_text(company.find("cac:PartyName/cbc:Name", _NS)),
                national_id_raw=_text(
                    company.find("cac:PartyLegalEntity/cbc:CompanyID", _NS)
                ),
                country=_text(
                    company.find(
                        "cac:PostalAddress/cac:Country/cbc:IdentificationCode", _NS
                    )
                ),
            )
        )

    # The buyer is referenced from ContractingParty/Party directly; a nested
    # ServiceProviderParty (procurement platform) also carries a ref — skip it.
    buyer_org_ref = _text(
        root.find(
            "cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID", _NS
        )
    )

    notice_result = root.find(".//efac:NoticeResult", _NS)
    tenders: dict[str, tuple[str, str, str, str, str]] = {}
    parties: dict[str, tuple[str, ...]] = {}
    winners: list[ParsedWinner] = []
    if notice_result is not None:
        for tender in notice_result.findall("efac:LotTender", _NS):
            tender_id = _text(tender.find("cbc:ID", _NS))
            # PayableAmount sits under LegalMonetaryTotal, not directly on the
            # LotTender. Reading it one level too high finds nothing and looks
            # like "this notice publishes no value".
            amount, currency = _money(
                tender, "cac:LegalMonetaryTotal/cbc:PayableAmount"
            )
            subcontract, subcontract_currency = _money(
                tender, "efac:SubcontractingTerm/efbc:TermAmount"
            )
            tenders[tender_id] = (
                _text(tender.find("efac:TenderingParty/cbc:ID", _NS)),
                amount,
                currency,
                subcontract,
                subcontract_currency,
            )
        for party in notice_result.findall("efac:TenderingParty", _NS):
            party_id = _text(party.find("cbc:ID", _NS))
            parties[party_id] = tuple(
                _text(t) for t in party.findall("efac:Tenderer/cbc:ID", _NS)
            )
        for lot_result in notice_result.findall("efac:LotResult", _NS):
            lot_id = _text(lot_result.find("efac:TenderLot/cbc:ID", _NS))
            ordinal = 0
            for tender_ref in lot_result.findall("efac:LotTender/cbc:ID", _NS):
                tender_id = _text(tender_ref)
                (
                    party_ref,
                    amount,
                    currency,
                    subcontract,
                    subcontract_currency,
                ) = tenders.get(tender_id, ("", "", "", "", ""))
                # A TenderingParty that names no Tenderer still describes a real
                # award: notice 103963-2025 declares TPA-0001 with an id and
                # nothing else, while its LotResult reads selec-w and the tender
                # carries SEK 4,800,000. Iterating an empty tuple dropped the
                # row, and with it the only record of that money. An empty
                # org_ref says "awarded, publisher did not say to whom", which
                # is the honest reading; dropping it asserted no award happened.
                org_refs = parties.get(party_ref) or ("",)
                for org_ref in org_refs:  # consortium-safe
                    ordinal += 1
                    winners.append(
                        ParsedWinner(
                            lot_id=lot_id,
                            tender_id=tender_id,
                            winner_ordinal=ordinal,
                            org_ref=org_ref,
                            awarded_amount=amount,
                            awarded_currency=currency,
                            subcontracting_amount=subcontract,
                            subcontracting_currency=subcontract_currency,
                        )
                    )

    return ParsedAwardNotice(
        buyer_org_ref=buyer_org_ref,
        organizations=tuple(organizations),
        winners=tuple(winners),
        notice_values=_parse_notice_values(root),
        lots=_parse_lots(root, notice_result),
    )


def _parse_notice_values(root: etree._Element) -> ParsedNoticeValues:
    project = root.find("cac:ProcurementProject/cac:RequestedTenderTotal", _NS)
    notice_result = root.find(".//efac:NoticeResult", _NS)

    estimated, estimated_currency = _money(project, "cbc:EstimatedOverallContractAmount")
    framework_max, framework_max_currency = _money(
        project, f"{_EXT}/efbc:FrameworkMaximumAmount"
    )
    total_max, total_max_currency = _money(
        notice_result, "efbc:OverallMaximumFrameworkContractsAmount"
    )
    total_approx, total_approx_currency = _money(
        notice_result, "efbc:OverallApproximateFrameworkContractsAmount"
    )
    return ParsedNoticeValues(
        estimated_value_amount=estimated,
        estimated_value_currency=estimated_currency,
        framework_maximum_amount=framework_max,
        framework_maximum_currency=framework_max_currency,
        framework_total_maximum_amount=total_max,
        framework_total_maximum_currency=total_max_currency,
        framework_total_approximate_amount=total_approx,
        framework_total_approximate_currency=total_approx_currency,
    )


def _parse_lots(
    root: etree._Element, notice_result: etree._Element | None
) -> tuple[ParsedLot, ...]:
    results: dict[str, etree._Element] = {}
    if notice_result is not None:
        for lot_result in notice_result.findall("efac:LotResult", _NS):
            lot_id = _text(lot_result.find("efac:TenderLot/cbc:ID", _NS))
            if lot_id:
                results.setdefault(lot_id, lot_result)

    lots: list[ParsedLot] = []
    for project_lot in root.findall("cac:ProcurementProjectLot", _NS):
        lot_id = _text(project_lot.find("cbc:ID", _NS))
        if not lot_id:
            continue
        project = project_lot.find(
            "cac:ProcurementProject/cac:RequestedTenderTotal", _NS
        )
        estimated, estimated_currency = _money(
            project, "cbc:EstimatedOverallContractAmount"
        )
        framework_max, framework_max_currency = _money(
            project, f"{_EXT}/efbc:FrameworkMaximumAmount"
        )

        lot_result = results.get(lot_id)
        framework_values = (
            lot_result.find("efac:FrameworkAgreementValues", _NS)
            if lot_result is not None
            else None
        )
        value_max, value_max_currency = _money(
            framework_values, "cbc:MaximumValueAmount"
        )
        reestimated, reestimated_currency = _money(
            framework_values, "efbc:ReestimatedValueAmount"
        )
        lower, lower_currency = _money(lot_result, "cbc:LowerTenderAmount")
        higher, higher_currency = _money(lot_result, "cbc:HigherTenderAmount")

        lots.append(
            ParsedLot(
                lot_id=lot_id,
                lot_title=_text(
                    project_lot.find("cac:ProcurementProject/cbc:Name", _NS)
                ),
                estimated_value_amount=estimated,
                estimated_value_currency=estimated_currency,
                framework_maximum_amount=framework_max,
                framework_maximum_currency=framework_max_currency,
                framework_value_maximum_amount=value_max,
                framework_value_maximum_currency=value_max_currency,
                framework_value_reestimated_amount=reestimated,
                framework_value_reestimated_currency=reestimated_currency,
                lower_tender_amount=lower,
                lower_tender_currency=lower_currency,
                higher_tender_amount=higher,
                higher_tender_currency=higher_currency,
            )
        )
    return tuple(lots)
