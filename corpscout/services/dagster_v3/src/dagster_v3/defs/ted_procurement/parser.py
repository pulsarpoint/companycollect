"""eForms ContractAwardNotice parser — country-agnostic.

Extracts organizations and resolved winner rows from one notice XML. The
winner linkage chain (verified against real FIN and SWE notices) is:

    efac:LotResult -> efac:LotTender(ref) -> efac:TenderingParty(ref)
        -> efac:Tenderer org refs (consortium = several refs)

Organizations carry the national registration number in
cac:PartyLegalEntity/cbc:CompanyID (schemeID 002) and the display name in
efac:Company/cac:PartyName/cbc:Name.
"""

from __future__ import annotations

from dataclasses import dataclass

from lxml import etree

_NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}


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


@dataclass(frozen=True)
class ParsedAwardNotice:
    buyer_org_ref: str
    organizations: tuple[ParsedOrganization, ...]
    winners: tuple[ParsedWinner, ...]


def _text(node: etree._Element | None) -> str:
    if node is None or node.text is None:
        return ""
    return node.text.strip()


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
    tenders: dict[str, tuple[str, str, str]] = {}
    parties: dict[str, tuple[str, ...]] = {}
    winners: list[ParsedWinner] = []
    if notice_result is not None:
        for tender in notice_result.findall("efac:LotTender", _NS):
            tender_id = _text(tender.find("cbc:ID", _NS))
            amount_node = tender.find(
                "cac:LegalMonetaryTotal/cbc:PayableAmount", _NS
            )
            tenders[tender_id] = (
                _text(tender.find("efac:TenderingParty/cbc:ID", _NS)),
                _text(amount_node),
                (amount_node.get("currencyID") or "") if amount_node is not None else "",
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
                party_ref, amount, currency = tenders.get(tender_id, ("", "", ""))
                for org_ref in parties.get(party_ref, ()):  # consortium-safe
                    ordinal += 1
                    winners.append(
                        ParsedWinner(
                            lot_id=lot_id,
                            tender_id=tender_id,
                            winner_ordinal=ordinal,
                            org_ref=org_ref,
                            awarded_amount=amount,
                            awarded_currency=currency,
                        )
                    )

    return ParsedAwardNotice(
        buyer_org_ref=buyer_org_ref,
        organizations=tuple(organizations),
        winners=tuple(winners),
    )
