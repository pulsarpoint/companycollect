"""eForms parsing for Doffin notices.

Doffin publishes the same eForms standard as TED, but the two are read
differently here on purpose. TED's search API gives only publication metadata,
so its winners come from the XML. Doffin's search API already resolves winners
per lot -- ``lots[].winner[].organizationId`` -- while publishing no realized
value at all, so the split is:

    search JSON  -> notices, lots, winners, buyer, the estimate (BT-27)
    notice XML   -> the realized money (BT-720 per winner, BT-161 per notice)

The join between them is the org number. A winner in the search JSON carries
``organizationId``; the XML identifies the same company through
``LotTender -> TenderingParty -> Tenderer -> Organization ->
cac:PartyLegalEntity/cbc:CompanyID``, which is that same number. So BT-720 is
attached by org number rather than by position, which survives the two sources
ordering their winners differently.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from lxml import etree

_NS = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}


@dataclass(frozen=True)
class Money:
    amount: str = ""
    currency: str = ""

    def __bool__(self) -> bool:
        return self.amount != ""


@dataclass(frozen=True)
class NoticeAmounts:
    """What one notice's XML says about money, keyed for joining to winners."""

    # BT-161: realized total for the whole notice.
    notice_value: Money = field(default_factory=Money)
    # BT-720 per winning organisation, keyed on the national org number.
    value_by_org_number: dict[str, Money] = field(default_factory=dict)
    # cbc:RegulatoryDomain -- a PUBLISHED flag, not the inference the design
    # originally assumed. '32014L0024' is Directive 2014/24/EU; 'other' is
    # nationally regulated.
    regulatory_domain: str = ""
    # A stable procurement-level UUID shared by a competition notice and its
    # award notice.
    contract_folder_id: str = ""


def _text(node: etree._Element | None) -> str:
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def _money(parent: etree._Element | None, path: str) -> Money:
    if parent is None:
        return Money()
    node = parent.find(path, _NS)
    if node is None:
        return Money()
    return Money(amount=_text(node), currency=(node.get("currencyID") or "").upper())


def normalize_org_number(raw: str) -> str:
    """A Norwegian organisasjonsnummer as ``no_companies.org_number`` holds it.

    Nine bare digits. Doffin already publishes them that way, so this mostly
    passes through; it also folds the spaced and ``NO…MVA`` VAT forms, which
    reduce to the same nine digits.

    Anything else returns empty rather than a best guess. That is the important
    half: a Swedish winner's ``556516-1352`` is ten digits, and coercing it
    would either invent a Norwegian company or collide with a real one. Doffin
    notices do carry foreign winners -- the register is not Norwegian-only --
    so this path is exercised, not theoretical.
    """
    digits = "".join(character for character in raw if character.isdigit())
    return digits if len(digits) == 9 else ""


def parse_notice_amounts(xml_bytes: bytes) -> NoticeAmounts:
    """Pull the realized money and the published flags out of one notice."""
    root = etree.fromstring(xml_bytes)
    notice_result = root.find(".//efac:NoticeResult", _NS)

    # BT-161 is a direct child of NoticeResult. Anything below a LotResult is a
    # different business term, so the search is scoped rather than a .// sweep.
    notice_value = Money()
    if notice_result is not None:
        node = notice_result.find("cbc:TotalAmount", _NS)
        if node is not None:
            notice_value = Money(
                amount=_text(node), currency=(node.get("currencyID") or "").upper()
            )

    organizations: dict[str, str] = {}
    for organization in root.iterfind(
        ".//efac:Organizations/efac:Organization", _NS
    ):
        company = organization.find("efac:Company", _NS)
        if company is None:
            continue
        org_ref = _text(company.find("cac:PartyIdentification/cbc:ID", _NS))
        national_id = _text(company.find("cac:PartyLegalEntity/cbc:CompanyID", _NS))
        if org_ref:
            organizations[org_ref] = national_id

    tendering_parties: dict[str, list[str]] = {}
    if notice_result is not None:
        for party in notice_result.findall("efac:TenderingParty", _NS):
            party_id = _text(party.find("cbc:ID", _NS))
            tendering_parties[party_id] = [
                _text(tenderer.find("cbc:ID", _NS))
                for tenderer in party.findall("efac:Tenderer", _NS)
            ]

    value_by_org_number: dict[str, Money] = {}
    if notice_result is not None:
        for tender in notice_result.findall("efac:LotTender", _NS):
            # PayableAmount sits under LegalMonetaryTotal, not directly on the
            # LotTender. Reading one level too high finds nothing and looks
            # exactly like "this notice publishes no value" -- which is how the
            # first pass at this concluded Doffin had only estimates.
            amount = _money(tender, "cac:LegalMonetaryTotal/cbc:PayableAmount")
            if not amount:
                continue
            party_id = _text(tender.find("efac:TenderingParty/cbc:ID", _NS))
            for org_ref in tendering_parties.get(party_id, []):
                national_id = organizations.get(org_ref, "")
                if not national_id:
                    continue
                # A consortium shares one tender: every member is recorded
                # against the same amount rather than the amount being split
                # between them, because the split is not published.
                #
                # Keyed on BOTH the raw id and its normalised form. The search
                # JSON and the XML are two different serialisations of the same
                # register, and a foreign winner appears in neither as nine
                # digits -- keying on one form alone silently drops whichever
                # side formats differently.
                value_by_org_number[national_id] = amount
                if (normalized := normalize_org_number(national_id)):
                    value_by_org_number[normalized] = amount

    return NoticeAmounts(
        notice_value=notice_value,
        value_by_org_number=value_by_org_number,
        regulatory_domain=_text(root.find("cbc:RegulatoryDomain", _NS)),
        contract_folder_id=_text(root.find("cbc:ContractFolderID", _NS)),
    )


def value_for_winner(amounts: NoticeAmounts, org_number_raw: str) -> Money:
    """This winner's realized BT-720, looked up by org number either way round."""
    if not org_number_raw:
        return Money()
    return (
        amounts.value_by_org_number.get(org_number_raw)
        or amounts.value_by_org_number.get(normalize_org_number(org_number_raw))
        or Money()
    )


@dataclass(frozen=True)
class NoticeWinnerRow:
    """One (notice, lot, winner) row, the grain the contracts view expects."""

    doffin_id: str
    lot_id: str
    lot_heading: str
    winner_ordinal: int
    winner_name: str
    winner_org_number_raw: str
    winner_org_number: str
    value: Money


def iter_notice_winner_rows(hit: dict) -> list[NoticeWinnerRow]:
    """Project one search hit into its (lot, winner) rows.

    A lot with no winner still yields a row. An award notice can name a lot that
    was cancelled or drew no admissible tender, and dropping it would make a lot
    count a count of *won* lots -- the same mistake as counting only priced lots.
    """
    rows: list[NoticeWinnerRow] = []
    doffin_id = str(hit.get("id") or "")
    ordinal = 0
    for index, lot in enumerate(hit.get("lots") or []):
        # Doffin's search hits carry no lot id, only order and heading.
        lot_id = f"LOT-{index:04d}"
        heading = str(lot.get("heading") or "")
        winners = lot.get("winner") or []
        if not winners:
            rows.append(
                NoticeWinnerRow(
                    doffin_id=doffin_id,
                    lot_id=lot_id,
                    lot_heading=heading,
                    winner_ordinal=0,
                    winner_name="",
                    winner_org_number_raw="",
                    winner_org_number="",
                    value=Money(),
                )
            )
            continue
        for winner in winners:
            ordinal += 1
            raw = str(winner.get("organizationId") or "")
            rows.append(
                NoticeWinnerRow(
                    doffin_id=doffin_id,
                    lot_id=lot_id,
                    lot_heading=heading,
                    winner_ordinal=ordinal,
                    winner_name=str(winner.get("name") or ""),
                    winner_org_number_raw=raw,
                    winner_org_number=normalize_org_number(raw),
                    value=Money(),
                )
            )
    return rows
