"""Legal form for the basic-info entity: SCB's *juridisk form* two-digit code.

Sweden has one national standard for legal form -- SCB's juridisk form, used by SCB,
Skatteverket and official statistics -- and one register vocabulary, Bolagsverket's
organisationsform tokens (`AB-ORGFO`, `E-ORGFO`, ...). The entity's `legal_form_code` is
the SCB code: the SCB extractor passes its register code through, and the Bolagsverket
extractor maps its token here so the main table carries a single vocabulary whichever
source wins. Bolagsverket's raw token stays in `se_bolagsverket_companies`, and
`se_code_labels` still labels both vocabularies.

A token this table does not know passes through unchanged rather than becoming NULL: a
visible `-ORGFO` value in the main table is a mapping gap to fix, a silent NULL is a
company that lost its legal form. `test_se_company_basic_info_legal_form` pins the
table against every token the register has delivered so far.
"""

# SCB juridisk form codes, as labelled in corpscout.se_code_labels (code_type 'legal_form').
SCB_LEGAL_FORM_CODES: frozenset[str] = frozenset(
    {
        "10", "21", "22", "23", "31", "41", "42", "43", "49", "51", "53", "54", "55",
        "61", "62", "63", "71", "72", "81", "82", "83", "84", "87", "88", "91", "92",
        "93", "94", "95", "96", "98", "99",
    }
)

# Bolagsverket organisationsform token -> SCB juridisk form code. Owner-approved
# 2026-09-04; the judgement calls are marked.
BOLAGSVERKET_LEGAL_FORM_TO_SCB: dict[str, str] = {
    "AB-ORGFO": "49",  # aktiebolag -> övriga aktiebolag
    "BAB-ORGFO": "41",  # bankaktiebolag
    "FAB-ORGFO": "42",  # försäkringsaktiebolag
    "TPAB-ORGFO": "42",  # tjänstepensionsaktiebolag: SCB has no own code; insurance company (judgement)
    "SE-ORGFO": "43",  # europabolag
    "E-ORGFO": "10",  # enskild näringsidkare -> fysisk person
    "HB-ORGFO": "31",  # handelsbolag: SCB does not split partnerships
    "KB-ORGFO": "31",  # kommanditbolag
    "EK-ORGFO": "51",  # ekonomisk förening
    "BF-ORGFO": "51",  # bostadsförening: an economic association in older form (judgement)
    "SF-ORGFO": "51",  # sambruksförening: association form (judgement)
    "BRF-ORGFO": "53",  # bostadsrättsförening
    "KHF-ORGFO": "54",  # kooperativ hyresrättsförening
    "SCE-ORGFO": "55",  # europakooperativ
    "I-ORGFO": "61",  # ideell förening
    "TSF-ORGFO": "63",  # registrerat trossamfund (SCB 91 is an undistributed estate, not this)
    "S-ORGFO": "72",  # stiftelse: Bolagsverket cannot tell family (71) from other (72) foundations
    "MB-ORGFO": "41",  # medlemsbank: a bank (judgement)
    "OFB-ORGFO": "92",  # ömsesidigt försäkringsbolag
    "OTPB-ORGFO": "92",  # ömsesidigt tjänstepensionsbolag: mutual insurance form (judgement)
    "SB-ORGFO": "93",  # sparbank
    "FOF-ORGFO": "94",  # försäkringsförening: the successor of understödsförening (judgement)
    "TPF-ORGFO": "94",  # tjänstepensionsförening: likewise (judgement)
    "FL-ORGFO": "96",  # filial -> utländsk juridisk person
    "BFL-ORGFO": "96",  # bankfilial
}


def _sql_array(values: list[str]) -> str:
    return "[" + ", ".join(f"'{value}'" for value in values) + "]"


def bolagsverket_legal_form_sql(column: str) -> str:
    """ClickHouse expression mapping a Bolagsverket token column to the SCB code.

    Trims and empties the token first so `' AB-ORGFO '` maps and `''`/NULL become NULL;
    an unknown token passes through unchanged (see the module docstring).
    """
    tokens = list(BOLAGSVERKET_LEGAL_FORM_TO_SCB)
    codes = [BOLAGSVERKET_LEGAL_FORM_TO_SCB[token] for token in tokens]
    raw = f"trim(ifNull({column}, ''))"
    return f"nullIf(transform({raw}, {_sql_array(tokens)}, {_sql_array(codes)}, {raw}), '')"
