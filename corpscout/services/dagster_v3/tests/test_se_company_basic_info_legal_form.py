"""The Bolagsverket -> SCB legal-form mapping: covers every token the register delivers,
lands only on SCB codes, and renders as one ClickHouse expression."""

from dagster_v3.defs.se_company.basic_info.legal_form import (
    BOLAGSVERKET_LEGAL_FORM_TO_SCB,
    SCB_LEGAL_FORM_CODES,
    bolagsverket_legal_form_sql,
)

# Every organisationsform token in se_bolagsverket_companies on 2026-09-04 (25 tokens).
REGISTER_TOKENS = {
    "AB-ORGFO", "E-ORGFO", "HB-ORGFO", "KB-ORGFO", "BRF-ORGFO", "EK-ORGFO", "FL-ORGFO",
    "I-ORGFO", "S-ORGFO", "BF-ORGFO", "KHF-ORGFO", "FAB-ORGFO", "OFB-ORGFO", "BFL-ORGFO",
    "SB-ORGFO", "BAB-ORGFO", "TSF-ORGFO", "FOF-ORGFO", "SF-ORGFO", "SE-ORGFO", "TPAB-ORGFO",
    "TPF-ORGFO", "SCE-ORGFO", "MB-ORGFO", "OTPB-ORGFO",
}


def test_every_register_token_is_mapped() -> None:
    assert set(BOLAGSVERKET_LEGAL_FORM_TO_SCB) == REGISTER_TOKENS


def test_every_mapping_lands_on_an_scb_code() -> None:
    unknown = {token: code for token, code in BOLAGSVERKET_LEGAL_FORM_TO_SCB.items() if code not in SCB_LEGAL_FORM_CODES}
    assert unknown == {}


def test_the_big_forms_map_to_the_expected_scb_codes() -> None:
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["AB-ORGFO"] == "49"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["E-ORGFO"] == "10"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["HB-ORGFO"] == BOLAGSVERKET_LEGAL_FORM_TO_SCB["KB-ORGFO"] == "31"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["BRF-ORGFO"] == "53"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["EK-ORGFO"] == "51"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["FL-ORGFO"] == "96"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["I-ORGFO"] == "61"
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["S-ORGFO"] == "72"
    # SCB 91 is an undistributed estate; a registered religious community is 63.
    assert BOLAGSVERKET_LEGAL_FORM_TO_SCB["TSF-ORGFO"] == "63"


def test_sql_maps_trimmed_tokens_and_passes_unknown_ones_through() -> None:
    sql = bolagsverket_legal_form_sql("register.legal_form_code")
    raw = "trim(ifNull(register.legal_form_code, ''))"
    assert sql.startswith(f"nullIf(transform({raw}, ['AB-ORGFO', ")
    assert "'BFL-ORGFO'], ['49', " in sql
    assert sql.endswith(f", '96'], {raw}), '')")
    assert sql.count("'") == 4 * len(BOLAGSVERKET_LEGAL_FORM_TO_SCB) + 4 + 2
