import pytest

from dagster_v3.defs.esma_firds.listing_scopes import country_listing_scope


def test_sweden_listing_scope_has_five_approved_equity_venues() -> None:
    scope = country_listing_scope("se")

    assert scope.mic_codes == {"XSTO", "FNSE", "XNGM", "NSME", "XSAT"}
    assert scope.includes(mic="XSTO", cfi_code="ESVUFR") is True
    assert scope.includes(mic="XPAR", cfi_code="ESVUFR") is False
    assert scope.includes(mic="XSTO", cfi_code="DBFTFR") is False


def test_unknown_country_listing_scope_is_explicitly_rejected() -> None:
    with pytest.raises(ValueError, match="No FIRDS listing scope"):
        country_listing_scope("FR")
