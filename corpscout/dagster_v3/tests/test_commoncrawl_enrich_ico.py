from commoncrawl_enrich import ico


def test_checksum_valid_known_icos():
    assert ico.ico_checksum_valid("31333532") is True   # ESET
    assert ico.ico_checksum_valid("45503249") is True    # Martinus
    assert ico.ico_checksum_valid("12345678") is False
    assert ico.ico_checksum_valid("3133353") is False    # too short
    assert ico.ico_checksum_valid("abcdefgh") is False


def test_extract_icos_anchored_and_spaced():
    text = "Kontakt — IČO: 31 333 532, DIČ: 2020317068. Iné číslo 99999999."
    assert ico.extract_icos(text) == ["31333532"]  # only checksum-valid, near a label


def test_extract_dic():
    assert ico.extract_dic("DIČ: 2020317068") == "2020317068"
    assert ico.extract_dic("no tax id here") == ""
