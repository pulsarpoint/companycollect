"""The CPV 2008 vocabulary: parsing the EU's XML into a tree."""

import io
import zipfile

import pytest

from dagster_v3.defs.cpv_vocabulary.source import (
    CPV_2008_XML_MEMBER,
    extract_cpv_xml,
    parse_cpv_vocabulary,
)

# Four real codes from one branch, enough to exercise every parent case. The
# gap is deliberate: 45213000 is absent, so 45213100's parent must fall back to
# the nearest ancestor that DOES exist rather than pointing at a missing node.
SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<CPV_CODE>
<CPV CODE="45000000-7">
	<TEXT LANG="DE">Bauarbeiten</TEXT>
	<TEXT LANG="EN">Construction work</TEXT>
</CPV>
<CPV CODE="45200000-9">
	<TEXT LANG="EN">Works for complete or part construction and civil engineering work</TEXT>
</CPV>
<CPV CODE="45210000-2">
	<TEXT LANG="EN">Building construction work</TEXT>
</CPV>
<CPV CODE="45213100-4">
	<TEXT LANG="EN">Construction work for commercial buildings</TEXT>
</CPV>
</CPV_CODE>
""".encode()


def _by_code(rows):
    return {row.code: row for row in rows}


def test_reads_code_and_english_label():
    rows = _by_code(parse_cpv_vocabulary(SAMPLE))
    assert rows["45000000"].label_en == "Construction work"
    assert rows["45213100"].label_en == "Construction work for commercial buildings"


def test_drops_the_check_digit():
    """The XML publishes 45000000-7; the registers publish 45000000."""
    rows = _by_code(parse_cpv_vocabulary(SAMPLE))
    assert "45000000" in rows
    assert not any("-" in row.code for row in parse_cpv_vocabulary(SAMPLE))


def test_significant_digits_place_the_node_in_the_tree():
    rows = _by_code(parse_cpv_vocabulary(SAMPLE))
    assert rows["45000000"].significant_digits == 2
    assert rows["45200000"].significant_digits == 3
    assert rows["45210000"].significant_digits == 4
    assert rows["45213100"].significant_digits == 6


def test_parent_is_the_nearest_ancestor_that_exists():
    rows = _by_code(parse_cpv_vocabulary(SAMPLE))
    assert rows["45000000"].parent_code == ""  # a division has no parent
    assert rows["45200000"].parent_code == "45000000"
    assert rows["45210000"].parent_code == "45200000"
    # 45213000 is not in this vocabulary, so the parent falls back a level.
    assert rows["45213100"].parent_code == "45210000"


def test_every_parent_is_itself_a_code():
    """A dangling parent would break the tree the filter walks."""
    rows = parse_cpv_vocabulary(SAMPLE)
    codes = {row.code for row in rows}
    for row in rows:
        if row.parent_code:
            assert row.parent_code in codes


def test_a_parent_is_always_a_prefix_of_its_child():
    """Prefix containment is what makes selecting a node select its subtree."""
    for row in parse_cpv_vocabulary(SAMPLE):
        if row.parent_code:
            assert row.code.startswith(row.parent_code.rstrip("0")[:2] or row.parent_code)


def test_skips_a_code_with_no_english_label():
    """Skipped, not stored blank -- a nameless row reads as a data bug.

    Paired with a labelled code on purpose: a document where NOTHING is
    labelled is a failed download, and that is the empty-vocabulary guard
    below, not this.
    """
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<CPV_CODE>
<CPV CODE="45000000-7"><TEXT LANG="EN">Construction work</TEXT></CPV>
<CPV CODE="45100000-8"><TEXT LANG="DE">Baustellenvorbereitung</TEXT></CPV>
</CPV_CODE>
"""
    assert [row.code for row in parse_cpv_vocabulary(xml)] == ["45000000"]


def test_rejects_an_empty_vocabulary():
    """A download that yields nothing must not be allowed to blank the table."""
    with pytest.raises(ValueError, match="no CPV codes"):
        parse_cpv_vocabulary(b'<?xml version="1.0"?><CPV_CODE></CPV_CODE>')


def test_extracts_the_main_vocabulary_not_the_supplementary_one():
    """The archive ships both. The supplementary codes are a different scheme
    (letters and a different meaning) and must never be loaded as CPV."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("code_cpv_suppl_2008.xml", b"<SUPPLEMENTARY/>")
        archive.writestr(CPV_2008_XML_MEMBER, SAMPLE)
    assert extract_cpv_xml(buffer.getvalue()) == SAMPLE


def test_missing_member_is_an_error_rather_than_an_empty_load():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("something_else.xml", b"<x/>")
    with pytest.raises(ValueError, match=CPV_2008_XML_MEMBER):
        extract_cpv_xml(buffer.getvalue())
