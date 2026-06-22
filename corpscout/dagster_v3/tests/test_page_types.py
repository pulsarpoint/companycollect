"""Tests for page-type signal matching (precision-sensitive: must not fire on prose)."""
import pytest

from commoncrawl_enrich import page_types as pt


@pytest.mark.parametrize("text,expected", [
    ("This domain is for sale. Re-visit to contact the owner.", "for_sale"),
    ("Buy this domain now", "for_sale"),
    ("Welcome to nginx! If you see this page, the nginx web server is working.", "default_server"),
    ("Apache2 Ubuntu Default Page: It works", "default_server"),
    ("Your new website is ready", "default_server"),
    ("This site is under construction", "under_construction"),
    ("Index of /pub/linux\nParent Directory\nName Last modified Size", "directory_listing"),
    ("Sedoparking domain parking", "parking_provider"),
])
def test_matches_known_page_types(text, expected):
    assert pt.match_signal(text) == expected


@pytest.mark.parametrize("text", [
    "We are a law firm. Our offices are courtesy of our partners.",   # 'courtesy of' must NOT fire
    "Original artwork for sale by the artist — make an offer.",        # generic 'for sale' must NOT fire
    "Our new product is coming soon to stores nationwide.",           # bare 'coming soon' must NOT fire
    "An index of topics covered in this course.",                     # 'index of' without listing structure
    "Welcome to our family restaurant in Lyon.",                      # 'welcome to' + brand, not nginx
])
def test_does_not_match_real_prose(text):
    assert pt.match_signal(text) is None


def test_only_scans_head():
    # a signal buried far past the head should be ignored (parking text is up top)
    text = "Real business content. " * 200 + " this domain is for sale"
    assert pt.match_signal(text) is None


def test_directory_listing_requires_structure():
    # 'index of /' alone (prose) is not enough; needs parent directory / last modified
    assert pt.match_signal("Here is an index of / our site map") is None
    assert pt.match_signal("Index of /files\nLast modified: today") == "directory_listing"
