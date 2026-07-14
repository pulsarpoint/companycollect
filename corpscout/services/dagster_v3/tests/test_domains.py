import pytest

from dagster_v3.domains import normalized_url_parts, root_domain


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://www.example.no/path", "example.no"),
        ("SHOP.Example.fi/catalog", "example.fi"),
        ("http://forums.bbc.co.uk/news", "bbc.co.uk"),
        ("example.no", "example.no"),
        ("", ""),
        ("localhost", ""),
        ("127.0.0.1", ""),
        ("https://127.0.0.1/path", ""),
    ],
)
def test_root_domain_uses_public_suffix_rules(raw: str, expected: str) -> None:
    assert root_domain(raw) == expected


def test_normalized_url_parts_adds_scheme_and_lowercases_host() -> None:
    normalized_url, host, path = normalized_url_parts("WWW.Example.NO/path")

    assert normalized_url == "https://www.example.no/path"
    assert host == "www.example.no"
    assert path == "/path"


@pytest.mark.parametrize(
    "raw",
    [
        # real garbage from EE contact data: an address mislabeled as a website.
        "U-Box 240 20229.MALMÖ",
        "https://foo:bar/x",  # non-numeric port -> urllib ValueError
        ":::",
        "http://[invalid",
    ],
)
def test_malformed_urls_degrade_to_empty_not_crash(raw: str) -> None:
    # Must not raise; a value that carries no parseable host yields no domain.
    assert root_domain(raw) == ""
