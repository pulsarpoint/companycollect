"""Focused tests for the ESEF website/domain extractor's block-boundary
hyphenated line-break rejoin logic and third-party referral tagging.

``website_candidates.py`` has no dedicated test module today (its existing
coverage lives in ``test_esef_ixbrl_segments.py`` alongside the rest of the
segment parser); this module holds the bounded fix's tests instead of growing
that unrelated file further.
"""

from pathlib import Path

import pytest
from lxml import etree

from dagster_v3.defs.esef_filings.website_candidates import (
    TaggedWebsiteValue,
    _block_context,
    _block_website_values,
    _visible_blocks,
    _website_values,
    extract_website_candidates,
)

# The real 2022 Handelsbanken filing (LEI NHBDILHZTYCNBV5UYZ31) used for the
# end-to-end check. Not checked into the repo -- skip when it is absent.
_REAL_2022_FILING = Path(
    "/Users/graovic/.claude/jobs/dea16ebc/tmp/hand2022/hand-2022-12-31-sv.xhtml"
)


def _parse(xhtml: str) -> etree._ElementTree:
    return etree.fromstring(xhtml.encode("utf-8")).getroottree()


# --- Case 1: "www.handels-" / "banken.com" (www.-prefixed, corroborated) ---


def test_block_rejoin_drops_hyphen_when_corroborated_by_email_domain() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Rapporten finns tillgänglig på Handelsbankens hemsida (www.handels-</p>
        <p>banken.com). Företaget har definierat hållbarhetsmål.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains={"handelsbanken.com"},
        hyphen_corroborating_domains={"handelsbanken.com"},
    )

    assert values == ["www.handelsbanken.com"]
    assert method == "visible_text_reconstructed"
    assert "banken.com" not in values


def test_block_rejoin_keeps_hyphen_when_uncorroborated() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Rapporten finns tillgänglig på Handelsbankens hemsida (www.handels-</p>
        <p>banken.com). Företaget har definierat hållbarhetsmål.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains=set(),
        hyphen_corroborating_domains=set(),
    )

    # No corroboration anywhere for the dehyphenated reading -> today's
    # hyphenated reading is kept, but the bare fragment must never appear.
    assert values == ["www.handels-banken.com"]
    assert method == "visible_text_reconstructed"
    assert "banken.com" not in values


# --- Case 2: bare "handels-" / "bankenfonder.se" (no www./http prefix) ---


def test_block_rejoin_handles_prefix_without_scheme_or_www() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Informationen publiceras på fondbolagets hemsida, handels-</p>
        <p>bankenfonder.se. Fondbolaget anser att hänsyn till hållbarhet är viktigt.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains={"handelsbankenfonder.se"},
        hyphen_corroborating_domains={"handelsbankenfonder.se"},
    )

    assert values == ["handelsbankenfonder.se"]
    assert method == "visible_text_reconstructed"
    assert "bankenfonder.se" not in values


def test_block_rejoin_without_scheme_keeps_hyphen_when_uncorroborated() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Informationen publiceras på fondbolagets hemsida, handels-</p>
        <p>bankenfonder.se. Fondbolaget anser att hänsyn till hållbarhet är viktigt.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains=set(),
        hyphen_corroborating_domains=set(),
    )

    assert values == ["handels-bankenfonder.se"]
    assert "bankenfonder.se" not in values


# --- Uncorroborated hyphen join keeps the hyphen (generic case) ---


def test_block_rejoin_generic_uncorroborated_join_keeps_hyphen() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Broken line: www.corporate-</p>
        <p>governanceboard.se and more text about the board.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains=set(),
        hyphen_corroborating_domains=set(),
    )

    assert values == ["www.corporate-governanceboard.se"]
    assert method == "visible_text_reconstructed"


# --- A genuinely hyphenated domain on one line is untouched ---


def test_hyphenated_domain_without_line_break_is_untouched() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>Visit www.svenska-handel.se for more information.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    values, method = _block_website_values(
        blocks,
        0,
        context=_block_context(blocks, 0),
        corroborating_domains=set(),
        hyphen_corroborating_domains=set(),
    )

    assert values == ["www.svenska-handel.se"]
    assert method == "visible_text"


# --- Leading punctuation before the prefix token is stripped ---


def test_block_rejoin_strips_leading_punctuation_from_prefix_token() -> None:
    tree = _parse(
        """<html xmlns="http://www.w3.org/1999/xhtml"><body>
        <p>på Handelsbankens hemsida (www.handels-</p>
        <p>banken.com) för mer information.</p>
        </body></html>"""
    )
    blocks = _visible_blocks(tree)
    index = 1
    values, _method = _block_website_values(
        blocks,
        index,
        context=_block_context(blocks, index),
        corroborating_domains={"handelsbanken.com"},
        hyphen_corroborating_domains={"handelsbanken.com"},
    )

    assert values == ["www.handelsbanken.com"]


# --- The same-line _SPLIT_DOMAIN_PATTERN reading gets the same corroboration rule ---


def test_same_line_split_domain_drops_hyphen_when_corroborated() -> None:
    values = _website_values(
        "Broken line: www.handels- banken.com för mer information.",
        allow_bare_domains=False,
        corroborating_domains={"handelsbanken.com"},
        hyphen_corroborating_domains={"handelsbanken.com"},
    )

    assert "www.handelsbanken.com" in values
    assert "www.handels-banken.com" not in values


def test_same_line_split_domain_keeps_hyphen_when_uncorroborated() -> None:
    values = _website_values(
        "Broken line: www.corporate- governanceboard.se",
        allow_bare_domains=False,
        corroborating_domains=set(),
        hyphen_corroborating_domains=set(),
    )

    assert "www.corporate-governanceboard.se" in values
    assert "www.corporategovernanceboard.se" not in values


# --- Third-party referrals become external_reference ---


def test_third_party_referral_gets_external_reference_role(tmp_path: Path) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <body>
    <p>och krav kring biologisk mångfald. Läs mer på</p>
    <p>svanen.se/spararen.</p>
    <p>Handelsbanken Fonder erbjuder vid årets slut ytterligare information.</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    candidates = extract_website_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[],
        known_email_domains=[],
    )

    by_domain = {candidate.registrable_domain: candidate for candidate in candidates}
    assert by_domain["svanen.se"].suggested_roles == ["external_reference"]


def test_tagged_website_fact_with_lone_referral_mention_stays_company_website(
    tmp_path: Path,
) -> None:
    # A single, otherwise-referral-shaped mention ("läs mer på", no
    # company-website signal word, mentioned only once) must NOT become
    # external_reference when the domain is independently corroborated by a
    # tagged WebsitesOfLegalEntity fact.
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <body>
    <p>Läs mer på sample-oils.se om vårt hållbarhetsarbete.</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    candidates = extract_website_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[
            TaggedWebsiteValue(
                report_member="reports/report.xhtml",
                concept_local_name="WebsitesOfLegalEntity",
                value="www.sample-oils.se",
            ),
        ],
        known_email_domains=[],
    )

    by_domain = {candidate.registrable_domain: candidate for candidate in candidates}
    assert by_domain["sample-oils.se"].suggested_roles == ["company_website"]


def test_tagged_website_fact_corroborates_hyphen_rejoin_for_its_own_domain(
    tmp_path: Path,
) -> None:
    # Reviewer-reported regression: a WebsitesOfLegalEntity tag corroborates
    # its own domain for role purposes, but omitting tagged_fact_domains from
    # hyphen_corroborating_domains left a hyphenated line-break split of that
    # SAME domain elsewhere in the body unrejoined -- surviving as a second,
    # spurious hyphenated candidate instead of merging into the tagged one.
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <body>
    <p>Besök vår hemsida (www.sample-</p>
    <p>oils.se) för mer information.</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    candidates = extract_website_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[
            TaggedWebsiteValue(
                report_member="reports/report.xhtml",
                concept_local_name="WebsitesOfLegalEntity",
                value="www.sampleoils.se",
            ),
        ],
        known_email_domains=[],
    )

    by_domain = {candidate.registrable_domain: candidate for candidate in candidates}
    assert set(by_domain) == {"sampleoils.se"}
    assert by_domain["sampleoils.se"].suggested_roles == ["company_website"]
    assert {
        evidence.extraction_method for evidence in by_domain["sampleoils.se"].evidence
    } == {"tagged_fact", "visible_text_reconstructed"}


# --- End-to-end check against the real 2022 Handelsbanken filing ---


@pytest.mark.skipif(
    not _REAL_2022_FILING.exists(),
    reason="real 2022 Handelsbanken filing fixture is not present on disk",
)
def test_end_to_end_handelsbanken_2022_report_website_candidates() -> None:
    candidates = extract_website_candidates(
        {"reports/hand-2022-12-31-sv.xhtml": _REAL_2022_FILING},
        tagged_values=[],
        known_email_domains=[],
    )
    by_domain = {candidate.registrable_domain: candidate for candidate in candidates}

    # `handelsbanken.se/ir` (an investor-relations path, mentioned unbroken
    # twice) is a real, legitimate candidate in this report -- unrelated to
    # the hyphen-rejoin/external_reference fix, included here so the set
    # comparison below is exact rather than a partial membership check.
    assert set(by_domain) == {
        "handelsbanken.com",
        "handelsbanken.se",
        "handelsbankenfonder.se",
        "svanen.se",
        "ipcc.ch",
    }

    assert by_domain["handelsbanken.com"].suggested_roles == [
        "company_website",
        "report_disclosure",
    ]
    assert len(by_domain["handelsbanken.com"].evidence) == 19

    assert by_domain["handelsbanken.se"].suggested_roles == ["unknown"]
    assert len(by_domain["handelsbanken.se"].evidence) == 2

    assert by_domain["handelsbankenfonder.se"].suggested_roles == ["unknown"]
    assert len(by_domain["handelsbankenfonder.se"].evidence) == 2

    assert by_domain["svanen.se"].suggested_roles == ["external_reference"]
    assert len(by_domain["svanen.se"].evidence) == 1

    assert by_domain["ipcc.ch"].suggested_roles == ["external_reference"]
    assert len(by_domain["ipcc.ch"].evidence) == 1

    assert "banken.com" not in by_domain
    assert "bankenfonder.se" not in by_domain
