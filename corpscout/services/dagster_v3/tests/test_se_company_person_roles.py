"""The per-source role maps (spec 2026-09-09 section 4.3, and 2026-09-11 section 4.3 for
Ratsit).

`role_code_for` is otherwise exercised through the normalize tests. Ratsit is the first
source whose map is keyed on the human label ALONE -- it delivers no machine code, so its
suggestions carry `role_key` NULL and the label is the only key there is -- which is worth
its own file.
"""

import pytest

from dagster_v3.defs.se_company.person.roles import (
    SOURCE_ROLE_MAPPINGS,
    SOURCE_ROLELESS_CODES,
    role_code_for,
)


@pytest.mark.parametrize(
    ("label", "code"),
    [
        ("VD", "chief_executive_officer"),
        ("Extern VD", "chief_executive_officer"),
        ("Vice VD", "deputy_chief_executive_officer"),
        ("Extern vice VD", "deputy_chief_executive_officer"),
        ("Ställföreträdande VD", "deputy_chief_executive_officer"),
        ("Extern firmatecknare", "legal_representative"),
        ("Prokurist", "procurist"),
        ("Delgivningsbar person", "other_representative"),
    ],
)
def test_every_ratsit_label_maps_to_its_catalog_code(label: str, code: str) -> None:
    """The eight labels Ratsit delivers 301,081 times (prod 2026-09-10). The extractor
    passes role_key=None: Ratsit has no machine code, so the label is the key."""
    assert role_code_for("ratsit", role_original=label, role_key=None) == code


def test_extern_keeps_the_role_and_lives_in_the_data_instead() -> None:
    """`Extern VD` is a CEO who is not an employee. The outsider flag is `data.external` on
    the suggestion (spec 4.2), never a different role code."""
    assert role_code_for("ratsit", role_original="Extern VD") == role_code_for(
        "ratsit", role_original="VD"
    )
    assert role_code_for("ratsit", role_original="Extern vice VD") == role_code_for(
        "ratsit", role_original="Vice VD"
    )


def test_an_unmapped_ratsit_label_publishes_as_itself() -> None:
    """`Aktuarie` (2 rows on prod 2026-09-10) has no catalog code; the passthrough rule
    publishes the delivered label, lowercased and whitespace-collapsed (owner ruling
    2026-08-28: never dropped, never bucketed)."""
    assert role_code_for("ratsit", role_original="  Aktuarie ") == "aktuarie"


def test_ratsit_has_a_map_and_no_roleless_labels() -> None:
    assert set(SOURCE_ROLE_MAPPINGS) == {"bolagsverket", "esef", "wikidata", "ratsit"}
    assert set(SOURCE_ROLE_MAPPINGS["ratsit"].values()) == {
        "chief_executive_officer",
        "deputy_chief_executive_officer",
        "legal_representative",
        "procurist",
        "other_representative",
    }
    # Every Ratsit label is both person evidence AND a role: nothing is roleless (spec 4.3),
    # so the source has no SOURCE_ROLELESS_CODES entry at all.
    assert "ratsit" not in SOURCE_ROLELESS_CODES
