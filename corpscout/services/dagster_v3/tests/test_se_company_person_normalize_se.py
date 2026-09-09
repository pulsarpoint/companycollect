"""The Swedish person normalizer (spec 2026-09-09 section 4): a golden corpus of real and
synthetic register rows, and the role mapping it carries."""

import json
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.person.normalize_se import (
    NORMALIZER_VERSION,
    PARSE_STATUSES,
    NormalizedPerson,
    RawPerson,
    normalize_se_person,
)
from dagster_v3.defs.se_company.person.roles import role_code_for

CORPUS = Path(__file__).resolve().parent / "fixtures" / "se_persons" / "golden.jsonl"

SEQUENCE_FIELDS = ("parse_notes", "first_tokens", "middle_tokens", "last_tokens")


def corpus() -> list[dict]:
    return [json.loads(line) for line in CORPUS.read_text(encoding="utf-8").splitlines() if line.strip()]


CASES = corpus()


@pytest.mark.parametrize(
    "case",
    CASES,
    ids=[f"{c['source']}:{json.dumps(c['raw'], ensure_ascii=False)[:60]}" for c in CASES],
)
def test_golden_corpus(case: dict) -> None:
    result = normalize_se_person(RawPerson(source=case["source"], **case["raw"]))
    for field_name, value in case["expected"].items():
        actual = getattr(result, field_name)
        expected = tuple(value) if field_name in SEQUENCE_FIELDS else value
        assert actual == expected, field_name


def test_the_corpus_covers_every_status_and_every_source() -> None:
    statuses = {c["expected"]["parse_status"] for c in CASES}
    assert statuses == set(PARSE_STATUSES)
    assert {c["source"] for c in CASES} >= {"bolagsverket", "esef", "wikidata", "reviewer"}
    assert len(CASES) >= 40


def test_identity_folds_diacritics_hyphens_and_initial_periods() -> None:
    """Håkan and Hakan meet, Ö and O meet, Sven-Erik is two tokens and S.E. is two tokens."""
    accented = normalize_se_person(RawPerson(source="bolagsverket", first_name="Håkan", last_name="Öberg"))
    plain = normalize_se_person(RawPerson(source="bolagsverket", first_name="Hakan", last_name="Oberg"))
    assert accented.first_tokens == plain.first_tokens == ("hakan",)
    assert accented.last_tokens == plain.last_tokens == ("oberg",)
    # The display spelling keeps what the source delivered; only the tokens fold.
    assert accented.display_name == "Håkan Öberg" and plain.display_name == "Hakan Oberg"

    hyphen = normalize_se_person(RawPerson(source="esef", full_name="Sven-Erik Andersson"))
    spaced = normalize_se_person(RawPerson(source="esef", full_name="Sven Erik Andersson"))
    assert hyphen.first_tokens == spaced.first_tokens == ("sven",)
    assert hyphen.middle_tokens == spaced.middle_tokens == ("erik",)
    assert hyphen.display_name == "Sven-Erik Andersson"


def test_particles_glue_to_the_last_name() -> None:
    result = normalize_se_person(RawPerson(source="esef", full_name="Carl von Essen"))
    assert result.display_first == "Carl" and result.display_last == "von Essen"
    assert result.last_tokens == ("von", "essen")
    assert result.parse_status == "ok"


def test_role_code_prefers_the_key_then_the_label_then_the_label_itself() -> None:
    # The map is keyed on the source's own code, which the extractor puts in role_key.
    assert role_code_for("bolagsverket", role_original="Ordförande", role_key="chairman") == "board_chair"
    # Bolagsverket's ORIGINAL_ROLE map is keyed on the Swedish label instead.
    assert (
        role_code_for("bolagsverket", role_original="Arbetstagarrepresentant", role_key="other")
        == "employee_board_representative"
    )
    # Nothing maps: the delivered label publishes as itself, lowercased and trimmed.
    assert role_code_for("bolagsverket", role_original="  Firmatecknare ", role_key="other") == "firmatecknare"
    assert role_code_for("reviewer", role_original="Ordförande") == "ordförande"
    assert role_code_for("wikidata", role_original="board member", role_key="P3320") == "board_member"
    assert role_code_for("esef", role_original="VD", role_key="chief_executive") == "chief_executive_officer"
    assert role_code_for("bolagsverket") is None


def test_roleless_source_codes_publish_no_role() -> None:
    """A Bolagsverket signatory with an unknown role kind is person evidence, not a role
    observation -- the one roleless entry the three moved maps carry."""
    assert role_code_for("bolagsverket", role_original="Styrelseledamot", role_key="unknown") is None


def test_version_constant_and_dataclass_shape() -> None:
    assert NORMALIZER_VERSION == "se-person-normalizer-v1"
    assert PARSE_STATUSES == ("ok", "partial", "no_person")
    assert NormalizedPerson.__slots__  # frozen dataclass with slots: hashable fold inputs
    assert RawPerson().source == ""
