"""CNAE 2.0 vocabulary parsing, and the division bridge to NACE."""

import pytest

from dagster_v3.defs.brazil_companies.cnae.vocabulary import (
    CNAE_VERSION,
    build_cnae_category_rows,
    cnae_display_code,
    nace_division_edges,
    parse_cnae_subclasses,
)

# One real IBGE subclass, with the full hierarchy it nests.
SAMPLE = [
    {
        "id": "4781400",
        "descricao": "COMÉRCIO VAREJISTA DE ARTIGOS DO VESTUÁRIO E ACESSÓRIOS",
        "classe": {
            "id": "47814",
            "descricao": "COMÉRCIO VAREJISTA DE ARTIGOS DO VESTUÁRIO E ACESSÓRIOS",
            "grupo": {
                "id": "478",
                "descricao": "COMÉRCIO VAREJISTA DE PRODUTOS NOVOS NÃO ESPECIFICADOS",
                "divisao": {
                    "id": "47",
                    "descricao": "COMÉRCIO VAREJISTA",
                    "secao": {"id": "G", "descricao": "COMÉRCIO; REPARAÇÃO DE VEÍCULOS"},
                },
            },
        },
    },
    {
        "id": "6201501",
        "descricao": "DESENVOLVIMENTO DE PROGRAMAS DE COMPUTADOR SOB ENCOMENDA",
        "classe": {
            "id": "62015",
            "descricao": "DESENVOLVIMENTO DE PROGRAMAS DE COMPUTADOR SOB ENCOMENDA",
            "grupo": {
                "id": "620",
                "descricao": "ATIVIDADES DOS SERVIÇOS DE TECNOLOGIA DA INFORMAÇÃO",
                "divisao": {
                    "id": "62",
                    "descricao": "ATIVIDADES DOS SERVIÇOS DE TECNOLOGIA DA INFORMAÇÃO",
                    "secao": {"id": "J", "descricao": "INFORMAÇÃO E COMUNICAÇÃO"},
                },
            },
        },
    },
]


def _by_code(rows):
    return {(row.level, row.normalized_code): row for row in rows}


def test_cnae_display_code_matches_how_brazil_writes_it():
    """Registers publish 4781400; CONCLA and every Brazilian reader write
    4781-4/00."""
    assert cnae_display_code("4781400") == "4781-4/00"
    assert cnae_display_code("6201501") == "6201-5/01"
    # Shorter levels keep their own conventional form.
    assert cnae_display_code("47814") == "4781-4"
    assert cnae_display_code("478") == "47.8"
    assert cnae_display_code("47") == "47"


def test_parses_every_level_of_the_hierarchy():
    rows = _by_code(parse_cnae_subclasses(SAMPLE))
    for level, code in [
        ("section", "G"),
        ("division", "47"),
        ("group", "478"),
        ("class", "47814"),
        ("subclass", "4781400"),
    ]:
        assert (level, code) in rows, f"missing {level} {code}"


def test_keeps_the_portuguese_description_as_published():
    rows = _by_code(parse_cnae_subclasses(SAMPLE))
    assert (
        rows[("subclass", "4781400")].description_pt
        == "COMÉRCIO VAREJISTA DE ARTIGOS DO VESTUÁRIO E ACESSÓRIOS"
    )


def test_each_level_points_at_its_parent():
    rows = _by_code(parse_cnae_subclasses(SAMPLE))
    assert rows[("subclass", "4781400")].parent_normalized_code == "47814"
    assert rows[("class", "47814")].parent_normalized_code == "478"
    assert rows[("group", "478")].parent_normalized_code == "47"
    assert rows[("division", "47")].parent_normalized_code == "G"
    assert rows[("section", "G")].parent_normalized_code == ""


def test_every_row_carries_its_section_and_division():
    """So a company can be grouped without walking the tree."""
    rows = _by_code(parse_cnae_subclasses(SAMPLE))
    sub = rows[("subclass", "6201501")]
    assert sub.section_code == "J"
    assert sub.division_code == "62"


def test_deduplicates_shared_ancestors():
    """Two subclasses under one division must not produce the division twice."""
    rows = parse_cnae_subclasses(SAMPLE + SAMPLE)
    divisions = [r for r in rows if r.level == "division" and r.normalized_code == "47"]
    assert len(divisions) == 1


def test_refuses_an_empty_payload():
    with pytest.raises(ValueError, match="no CNAE subclasses"):
        parse_cnae_subclasses([])


def test_build_rows_are_tuples_in_column_order():
    rows = build_cnae_category_rows(
        subclasses=SAMPLE, source_run_id="run-1", source_url="https://example"
    )
    assert all(isinstance(row, tuple) for row in rows)
    assert all(row[0] == CNAE_VERSION for row in rows)


class TestNaceDivisionEdges:
    """The bridge is DIVISION level and nothing deeper.

    CNAE 2.0 and NACE Rev.2 both descend from ISIC Rev.4, so their 2-digit
    divisions agree -- all 87 CNAE divisions exist in NACE. Below that they
    diverge and a shared code is a false friend: CNAE 4781 is retail of
    clothing, NACE 47.81 is retail via market stalls of food. That code alone
    covers 3,687,768 Brazilian establishments, so mapping on 4-digit equality
    would misfile the single largest group in the register.
    """

    def test_maps_a_subclass_to_its_division(self):
        edges = nace_division_edges(
            parse_cnae_subclasses(SAMPLE),
            nace_divisions={"47": "Retail trade, except of motor vehicles", "62": "IT"},
        )
        by_cnae = {e.cnae_normalized_code: e for e in edges}
        assert by_cnae["4781400"].nace_normalized_code == "47"
        assert by_cnae["6201501"].nace_normalized_code == "62"

    def test_never_emits_a_four_digit_target(self):
        edges = nace_division_edges(
            parse_cnae_subclasses(SAMPLE),
            nace_divisions={"47": "Retail", "62": "IT"},
        )
        assert all(len(e.nace_normalized_code) == 2 for e in edges)

    def test_skips_a_division_nace_does_not_have(self):
        """Rather than inventing a target. Only NACE 98 is in this position."""
        edges = nace_division_edges(
            parse_cnae_subclasses(SAMPLE), nace_divisions={"47": "Retail"}
        )
        assert {e.cnae_normalized_code for e in edges} == {"4781400"}

    def test_only_subclasses_get_edges(self):
        """The mapping table is keyed on what the register publishes, and
        br_establishments publishes 7-digit subclasses."""
        edges = nace_division_edges(
            parse_cnae_subclasses(SAMPLE),
            nace_divisions={"47": "Retail", "62": "IT"},
        )
        assert all(len(e.cnae_normalized_code) == 7 for e in edges)


def test_nace_division_label_drops_the_repeated_code():
    from dagster_v3.defs.brazil_companies.cnae.vocabulary import nace_division_label

    # nace_categories stores the code inside the description; the code is
    # already its own column, so keeping it renders "NACE 47 47 Retail trade".
    assert (
        nace_division_label("47", "47 Retail trade, except of motor vehicles")
        == "Retail trade, except of motor vehicles"
    )
    # Left alone when the description does not repeat the code.
    assert nace_division_label("47", "Retail trade") == "Retail trade"
    # Must not strip a code that merely starts the same way.
    assert nace_division_label("4", "47 Retail trade") == "47 Retail trade"
