from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_companies.rfb import relations, tables

DATASET = tables.DLT_DATASET_NAME


def _socios_stage(path: Path) -> None:
    """A raw socios stage with all three partner kinds plus a legal
    representative -- the two-edges-per-row case."""
    connection = duckdb.connect(str(path))
    connection.execute(f"create schema if not exists {DATASET}")
    connection.execute(
        f"""
        create table {DATASET}.socios_raw (
            cnpj_basico varchar, identificador_socio varchar,
            nome_socio_razao_social varchar, cnpj_cpf_socio varchar,
            qualificacao_socio varchar, data_entrada_sociedade varchar,
            pais varchar, representante_legal varchar,
            nome_representante varchar, qualificacao_representante varchar,
            faixa_etaria varchar
        )
        """
    )
    connection.executemany(
        f"insert into {DATASET}.socios_raw values (?,?,?,?,?,?,?,?,?,?,?)",
        [
            # corporate partner
            ("11111111", "1", "HOLDING ALFA LTDA", "22222222000199",
             "22", "20180314", "", "", "", "", "0"),
            # natural person, masked CPF
            ("11111111", "2", "MARIA SOUZA", "***456789**",
             "49", "20190701", "", "", "", "", "5"),
            # foreign partner WITH a legal representative -- two edges, one row
            ("33333333", "3", "ALFA HOLDINGS BV", "",
             "37", "20200115", "NETHERLANDS", "***111222**", "JOAO LIMA", "10", "4"),
            # empty optionals: must land '' and a NULL date, never None strings
            ("44444444", "2", "ANA COSTA", "***999888**", "22", "", "", "", "", "", ""),
        ],
    )
    connection.close()


def test_relations_keep_every_partner_kind_verbatim(tmp_path: Path) -> None:
    """One edge model: the discriminator distinguishes company, person and
    foreign partners rather than three tables doing it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    counts = relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    assert counts["company_relations"] == 4
    rows = connection.execute(
        f"""
        select cnpj_basico, related_entity_kind, related_name, related_tax_id,
               relation_code, relation_since, related_country
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        order by cnpj_basico, related_entity_kind
        """
    ).fetchall()
    assert rows[0][:5] == (
        "11111111", "1", "HOLDING ALFA LTDA", "22222222000199", "22",
    )
    assert rows[0][5].isoformat() == "2018-03-14"
    assert rows[1][:5] == ("11111111", "2", "MARIA SOUZA", "***456789**", "49")
    assert rows[2][1] == "3"
    assert rows[2][6] == "NETHERLANDS"


def test_legal_representative_is_carried_as_a_second_edge(tmp_path: Path) -> None:
    """A foreign partner's representative is another named person. It rides on
    the same row because that is how RFB publishes it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    assert connection.execute(
        f"""
        select representative_tax_id, representative_name, representative_code
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        where cnpj_basico = '33333333'
        """
    ).fetchone() == ("***111222**", "JOAO LIMA", "10")


def test_absent_values_land_as_empty_string_not_null(tmp_path: Path) -> None:
    """Non-nullable ClickHouse Strings: the native driver calls .encode() per
    value and dies on None. Only real data with blanks triggers it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    # The snapshot build only produces BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    # (a subset of BR_COMPANY_RELATIONS_COLUMNS -- the rest are computed by the
    # history merge from columns this table doesn't have).
    string_columns = [
        column
        for column in tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
        if column != "relation_since"
    ]
    nulls = " + ".join(
        f"count(*) filter (where {column} is null)" for column in string_columns
    )
    assert connection.execute(
        f"select {nulls} from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}"
    ).fetchone() == (0,)
    assert connection.execute(
        f"""
        select relation_since, related_country
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        where cnpj_basico = '44444444'
        """
    ).fetchone() == (None, "")


def test_relation_since_key_is_the_source_entry_date_verbatim(tmp_path: Path) -> None:
    """RFB publishes no departures, but it DOES publish re-entries: a partner who
    rejoins carries a new data_entrada_sociedade. That makes a second spell
    detectable from one snapshot, so the entry date is part of a spell's
    identity -- as text, because ORDER BY cannot hold Nullable.
    """
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    rows = connection.execute(
        f"""
        select relation_since_key, relation_since
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        order by cnpj_basico, related_entity_kind
        """
    ).fetchall()
    assert rows[0][0] == "20180314"
    assert rows[0][1].isoformat() == "2018-03-14"
    # the row whose entry date is blank keeps '' -- never NULL, because the
    # column is part of a non-nullable sort key
    assert ("", None) in rows


def test_relations_refuse_an_empty_source(tmp_path: Path) -> None:
    socios_path = tmp_path / "socios.duckdb"
    connection = duckdb.connect(str(socios_path))
    connection.execute(f"create schema if not exists {DATASET}")
    connection.execute(
        f"create table {DATASET}.socios_raw (cnpj_basico varchar, "
        f"identificador_socio varchar, nome_socio_razao_social varchar, "
        f"cnpj_cpf_socio varchar, qualificacao_socio varchar, "
        f"data_entrada_sociedade varchar, pais varchar, representante_legal varchar, "
        f"nome_representante varchar, qualificacao_representante varchar, "
        f"faixa_etaria varchar)"
    )
    connection.close()

    with pytest.raises(ValueError, match="no company relations"):
        relations.build_brazil_rfb_company_relations(
            connection=duckdb.connect(":memory:"),
            source_run_id="run-1",
            snapshot_year_month="2026-07",
            socios_database_path=socios_path,
        )


def test_relations_never_emit_null_in_the_dedup_tiebreak_columns(
    tmp_path: Path,
) -> None:
    """history.py's argMax dedup is only equivalent to the row_number() form it
    replaced because these columns are never NULL.

    argMax SKIPS rows whose argument is NULL, in both engines. If the winning
    row held NULL in an argMax'd column, argMax would take the runner-up's
    value for that column and the winner's for the others -- a mixed row
    assembled from two source rows, written permanently, with nothing failing.

    This test is the pin for that invariant, which otherwise lives only in the
    coalescing inside build_brazil_rfb_company_relations one file away.
    """
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    tiebreak_columns = (
        "related_name",
        "related_country",
        "age_band",
        "representative_tax_id",
        "representative_name",
        "representative_code",
    )
    nulls = " + ".join(
        f"count(*) filter (where {column} is null)" for column in tiebreak_columns
    )
    assert connection.execute(
        f"select {nulls} from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}"
    ).fetchone() == (0,)

    # ...and relation_since must be constant within a SPELL_KEY group, since it
    # is the one NULL-capable column the dedup orders on. Both it and
    # relation_since_key derive from the same trimmed source value, so a group
    # with two distinct relation_since values would mean that has been broken.
    assert connection.execute(
        f"""
        select count(*) from (
            select cnpj_basico, related_entity_kind, related_tax_id,
                   relation_code, relation_since_key
            from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
            group by 1, 2, 3, 4, 5
            having count(distinct relation_since) > 1
        )
        """
    ).fetchone() == (0,)
