import csv
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.france_decp_procurement import tables
from dagster_v3.defs.france_decp_procurement.normalize import (
    build_contract_holder_candidates,
    expand_contract_holders,
    normalize_decp_identifier,
    replace_raw_table,
)


def test_decp_uses_the_current_bulk_csv_api() -> None:
    assert tables.SOURCE_URL == (
        "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
        "decp-2022-marches-valides/exports/csv"
    )
    assert tables.SOURCE_LICENCE == "Licence Ouverte v2.0 (Etalab)"


def test_french_holder_identifiers_resolve_to_siren() -> None:
    assert normalize_decp_identifier("530 796 176 00028", "SIRET") == "530796176"
    assert normalize_decp_identifier("530796176", "SIREN") == "530796176"
    assert normalize_decp_identifier("FR 12 530 796 176", "TVA") == "530796176"
    assert normalize_decp_identifier("CDL", "CDL") == ""


def test_each_published_holder_becomes_a_row_without_splitting_contract_value() -> None:
    rows = expand_contract_holders(
        {
            "id": "25-1073716-1",
            "nature": "Marché",
            "objet": "Prestations audiovisuelles",
            "codecpv": "92111260-2",
            "procedure": "Appel d'offres ouvert",
            "titulaire_id_1": "81251690400016",
            "titulaire_typeidentifiant_1": "SIRET",
            "titulaire_id_2": "85399314500012",
            "titulaire_typeidentifiant_2": "SIRET",
            "titulaire_id_3": "CDL",
            "titulaire_typeidentifiant_3": "CDL",
            "acheteur_id": "77574110100031",
            "dureemois": "48",
            "datenotification": "2025-01-15",
            "datepublicationdonnees": "2025-02-07",
            "montant": "700000.0",
            "montantmodification": "CDL",
            "montantactesoustraitance": "12500.50",
            "source": "DEMATIS",
        },
        source_run_id="run",
        source_object_key="raw/sha256=x/decp.csv",
        source_retrieved_at=datetime(2026, 7, 28, tzinfo=UTC),
        resolved_at=datetime(2026, 7, 28, tzinfo=UTC),
    )

    assert [row["holder_siren"] for row in rows] == ["812516904", "853993145"]
    assert [row["holder_ordinal"] for row in rows] == [1, 2]
    assert {row["contract_amount_eur"] for row in rows} == {"700000.0"}
    assert {row["contract_amount_attributable"] for row in rows} == {0}
    assert rows[0]["notification_date"] == date(2025, 1, 15)
    assert rows[0]["publication_date"] == date(2025, 2, 7)
    assert rows[0]["subcontract_amount_eur"] == "12500.50"


def test_unusable_holder_identifiers_remain_auditable() -> None:
    rows = expand_contract_holders(
        {
            "id": "contract-1",
            "titulaire_id_1": "foreign:123",
            "titulaire_typeidentifiant_1": "AUTRE",
            "titulaire_id_2": "CDL",
            "titulaire_typeidentifiant_2": "CDL",
            "titulaire_id_3": "CDL",
            "titulaire_typeidentifiant_3": "CDL",
        },
        source_run_id="run",
        source_object_key="raw/decp.csv",
        source_retrieved_at=datetime(2026, 7, 28, tzinfo=UTC),
        resolved_at=datetime(2026, 7, 28, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0]["holder_id_raw"] == "foreign:123"
    assert rows[0]["holder_siren"] == ""
    assert rows[0]["match_eligibility"] == "invalid_holder_identifier"


def test_contract_holder_candidates_keep_the_latest_source_version(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "decp.csv"
    source_rows = [
        _source_row(
            publication_date="2025-01-31",
            modification_publication_date="",
            title="Original publication",
            amount="100.00",
        ),
        _source_row(
            publication_date="2026-06-22",
            modification_publication_date="",
            title="Corrected publication",
            amount="200.00",
        ),
        _source_row(
            publication_date="2026-06-22",
            modification_publication_date="2026-07-01",
            title="Modified publication",
            amount="250.00",
        ),
        _source_row(
            publication_date="2026-06-22",
            modification_publication_date="2026-07-01",
            title="Final publication",
            amount="275.00",
        ),
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tables.EXPECTED_SOURCE_COLUMNS,
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(source_rows)

    connection = duckdb.connect()
    retrieved_at = datetime(2026, 7, 30, tzinfo=UTC)
    replace_raw_table(
        connection=connection,
        csv_path=csv_path,
        source_run_id="run",
        source_object_key="raw/decp.csv",
        source_retrieved_at=retrieved_at,
    )

    counts = build_contract_holder_candidates(
        connection=connection,
        source_run_id="run",
        resolved_at=retrieved_at,
    )

    assert counts == {
        "source_version_rows": 4,
        "candidate_rows": 1,
        "collapsed_version_rows": 3,
        "eligible_rows": 1,
        "contracts": 1,
    }
    candidate_columns = tuple(
        row[0]
        for row in connection.execute(
            f"DESCRIBE {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
        ).fetchall()
    )
    assert candidate_columns == tables.CANDIDATE_COLUMNS
    assert connection.execute(
        f"""
        SELECT title, contract_amount_eur
        FROM {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}
        """
    ).fetchone() == ("Final publication", Decimal("275.00"))


def _source_row(
    *,
    publication_date: str,
    modification_publication_date: str,
    title: str,
    amount: str,
) -> dict[str, str]:
    row = dict.fromkeys(tables.EXPECTED_SOURCE_COLUMNS, "")
    row.update(
        {
            "id": "2024U249731005",
            "acheteur_id": "13000222300027",
            "titulaire_id_1": "34813988200032",
            "titulaire_typeidentifiant_1": "SIRET",
            "titulaire_id_2": "CDL",
            "titulaire_id_3": "CDL",
            "datenotification": "2025-01-15",
            "datepublicationdonnees": publication_date,
            "datepublicationdonneesmodificationmodification": (
                modification_publication_date
            ),
            "objet": title,
            "montant": amount,
            "source": "DECP",
        }
    )
    return row
