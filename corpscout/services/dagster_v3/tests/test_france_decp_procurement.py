from datetime import UTC, date, datetime

from dagster_v3.defs.france_decp_procurement import tables
from dagster_v3.defs.france_decp_procurement.normalize import (
    expand_contract_holders,
    normalize_decp_identifier,
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
