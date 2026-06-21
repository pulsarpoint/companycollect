from datetime import date

from dagster_v3.defs.estonia_ar import resources

# Real register header (lihtandmed); `;`-delimited. Row 3 has a blank reg code and
# must be skipped. Row 2 is in liquidation with a VAT number.
HEADER = (
    "nimi;ariregistri_kood;ettevotja_oiguslik_vorm;ettevotja_oigusliku_vormi_alaliik;"
    "kmkr_nr;ettevotja_staatus;ettevotja_staatus_tekstina;ettevotja_esmakande_kpv;"
    "ettevotja_aadress;asukoht_ettevotja_aadressis;asukoha_ehak_kood;"
    "asukoha_ehak_tekstina;indeks_ettevotja_aadressis;ads_adr_id;ads_ads_oid;"
    "ads_normaliseeritud_taisaadress;teabesysteemi_link"
)
SAMPLE_CSV = "\n".join(
    [
        HEADER,
        "007 Agent OÜ;16752073;Osaühing;;;R;Registrisse kantud;05.06.2023;;"
        "Regati pst 12;0596;Pirita linnaosa, Tallinn;11911;2363082;;"
        "Harju maakond, Tallinn, Regati pst 12;https://ariregister.rik.ee/est/company/16752073",
        "Näidis AS;10000018;Aktsiaselts;;EE100247414;L;Likvideerimisel;14.10.2020;;"
        "Tartu mnt 1;0784;Tallinn;10115;123;;"
        "Harju maakond, Tallinn, Tartu mnt 1;https://ariregister.rik.ee/est/company/10000018",
        "No Code OÜ;;Osaühing;;;R;Registrisse kantud;01.01.2020;;;;;;;;;",
    ]
)


def _rows() -> list[dict]:
    return list(resources.iter_estonia_ar_entity_rows_from_text(SAMPLE_CSV, run_id="run-1"))


def test_blank_reg_code_rows_are_skipped():
    rows = _rows()
    assert len(rows) == 2
    assert [r["reg_code"] for r in rows] == ["16752073", "10000018"]


def test_active_company_row_mapped_with_english_and_parsed_date():
    row = _rows()[0]
    assert row["country_iso2"] == "EE"
    assert row["source_slug"] == "estonia_ar_register"
    assert row["source_record_id"] == "16752073"
    assert row["name"] == "007 Agent OÜ"
    assert row["vat_id"] == ""
    assert row["legal_form_original"] == "Osaühing"
    assert row["legal_form_en"] == "Private limited company"
    assert row["status_code"] == "R"
    assert row["status_original"] == "Registrisse kantud"
    assert row["status_en"] == "Registered"
    assert row["is_active"] is True
    assert row["first_entry_date"] == date(2023, 6, 5)
    assert row["address"] == "Harju maakond, Tallinn, Regati pst 12"
    assert row["location"] == "Pirita linnaosa, Tallinn"
    assert row["ehak_code"] == "0596"
    assert row["postal_code"] == "11911"
    assert row["address_id"] == "2363082"
    assert row["company_url"] == "https://ariregister.rik.ee/est/company/16752073"
    assert len(row["source_payload_hash"]) == 64


def test_liquidation_company_row_is_inactive_with_vat():
    row = _rows()[1]
    assert row["reg_code"] == "10000018"
    assert row["vat_id"] == "EE100247414"
    assert row["legal_form_en"] == "Public limited company"
    assert row["status_code"] == "L"
    assert row["status_en"] == "In liquidation"
    assert row["is_active"] is False
    assert row["first_entry_date"] == date(2020, 10, 14)


def test_unknown_legal_form_and_bad_date_default_safely():
    csv_text = "\n".join(
        [
            HEADER,
            "Weird X;99999999;Tundmatu vorm;;;X;Mingi staatus;not-a-date;;;;;;;;;",
        ]
    )
    row = list(
        resources.iter_estonia_ar_entity_rows_from_text(csv_text, run_id="r")
    )[0]
    assert row["legal_form_en"] == ""  # unknown form -> empty EN
    assert row["status_en"] == ""  # unknown status code -> empty EN
    assert row["is_active"] is False
    assert row["first_entry_date"] is None  # unparseable date -> None
