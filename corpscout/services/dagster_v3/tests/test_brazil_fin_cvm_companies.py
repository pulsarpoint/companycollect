from datetime import UTC, datetime
import json

import duckdb


def test_load_brazil_fin_cvm_companies_csv_normalizes_issuer_rows(tmp_path) -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies

    csv_path = tmp_path / "cad_cia_aberta.csv"
    csv_path.write_bytes(
        "\n".join(
            [
                "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR",
                "08.773.135/0001-00;2W ECOBANK S.A.;2W ECOBANK;2020-10-29;2007-03-23;;;ATIVO;2020-10-29;25224;Energia Elétrica;;Categoria A;2020-10-29;OPERACIONAL;2020-10-29;PRIVADO;SEDE;Avenida Dr. Chucri Zaidan, 1550;8 andar;Chácara Santo Antônio;SÃO PAULO;SP;BRASIL;04711130;11;39579400;11;39579499;ri@example.com;DIRETOR DE RELAÇÕES COM INVESTIDORES;FERNANDO VIEIRA;2026-04-22;AV DR. CHUCRI ZAIDAN, 1550;8 ANDAR;CHÁCARA STO. ANTÔNIO;SÃO PAULO;SP;BRASIL;04711130;11;39579400;;;juridico@example.com;10.830.108/0001-65;GRANT THORNTON",
            ]
        ).encode("latin-1")
    )

    connection = duckdb.connect(":memory:")
    counts = companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=csv_path,
        source_url="https://example.test/cad_cia_aberta.csv",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert counts == {
        "company_row_count": 1,
        "active_company_row_count": 1,
        "distinct_cnpj_count": 1,
    }
    row = connection.execute(
        """
        select
            country_iso2,
            source_slug,
            source_record_id,
            cnpj,
            cnpj_basico,
            cvm_code,
            legal_name,
            trade_name,
            registration_date,
            industry_sector,
            registration_status,
            issuer_status,
            municipality,
            state,
            email,
            responsible_email,
            auditor_cnpj,
            auditor_name,
            source_url,
            source_file_name,
            source_row_number
        from brazil_cvm.companies
        """
    ).fetchone()

    assert row == (
        "BR",
        "brazil_cvm_companies",
        "brazil_cvm_companies|08773135000100|25224",
        "08773135000100",
        "08773135",
        "25224",
        "2W ECOBANK S.A.",
        "2W ECOBANK",
        datetime(2020, 10, 29).date(),
        "Energia Elétrica",
        "ATIVO",
        "OPERACIONAL",
        "SÃO PAULO",
        "SP",
        "ri@example.com",
        "juridico@example.com",
        "10830108000165",
        "GRANT THORNTON",
        "https://example.test/cad_cia_aberta.csv",
        "cad_cia_aberta.csv",
        1,
    )


def test_load_brazil_fin_cvm_companies_csv_normalizes_missing_auditor_cnpj_to_empty_string(
    tmp_path,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies

    csv_path = tmp_path / "cad_cia_aberta.csv"
    header = "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR"
    values = [""] * len(header.split(";"))
    values[0] = "08.773.135/0001-00"
    values[1] = "2W ECOBANK S.A."
    values[3] = "2020-10-29"
    values[7] = "ATIVO"
    values[9] = "25224"
    csv_path.write_text(
        f"{header}\n{';'.join(values)}",
        encoding="latin-1",
    )

    connection = duckdb.connect(":memory:")
    companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=csv_path,
        source_url="https://example.test/cad_cia_aberta.csv",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    row = connection.execute(
        "select auditor_cnpj, auditor_name from brazil_cvm.companies"
    ).fetchone()

    assert row == ("", "")


def test_load_brazil_fin_cvm_companies_csv_replaces_existing_table(tmp_path) -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies

    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    header = "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR"
    first_csv.write_text(
        f"{header}\n00.000.000/0001-00;OLD SA;;2020-01-01;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        encoding="latin-1",
    )
    second_csv.write_text(
        f"{header}\n11.111.111/0001-11;NEW SA;;2021-01-01;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        encoding="latin-1",
    )

    connection = duckdb.connect(":memory:")
    companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=first_csv,
        source_url="https://example.test/first.csv",
        source_run_id="run-1",
    )
    companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=second_csv,
        source_url="https://example.test/second.csv",
        source_run_id="run-2",
    )

    rows = connection.execute(
        "select cnpj, legal_name from brazil_cvm.companies order by cnpj"
    ).fetchall()

    assert rows == [("11111111000111", "NEW SA")]


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.written_objects: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_objects.append((bucket, key))
        self.objects[(bucket, key)] = body

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_objects.append((bucket, key))
        self.objects[(bucket, key)] = body.encode("utf-8")

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]


class FakeResponse:
    def __init__(self, body: bytes) -> None:
        self.content = body
        self.headers = {
            "Content-Type": "text/csv",
            "Last-Modified": "Sun, 05 Jul 2026 10:00:00 GMT",
        }

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.requested: list[tuple[str, int]] = []

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        self.requested.append((url, timeout))
        return FakeResponse(self.body)


def test_sync_brazil_fin_cvm_companies_csv_stores_content_addressed_raw_copy() -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies
    from dagster_v3.defs.brazil_financial.cvm.source import BRAZIL_CVM_RAW_BUCKET

    csv_body = b"CNPJ_CIA;DENOM_SOCIAL\n00.000.000/0001-00;EXAMPLE SA\n"
    object_store = FakeObjectStore()
    session = FakeSession(csv_body)

    result = companies.sync_brazil_fin_cvm_companies_csv(
        object_store=object_store,
        session=session,
        synced_at=datetime(2026, 7, 5, 12, 0, tzinfo=UTC),
    )

    assert session.requested == [(companies.CVM_COMPANIES_SOURCE_URL, 300)]
    assert object_store.created_buckets == [BRAZIL_CVM_RAW_BUCKET]
    assert result.csv_key.startswith("brazil_cvm/cad/raw_csv/sha256=")
    assert result.csv_key.endswith("/cad_cia_aberta.csv")
    assert result.metadata_key == "brazil_cvm/cad/raw_csv/latest/metadata.json"
    assert result.downloaded is True
    assert result.reused_existing_csv is False
    assert result.size_bytes == len(csv_body)
    assert result.sha256
    assert object_store.objects[(BRAZIL_CVM_RAW_BUCKET, result.csv_key)] == csv_body
    metadata = json.loads(
        object_store.objects[(BRAZIL_CVM_RAW_BUCKET, result.metadata_key)].decode()
    )
    assert metadata["csv_key"] == result.csv_key
    assert metadata["sha256"] == result.sha256
    assert metadata["source_url"] == companies.CVM_COMPANIES_SOURCE_URL
    assert metadata["source_last_modified"] == "Sun, 05 Jul 2026 10:00:00 GMT"


def test_load_brazil_fin_cvm_companies_from_object_store_uses_latest_raw_csv() -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies
    from dagster_v3.defs.brazil_financial.cvm.source import BRAZIL_CVM_RAW_BUCKET

    csv_body = "\n".join(
        [
            "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR",
            "08.773.135/0001-00;2W ECOBANK S.A.;2W ECOBANK;2020-10-29;2007-03-23;;;ATIVO;2020-10-29;25224;Energia Elétrica;;Categoria A;2020-10-29;OPERACIONAL;2020-10-29;PRIVADO;SEDE;Avenida Dr. Chucri Zaidan, 1550;8 andar;Chácara Santo Antônio;SÃO PAULO;SP;BRASIL;04711130;11;39579400;11;39579499;ri@example.com;DIRETOR DE RELAÇÕES COM INVESTIDORES;FERNANDO VIEIRA;2026-04-22;AV DR. CHUCRI ZAIDAN, 1550;8 ANDAR;CHÁCARA STO. ANTÔNIO;SÃO PAULO;SP;BRASIL;04711130;11;39579400;;;juridico@example.com;10.830.108/0001-65;GRANT THORNTON",
        ]
    ).encode("latin-1")
    object_store = FakeObjectStore()
    csv_key = "brazil_cvm/cad/raw_csv/sha256=test/cad_cia_aberta.csv"
    metadata_key = companies.CVM_COMPANIES_METADATA_OBJECT_KEY
    object_store.objects[(BRAZIL_CVM_RAW_BUCKET, csv_key)] = csv_body
    object_store.objects[(BRAZIL_CVM_RAW_BUCKET, metadata_key)] = json.dumps(
        {
            "source_url": "https://example.test/cad_cia_aberta.csv",
            "csv_key": csv_key,
            "source_file_name": "cad_cia_aberta.csv",
        }
    ).encode()

    connection = duckdb.connect(":memory:")
    counts = companies.load_brazil_fin_cvm_companies_from_object_store(
        connection=connection,
        object_store=object_store,
        source_run_id="run-raw",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert counts["company_row_count"] == 1
    row = connection.execute(
        "select legal_name, source_url from brazil_cvm.companies"
    ).fetchone()
    assert row == ("2W ECOBANK S.A.", "https://example.test/cad_cia_aberta.csv")
