import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA
from dagster_v3.defs.brazil_financial.cvm.source import (
    BRAZIL_CVM_RAW_BUCKET,
    fre_archive_object_key,
    normalize_fre_year,
)

FRE_DOCUMENTS_TABLE = "fre_documents"
FRE_CAPITAL_SOCIAL_TABLE = "fre_capital_social"
FRE_CAPITAL_SOCIAL_CLASSES_TABLE = "fre_capital_social_classes"
FRE_CAPITAL_DISTRIBUTION_TABLE = "fre_capital_distribution"
FRE_AUDITORS_TABLE = "fre_auditors"
FRE_RESPONSIBLES_TABLE = "fre_responsibles"
FRE_RELATED_PARTY_TRANSACTIONS_TABLE = "fre_related_party_transactions"
FRE_REMUNERATION_TOTAL_ORGANS_TABLE = "fre_remuneration_total_organs"
FRE_SHAREHOLDERS_TABLE = "fre_shareholders"
FRE_PARSE_RUNS_TABLE = "fre_parse_runs"

SOURCE_SLUG = "brazil_cvm_fre"
CSV_ENCODING = "latin-1"
CSV_FALLBACK_ENCODING = "utf-8"
CSV_WINDOWS_1252_ENCODING = "cp1252"

FRE_DOCUMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "fre_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "document_category",
    "document_id",
    "received_date",
    "document_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)

FRE_COMMON_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "fre_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "reference_date",
    "version",
    "document_id",
)

FRE_SOURCE_COLUMNS = (
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)

FRE_CAPITAL_SOCIAL_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "capital_social_id",
    "capital_type",
    "authorization_approval_date",
    "capital_amount",
    "payment_deadline",
    "ordinary_shares",
    "preferred_shares",
    "total_shares",
    *FRE_SOURCE_COLUMNS,
)

FRE_CAPITAL_SOCIAL_CLASSES_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "capital_social_id",
    "preferred_share_class_type",
    "share_count",
    *FRE_SOURCE_COLUMNS,
)

FRE_CAPITAL_DISTRIBUTION_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "individual_shareholder_count",
    "company_shareholder_count",
    "institutional_investor_count",
    "ordinary_shares_outstanding",
    "ordinary_shares_outstanding_percent",
    "preferred_shares_outstanding",
    "preferred_shares_outstanding_percent",
    "total_shares_outstanding",
    "total_shares_outstanding_percent",
    "last_assembly_date",
    *FRE_SOURCE_COLUMNS,
)

FRE_AUDITORS_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "auditor_id",
    "auditor_name",
    "auditor_cpf",
    "auditor_cnpj",
    "auditor_cvm_code",
    "auditor_origin_type",
    "contract_start_date",
    "contract_end_date",
    "service_start_date",
    "contracted_service",
    "auditor_remuneration",
    "substitution_reason",
    "presented_reason",
    *FRE_SOURCE_COLUMNS,
)

FRE_RESPONSIBLES_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "responsible_name",
    "responsible_role",
    *FRE_SOURCE_COLUMNS,
)

FRE_RELATED_PARTY_TRANSACTIONS_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "related_party",
    "person_type",
    "related_party_document",
    "issuer_relationship",
    "data_transaction",
    "contract_object",
    "transaction_amount",
    "existing_balance_original",
    "related_party_interest_amount_original",
    "insurance_guarantee",
    "transaction_duration",
    "loan_debt",
    "termination",
    "operation_nature_reason",
    "interest_rate",
    "issuer_contractual_position",
    "issuer_contractual_position_specification",
    *FRE_SOURCE_COLUMNS,
)

FRE_REMUNERATION_TOTAL_ORGANS_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "fiscal_year_start_date",
    "fiscal_year_end_date",
    "total_remuneration",
    "administration_body",
    "member_count",
    "body_total_remuneration",
    "paid_member_count",
    "salary",
    "direct_indirect_benefits",
    "committee_participation",
    "other_fixed_amounts",
    "other_fixed_remuneration_description",
    "bonus",
    "profit_sharing",
    "meeting_participation",
    "other_variable_amounts",
    "commissions",
    "other_variable_remuneration_description",
    "post_employment",
    "position_termination",
    "share_based",
    "observation",
    *FRE_SOURCE_COLUMNS,
)

FRE_SHAREHOLDERS_COLUMNS = (
    *FRE_COMMON_COLUMNS,
    "shareholder_id",
    "shareholder_name",
    "shareholder_person_type",
    "shareholder_document",
    "related_shareholder_id",
    "related_shareholder_name",
    "related_shareholder_person_type",
    "related_shareholder_document",
    "ordinary_shares_outstanding",
    "ordinary_shares_outstanding_percent",
    "preferred_shares_outstanding",
    "preferred_shares_outstanding_percent",
    "total_shares_outstanding",
    "total_shares_outstanding_percent",
    "nationality",
    "state",
    "foreign_resident",
    "legal_representative",
    "legal_representative_person_type",
    "legal_representative_document",
    "capital_composition_date",
    "last_change_date",
    "controlling_shareholder",
    "shareholder_agreement_participant",
    *FRE_SOURCE_COLUMNS,
)


@dataclass(frozen=True)
class _FreMember:
    csv_name: str
    table_name: str
    count_key: str
    loader: Callable[..., None]


def load_brazil_fin_cvm_fre_archive(
    *,
    connection: Any,
    archive_path: str | Path,
    year: str | int,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime | None = None,
) -> dict[str, int]:
    normalized_year = normalize_fre_year(year)
    resolved_at = resolved_at or datetime.now(UTC)

    with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_fre_csv_") as tmpdir:
        members = _extract_known_members(
            archive_path=Path(archive_path),
            year=normalized_year,
            target_dir=Path(tmpdir),
        )
        _ensure_tables(connection)
        _replace_year(connection=connection, year=normalized_year)
        for member, csv_path in members:
            member.loader(
                connection=connection,
                csv_path=csv_path,
                year=normalized_year,
                source_archive_key=source_archive_key,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )

    counts = _year_counts(connection=connection, year=normalized_year)
    _record_parse_run(
        connection=connection,
        year=normalized_year,
        source_archive_key=source_archive_key,
        source_run_id=source_run_id,
        resolved_at=resolved_at,
        counts=counts,
    )
    return counts


def parse_brazil_fin_cvm_fre_archive_from_object_store(
    *,
    connection: Any,
    object_store: Any,
    year: str | int,
    source_run_id: str,
    resolved_at: datetime | None = None,
) -> dict[str, int]:
    normalized_year = normalize_fre_year(year)
    archive_key = fre_archive_object_key(normalized_year)
    with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_fre_archive_") as tmpdir:
        archive_path = Path(tmpdir) / f"fre_cia_aberta_{normalized_year}.zip"
        object_store.download_file(
            archive_key,
            archive_path,
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return load_brazil_fin_cvm_fre_archive(
            connection=connection,
            archive_path=archive_path,
            year=normalized_year,
            source_archive_key=archive_key,
            source_run_id=source_run_id,
            resolved_at=resolved_at,
        )


def _fre_members_for_year(year: str) -> tuple[_FreMember, ...]:
    return (
        _FreMember(
            csv_name=f"fre_cia_aberta_{year}.csv",
            table_name=FRE_DOCUMENTS_TABLE,
            count_key="document_row_count",
            loader=_load_documents,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_capital_social_{year}.csv",
            table_name=FRE_CAPITAL_SOCIAL_TABLE,
            count_key="capital_social_row_count",
            loader=_load_capital_social,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_capital_social_classe_acao_{year}.csv",
            table_name=FRE_CAPITAL_SOCIAL_CLASSES_TABLE,
            count_key="capital_social_class_row_count",
            loader=_load_capital_social_classes,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_distribuicao_capital_{year}.csv",
            table_name=FRE_CAPITAL_DISTRIBUTION_TABLE,
            count_key="capital_distribution_row_count",
            loader=_load_capital_distribution,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_auditor_{year}.csv",
            table_name=FRE_AUDITORS_TABLE,
            count_key="auditor_row_count",
            loader=_load_auditors,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_responsavel_{year}.csv",
            table_name=FRE_RESPONSIBLES_TABLE,
            count_key="responsible_row_count",
            loader=_load_responsibles,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_transacao_parte_relacionada_{year}.csv",
            table_name=FRE_RELATED_PARTY_TRANSACTIONS_TABLE,
            count_key="related_party_transaction_row_count",
            loader=_load_related_party_transactions,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_remuneracao_total_orgao_{year}.csv",
            table_name=FRE_REMUNERATION_TOTAL_ORGANS_TABLE,
            count_key="remuneration_total_organ_row_count",
            loader=_load_remuneration_total_organs,
        ),
        _FreMember(
            csv_name=f"fre_cia_aberta_posicao_acionaria_{year}.csv",
            table_name=FRE_SHAREHOLDERS_TABLE,
            count_key="shareholder_row_count",
            loader=_load_shareholders,
        ),
    )


def _extract_known_members(
    *,
    archive_path: Path,
    year: str,
    target_dir: Path,
) -> tuple[tuple[_FreMember, Path], ...]:
    member_by_name = {member.csv_name: member for member in _fre_members_for_year(year)}
    extracted_members: list[tuple[_FreMember, Path]] = []
    with ZipFile(archive_path) as zip_file:
        for member_name in zip_file.namelist():
            if member_name.endswith("/"):
                continue
            if "/" in member_name:
                raise ValueError(
                    f"Unexpected Brazil CVM FRE nested member path: {member_name}"
                )
            if not member_name.endswith(".csv"):
                raise ValueError(
                    f"Unexpected Brazil CVM FRE archive member: {member_name}"
                )
            member = member_by_name.get(member_name)
            if member is None:
                continue
            target_path = target_dir / member_name
            target_path.write_bytes(zip_file.read(member_name))
            extracted_members.append((member, target_path))
    return tuple(extracted_members)


def _ensure_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_DOCUMENTS_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            fre_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            document_category varchar,
            document_id bigint,
            received_date date,
            document_url varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )
    _ensure_capital_social_table(connection)
    _ensure_capital_social_classes_table(connection)
    _ensure_capital_distribution_table(connection)
    _ensure_auditors_table(connection)
    _ensure_responsibles_table(connection)
    _ensure_related_party_transactions_table(connection)
    _ensure_remuneration_total_organs_table(connection)
    _ensure_shareholders_table(connection)
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_PARSE_RUNS_TABLE} (
            fre_year integer,
            source_archive_key varchar,
            source_run_id varchar,
            document_row_count integer,
            capital_social_row_count integer,
            capital_social_class_row_count integer,
            capital_distribution_row_count integer,
            auditor_row_count integer,
            responsible_row_count integer,
            related_party_transaction_row_count integer,
            remuneration_total_organ_row_count integer,
            shareholder_row_count integer,
            resolved_at timestamp
        )
        """
    )


def _ensure_capital_social_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_CAPITAL_SOCIAL_TABLE} (
            {_common_column_sql()}
            capital_social_id bigint,
            capital_type varchar,
            authorization_approval_date date,
            capital_amount decimal(38, 6),
            payment_deadline varchar,
            ordinary_shares bigint,
            preferred_shares bigint,
            total_shares bigint,
            {_source_column_sql()}
        )
        """
    )


def _ensure_capital_social_classes_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_CAPITAL_SOCIAL_CLASSES_TABLE} (
            {_common_column_sql()}
            capital_social_id bigint,
            preferred_share_class_type varchar,
            share_count bigint,
            {_source_column_sql()}
        )
        """
    )


def _ensure_capital_distribution_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_CAPITAL_DISTRIBUTION_TABLE} (
            {_common_column_sql()}
            individual_shareholder_count bigint,
            company_shareholder_count bigint,
            institutional_investor_count bigint,
            ordinary_shares_outstanding bigint,
            ordinary_shares_outstanding_percent decimal(18, 6),
            preferred_shares_outstanding bigint,
            preferred_shares_outstanding_percent decimal(18, 6),
            total_shares_outstanding bigint,
            total_shares_outstanding_percent decimal(18, 6),
            last_assembly_date date,
            {_source_column_sql()}
        )
        """
    )


def _ensure_auditors_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_AUDITORS_TABLE} (
            {_common_column_sql()}
            auditor_id bigint,
            auditor_name varchar,
            auditor_cpf varchar,
            auditor_cnpj varchar,
            auditor_cvm_code varchar,
            auditor_origin_type varchar,
            contract_start_date date,
            contract_end_date date,
            service_start_date date,
            contracted_service varchar,
            auditor_remuneration decimal(38, 6),
            substitution_reason varchar,
            presented_reason varchar,
            {_source_column_sql()}
        )
        """
    )


def _ensure_responsibles_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_RESPONSIBLES_TABLE} (
            {_common_column_sql()}
            responsible_name varchar,
            responsible_role varchar,
            {_source_column_sql()}
        )
        """
    )


def _ensure_related_party_transactions_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_RELATED_PARTY_TRANSACTIONS_TABLE} (
            {_common_column_sql()}
            related_party varchar,
            person_type varchar,
            related_party_document varchar,
            issuer_relationship varchar,
            data_transaction date,
            contract_object varchar,
            transaction_amount decimal(38, 6),
            existing_balance_original varchar,
            related_party_interest_amount_original varchar,
            insurance_guarantee varchar,
            transaction_duration varchar,
            loan_debt varchar,
            termination varchar,
            operation_nature_reason varchar,
            interest_rate varchar,
            issuer_contractual_position varchar,
            issuer_contractual_position_specification varchar,
            {_source_column_sql()}
        )
        """
    )


def _ensure_remuneration_total_organs_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_REMUNERATION_TOTAL_ORGANS_TABLE} (
            {_common_column_sql()}
            fiscal_year_start_date date,
            fiscal_year_end_date date,
            total_remuneration decimal(38, 6),
            administration_body varchar,
            member_count decimal(18, 6),
            body_total_remuneration decimal(38, 6),
            paid_member_count decimal(18, 6),
            salary decimal(38, 6),
            direct_indirect_benefits decimal(38, 6),
            committee_participation decimal(38, 6),
            other_fixed_amounts decimal(38, 6),
            other_fixed_remuneration_description varchar,
            bonus decimal(38, 6),
            profit_sharing decimal(38, 6),
            meeting_participation decimal(38, 6),
            other_variable_amounts decimal(38, 6),
            commissions decimal(38, 6),
            other_variable_remuneration_description varchar,
            post_employment decimal(38, 6),
            position_termination decimal(38, 6),
            share_based decimal(38, 6),
            observation varchar,
            {_source_column_sql()}
        )
        """
    )


def _ensure_shareholders_table(connection: Any) -> None:
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_SHAREHOLDERS_TABLE} (
            {_common_column_sql()}
            shareholder_id bigint,
            shareholder_name varchar,
            shareholder_person_type varchar,
            shareholder_document varchar,
            related_shareholder_id bigint,
            related_shareholder_name varchar,
            related_shareholder_person_type varchar,
            related_shareholder_document varchar,
            ordinary_shares_outstanding bigint,
            ordinary_shares_outstanding_percent decimal(18, 6),
            preferred_shares_outstanding bigint,
            preferred_shares_outstanding_percent decimal(18, 6),
            total_shares_outstanding bigint,
            total_shares_outstanding_percent decimal(18, 6),
            nationality varchar,
            state varchar,
            foreign_resident varchar,
            legal_representative varchar,
            legal_representative_person_type varchar,
            legal_representative_document varchar,
            capital_composition_date date,
            last_change_date date,
            controlling_shareholder varchar,
            shareholder_agreement_participant varchar,
            {_source_column_sql()}
        )
        """
    )


def _common_column_sql() -> str:
    return """
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            fre_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            reference_date date,
            version integer,
            document_id bigint,
    """


def _source_column_sql() -> str:
    return """
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
    """


def _replace_year(*, connection: Any, year: str) -> None:
    for table_name in (
        FRE_DOCUMENTS_TABLE,
        FRE_CAPITAL_SOCIAL_TABLE,
        FRE_CAPITAL_SOCIAL_CLASSES_TABLE,
        FRE_CAPITAL_DISTRIBUTION_TABLE,
        FRE_AUDITORS_TABLE,
        FRE_RESPONSIBLES_TABLE,
        FRE_RELATED_PARTY_TRANSACTIONS_TABLE,
        FRE_REMUNERATION_TOTAL_ORGANS_TABLE,
        FRE_SHAREHOLDERS_TABLE,
    ):
        connection.execute(
            f"delete from {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} where fre_year = ?",
            [int(year)],
        )


def _load_documents(
    *,
    connection: Any,
    csv_path: Path,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_DOCUMENTS_TABLE}
        ({", ".join(FRE_DOCUMENTS_COLUMNS)})
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_CIA, DT_REFER, VERSAO, ID_DOC),
            ?,
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(DENOM_CIA, ''),
            coalesce(CD_CVM, ''),
            try_cast(nullif(DT_REFER, '') as date),
            try_cast(nullif(VERSAO, '') as integer),
            coalesce(CATEG_DOC, ''),
            coalesce(try_cast(nullif(ID_DOC, '') as bigint), 0),
            try_cast(nullif(DT_RECEB, '') as date),
            coalesce(LINK_DOC, ''),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_fre_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            int(year),
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _load_capital_social(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_CAPITAL_SOCIAL_TABLE,
        columns=FRE_CAPITAL_SOCIAL_COLUMNS,
        extra_select_sql="""
            coalesce(try_cast(nullif(ID_Capital_Social, '') as bigint), 0),
            coalesce(Tipo_Capital, ''),
            try_cast(nullif(Data_Autorizacao_Aprovacao, '') as date),
            try_cast(nullif(Valor_Capital, '') as decimal(38, 6)),
            coalesce(Prazo_Integralizacao, ''),
            try_cast(nullif(Quantidade_Acoes_Ordinarias, '') as bigint),
            try_cast(nullif(Quantidade_Acoes_Preferenciais, '') as bigint),
            try_cast(nullif(Quantidade_Total_Acoes, '') as bigint)
        """,
        **kwargs,
    )


def _load_capital_social_classes(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_CAPITAL_SOCIAL_CLASSES_TABLE,
        columns=FRE_CAPITAL_SOCIAL_CLASSES_COLUMNS,
        extra_select_sql="""
            coalesce(try_cast(nullif(ID_Capital_Social, '') as bigint), 0),
            coalesce(Tipo_Classe_Acao_Preferencial, ''),
            try_cast(nullif(Quantidade_Acoes, '') as bigint)
        """,
        **kwargs,
    )


def _load_capital_distribution(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_CAPITAL_DISTRIBUTION_TABLE,
        columns=FRE_CAPITAL_DISTRIBUTION_COLUMNS,
        extra_select_sql="""
            try_cast(nullif(Quantidade_Acionistas_PF, '') as bigint),
            try_cast(nullif(Quantidade_Acionistas_PJ, '') as bigint),
            try_cast(nullif(Quantidade_Acionistas_Investidores_Institucionais, '') as bigint),
            try_cast(nullif(Quantidade_Acoes_Ordinarias_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Acoes_Ordinarias_Circulacao, '') as decimal(18, 6)),
            try_cast(nullif(Quantidade_Acoes_Preferenciais_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Acoes_Preferenciais_Circulacao, '') as decimal(18, 6)),
            try_cast(nullif(Quantidade_Total_Acoes_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Total_Acoes_Circulacao, '') as decimal(18, 6)),
            try_cast(nullif(Data_Ultima_Assembleia, '') as date)
        """,
        **kwargs,
    )


def _load_auditors(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_AUDITORS_TABLE,
        columns=FRE_AUDITORS_COLUMNS,
        extra_select_sql="""
            coalesce(try_cast(nullif(ID_Auditor, '') as bigint), 0),
            coalesce(Auditor, ''),
            coalesce(CPF_Auditor, ''),
            regexp_replace(coalesce(CNPJ_Auditor, ''), '[^0-9]', '', 'g'),
            coalesce(Codigo_CVM_Auditor, ''),
            coalesce(Tipo_Origem_Auditor, ''),
            try_cast(nullif(Data_Inicio_Contratacao, '') as date),
            try_cast(nullif(Data_Fim_Contratacao, '') as date),
            try_cast(nullif(Data_Inicio_Prestacao_Servico, '') as date),
            coalesce(Servico_Contratado, ''),
            try_cast(nullif(Remuneracao_Auditor, '') as decimal(38, 6)),
            coalesce(Justificativa_Substituicao, ''),
            coalesce(Razao_Apresentada, '')
        """,
        **kwargs,
    )


def _load_responsibles(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_RESPONSIBLES_TABLE,
        columns=FRE_RESPONSIBLES_COLUMNS,
        extra_select_sql="""
            coalesce(Nome_Responsavel, ''),
            coalesce(Cargo_Responsavel, '')
        """,
        **kwargs,
    )


def _load_related_party_transactions(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_RELATED_PARTY_TRANSACTIONS_TABLE,
        columns=FRE_RELATED_PARTY_TRANSACTIONS_COLUMNS,
        extra_select_sql="""
            coalesce(Parte_Relacionada, ''),
            coalesce(Tipo_Pessoa, ''),
            coalesce(Documento_Parte_Relacionada, ''),
            coalesce(Relacao_Emissor, ''),
            try_cast(nullif(Data_Transacao, '') as date),
            coalesce(Objeto_Contrato, ''),
            try_cast(nullif(Montante_Envolvido, '') as decimal(38, 6)),
            coalesce(Saldo_Existente, ''),
            coalesce(Montante_Interesse_Parte_Relacionada, ''),
            coalesce(Garantia_Seguro, ''),
            coalesce(Duracao_Transacao, ''),
            coalesce(Emprestimo_Divida, ''),
            coalesce(Rescisao, ''),
            coalesce(Natureza_Razao_Operacao, ''),
            coalesce(Taxa_Juros, ''),
            coalesce(Posicao_Contratual_Emissor, ''),
            coalesce(Especificacao_Posicao_Contratual_Emissor, '')
        """,
        **kwargs,
    )


def _load_remuneration_total_organs(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_REMUNERATION_TOTAL_ORGANS_TABLE,
        columns=FRE_REMUNERATION_TOTAL_ORGANS_COLUMNS,
        extra_select_sql="""
            try_cast(nullif(Data_Inicio_Exercicio_Social, '') as date),
            try_cast(nullif(Data_Fim_Exercicio_Social, '') as date),
            try_cast(nullif(Total_Remuneracao, '') as decimal(38, 6)),
            coalesce(Orgao_Administracao, ''),
            try_cast(nullif(Numero_Membros, '') as decimal(18, 6)),
            try_cast(nullif(Total_Remuneracao_Orgao, '') as decimal(38, 6)),
            try_cast(nullif(Numero_Membros_Remunerados, '') as decimal(18, 6)),
            try_cast(nullif(Salario, '') as decimal(38, 6)),
            try_cast(nullif(Beneficios_Diretos_Indiretos, '') as decimal(38, 6)),
            try_cast(nullif(Participacoes_Comites, '') as decimal(38, 6)),
            try_cast(nullif(Outros_Valores_Fixos, '') as decimal(38, 6)),
            coalesce(Descricao_Outros_Remuneracoes_Fixas, ''),
            try_cast(nullif(Bonus, '') as decimal(38, 6)),
            try_cast(nullif(Participacao_Resultados, '') as decimal(38, 6)),
            try_cast(nullif(Participacao_Reunioes, '') as decimal(38, 6)),
            try_cast(nullif(Outros_Valores_Variaveis, '') as decimal(38, 6)),
            try_cast(nullif(Comissoes, '') as decimal(38, 6)),
            coalesce(Descricao_Outros_Remuneracoes_Variaveis, ''),
            try_cast(nullif(Pos_emprego, '') as decimal(38, 6)),
            try_cast(nullif(Cessacao_Cargo, '') as decimal(38, 6)),
            try_cast(nullif(Baseada_Acoes, '') as decimal(38, 6)),
            coalesce(Observacao, '')
        """,
        **kwargs,
    )


def _load_shareholders(**kwargs: Any) -> None:
    _load_table(
        table_name=FRE_SHAREHOLDERS_TABLE,
        columns=FRE_SHAREHOLDERS_COLUMNS,
        extra_select_sql="""
            coalesce(try_cast(nullif(ID_Acionista, '') as bigint), 0),
            coalesce(Acionista, ''),
            coalesce(Tipo_Pessoa_Acionista, ''),
            coalesce(CPF_CNPJ_Acionista, ''),
            try_cast(nullif(ID_Acionista_Relacionado, '') as bigint),
            coalesce(Acionista_Relacionado, ''),
            coalesce(Tipo_Pessoa_Acionista_Relacionado, ''),
            coalesce(CPF_CNPJ_Acionista_Relacionado, ''),
            try_cast(nullif(Quantidade_Acao_Ordinaria_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Acao_Ordinaria_Circulacao, '') as decimal(18, 6)),
            try_cast(nullif(Quantidade_Acao_Preferencial_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Acao_Preferencial_Circulacao, '') as decimal(18, 6)),
            try_cast(nullif(Quantidade_Total_Acoes_Circulacao, '') as bigint),
            try_cast(nullif(Percentual_Total_Acoes_Circulacao, '') as decimal(18, 6)),
            coalesce(Nacionalidade, ''),
            coalesce(Sigla_UF, ''),
            coalesce(Residente_Exterior, ''),
            coalesce(Representante_Legal, ''),
            coalesce(Tipo_Pessoa_Representante_Legal, ''),
            coalesce(CPF_CNPJ_Representante_legal, ''),
            try_cast(nullif(Data_Composicao_Capital_Social, '') as date),
            try_cast(nullif(Data_Ultima_Alteracao, '') as date),
            coalesce(Acionista_Controlador, ''),
            coalesce(Participante_Acordo_Acionistas, '')
        """,
        **kwargs,
    )


def _load_table(
    *,
    connection: Any,
    csv_path: Path,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
    table_name: str,
    columns: tuple[str, ...],
    extra_select_sql: str,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name}
        ({", ".join(columns)})
        select
            {_common_select_sql()},
            {extra_select_sql},
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_fre_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            int(year),
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _common_select_sql() -> str:
    return """
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_Companhia, Data_Referencia, Versao, ID_Documento, source_row_number),
            ?,
            regexp_replace(CNPJ_Companhia, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_Companhia, '[^0-9]', '', 'g'), 1, 8),
            coalesce(Nome_Companhia, ''),
            try_cast(nullif(Data_Referencia, '') as date),
            try_cast(nullif(Versao, '') as integer),
            coalesce(try_cast(nullif(ID_Documento, '') as bigint), 0)
    """


def _read_member_to_temp_table(*, connection: Any, csv_path: Path) -> None:
    try:
        _read_member_to_temp_table_with_quote_sanitizer(
            connection=connection,
            csv_path=csv_path,
            encoding=CSV_ENCODING,
        )
    except Exception as exc:
        if "File is not latin-1 encoded" not in str(exc):
            raise
        fallback_path = _transcode_windows_1252_csv_to_utf8(csv_path)
        _read_member_to_temp_table_with_quote_sanitizer(
            connection=connection,
            csv_path=fallback_path,
            encoding=CSV_FALLBACK_ENCODING,
        )


def _read_member_to_temp_table_with_quote_sanitizer(
    *,
    connection: Any,
    csv_path: Path,
    encoding: str,
) -> None:
    try:
        _read_member_to_temp_table_with_encoding(
            connection=connection,
            csv_path=csv_path,
            encoding=encoding,
        )
        if _fre_member_temp_table_has_identifier_column(connection):
            return
    except Exception as exc:
        if not _is_malformed_quote_csv_error(exc):
            raise

    sanitized_path = _sanitize_malformed_literal_quote_fields(
        csv_path,
        encoding=encoding,
    )
    _read_member_to_temp_table_with_encoding(
        connection=connection,
        csv_path=sanitized_path,
        encoding=encoding,
    )


def _read_member_to_temp_table_with_encoding(
    *,
    connection: Any,
    csv_path: Path,
    encoding: str,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table _brazil_fin_cvm_fre_member as
        select
            row_number() over ()::bigint as source_row_number,
            *
        from read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true,
            encoding='{encoding}',
            ignore_errors=false
        )
        """,
        [str(csv_path)],
    )


def _fre_member_temp_table_has_identifier_column(connection: Any) -> bool:
    column_names = {
        row[0]
        for row in connection.execute("describe _brazil_fin_cvm_fre_member").fetchall()
    }
    return "CNPJ_CIA" in column_names or "CNPJ_Companhia" in column_names


def _transcode_windows_1252_csv_to_utf8(csv_path: Path) -> Path:
    fallback_path = csv_path.with_suffix(f"{csv_path.suffix}.utf8")
    fallback_path.write_text(
        csv_path.read_bytes().decode(CSV_WINDOWS_1252_ENCODING),
        encoding=CSV_FALLBACK_ENCODING,
    )
    return fallback_path


def _is_malformed_quote_csv_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "unterminated quote" in message
        or "not possible to automatically detect the CSV parsing dialect" in message
    )


def _sanitize_malformed_literal_quote_fields(csv_path: Path, *, encoding: str) -> Path:
    sanitized_path = csv_path.with_suffix(f"{csv_path.suffix}.sanitized")
    sanitized_path.write_text(
        "\n".join(
            _sanitize_malformed_literal_quote_line(line)
            for line in csv_path.read_text(encoding=encoding).splitlines()
        )
        + "\n",
        encoding=encoding,
    )
    return sanitized_path


def _sanitize_malformed_literal_quote_line(line: str) -> str:
    fields = _split_semicolon_csv_line_preserving_quotes(line)
    return ";".join(_sanitize_malformed_literal_quote_field(field) for field in fields)


def _split_semicolon_csv_line_preserving_quotes(line: str) -> list[str]:
    fields: list[str] = []
    index = 0
    while index <= len(line):
        if index == len(line):
            fields.append("")
            break
        if line[index] == ";":
            fields.append("")
            index += 1
            continue
        field_end = _find_semicolon_csv_field_end(line, index)
        fields.append(line[index:field_end])
        index = field_end + 1
    if line and line[-1] != ";":
        return fields[:-1] if fields and fields[-1] == "" else fields
    return fields


def _find_semicolon_csv_field_end(line: str, field_start: int) -> int:
    if line[field_start] != '"':
        delimiter_index = line.find(";", field_start)
        return len(line) if delimiter_index == -1 else delimiter_index

    quote_index = field_start + 1
    while quote_index < len(line):
        if line[quote_index] != '"':
            quote_index += 1
            continue
        next_index = quote_index + 1
        if next_index < len(line) and line[next_index] == '"':
            quote_index += 2
            continue
        if next_index == len(line) or line[next_index] == ";":
            return next_index
        delimiter_index = line.find(";", field_start)
        return len(line) if delimiter_index == -1 else delimiter_index
    delimiter_index = line.find(";", field_start)
    return len(line) if delimiter_index == -1 else delimiter_index


def _sanitize_malformed_literal_quote_field(field: str) -> str:
    if not field.startswith('"'):
        return field
    closing_quote_index = field.find('"', 1)
    if closing_quote_index == len(field) - 1:
        return field
    return '"' + field.replace('"', '""') + '"'


def _year_counts(*, connection: Any, year: str) -> dict[str, int]:
    return {
        member.count_key: _count_year(connection, member.table_name, year)
        for member in _fre_members_for_year(year)
    }


def _count_year(connection: Any, table_name: str, year: str) -> int:
    return int(
        connection.execute(
            f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} where fre_year = ?",
            [int(year)],
        ).fetchone()[0]
    )


def _record_parse_run(
    *,
    connection: Any,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
    counts: dict[str, int],
) -> None:
    connection.execute(
        f"delete from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_PARSE_RUNS_TABLE} where fre_year = ?",
        [int(year)],
    )
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_PARSE_RUNS_TABLE}
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            int(year),
            source_archive_key,
            source_run_id,
            counts["document_row_count"],
            counts["capital_social_row_count"],
            counts["capital_social_class_row_count"],
            counts["capital_distribution_row_count"],
            counts["auditor_row_count"],
            counts["responsible_row_count"],
            counts["related_party_transaction_row_count"],
            counts["remuneration_total_organ_row_count"],
            counts["shareholder_row_count"],
            resolved_at,
        ],
    )
