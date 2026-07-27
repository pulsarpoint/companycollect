COUNTRY_CODE = "BR"
SOURCE_SLUG = "brazil_pncp_procurement"
GROUP_NAME = "brazil_pncp"

# The consultation API. There is no bulk download -- confirmed against
# portaldatransparencia.gov.br and dadosabertos.compras.gov.br, and publicly
# criticised by Transparencia Brasil -- so paging is the only route.
API_BASE_URL = "https://pncp.gov.br/api/consulta/v1"
CONTRACTS_BY_PUBLICATION_PATH = "/contratos"
CONTRACTS_BY_UPDATE_PATH = "/contratos/atualizacao"
CATALOG_URL = "https://www.gov.br/pncp/pt-br/acesso-a-informacao/dados-abertos"

# Measured 2026-07-26: 1000 is rejected with "Tamanho de pagina invalido".
MAX_PAGE_SIZE = 500

# A contract has an address of its own, unlike Sweden's bulk-CSV register.
# Both forms verified HTTP 200; the human-facing one is what gets stored.
CONTRACT_URL_TEMPLATE = "https://pncp.gov.br/app/contratos/{buyer_cnpj}/{year}/{sequential}"

S3_BUCKET = "source-brazil-pncp"
S3_RAW_PREFIX = "raw"
# The daily window's pages. Kept separate from the monthly snapshot rather than
# merged into it: the monthly prefix is contiguous pages 1..N of one month, and
# the resume logic depends on that being true. A rolling window's pages are a
# different slicing of the same register and would break the contiguity the
# backfill relies on.
#
# They are the durable raw record for everything published since the backfill
# ran, so they are kept, not cleaned up.
S3_DAILY_PREFIX = "daily"

# The daily job reads both endpoints. Measured over 2026-07-14..20: publication
# 33,205 records over 67 pages, update 72,560 over 146 -- so ~213 pages a run.
DAILY_WINDOW_DAYS = 7

DUCKDB_FILE_NAME = "brazil_pncp_source.duckdb"
DUCKDB_SCHEMA = "brazil_pncp"
DUCKDB_POOL = "brazil_pncp_duckdb"
RAW_TABLE = "raw_contracts"
CANDIDATES_TABLE = "contract_candidates"

CLICKHOUSE_DATABASE = "corpscout"
CONTRACTS_TABLE = "br_pncp_contracts"
QUALIFIED_CONTRACTS_TABLE = f"{CLICKHOUSE_DATABASE}.{CONTRACTS_TABLE}"
CONTRACTS_VIEW = "br_government_contracts"

# Every field the endpoint returns. Kept whole rather than curated: the register
# publishes what it publishes, and choosing a subset at ingest is the loss this
# whole design exists to avoid.
API_FIELDS = (
    "numeroControlePNCP",
    "numeroControlePncpCompra",
    "numeroControlePncpAta",
    "anoContrato",
    "sequencialContrato",
    "numeroContratoEmpenho",
    "numeroRetificacao",
    "processo",
    "tipoContrato",
    "categoriaProcesso",
    "objetoContrato",
    "informacaoComplementar",
    "dataPublicacaoPncp",
    "dataAssinatura",
    "dataVigenciaInicio",
    "dataVigenciaFim",
    "dataAtualizacao",
    "dataAtualizacaoGlobal",
    "niFornecedor",
    "tipoPessoa",
    "nomeRazaoSocialFornecedor",
    "codigoPaisFornecedor",
    "niFornecedorSubContratado",
    "tipoPessoaSubContratada",
    "nomeFornecedorSubContratado",
    "orgaoEntidade",
    "orgaoSubRogado",
    "unidadeOrgao",
    "unidadeSubRogada",
    "valorInicial",
    "valorParcela",
    "valorGlobal",
    "valorAcumulado",
    "numeroParcelas",
    "receita",
    "emendaParlamentar",
    "frutoAdesao",
    "temRemanejamento",
    "identificadorCipi",
    "urlCipi",
    "usuarioNome",
)

CANDIDATE_COLUMNS = (
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_url",
    "numero_controle_pncp",
    "numero_controle_pncp_compra",
    "ano_contrato",
    "sequencial_contrato",
    "numero_contrato_empenho",
    "numero_retificacao",
    "processo",
    "tipo_contrato",
    "categoria_processo",
    "objeto_contrato",
    "informacao_complementar",
    "data_publicacao_pncp",
    "data_assinatura",
    "data_vigencia_inicio",
    "data_vigencia_fim",
    "data_atualizacao_global",
    "supplier_cnpj",
    "supplier_cnpj_basico",
    "supplier_name",
    "supplier_person_type",
    "supplier_country_code",
    "subcontractor_cnpj",
    "subcontractor_name",
    "subcontractor_person_type",
    "buyer_cnpj",
    "buyer_name",
    "buyer_power_id",
    "buyer_sphere_id",
    "buyer_unit_code",
    "buyer_unit_name",
    "buyer_state_code",
    "buyer_municipality",
    # All five value fields are kept. The API documents none of them, so which
    # one is "the" contract value is decided in the view, not here.
    "valor_inicial",
    "valor_parcela",
    "valor_global",
    "valor_acumulado",
    "numero_parcelas",
    "is_revenue_contract",
    "parliamentary_amendment",
    "from_adhesion",
    "has_reallocation",
    "match_eligibility",
    "source_retrieved_at",
    "resolved_at",
)

# Every native value gets a USD counterpart. Converting only the one the view
# reads would put the other three beyond reach of any cross-country comparison,
# which is the same loss as not storing them -- deferred one layer. Which figure
# is *shown* is decided in the view and the UI, where it can be labelled.
USD_VALUE_COLUMNS = (
    "valor_global_usd",
    "valor_inicial_usd",
    "valor_parcela_usd",
    "valor_acumulado_usd",
)

# One rate per contract covers all four figures: same contract, same date.
FX_PROVENANCE_COLUMNS = ("fx_rate_to_usd", "fx_rate_date", "fx_source")

# The export adds the resolved company and its FX conversion, and drops nothing:
# there is no raw payload column to exclude because the raw JSON stays in DuckDB.
CONTRACTS_COLUMNS = (
    "company_id",
    "company_match_status",
    *CANDIDATE_COLUMNS,
    *USD_VALUE_COLUMNS,
    *FX_PROVENANCE_COLUMNS,
)

# tipoPessoa: PJ is a legal entity, PF a natural person. They differ by one
# character, so matching is exact -- the same trap as Sweden's
# "Inte direktivstyrd" containing "direktivstyrd".
PERSON_TYPE_LEGAL_ENTITY = "PJ"
PERSON_TYPE_NATURAL_PERSON = "PF"
