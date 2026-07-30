/**
 * Brazil (PNCP) contract presentation.
 *
 * Built per country on purpose. Measured across the six ingested procurement
 * registers, the number of domain columns shared by all of them is ZERO, and
 * br_pncp_contracts overlaps every other register by 0-2.9% — Finland's Hilma
 * by literally nothing. So there is no common contract shape to render
 * generically: PNCP publishes processo, empenho, parcelas, adhesion and
 * budget-amendment flags; TED publishes CPV, lots and framework ceilings.
 * A generic renderer can only print `column_name: value`, which is how this
 * page came to show `{"id":1,"nome":"Contrato (termo inicial)"}` to a reader.
 *
 * Labels pair an English term with PNCP's own API field name. The source field
 * name is used rather than a hand-written Portuguese label because it is what
 * we actually read — verifiable against the API response, and the same argument
 * `value_source_field` already makes for monetary figures. A Portuguese label we
 * invent is our text, not the register's, and can drift from it silently.
 *
 * Every decode falls back to the raw value for an unrecognised code. A new PNCP
 * domain value must show the source's own word rather than a guess.
 */

export type BrField = {
  /** br_pncp_contracts column. */
  key: string;
  /** English label, shown first — this is what a reader is expected to use. */
  en: string;
  /** PNCP's API field name, kept beside it so the value stays traceable. */
  source: string;
};

export type BrSection = { title: string; fields: BrField[] };

function text(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s === "" ? null : s;
}

/**
 * PNCP nests its domain values as `{"id":n,"nome":"..."}` and the ingest stores
 * that JSON verbatim, so every read has to unwrap it. Splitting id and name into
 * their own columns is an ingest change (116,226 rows) tracked separately; until
 * then the name is parsed here rather than leaking the blob onto the page.
 */
export function brJsonName(raw: unknown): string | null {
  const value = text(raw);
  if (value == null) return null;
  try {
    const parsed = JSON.parse(value);
    const name = parsed?.nome;
    return typeof name === "string" && name.trim() !== "" ? name.trim() : value;
  } catch {
    return value;
  }
}

/** English for a Portuguese domain name, keeping the source term in parentheses. */
function translated(portuguese: string | null, table: Record<string, string>): string | null {
  if (portuguese == null) return null;
  const english = table[portuguese];
  return english ? `${english} (${portuguese})` : portuguese;
}

// tipoContrato, all 8 values present in the corpus. An "empenho" is a budgetary
// commitment note rather than a contract document, and it is the single most
// common type (50,892 of 116,226) — calling it a contract would misdescribe
// nearly half the corpus.
const BR_CONTRACT_TYPES: Record<string, string> = {
  Empenho: "Commitment note",
  "Contrato (termo inicial)": "Contract, initial term",
  Outros: "Other",
  "Termo de Adesão": "Adhesion agreement",
  "Carta Contrato": "Contract letter",
  Concessão: "Concession",
  Comodato: "Gratuitous loan of goods",
  Arrendamento: "Lease",
};

// categoriaProcesso, all 11 values present in the corpus.
const BR_PROCESS_CATEGORIES: Record<string, string> = {
  Compras: "Goods purchase",
  Serviços: "Services",
  "Serviços de Saúde": "Health services",
  Cessão: "Assignment of use",
  "Serviços de Engenharia": "Engineering services",
  Obras: "Public works",
  "Informática (TIC)": "IT and communications",
  "Locação Imóveis": "Property lease",
  "Mão de Obra": "Labour supply",
  "Alienação de bens móveis/imóveis": "Disposal of movable or immovable assets",
  Internacional: "International",
};

export function brContractType(raw: unknown): string | null {
  return translated(brJsonName(raw), BR_CONTRACT_TYPES);
}

export function brProcessCategory(raw: unknown): string | null {
  return translated(brJsonName(raw), BR_PROCESS_CATEGORIES);
}

// esferaId — the LEVEL of government. Verified against buyer names across all
// 116,226 rows: M is MUNICIPIO/FUNDO MUNICIPAL, F is MINISTERIO/UNIVERSIDADE
// FEDERAL, E is SECRETARIA DE ESTADO, and all 160 D rows carry state code DF.
// N is not missing data: it is inter-municipal health consortia, bodies funded
// by several municipalities that sit at no single level.
const BR_SPHERES: Record<string, string> = {
  M: "Municipal level",
  F: "Federal level",
  E: "State level",
  D: "Federal District",
  N: "Not applicable",
};

// poderId — the BRANCH of government, a different code set that happens to
// share the letter E with esferaId. One sampled contract carries sphere=E and
// power=E meaning "state level, executive branch"; rendering the raw letters
// would print "E" twice and say nothing.
const BR_POWERS: Record<string, string> = {
  E: "Executive branch",
  L: "Legislative branch",
  J: "Judiciary",
  // 56,046 rows (48%), overwhelmingly MUNICIPIO entries that are plainly
  // executive but never declared a branch. "Not stated" rather than "Not
  // applicable": the branch exists, the publisher omitted it. This is also why
  // power is unsafe to aggregate on while sphere (1.4% N) is fine.
  N: "Not stated",
};

const BR_PERSON_TYPES: Record<string, string> = {
  PJ: "Company",
  PF: "Individual",
  PE: "Foreign entity",
};

// ISO 3166-1 alpha-3, covering every value present in the corpus. Intl
// .DisplayNames only resolves alpha-2, so the mapping is explicit; an unlisted
// code renders raw rather than being dropped or guessed at.
const BR_COUNTRY_NAMES: Record<string, string> = {
  BRA: "Brazil",
  USA: "United States",
  DEU: "Germany",
  GBR: "United Kingdom",
  BEL: "Belgium",
  CHE: "Switzerland",
  NLD: "Netherlands",
  CAN: "Canada",
  URY: "Uruguay",
  DNK: "Denmark",
  PRT: "Portugal",
  CHN: "China",
  KOR: "South Korea",
  MEX: "Mexico",
  FRA: "France",
  ESP: "Spain",
  ARE: "United Arab Emirates",
  IND: "India",
  MYS: "Malaysia",
  JPN: "Japan",
  ISR: "Israel",
  AUT: "Austria",
  IRL: "Ireland",
  ARG: "Argentina",
  ITA: "Italy",
};

function decoded(value: unknown, table: Record<string, string>): string | null {
  const code = text(value);
  if (code == null) return null;
  const label = table[code];
  return label ? `${label} (${code})` : code;
}

export function brSphere(value: unknown): string | null {
  return decoded(value, BR_SPHERES);
}

export function brPower(value: unknown): string | null {
  return decoded(value, BR_POWERS);
}

export function brPersonType(value: unknown): string | null {
  return decoded(value, BR_PERSON_TYPES);
}

/**
 * Blank is left blank. 73,294 rows (63%) carry no country while 42,684 say BRA
 * explicitly, so blank means the publisher did not state it — not "Brazil".
 * Filling it in would fabricate the one field a reader uses to spot the 145 US,
 * 26 German and 18 UK suppliers in the corpus.
 */
export function brSupplierCountry(value: unknown): string | null {
  return decoded(value, BR_COUNTRY_NAMES);
}

/** 109,204 of 116,226 contracts are numeroRetificacao 0, where "0" says nothing. */
export function brAmendment(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n === 0 ? "Original — no amendment" : `Amendment ${n}`;
}

/** `receita`: whether the contract brings money in rather than pays it out. */
export function brRevenueOrExpenditure(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  return Number(value) === 1 ? "Revenue" : "Expenditure";
}

/**
 * Nullable(UInt8) source flags are three-state, and NULL is by far the common
 * case: parliamentary_amendment is NULL on 116,121 of 116,226 rows. Returning
 * null hides the row entirely, because rendering NULL as "No" would put a claim
 * on the page that PNCP never made.
 */
export function brTriStateFlag(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  return Number(value) === 1 ? "Yes" : "No";
}

/**
 * The contract as PNCP's own document is organised, not as our columns are
 * ordered. Monetary columns are handled separately (each needs BRL and USD
 * side by side), as are the three near-empty budget flags, which are rendered
 * only when a contract asserts them.
 */
export const BR_CONTRACT_SECTIONS: BrSection[] = [
  {
    title: "Contract",
    fields: [
      { key: "tipo_contrato", en: "Instrument type", source: "tipoContrato.nome" },
      { key: "categoria_processo", en: "Category", source: "categoriaProcesso.nome" },
      { key: "is_revenue_contract", en: "Direction", source: "receita" },
      { key: "numero_contrato_empenho", en: "Contract or commitment no.", source: "numeroContratoEmpenho" },
      { key: "processo", en: "Administrative process no.", source: "processo" },
      { key: "numero_retificacao", en: "Amendment status", source: "numeroRetificacao" },
      { key: "informacao_complementar", en: "Additional information", source: "informacaoComplementar" },
    ],
  },
  {
    title: "Dates",
    fields: [
      { key: "data_assinatura", en: "Signed", source: "dataAssinatura" },
      { key: "data_vigencia_inicio", en: "In force from", source: "dataVigenciaInicio" },
      { key: "data_vigencia_fim", en: "In force until", source: "dataVigenciaFim" },
      { key: "data_publicacao_pncp", en: "Published on PNCP", source: "dataPublicacaoPncp" },
      { key: "data_atualizacao_global", en: "Last updated at source", source: "dataAtualizacaoGlobal" },
    ],
  },
  {
    title: "Buyer",
    fields: [
      { key: "buyer_name", en: "Buying body", source: "orgaoEntidade.razaoSocial" },
      { key: "buyer_cnpj", en: "Buyer CNPJ", source: "orgaoEntidade.cnpj" },
      { key: "buyer_sphere_id", en: "Level of government", source: "orgaoEntidade.esferaId" },
      { key: "buyer_power_id", en: "Branch of government", source: "orgaoEntidade.poderId" },
      { key: "buyer_unit_name", en: "Purchasing unit", source: "unidadeOrgao.nomeUnidade" },
      { key: "buyer_unit_code", en: "Purchasing unit code", source: "unidadeOrgao.codigoUnidade" },
      { key: "buyer_municipality", en: "Municipality", source: "unidadeOrgao.municipioNome" },
      {
        key: "buyer_municipality_ibge_code",
        en: "Municipality IBGE code",
        source: "unidadeOrgao.codigoIbge",
      },
      { key: "buyer_state_code", en: "State", source: "unidadeOrgao.ufSigla" },
    ],
  },
  {
    title: "Supplier",
    fields: [
      { key: "supplier_name", en: "Supplier", source: "nomeRazaoSocialFornecedor" },
      { key: "supplier_cnpj", en: "Supplier CNPJ", source: "niFornecedor" },
      { key: "supplier_person_type", en: "Supplier entity type", source: "tipoPessoa" },
      { key: "supplier_country_code", en: "Supplier country", source: "codigoPaisFornecedor" },
      { key: "subcontractor_name", en: "Subcontractor", source: "nomeFornecedorSubContratado" },
      { key: "subcontractor_cnpj", en: "Subcontractor CNPJ", source: "niFornecedorSubContratado" },
      { key: "subcontractor_person_type", en: "Subcontractor entity type", source: "tipoPessoaSubContratada" },
    ],
  },
  {
    title: "Identifiers",
    fields: [
      { key: "numero_controle_pncp", en: "PNCP control number", source: "numeroControlePNCP" },
      { key: "numero_controle_pncp_compra", en: "Originating purchase", source: "numeroControlePncpCompra" },
      { key: "ano_contrato", en: "Contract year", source: "anoContrato" },
      { key: "sequencial_contrato", en: "Sequence within the year", source: "sequencialContrato" },
    ],
  },
];

/**
 * `objeto_contrato` is the one string that says what the money bought, and PNCP
 * publishes it only in Portuguese. `objeto_contrato_en` comes from
 * br_pncp_contracts_translated (machine translation via text_translations) and
 * is absent entirely when the page reads the untranslated base table.
 *
 * English leads, per the page's rule, but the Portuguese is kept beneath it
 * rather than replaced: the translation is machine-made and the original is what
 * the register actually published. When the two are identical -- acronyms,
 * proper nouns -- the original is dropped, because printing one string twice
 * reads as a rendering fault.
 */
export function brProcuredObject(
  fields: Record<string, unknown>,
): { primary: string; original: string | null } | null {
  const original = text(fields.objeto_contrato);
  const english = text(fields.objeto_contrato_en);
  if (original == null) return english == null ? null : { primary: english, original: null };
  if (english == null || english === original) return { primary: original, original: null };
  return { primary: english, original };
}

/**
 * Use the parsed domain name where it exists, falling back to the raw blob.
 *
 * 000216 splits tipoContrato/categoriaProcesso into id + name columns, but it
 * only ADDS them -- values appear per month as each is re-published, so both
 * shapes coexist for a while and neither may render as empty. brContractType
 * already tolerates either (brJsonName returns the raw string when it is not
 * parseable JSON), so substituting the value is all that is needed.
 */
export function brPreferParsedDomain(
  fields: Record<string, unknown>,
): Record<string, unknown> {
  const parsed = { ...fields };
  for (const [raw, name] of [
    ["tipo_contrato", "tipo_contrato_name"],
    ["categoria_processo", "categoria_processo_name"],
  ] as const) {
    const value = text(fields[name]);
    if (value != null) parsed[raw] = value;
  }
  return parsed;
}

/** The decode a field needs, or null to print it as stored. */
const BR_DECODERS: Record<string, (value: unknown) => string | null> = {
  tipo_contrato: brContractType,
  categoria_processo: brProcessCategory,
  buyer_sphere_id: brSphere,
  buyer_power_id: brPower,
  supplier_person_type: brPersonType,
  subcontractor_person_type: brPersonType,
  supplier_country_code: brSupplierCountry,
  numero_retificacao: brAmendment,
  is_revenue_contract: brRevenueOrExpenditure,
};

/** Display value for one field, or null when it should not be rendered. */
export function brFieldValue(key: string, value: unknown): string | null {
  const decoder = BR_DECODERS[key];
  return decoder ? decoder(value) : text(value);
}

/** The three flags that are ~always NULL, shown only when actually asserted. */
export const BR_OPTIONAL_FLAGS: BrField[] = [
  { key: "parliamentary_amendment", en: "Parliamentary budget amendment", source: "emendaParlamentar" },
  { key: "from_adhesion", en: "Via adhesion to another body's contract", source: "frutoAdesao" },
  { key: "has_reallocation", en: "Has reallocation", source: "temRemanejamento" },
];

/** Money columns, each paired with its USD twin. */
export const BR_MONEY_FIELDS: (BrField & { usdKey: string })[] = [
  { key: "valor_global", usdKey: "valor_global_usd", en: "Global value", source: "valorGlobal" },
  { key: "valor_inicial", usdKey: "valor_inicial_usd", en: "Initial value", source: "valorInicial" },
  { key: "valor_parcela", usdKey: "valor_parcela_usd", en: "Instalment value", source: "valorParcela" },
  { key: "valor_acumulado", usdKey: "valor_acumulado_usd", en: "Accumulated value", source: "valorAcumulado" },
];

const BRL = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const USD = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function brMoneyBRL(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? `R$ ${BRL.format(n)}` : null;
}

export function brMoneyUSD(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? `$${USD.format(n)}` : null;
}

/**
 * Our matcher's verdict on the supplier, in words. This is our opinion rather
 * than the register's data, so it belongs in the provenance footer: 2,712
 * suppliers are natural persons with no company record, which is why those
 * contracts carry no company link, and a reader cannot infer that from the
 * bare enum `natural_person`.
 */
const BR_MATCH_STATUS: Record<string, string> = {
  exact: "Supplier linked to a company record by exact CNPJ match",
  natural_person: "Supplier is a natural person, so no company record is expected",
  invalid_supplier_id: "Supplier id published by PNCP is not a valid CNPJ",
  unknown_person_type: "PNCP did not state whether the supplier is a company or a person",
  missing_supplier_id: "PNCP published no supplier id",
};

export function brMatchStatus(value: unknown): string | null {
  const code = text(value);
  if (code == null) return null;
  return BR_MATCH_STATUS[code] ?? code;
}

/** English label first, PNCP's own field name beneath it, value last. */
function BrFieldRow({ field, value }: { field: BrField; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 overflow-hidden">
      <dt className="text-xs leading-tight">
        <span className="text-foreground">{field.en}</span>
        <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
          {field.source}
        </span>
      </dt>
      <dd className="text-sm break-words" title={value}>
        {value}
      </dd>
    </div>
  );
}

function BrFieldGrid({ children }: { children: React.ReactNode }) {
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
      {children}
    </dl>
  );
}

/**
 * One PNCP contract, presented as the register's own document.
 *
 * A field with no value is omitted rather than printed as an empty row or as a
 * fabricated default — see brSupplierCountry and brTriStateFlag for the two
 * cases where a blank carries meaning and guessing would mislead.
 */
export function BrContractRecord({ fields }: { fields: Record<string, unknown> }) {
  const record = brPreferParsedDomain(fields);
  const sections = BR_CONTRACT_SECTIONS.map((section) => ({
    title: section.title,
    rows: section.fields
      .map((field) => ({ field, value: brFieldValue(field.key, record[field.key]) }))
      .filter((row): row is { field: BrField; value: string } => row.value !== null),
  })).filter((section) => section.rows.length > 0);

  const money = BR_MONEY_FIELDS.map((field) => ({
    field,
    brl: brMoneyBRL(fields[field.key]),
    usd: brMoneyUSD(fields[field.usdKey]),
  })).filter((row) => row.brl !== null);

  const instalments = brFieldValue("numero_parcelas", fields.numero_parcelas);

  const flags = BR_OPTIONAL_FLAGS.map((field) => ({
    field,
    value: brTriStateFlag(fields[field.key]),
  })).filter((row): row is { field: BrField; value: string } => row.value !== null);

  const procured = brProcuredObject(fields);
  const matchStatus = brMatchStatus(fields.company_match_status);
  const fxRate = fields.fx_rate_to_usd == null ? null : String(fields.fx_rate_to_usd);
  const fxDate = fields.fx_rate_date == null ? null : String(fields.fx_rate_date);

  return (
    <div className="flex flex-col gap-6">
      {procured ? (
        <section className="flex flex-col gap-1">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            What was procured
            <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px] normal-case">
              objetoContrato
            </span>
          </h3>
          <p className="text-sm">{procured.primary}</p>
          {procured.original ? (
            <p className="text-muted-foreground text-xs" lang="pt">
              {procured.original}
            </p>
          ) : null}
        </section>
      ) : null}

      {sections.map((section) => (
        <section key={section.title} className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            {section.title}
          </h3>
          <BrFieldGrid>
            {section.rows.map((row) => (
              <BrFieldRow key={row.field.key} field={row.field} value={row.value} />
            ))}
          </BrFieldGrid>
        </section>
      ))}

      {money.length > 0 ? (
        <section className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            Value
          </h3>
          <BrFieldGrid>
            {money.map(({ field, brl, usd }) => (
              <div key={field.key} className="flex flex-col gap-0.5">
                <dt className="text-xs leading-tight">
                  <span className="text-foreground">{field.en}</span>
                  <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
                    {field.source}
                  </span>
                </dt>
                <dd className="text-sm tabular-nums">
                  {brl}
                  {usd ? (
                    <span className="text-muted-foreground ml-2 text-xs">{usd}</span>
                  ) : null}
                </dd>
              </div>
            ))}
            {instalments ? (
              <BrFieldRow
                field={{ key: "numero_parcelas", en: "Instalments", source: "numeroParcelas" }}
                value={instalments}
              />
            ) : null}
          </BrFieldGrid>
          {fxRate && fxDate ? (
            <p className="text-muted-foreground text-xs">
              USD figures converted at {fxRate} on {fxDate}
              {fields.fx_source ? ` (${String(fields.fx_source)})` : ""}. The BRL
              figure is what PNCP published.
            </p>
          ) : null}
        </section>
      ) : null}

      {flags.length > 0 ? (
        <section className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            Budget
          </h3>
          <BrFieldGrid>
            {flags.map((row) => (
              <BrFieldRow key={row.field.key} field={row.field} value={row.value} />
            ))}
          </BrFieldGrid>
        </section>
      ) : null}

      {matchStatus ? (
        <p className="text-muted-foreground border-t pt-3 text-xs">{matchStatus}</p>
      ) : null}
    </div>
  );
}
