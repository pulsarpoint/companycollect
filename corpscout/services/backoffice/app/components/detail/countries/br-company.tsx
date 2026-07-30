/**
 * Brazil (RFB) company presentation.
 *
 * The generic field grid printed RFB's raw codes as SEPARATE ROWS beside the
 * labels they decode to: `Legal nature code 2062` next to `Legal nature
 * description pt`, `Company size code 01` next to `Company size en: Micro`, and
 * `Status code 02` next to both `Status: Active` and `Is active: yes` — three
 * renderings of one fact.
 *
 * The fix is to PAIR them, not to drop them. A unified value we derived is
 * unfalsifiable on its own, and for size and status the code is the only original
 * RFB publishes — there is no Portuguese text behind them. So each reads
 * `Micro (01)`, the same shape as `State level (E)` and `Brazil (BRA)`.
 *
 * `legal_nature_description_pt` is Portuguese, on a page whose labels are
 * English. It is a CLOSED domain — 90 codes across all 68,629,147 companies, from
 * CONCLA's official table — so it is translated by a static map here rather than
 * queued to the LLM translator, exactly as PNCP's tipoContrato is. Contract
 * objects go to the translator because they are free text; this is a code list.
 */

/**
 * CONCLA legal natures, all 90 present in br_companies. Counts are the corpus as
 * of 2026-07-30, kept for the ones worth recognising at a glance.
 *
 * Note 4090 "Candidato a Cargo Político Eletivo" (2,937,479 rows) and 4120
 * "Produtor Rural (Pessoa Física)" (636,055): RFB issues CNPJs to election
 * candidates and to individual rural producers, so a large slice of "companies"
 * are neither companies nor, in the candidates' case, businesses at all.
 */
const BR_LEGAL_NATURE_EN: Record<string, string> = {
  "0000": "Legal nature not stated",
  "1015": "Federal executive public body",
  "1023": "State or Federal District executive public body",
  "1031": "Municipal executive public body",
  "1040": "Federal legislative public body",
  "1058": "State or Federal District legislative public body",
  "1066": "Municipal legislative public body",
  "1074": "Federal judiciary public body",
  "1082": "State judiciary public body",
  "1104": "Federal autonomous agency",
  "1112": "State or Federal District autonomous agency",
  "1120": "Municipal autonomous agency",
  "1139": "Federal public foundation under public law",
  "1147": "State or Federal District public foundation under public law",
  "1155": "Municipal public foundation under public law",
  "1163": "Federal autonomous public body",
  "1171": "State or Federal District autonomous public body",
  "1180": "Municipal autonomous public body",
  "1198": "Multinational commission",
  "1210": "Public consortium under public law (public association)",
  "1228": "Public consortium under private law",
  "1236": "State or Federal District",
  "1244": "Municipality",
  "1252": "Federal public foundation under private law",
  "1260": "State or Federal District public foundation under private law",
  "1279": "Municipal public foundation under private law",
  "1287": "Federal indirect-administration public fund",
  "1295": "State or Federal District indirect-administration public fund",
  "1309": "Municipal indirect-administration public fund",
  "1317": "Federal direct-administration public fund",
  "1325": "State or Federal District direct-administration public fund",
  "1333": "Municipal direct-administration public fund",
  "1341": "The Union (federal government)",
  "1350": "Public entity under a special regime",
  "2011": "State-owned enterprise",
  "2038": "Mixed-capital company",
  "2046": "Publicly traded corporation",
  "2054": "Closely held corporation",
  "2062": "Private limited company",
  "2070": "General partnership",
  "2089": "Limited partnership",
  "2097": "Partnership limited by shares",
  "2100": "Capital and industry partnership",
  "2127": "Undisclosed partnership",
  "2135": "Sole trader",
  "2143": "Cooperative",
  "2151": "Consortium of companies",
  "2160": "Group of companies",
  "2178": "Brazilian establishment of a foreign company",
  "2216": "Company domiciled abroad",
  "2224": "Investment club or fund",
  "2232": "Simple partnership",
  "2240": "Simple limited partnership",
  "2259": "Simple general partnership",
  "2267": "Simple limited partnership (comandita)",
  "2275": "Binational company",
  "2283": "Employers' consortium",
  "2291": "Simple consortium",
  "2305": "Single-member limited liability company (business)",
  "2313": "Single-member limited liability company (simple)",
  "2321": "Single-lawyer law firm",
  "2330": "Consumer cooperative",
  "2348": "Simple innovation company",
  "3034": "Notary or registry office",
  "3069": "Private foundation",
  "3077": "Autonomous social service",
  "3085": "Condominium association",
  "3107": "Prior conciliation commission",
  "3115": "Mediation and arbitration body",
  "3131": "Trade union body",
  "3204": "Brazilian establishment of a foreign foundation or association",
  "3212": "Foundation or association domiciled abroad",
  "3220": "Religious organisation",
  "3239": "Indigenous community",
  "3247": "Private fund",
  "3255": "National political party leadership body",
  "3263": "Regional political party leadership body",
  "3271": "Local political party leadership body",
  "3280": "Political party finance committee",
  "3298": "Plebiscite or referendum front",
  "3301": "Social organisation (OS)",
  "3328": "Closed supplementary pension benefit plan",
  "3999": "Private association",
  "4014": "Sole-trader real-estate company",
  "4090": "Candidate for elected political office",
  "4120": "Individual rural producer",
  "5010": "International organisation",
  "5029": "Foreign diplomatic mission",
  "5037": "Other extraterritorial institutions",
  "8885": "Legal nature not stated",
};

/**
 * English for a legal nature, keeping the Portuguese the register published.
 *
 * An unmapped code falls back to the Portuguese description, then to the code
 * itself. A new CONCLA value must show what RFB actually said rather than
 * disappear or be guessed at.
 */
export function brLegalNature(
  code: unknown,
  portuguese: unknown,
): string | null {
  const key = code == null ? "" : String(code).trim();
  const pt = portuguese == null ? "" : String(portuguese).trim();
  const english = BR_LEGAL_NATURE_EN[key];
  if (english) return pt !== "" ? `${english} (${pt})` : english;
  if (pt !== "") return pt;
  return key !== "" ? key : null;
}

/**
 * A unified value always carries the register's original beside it.
 *
 * The first version of this file DELETED these codes, which was the wrong
 * correction: the codes were redundant as SEPARATE ROWS next to their label, not
 * redundant as values. RFB publishes size and status as codes with no Portuguese
 * text at all, so for those two the code IS the original -- dropping it left only
 * a value we derived, with nothing to check it against.
 *
 * So each pairs into one row instead, the same shape used for
 * `State level (E)`, `Brazil (BRA)` and `Contract, initial term (...)`:
 *
 *   Company size    Micro (01)          was: "Micro" + a separate "01" row
 *   Status          Active (02)         was: "Active" + "02" + "is_active: yes"
 *   Municipality    SALVADOR (3849)     was: name + a separate RFB key
 *   Legal nature    Private limited company (Sociedade Empresária Limitada)
 *
 * is_active stays dropped: it is a second DERIVATION of status, not an original
 * of anything, so it adds a third rendering of one fact and no checkability.
 */
const BR_PAIRED_FIELDS: readonly (readonly [display: string, original: string])[] = [
  ["company_size_en", "company_size_code"],
  // status_en, not status: the record is SELECT * from br_companies, whose
  // columns are status_code / status_en / status_date. Pairing a key the record
  // does not have would CREATE a row showing the bare code, which is how the
  // first attempt made this read "Status: 02".
  ["status_en", "status_code"],
  ["municipality_name", "municipality_code"],
] as const;

/** Derived twice over, so it is dropped rather than paired. */
const BR_REDUNDANT_DERIVED_FIELDS = ["is_active"] as const;

function paired(display: unknown, original: unknown): string | null {
  const value = display == null ? "" : String(display).trim();
  const code = original == null ? "" : String(original).trim();
  if (value === "") return code === "" ? null : code;
  return code === "" ? value : `${value} (${code})`;
}

export function decorateBrRecord(
  record: Record<string, unknown>,
): Record<string, unknown> {
  const decorated = { ...record };

  for (const [display, original] of BR_PAIRED_FIELDS) {
    // Only fold into a field the record actually has. Otherwise the original is
    // left exactly where it was: better a raw code under its own label than an
    // invented row, and better than silently dropping the only value present.
    if (!(display in record)) continue;
    const value = paired(record[display], record[original]);
    if (value != null) decorated[display] = value;
    delete decorated[original];
  }
  for (const key of BR_REDUNDANT_DERIVED_FIELDS) delete decorated[key];

  // "Company size en" is a column name, not a label. The "_en" told a reader
  // which of two language variants they were looking at, and there is no
  // Portuguese variant of this one -- RFB publishes only the code.
  if ("company_size_en" in decorated) {
    decorated.company_size = decorated.company_size_en;
    delete decorated.company_size_en;
  }

  const legalNature = brLegalNature(
    record.legal_nature_code,
    record.legal_nature_description_pt,
  );
  if (legalNature != null) {
    // Renamed as well as translated: "description pt" is no longer true of it,
    // and the code is already inside the value.
    delete decorated.legal_nature_description_pt;
    delete decorated.legal_nature_code;
    decorated.legal_nature = legalNature;
  }
  return decorated;
}
