/**
 * CPV codes, made readable.
 *
 * A notice's classification was rendered by dumping the array through
 * JSON.stringify, which produced this and expected a reader to do something with
 * it:
 *
 *   ["71314000","71314200","66140000","66000000","71300000","09000000",
 *    "66100000","71310000","09300000","09310000","71000000"]
 *
 * Eleven codes, but only THREE subjects: `71000000 → 71300000 → 71310000 →
 * 71314000 → 71314200` is a single hierarchy chain, and so are the 66s and the
 * 09s. CPV is a tree read left to right — two digits are the division, then group,
 * class, category, with trailing zeros meaning "no more precision given" — so a
 * notice listing an ancestor and its descendants is saying one thing at several
 * depths, not several things.
 *
 * So codes are grouped by division and the deepest one in each chain is kept. That
 * example becomes: Engineering services · Financial and insurance services ·
 * Energy and fuel.
 *
 * Only DIVISIONS are named, not all ~9,500 codes. That is a deliberate limit: 40%
 * of the codes actually used are division-only (`72000000`, `45000000`), so for
 * those the division label IS the exact meaning, and for the rest it is the honest
 * summary with the full code kept beside it. Naming 9,500 codes from memory would
 * be inventing most of them.
 */

/** All 46 divisions present across the loaded registers, per the EU CPV table. */
const CPV_DIVISIONS: Record<string, string> = {
  "03": "Agriculture, farming, fishing and forestry products",
  "09": "Energy and fuel",
  "14": "Mining, basic metals and related products",
  "15": "Food, beverages and tobacco",
  "16": "Agricultural machinery",
  "18": "Clothing, footwear and luggage",
  "19": "Leather, textiles, plastic and rubber",
  "22": "Printed matter",
  "24": "Chemical products",
  "30": "Office and computing machinery",
  "31": "Electrical machinery and lighting",
  "32": "Radio, television and telecommunication equipment",
  "33": "Medical equipment and pharmaceuticals",
  "34": "Transport equipment",
  "35": "Security, fire-fighting, police and defence equipment",
  // Division 36 became 37 in the CPV 2008 revision. Exactly ONE notice in the
  // corpus still uses it against 37's 1,465, so it is named for what it is
  // rather than given 37's label and quietly conflated with it.
  "36": "Musical instruments, sport goods and handicraft (CPV 2003)",
  "37": "Musical instruments, sport goods, games and handicraft",
  "38": "Laboratory, optical and precision equipment",
  "39": "Furniture, furnishings and cleaning products",
  "41": "Collected and purified water",
  "42": "Industrial machinery",
  "43": "Mining, quarrying and construction machinery",
  "44": "Construction structures and materials",
  "45": "Construction work",
  "48": "Software and information systems",
  "50": "Repair and maintenance services",
  "51": "Installation services",
  "55": "Hotel, restaurant and retail services",
  "60": "Transport services",
  "63": "Supporting transport and travel agency services",
  "64": "Postal and telecommunications services",
  "65": "Public utilities",
  "66": "Financial and insurance services",
  "70": "Real estate services",
  "71": "Architectural, engineering and inspection services",
  "72": "IT services",
  "73": "Research and development services",
  "75": "Administration, defence and social security services",
  "76": "Services for the oil and gas industry",
  "77": "Agricultural, forestry and horticultural services",
  "79": "Business services: law, marketing, consulting and security",
  "80": "Education and training services",
  "85": "Health and social work services",
  "90": "Sewage, refuse, cleaning and environmental services",
  "92": "Recreational, cultural and sporting services",
  "98": "Other community, social and personal services",
};

/**
 * Every code in a value, however the register packed them.
 *
 * Registers disagree about the shape: Doffin and TED publish an array, and
 * Hilma publishes ONE comma-joined string — "72317000, 48800000, 72000000" —
 * on 1,782 of Finland's 9,275 classified rows. Treating that string as a single
 * code read its digits as one number, so a contract about software and IT
 * services rendered one nonsense code and matched only the first division it
 * happened to list.
 */
export function cpvCodeList(raw: unknown): string[] {
  return (Array.isArray(raw) ? raw : [raw])
    .flatMap((v) => (v == null ? [] : String(v).split(/[,;|\s]+/)))
    .map((v) => v.trim())
    .filter((v) => v !== "");
}

/**
 * The significant prefix of a code — what selecting it in a tree means.
 *
 * CPV is read left to right and trailing zeros mean "no more detail given", so
 * a code's significant prefix is also its position in the hierarchy:
 * `45000000` is Construction work, `45210000` building construction,
 * `45213100` commercial buildings. Stripping the zeros turns each into the
 * prefix every descendant shares — `45`, `4521`, `452131` — so matching on it
 * selects a node and everything beneath it.
 *
 * Never shorter than two digits, because the division is the coarsest real
 * unit: `30000000` strips to `3`, which is not a division, so it is held at
 * `30`. Returns null for anything that is not a usable code.
 */
export function cpvPrefix(raw: unknown): string | null {
  const digits = String(raw ?? "").replace(/\D/g, "").slice(0, 8);
  if (digits.length < 2) return null;
  return digits.slice(0, cpvDepth(digits));
}

/** How specific a code is — trailing zeros mean the buyer gave no more detail. */
export function cpvDepth(code: string): number {
  const digits = code.replace(/\D/g, "").slice(0, 8);
  if (digits.length < 2) return 0;
  const trimmed = digits.replace(/0+$/, "");
  return Math.max(2, trimmed.length);
}

/** The division's name, or null when the code is not one we can place. */
export function cpvDivisionLabel(code: string): string | null {
  const division = code.replace(/\D/g, "").slice(0, 2);
  return CPV_DIVISIONS[division] ?? null;
}

export type CpvSubject = {
  /** The deepest code in this division's chain — what the buyer actually said. */
  code: string;
  division: string;
  label: string;
  /** Every code the notice listed for this division, deepest first. */
  codes: string[];
};

/**
 * Group a notice's codes into subjects: one per division, keeping the deepest code.
 *
 * Deepest first within a division, and divisions ordered by their deepest code so
 * the most specific subject leads — a notice naming wheelchairs and "medical,
 * broadly" is about wheelchairs. Equal depths break on the division code, so the
 * output never depends on the order the register happened to list them in.
 */
export function cpvSubjects(raw: unknown): CpvSubject[] {
  const codes = cpvCodeList(raw);
  if (codes.length === 0) return [];

  const byDivision = new Map<string, string[]>();
  for (const code of codes) {
    const division = code.replace(/\D/g, "").slice(0, 2);
    if (division === "") continue;
    const list = byDivision.get(division) ?? [];
    if (!list.includes(code)) list.push(code);
    byDivision.set(division, list);
  }

  return [...byDivision.entries()]
    .map(([division, list]) => {
      const sorted = [...list].sort((a, b) => cpvDepth(b) - cpvDepth(a));
      return {
        code: sorted[0],
        division,
        label: CPV_DIVISIONS[division] ?? `CPV division ${division}`,
        codes: sorted,
      };
    })
    .sort(
      (a, b) =>
        // Depth first, then the division code. Without the tiebreak two equally
        // specific subjects order by whichever appeared first in the array, so the
        // same notice could render its subjects differently depending on how the
        // register happened to list them.
        cpvDepth(b.code) - cpvDepth(a.code) || a.division.localeCompare(b.division),
    );
}
