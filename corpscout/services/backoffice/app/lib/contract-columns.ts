/**
 * Which columns a country's contracts table shows, held in the URL.
 *
 * The table used to carry one fixed column set for every country, which meant
 * every reader saw Agreement type — a column Brazil fills on 100% of its rows
 * and Estonia and Norway fill on none — while CPV, which Estonia fills on 98.8%
 * and Norway on 59.8%, had no column at all.
 *
 * The split is driven by COVERAGE, not by EU membership. Norway is not in the
 * EU and has the second-best CPV coverage of any loaded register, so an EU test
 * would put the wrong country on the wrong side. `available` is measured from
 * the data (see `getContractColumnAvailability`), so a column that a register
 * never fills is not offered, and a shared URL naming it is ignored rather than
 * adding a permanently empty column.
 *
 * Client-safe: no `.server` imports, so the picker can use it directly.
 */

export type ContractColumnId =
  | "date"
  | "buyer"
  | "winner"
  | "title"
  | "amount_original"
  | "amount_usd"
  | "agreement_type"
  | "cpv"
  | "source";

export type ContractColumn = {
  id: ContractColumnId;
  label: string;
  /** Cannot be hidden. */
  locked?: boolean;
};

/**
 * Every column, in the order the table renders them. The array IS the canonical
 * order — a selection is sorted back into it, so the table never reorders
 * itself because of the sequence a reader happened to tick boxes in.
 */
export const CONTRACT_COLUMNS: ContractColumn[] = [
  { id: "date", label: "Date" },
  { id: "buyer", label: "Buyer" },
  { id: "winner", label: "Winner" },
  // The only route to the contract's own page. Hiding it would leave a reader
  // with rows they can see and cannot open.
  { id: "title", label: "Contract", locked: true },
  { id: "amount_original", label: "Amount (original)" },
  { id: "amount_usd", label: "Amount (USD)" },
  { id: "agreement_type", label: "Agreement type" },
  { id: "cpv", label: "CPV" },
  { id: "source", label: "Source" },
];

const BY_ID = new Map(CONTRACT_COLUMNS.map((c) => [c.id, c]));
const LOCKED = CONTRACT_COLUMNS.filter((c) => c.locked).map((c) => c.id);

export function contractColumnLabel(id: ContractColumnId): string {
  return BY_ID.get(id)?.label ?? id;
}

function isColumnId(value: string): value is ContractColumnId {
  return BY_ID.has(value as ContractColumnId);
}

/** Canonical order, deduplicated, restricted to what the country publishes. */
function canonical(
  ids: Iterable<ContractColumnId>,
  available: ContractColumnId[],
): ContractColumnId[] {
  const wanted = new Set(ids);
  const offered = new Set(available);
  return CONTRACT_COLUMNS.filter(
    (c) => wanted.has(c.id) && (offered.has(c.id) || c.locked),
  ).map((c) => c.id);
}

/**
 * Everything the country publishes.
 *
 * Showing all of it by default and letting a reader hide what they do not want
 * beats guessing: a column is only offered when the register actually fills it,
 * so the default is never padded with blanks.
 */
export function defaultContractColumns(
  available: ContractColumnId[],
): ContractColumnId[] {
  return canonical(available, available);
}

export function parseContractColumns(
  searchParams: URLSearchParams,
  available: ContractColumnId[],
): ContractColumnId[] {
  const raw = searchParams.get("cols");
  // Absent means "not customised" and takes the default. Present-but-empty
  // means the reader unticked everything, which is a choice worth keeping.
  if (raw === null) return defaultContractColumns(available);

  const chosen = raw
    .split(",")
    .map((v) => v.trim())
    .filter(isColumnId);

  return canonical([...chosen, ...LOCKED], available);
}

/** The `cols` value, or null when the selection is just the default. */
export function serializeContractColumns(
  visible: ContractColumnId[],
  available: ContractColumnId[],
): string | null {
  const chosen = canonical(visible, available);
  const fallback = defaultContractColumns(available);
  if (chosen.length === fallback.length && chosen.every((id, i) => id === fallback[i])) {
    return null;
  }
  return chosen.join(",");
}
