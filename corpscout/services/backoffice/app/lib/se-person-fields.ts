/**
 * The person entity's catalogue, the client-safe validation of a typed person, and a
 * TypeScript port of the normalizer's identity rules (spec 2026-09-09 sections 3.1, 4
 * and 7). No `.server` import: the route's module must not drag ClickHouse into the
 * client bundle.
 *
 * WHY THE PORT (Ruling 1). Two things need the normalizer's own answer before any fold
 * runs: the sheet must refuse a name that would score `partial` or `no_person` (such a
 * row is stored and never folded into a person, so activating it would publish
 * nothing), and Activate must know whether a Correct's text folds back into the person
 * it corrects -- if it does, hiding that person would hide the correction with it. This
 * is `normalize_se.py` sections 4.1, 4.2 and 4.4 rule for rule; keep the two in step,
 * and when they disagree the fold wins (both ways of being wrong are visible: a missed
 * hide rule leaves a duplicate person, a spurious one hides a person Reset brings back).
 */

export const PERSON_SOURCES = [
  "bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft",
] as const;
export const REVIEWER_SOURCE = "reviewer";
export const DRAFT_SOURCE = "reviewer_draft";
export const PERSON_STATUSES = ["active", "hidden", "withdrawn"] as const;
/** Ruling 4: the three `data` keys the backoffice writes itself. */
export const RESERVED_DATA_KEYS = ["decided_by", "note", "replaces_key"] as const;
export const MAX_NOTE_LENGTH = 500;
export const MAX_DATA_LENGTH = 4000;
export const MAX_NAME_LENGTH = 100;
export const MAX_ROLE_ENTRIES = 20;
export const MIN_BIRTH_YEAR = 1850;
export const MIN_ROLE_YEAR = 1900;
/** ClickHouse `Date` floors at 1970-01-01 (`toDate('1901-05-04')` -> `1970-01-01` on
 * prod), so a role that renders as a span, an open span or an end-only year -- anything
 * that writes into `role_from`/`role_to` rather than `fiscal_year` -- must not start
 * before it: a year below this would be silently rewritten to 1970 and an open span
 * would inflate into decades of fabricated role-years. A single fiscal year is exempt --
 * it goes to `fiscal_year UInt16`, which does not floor -- and keeps `MIN_ROLE_YEAR`. */
export const MIN_SPAN_YEAR = 1970;
/** Wikidata end dates run past today (2029 on prod), so a typed role may too. */
export const ROLE_YEAR_SLACK = 5;

export type SePersonSource = (typeof PERSON_SOURCES)[number];
export type SePersonStatus = (typeof PERSON_STATUSES)[number];

/** The sources the main table can actually hold: `reviewer_draft` never folds into a
 * published row and `ratsit` is reserved with no data yet, so both always return zero
 * rows from the People list's Source filter. The list offers these four, not the whole
 * catalogue. */
export const MAIN_PERSON_SOURCES: readonly SePersonSource[] = PERSON_SOURCES.filter(
  (source) => source !== DRAFT_SOURCE && source !== "ratsit",
);

const KEY_PATTERN = /^[0-9a-f]{64}$/;
const CONTROL = /[\u0000-\u001f\u007f]/;
/** The note is a Textarea: tab, line feed and carriage return are what a browser sends
 * for a multi-line note, so only the other C0 controls and DEL are refused (the same
 * class `se-address-fields.ts` uses). */
const NOTE_CONTROL = /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/;
const QID_PATTERN = /^Q[1-9][0-9]{0,11}$/;
export const PERSON_GROUP_SLOT_PATTERN = /^r[0-9]{17}$/;

export function isPersonSource(value: string): value is SePersonSource {
  return (PERSON_SOURCES as readonly string[]).includes(value);
}
export function isPersonStatus(value: string): value is SePersonStatus {
  return (PERSON_STATUSES as readonly string[]).includes(value);
}
export function isPersonKey(value: string): boolean {
  return KEY_PATTERN.test(value);
}

const SOURCE_LABELS: Record<SePersonSource, string> = {
  bolagsverket: "Bolagsverket", esef: "ESEF", wikidata: "Wikidata", ratsit: "Ratsit",
  reviewer: "Reviewer", reviewer_draft: "Reviewer draft",
};
export function personSourceLabel(source: string): string {
  return isPersonSource(source) ? SOURCE_LABELS[source] : source;
}

/** One row of `corpscout.company_person_role_type`, as the loader hands it to the
 * client (Ruling 3): the catalog is a live table, never a hard-coded list. */
export interface SePersonRoleOption {
  code: string;
  label: string;
  group: string;
}
/** The catalog's display name for a code, or the code itself -- an unmapped source
 * label is published as itself (spec 4.3) and must still read. */
export function roleLabel(code: string, options: readonly SePersonRoleOption[]): string {
  return options.find((option) => option.code === code)?.label ?? code;
}

export function selectedPersonFromSearch(params: URLSearchParams): string | null {
  const key = params.get("person") ?? "";
  return isPersonKey(key) ? key : null;
}

/* ------------------------------------------------------------------ */
/* Ruling 2: the reviewer's slots                                       */
/* ------------------------------------------------------------------ */

/** The group slot of one reviewer person: `r` + the stamp's 17 digits. */
export function personGroupSlot(stamp: string): string {
  return `r${stamp.replace(/\D/g, "")}`;
}
/** One row's slot inside that group: the group plus a two-digit ordinal (01..99). */
export function personRowSlot(group: string, index: number): string {
  return `${group}${String(index).padStart(2, "0")}`;
}
/** The group a row slot belongs to: `r` + 17 digits, the first 18 characters. */
export function groupOfRowSlot(rowSlot: string): string {
  return rowSlot.slice(0, 18);
}

/* ------------------------------------------------------------------ */
/* The normalizer's identity rules, ported (spec 4.1, 4.2, 4.4)         */
/* ------------------------------------------------------------------ */

const WHITESPACE = /\s+/g;
const DIGIT = /\d/;
const SUBTOKEN_SPLIT = /[.\-]+/;
const NON_TOKEN = /[^a-z0-9]+/g;
const PARTICLES = new Set(["von", "af", "de", "van", "der", "la", "le"]);
const TITLE_WORDS = new Set([
  "dr", "prof", "professor", "doktor", "herr", "fru", "froken", "mr", "mrs", "ms",
]);
const TITLE_PHRASES: readonly (readonly string[])[] = [
  ["jur", "kand"], ["civ", "ing"], ["civ", "ekon"],
  ["ekon", "dr"], ["fil", "dr"], ["med", "dr"], ["jur", "dr"],
];
const ROLE_PHRASES: readonly (readonly string[])[] = [
  ["styrelseledamot"], ["styrelseordforande"], ["styrelsesuppleant"], ["ordforande"],
  ["suppleant"], ["ledamot"], ["revisor"], ["likvidator"], ["firmatecknare"],
  ["arbetstagarrepresentant"], ["vd"], ["verkstallande", "direktor"], ["vice", "vd"],
  ["huvudansvarig", "revisor"], ["auktoriserad", "revisor"],
  ["board", "member"], ["board", "chair"], ["chairman"], ["auditor"], ["liquidator"],
  ["chief", "executive", "officer"], ["director"], ["founder"], ["owner"],
];
/** `ek` is deliberately absent: Ek is a common Swedish surname. */
const COMPANY_TOKENS = new Set([
  "ab", "hb", "kb", "aktiebolag", "handelsbolag", "kommanditbolag",
]);

function clean(text: string | undefined): string {
  return (text ?? "").trim().replace(WHITESPACE, " ");
}
/** Case-folded and diacritic-free: Hakan and Håkan meet, Ö and O meet (spec 4.2).
 * `toLowerCase` where Python casefolds -- the two differ only on characters no Swedish
 * name carries (ß), and a difference here can only mislead the Correct hint. */
function foldText(text: string): string {
  return text.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase();
}
function foldWord(word: string): string {
  return foldText(word).replace(/\./g, "");
}
/** The identity tokens of one word: folded, split on hyphens and periods, letters and
 * digits only. Sven-Erik gives sven, erik; S.E. gives s, e. */
function subtokens(word: string): string[] {
  return foldText(word)
    .split(SUBTOKEN_SPLIT)
    .map((piece) => piece.replace(NON_TOKEN, ""))
    .filter((piece) => piece !== "");
}
function dropTitles(words: string[]): { kept: string[]; dropped: string[] } {
  const folded = words.map(foldWord);
  const kept: string[] = [];
  const dropped: string[] = [];
  let index = 0;
  while (index < words.length) {
    const phrase = TITLE_PHRASES.find((candidate) =>
      candidate.every((part, offset) => folded[index + offset] === part),
    );
    if (phrase !== undefined) {
      dropped.push(phrase.join(" "));
      index += phrase.length;
      continue;
    }
    if (TITLE_WORDS.has(folded[index] ?? "")) {
      dropped.push(folded[index] ?? "");
      index += 1;
      continue;
    }
    kept.push(words[index] ?? "");
    index += 1;
  }
  return { kept, dropped };
}
function hasRolePhrase(tokens: string[]): boolean {
  return ROLE_PHRASES.some((phrase) =>
    tokens.some((_, start) => phrase.every((part, offset) => tokens[start + offset] === part)),
  );
}
/** "Last, First" for a comma form; otherwise the last word is the last name, preceded
 * by any run of particles ("Carl von Essen" is Carl / von Essen). */
function splitFullName(full: string): { firstWords: string[]; lastWords: string[] } {
  if (full.includes(",")) {
    const [lastPart = "", firstPart = ""] = full.split(/,(.*)/s);
    return {
      firstWords: clean(firstPart) === "" ? [] : dropTitles(clean(firstPart).split(" ")).kept,
      lastWords: clean(lastPart) === "" ? [] : dropTitles(clean(lastPart).split(" ")).kept,
    };
  }
  const words = dropTitles(full.split(" ")).kept;
  let index = Math.max(words.length - 1, 0);
  while (index > 0 && PARTICLES.has(foldWord(words[index - 1] ?? ""))) index -= 1;
  return { firstWords: words.slice(0, index), lastWords: words.slice(index) };
}

export interface SePersonTokens {
  status: "ok" | "partial" | "no_person";
  /** Why it is not `ok`; `""` when it is. The reviewer reads this. */
  reason: string;
  firstTokens: string[];
  middleTokens: string[];
  lastTokens: string[];
  displayFirst: string;
  displayLast: string;
  displayName: string;
}

/** `normalize_se.py::normalize_se_person` without the role half (spec 4.1, 4.2, 4.4). */
export function normalizeSePersonName(raw: {
  fullName?: string;
  firstName?: string;
  lastName?: string;
}): SePersonTokens {
  const firstIn = clean(raw.firstName);
  const lastIn = clean(raw.lastName);
  const full = clean(raw.fullName);
  const splitDelivered = firstIn !== "" || lastIn !== "";
  const sourceText = splitDelivered ? [firstIn, lastIn].filter((p) => p !== "").join(" ") : full;
  const rejected = (reason: string): SePersonTokens => ({
    status: "no_person", reason,
    firstTokens: [], middleTokens: [], lastTokens: [],
    displayFirst: "", displayLast: "", displayName: sourceText,
  });
  if (sourceText === "") return rejected("empty name");
  const tokens = sourceText.split(" ").flatMap(subtokens);
  if (DIGIT.test(sourceText)) return rejected("digits in the name field");
  if (tokens.some((token) => COMPANY_TOKENS.has(token))) {
    return rejected("company suffix in the name field");
  }
  if (hasRolePhrase(tokens)) return rejected("role word in the name field");

  const { firstWords, lastWords } = splitDelivered
    ? {
        firstWords: firstIn === "" ? [] : dropTitles(firstIn.split(" ")).kept,
        lastWords: lastIn === "" ? [] : dropTitles(lastIn.split(" ")).kept,
      }
    : splitFullName(full);
  const givenTokens = firstWords.flatMap(subtokens);
  const lastTokens = lastWords.flatMap(subtokens);
  const firstTokens = givenTokens.slice(0, 1);
  const middleTokens = givenTokens.slice(1);
  const displayFirst = firstWords.join(" ");
  const displayLast = lastWords.join(" ");
  let status: SePersonTokens["status"] = "ok";
  let reason = "";
  if (firstTokens.length === 0 || lastTokens.length === 0) {
    status = "partial";
    reason = "only one name word";
  } else if ([...firstTokens, ...middleTokens].every((token) => token.length === 1)) {
    status = "partial";
    reason = "initials only";
  }
  return {
    status, reason, firstTokens, middleTokens, lastTokens, displayFirst, displayLast,
    displayName: [displayFirst, displayLast].filter((part) => part !== "").join(" "),
  };
}

export interface SePersonIdentity {
  firstTokens: readonly string[];
  middleTokens: readonly string[];
  lastTokens: readonly string[];
  /** `""` when unknown -- a missing year never conflicts. */
  birthYear: string;
  wikidataId: string;
}

function sameTokens(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((token, index) => token === b[index]);
}
function subsetOf(a: readonly string[], b: readonly string[]): boolean {
  return a.every((token) => b.includes(token));
}

/**
 * Spec 5.1's guarded relation, as much of it as one pair can answer: never with two
 * different birth years; a shared QID is enough; otherwise the first and last tokens
 * must be equal and the middle tokens equal or one a subset of the other.
 *
 * The fold's own middle-name test is stricter -- it asks whether the fuller set is the
 * UNIQUE minimal superset among the company's rows, which no pair can know -- so this
 * answers "could fold together". Ruling 1 says where that lands: a Correct that folds
 * back gets no hide rule, and an over-eager `true` costs a duplicate person, not a
 * hidden one.
 */
export function foldsIntoPerson(a: SePersonIdentity, b: SePersonIdentity): boolean {
  if (a.birthYear !== "" && b.birthYear !== "" && a.birthYear !== b.birthYear) return false;
  if (a.wikidataId !== "" && a.wikidataId === b.wikidataId) return true;
  if (!sameTokens(a.firstTokens, b.firstTokens)) return false;
  if (!sameTokens(a.lastTokens, b.lastTokens)) return false;
  return subsetOf(a.middleTokens, b.middleTokens) || subsetOf(b.middleTokens, a.middleTokens);
}

/* ------------------------------------------------------------------ */
/* What a reviewer may type                                             */
/* ------------------------------------------------------------------ */

export interface SePersonRoleInput {
  code: string;
  /** `""` or a four-digit year. */
  fromYear: string;
  toYear: string;
}
export interface SePersonInput {
  firstName: string;
  lastName: string;
  birthYear: string;
  wikidataId: string;
  roles: SePersonRoleInput[];
  /** A JSON object, re-serialized; `"{}"` when the reviewer typed nothing. */
  data: string;
  note: string;
}
export type SePersonValidation =
  | { ok: true; input: SePersonInput }
  | { ok: false; error: string };

function plain(
  label: string,
  value: string,
  max: number,
  control: RegExp = CONTROL,
): string | { error: string } {
  const trimmed = value.trim();
  if (control.test(trimmed)) return { error: `${label} must be plain text.` };
  if (trimmed.length > max) return { error: `${label} is longer than ${max} characters.` };
  return trimmed;
}
function isYear(value: string, min: number, max: number): boolean {
  return /^[0-9]{4}$/.test(value) && Number(value) >= min && Number(value) <= max;
}

/**
 * Spec 7's validation: a name the normalizer scores `ok` (anything else never folds
 * into a person), a plausible birth year and QID, roles from the catalog with sane
 * years, a `data` object without the reserved keys, and a note.
 */
export function validateSePersonInput(
  raw: {
    firstName: string; lastName: string; birthYear: string; wikidataId: string;
    roles: SePersonRoleInput[]; data: string; note: string;
  },
  roleCodes: readonly string[],
  currentYear: number = new Date().getUTCFullYear(),
): SePersonValidation {
  const firstName = plain("First name", raw.firstName ?? "", MAX_NAME_LENGTH);
  if (typeof firstName !== "string") return { ok: false, error: firstName.error };
  const lastName = plain("Last name", raw.lastName ?? "", MAX_NAME_LENGTH);
  if (typeof lastName !== "string") return { ok: false, error: lastName.error };
  const tokens = normalizeSePersonName({ firstName, lastName });
  if (tokens.status !== "ok") return { ok: false, error: tokens.reason };

  const birthYear = (raw.birthYear ?? "").trim();
  if (birthYear !== "" && !isYear(birthYear, MIN_BIRTH_YEAR, currentYear)) {
    return { ok: false, error: `Birth year must be between ${MIN_BIRTH_YEAR} and ${currentYear}.` };
  }
  const wikidataId = (raw.wikidataId ?? "").trim();
  if (wikidataId !== "" && !QID_PATTERN.test(wikidataId)) {
    return { ok: false, error: "Wikidata id must look like Q42." };
  }

  const maxRoleYear = currentYear + ROLE_YEAR_SLACK;
  const entries = (raw.roles ?? [])
    .map((role, index) => ({ role, index }))
    .filter(
      ({ role }) => role.code.trim() !== "" || role.fromYear.trim() !== "" || role.toYear.trim() !== "",
    );
  if (entries.length > MAX_ROLE_ENTRIES) {
    return { ok: false, error: `At most ${MAX_ROLE_ENTRIES} roles.` };
  }
  const roles: SePersonRoleInput[] = [];
  for (const { role: entry, index } of entries) {
    const code = entry.code.trim();
    // A row with years but no code has nothing else wrong with it -- naming it here,
    // rather than falling through to "Unknown role: .", is what tells the reviewer
    // where to look.
    if (code === "") return { ok: false, error: `Pick a role for row ${index + 1}.` };
    if (!roleCodes.includes(code)) return { ok: false, error: `Unknown role: ${code}.` };
    const fromYear = entry.fromYear.trim();
    const toYear = entry.toYear.trim();
    // A single fiscal year (equal, non-empty) keeps the 1900 floor; a span, an open
    // span or an end-only year writes into `role_from`/`role_to` and must not cross the
    // ClickHouse `Date` floor at 1970.
    const single = fromYear !== "" && fromYear === toYear;
    const minYear = single ? MIN_ROLE_YEAR : MIN_SPAN_YEAR;
    for (const [label, year] of [["From year", fromYear], ["To year", toYear]] as const) {
      if (year !== "" && !isYear(year, minYear, maxRoleYear)) {
        return { ok: false, error: `${label} must be between ${minYear} and ${maxRoleYear}.` };
      }
    }
    if (fromYear !== "" && toYear !== "" && Number(toYear) < Number(fromYear)) {
      return { ok: false, error: "A role cannot end before it starts." };
    }
    roles.push({ code, fromYear, toYear });
  }

  const dataText = (raw.data ?? "").trim();
  if (dataText.length > MAX_DATA_LENGTH) {
    return { ok: false, error: `Data is longer than ${MAX_DATA_LENGTH} characters.` };
  }
  let data = "{}";
  if (dataText !== "") {
    let parsed: unknown;
    try {
      parsed = JSON.parse(dataText);
    } catch {
      return { ok: false, error: "Data must be a JSON object." };
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ok: false, error: "Data must be a JSON object." };
    }
    for (const key of RESERVED_DATA_KEYS) {
      if (Object.hasOwn(parsed as object, key)) {
        return { ok: false, error: `Data may not carry the reserved key ${key}.` };
      }
    }
    data = JSON.stringify(parsed);
  }

  const note = plain("Note", raw.note ?? "", MAX_NOTE_LENGTH, NOTE_CONTROL);
  if (typeof note !== "string") return { ok: false, error: note.error };
  return { ok: true, input: { firstName, lastName, birthYear, wikidataId, roles, data, note } };
}

/** The note rules of spec 7, in one place: line breaks and tabs pass, every other
 * control character and anything past 500 characters does not. `validateSePersonInput`
 * uses `plain(...)` directly; the decision parser -- whose note reaches a rule row or a
 * `data.note` without ever passing through the sheet's validation -- calls this. */
export function noteError(note: string): string | null {
  const checked = plain("Note", note, MAX_NOTE_LENGTH, NOTE_CONTROL);
  return typeof checked === "string" ? null : checked.error;
}

/** Dagster's selection (`batch.py::_changed_company_ids`) in miniature: a non-draft
 * normalized version, a non-draft raw row, a rule version or one of the company's own
 * precedence rows newer than the newest fold, or foldable rows with no fold at all.
 * Drafts and the GLOBAL precedence export are never among `stamps` (Ruling 7). */
export function personFoldPending(
  foldedAt: string | null,
  stamps: readonly string[],
  hasFoldable: boolean,
): boolean {
  if (foldedAt === null) return hasFoldable;
  return stamps.some((stamp) => stamp > foldedAt);
}
