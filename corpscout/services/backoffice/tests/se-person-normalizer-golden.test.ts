import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { normalizeSePersonName } from "~/lib/se-person-fields";

/**
 * Bounds the divergence between this TypeScript port and the real normalizer
 * (`dagster_v3/src/dagster_v3/defs/se_company/person/normalize_se.py`) by
 * replaying the normalizer's own golden corpus: for every fixture whose
 * expected `parse_status` is "ok", the port's first/middle/last tokens must
 * equal the corpus's expected tokens (controller ruling, task 1 of the
 * backoffice People tab).
 *
 * The corpus lives in the dagster_v3 package, a sibling of this one under
 * `corpscout/services/`; the path is resolved from this test file's own
 * location so it does not depend on the process's working directory.
 */

interface GoldenCase {
  source: string;
  raw: {
    full_name?: string;
    first_name?: string;
    last_name?: string;
    birth_year?: number;
    wikidata_id?: string;
    role_original?: string;
    role_key?: string;
  };
  expected: {
    parse_status: "ok" | "partial" | "no_person";
    parse_notes: string[];
    first_tokens: string[];
    middle_tokens: string[];
    last_tokens: string[];
    display_first: string;
    display_last: string;
    display_name: string;
    role_code: string | null;
  };
}

const CORPUS_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "dagster_v3",
  "tests",
  "fixtures",
  "se_persons",
  "golden.jsonl",
);

function readCorpus(): GoldenCase[] {
  const text = readFileSync(CORPUS_PATH, "utf-8");
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .map((line) => JSON.parse(line) as GoldenCase);
}

/**
 * The port takes either a one-string `fullName` (spec 4.1's comma form and
 * plain form both go through `normalizeSePersonName({ fullName })`) or a
 * delivered `firstName`/`lastName` pair -- the two shapes every source in
 * the corpus actually uses (Bolagsverket splits, ESEF/Wikidata deliver one
 * string). A row that fits neither shape exercises a rule this port does
 * not implement and is skipped rather than silently miscompared.
 */
function portInputFor(
  raw: GoldenCase["raw"],
): { fullName?: string; firstName?: string; lastName?: string } | null {
  if (typeof raw.full_name === "string" && raw.full_name !== "") {
    return { fullName: raw.full_name };
  }
  if (raw.first_name !== undefined || raw.last_name !== undefined) {
    return { firstName: raw.first_name ?? "", lastName: raw.last_name ?? "" };
  }
  return null;
}

describe("normalizeSePersonName against the normalizer's golden corpus", () => {
  const cases = readCorpus();
  const okCases = cases.filter((row) => row.expected.parse_status === "ok");
  const checked = okCases.filter((row) => portInputFor(row.raw) !== null);
  const skipped = okCases.filter((row) => portInputFor(row.raw) === null);

  it("loads a non-trivial corpus with both split and one-string rows", () => {
    expect(cases.length).toBeGreaterThan(0);
    expect(okCases.length).toBeGreaterThan(0);
  });

  it(
    `matches first/middle/last tokens for every "ok" case the port implements ` +
      `(${checked.length} checked, ${skipped.length} skipped)`,
    () => {
      for (const row of checked) {
        const input = portInputFor(row.raw);
        // Non-null by construction: `row` came from `checked`.
        const tokens = normalizeSePersonName(input as NonNullable<typeof input>);
        const context = `${row.source} raw=${JSON.stringify(row.raw)}`;
        expect(tokens.firstTokens, context).toEqual(row.expected.first_tokens);
        expect(tokens.middleTokens, context).toEqual(row.expected.middle_tokens);
        expect(tokens.lastTokens, context).toEqual(row.expected.last_tokens);
      }
    },
  );
});
