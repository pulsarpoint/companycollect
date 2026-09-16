import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  chQuery: vi.fn(),
  chInsertTechnologyProposals: vi.fn(),
  chInsertTechnologyReview: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => mocks);
const { normalizeTechnologyName } = await import("~/lib/technology-proposals");
const {
  submitTechnologyProposals,
  reviewTechnologyProposal,
  resolveCatalogName,
} = await import("~/lib/technology-proposals.server");

const fixtureDir = resolve(
  "../../../codex-sd-examples/company_research/tests/fixtures",
);
const fixture = JSON.parse(
  readFileSync(resolve(fixtureDir, "technology_submission.json"), "utf8"),
);
const snapshot = JSON.parse(
  readFileSync(resolve(fixtureDir, "technology_catalog.json"), "utf8"),
);
let catalog = structuredClone(snapshot);
let observations: object[] = [];
let reviews: object[] = [];

beforeEach(() => {
  vi.clearAllMocks();
  catalog = structuredClone(snapshot);
  observations = [];
  reviews = [];
  mocks.chQuery.mockImplementation(async (sql: string) => {
    if (sql.includes("technology_catalog FINAL")) return catalog.entries;
    if (sql.includes("technology_aliases")) return catalog.aliases;
    if (sql.includes("technology_proposal_latest_reviews")) return [];
    if (sql.includes("technology_proposal_reviews")) return reviews;
    if (sql.includes("new_tech")) return observations;
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  mocks.chInsertTechnologyProposals.mockResolvedValue(undefined);
  mocks.chInsertTechnologyReview.mockResolvedValue(undefined);
});

async function submittedProposal() {
  await submitTechnologyProposals(structuredClone(fixture));
  observations = mocks.chInsertTechnologyProposals.mock.calls[0][0];
  return fixture.records[0].data.catalog_match.proposal_id;
}

function reviewForm(overrides: Record<string, string> = {}) {
  const form = new FormData();
  for (const [key, value] of Object.entries({
    decision: "approve_new",
    technology: "Yocto",
    description: "Embedded Linux build tooling",
    website: "https://www.yoctoproject.org/",
    category_ids: "1",
    saas: "false",
    oss: "true",
    pricing: "",
    reviewed_by: "Test administrator",
    review_note: "Checked the source and official project website",
    expected_review_id: "",
    ...overrides,
  }))
    form.set(key, value);
  return form;
}

describe("technology proposal intake", () => {
  it.each(["advertised_expertise", "develops", "offers"])(
    "preserves %s without promoting it to usage",
    async (signal) => {
      const input = structuredClone(fixture);
      input.records[0].data.signal = signal;
      input.records[0].data.scope = "company";
      await submitTechnologyProposals(input);
      expect(mocks.chInsertTechnologyProposals.mock.calls[0][0][0].signal).toBe(
        signal,
      );
    },
  );
  it.each(["missing", "rejected", "processing_failed"])(
    "blocks %s required metadata review while source evidence is accepted",
    async (status) => {
      const input = structuredClone(fixture);
      input.records[0].data.required_reviews = ["proposal_metadata"];
      if (status !== "missing")
        input.records[0].data.proposal_review = { status };
      await expect(submitTechnologyProposals(input)).rejects.toThrow(
        "Required technology reviews",
      );
      expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
    },
  );
  it("binds metadata approval to the exact proposal draft", async () => {
    const input = structuredClone(fixture);
    const data = input.records[0].data;
    data.required_reviews = ["proposal_metadata"];
    data.proposal_review = {
      status: "accepted",
      metadata_sha256: createHash("sha256")
        .update(
          JSON.stringify(
            Object.fromEntries(
              Object.entries(data.catalog_match.proposed_technology).sort(
                ([a], [b]) => (a < b ? -1 : a > b ? 1 : 0),
              ),
            ),
          ),
        )
        .digest("hex"),
    };
    await submitTechnologyProposals(input);
    mocks.chInsertTechnologyProposals.mockClear();
    data.catalog_match.proposed_technology.description = "Changed after review";
    await expect(submitTechnologyProposals(input)).rejects.toThrow(
      "Proposal metadata changed after review",
    );
    expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
  });

  it.each(["matched", "proposed"])(
    "requires verified sources for %s observations",
    async (status) => {
      const invalid = structuredClone(fixture);
      invalid.records[0].data.catalog_match.status = status;
      invalid.records[0].data.catalog_match.canonical_technology =
        status === "matched" ? catalog.entries[0].technology : null;
      invalid.records[0].sources[0].evidence_status = "needs_review";
      await expect(submitTechnologyProposals(invalid)).rejects.toThrow(
        "Source evidence and its verification status are required",
      );
      expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
    },
  );
  it.each(["claim", "interpretation", "catalog", "source"])(
    "rejects unaccepted %s evidence before inserting",
    async (failure) => {
      const invalid = structuredClone(fixture);
      const record = invalid.records[0];
      if (failure === "claim") record.evidence_status = "needs_review";
      if (failure === "interpretation")
        record.data.interpretation_review = { supported: false };
      if (failure === "catalog")
        record.data.catalog_error = "Resolution failed";
      if (failure === "source")
        record.sources[0].evidence_status = "needs_review";
      await expect(submitTechnologyProposals(invalid)).rejects.toThrow();
      expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
    },
  );
  it.each(["signal", "scope"])(
    "rejects an unknown %s before inserting",
    async (field) => {
      const invalid = structuredClone(fixture);
      invalid.records[0].data[field] = "invented";
      await expect(submitTechnologyProposals(invalid)).rejects.toThrow(
        "Invalid technology signal or scope",
      );
      expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
    },
  );
  it("accepts the Python crawler fixture and preserves evidence and nullable metadata", async () => {
    const result = await submitTechnologyProposals(fixture);
    expect(result).toMatchObject({ proposed: 1, matched: 0 });
    const row = mocks.chInsertTechnologyProposals.mock.calls[0][0][0];
    expect(row).toMatchObject({
      proposed_name: "Yocto",
      observed_name: "Yocto",
      company: "DemoWorks",
      saas: null,
      oss: null,
      website: "",
      signal: "stated_use",
    });
    expect(row.sources[0].evidence).toContain("Our team uses Yocto.");
    await submitTechnologyProposals(fixture);
    const retry = mocks.chInsertTechnologyProposals.mock.calls[1][0][0];
    expect([retry.proposal_id, retry.run_id, retry.record_id]).toEqual([
      row.proposal_id,
      row.run_id,
      row.record_id,
    ]);
  });

  it.each([
    "missing search",
    "invalid proposal ID",
    "missing sources",
    "unknown category",
    "missing category",
    "missing description",
    "unresolved status",
  ])("rejects %s before writing", async (kind) => {
    const input = structuredClone(fixture);
    const record = input.records[0],
      match = record.data.catalog_match;
    if (kind === "missing search") match.searches = [];
    if (kind === "invalid proposal ID") match.proposal_id = "made-up";
    if (kind === "missing sources") record.sources = [];
    if (kind === "unknown category")
      match.proposed_technology.category_ids = [65535];
    if (kind === "missing category") {
      match.proposed_technology.category_ids = [];
      match.proposed_technology.category_suggestion = null;
    }
    if (kind === "missing description")
      match.proposed_technology.description = "   ";
    if (kind === "unresolved status") match.status = "unresolved";
    await expect(submitTechnologyProposals(input)).rejects.toThrow();
    expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
  });

  it("validates the complete batch before any writes", async () => {
    const input = structuredClone(fixture);
    input.records.push(structuredClone(input.records[0]));
    await expect(submitTechnologyProposals(input)).rejects.toThrow(
      "Duplicate observation",
    );
    expect(mocks.chInsertTechnologyProposals).not.toHaveBeenCalled();
  });

  it("rechecks a stale crawler proposal against the current catalog", async () => {
    catalog.entries.push({ ...catalog.entries[0], technology: "Yocto" });
    const result = await submitTechnologyProposals(fixture);
    expect(result.proposals[0].current_canonical_technology).toBe("Yocto");
  });

  it("matches case and aliases without collapsing distinct punctuation or homonyms", () => {
    expect(resolveCatalogName("Git", catalog)).toBe("git");
    expect(resolveCatalogName("AWS", catalog)).toBe("Amazon Web Services");
    expect(resolveCatalogName("C++", catalog)).toBe("C++");
    expect(resolveCatalogName("C#", catalog)).toBe("C#");
    expect(resolveCatalogName("MOXIE", catalog)).toBeNull();
    expect(normalizeTechnologyName(" ＧＩＴ\u00a0")).toBe("git");
    expect(normalizeTechnologyName("Straße")).toBe("strasse");
  });
});

describe("administrator technology review", () => {
  it("stores reviewed catalog metadata and retains the proposal instead of deleting it", async () => {
    const id = await submittedProposal();
    const review = await reviewTechnologyProposal(id, reviewForm());
    expect(review).toMatchObject({
      proposal_id: id,
      decision: "approve_new",
      technology: "Yocto",
      category_ids: [1],
      categories: ["Development"],
      saas: 0,
      oss: 1,
    });
    expect(review.source_references).toContain("https://www.yoctoproject.org/");
    expect(mocks.chInsertTechnologyReview).toHaveBeenCalledWith(review);
  });

  it("maps an observation to an existing technology without creating a global alias automatically", async () => {
    const id = await submittedProposal();
    const review = await reviewTechnologyProposal(
      id,
      reviewForm({ decision: "map_existing", technology: "Python" }),
    );
    expect(review).toMatchObject({
      decision: "map_existing",
      technology: "Python",
      alias: "",
      alias_key: "",
    });
  });

  it("rejects a proposal with no canonical identity", async () => {
    const id = await submittedProposal();
    const review = await reviewTechnologyProposal(
      id,
      reviewForm({ decision: "reject", technology: "" }),
    );
    expect(review).toMatchObject({ decision: "reject", technology: "" });
  });

  it.each<Record<string, string>>([
    { technology: "GIT" },
    { category_ids: "65535" },
    { website: "javascript:alert(1)" },
    { reviewed_by: "" },
    { review_note: "" },
    { saas: "" },
    { alias: "Git" },
    { decision: "map_existing", technology: "Absent" },
  ])("rejects invalid review %j", async (change) => {
    const id = await submittedProposal();
    await expect(
      reviewTechnologyProposal(id, reviewForm(change)),
    ).rejects.toThrow();
    expect(mocks.chInsertTechnologyReview).not.toHaveBeenCalled();
  });

  it("requires reloading a review changed by another administrator", async () => {
    const id = await submittedProposal();
    reviews = [{ review_id: "new-review" }];
    await expect(reviewTechnologyProposal(id, reviewForm())).rejects.toThrow(
      "newer review",
    );
    expect(mocks.chInsertTechnologyReview).not.toHaveBeenCalled();
  });
});
