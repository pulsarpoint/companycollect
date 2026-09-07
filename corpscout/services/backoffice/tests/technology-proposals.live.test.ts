import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import {
  loadTechnologyProposal,
  reviewTechnologyProposal,
  submitTechnologyProposals,
} from "~/lib/technology-proposals.server";

// Invoked by the Dagster integration test, which owns the disposable database.
// A generic VITEST_LIVE opt-in alone must never write to a deployed ClickHouse.
it.runIf(process.env.TECHNOLOGY_PROPOSAL_ISOLATED_TEST === "1")(
  "submits, deduplicates, reviews and maps a crawler proposal in isolated ClickHouse",
  async () => {
    const url = new URL(process.env.CLICKHOUSE_URL!);
    expect(url.hostname).toBe("127.0.0.1");
    expect(process.env.CLICKHOUSE_PASSWORD).toBe("technology-test-password");
    const fixture = JSON.parse(
      readFileSync(
        resolve(
          "../../../codex-sd-examples/company_research/tests/fixtures/technology_submission.json",
        ),
        "utf8",
      ),
    );
    const first = await submitTechnologyProposals(fixture);
    await submitTechnologyProposals(fixture);
    const proposalId = first.proposals[0].proposal_id;
    const count = await chQuery<{ count: number }>(
      "SELECT toUInt32(count()) AS count FROM corpscout.new_tech FINAL WHERE proposal_id = {id:String}",
      { id: proposalId },
    );
    expect(count[0].count).toBe(1);
    const detail = await loadTechnologyProposal(proposalId);
    expect(detail.observations[0].sources[0].evidence).toContain(
      "Our team uses Yocto.",
    );
    const form = new FormData();
    for (const [key, value] of Object.entries({
      expected_review_id: "",
      decision: "approve_new",
      technology: "Yocto Project",
      description: "Embedded Linux build tools",
      website: "https://www.yoctoproject.org/",
      category_ids: "1",
      saas: "false",
      oss: "true",
      reviewed_by: "Integration test administrator",
      review_note: "Synthetic isolated review",
      alias: "Yocto",
    }))
      form.set(key, value);
    const approved = await reviewTechnologyProposal(proposalId, form);
    expect(approved.decision).toBe("approve_new");
    const mapping = await chQuery<{ technology: string }>(
      "SELECT technology FROM corpscout.technology_proposal_mappings WHERE proposal_id = {id:String}",
      { id: proposalId },
    );
    expect(mapping).toEqual([{ technology: "Yocto Project" }]);
    expect(
      (await loadTechnologyProposal(proposalId)).reviews[0].review_id,
    ).toBe(approved.review_id);
  },
  30000,
);
