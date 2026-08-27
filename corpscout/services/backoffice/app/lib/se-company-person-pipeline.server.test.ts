import { describe, expect, it } from "vitest";
import {
  SE_COMPANY_PERSON_ASSET,
  SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET,
  SE_COMPANY_PERSON_PROMOTION_ASSET,
  SE_COMPANY_PERSON_ROLE_ASSET,
  SE_COMPANY_PERSON_ROLE_DRAFT_ASSET,
} from "~/lib/dagster.server";
import {
  buildCleanCopyRunConfig,
  buildLlmSuggestionsRunConfig,
  buildPromotionRunConfig,
  buildSimpleSyncRunConfig,
} from "~/lib/se-company-person-pipeline.server";

/**
 * The three-asset resolution split (dagster_v3 company_people/normalization.py's
 * module docstring): se_company_person_clickhouse (clean copy, single-source, no
 * LLM config), se_company_person_llm_suggestions (multi-source, writes suggestions
 * ONLY, execute-gated like the merge job), se_company_person_promotion
 * (deterministic, min_confidence-gated, no execute gate). Each config builder here
 * is a PORT of the matching dagster_v3 Config class -- op key and field names must
 * match exactly.
 */

describe("buildCleanCopyRunConfig", () => {
  it("sends exactly company_ids/max_companies/company_batch_size -- no LLM fields at all", () => {
    const config = buildCleanCopyRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
    });

    expect(config).toEqual({
      ops: {
        [SE_COMPANY_PERSON_ASSET]: {
          config: {
            company_ids: [],
            max_companies: 10_000,
            company_batch_size: 5_000,
          },
        },
      },
    });
  });

  it("never carries execute, llm_profile, or any other LLM-shaped key", () => {
    const config = buildCleanCopyRunConfig({
      companyIds: ["5560125220"],
      maxCompanies: 100,
      companyBatchSize: 50,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };
    const opConfig = config.ops[SE_COMPANY_PERSON_ASSET].config;

    expect(Object.keys(opConfig).sort()).toEqual([
      "company_batch_size",
      "company_ids",
      "max_companies",
    ]);
    expect(opConfig).not.toHaveProperty("execute");
    expect(opConfig).not.toHaveProperty("llm_profile");
    expect(opConfig).not.toHaveProperty("maximum_observations_per_request");
    expect(opConfig).not.toHaveProperty("timeout_seconds");
  });

  it("normalizes the company scope the way every other builder here does", () => {
    const config = buildCleanCopyRunConfig({
      companyIds: [" 5560125220 ", "5565200028", "5560125220", ""],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
    }) as { ops: Record<string, { config: { company_ids: string[] } }> };

    expect(config.ops[SE_COMPANY_PERSON_ASSET].config.company_ids).toEqual([
      "5560125220",
      "5565200028",
    ]);
  });
});

describe("buildSimpleSyncRunConfig", () => {
  it("selects all three ops of se_company_person_job -- role_draft, person, role", () => {
    const config = buildSimpleSyncRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };

    expect(Object.keys(config.ops).sort()).toEqual(
      [SE_COMPANY_PERSON_ROLE_DRAFT_ASSET, SE_COMPANY_PERSON_ASSET, SE_COMPANY_PERSON_ROLE_ASSET].sort(),
    );
  });

  it("gives the person op the full clean-copy config, exactly like buildCleanCopyRunConfig's", () => {
    const config = buildSimpleSyncRunConfig({
      companyIds: ["5560125220"],
      maxCompanies: 100,
      companyBatchSize: 50,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };

    expect(config.ops[SE_COMPANY_PERSON_ASSET].config).toEqual({
      company_ids: ["5560125220"],
      max_companies: 100,
      company_batch_size: 50,
    });
  });

  it("gives both role ops ONLY company_ids -- SECompanyPersonRoleConfig has no max_companies/company_batch_size field", () => {
    const config = buildSimpleSyncRunConfig({
      companyIds: ["5560125220"],
      maxCompanies: 100,
      companyBatchSize: 50,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };

    expect(config.ops[SE_COMPANY_PERSON_ROLE_DRAFT_ASSET].config).toEqual({
      company_ids: ["5560125220"],
    });
    expect(config.ops[SE_COMPANY_PERSON_ROLE_ASSET].config).toEqual({
      company_ids: ["5560125220"],
    });
  });

  it("normalizes and shares the SAME company scope across all three ops", () => {
    const config = buildSimpleSyncRunConfig({
      companyIds: [" 5560125220 ", "5565200028", "5560125220", ""],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
    }) as { ops: Record<string, { config: { company_ids: string[] } }> };

    const expected = ["5560125220", "5565200028"];
    expect(config.ops[SE_COMPANY_PERSON_ROLE_DRAFT_ASSET].config.company_ids).toEqual(expected);
    expect(config.ops[SE_COMPANY_PERSON_ASSET].config.company_ids).toEqual(expected);
    expect(config.ops[SE_COMPANY_PERSON_ROLE_ASSET].config.company_ids).toEqual(expected);
  });
});

describe("buildLlmSuggestionsRunConfig", () => {
  it("sends execute and llm_profile alongside the full numeric config, one op key", () => {
    const config = buildLlmSuggestionsRunConfig({
      execute: true,
      llmProfile: "deepseek-default",
      companyIds: ["5560125220"],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      maximumObservationsPerRequest: 50,
      timeoutSeconds: 180,
    });

    expect(config).toEqual({
      ops: {
        [SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET]: {
          config: {
            execute: true,
            llm_profile: "deepseek-default",
            company_ids: ["5560125220"],
            max_companies: 10_000,
            company_batch_size: 5_000,
            maximum_observations_per_request: 50,
            timeout_seconds: 180,
          },
        },
      },
    });
  });

  it("defaults execute to whatever the caller passes -- false stays false (preview)", () => {
    const config = buildLlmSuggestionsRunConfig({
      execute: false,
      llmProfile: "deepseek-default",
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      maximumObservationsPerRequest: 50,
      timeoutSeconds: 180,
    }) as { ops: Record<string, { config: { execute: boolean } }> };

    expect(config.ops[SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET].config.execute).toBe(false);
  });

  it("never touches se_company_person_clickhouse's or the promotion asset's op key", () => {
    const config = buildLlmSuggestionsRunConfig({
      execute: true,
      llmProfile: "deepseek-default",
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      maximumObservationsPerRequest: 50,
      timeoutSeconds: 180,
    }) as { ops: Record<string, unknown> };

    expect(Object.keys(config.ops)).toEqual([SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET]);
    expect(config.ops).not.toHaveProperty(SE_COMPANY_PERSON_ASSET);
    expect(config.ops).not.toHaveProperty(SE_COMPANY_PERSON_PROMOTION_ASSET);
  });
});

describe("buildPromotionRunConfig", () => {
  it("sends min_confidence alongside the scope/bounds -- no LLM fields, no execute", () => {
    const config = buildPromotionRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      minConfidence: 0.5,
    });

    expect(config).toEqual({
      ops: {
        [SE_COMPANY_PERSON_PROMOTION_ASSET]: {
          config: {
            company_ids: [],
            max_companies: 10_000,
            company_batch_size: 5_000,
            min_confidence: 0.5,
          },
        },
      },
    });
  });

  it("clamps min_confidence into [0, 1] rather than rejecting an out-of-range value", () => {
    const tooHigh = buildPromotionRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      minConfidence: 5,
    }) as { ops: Record<string, { config: { min_confidence: number } }> };
    const tooLow = buildPromotionRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      minConfidence: -1,
    }) as { ops: Record<string, { config: { min_confidence: number } }> };

    expect(tooHigh.ops[SE_COMPANY_PERSON_PROMOTION_ASSET].config.min_confidence).toBe(1);
    expect(tooLow.ops[SE_COMPANY_PERSON_PROMOTION_ASSET].config.min_confidence).toBe(0);
  });

  it("never carries execute or llm_profile -- promotion calls no model", () => {
    const config = buildPromotionRunConfig({
      companyIds: [],
      maxCompanies: 10_000,
      companyBatchSize: 5_000,
      minConfidence: 0,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };
    const opConfig = config.ops[SE_COMPANY_PERSON_PROMOTION_ASSET].config;

    expect(opConfig).not.toHaveProperty("execute");
    expect(opConfig).not.toHaveProperty("llm_profile");
  });
});
