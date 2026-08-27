import { describe, expect, it } from "vitest";
import {
  clampMinConfidence,
  DEFAULT_PERSON_LLM_PROFILE_NAME,
  MAX_CONFIDENCE,
  MERGE_LLM_PROFILES,
  MIN_CONFIDENCE,
  PERSON_LLM_PROFILES,
  personLlmProfile,
} from "~/lib/se-company-person-pipeline";

describe("PERSON_LLM_PROFILES", () => {
  it("hand-mirrors dagster_v3's PERSON_LLM_PROFILES (company_people/normalization.py) --", () => {
    // A SEPARATE registry from MERGE_LLM_PROFILES (deferred minor from Task 4's
    // report, still true here): today they happen to carry identical values, but
    // this pins the shape/default this page actually sends, not merge's.
    expect(personLlmProfile(DEFAULT_PERSON_LLM_PROFILE_NAME)).toEqual({
      name: "deepseek-default",
      provider: "deepseek",
      model: "deepseek-v4-flash",
      baseUrl: "https://api.deepseek.com",
    });
  });

  it("returns null for an unknown profile name", () => {
    expect(personLlmProfile("not-a-real-profile")).toBeNull();
  });

  it("is a separate list object from MERGE_LLM_PROFILES, even though values match today", () => {
    expect(PERSON_LLM_PROFILES).not.toBe(MERGE_LLM_PROFILES);
  });
});

describe("clampMinConfidence", () => {
  it("mirrors SECompanyPersonPromotionConfig.min_confidence (Field(ge=0, le=1))", () => {
    expect(MIN_CONFIDENCE).toBe(0);
    expect(MAX_CONFIDENCE).toBe(1);
    expect(clampMinConfidence(0.5)).toBe(0.5);
    expect(clampMinConfidence(-1)).toBe(0);
    expect(clampMinConfidence(2)).toBe(1);
  });

  it("keeps fractional precision -- unlike the integer clamps, it must not truncate", () => {
    expect(clampMinConfidence(0.73)).toBe(0.73);
  });

  it("falls back to the default for a non-finite value", () => {
    expect(clampMinConfidence(Number.NaN)).toBe(0);
  });
});
