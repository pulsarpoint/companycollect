import { describe, expect, it } from "vitest";
import {
  clampConcurrency,
  dagsterApiKeyVariable,
  INFO_ARTIFACT_SOURCES,
  isInfoArtifact,
  MAX_CONCURRENCY,
  MIN_CONCURRENCY,
} from "~/lib/se-company-info-pipeline";

describe("the client-safe pipeline helpers", () => {
  it("clamps concurrency to the range the asset accepts", () => {
    // LlmProfileConfig.concurrency is ge=1, le=8: a value outside that range is
    // a config error on the Dagster side, so the form never submits one.
    expect(clampConcurrency(0)).toBe(MIN_CONCURRENCY);
    expect(clampConcurrency(99)).toBe(MAX_CONCURRENCY);
    expect(clampConcurrency(Number.NaN)).toBe(MIN_CONCURRENCY);
    expect(clampConcurrency(3.9)).toBe(3);
  });

  it("derives the host variable the key is read from, and never the key", () => {
    expect(dagsterApiKeyVariable("deepseek")).toBe("DEEPSEEK_API_KEY");
    expect(dagsterApiKeyVariable("openai")).toBe("OPENAI_API_KEY");
  });

  it("offers exactly the three artifacts and rejects anything else", () => {
    expect([...INFO_ARTIFACT_SOURCES]).toEqual(["scb", "esef", "wikidata"]);
    expect(isInfoArtifact("esef")).toBe(true);
    expect(isInfoArtifact("se_company_info")).toBe(false);
    expect(isInfoArtifact("")).toBe(false);
  });

  it("carries no Dagster asset name into the client bundle", () => {
    // The route component imports this module, so anything named here ships to
    // the browser. Asset names (and the ClickHouse reads behind them) belong in
    // se-company-info-pipeline.server.ts.
    expect(INFO_ARTIFACT_SOURCES.join(" ")).not.toContain("clickhouse");
  });
});
