import { describe, expect, it, vi } from "vitest";
import {
  buildArtifactRunConfig,
  buildInfoRunConfig,
  DEFAULT_MAX_TOKENS,
  DEFAULT_PROMPT_VERSION,
  INFO_ARTIFACT_FRESHNESS_SQL,
  INFO_ASSET,
  INFO_OBSERVATION_AVERAGES_SQL,
  INFO_SELECTION_COUNTS_SQL,
  infoArtifactAsset,
  loadSeCompanyInfoPipelineStats,
} from "~/lib/se-company-info-pipeline.server";

const PROFILE = {
  provider: "deepseek",
  model: "deepseek-v4-flash",
  baseUrl: "https://api.deepseek.com",
  concurrency: 4,
};

const EPOCH = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')";
const PUBLISHED_AT = `ifNull(published.resolved_at, ${EPOCH})`;

describe("the selection query", () => {
  it("ports Dagster's change scan predicate for predicate", () => {
    const sql = INFO_SELECTION_COUNTS_SQL;
    // The three CTEs of build_changed_companies_sql, and the final read FINAL --
    // it is keyed by company_id with a new row per resolution.
    expect(sql).toContain("FROM corpscout.se_company_info AS final FINAL");
    for (const table of [
      "se_company_info_scb",
      "se_company_info_esef",
      "se_company_info_wikidata",
    ]) {
      expect(sql).toContain(`FROM corpscout.${table} GROUP BY company_id`);
    }
    expect(sql).toContain("FROM corpscout.se_company_info_correction");

    // Never published, and every LEFT JOIN miss read through ifNull: a bare
    // comparison is NULL under join_use_nulls = 1, which would silently count 0.
    expect(sql).toContain("ifNull(published.company_id, '') = '' AS never_published");
    expect(sql).toContain(
      `ifNull(ledger.latest_correction_at, ${EPOCH}) > ${PUBLISHED_AT} AS ledger_pending`,
    );
    expect(sql).not.toContain("> published.resolved_at");
    expect(sql).not.toContain("published.description_source_count > 1");

    // Per-source freshness, as maxIf over the union's own source column. NOT
    // named latest_observed_at: that collides with an outer aggregate alias and
    // ClickHouse 26.5 rejects the query (ILLEGAL_AGGREGATION).
    for (const source of ["scb", "esef", "wikidata"]) {
      expect(sql).toContain(
        `maxIf(source_observed_at, source = '${source}') AS ${source}_observed_at`,
      );
      expect(sql).toContain(
        `artifacts.${source}_observed_at > ${PUBLISHED_AT} AS new_evidence_${source}`,
      );
    }

    // "Still owed a description", keyed on the applied-correction list -- a
    // reject_suggestion leaves description_source at the deterministic source,
    // so only correction_ids tells a reviewed company from a never-modelled one.
    expect(sql).toContain(
      "(ifNull(published.description_source_count, 0) > 1 AND published.suggestion_id IS NULL AND length(published.correction_ids) = 0) AS pending_model",
    );
    expect(sql).not.toContain("ifNull(published.correction_ids");
  });

  it("counts the same three selections the page's three actions launch", () => {
    const sql = INFO_SELECTION_COUNTS_SQL;
    const changed =
      "never_published OR new_evidence_scb OR new_evidence_esef OR new_evidence_wikidata OR ledger_pending";
    // A model-on resolve run also picks up whatever the model still owes
    // (info.py's include_pending term); a model-off one does not.
    expect(sql).toContain(`toString(countIf(${changed} OR pending_model)) AS changed_count`);
    expect(sql).toContain(`toString(countIf(${changed})) AS changed_without_model_count`);
    // The model pass selects exactly the pending companies through its own branch.
    expect(sql).toContain("toString(countIf(pending_model)) AS pending_model_count");
    // ... and the cost forecast is the multi-source ones among the selection.
    expect(sql).toContain(
      `toString(countIf((${changed} OR pending_model) AND multi_source)) AS would_call_model_count`,
    );
    // Counts cross the wire as strings: a UInt64 over 2^53 loses precision as a
    // JSON number, exactly as the other admin list pages read their totals.
    expect(sql.match(/toString\(countIf\(/g)).toHaveLength(9);
    expect(sql).toContain("toString(count()) AS company_count");
    expect(sql).toContain("FROM selection");
  });

  it("reads artifact freshness and the observed cost per model", () => {
    expect(INFO_ARTIFACT_FRESHNESS_SQL).toContain(
      "toString(max(observed_at)) AS latest_observed_at",
    );
    expect(INFO_ARTIFACT_FRESHNESS_SQL.match(/UNION ALL/g)).toHaveLength(2);
    // Rows stored without usage numbers would drag the averages towards zero.
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("WHERE prompt_tokens > 0");
    // An aggregate aliased to its own source column makes the WHERE bind to the
    // aggregate: ClickHouse 26.5 answers ILLEGAL_AGGREGATION and the page 500s.
    // Nothing here can execute SQL, so the alias is pinned by name instead.
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("AS avg_prompt_tokens");
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("AS avg_completion_tokens");
    expect(INFO_OBSERVATION_AVERAGES_SQL).not.toMatch(/\) AS prompt_tokens/);
    expect(INFO_OBSERVATION_AVERAGES_SQL).not.toMatch(/\) AS completion_tokens/);
    expect(INFO_OBSERVATION_AVERAGES_SQL).toContain("GROUP BY model_name");
  });
});

describe("loadSeCompanyInfoPipelineStats", () => {
  it("shapes the three reads into the numbers the page renders", async () => {
    const queryImpl = vi.fn(async (sql: string) => {
      if (sql.startsWith("WITH artifacts")) {
        return [
          {
            company_count: "3500000",
            changed_count: "1240",
            changed_without_model_count: "900",
            would_call_model_count: "340",
            never_published_count: "12",
            new_evidence_scb_count: "800",
            new_evidence_esef_count: "40",
            new_evidence_wikidata_count: "60",
            ledger_pending_count: "5",
            pending_model_count: "410",
          },
        ];
      }
      if (sql.startsWith("SELECT 'scb' AS source")) {
        return [
          { source: "scb", latest_observed_at: "2026-08-22 03:00:00.000", row_count: "7000000" },
        ];
      }
      return [
        {
          model_name: "deepseek-v4-flash",
          call_count: "1200",
          avg_prompt_tokens: "640",
          avg_completion_tokens: "240",
          latest_created_at: "2026-08-22 09:00:00.000",
        },
      ];
    });

    const stats = await loadSeCompanyInfoPipelineStats({ queryImpl });

    expect(queryImpl).toHaveBeenCalledTimes(3);
    expect(stats.selection).toEqual({
      companyCount: 3_500_000,
      changedCount: 1_240,
      changedWithoutModelCount: 900,
      wouldCallModelCount: 340,
      neverPublishedCount: 12,
      newEvidenceCounts: { scb: 800, esef: 40, wikidata: 60 },
      ledgerPendingCount: 5,
      pendingModelCount: 410,
    });
    expect(stats.artifacts).toEqual([
      { source: "scb", latestObservedAt: "2026-08-22 03:00:00.000", rowCount: 7_000_000 },
    ]);
    expect(stats.models[0].promptTokens).toBe(640);
  });

  it("reads zeros rather than NaN when nothing has been published yet", async () => {
    const queryImpl = vi.fn(async () => []);
    const stats = await loadSeCompanyInfoPipelineStats({ queryImpl });
    expect(stats.selection.changedCount).toBe(0);
    expect(stats.selection.newEvidenceCounts.esef).toBe(0);
    expect(stats.artifacts).toEqual([]);
    expect(stats.models).toEqual([]);
  });
});

describe("buildInfoRunConfig", () => {
  it("always sends execute: true and the whole model profile", () => {
    expect(buildInfoRunConfig({ maxCompanies: 1000, useModel: true, llm: PROFILE })).toEqual({
      ops: {
        [INFO_ASSET]: {
          config: {
            execute: true,
            max_companies: 1000,
            resolve_multi_source_with_llm: true,
            pending_model_only: false,
            llm: {
              provider: "deepseek",
              model: "deepseek-v4-flash",
              base_url: "https://api.deepseek.com",
              temperature: 0,
              max_tokens: DEFAULT_MAX_TOKENS,
              prompt_version: DEFAULT_PROMPT_VERSION,
              concurrency: 4,
            },
          },
        },
      },
    });
  });

  it("never carries a credential of any kind", () => {
    // The whole point of the profile split: the host resolves the key by
    // provider name, so nothing key-shaped may appear in a run config that
    // Dagster stores and shows in its UI forever.
    const config = buildInfoRunConfig({
      maxCompanies: 10,
      useModel: true,
      llm: PROFILE,
    }) as { ops: Record<string, { config: { llm: Record<string, unknown> } }> };
    // An exact key set, not a search for suspicious words: adding a field to the
    // llm block has to be a deliberate change to this list.
    expect(Object.keys(config.ops[INFO_ASSET].config.llm).sort()).toEqual([
      "base_url",
      "concurrency",
      "max_tokens",
      "model",
      "prompt_version",
      "provider",
      "temperature",
    ]);
    const serialized = JSON.stringify(config).toLowerCase();
    expect(serialized).not.toContain("api_key");
    expect(serialized).not.toContain("apikey");
    expect(serialized).not.toContain("secret");
  });

  it("spells the model pass and the model-off run the way the asset expects", () => {
    const modelPass = buildInfoRunConfig({
      maxCompanies: 500,
      useModel: true,
      pendingModelOnly: true,
      llm: PROFILE,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };
    expect(modelPass.ops[INFO_ASSET].config.pending_model_only).toBe(true);
    expect(modelPass.ops[INFO_ASSET].config.resolve_multi_source_with_llm).toBe(true);

    const modelOff = buildInfoRunConfig({
      maxCompanies: 500,
      useModel: false,
      llm: PROFILE,
    }) as { ops: Record<string, { config: Record<string, unknown> }> };
    expect(modelOff.ops[INFO_ASSET].config.resolve_multi_source_with_llm).toBe(false);
    expect(modelOff.ops[INFO_ASSET].config.pending_model_only).toBe(false);
  });

  it("clamps the concurrency it is handed rather than passing it through", () => {
    const config = buildInfoRunConfig({
      maxCompanies: 10,
      useModel: true,
      llm: { ...PROFILE, concurrency: 99 },
    }) as { ops: Record<string, { config: { llm: { concurrency: number } } }> };
    expect(config.ops[INFO_ASSET].config.llm.concurrency).toBe(8);
  });
});

describe("buildArtifactRunConfig", () => {
  it("sends no config at all", () => {
    // An ops entry for an asset outside the run's selection is a config error,
    // and the artifact assets take no config of their own.
    expect(buildArtifactRunConfig()).toEqual({});
    expect(infoArtifactAsset("scb")).toBe("se_company_info_scb_clickhouse");
    expect(infoArtifactAsset("wikidata")).toBe("se_company_info_wikidata_clickhouse");
  });
});
