import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyInfoFieldValues: clickhouse.insert,
  chQuery: clickhouse.query,
  chCommand: vi.fn(),
}));

const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);

const resolveModule = vi.hoisted(() => ({ resolveCompanyFields: vi.fn() }));
vi.mock("~/lib/se-company-field-resolve.server", async (importOriginal) => ({
  ...(await importOriginal<
    typeof import("~/lib/se-company-field-resolve.server")
  >()),
  resolveCompanyFields: resolveModule.resolveCompanyFields,
}));

import {
  ARTIFACT_ROWS_SQL,
  appendSeCompanyInfoFieldValues,
  FIELD_VALUES_SQL,
  INFO_SQL,
  loadSeCompanyInfoDetail,
  NACE_LABEL_SQL,
  SUGGESTIONS_SQL,
  type SeCompanyInfoFieldValueRow,
} from "~/lib/se-company-info.server";
import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";
import {
  ARTIFACT_PAYLOAD_FIELDS,
  type ArtifactSource,
} from "~/lib/se-company-info-payload";
import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const COMPANY = "5565200028";

/**
 * A reviewer's release row as FIELD_VALUES_SQL returns it. `source_at` is the
 * one Nullable column the query wraps in toString(), and toString() over a
 * NULL still yields JS null -- for exactly the rows
 * appendSeCompanyInfoFieldValues writes when a reviewer decides the text
 * themselves. Typed here so the row interface has to admit it.
 */
const RELEASED_VALUE_ROW: SeCompanyInfoFieldValueRow = {
  value_id: "33333333-3333-4333-8333-333333333333",
  field: "description_sv",
  value: null,
  source: "reviewer",
  source_ref: "",
  source_at: null,
  decided_by: "backoffice",
  note: "SCB's Swedish copy was boilerplate",
  created_at: "2026-09-01 09:00:00.000",
  is_live: 1,
};

/**
 * The payload columns of each artifact table, straight from the DDL
 * (migration 000297, 000300's activity_description_en, 000306's two
 * legal-form labels and 000365's three ESEF enrichment columns) minus the
 * envelope
 * (company_id, source_record_uid, observed_at, source_run_id) and the
 * MATERIALIZED evidence_hash. Hand-copied here on purpose: it is the
 * independent statement of the schema that both the display list and the
 * query are checked against, so a column the migrations add can never stay
 * invisible on the review page.
 */
const DDL_PAYLOAD_COLUMNS: Record<ArtifactSource, string[]> = {
  scb: [
    "legal_name",
    "legal_name_raw",
    "legal_form_code",
    "legal_form_label_en",
    "legal_form_label_sv",
    "status",
    "incorporation_date",
    "dissolution_date",
    "activity_description",
    "activity_description_en",
    "primary_sni_code",
    "primary_nace_code",
  ],
  esef: [
    "source_document_id",
    "lei",
    "entity_name",
    "fiscal_year",
    "company_description",
    "description_language",
    "description_confidence",
    "products_and_services_json",
    "customer_markets_json",
    "operating_geographies_json",
    "business_segments_json",
    "material_group_relationships_json",
  ],
  wikidata: [
    "wikidata_id",
    "wikidata_url",
    "name",
    "official_name",
    "company_description",
    "inception_date",
    "legal_form_label",
    "industry_wikidata_id",
    "industry_label",
    "headquarters_label",
    "employee_count",
  ],
};

describe("company info queries", () => {
  it("qualify WHERE columns and expose provenance", () => {
    expect(INFO_SQL).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(INFO_SQL).toContain("WHERE i.company_id = {companyId:String}");
    for (const c of [
      "toString(i.evidence_set_hash) AS evidence_set_hash",
      "i.correction_ids",
      "toString(i.suggestion_id) AS suggestion_id",
      // Task 17: the Bool llm_enhanced replaces the description_source label,
      // cast to UInt8 so JSONEachRow yields one predictable 0/1 shape.
      "toUInt8(i.llm_enhanced) AS llm_enhanced",
      // Task 14: the published row holds both languages natively (migration 000301).
      "i.description AS description",
      "i.description_sv AS description_sv",
      // Task 19: what legal_form_code is CALLED, copied from the curated
      // dictionary by Dagster (migration 000306). Plain String columns -- an
      // absent label is '', which is what the artifact's join miss wrote.
      "i.legal_form_label_en AS legal_form_label_en",
      "i.legal_form_label_sv AS legal_form_label_sv",
    ]) {
      expect(INFO_SQL).toContain(c);
    }
    expect(INFO_SQL).not.toContain("description_source AS");
    expect(ARTIFACT_ROWS_SQL).toContain("'scb' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'esef' AS source");
    expect(ARTIFACT_ROWS_SQL).toContain("'wikidata' AS source");
    // Each artifact leg reads FINAL -- pin the exact FROM clause per leg, not
    // just the source literal, so a leg silently losing FINAL still fails.
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_scb AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_esef AS a FINAL");
    expect(ARTIFACT_ROWS_SQL).toContain("FROM corpscout.se_company_info_wikidata AS a FINAL");
    // Task 16: the review page is the hub, so each leg carries its FULL payload
    // as one JSON map instead of a single pre-picked summary column.
    expect(ARTIFACT_ROWS_SQL).not.toContain("AS summary");
    // A trailing ORDER BY only binds to the last SELECT of a UNION ALL chain
    // in ClickHouse, so the union must be wrapped and the ORDER BY placed
    // outside it -- pin the closing paren immediately followed by the ORDER
    // BY. The source_record_uid tiebreak (bulk-loaded SCB rows share one
    // observed_at) and the LIMIT (ESEF grows per filing; the other three
    // queries are bounded by construction already) are pinned separately.
    expect(ARTIFACT_ROWS_SQL).toContain(")\nORDER BY source, observed_at DESC");
    expect(ARTIFACT_ROWS_SQL).toContain(", source_record_uid");
    expect(ARTIFACT_ROWS_SQL).toContain("LIMIT 500");
    expect(SUGGESTIONS_SQL).toContain("FROM corpscout.se_company_info_enrichment_observation AS s");
    // Pin the full expression including its alias for both suggestion flags,
    // so a swap between is_published and is_newest fails: P16 ruling made
    // them two independent flags, not a single is_current.
    expect(SUGGESTIONS_SQL).toContain(
      "toUInt8(ifNull(s.suggestion_id = {publishedSuggestionId:Nullable(UUID)}, 0)) AS is_published",
    );
    expect(SUGGESTIONS_SQL).toContain(
      `toUInt8(s.suggestion_id = (
    SELECT suggestion_id
    FROM corpscout.se_company_info_enrichment_observation
    WHERE company_id = {companyId:String}
    ORDER BY created_at DESC, suggestion_id DESC
    LIMIT 1
  )) AS is_newest`,
    );
    // The live row per field is the LATEST one, and the tie-break is on the
    // uuid's TEXT -- Dagster's apply_field_values picks with
    // `max((created_at, str(value_id)))`, so a ClickHouse argMax over the UUID
    // itself would order the tie by the uuid's bytes and the two layers could
    // disagree about which row is live.
    expect(FIELD_VALUES_SQL).toContain(
      "FROM corpscout.se_company_info_field_value AS v",
    );
    expect(FIELD_VALUES_SQL).toContain(
      "argMax(value_id, (created_at, toString(value_id))) AS value_id",
    );
    expect(FIELD_VALUES_SQL).not.toContain("argMax(value_id, (created_at, value_id))");
    expect(FIELD_VALUES_SQL).toContain("toUInt8(v.value_id = live.value_id) AS is_live");
    expect(FIELD_VALUES_SQL).toContain("ORDER BY v.created_at DESC, v.value_id DESC");
    // The store is the whole history for this company: no ledger vocabulary
    // (kinds, evidence hashes, supersession) survives in it.
    expect(FIELD_VALUES_SQL).not.toContain("evidence");
    expect(FIELD_VALUES_SQL).not.toContain("supersedes");
  });

  it("projects every artifact leg with the same alias order, envelope first then payload_json", () => {
    // UNION ALL matches legs positionally, so every leg must project the same
    // five aliases in the same order -- pin the order, not just the presence.
    // Select-list aliases only: each ends the line, unlike the `AS a FINAL`
    // table alias every leg's FROM carries.
    const aliases = [...ARTIFACT_ROWS_SQL.matchAll(/AS (\w+)(?=,|\n)/g)].map((m) => m[1]);
    expect(aliases).toEqual([
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
      "source",
      "source_record_uid",
      "observed_at",
      "evidence_hash",
      "payload_json",
    ]);
  });

  it("carries EVERY payload column of every artifact table in its toJSONString(map(...))", () => {
    for (const [source, columns] of Object.entries(DDL_PAYLOAD_COLUMNS)) {
      // The display list is the query's own column list, so a column missing
      // from one is missing from both -- check it against the DDL directly.
      const fields = ARTIFACT_PAYLOAD_FIELDS[source as ArtifactSource];
      expect([...fields.map((field) => field.key)].sort()).toEqual([...columns].sort());
      for (const column of columns) {
        // The Dagster build_artifact_rows_sql convention: the cast is INSIDE
        // ifNull (ClickHouse has no common type for a Date/number and '').
        expect(ARTIFACT_ROWS_SQL).toContain(`'${column}', ifNull(toString(a.${column}), '')`);
      }
    }
    expect(ARTIFACT_ROWS_SQL.match(/toJSONString\(map\(/g)).toHaveLength(3);
  });
});

describe("appendSeCompanyInfoFieldValues", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
    registryModule.loadFieldRegistry.mockReset();
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);
    resolveModule.resolveCompanyFields.mockReset();
    resolveModule.resolveCompanyFields.mockResolvedValue({
      resolved: [],
      skipped: [],
    });
  });

  const scbValue = {
    companyId: COMPANY,
    field: "description",
    value: "Alpha builds payment software.",
    source: "scb",
    sourceRef: "scb:1",
    sourceAt: "2026-08-01 00:00:00.000",
  };

  it("refuses when the company is not published, without writing", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    await expect(appendSeCompanyInfoFieldValues([scbValue])).rejects.toThrow(
      "This company is not published.",
    );
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  // Validation runs over the WHOLE batch before anything is read or written:
  // one bad row must not leave the good half of an About-card decision behind
  // in the store.
  it("validates every input before touching ClickHouse", async () => {
    await expect(
      appendSeCompanyInfoFieldValues([
        scbValue,
        { ...scbValue, field: "not_a_field" },
      ]),
    ).rejects.toThrow(SeInfoFieldValueValidationError);
    expect(clickhouse.query).not.toHaveBeenCalled();
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("validates against the registry it is handed, without loading one", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);

    await appendSeCompanyInfoFieldValues(
      [{ ...scbValue, field: "legal_name", value: "Alpha AB" }],
      { registry: REGISTRY_FIXTURE },
    );

    expect(registryModule.loadFieldRegistry).not.toHaveBeenCalled();
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
  });

  it("loads the registry when none is handed in, and refuses a source it does not list", async () => {
    await expect(
      appendSeCompanyInfoFieldValues([{ ...scbValue, source: "ratsit" }]),
    ).rejects.toThrow("Unknown source.");
    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(1);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("appends every row in one insert, with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);

    const { valueIds, resolved, skipped } = await appendSeCompanyInfoFieldValues([
      scbValue,
      // A null value is the release instruction: it hands the Swedish text
      // back to whatever the pipeline computes.
      {
        companyId: COMPANY,
        field: "description_sv",
        value: null,
        source: "reviewer",
        note: "SCB's Swedish copy was boilerplate",
      },
    ]);

    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [publishedSql, publishedParams] = clickhouse.query.mock.calls[0];
    expect(publishedSql).toContain("FROM corpscout.se_company_info FINAL");
    expect(publishedParams).toEqual({ companyId: COMPANY });
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(valueIds).toHaveLength(2);
    expect(rows[0]).toEqual({
      value_id: valueIds[0],
      company_id: COMPANY,
      field: "description",
      value: "Alpha builds payment software.",
      source: "scb",
      source_ref: "scb:1",
      source_at: "2026-08-01 00:00:00.000",
      decided_by: "backoffice",
      note: "",
      created_at: rows[0].created_at,
    });
    expect(rows[1]).toMatchObject({
      value_id: valueIds[1],
      field: "description_sv",
      value: null,
      source: "reviewer",
      // A reviewer's own wording comes from no record.
      source_ref: "",
      source_at: null,
      decided_by: "backoffice",
      note: "SCB's Swedish copy was boilerplate",
    });
    for (const row of rows) {
      expect(row.created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
    }
    // One resolve call for the whole decision, in the order decided, against
    // the registry the store validated with, at the same instant as created_at
    // -- so resolved_at and created_at read alike on the two tables.
    expect(resolveModule.resolveCompanyFields).toHaveBeenCalledTimes(1);
    const [companyId, fields, opts] =
      resolveModule.resolveCompanyFields.mock.calls[0];
    expect(companyId).toBe(COMPANY);
    expect(fields).toEqual(["description", "description_sv"]);
    expect(opts.registry).toBe(REGISTRY_FIXTURE);
    expect(opts.now.toISOString().replace("T", " ").replace("Z", "")).toBe(
      rows[0].created_at,
    );
    expect(resolved).toEqual([]);
    expect(skipped).toEqual([]);
  });

  it("returns what the resolver resolved and skipped", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    resolveModule.resolveCompanyFields.mockResolvedValue({
      resolved: ["description"],
      skipped: [{ field: "website", reason: "python_only" }],
    });

    const result = await appendSeCompanyInfoFieldValues(
      [
        scbValue,
        {
          companyId: COMPANY,
          field: "website",
          value: "https://alpha.example",
          source: "reviewer",
        },
      ],
      { registry: REGISTRY_FIXTURE },
    );

    expect(result.valueIds).toHaveLength(2);
    expect(result.resolved).toEqual(["description"]);
    expect(result.skipped).toEqual([{ field: "website", reason: "python_only" }]);
  });

  it("uses the caller's clock for created_at and resolved_at", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    const now = new Date("2026-09-02T10:15:30.123Z");

    await appendSeCompanyInfoFieldValues([scbValue], {
      registry: REGISTRY_FIXTURE,
      now,
    });

    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows[0].created_at).toBe("2026-09-02 10:15:30.123");
    expect(resolveModule.resolveCompanyFields.mock.calls[0][2].now).toBe(now);
  });

  // The rows are already in the store when resolving fails; the sensor picks
  // them up. The failure must say so and carry the ids -- not read as "the
  // decision was refused", and not silently succeed.
  it("keeps the decision and raises SeCompanyFieldResolveError when resolving fails after the insert", async () => {
    clickhouse.query.mockResolvedValueOnce([{ "1": 1 }]);
    clickhouse.insert.mockResolvedValue(undefined);
    resolveModule.resolveCompanyFields.mockRejectedValue(
      new Error("Code: 241. DB::Exception: Memory limit"),
    );

    const error: unknown = await appendSeCompanyInfoFieldValues([scbValue], {
      registry: REGISTRY_FIXTURE,
    }).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(SeCompanyFieldResolveError);
    const resolveError = error as SeCompanyFieldResolveError;
    expect(resolveError.message).toBe(
      "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
    );
    expect(clickhouse.insert).toHaveBeenCalledTimes(1);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(resolveError.valueIds).toEqual([rows[0].value_id]);
  });

  it("refuses an empty batch", async () => {
    await expect(appendSeCompanyInfoFieldValues([])).rejects.toThrow(
      SeInfoFieldValueValidationError,
    );
    expect(clickhouse.query).not.toHaveBeenCalled();
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  // Every row of a batch shares one created_at, so two rows for the SAME field
  // would tie there and the winner would fall to the uuid text -- a coin flip,
  // not a decision. The batch is refused instead.
  it("refuses a batch that decides one field twice", async () => {
    await expect(
      appendSeCompanyInfoFieldValues([
        scbValue,
        { ...scbValue, value: "A different summary." },
      ]),
    ).rejects.toThrow("Each field may appear only once per decision.");
    expect(clickhouse.query).not.toHaveBeenCalled();
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("refuses a batch that mixes companies, because one published check cannot cover both", async () => {
    await expect(
      appendSeCompanyInfoFieldValues([
        scbValue,
        { ...scbValue, companyId: "5560125220" },
      ]),
    ).rejects.toThrow("same company");
    expect(clickhouse.query).not.toHaveBeenCalled();
  });

  it("loadSeCompanyInfoDetail threads ids into the detail queries and returns null when missing", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadSeCompanyInfoDetail(COMPANY)).toBeNull();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
  });

  it("loadSeCompanyInfoDetail resolves the NACE label when the row carries a code", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          evidence_set_hash: "a".repeat(64),
          correction_ids: [],
          primary_nace_code: "62.01",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        { description_en: "62.01 Computer programming activities" },
      ]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.naceLabel).toBe("Computer programming activities");
    expect(clickhouse.query).toHaveBeenCalledTimes(5);
    expect(clickhouse.query).toHaveBeenNthCalledWith(5, NACE_LABEL_SQL, {
      code: "62.01",
    });
    // The published rows carry dot-less codes; the lookup matches
    // normalized_code and the prefix strip handles the dotted description.
    expect(NACE_LABEL_SQL).toContain("normalized_code");
  });

  it("loadSeCompanyInfoDetail strips the dotted code prefix for dot-less lookups", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: null,
          evidence_set_hash: "a".repeat(64),
          correction_ids: [],
          primary_nace_code: "6419",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        { description_en: "64.19 Other monetary intermediation" },
      ]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.naceLabel).toBe("Other monetary intermediation");
  });

  it("loadSeCompanyInfoDetail threads the published suggestion id and reads the field-value history", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
          company_id: COMPANY,
          suggestion_id: "11111111-1111-4111-8111-111111111111",
          evidence_set_hash: "a".repeat(64),
          correction_ids: ["22222222-2222-4222-8222-222222222222"],
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([RELEASED_VALUE_ROW]);
    const detail = await loadSeCompanyInfoDetail(COMPANY);
    expect(detail).not.toBeNull();
    // The history is handed through as the query returned it, null source_at
    // and all.
    expect(detail?.fieldValues).toEqual([RELEASED_VALUE_ROW]);
    expect(detail?.fieldValues[0].source_at).toBeNull();
    expect(clickhouse.query).toHaveBeenCalledTimes(4);
    expect(clickhouse.query).toHaveBeenNthCalledWith(2, ARTIFACT_ROWS_SQL, { companyId: COMPANY });
    expect(clickhouse.query).toHaveBeenNthCalledWith(3, SUGGESTIONS_SQL, {
      companyId: COMPANY,
      publishedSuggestionId: "11111111-1111-4111-8111-111111111111",
    });
    // The field-value store needs nothing but the company: which row is live
    // is decided inside the SQL, not by a hash or an applied-id list the
    // published row carries.
    expect(clickhouse.query).toHaveBeenNthCalledWith(4, FIELD_VALUES_SQL, {
      companyId: COMPANY,
    });
  });

  it("parses each artifact's payload_json into a name->string map, and never lets a bad one throw", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        { company_id: COMPANY, suggestion_id: null, evidence_set_hash: "a".repeat(64), correction_ids: [] },
      ])
      .mockResolvedValueOnce([
        {
          source: "scb",
          source_record_uid: "scb:1",
          observed_at: "2026-08-01 00:00:00.000",
          evidence_hash: "b".repeat(64),
          payload_json: '{"legal_name":"Alpha AB","incorporation_date":"2001-02-03"}',
        },
        {
          source: "wikidata",
          source_record_uid: "wikidata:Q1",
          observed_at: "2026-08-01 00:00:00.000",
          evidence_hash: "c".repeat(64),
          payload_json: "not json",
        },
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    const detail = await loadSeCompanyInfoDetail(COMPANY);

    expect(detail?.artifacts[0]).toEqual({
      source: "scb",
      source_record_uid: "scb:1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "b".repeat(64),
      payload: { legal_name: "Alpha AB", incorporation_date: "2001-02-03" },
    });
    // A malformed payload is an empty map, not a 500 on the review page.
    expect(detail?.artifacts[1].payload).toEqual({});
  });
});
