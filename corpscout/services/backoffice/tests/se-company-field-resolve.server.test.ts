import { beforeEach, describe, expect, it, vi } from "vitest";

// The fake ClickHouse command runner: every statement the resolver executes
// lands here, in order, with the exact parameters it bound.
const clickhouse = vi.hoisted(() => ({ command: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chCommand: clickhouse.command }));
const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);

import {
  formatClickHouseDateTime64,
  resolveCompanyFields,
  SeCompanyFieldResolveError,
} from "~/lib/se-company-field-resolve.server";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const COMPANY = "5565200028";
const NOW = new Date("2026-09-02T10:15:30.123Z");
const entry = (field: string) =>
  REGISTRY_FIXTURE.fields.find((candidate) => candidate.field === field)!;

describe("formatClickHouseDateTime64", () => {
  // The driver renders a JS Date as epoch seconds ("1788...") which ClickHouse
  // accepts but which is not the text form the field-value rows carry; the
  // resolver binds this text so resolved_at reads like created_at.
  it("renders the DateTime64(3) text form in UTC, millisecond precision kept", () => {
    expect(formatClickHouseDateTime64(NOW)).toBe("2026-09-02 10:15:30.123");
    expect(
      formatClickHouseDateTime64(new Date("2026-01-05T00:00:00.000Z")),
    ).toBe("2026-01-05 00:00:00.000");
  });
});

describe("resolveCompanyFields", () => {
  beforeEach(() => {
    clickhouse.command.mockReset();
    clickhouse.command.mockResolvedValue(undefined);
    registryModule.loadFieldRegistry.mockReset();
  });

  it("runs each decided field's statement in field order, then the projection, with exact parameters", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["description", "legal_name"],
      { registry: REGISTRY_FIXTURE, now: NOW, sourceRunId: "backoffice:test" },
    );

    expect(result).toEqual({
      resolved: ["description", "legal_name"],
      skipped: [],
    });
    expect(clickhouse.command.mock.calls).toEqual([
      [
        entry("description").resolveSql,
        {
          field: "description",
          company_ids: [COMPANY],
          source_run_id: "backoffice:test",
          resolved_at: "2026-09-02 10:15:30.123",
        },
      ],
      [
        entry("legal_name").resolveSql,
        {
          field: "legal_name",
          company_ids: [COMPANY],
          source_run_id: "backoffice:test",
          resolved_at: "2026-09-02 10:15:30.123",
        },
      ],
      [REGISTRY_FIXTURE.projectionSql, { company_ids: [COMPANY] }],
    ]);
    expect(registryModule.loadFieldRegistry).not.toHaveBeenCalled();
  });

  it("stamps one backoffice:<uuid> run id on every statement when none is given", async () => {
    await resolveCompanyFields(COMPANY, ["description", "description_sv"], {
      registry: REGISTRY_FIXTURE,
    });

    const runIds = clickhouse.command.mock.calls
      .slice(0, 2)
      .map(([, params]) => (params as { source_run_id: string }).source_run_id);
    expect(runIds[0]).toMatch(
      /^backoffice:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(runIds[1]).toBe(runIds[0]);
    for (const [, params] of clickhouse.command.mock.calls.slice(0, 2)) {
      expect((params as { resolved_at: string }).resolved_at).toMatch(
        /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/,
      );
    }
  });

  // A python_only field is resolved by Dagster alone (spec 4.1 / 9): the
  // backoffice reports it so the page can say "applies on the next run".
  it("skips a python_only field with its reason and still projects the rest", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["website", "description"],
      { registry: REGISTRY_FIXTURE, now: NOW, sourceRunId: "backoffice:test" },
    );

    expect(result).toEqual({
      resolved: ["description"],
      skipped: [{ field: "website", reason: "python_only" }],
    });
    expect(clickhouse.command.mock.calls.map(([sql]) => sql)).toEqual([
      entry("description").resolveSql,
      REGISTRY_FIXTURE.projectionSql,
    ]);
  });

  it("skips a field the registry does not know", async () => {
    const result = await resolveCompanyFields(COMPANY, ["not_a_field"], {
      registry: REGISTRY_FIXTURE,
    });

    expect(result).toEqual({
      resolved: [],
      skipped: [{ field: "not_a_field", reason: "unknown_field" }],
    });
  });

  it("does not re-pivot the wide row when nothing was resolved", async () => {
    await resolveCompanyFields(COMPANY, ["website"], {
      registry: REGISTRY_FIXTURE,
    });
    expect(clickhouse.command).not.toHaveBeenCalled();
  });

  // The live test's production branch resolves a synthetic company into the
  // long table only: the wide row would flow into se_companies_serving.
  it("writes the long table only when project is false", async () => {
    const result = await resolveCompanyFields(
      COMPANY,
      ["description", "legal_name"],
      {
        registry: REGISTRY_FIXTURE,
        now: NOW,
        sourceRunId: "backoffice:test",
        project: false,
      },
    );

    expect(result).toEqual({
      resolved: ["description", "legal_name"],
      skipped: [],
    });
    expect(clickhouse.command.mock.calls.map(([sql]) => sql)).toEqual([
      entry("description").resolveSql,
      entry("legal_name").resolveSql,
    ]);
  });

  it("loads the registry when the caller does not pass one", async () => {
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);

    await resolveCompanyFields(COMPANY, ["description"], {
      now: NOW,
      sourceRunId: "backoffice:test",
    });

    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(1);
    expect(clickhouse.command).toHaveBeenCalledTimes(2);
  });

  it("lets a ClickHouse failure propagate untouched", async () => {
    clickhouse.command.mockRejectedValueOnce(new Error("Code: 241. DB::Exception: Memory limit"));
    await expect(
      resolveCompanyFields(COMPANY, ["description"], { registry: REGISTRY_FIXTURE }),
    ).rejects.toThrow("Memory limit");
  });
});

describe("SeCompanyFieldResolveError", () => {
  it("names what was saved and why resolving failed", () => {
    const error = new SeCompanyFieldResolveError(
      ["66666666-6666-4666-8666-666666666666"],
      new Error("Code: 241. DB::Exception: Memory limit"),
    );
    expect(error.name).toBe("SeCompanyFieldResolveError");
    expect(error.valueIds).toEqual(["66666666-6666-4666-8666-666666666666"]);
    expect(error.message).toBe(
      "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
    );
    expect(error.cause).toBeInstanceOf(Error);
  });
});
