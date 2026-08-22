import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyPersonCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  appendSeCompanyPersonCorrection,
  seCompanyPersonId,
} from "~/lib/se-company-person.server";
import { SePersonCorrectionValidationError } from "~/lib/se-person-corrections";

const COMPANY = "5565200028";
const PERSON = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

describe("seCompanyPersonId", () => {
  it("matches the Dagster person_id_for hash", () => {
    expect(seCompanyPersonId(COMPANY, "David Mindus")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "  david   MINDUS ")).toBe(PERSON);
    expect(seCompanyPersonId(COMPANY, "Anna Karin Svensson")).toBe(
      "6942ffc1-e104-ebea-7aa0-ef7377e8a508",
    );
  });
});

describe("appendSeCompanyPersonCorrection", () => {
  beforeEach(() => {
    clickhouse.insert.mockReset();
    clickhouse.query.mockReset();
  });

  it("refuses when the published evidence hash moved", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "b".repeat(64) }]);

    await expect(
      appendSeCompanyPersonCorrection({
        companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
        payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
        reason: "spelling", activeRoleCodes: new Set(),
      }),
    ).rejects.toThrow(SePersonCorrectionValidationError);
    expect(clickhouse.insert).not.toHaveBeenCalled();
  });

  it("appends one row with backoffice provenance", async () => {
    clickhouse.query.mockResolvedValueOnce([{ draft_set_hash: "a".repeat(64) }]);
    clickhouse.insert.mockResolvedValue(undefined);

    const result = await appendSeCompanyPersonCorrection({
      companyId: COMPANY, kind: "override_field", subjectPersonId: PERSON,
      payload: { name: "David G. Mindus" }, evidenceHash: "a".repeat(64),
      reason: "spelling", activeRoleCodes: new Set(),
    });

    expect(result.correctionId).toMatch(/^[0-9a-f-]{36}$/);
    const [rows] = clickhouse.insert.mock.calls[0];
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      correction_id: result.correctionId,
      company_id: COMPANY,
      correction_kind: "override_field",
      subject_person_id: PERSON,
      payload: JSON.stringify({ name: "David G. Mindus" }),
      decided_by: "backoffice",
    });
    expect(rows[0].created_at).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });
});
